#![cfg(target_os = "linux")]

//! `Request::cancel` cancels a request and still reports what it did.

use std::cell::Cell;
use std::future::Future;
use std::io::{self, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Duration;

use norn_uring::net::TcpListener;
use norn_uring::{CQEResult, Handle, Operation, Request, Singleshot, TimeoutOutcome};

/// A NOP, or a read from a pipe nobody writes, which completes only when it
/// is cancelled. Counts its configurations.
struct Probe {
    read_fd: Option<i32>,
    buf: Box<[u8; 8]>,
    configured: Rc<Cell<usize>>,
}

impl Probe {
    fn nop(configured: &Rc<Cell<usize>>) -> Self {
        Self {
            read_fd: None,
            buf: Box::new([0; 8]),
            configured: Rc::clone(configured),
        }
    }

    fn blocked_read(fd: i32, configured: &Rc<Cell<usize>>) -> Self {
        Self {
            read_fd: Some(fd),
            ..Self::nop(configured)
        }
    }
}

// Safety: the read targets a pipe the test keeps open until the driver shuts
// down, into a buffer owned by the operation; each entry has one terminal CQE.
unsafe impl Operation for Probe {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        self.configured.set(self.configured.get() + 1);
        Ok(match self.read_fd {
            None => io_uring::opcode::Nop::new().build(),
            Some(fd) => {
                io_uring::opcode::Read::new(io_uring::types::Fd(fd), self.buf.as_mut_ptr(), 8)
                    .build()
            }
        })
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

/// Whether the completion was synthetic, and its result.
type Outcome = (bool, io::Result<u32>);

impl Singleshot for Probe {
    type Output = Outcome;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        (completion.is_synthetic(), completion.into_result())
    }
}

fn assert_cancelled<T: std::fmt::Debug>(result: &io::Result<T>) {
    match result {
        Err(error) => assert_eq!(error.raw_os_error(), Some(libc::ECANCELED), "{error}"),
        Ok(value) => panic!("expected ECANCELED, got {value:?}"),
    }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0; 2];
    // Safety: `fds` has room for both descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // Safety: pipe2 returned two descriptors that nothing else owns.
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

/// Poll `future` once with a no-op waker.
fn poll_once<F: Future + ?Sized>(future: std::pin::Pin<&mut F>) -> Poll<F::Output> {
    let waker = futures_test::task::noop_waker();
    future.poll(&mut Context::from_waker(&waker))
}

async fn settle() {
    Handle::current()
        .timeout(Duration::from_millis(5))
        .await
        .unwrap();
}

fn run<F: Future>(entries: u32, f: impl FnOnce() -> F) -> F::Output {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), entries).unwrap();
    let mut ex = norn_executor::LocalExecutor::new(driver);
    ex.block_on(f())
}

#[test]
fn cancel_returns_bytes_that_arrived_first() {
    run(8, || async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8)
            .await
            .unwrap();
        let mut peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().await.unwrap();

        let mut recv = pin!(socket.recv(vec![0u8; 64]));
        assert!(poll_once(recv.as_mut()).is_pending());
        settle().await;
        peer.write_all(b"hello").unwrap();
        settle().await;
        // The receive completed before it was cancelled: dropping it here
        // would have lost these bytes.
        let (received, buffer) = recv.cancel().await;
        assert_eq!(&buffer[..received.unwrap()], b"hello");

        peer.write_all(b"world").unwrap();
        let (received, buffer) = socket.recv(vec![0u8; 64]).await;
        assert_eq!(&buffer[..received.unwrap()], b"world");
    });
}

#[test]
fn cancel_of_a_pending_receive_returns_ecanceled_and_the_buffer() {
    run(8, || async {
        let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8)
            .await
            .unwrap();
        let _peer = std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
        let (socket, _) = listener.accept().await.unwrap();

        let mut recv = pin!(socket.recv(vec![0u8; 64]));
        assert!(poll_once(recv.as_mut()).is_pending());
        settle().await;
        let (received, buffer) = recv.cancel().await;
        assert_cancelled(&received);
        assert_eq!(buffer.len(), 64);
    });
}

#[test]
fn cancel_before_the_first_poll_neither_configures_nor_submits() {
    run(8, || async {
        let configured = Rc::new(Cell::new(0));
        let mut op = pin!(Handle::current().submit(Probe::nop(&configured)));
        let (synthetic, result) = op.as_mut().cancel().await;
        assert!(synthetic);
        assert_cancelled(&result);
        assert_eq!(configured.get(), 0);
    });
}

#[test]
fn cancel_of_an_operation_waiting_for_sq_space_never_reaches_the_kernel() {
    // A one-entry SQ: the second operation waits for space.
    run(1, || async {
        let configured = Rc::new(Cell::new(0));
        let handle = Handle::current();
        let mut first = pin!(handle.submit(Probe::nop(&configured)));
        let mut second = pin!(handle.submit(Probe::nop(&configured)));
        assert!(poll_once(first.as_mut()).is_pending());
        assert!(poll_once(second.as_mut()).is_pending());

        let (synthetic, result) = second.as_mut().cancel().await;
        assert!(synthetic, "a queued entry must not reach the kernel");
        assert_cancelled(&result);
        let (synthetic, result) = first.await;
        assert_eq!((synthetic, result.unwrap()), (false, 0));
        assert_eq!(configured.get(), 2);
    });
}

#[test]
fn cancel_of_a_linked_chain_reports_every_member() {
    let (reader, _writer) = pipe();
    let fd = reader.as_raw_fd();
    run(8, || async move {
        let configured = Rc::new(Cell::new(0));
        let handle = Handle::current();
        let mut chain = pin!(handle
            .submit(Probe::nop(&configured))
            .then(handle.submit(Probe::blocked_read(fd, &configured))));
        assert!(poll_once(chain.as_mut()).is_pending());
        settle().await;

        let ((head_synthetic, head), (read_synthetic, read)) = chain.cancel().await;
        assert_eq!((head_synthetic, head.unwrap()), (false, 0));
        assert!(!read_synthetic, "the read reached the kernel");
        assert_cancelled(&read);
    });
    drop(reader);
}

#[test]
fn cancel_of_an_unsubmitted_chain_submits_nothing() {
    let (reader, _writer) = pipe();
    let fd = reader.as_raw_fd();
    run(8, || async move {
        let configured = Rc::new(Cell::new(0));
        let handle = Handle::current();
        let mut chain = pin!(handle
            .submit(Probe::nop(&configured))
            .then(handle.submit(Probe::blocked_read(fd, &configured))));

        let ((head_synthetic, head), (read_synthetic, read)) = chain.as_mut().cancel().await;
        assert!(head_synthetic && read_synthetic);
        assert_cancelled(&head);
        assert_cancelled(&read);
        assert_eq!(configured.get(), 0);
    });
    drop(reader);
}

#[test]
fn cancel_of_a_request_with_a_deadline_cancels_the_request() {
    let (reader, _writer) = pipe();
    let fd = reader.as_raw_fd();
    run(8, || async move {
        let configured = Rc::new(Cell::new(0));
        let mut read = pin!(Handle::current()
            .submit(Probe::blocked_read(fd, &configured))
            .timeout(Duration::from_secs(60)));
        assert!(poll_once(read.as_mut()).is_pending());
        settle().await;

        let (synthetic, result) = read.cancel().await;
        assert!(!synthetic);
        assert_cancelled(&result);
    });
    drop(reader);
}

#[test]
fn cancel_of_a_standalone_timeout_reports_canceled() {
    run(8, || async {
        let handle = Handle::current();
        let mut armed = pin!(handle.timeout(Duration::from_secs(60)));
        assert!(poll_once(armed.as_mut()).is_pending());
        settle().await;
        assert_eq!(armed.cancel().await.unwrap(), TimeoutOutcome::Canceled);

        let mut unpolled = pin!(handle.timeout(Duration::from_secs(60)));
        assert_eq!(
            unpolled.as_mut().cancel().await.unwrap(),
            TimeoutOutcome::Canceled
        );
    });
}
