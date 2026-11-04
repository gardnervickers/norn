#![cfg(target_os = "linux")]

//! Driver shutdown must reclaim a linked request whose dependent is issued
//! only after shutdown's synchronous cancellation has run.

use std::io;
use std::os::fd::{AsRawFd, OwnedFd};
use std::sync::mpsc;
use std::time::Duration;

use norn_uring::{CQEResult, Handle, Operation, Request, Singleshot};

/// A NOP, or a read from a pipe that is never written and so completes only
/// when it is cancelled.
enum Probe {
    Nop,
    BlockedRead { fd: i32, buf: Box<[u8; 8]> },
}

// Safety: the read targets a pipe kept open by the test until the driver has
// shut down, into a boxed buffer owned by the operation; each entry produces
// one terminal CQE.
unsafe impl Operation for Probe {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(match self {
            Probe::Nop => io_uring::opcode::Nop::new().build(),
            Probe::BlockedRead { fd, buf } => {
                io_uring::opcode::Read::new(io_uring::types::Fd(*fd), buf.as_mut_ptr(), 8).build()
            }
        })
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Probe {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0; 2];
    // Safety: `fds` has room for both descriptors.
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    // Safety: pipe2 returned two new descriptors owned by nobody else.
    unsafe {
        use std::os::fd::FromRawFd;
        (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1]))
    }
}

fn drop_runtime_with_issuing_dependent(builder: io_uring::Builder) {
    let (done, finished) = mpsc::channel();
    std::thread::spawn(move || {
        let (reader, _writer) = pipe();
        let driver = norn_uring::Driver::new(builder, 8).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let fd = reader.as_raw_fd();
        ex.block_on(async move {
            let handle = Handle::current();
            let chain = handle
                .submit(Probe::Nop)
                .then(handle.submit(Probe::BlockedRead {
                    fd,
                    buf: Box::new([0; 8]),
                }));
            // The task queues both entries and then waits on the read.
            drop(norn_executor::spawn(async move {
                let _ = chain.await;
            }));
            yield_now().await;
        });
        // Dropping the runtime drops the task, queueing cancellations behind
        // the chain. Shutdown submits them all: the NOP completes, the read is
        // queued to be issued, and both cancellations find nothing.
        drop(ex);
        drop(reader);
        done.send(()).unwrap();
    });
    finished
        .recv_timeout(Duration::from_secs(5))
        .expect("driver shutdown never reclaimed the linked read");
}

async fn yield_now() {
    let mut yielded = false;
    std::future::poll_fn(|cx| {
        if yielded {
            return std::task::Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
    .await
}

#[test]
fn shutdown_reclaims_a_linked_read_issued_after_cancellation() {
    let mut builder = io_uring::IoUring::builder();
    builder
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer();
    drop_runtime_with_issuing_dependent(builder);
}

#[test]
fn shutdown_reclaims_a_linked_read_with_default_setup() {
    drop_runtime_with_issuing_dependent(io_uring::IoUring::builder());
}
