#![cfg(target_os = "linux")]

//! Dropping a zero-copy send whose notification is still pending.
//!
//! ASYNC_CANCEL cannot find a zero-copy send after its primary CQE, so its
//! cancellation reports ENOENT while the operation stays incomplete until the
//! notification arrives. Retrying such a cancellation every park would turn
//! that wait into a busy loop.

use std::cell::Cell;
use std::future::{poll_fn, Future};
use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Poll;
use std::time::Duration;

use norn_uring::{CQEResult, Handle, Operation, Request, Singleshot};

struct ZcSend {
    fd: i32,
    buf: Vec<u8>,
    primary_seen: Rc<Cell<bool>>,
}

// Safety: `buf` is heap-allocated and owned by the operation until its
// terminal (notification) completion.
unsafe impl Operation for ZcSend {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::SendZc::new(
            io_uring::types::Fd(self.fd),
            self.buf.as_ptr(),
            self.buf.len() as u32,
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        if !result.is_notification() {
            self.primary_seen.set(true);
        }
        result
    }
}

impl Singleshot for ZcSend {
    type Output = ();

    fn update(&mut self, _completion: Self::Completion) {}

    fn complete(self, _completion: Self::Completion) -> Self::Output {}
}

struct Nop;

// Safety: the NOP references no memory.
unsafe impl Operation for Nop {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new().build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Nop {
    type Output = ();

    fn complete(self, _completion: Self::Completion) -> Self::Output {}
}

fn thread_cpu_time() -> Duration {
    let mut ts = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // Safety: `ts` is a valid output location.
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, &mut ts) },
        0
    );
    Duration::new(ts.tv_sec as u64, ts.tv_nsec as u32)
}

/// A connected client whose queued bytes, and so any zero-copy notification,
/// are held by TCP_CORK for the kernel's 200 ms cork limit.
fn corked_client() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let client = TcpStream::connect(listener.local_addr().unwrap()).unwrap();
    let (server, _) = listener.accept().unwrap();
    let one: libc::c_int = 1;
    // Safety: valid socket and option pointer.
    assert_eq!(
        unsafe {
            libc::setsockopt(
                client.as_raw_fd(),
                libc::IPPROTO_TCP,
                libc::TCP_CORK,
                (&one as *const libc::c_int).cast(),
                std::mem::size_of_val(&one) as libc::socklen_t,
            )
        },
        0
    );
    (client, server)
}

/// Drive the request until the send's primary CQE arrives, drop it, and return
/// the CPU time the runtime spends during a following 100 ms idle wait.
fn idle_cpu_after_drop<R, F>(make: F) -> Duration
where
    R: Future,
    F: FnOnce(&Handle, Rc<Cell<bool>>) -> R,
{
    idle_cpu_after_drop_with(io_uring::IoUring::builder(), make)
}

/// [`idle_cpu_after_drop`] on a ring from `builder`.
fn idle_cpu_after_drop_with<R, F>(builder: io_uring::Builder, make: F) -> Duration
where
    R: Future,
    F: FnOnce(&Handle, Rc<Cell<bool>>) -> R,
{
    let driver = norn_uring::Driver::new(builder, 32).unwrap();
    let mut ex = norn_executor::LocalExecutor::new(driver);
    ex.block_on(async {
        let handle = Handle::current();
        let primary_seen = Rc::new(Cell::new(false));
        let mut request: Pin<Box<R>> = Box::pin(make(&handle, Rc::clone(&primary_seen)));
        // Poll between short timed parks rather than waking ourselves: a
        // DEFER_TASKRUN ring issues a linked request only in a waiting enter.
        while !primary_seen.get() {
            poll_fn(|cx| {
                let _ = request.as_mut().poll(cx);
                Poll::Ready(())
            })
            .await;
            if !primary_seen.get() {
                handle
                    .timeout(Duration::from_millis(1))
                    .await
                    .expect("timeout failed");
            }
        }
        drop(request);

        let start = thread_cpu_time();
        handle
            .timeout(Duration::from_millis(100))
            .await
            .expect("timeout failed");
        thread_cpu_time() - start
    })
}

#[test]
fn dropped_zc_send_awaiting_notification_does_not_spin() {
    let (client, _server) = corked_client();
    let cpu = idle_cpu_after_drop(|handle, primary_seen| {
        handle.submit(ZcSend {
            fd: client.as_raw_fd(),
            buf: vec![7; 100],
            primary_seen,
        })
    });
    assert!(
        cpu < Duration::from_millis(30),
        "the driver spent {cpu:?} of CPU time during a 100 ms idle wait"
    );
}

#[test]
fn dropped_linked_zc_send_awaiting_notification_does_not_spin() {
    // A linked request's cancellation is retried after ENOENT, but only once.
    let (client, _server) = corked_client();
    let cpu = idle_cpu_after_drop(|handle, primary_seen| {
        handle.submit(Nop).then(handle.submit(ZcSend {
            fd: client.as_raw_fd(),
            buf: vec![7; 100],
            primary_seen,
        }))
    });
    assert!(
        cpu < Duration::from_millis(30),
        "the driver spent {cpu:?} of CPU time during a 100 ms idle wait"
    );
}

fn defer_taskrun_builder() -> Option<io_uring::Builder> {
    let mut builder = io_uring::IoUring::builder();
    builder
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer();
    if builder.build(2).is_err() {
        eprintln!("skipping: the kernel does not support DEFER_TASKRUN");
        return None;
    }
    Some(builder)
}

#[test]
fn dropped_zc_send_awaiting_notification_does_not_spin_with_defer_taskrun() {
    let Some(builder) = defer_taskrun_builder() else {
        return;
    };
    let (client, _server) = corked_client();
    let cpu = idle_cpu_after_drop_with(builder, |handle, primary_seen| {
        handle.submit(ZcSend {
            fd: client.as_raw_fd(),
            buf: vec![7; 100],
            primary_seen,
        })
    });
    assert!(
        cpu < Duration::from_millis(30),
        "the driver spent {cpu:?} of CPU time during a 100 ms idle wait"
    );
}

#[test]
fn dropped_linked_zc_send_awaiting_notification_does_not_spin_with_defer_taskrun() {
    let Some(builder) = defer_taskrun_builder() else {
        return;
    };
    let (client, _server) = corked_client();
    let cpu = idle_cpu_after_drop_with(builder, |handle, primary_seen| {
        handle.submit(Nop).then(handle.submit(ZcSend {
            fd: client.as_raw_fd(),
            buf: vec![7; 100],
            primary_seen,
        }))
    });
    assert!(
        cpu < Duration::from_millis(30),
        "the driver spent {cpu:?} of CPU time during a 100 ms idle wait"
    );
}
