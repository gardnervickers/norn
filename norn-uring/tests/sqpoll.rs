#![cfg(target_os = "linux")]

//! Submission-queue backpressure under `IORING_SETUP_SQPOLL`, where the kernel
//! thread consumes SQEs without a synchronous submit reporting them.

use std::future::Future;
use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::net::UnixStream;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode};
use norn_uring::{CQEResult, Driver, Operation, Singleshot};

struct Nop;

// Safety: NOP references no resources and produces one terminal CQE.
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
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

/// A read that stays in flight until its peer writes.
struct BlockedRead {
    reader: UnixStream,
    buf: Box<[u8; 8]>,
}

// Safety: the descriptor and boxed buffer are owned by the operation and stay
// valid until the operation is destroyed after its terminal CQE.
unsafe impl Operation for BlockedRead {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Read::new(
            io_uring::types::Fd(self.reader.as_raw_fd()),
            self.buf.as_mut_ptr(),
            self.buf.len() as u32,
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for BlockedRead {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

fn sqpoll_driver(entries: u32) -> Driver {
    let mut builder = io_uring::IoUring::builder();
    builder.setup_sqpoll(1000);
    Driver::new(builder, entries).expect("SQPOLL ring setup failed")
}

fn poll<F: Future>(fut: Pin<&mut F>, cx: &mut Context<'_>) -> Poll<F::Output> {
    fut.poll(cx)
}

/// Run `f` on its own thread and fail if it does not finish, instead of hanging.
fn bounded(name: &str, f: impl FnOnce() + Send + 'static) {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        f();
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .unwrap_or_else(|_| panic!("{name}: SQ waiter was stranded"));
}

#[test]
fn sqpoll_wakes_sq_waiter_after_kernel_consumes_queue() {
    bounded("nop", || {
        let mut driver = sqpoll_driver(1);
        let handle = driver.handle();
        let mut first = std::pin::pin!(handle.submit(Nop));
        let mut second = std::pin::pin!(handle.submit(Nop));
        let (waker, wakes) = futures_test::task::new_count_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(poll(first.as_mut(), &mut cx).is_pending());
        assert!(poll(second.as_mut(), &mut cx).is_pending());
        // Let the kernel thread consume the SQ before the next driver submit,
        // so no synchronous submit reports the freed space.
        std::thread::sleep(Duration::from_millis(20));

        let mut first_done = false;
        let mut second_done = false;
        for _ in 0..100 {
            driver.park(ParkMode::NoPark).unwrap();
            if !first_done {
                first_done = poll(first.as_mut(), &mut cx).is_ready();
            }
            if !second_done {
                second_done = poll(second.as_mut(), &mut cx).is_ready();
            }
            if first_done && second_done {
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(first_done && second_done, "wakes={}", wakes.get());
        assert!(handle.health_error().is_none());
    });
}

#[test]
fn sqpoll_repoll_observes_capacity_without_a_park() {
    bounded("repoll", || {
        let driver = sqpoll_driver(1);
        let handle = driver.handle();
        let mut first = std::pin::pin!(handle.submit(Nop));
        let mut second = std::pin::pin!(handle.submit(Nop));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(poll(first.as_mut(), &mut cx).is_pending());
        assert!(poll(second.as_mut(), &mut cx).is_pending());
        // Polling the driver once wakes the kernel thread if it is idle; after
        // that, a waiter re-poll must notice the free slot by itself.
        let mut driver = driver;
        driver.park(ParkMode::NoPark).unwrap();
        for _ in 0..500 {
            // A poll that submits returns Pending awaiting the CQE; reaching
            // that state proves the waiter left the SQ backpressure queue.
            let _ = poll(second.as_mut(), &mut cx);
            if !driver.needs_park() {
                return;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        panic!("re-polled waiter never observed free SQ capacity");
    });
}

#[test]
fn sqpoll_waiting_park_does_not_strand_waiter_behind_long_lived_sqe() {
    bounded("long-lived", || {
        let mut driver = sqpoll_driver(1);
        let handle = driver.handle();
        let (reader, writer) = UnixStream::pair().unwrap();
        let mut read = Box::pin(handle.submit(BlockedRead {
            reader,
            buf: Box::new([0; 8]),
        }));
        let mut nop = std::pin::pin!(handle.submit(Nop));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(poll(read.as_mut(), &mut cx).is_pending());
        assert!(poll(nop.as_mut(), &mut cx).is_pending());

        // Let the kernel thread consume the read, leaving room for the eventfd
        // read a waiting park arms. The read never completes, so a park that
        // waited for a CQE instead of handing out SQ capacity would block forever.
        std::thread::sleep(Duration::from_millis(20));
        let mut done = false;
        for _ in 0..100 {
            driver.park(ParkMode::NextCompletion).unwrap();
            if let Poll::Ready(result) = poll(nop.as_mut(), &mut cx) {
                result.unwrap();
                done = true;
                break;
            }
        }
        assert!(done, "waiter never submitted behind a long-lived SQE");
        drop(read);
        drop(writer);
    });
}

#[test]
fn sqpoll_dropped_waiter_hands_notification_to_next() {
    bounded("handoff", || {
        let mut driver = sqpoll_driver(1);
        let handle = driver.handle();
        let mut first = std::pin::pin!(handle.submit(Nop));
        let mut second = Box::pin(handle.submit(Nop));
        let mut third = std::pin::pin!(handle.submit(Nop));
        let waker = futures_test::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(poll(first.as_mut(), &mut cx).is_pending());
        assert!(poll(second.as_mut(), &mut cx).is_pending());
        assert!(poll(third.as_mut(), &mut cx).is_pending());
        std::thread::sleep(Duration::from_millis(20));

        // Free capacity is handed to `second`, which is then abandoned.
        driver.park(ParkMode::NoPark).unwrap();
        drop(second);
        let mut first_done = false;
        let mut done = false;
        for _ in 0..100 {
            driver.park(ParkMode::NoPark).unwrap();
            if !first_done {
                first_done = poll(first.as_mut(), &mut cx).is_ready();
            }
            if poll(third.as_mut(), &mut cx).is_ready() {
                done = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(2));
        }
        assert!(done, "a dropped waiter must pass its notification on");
    });
}
