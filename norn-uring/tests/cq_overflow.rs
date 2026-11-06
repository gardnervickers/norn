#![cfg(target_os = "linux")]

//! Completions the kernel holds back from the CQ ring (an overflow list, or
//! deferred task work) must still be delivered when parks skip waiting.

use std::sync::mpsc;
use std::time::Duration;

use norn_executor::park::{Park, Unpark};
use norn_executor::{spawn, LocalExecutor};
use norn_uring::{Driver, DriverOptions, TimeoutOutcome};

/// Keep the kernel's default CQ, twice the SQ, so these tests can overflow it.
fn small_cq() -> DriverOptions {
    DriverOptions {
        cq_entries: None,
        ..DriverOptions::default()
    }
}

/// A burst of completions overflows the CQ while the runtime thread is busy,
/// and another thread then unparks the runtime. The eventfd read's completion
/// is queued behind the overflow, so every later waiting park degrades to a
/// non-blocking park that never enters the kernel. The executor spins and the
/// overflowed completions are never delivered.
#[test]
fn remote_wake_during_cq_overflow_does_not_livelock_the_executor() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        // One SQ entry, two CQ entries.
        let driver = Driver::new_with_options(io_uring::IoUring::builder(), 1, small_cq()).unwrap();
        let unparker = driver.unparker();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async move {
            let timers: Vec<_> = (0..3)
                .map(|_| spawn(handle.timeout(Duration::from_millis(20))))
                .collect();
            // An idle wait arms the eventfd read used for remote unparks.
            assert_eq!(
                handle.timeout(Duration::from_millis(5)).await.unwrap(),
                TimeoutOutcome::Expired
            );
            // The runtime thread is busy while all three timers expire, so
            // their completions overflow the two-entry CQ.
            std::thread::sleep(Duration::from_millis(100));
            // Another thread unparks the runtime, as a channel send does.
            std::thread::spawn(move || unparker.unpark())
                .join()
                .unwrap();
            for timer in timers {
                assert_eq!(timer.await.unwrap().unwrap(), TimeoutOutcome::Expired);
            }
        });
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("executor livelocked: overflowed completions were never delivered");
}

/// With `IORING_SETUP_DEFER_TASKRUN` (the configuration used by the project's
/// io_uring benchmarks) completions are only posted by an enter that carries
/// `IORING_ENTER_GETEVENTS`. A remote unpark while the eventfd read is armed
/// leaves the unparker woken and parked until that read's CQE is reaped, but
/// every waiting park now degrades to a non-blocking park that makes no
/// `GETEVENTS` enter, so the CQE is never posted.
#[test]
fn remote_wake_with_defer_taskrun_does_not_livelock_the_executor() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut builder = io_uring::IoUring::builder();
        builder
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let driver = Driver::new_with_options(builder, 8, small_cq()).unwrap();
        let unparker = driver.unparker();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async move {
            // An idle wait arms the eventfd read used for remote unparks.
            assert_eq!(
                handle.timeout(Duration::from_millis(5)).await.unwrap(),
                TimeoutOutcome::Expired
            );
            // Another thread unparks the runtime while it is running.
            std::thread::spawn(move || unparker.unpark())
                .join()
                .unwrap();
            // Any later I/O wait never completes.
            assert_eq!(
                handle.timeout(Duration::from_millis(5)).await.unwrap(),
                TimeoutOutcome::Expired
            );
        });
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("executor livelocked: deferred completions were never posted");
}

/// Yield to the executor once.
struct YieldNow(bool);

impl std::future::Future for YieldNow {
    type Output = ();

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        if self.0 {
            return std::task::Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    }
}

/// Without any remote wake: after a CQ overflow, a runtime that always has
/// runnable work parks only with `NoPark`. With an empty SQ that park makes no
/// syscall, so the overflow list is never flushed and the overflowed I/O
/// completion is starved for as long as the runtime stays busy.
#[test]
fn cq_overflow_does_not_starve_completions_while_tasks_are_runnable() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let driver = Driver::new_with_options(io_uring::IoUring::builder(), 1, small_cq()).unwrap();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);
        let completed = executor.block_on(async move {
            let done = std::rc::Rc::new(std::cell::Cell::new(0));
            for _ in 0..3 {
                let done = std::rc::Rc::clone(&done);
                let timer = handle.timeout(Duration::from_millis(20));
                drop(spawn(async move {
                    timer.await.unwrap();
                    done.set(done.get() + 1);
                }));
            }
            // Let the timers reach the kernel.
            handle.timeout(Duration::from_millis(5)).await.unwrap();
            // The runtime is busy while all three expire: the 2-entry CQ overflows.
            std::thread::sleep(Duration::from_millis(100));
            // Keep the runtime busy with CPU work that yields cooperatively.
            for _ in 0..100_000 {
                if done.get() == 3 {
                    break;
                }
                YieldNow(false).await;
            }
            done.get()
        });
        tx.send(completed).unwrap();
    });
    let completed = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("executor thread blocked");
    assert_eq!(
        completed, 3,
        "an overflowed completion was starved across 100000 executor turns"
    );
}

/// With `IORING_SETUP_DEFER_TASKRUN` (the project's io_uring benchmark
/// configuration) and no overflow at all: a runtime that always has runnable
/// work never makes a `GETEVENTS` enter, so no I/O completion is ever posted
/// while it stays busy.
#[test]
fn defer_taskrun_does_not_starve_completions_while_tasks_are_runnable() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut builder = io_uring::IoUring::builder();
        builder
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let driver = Driver::new_with_options(builder, 64, small_cq()).unwrap();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);
        let completed = executor.block_on(async move {
            let done = std::rc::Rc::new(std::cell::Cell::new(false));
            let timer = handle.timeout(Duration::from_millis(1));
            {
                let done = std::rc::Rc::clone(&done);
                drop(spawn(async move {
                    timer.await.unwrap();
                    done.set(true);
                }));
            }
            let start = std::time::Instant::now();
            // Cooperative CPU work for up to two seconds.
            while !done.get() && start.elapsed() < Duration::from_secs(2) {
                YieldNow(false).await;
            }
            done.get()
        });
        tx.send(completed).unwrap();
    });
    let completed = rx
        .recv_timeout(Duration::from_secs(30))
        .expect("executor thread blocked");
    assert!(
        completed,
        "a 1 ms timeout did not complete during 2 s of cooperative work"
    );
}

/// On an SQPOLL ring the kernel thread posts completions without the driver
/// entering the kernel, so a burst of no-ops overflows a small CQ. Another
/// thread keeps unparking the runtime, so every park is non-blocking. The
/// overflow list is only flushed by an enter, which an empty SQ never made.
#[test]
fn sqpoll_overflow_with_remote_wakes_does_not_livelock_the_executor() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;

    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        for _ in 0..5 {
            let mut builder = io_uring::IoUring::builder();
            builder.setup_sqpoll(10);
            let driver = Driver::new_with_options(builder, 4, small_cq()).unwrap();
            let unparker = driver.unparker();
            let stop = Arc::new(AtomicBool::new(false));
            let waker = {
                let stop = Arc::clone(&stop);
                std::thread::spawn(move || {
                    while !stop.load(Ordering::Relaxed) {
                        unparker.unpark();
                        std::thread::yield_now();
                    }
                })
            };
            let mut executor = LocalExecutor::new(driver);
            executor.block_on(async {
                for _ in 0..20 {
                    let ops: Vec<_> = (0..64).map(|_| norn_uring::noop()).collect();
                    futures_util::future::join_all(ops).await;
                }
            });
            stop.store(true, Ordering::Relaxed);
            waker.join().unwrap();
        }
        tx.send(()).unwrap();
    });
    rx.recv_timeout(Duration::from_secs(10))
        .expect("executor livelocked: SQPOLL overflow was never flushed");
}
