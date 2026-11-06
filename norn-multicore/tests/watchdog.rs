//! The stall watchdog reports a worker that runs too long without parking,
//! once per stall, and never an idle or finished one.

use std::future::Future;
use std::pin::Pin;
use std::sync::mpsc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use norn_multicore::{RuntimeGroup, Stall, WorkerId};

/// Blocks the calling worker thread inside a named frame, so a captured
/// stack can be checked for it.
#[inline(never)]
fn stuck_in_marker_frame(duration: Duration) {
    std::thread::sleep(duration);
    std::hint::black_box(());
}

fn run_group(threshold: Duration, block_worker_one_for: Option<Duration>) -> Vec<Stall> {
    let (stalls_tx, stalls_rx) = mpsc::channel();
    let mut builder = RuntimeGroup::builder(2);
    builder.stall_watchdog(threshold, move |stall| {
        stalls_tx.send(stall.clone()).unwrap();
    });
    // Worker 0 stays idle in its park for the whole run.
    builder
        .worker(WorkerId::new(0), |context| async move {
            context.shutdown_requested().await;
        })
        .unwrap();
    builder
        .worker(WorkerId::new(1), move |context| async move {
            if let Some(duration) = block_worker_one_for {
                stuck_in_marker_frame(duration);
            }
            context.request_shutdown();
        })
        .unwrap();
    let group = builder.start().unwrap();
    // Leave the watchdog time to sample the finished and idle workers too.
    let deadline = Instant::now() + Duration::from_millis(400);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    group.join().unwrap();
    stalls_rx.try_iter().collect()
}

#[test]
fn reports_a_blocked_worker_once() {
    let stalls = run_group(Duration::from_millis(50), Some(Duration::from_millis(300)));
    assert_eq!(stalls.len(), 1, "{stalls:?}");
    let stall = &stalls[0];
    assert_eq!(stall.worker, WorkerId::new(1));
    assert!(stall.running_for >= Duration::from_millis(50), "{stall:?}");
    assert!(stall.executor.running, "{stall:?}");
    #[cfg(not(all(feature = "stall-stacks", target_os = "linux")))]
    assert!(stall.stack.is_none());
}

#[test]
fn never_reports_idle_or_finished_workers() {
    let stalls = run_group(Duration::from_millis(20), None);
    assert!(stalls.is_empty(), "{stalls:?}");
}

#[cfg(all(feature = "stall-stacks", target_os = "linux"))]
#[test]
fn captures_the_stuck_workers_stack() {
    let stalls = run_group(Duration::from_millis(50), Some(Duration::from_millis(300)));
    assert_eq!(stalls.len(), 1, "{stalls:?}");
    let stack = stalls[0].stack.as_ref().expect("no stack captured");
    assert!(
        stack
            .iter()
            .any(|frame| frame.contains("stuck_in_marker_frame")),
        "{stack:#?}"
    );
}

/// Returns `Pending` once after waking itself, so the executor parks.
struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// A slow callback for one worker must not make another worker, whose runs
/// stay under the threshold, look stuck.
#[test]
fn a_slow_callback_does_not_report_other_workers() {
    let threshold = Duration::from_millis(100);
    let (stalls_tx, stalls_rx) = mpsc::channel::<Stall>();
    let mut builder = RuntimeGroup::builder(2);
    builder.stall_watchdog(threshold, move |stall| {
        stalls_tx.send(stall.clone()).unwrap();
        if stall.worker == WorkerId::new(0) {
            std::thread::sleep(Duration::from_millis(400));
        }
    });
    builder
        .worker(WorkerId::new(0), |context| async move {
            for _ in 0..5 {
                std::thread::sleep(Duration::from_millis(200));
                YieldNow(false).await;
                std::thread::sleep(Duration::from_millis(5));
                YieldNow(false).await;
            }
            context.shutdown_requested().await;
        })
        .unwrap();
    builder
        .worker(WorkerId::new(1), |context| async move {
            let end = Instant::now() + Duration::from_millis(3000);
            while Instant::now() < end {
                // Each run is 90 ms, under the threshold.
                std::thread::sleep(Duration::from_millis(90));
                YieldNow(false).await;
            }
            context.request_shutdown();
        })
        .unwrap();
    builder.start().unwrap().join().unwrap();
    let stalls: Vec<Stall> = stalls_rx.try_iter().collect();
    assert!(
        stalls.iter().all(|stall| stall.worker == WorkerId::new(0)),
        "{stalls:?}"
    );
    assert!(!stalls.is_empty());
}

/// A capture signal left pending on one worker by a withdrawn request must
/// not be taken for another worker's stack.
#[cfg(all(feature = "stall-stacks", target_os = "linux"))]
#[test]
fn a_late_signal_is_not_taken_for_another_workers_stack() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, OnceLock};

    fn set_blocked(block: bool) {
        // Safety: a signal mask change on the calling thread.
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGRTMIN() + 3);
            let how = if block {
                libc::SIG_BLOCK
            } else {
                libc::SIG_UNBLOCK
            };
            assert_eq!(libc::pthread_sigmask(how, &set, std::ptr::null_mut()), 0);
        }
    }

    #[inline(never)]
    fn worker_zero_marker(duration: Duration) {
        std::thread::sleep(duration);
        std::hint::black_box(());
    }

    /// Unblocks the signal inside a named frame, which is where the pending
    /// signal is delivered.
    #[inline(never)]
    fn worker_zero_unblocks() {
        set_blocked(false);
        std::hint::black_box(());
    }

    #[inline(never)]
    fn worker_one_marker(duration: Duration) {
        std::thread::sleep(duration);
        std::hint::black_box(());
    }

    let (stalls_tx, stalls_rx) = mpsc::channel::<Stall>();
    let mut builder = RuntimeGroup::builder(2);
    builder.stall_watchdog(Duration::from_millis(40), move |stall| {
        stalls_tx.send(stall.clone()).unwrap();
    });
    let start = Arc::new(OnceLock::<Instant>::new());
    let go = Arc::new(AtomicBool::new(false));
    {
        let (start, go) = (Arc::clone(&start), Arc::clone(&go));
        builder
            .worker(WorkerId::new(0), move |context| async move {
                while !go.load(Ordering::Acquire) {
                    YieldNow(false).await;
                }
                let start = *start.get().unwrap();
                // Stalled with the signal blocked: the capture is withdrawn
                // and the signal stays pending on this thread.
                set_blocked(true);
                worker_zero_marker(Duration::from_millis(250));
                while start.elapsed() < Duration::from_millis(560) {
                    std::thread::sleep(Duration::from_millis(1));
                    YieldNow(false).await;
                }
                // Delivered now, while worker 1's capture is pending.
                worker_zero_unblocks();
                context.shutdown_requested().await;
            })
            .unwrap();
    }
    {
        let (start, go) = (Arc::clone(&start), Arc::clone(&go));
        builder
            .worker(WorkerId::new(1), move |context| async move {
                while !go.load(Ordering::Acquire) {
                    YieldNow(false).await;
                }
                let start = *start.get().unwrap();
                while start.elapsed() < Duration::from_millis(450) {
                    std::thread::sleep(Duration::from_millis(1));
                    YieldNow(false).await;
                }
                set_blocked(true);
                worker_one_marker(Duration::from_millis(300));
                set_blocked(false);
                context.request_shutdown();
            })
            .unwrap();
    }
    let group = builder.start().unwrap();
    start.set(Instant::now()).unwrap();
    go.store(true, Ordering::Release);
    group.join().unwrap();
    let stalls: Vec<Stall> = stalls_rx.try_iter().collect();
    for stall in stalls
        .iter()
        .filter(|stall| stall.worker == WorkerId::new(1))
    {
        if let Some(stack) = &stall.stack {
            assert!(
                !stack.iter().any(|frame| frame.contains("worker_zero")),
                "{stack:#?}"
            );
        }
    }
}
