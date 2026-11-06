//! The stall watchdog reports a worker that runs too long without parking,
//! once per stall, and never an idle or finished one.

use std::sync::mpsc;
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
