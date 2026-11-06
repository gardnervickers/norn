#![cfg(target_os = "linux")]

//! Ring counters are exact, readable from other threads, and survive the
//! driver.

use std::time::Duration;

use norn_executor::park::{Park, Unpark};
use norn_executor::{spawn, LocalExecutor};
use norn_uring::{Driver, RingStatus, TimeoutOutcome};

#[test]
fn counts_submissions_completions_and_lifecycle() {
    let driver = Driver::new(io_uring::IoUring::builder(), 32).unwrap();
    let metrics = driver.metrics();
    let mut executor = LocalExecutor::new(driver);
    executor.block_on(async {
        for _ in 0..100 {
            norn_uring::noop().await.unwrap();
        }
    });
    let snapshot = metrics.snapshot();
    assert!(snapshot.sqes_submitted >= 100, "{snapshot:?}");
    assert!(snapshot.cqes >= 100, "{snapshot:?}");
    assert!(snapshot.enters >= 1, "{snapshot:?}");
    assert_eq!(snapshot.sq_backlog, 0);
    assert_eq!(snapshot.cqes_dropped, 0);
    assert_eq!(snapshot.status, RingStatus::Running);

    drop(executor);
    let snapshot = metrics.snapshot();
    assert_eq!(snapshot.status, RingStatus::Shutdown);
    // Every entry the driver pushed has been reaped.
    assert_eq!(snapshot.in_flight, 0, "{snapshot:?}");
    assert_eq!(snapshot.sqes_submitted, snapshot.cqes, "{snapshot:?}");
}

/// Entries that wait for room in a small submission queue are counted, and
/// the backlog gauge returns to zero once they are submitted.
#[test]
fn counts_submission_backlog() {
    let driver = Driver::new(io_uring::IoUring::builder(), 4).unwrap();
    let metrics = driver.metrics();
    let mut executor = LocalExecutor::new(driver);
    executor.block_on(async {
        let noops = (0..64).map(|_| norn_uring::noop());
        for result in futures_util::future::join_all(noops).await {
            result.unwrap();
        }
    });
    let snapshot = metrics.snapshot();
    assert!(snapshot.sq_backlogged >= 60, "{snapshot:?}");
    assert_eq!(snapshot.sq_backlog, 0, "{snapshot:?}");
}

/// The overflow scenario from `cq_overflow.rs`: a burst of completions
/// overflows a two-entry CQ while the runtime thread is busy.
#[test]
fn counts_completion_queue_overflow() {
    let metrics = std::thread::spawn(|| {
        let driver = Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let metrics = driver.metrics();
        let unparker = driver.unparker();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async move {
            let timers: Vec<_> = (0..3)
                .map(|_| spawn(handle.timeout(Duration::from_millis(20))))
                .collect();
            assert_eq!(
                handle.timeout(Duration::from_millis(5)).await.unwrap(),
                TimeoutOutcome::Expired
            );
            std::thread::sleep(Duration::from_millis(100));
            std::thread::spawn(move || unparker.unpark())
                .join()
                .unwrap();
            for timer in timers {
                assert_eq!(timer.await.unwrap().unwrap(), TimeoutOutcome::Expired);
            }
        });
        metrics
    })
    .join()
    .unwrap();
    let snapshot = metrics.snapshot();
    assert!(snapshot.cq_overflows >= 1, "{snapshot:?}");
    assert_eq!(snapshot.cqes_dropped, 0, "{snapshot:?}");
    assert!(snapshot.unparks >= 1, "{snapshot:?}");
}

/// Another thread reads a ring's counters while its thread is parked.
#[test]
fn counters_are_readable_while_the_ring_waits() {
    let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let metrics = driver.metrics();
    let handle = driver.handle();
    let mut executor = LocalExecutor::new(driver);
    let reader = std::thread::spawn(move || {
        // The ring parks in a 200 ms timed wait; sample it meanwhile.
        std::thread::sleep(Duration::from_millis(50));
        metrics.snapshot()
    });
    executor.block_on(async move {
        assert_eq!(
            handle.timeout(Duration::from_millis(200)).await.unwrap(),
            TimeoutOutcome::Expired
        );
    });
    let during = reader.join().unwrap();
    assert!(during.waiting_enters >= 1, "{during:?}");
    // The timeout operation was in flight when the ring parked.
    assert!(during.in_flight >= 1, "{during:?}");
}

/// The ring and the timer wheel above it each add their view to the
/// executor's metric sources.
#[test]
fn park_stack_adds_ring_and_timer_sources() {
    let ring = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let timer = norn_timer::Driver::new(ring, norn_timer::Clock::system());
    let mut executor = LocalExecutor::new(timer);
    executor.block_on(async {
        norn_timer::Handle::current()
            .sleep(Duration::from_millis(1))
            .await
            .unwrap();
        norn_uring::noop().await.unwrap();
    });
    let sources = executor.metric_sources();
    assert_eq!(sources.len(), 3);
    let ring = sources.get::<norn_uring::RingMetrics>().unwrap().snapshot();
    assert!(ring.cqes >= 1, "{ring:?}");
    let timer = sources
        .get::<norn_timer::TimerMetrics>()
        .unwrap()
        .snapshot();
    assert_eq!(timer.registered, 1);
    assert_eq!(timer.fired, 1);
    let mut names = Vec::new();
    norn_metrics::Visit::visit(&sources, &mut |metric| names.push(metric.name));
    let position = |name| names.iter().position(|n| *n == name).unwrap();
    // Executor first, then the timer, then the ring it wraps.
    assert!(position("norn_executor_polls_total") < position("norn_timer_fired_total"));
    assert!(position("norn_timer_fired_total") < position("norn_uring_cqes_total"));
}
