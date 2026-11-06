//! Dropping a `RuntimeGroup` while its owner thread is panicking must not
//! block joining workers: a worker that ignores shutdown would otherwise turn
//! the panic into a hang.
//!
//! The group detaches its workers in that case, so the tests release every
//! worker and wait for it to finish before returning. Miri rejects a test
//! that leaves a thread running when the test process ends.

use std::sync::mpsc;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use norn_multicore::{Builder, WorkerId};

/// Wait until the worker that held a clone of `alive` has dropped its future,
/// then give the rest of its thread teardown time to finish.
///
/// A detached worker has no join handle to wait on. Dropping the future is
/// the last thing the test can observe, and the thread exits right after it.
/// Sleeping lets that remainder run: under Miri the only runnable thread is
/// the worker, so it runs to completion before the sleep returns.
fn wait_for_worker_exit(alive: Arc<()>) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while Arc::strong_count(&alive) > 1 {
        assert!(Instant::now() < deadline, "worker never finished");
        thread::yield_now();
    }
    thread::sleep(Duration::from_millis(100));
}

#[test]
fn owner_panic_with_live_group_propagates() {
    let (done_tx, done_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let alive = Arc::new(());
    let worker_alive = Arc::clone(&alive);

    let owner = thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let mut builder = Builder::new(1);
            builder
                .worker(WorkerId::new(0), move |_| async move {
                    // Ignore shutdown on purpose. Only the test's release
                    // signal lets this worker return.
                    let _alive = worker_alive;
                    let _ = release_rx.recv();
                })
                .unwrap();
            let _group = builder.start().unwrap();
            panic!("owner failed an assertion");
        });
        let _ = done_tx.send(result.is_err());
    });

    let propagated = done_rx.recv_timeout(Duration::from_secs(5));
    // Release the worker before asserting, so a regression (the group's drop
    // blocked joining it) fails the test instead of also leaking a thread.
    release_tx.send(()).unwrap();
    owner.join().unwrap();
    wait_for_worker_exit(alive);
    assert_eq!(
        propagated,
        Ok(true),
        "owner panic did not propagate: RuntimeGroup::drop is blocked joining a worker"
    );
}

#[test]
fn owner_panic_still_requests_shutdown_of_detached_workers() {
    let (observed_tx, observed_rx) = mpsc::channel();
    let alive = Arc::new(());
    let worker_alive = Arc::clone(&alive);

    let owner = thread::spawn(move || {
        let mut builder = Builder::new(1);
        builder
            .worker(WorkerId::new(0), move |context| async move {
                let _alive = worker_alive;
                context.shutdown_requested().await;
                let _ = observed_tx.send(());
            })
            .unwrap();
        let _group = builder.start().unwrap();
        panic!("owner failed an assertion");
    });

    assert!(owner.join().is_err(), "the owner's panic propagates");
    let observed = observed_rx.recv_timeout(Duration::from_secs(5));
    wait_for_worker_exit(alive);
    observed.expect("detached worker never observed the shutdown request");
}
