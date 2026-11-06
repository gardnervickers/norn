//! Dropping a `RuntimeGroup` while its owner thread is panicking must not
//! block joining workers: a worker that ignores shutdown would otherwise turn
//! the panic into a hang.
//!
//! The group detaches its workers in that case, so the tests release every
//! worker and wait for it to finish before returning. Miri rejects a test
//! that leaves a thread running when the test process ends.

use std::cell::RefCell;
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

use norn_multicore::{Builder, WorkerId};

thread_local! {
    static ON_EXIT: RefCell<Option<NotifyOnDrop>> = const { RefCell::new(None) };
}

/// Sends when dropped. Stored in a thread-local, so that is when the thread
/// exits.
struct NotifyOnDrop(mpsc::Sender<()>);

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        let _ = self.0.send(());
    }
}

/// Make the current thread send on `exited` as it exits. Call it on a worker.
fn notify_on_thread_exit(exited: mpsc::Sender<()>) {
    ON_EXIT.with(|slot| *slot.borrow_mut() = Some(NotifyOnDrop(exited)));
}

/// Wait until the worker thread that called [`notify_on_thread_exit`] is
/// exiting.
///
/// A detached worker has no join handle to wait on. Thread-local destructors
/// are the last user code a thread runs, so the signal arrives as the worker
/// finishes. The yields let the thread's remaining teardown run before the
/// test returns, because Miri rejects a test that leaves a thread running.
fn wait_for_worker_exit(exited: &mpsc::Receiver<()>) {
    exited
        .recv_timeout(Duration::from_secs(10))
        .expect("worker never exited");
    for _ in 0..16 {
        thread::yield_now();
    }
}

#[test]
fn owner_panic_with_live_group_propagates() {
    let (done_tx, done_rx) = mpsc::channel();
    let (release_tx, release_rx) = mpsc::channel::<()>();
    let (exited_tx, exited_rx) = mpsc::channel();

    let owner = thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let mut builder = Builder::new(1);
            builder
                .worker(WorkerId::new(0), move |_| async move {
                    // Ignore shutdown on purpose. Only the test's release
                    // signal lets this worker return.
                    notify_on_thread_exit(exited_tx);
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
    wait_for_worker_exit(&exited_rx);
    assert_eq!(
        propagated,
        Ok(true),
        "owner panic did not propagate: RuntimeGroup::drop is blocked joining a worker"
    );
}

#[test]
fn owner_panic_still_requests_shutdown_of_detached_workers() {
    let (observed_tx, observed_rx) = mpsc::channel();
    let (exited_tx, exited_rx) = mpsc::channel();

    let owner = thread::spawn(move || {
        let mut builder = Builder::new(1);
        builder
            .worker(WorkerId::new(0), move |context| async move {
                notify_on_thread_exit(exited_tx);
                context.shutdown_requested().await;
                let _ = observed_tx.send(());
            })
            .unwrap();
        let _group = builder.start().unwrap();
        panic!("owner failed an assertion");
    });

    assert!(owner.join().is_err(), "the owner's panic propagates");
    let observed = observed_rx.recv_timeout(Duration::from_secs(5));
    wait_for_worker_exit(&exited_rx);
    observed.expect("detached worker never observed the shutdown request");
}
