//! Pass-4 reproducer: `RuntimeGroup::join` called on one of the group's own
//! workers requests shutdown of the calling worker when the consumed group
//! is dropped, although `join` only waits for workers to finish naturally.

use std::sync::mpsc;

use norn_multicore::{RuntimeGroup, WorkerId};

#[test]
fn join_on_a_worker_does_not_request_the_callers_shutdown() {
    let mut builder = RuntimeGroup::builder(2);
    let (group_tx, group_rx) = mpsc::channel::<RuntimeGroup>();
    let (result_tx, result_rx) = mpsc::channel();

    builder
        .worker(WorkerId::new(0), move |context| async move {
            assert!(!context.is_shutdown_requested());
            group_rx.recv().unwrap().join().unwrap();
            // Worker 1 finished on its own; nobody asked for shutdown.
            result_tx.send(context.is_shutdown_requested()).unwrap();
        })
        .unwrap();
    builder.worker(WorkerId::new(1), |_| async {}).unwrap();

    group_tx.send(builder.start().unwrap()).unwrap();
    let requested = result_rx.recv().expect("worker 0 panicked");
    assert!(
        !requested,
        "join() on a worker requested shutdown of the calling worker"
    );
}
