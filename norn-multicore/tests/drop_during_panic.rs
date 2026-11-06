//! Dropping a `RuntimeGroup` while its owner thread is panicking must not
//! block joining workers: a worker that ignores shutdown would otherwise turn
//! the panic into a hang.

use std::sync::mpsc;
use std::time::Duration;

use norn_multicore::{Builder, WorkerId};

#[test]
fn owner_panic_with_live_group_propagates() {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let result = std::panic::catch_unwind(|| {
            let mut builder = Builder::new(1);
            builder
                .worker(WorkerId::new(0), |_| std::future::pending::<()>())
                .unwrap();
            let _group = builder.start().unwrap();
            panic!("owner failed an assertion");
        });
        let _ = done_tx.send(result.is_err());
    });
    assert_eq!(
        done_rx.recv_timeout(Duration::from_secs(5)),
        Ok(true),
        "owner panic did not propagate: RuntimeGroup::drop is blocked joining a worker"
    );
}

#[test]
fn owner_panic_still_requests_shutdown_of_detached_workers() {
    let (observed_tx, observed_rx) = mpsc::channel();
    let owner = std::thread::spawn(move || {
        let mut builder = Builder::new(1);
        builder
            .worker(WorkerId::new(0), move |context| async move {
                context.shutdown_requested().await;
                let _ = observed_tx.send(());
            })
            .unwrap();
        let _group = builder.start().unwrap();
        panic!("owner failed an assertion");
    });
    assert!(owner.join().is_err(), "the owner's panic propagates");
    observed_rx
        .recv_timeout(Duration::from_secs(5))
        .expect("detached worker never observed the shutdown request");
}
