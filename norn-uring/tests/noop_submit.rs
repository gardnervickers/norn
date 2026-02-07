#![cfg(target_os = "linux")]

use std::pin::pin;

use futures_core::Future;

mod util;

/// Races two noops against each other to test the cancellation
/// path.
#[test]
fn raced_submit() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let mut futs = vec![];
        for _ in 0..1000 {
            let fut = race(norn_uring::noop(), norn_uring::noop());
            futs.push(fut);
        }
        futures_util::future::join_all(futs).await;
        Ok(())
    })
}

/// Test that we can submit many operations (greater than the size of the
/// sq) and still get all completions.
#[test]
fn concurrent_submit() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let mut futs = vec![];
        for _ in 0..1000 {
            futs.push(norn_uring::noop());
        }
        futures_util::future::join_all(futs).await;
        Ok(())
    })
}

#[test]
fn shutdown_with_detached_noops() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        for _ in 0..2048 {
            norn_executor::spawn(async {
                let _ = norn_uring::noop().await;
            })
            .detach();
        }
        norn_uring::noop().await;
        Ok(())
    })
}

async fn race(f1: impl Future, f2: impl Future) {
    futures_util::future::select(pin!(f1), pin!(f2)).await;
}
