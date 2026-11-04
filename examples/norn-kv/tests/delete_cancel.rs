//! Cancelling `Store::delete` cannot let its tombstone land over a later `put`.
#![cfg(target_os = "linux")]

use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::task::Context;
use std::time::{Duration, Instant};

use norn_kv::{Error, Key, Store, StoreConfig};

fn one_slot() -> StoreConfig {
    StoreConfig {
        slot_count: 1,
        ..StoreConfig::default()
    }
}

fn with_store_dir(name: &str, f: impl FnOnce(&Path)) {
    let dir = std::env::temp_dir().join(format!("norn-kv-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    f(&dir.join("kv.dat"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Poll `future` once, as a caller-side timeout or select would before
/// dropping it, and check that it is waiting on submitted I/O.
fn poll_once_and_drop<F: Future>(future: F) {
    let mut future = pin!(future);
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    assert!(future.as_mut().poll(&mut cx).is_pending());
}

fn is_full(res: &norn_kv::Result<Key>) -> bool {
    matches!(
        res,
        Err(Error::Io {
            op: "allocate slot",
            ..
        })
    )
}

/// Retry `put` until a cancelled write's slot is released.
async fn put_when_released(store: &mut Store, value: &[u8]) -> Key {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        norn_uring::Handle::current()
            .timeout(Duration::from_millis(1))
            .await
            .unwrap();
        let res = store.put(value.to_vec()).await;
        if is_full(&res) && Instant::now() < deadline {
            continue;
        }
        return res.expect("the only slot leaked after a cancelled write");
    }
}

#[test]
fn cancelled_delete_keeps_its_slot_until_the_tombstone_finishes() {
    with_store_dir("delete-cancel", |path| {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        let key = executor.block_on(async {
            let mut store = Store::open(path, one_slot()).await.unwrap();
            let old = store.put(b"old".to_vec()).await.unwrap();
            poll_once_and_drop(store.delete(old));

            // The cancelled delete still counts: the key is gone, and a retry
            // must not free the slot while the first tombstone is in flight.
            assert_eq!(store.get(old).await.unwrap(), None);
            assert!(!store.delete(old).await.unwrap());

            // The driver has not parked since the drop, so it cannot have
            // reaped the tombstone's terminal completion yet.
            let early = store.put(b"too early".to_vec()).await;
            assert!(
                is_full(&early),
                "slot was reused before the cancelled tombstone finished: {early:?}"
            );

            let new = put_when_released(&mut store, b"new").await;
            assert_ne!(old, new);
            assert_eq!(store.get(new).await.unwrap(), Some(b"new".to_vec()));
            new
        });
        drop(executor);

        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        executor.block_on(async {
            let store = Store::open(path, one_slot()).await.unwrap();
            assert_eq!(store.get(key).await.unwrap(), Some(b"new".to_vec()));
        });
    });
}

#[test]
fn predicted_key_of_a_cancelled_put_is_not_live() {
    with_store_dir("put-cancel-predicted", |path| {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        executor.block_on(async {
            let mut store = Store::open(path, one_slot()).await.unwrap();
            let first = store.put(b"first".to_vec()).await.unwrap();
            assert!(store.delete(first).await.unwrap());
            poll_once_and_drop(store.put(b"cancelled".to_vec()));

            // With one slot the key is just the generation, so the cancelled
            // put's key is the next one. It must not be readable or deletable
            // while its write is in flight: a delete would free the slot early.
            let predicted = Key::from_raw(first.into_raw() + 1);
            assert_eq!(store.get(predicted).await.unwrap(), None);
            assert!(!store.delete(predicted).await.unwrap());

            let next = put_when_released(&mut store, b"next").await;
            assert_eq!(store.get(next).await.unwrap(), Some(b"next".to_vec()));
        });
    });
}
