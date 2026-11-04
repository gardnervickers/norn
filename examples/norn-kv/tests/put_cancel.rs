//! Cancelling `Store::put` returns its slot once the write can no longer land.
#![cfg(target_os = "linux")]

use std::future::Future;
use std::pin::pin;
use std::task::Context;
use std::time::{Duration, Instant};

use norn_kv::{Error, Store, StoreConfig};

#[test]
fn cancelled_put_releases_its_slot_after_the_write_finishes() {
    let dir = std::env::temp_dir().join(format!("norn-kv-put-cancel-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("kv.dat");
    let config = StoreConfig {
        slot_count: 1,
        ..StoreConfig::default()
    };

    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let mut store = Store::open(&path, config).await.unwrap();
        {
            // A caller-side timeout or select drops `put` after its first
            // poll: the slot is allocated and the write is submitted.
            let mut put = pin!(store.put(b"cancelled".to_vec()));
            let waker = futures::task::noop_waker();
            let mut cx = Context::from_waker(&waker);
            assert!(put.as_mut().poll(&mut cx).is_pending());
        }

        // The driver has not been parked since the drop, so it cannot have
        // reaped the write's terminal completion. The slot must still be
        // reserved: reusing it now could let the old write land over a new one.
        let early = store.put(b"too early".to_vec()).await;
        assert!(
            matches!(
                &early,
                Err(Error::Io {
                    op: "allocate slot",
                    ..
                })
            ),
            "slot was reused before the cancelled write finished: {early:?}"
        );

        // Once the driver observes the write's final completion, the slot is
        // returned to the free list.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            norn_uring::Handle::current()
                .timeout(Duration::from_millis(1))
                .await
                .unwrap();
            match store.put(b"next".to_vec()).await {
                Ok(key) => {
                    assert_eq!(store.get(key).await.unwrap(), Some(b"next".to_vec()));
                    break;
                }
                Err(Error::Io {
                    op: "allocate slot",
                    ..
                }) if Instant::now() < deadline => {}
                Err(err) => panic!("the only slot leaked after a cancelled put: {err:?}"),
            }
        }
    });
    let _ = std::fs::remove_dir_all(&dir);
}
