#[cfg(target_os = "linux")]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    use norn_executor::LocalExecutor;

    let path = std::env::temp_dir().join(format!("norn-kv-demo-{}.dat", std::process::id()));
    let builder = io_uring::IoUring::builder();
    let driver = norn_uring::Driver::new(builder, 64)?;
    let mut executor = LocalExecutor::new(driver);

    executor.block_on(async {
        run_demo(&path).await?;
        norn_uring::fs::remove_file(&path).await?;
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}

#[cfg(not(target_os = "linux"))]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::temp_dir().join(format!("norn-kv-demo-{}.dat", std::process::id()));
    block_on_ready(run_demo(&path))?;
    std::fs::remove_file(&path)?;
    Ok(())
}

async fn run_demo(path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    use norn_kv::{Store, StoreConfig};

    let config = StoreConfig {
        slot_count: 16,
        ..StoreConfig::default()
    };
    let (beta, gamma) = {
        let mut store = Store::open(path, config).await?;

        let alpha = store.put(b"alpha".to_vec()).await?;
        let beta = store.put(b"beta".to_vec()).await?;
        println!("put alpha -> {alpha:?}");
        println!("put beta  -> {beta:?}");
        println!("get alpha -> {:?}", store.get(alpha).await?);

        assert!(store.delete(alpha).await?);
        let gamma = store.put(b"gamma".to_vec()).await?;
        println!("delete alpha -> true");
        println!("put gamma -> {gamma:?}");
        println!("stale alpha -> {:?}", store.get(alpha).await?);
        println!("get gamma -> {:?}", store.get(gamma).await?);
        (beta, gamma)
    };

    let recovered = Store::open(path, config).await?;
    println!("recover beta  -> {:?}", recovered.get(beta).await?);
    println!("recover gamma -> {:?}", recovered.get(gamma).await?);

    Ok(())
}

#[cfg(not(target_os = "linux"))]
fn block_on_ready<F: std::future::Future>(future: F) -> F::Output {
    use std::pin::pin;
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    fn clone(_: *const ()) -> RawWaker {
        raw_waker()
    }
    fn wake(_: *const ()) {}
    fn raw_waker() -> RawWaker {
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, wake, wake, wake),
        )
    }

    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    match future.as_mut().poll(&mut cx) {
        Poll::Ready(output) => output,
        Poll::Pending => panic!("blocking norn-kv demo future unexpectedly yielded"),
    }
}
