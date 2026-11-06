//! Dropping a channel driver while the thread is already unwinding runs its
//! shutdown wakeups. A panicking receiver waker must not abort the process.

use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_channel::{mpsc, Driver};
use norn_executor::park::SpinPark;

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("test wake panic");
    }
}

fn drop_during_unwind_case() {
    let driver = Driver::new(SpinPark);
    let handle = driver.handle();
    let (_sender, receiver) = mpsc::bounded::<u32>(&driver.endpoint(), 1);
    let mut receiver = receiver.attach(&handle);
    let waker = Waker::from(Arc::new(PanicWake));
    let mut cx = Context::from_waker(&waker);
    let mut recv = std::pin::pin!(receiver.recv());
    assert!(recv.as_mut().poll(&mut cx).is_pending());
    let r = catch_unwind(AssertUnwindSafe(move || {
        let _driver = driver;
        panic!("unrelated panic while driver is live");
    }));
    assert!(r.is_err());
}

/// Run `f` in a child process so an abort fails the test instead of the run.
fn run_in_child(name: &str, f: fn()) {
    const CHILD: &str = "NORN_CHANNEL_DROP_UNWIND_CHILD";
    if std::env::var_os(CHILD).is_some() {
        f();
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(status.success(), "child failed: {status}");
}

#[test]
#[cfg_attr(miri, ignore = "spawns a child process")]
fn driver_dropped_during_unwind_with_panicking_waker_does_not_abort() {
    run_in_child(
        "driver_dropped_during_unwind_with_panicking_waker_does_not_abort",
        drop_during_unwind_case,
    );
}
