//! Pass-4 reproducer: two panicking wakers in one timer wake batch.
//!
//! `WakeBatch` wakes the remaining wakers from its `Drop` while the first
//! panic unwinds, so a second panicking waker panics during unwinding and the
//! process aborts. norn-channel fixed the same pattern in #221.
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, SpinPark};
use norn_timer::{Clock, Driver};

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("test wake panic");
    }
}

fn two_panicking_wakers(shutdown: bool) {
    let clock = Clock::simulated();
    let mut driver = Driver::new(SpinPark, clock.clone());
    let handle = driver.handle();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut cx = Context::from_waker(&waker);
    let mut first = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    let mut second = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    assert!(first.as_mut().poll(&mut cx).is_pending());
    assert!(second.as_mut().poll(&mut cx).is_pending());

    clock.advance(Duration::from_millis(1));
    let result = catch_unwind(AssertUnwindSafe(|| {
        if shutdown {
            driver.shutdown();
        } else {
            driver.park(ParkMode::NoPark).unwrap();
        }
    }));
    assert!(result.is_err());
}

fn run_in_child(name: &str, shutdown: bool) {
    const CHILD: &str = "NORN_TIMER_DOUBLE_WAKE_PANIC_CHILD";
    if std::env::var_os(CHILD).is_some() {
        two_panicking_wakers(shutdown);
        return;
    }
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg(name)
        .env(CHILD, "1")
        .status()
        .unwrap();
    assert!(
        status.success(),
        "a second panicking timer waker aborted the process: {status}"
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a child process")]
fn expiration_with_two_panicking_wakers_does_not_abort() {
    run_in_child("expiration_with_two_panicking_wakers_does_not_abort", false);
}

#[test]
#[cfg_attr(miri, ignore = "spawns a child process")]
fn shutdown_with_two_panicking_wakers_does_not_abort() {
    run_in_child("shutdown_with_two_panicking_wakers_does_not_abort", true);
}
