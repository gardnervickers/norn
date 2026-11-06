//! Waker panics that must not abort: a panicking payload destructor, and a
//! driver dropped while the thread is already unwinding.
use std::future::Future;
use std::panic::{catch_unwind, panic_any, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, SpinPark};
use norn_timer::{Clock, Driver};

static WOKEN: AtomicUsize = AtomicUsize::new(0);
static ARMED: AtomicBool = AtomicBool::new(true);

struct DropPanics;
impl Drop for DropPanics {
    fn drop(&mut self) {
        if ARMED.swap(false, Ordering::SeqCst) {
            panic!("payload drop panic");
        }
    }
}

struct PayloadPanicWake;
impl Wake for PayloadPanicWake {
    fn wake(self: Arc<Self>) {
        WOKEN.fetch_add(1, Ordering::SeqCst);
        panic_any(DropPanics);
    }
}

struct PanicWake;
impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("test wake panic");
    }
}

fn payload_drop_case() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(SpinPark, clock.clone());
    let handle = driver.handle();
    let waker = Waker::from(Arc::new(PayloadPanicWake));
    let mut cx = Context::from_waker(&waker);
    let mut a = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    let mut b = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    let mut c = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    assert!(a.as_mut().poll(&mut cx).is_pending());
    assert!(b.as_mut().poll(&mut cx).is_pending());
    assert!(c.as_mut().poll(&mut cx).is_pending());
    clock.advance(Duration::from_millis(1));
    let r = catch_unwind(AssertUnwindSafe(|| driver.park(ParkMode::NoPark).unwrap()));
    assert!(r.is_err());
    assert_eq!(
        WOKEN.load(Ordering::SeqCst),
        3,
        "a completed timer was never woken"
    );
}

fn drop_during_unwind_case() {
    let clock = Clock::simulated();
    let driver = Driver::new(SpinPark, clock.clone());
    let handle = driver.handle();
    let waker = Waker::from(Arc::new(PanicWake));
    let mut cx = Context::from_waker(&waker);
    let mut a = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    assert!(a.as_mut().poll(&mut cx).is_pending());
    let r = catch_unwind(AssertUnwindSafe(move || {
        let _driver = driver;
        panic!("unrelated panic while driver is live");
    }));
    assert!(r.is_err());
}

fn run_in_child(name: &str, f: fn()) {
    const CHILD: &str = "NORN_REVIEW255_CHILD";
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
fn second_payload_with_panicking_drop_still_wakes_rest() {
    run_in_child(
        "second_payload_with_panicking_drop_still_wakes_rest",
        payload_drop_case,
    );
}

#[test]
#[cfg_attr(miri, ignore = "spawns a child process")]
fn driver_dropped_during_unwind_with_panicking_waker_does_not_abort() {
    run_in_child(
        "driver_dropped_during_unwind_with_panicking_waker_does_not_abort",
        drop_during_unwind_case,
    );
}
