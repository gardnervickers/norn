//! Reentrant waker regressions for `Sleep::poll`.
use std::cell::RefCell;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_timer::{Clock, Driver};

thread_local! {
    static SHUTDOWN_DRIVER: RefCell<Option<Driver<RecordPark>>> = const { RefCell::new(None) };
}

struct ShutdownTimerOnDrop;
struct CountWake(std::sync::atomic::AtomicUsize);
impl std::task::Wake for CountWake {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
// The destructor is the callback exercised by this regression.
#[allow(clippy::manual_noop_waker)]
impl std::task::Wake for ShutdownTimerOnDrop {
    fn wake(self: std::sync::Arc<Self>) {}
}
impl Drop for ShutdownTimerOnDrop {
    fn drop(&mut self) {
        SHUTDOWN_DRIVER.with(|slot| {
            let driver = slot.borrow_mut().take();
            drop(driver);
        });
    }
}

#[test]
fn replacing_sleep_waker_allows_old_waker_destructor_to_drop_driver() {
    let driver = Driver::new(RecordPark(Rc::default()), Clock::simulated());
    let mut sleep = std::pin::pin!(driver.handle().sleep(Duration::from_secs(1)));
    SHUTDOWN_DRIVER.with(|slot| *slot.borrow_mut() = Some(driver));
    let old_waker = Waker::from(std::sync::Arc::new(ShutdownTimerOnDrop));
    assert!(sleep
        .as_mut()
        .poll(&mut Context::from_waker(&old_waker))
        .is_pending());
    drop(old_waker);
    let notified = std::sync::Arc::new(CountWake(std::sync::atomic::AtomicUsize::new(0)));
    let new_waker = Waker::from(notified.clone());
    let result = sleep.as_mut().poll(&mut Context::from_waker(&new_waker));
    assert!(
        result == Poll::Ready(Err(norn_timer::Error::Shutdown))
            || notified.0.load(std::sync::atomic::Ordering::Relaxed) > 0
    );
}

#[derive(Clone, Debug)]
struct NoopUnpark;
impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

struct RecordPark(Rc<RefCell<Vec<ParkMode>>>);
impl Park for RecordPark {
    type Guard = ();
    type Unparker = NoopUnpark;
    fn enter(&self) {}
    fn unparker(&self) -> NoopUnpark {
        NoopUnpark
    }
    fn needs_park(&self) -> bool {
        false
    }
    fn shutdown(&mut self) {}
    fn park(&mut self, mode: ParkMode) -> std::io::Result<()> {
        self.0.borrow_mut().push(mode);
        Ok(())
    }
}

#[test]
fn replacing_sleep_waker_allows_clone_callback_to_drop_driver() {
    use std::task::{RawWaker, RawWakerVTable};

    unsafe fn clone(_: *const ()) -> RawWaker {
        SHUTDOWN_DRIVER.with(|slot| {
            let driver = slot.borrow_mut().take();
            drop(driver);
        });
        raw_waker()
    }
    fn raw_waker() -> RawWaker {
        // No owned data; clones and drops need no reference counting.
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, |_| {}, |_| {}, |_| {}),
        )
    }

    let driver = Driver::new(RecordPark(Rc::default()), Clock::simulated());
    let mut sleep = std::pin::pin!(driver.handle().sleep(Duration::from_secs(1)));
    let mut cx = Context::from_waker(Waker::noop());
    assert!(sleep.as_mut().poll(&mut cx).is_pending());
    SHUTDOWN_DRIVER.with(|slot| *slot.borrow_mut() = Some(driver));
    // Safety: the vtable holds no data, and all operations are valid on null.
    let waker = unsafe { Waker::from_raw(raw_waker()) };
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(&waker)),
        Poll::Ready(Err(norn_timer::Error::Shutdown))
    );
}
