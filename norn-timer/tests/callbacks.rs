use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, SpinPark, Unpark};
use norn_timer::{Clock, Driver, Error, Sleep};

thread_local! {
    static OTHER_SLEEP: RefCell<Option<Pin<Box<Sleep>>>> = const { RefCell::new(None) };
}

struct DropOtherSleep;

impl Wake for DropOtherSleep {
    fn wake(self: Arc<Self>) {
        OTHER_SLEEP.with(|sleep| drop(sleep.borrow_mut().take()));
    }
}

fn drop_another_timer(shutdown: bool, other_deadline: Duration) {
    let clock = Clock::simulated();
    let mut driver = Driver::new(SpinPark, clock.clone());
    let handle = driver.handle();
    let mut other = Box::pin(handle.sleep(other_deadline));
    assert!(other
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    OTHER_SLEEP.with(|sleep| *sleep.borrow_mut() = Some(other));

    let mut firing = std::pin::pin!(handle.sleep(Duration::from_millis(1000)));
    let waker = Waker::from(Arc::new(DropOtherSleep));
    let mut cx = Context::from_waker(&waker);
    assert!(firing.as_mut().poll(&mut cx).is_pending());
    if shutdown {
        drop(driver);
        assert_eq!(
            firing.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Shutdown))
        );
    } else {
        clock.advance(Duration::from_millis(1000));
        driver.park(ParkMode::NoPark).unwrap();
        assert_eq!(firing.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }
    OTHER_SLEEP.with(|sleep| assert!(sleep.borrow().is_none()));
}

#[test]
fn expiration_callback_can_drop_another_due_timer() {
    drop_another_timer(false, Duration::from_millis(1000));
}

#[test]
fn expiration_callback_can_drop_a_timer_reinserted_from_the_same_slot() {
    drop_another_timer(false, Duration::from_millis(1001));
}

#[test]
fn shutdown_callback_can_drop_another_timer() {
    drop_another_timer(true, Duration::from_millis(1000));
}

struct PanicWake;

impl Wake for PanicWake {
    fn wake(self: Arc<Self>) {
        panic!("test wake panic");
    }
}

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn panic_preserves_other_wakes(shutdown: bool) {
    let clock = Clock::simulated();
    let mut driver = Driver::new(SpinPark, clock.clone());
    let handle = driver.handle();
    let mut other = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    let count = Arc::new(CountWake(AtomicUsize::new(0)));
    let count_waker = Waker::from(count.clone());
    assert!(other
        .as_mut()
        .poll(&mut Context::from_waker(&count_waker))
        .is_pending());
    let mut first = std::pin::pin!(handle.sleep(Duration::from_millis(1)));
    let panic_waker = Waker::from(Arc::new(PanicWake));
    assert!(first
        .as_mut()
        .poll(&mut Context::from_waker(&panic_waker))
        .is_pending());

    clock.advance(Duration::from_millis(1));
    assert!(catch_unwind(AssertUnwindSafe(|| {
        if shutdown {
            driver.shutdown();
        } else {
            driver.park(ParkMode::NoPark).unwrap();
        }
    }))
    .is_err());
    assert_eq!(count.0.load(Ordering::Relaxed), 1);
    let expected = if shutdown {
        Err(Error::Shutdown)
    } else {
        Ok(())
    };
    assert_eq!(
        other.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(expected)
    );
    driver.shutdown();
}

#[test]
fn expiration_wake_panic_still_notifies_other_completed_timers() {
    panic_preserves_other_wakes(false);
}

#[test]
fn shutdown_wake_panic_still_notifies_other_completed_timers() {
    panic_preserves_other_wakes(true);
}

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

/// An inner park that records whether it was shut down.
#[derive(Debug)]
struct RecordShutdown(Rc<Cell<bool>>);

impl Park for RecordShutdown {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, _: ParkMode) -> io::Result<()> {
        Ok(())
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnpark
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {
        self.0.set(true);
    }
}

/// A panicking sleeper waker must not skip the inner park's shutdown.
#[test]
fn shutdown_wake_panic_still_shuts_down_the_inner_park() {
    let shut_down = Rc::new(Cell::new(false));
    let mut driver = Driver::new(RecordShutdown(Rc::clone(&shut_down)), Clock::simulated());
    let mut sleep = std::pin::pin!(driver.handle().sleep(Duration::from_millis(1)));
    let panic_waker = Waker::from(Arc::new(PanicWake));
    assert!(sleep
        .as_mut()
        .poll(&mut Context::from_waker(&panic_waker))
        .is_pending());

    assert!(catch_unwind(AssertUnwindSafe(|| driver.shutdown())).is_err());
    assert!(
        shut_down.get(),
        "timer Park::shutdown skipped inner.shutdown after a sleep waker panicked"
    );
}
