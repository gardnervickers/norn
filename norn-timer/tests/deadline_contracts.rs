use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_timer::{Clock, Driver, Error, Sleep};

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

#[derive(Debug)]
struct RecordPark(Rc<RefCell<Vec<ParkMode>>>);

impl Park for RecordPark {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> std::io::Result<()> {
        self.0.borrow_mut().push(mode);
        Ok(())
    }

    fn enter(&self) {}

    fn unparker(&self) -> Self::Unparker {
        NoopUnpark
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}

fn poll_pending(sleep: Pin<&mut Sleep>) {
    assert!(sleep
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
}

fn assert_sleeps_due_at_construction_complete(elapsed: Duration) {
    let clock = Clock::simulated();
    let driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();
    clock.advance(elapsed);

    let mut relative = Box::pin(handle.sleep(Duration::ZERO));
    assert_eq!(
        relative
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );

    let mut absolute = Box::pin(handle.sleep_until(clock.now()));
    assert_eq!(
        absolute
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn sleeps_due_at_construction_complete_with_a_stale_wheel() {
    assert_sleeps_due_at_construction_complete(Duration::from_millis(1));
    assert_sleeps_due_at_construction_complete(Duration::from_micros(500));
}

#[test]
fn relative_sleep_starts_at_construction_not_first_poll() {
    let clock = Clock::simulated();
    let modes = Rc::new(RefCell::new(Vec::new()));
    let mut driver = Driver::new(RecordPark(Rc::clone(&modes)), clock.clone());
    let handle = driver.handle();
    let mut sleep = Box::pin(handle.sleep(Duration::from_millis(100)));

    clock.advance(Duration::from_millis(50));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(50));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn relative_sleep_uses_clock_when_wheel_elapsed_is_stale() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();

    clock.advance(Duration::from_millis(500));
    let mut sleep = Box::pin(handle.sleep(Duration::from_millis(50)));
    poll_pending(sleep.as_mut());
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(50));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn fractional_relative_deadlines_round_up_without_expiring_early() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();

    clock.advance(Duration::from_micros(500));
    let mut sleep = Box::pin(handle.sleep(Duration::from_millis(1)));
    poll_pending(sleep.as_mut());
    clock.advance(Duration::from_micros(999));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());
    clock.advance(Duration::from_micros(401));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());
    clock.advance(Duration::from_micros(100));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );

    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let mut sleep = Box::pin(driver.handle().sleep(Duration::from_nanos(1)));
    poll_pending(sleep.as_mut());
    clock.advance(Duration::from_nanos(999_999));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());
    clock.advance(Duration::from_nanos(1));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn relative_reset_restarts_duration_from_reset() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();
    let mut sleep = Box::pin(handle.sleep(Duration::from_millis(100)));
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(50));
    sleep.as_mut().reset();
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(99));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(1));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn absolute_reset_retains_original_deadline() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();
    let deadline = handle.clock().now() + Duration::from_millis(100);
    let mut sleep = Box::pin(handle.sleep_until(deadline));

    clock.advance(Duration::from_millis(50));
    sleep.as_mut().reset();
    poll_pending(sleep.as_mut());
    driver.park(ParkMode::NoPark).unwrap();

    clock.advance(Duration::from_millis(50));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn dropping_a_registered_reset_sleep_does_not_affect_other_timers() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let handle = driver.handle();
    let mut reset = Box::pin(handle.sleep(Duration::from_millis(100)));
    let mut other = Box::pin(handle.sleep(Duration::from_millis(100)));
    poll_pending(reset.as_mut());
    poll_pending(other.as_mut());

    reset.as_mut().reset();
    drop(reset);
    clock.advance(Duration::from_millis(100));
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        other.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn no_park_is_never_replaced_by_a_future_timer_deadline() {
    let clock = Clock::simulated();
    let modes = Rc::new(RefCell::new(Vec::new()));
    let mut driver = Driver::new(RecordPark(Rc::clone(&modes)), clock);
    let mut sleep = Box::pin(driver.handle().sleep(Duration::from_secs(1)));
    poll_pending(sleep.as_mut());

    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(modes.borrow().last(), Some(&ParkMode::NoPark));
}

#[test]
fn firing_a_timer_does_not_park_for_a_later_timer() {
    let clock = Clock::simulated();
    let modes = Rc::new(RefCell::new(Vec::new()));
    let mut driver = Driver::new(RecordPark(Rc::clone(&modes)), clock.clone());
    let handle = driver.handle();
    let mut early = Box::pin(handle.sleep(Duration::from_millis(1)));
    let mut late = Box::pin(handle.sleep(Duration::from_secs(1)));
    poll_pending(early.as_mut());
    poll_pending(late.as_mut());

    clock.advance(Duration::from_millis(1));
    driver.park(ParkMode::NextCompletion).unwrap();
    assert_eq!(modes.borrow().last(), Some(&ParkMode::NoPark));
}

#[test]
fn shutdown_wins_over_new_zero_duration_sleep() {
    let driver = Driver::new((), Clock::simulated());
    let handle = driver.handle();
    let deadline = handle.clock().now();
    drop(driver);

    let mut sleep = Box::pin(handle.sleep(Duration::ZERO));
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(Error::Shutdown))
    );

    let mut sleep = Box::pin(handle.sleep_until(deadline));
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Err(Error::Shutdown))
    );
}

#[test]
fn duration_beyond_u64_milliseconds_does_not_expire_early() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let duration = Duration::from_secs((u64::MAX / 1_000) + 1);
    let mut sleep = Box::pin(driver.handle().sleep(duration));
    poll_pending(sleep.as_mut());

    clock.advance(Duration::from_millis(385));
    driver.park(ParkMode::NoPark).unwrap();
    poll_pending(sleep.as_mut());
}

#[test]
fn maximum_duration_uses_ticks_without_instant_overflow() {
    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    let mut sleep = Box::pin(driver.handle().sleep(Duration::MAX));
    poll_pending(sleep.as_mut());

    clock.advance(Duration::MAX);
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        sleep.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
}

#[test]
fn saturated_deadline_does_not_block_an_earlier_top_level_timer() {
    const TOP_LEVEL_WRAP: u64 = 1 << 36;

    let clock = Clock::simulated();
    let mut driver = Driver::new(RecordPark(Rc::new(RefCell::new(Vec::new()))), clock.clone());
    clock.advance(Duration::from_millis(TOP_LEVEL_WRAP - 500));
    driver.park(ParkMode::NoPark).unwrap();
    let handle = driver.handle();
    let mut saturated = Box::pin(handle.sleep(Duration::MAX));
    let nearer_duration = Duration::from_millis((1 << 30) + 1_000);
    let mut nearer = Box::pin(handle.sleep(nearer_duration));
    poll_pending(saturated.as_mut());
    poll_pending(nearer.as_mut());

    clock.advance(nearer_duration);
    driver.park(ParkMode::NoPark).unwrap();
    assert_eq!(
        nearer
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop())),
        Poll::Ready(Ok(()))
    );
    poll_pending(saturated.as_mut());
}
