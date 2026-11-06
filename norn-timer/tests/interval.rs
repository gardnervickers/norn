use std::cell::Cell;
use std::future::Future;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::LocalExecutor;
use norn_timer::{Clock, Driver, Error, Handle, Interval, MissedTickBehavior};

#[derive(Clone, Copy, Debug)]
struct NoopUnpark;

impl Unpark for NoopUnpark {
    fn unpark(&self) {}
}

/// Inner park that never blocks; tests advance the simulated clock by hand.
#[derive(Debug)]
struct NoopPark;

impl Park for NoopPark {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, _mode: ParkMode) -> std::io::Result<()> {
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

struct Harness {
    driver: Driver<NoopPark>,
    clock: Clock,
    handle: Handle,
}

impl Harness {
    fn new() -> Self {
        let clock = Clock::simulated();
        let driver = Driver::new(NoopPark, clock.clone());
        let handle = driver.handle();
        Self {
            driver,
            clock,
            handle,
        }
    }

    /// Advance simulated time and let the driver fire due timers.
    fn advance(&mut self, millis: u64) {
        self.clock.advance(Duration::from_millis(millis));
        self.driver.park(ParkMode::NoPark).unwrap();
    }
}

fn ms(millis: u64) -> Duration {
    Duration::from_millis(millis)
}

fn poll(interval: &mut Interval) -> Poll<Result<(), Error>> {
    interval.poll_tick(&mut Context::from_waker(Waker::noop()))
}

/// Count the ticks that are ready without advancing time.
fn ready_ticks(interval: &mut Interval) -> usize {
    let mut ticks = 0;
    while let Poll::Ready(result) = poll(interval) {
        result.unwrap();
        ticks += 1;
        assert!(ticks < 1_000, "interval never went pending");
    }
    ticks
}

#[test]
fn first_tick_is_immediate_and_later_ticks_follow_the_period() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(interval.period(), ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);

    h.advance(9);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(10);
    assert_eq!(ready_ticks(&mut interval), 1);
}

#[test]
fn immediate_ticks_do_not_wait_for_the_next_millisecond() {
    let h = Harness::new();
    h.clock.advance(Duration::from_micros(500));
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1, "first tick at 0.5 ms");

    h.clock.advance(Duration::from_micros(2_900));
    interval.reset_immediately();
    assert_eq!(ready_ticks(&mut interval), 1, "reset_immediately at 3.4 ms");

    let mut at_now = h.handle.interval_at(h.clock.now(), ms(10));
    assert_eq!(ready_ticks(&mut at_now), 1, "interval_at(now) at 3.4 ms");
}

#[test]
fn system_clock_first_tick_is_immediate() {
    let driver = Driver::new(NoopPark, Clock::system());
    let mut interval = driver.handle().interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
}

#[test]
fn fractional_periods_do_not_drift() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(Duration::from_micros(1_500));
    let mut ticks = ready_ticks(&mut interval);
    for _ in 0..300 {
        h.advance(1);
        ticks += ready_ticks(&mut interval);
    }
    // Ticks are due at every multiple of 1.5 ms in [0, 300].
    assert_eq!(ticks, 201);
}

#[test]
fn skip_is_the_default() {
    assert_eq!(MissedTickBehavior::default(), MissedTickBehavior::Skip);
    let h = Harness::new();
    let interval = h.handle.interval(ms(10));
    assert_eq!(interval.missed_tick_behavior(), MissedTickBehavior::Skip);
}

/// Consume the tick at 0 ms, then deliver the tick due at 10 ms at 35 ms.
fn deliver_late(behavior: MissedTickBehavior) -> (Harness, Interval) {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    interval.set_missed_tick_behavior(behavior);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(35);
    (h, interval)
}

#[test]
fn burst_delivers_missed_ticks_then_resumes_the_schedule() {
    let (mut h, mut interval) = deliver_late(MissedTickBehavior::Burst);
    // The ticks due at 10, 20 and 30 ms.
    assert_eq!(ready_ticks(&mut interval), 3);
    h.advance(4);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 40 ms");
}

#[test]
fn delay_restarts_the_schedule_from_the_late_tick() {
    let (mut h, mut interval) = deliver_late(MissedTickBehavior::Delay);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(9);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 45 ms");
    h.advance(10);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 55 ms");
}

#[test]
fn skip_drops_missed_ticks_and_keeps_alignment() {
    let (mut h, mut interval) = deliver_late(MissedTickBehavior::Skip);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(4);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 40 ms");
    h.advance(10);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 50 ms");
}

#[test]
fn skip_on_an_exact_deadline_waits_for_the_following_tick() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
    // Deliver the 10 ms tick exactly when the 20 ms tick is due: that tick is
    // missed, so the next one is due at 30 ms.
    h.advance(20);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(9);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1);
}

#[test]
fn lateness_under_one_period_keeps_the_schedule_for_every_behavior() {
    for behavior in [
        MissedTickBehavior::Burst,
        MissedTickBehavior::Delay,
        MissedTickBehavior::Skip,
    ] {
        let mut h = Harness::new();
        let mut interval = h.handle.interval(ms(10));
        interval.set_missed_tick_behavior(behavior);
        assert_eq!(ready_ticks(&mut interval), 1);
        h.advance(15);
        assert_eq!(ready_ticks(&mut interval), 1, "{behavior:?}");
        h.advance(4);
        assert_eq!(ready_ticks(&mut interval), 0, "{behavior:?}");
        h.advance(1);
        assert_eq!(ready_ticks(&mut interval), 1, "{behavior:?}: tick at 20 ms");
    }
}

#[test]
fn interval_at_starts_at_a_future_instant() {
    let mut h = Harness::new();
    let start = h.clock.now() + ms(25);
    let mut interval = h.handle.interval_at(start, ms(10));
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(24);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(10);
    assert_eq!(ready_ticks(&mut interval), 1);
}

#[test]
fn interval_at_a_past_instant_treats_the_start_as_late() {
    let mut h = Harness::new();
    h.advance(100);
    let start = h.clock.now() - ms(35);
    let mut interval = h.handle.interval_at(start, ms(10));
    interval.set_missed_tick_behavior(MissedTickBehavior::Burst);
    // The ticks due at 65, 75, 85 and 95 ms.
    assert_eq!(ready_ticks(&mut interval), 4);
    h.advance(5);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 105 ms");
}

#[test]
fn reset_restarts_one_period_from_now() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(7);
    interval.reset();
    h.advance(9);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 17 ms");
}

#[test]
fn reset_immediately_ticks_now_and_realigns() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(3);
    interval.reset_immediately();
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(9);
    assert_eq!(ready_ticks(&mut interval), 0);
    h.advance(1);
    assert_eq!(ready_ticks(&mut interval), 1, "tick due at 13 ms");
}

#[test]
fn dropping_a_pending_tick_does_not_consume_it() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
    {
        let mut tick = std::pin::pin!(interval.tick());
        let cx = &mut Context::from_waker(Waker::noop());
        assert!(tick.as_mut().poll(cx).is_pending());
    }
    h.advance(10);
    assert_eq!(ready_ticks(&mut interval), 1);
}

#[test]
fn shutdown_fails_the_pending_tick_and_every_later_one() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(ms(10));
    assert_eq!(ready_ticks(&mut interval), 1);
    assert!(poll(&mut interval).is_pending());
    h.driver.shutdown();
    for _ in 0..3 {
        assert_eq!(poll(&mut interval), Poll::Ready(Err(Error::Shutdown)));
    }
    interval.reset();
    assert_eq!(poll(&mut interval), Poll::Ready(Err(Error::Shutdown)));
}

#[test]
fn huge_periods_saturate_instead_of_panicking() {
    let mut h = Harness::new();
    let mut interval = h.handle.interval(Duration::MAX);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    assert_eq!(ready_ticks(&mut interval), 1);
    h.advance(1_000);
    assert_eq!(ready_ticks(&mut interval), 0);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);
    interval.reset_immediately();
    assert_eq!(ready_ticks(&mut interval), 1);
    assert_eq!(ready_ticks(&mut interval), 0);
}

#[test]
#[should_panic(expected = "`period` must be nonzero")]
fn zero_period_panics() {
    let h = Harness::new();
    let _ = h.handle.interval(Duration::ZERO);
}

/// Inner park that jumps the simulated clock to the requested timeout.
#[derive(Debug)]
struct JumpPark(Clock, Rc<Cell<usize>>);

impl Park for JumpPark {
    type Unparker = NoopUnpark;
    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> std::io::Result<()> {
        if let ParkMode::Timeout(timeout) = mode {
            self.1.set(self.1.get() + 1);
            self.0.advance(timeout);
        }
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

#[test]
fn executor_wakes_the_ticking_task_at_each_deadline() {
    let clock = Clock::simulated();
    let parks = Rc::new(Cell::new(0));
    let driver = Driver::new(JumpPark(clock.clone(), parks.clone()), clock);
    let mut executor = LocalExecutor::new(driver);

    executor.block_on(async {
        let handle = Handle::current();
        let start = handle.clock().now();
        let mut interval = handle.interval(ms(10));
        for _ in 0..6 {
            interval.tick().await.unwrap();
        }
        assert_eq!(handle.clock().now() - start, ms(50));
    });
    assert_eq!(parks.get(), 5, "one timed park per tick after the first");
}
