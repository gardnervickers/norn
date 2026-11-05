use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use crate::{error, Handle, Sleep};

/// How an [`Interval`] schedules ticks after missing one.
///
/// A tick is *missed* when, by the time the previous tick is delivered, the
/// next tick's deadline has already passed. This happens when the task
/// awaiting the interval runs late (for example, because another task held
/// the thread) or stops calling [`Interval::tick`] for a while. Lateness of
/// less than one period is not a miss, and every behavior then keeps the
/// original schedule.
///
/// The examples below use a 10 ms period starting at 0 ms, where the tick due
/// at 10 ms is delivered at 35 ms.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Hash)]
pub enum MissedTickBehavior {
    /// Deliver every missed tick immediately, then resume the original
    /// schedule.
    ///
    /// The ticks due at 20 and 30 ms complete without waiting, and the next
    /// tick is due at 40 ms. The total tick count over a long run matches
    /// `elapsed / period`, at the cost of a burst of back-to-back ticks after
    /// a stall.
    Burst,
    /// Restart the schedule one period after the late tick was delivered.
    ///
    /// The next ticks are due at 45, 55 and 65 ms. Ticks are never closer
    /// together than one period after a miss, but the schedule drifts by the
    /// amount of each miss.
    Delay,
    /// Drop the missed ticks and resume at the next deadline on the original
    /// schedule.
    ///
    /// The next tick is due at 40 ms. Ticks stay aligned with the start time
    /// and never arrive in a burst; ticks due while the task was late are
    /// lost.
    ///
    /// This is the default. A single-threaded runtime stalls every task
    /// together, so catching up with a burst prolongs the stall, and delaying
    /// loses the schedule's alignment for no benefit.
    #[default]
    Skip,
}

/// A timer that completes repeatedly at a fixed period.
///
/// Created by [`Handle::interval`] and [`Handle::interval_at`]. Call
/// [`Interval::tick`] in a loop. The first tick completes at the start time,
/// immediately for [`Handle::interval`], and each later tick completes one
/// period after the previous scheduled tick. Ticks round up to the timer
/// wheel's millisecond resolution without accumulating rounding error, so the
/// schedule does not drift. [`MissedTickBehavior`] controls what happens
/// when a tick is missed.
///
/// The schedule is computed from tick deadlines, not from when [`Interval::tick`]
/// is called, so time spent handling a tick does not delay the next one.
#[derive(Debug)]
pub struct Interval {
    sleep: Pin<Box<Sleep>>,
    /// Offset from the clock origin of the tick the sleep is waiting for.
    next: Duration,
    period: Duration,
    behavior: MissedTickBehavior,
}

impl Interval {
    pub(crate) fn new(handle: &Handle, start: Duration, period: Duration) -> Self {
        assert!(!period.is_zero(), "`period` must be nonzero");
        let deadline = handle.clock().offset_to_tick(start);
        Self {
            sleep: Box::pin(Sleep::new_at(handle.wheels.clone(), deadline)),
            next: start,
            period,
            behavior: MissedTickBehavior::default(),
        }
    }

    /// Complete when the next tick is due.
    ///
    /// # Errors
    ///
    /// Returns [`crate::Error::Shutdown`] if the timer driver shuts down
    /// before the tick. Every later tick then fails the same way.
    ///
    /// # Cancel safety
    ///
    /// This is cancel safe. Dropping the returned future before it completes
    /// does not consume a tick.
    pub fn tick(&mut self) -> impl Future<Output = Result<(), error::Error>> + '_ {
        poll_fn(|cx| self.poll_tick(cx))
    }

    /// Poll for the next tick.
    ///
    /// On `Poll::Pending`, the waker from `cx` is woken when the tick is due.
    /// Only the waker from the most recent call is woken.
    ///
    /// # Errors
    ///
    /// See [`Interval::tick`].
    pub fn poll_tick(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), error::Error>> {
        let result = std::task::ready!(self.sleep.as_mut().poll(cx));
        if result.is_ok() {
            let now = self.sleep.wheels().clock().elapsed();
            self.next = self.following(now);
        }
        // After shutdown the wheel completes every registration with an error,
        // so re-arming keeps failing later ticks instead of reporting success.
        let deadline = self.sleep.wheels().clock().offset_to_tick(self.next);
        self.sleep.as_mut().reset_at(deadline);
        Poll::Ready(result)
    }

    /// Compute the tick after the one at `self.next`, delivered at `now`.
    fn following(&self, now: Duration) -> Duration {
        let next = self.next.saturating_add(self.period);
        if now < next {
            return next;
        }
        match self.behavior {
            MissedTickBehavior::Burst => next,
            MissedTickBehavior::Delay => now.saturating_add(self.period),
            MissedTickBehavior::Skip => {
                // The smallest `self.next + k * period` strictly after `now`.
                let periods = (now - self.next).as_nanos() / self.period.as_nanos() + 1;
                let nanos = self
                    .period
                    .as_nanos()
                    .saturating_mul(periods)
                    .saturating_add(self.next.as_nanos());
                duration_from_nanos_saturating(nanos)
            }
        }
    }

    /// Restart the schedule so the next tick is due one period from now.
    pub fn reset(&mut self) {
        let now = self.sleep.wheels().clock().elapsed();
        self.reset_to(now.saturating_add(self.period));
    }

    /// Restart the schedule so the next tick is due immediately.
    pub fn reset_immediately(&mut self) {
        let now = self.sleep.wheels().clock().elapsed();
        self.reset_to(now);
    }

    fn reset_to(&mut self, next: Duration) {
        self.next = next;
        let deadline = self.sleep.wheels().clock().offset_to_tick(next);
        self.sleep.as_mut().reset_at(deadline);
    }

    /// Return the period between ticks.
    pub fn period(&self) -> Duration {
        self.period
    }

    /// Return how this interval handles missed ticks.
    pub fn missed_tick_behavior(&self) -> MissedTickBehavior {
        self.behavior
    }

    /// Set how this interval handles missed ticks.
    ///
    /// The new behavior applies from the next tick delivered.
    pub fn set_missed_tick_behavior(&mut self, behavior: MissedTickBehavior) {
        self.behavior = behavior;
    }
}

fn duration_from_nanos_saturating(nanos: u128) -> Duration {
    const NANOS_PER_SEC: u128 = 1_000_000_000;
    let secs = nanos / NANOS_PER_SEC;
    match u64::try_from(secs) {
        Ok(secs) => Duration::new(secs, (nanos % NANOS_PER_SEC) as u32),
        Err(_) => Duration::MAX,
    }
}
