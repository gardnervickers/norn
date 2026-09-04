use std::cell::Cell;
use std::rc::Rc;
use std::time::{Duration, Instant};

/// A clock for tracking time.
#[derive(Debug, Clone)]
pub struct Clock {
    start: Instant,
    time: TimeSource,
}

impl Clock {
    /// Create a new system clock.
    ///
    /// The system clock will start with the current system time.
    pub fn system() -> Self {
        Self {
            start: Instant::now(),
            time: TimeSource::System,
        }
    }

    /// Create a new simulated clock.
    ///
    /// The simulated clock will start with frozen time.
    /// Time can be advanced by calling [`Clock::advance`].
    pub fn simulated() -> Self {
        Self {
            start: Instant::now(),
            time: TimeSource::Simulated {
                offset: Rc::new(Cell::new(Duration::from_secs(0))),
            },
        }
    }

    /// Convert the provided instant to a tick which can be used inside the time driver.
    pub(crate) fn instant_to_tick(&self, t: Instant) -> u64 {
        let target = t
            .checked_duration_since(self.start)
            .unwrap_or(Duration::ZERO);
        if target <= self.elapsed() {
            0
        } else {
            duration_to_deadline_tick(target)
        }
    }

    /// Return the deadline after `duration` starting at the current time.
    ///
    /// This intentionally performs the arithmetic as a [`Duration`] before
    /// converting to ticks instead of adding to an [`Instant`]: valid
    /// `Duration` values can be larger than the range representable by an
    /// `Instant` on a platform.
    pub(crate) fn deadline_after(&self, duration: Duration) -> u64 {
        if duration.is_zero() {
            self.tick()
        } else {
            duration_to_deadline_tick(self.elapsed().saturating_add(duration))
        }
    }

    /// Convert a tick to a duration value.
    pub(crate) fn tick_to_duration(&self, t: u64) -> Duration {
        Duration::from_millis(t)
    }

    /// Return the current tick.
    pub(crate) fn tick(&self) -> u64 {
        duration_to_elapsed_tick(self.elapsed())
    }

    /// Return the current instant.
    ///
    /// # Panics
    ///
    /// For a simulated clock, this panics when the simulated offset cannot be
    /// represented by the platform's [`Instant`] type.
    pub fn now(&self) -> Instant {
        match &self.time {
            TimeSource::System => Instant::now(),
            TimeSource::Simulated { offset } => {
                let offset = offset.get();
                self.start + offset
            }
        }
    }
    /// Advance simulated time.
    ///
    /// Repeated advances saturate at [`Duration::MAX`].
    ///
    /// ### Panics
    /// Panics if called on a system clock created with [`Clock::system`].
    pub fn advance(&self, duration: Duration) {
        match &self.time {
            TimeSource::System => panic!("Cannot advance system clock"),
            TimeSource::Simulated { offset } => {
                offset.set(offset.get().saturating_add(duration));
            }
        }
    }

    fn elapsed(&self) -> Duration {
        match &self.time {
            TimeSource::System => Instant::now()
                .checked_duration_since(self.start)
                .unwrap_or(Duration::ZERO),
            TimeSource::Simulated { offset } => offset.get(),
        }
    }
}

/// Convert elapsed wall-clock time to complete millisecond ticks.
fn duration_to_elapsed_tick(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

/// Convert a representable deadline to timer ticks without firing it early.
fn duration_to_deadline_tick(duration: Duration) -> u64 {
    const NANOS_PER_MILLISECOND: u32 = 1_000_000;

    let milliseconds = duration.as_millis();
    let milliseconds = if duration
        .subsec_nanos()
        .is_multiple_of(NANOS_PER_MILLISECOND)
    {
        milliseconds
    } else {
        milliseconds.saturating_add(1)
    };
    milliseconds.min(u128::from(u64::MAX)) as u64
}

#[derive(Debug, Clone)]
enum TimeSource {
    System,
    Simulated { offset: Rc<Cell<Duration>> },
}
