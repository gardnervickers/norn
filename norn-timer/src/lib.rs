//! Timer-wheel support for Norn runtimes.
//!
//! [`Driver`] wraps another [`Park`] implementation, advances a hierarchical
//! timer wheel when the executor parks, and limits the inner park duration to
//! the next timer deadline. [`Handle`] creates [`Sleep`] futures. [`Clock`] can
//! track system time or manually advanced time for deterministic tests.
//!
//! # Example
//!
//! ```no_run
//! use std::time::Duration;
//!
//! use norn_executor::park::ThreadPark;
//! use norn_executor::LocalExecutor;
//! use norn_timer::{Clock, Driver, Handle};
//!
//! let timer = Driver::new(ThreadPark::default(), Clock::system());
//! let mut executor = LocalExecutor::new(timer);
//! executor.block_on(async {
//!     Handle::current()
//!         .sleep(Duration::from_millis(10))
//!         .await
//!         .unwrap();
//! });
//! ```
//!
//! The wheel layout is based on the timer implementation in
//! [Tokio](https://github.com/tokio-rs/tokio).
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    rustdoc::bare_urls,
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_safety_doc
)]
use std::rc::Rc;
use std::time::{Duration, Instant};

pub use clock::Clock;
pub use entry::Sleep;
pub use error::Error;
use norn_executor::park::{Park, ParkMode};

mod clock;
mod context;
mod entry;
mod error;
mod level;
#[cfg(test)]
mod tests;
mod wheels;

const NUM_LEVELS: usize = 6;
const MAX_DURATION: u64 = (1 << (NUM_LEVELS.pow(2))) - 1;

/// [`Driver`] for time based operations.
///
/// This supports driving multiple timers simultaneously.
pub struct Driver<P> {
    wheels: Rc<wheels::Wheels>,
    inner: P,
    clock: Clock,
}

impl<P> std::fmt::Debug for Driver<P> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("clock", &self.clock)
            .finish()
    }
}

/// Handle to the timer driver.
///
/// This can be used to create new timers.
#[derive(Clone)]
pub struct Handle {
    wheels: Rc<wheels::Wheels>,
    clock: Clock,
}

impl std::fmt::Debug for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle")
            .field("clock", &self.clock)
            .finish()
    }
}

impl Handle {
    /// Create a new timer with the specified duration.
    ///
    /// The duration starts at construction. Once it has elapsed, the timer
    /// will fire. Nonzero durations round up to the timer wheel's millisecond
    /// tick. Deadlines beyond `u64::MAX` milliseconds since the clock origin
    /// saturate at that final tick. See [`Handle::sleep_until`] for an absolute
    /// deadline.
    pub fn sleep(&self, duration: Duration) -> Sleep {
        Sleep::new(self.wheels.clone(), self.clock.clone(), duration)
    }

    /// Create a new timer that completes at an absolute deadline.
    ///
    /// If the deadline has already elapsed at construction, the sleep completes
    /// on its next poll. The deadline uses the same clock as this handle, so
    /// simulated clocks can be advanced after constructing the sleep without
    /// changing its target time. Future absolute deadlines round up to the
    /// timer wheel's millisecond tick and saturate at `u64::MAX` milliseconds
    /// since the clock origin.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        Sleep::new_at(
            self.wheels.clone(),
            self.clock.clone(),
            self.clock.instant_to_tick(deadline),
        )
    }

    /// Get the clock used by the timer.
    pub fn clock(&self) -> &Clock {
        &self.clock
    }

    /// Get a handle to the current timer.
    ///
    /// ### Panics
    /// This will panic if called from outside of a timer context.
    pub fn current() -> Self {
        context::Context::handle().expect("timer not started")
    }
}

impl<P> Driver<P> {
    /// Create a new timer driver with the provided clock.
    ///
    /// The clock will be used to determine the current time.
    pub fn new(inner: P, clock: Clock) -> Self {
        Self {
            wheels: Rc::new(wheels::Wheels::new()),
            inner,
            clock,
        }
    }

    /// Get a handle to the timer driver.
    pub fn handle(&self) -> Handle {
        Handle {
            clock: self.clock.clone(),
            wheels: self.wheels.clone(),
        }
    }
}

impl<P> Drop for Driver<P> {
    fn drop(&mut self) {
        // Always wake outstanding sleepers even if callers drop the timer
        // driver directly without going through Park::shutdown.
        self.wheels.shutdown();
    }
}

impl<P> Park for Driver<P>
where
    P: Park,
{
    type Unparker = P::Unparker;

    type Guard = (context::ContextGuard, P::Guard);

    fn park(&mut self, mut mode: ParkMode) -> Result<(), std::io::Error> {
        let ticks = self.clock.tick();

        let (fired, next_expiration) = self.wheels.advance(ticks);
        if fired > 0 {
            mode = ParkMode::NoPark;
        }
        if let Some(expiration) = next_expiration {
            let delta = expiration.deadline().saturating_sub(ticks);
            let duration = self.clock.tick_to_duration(delta);
            match mode {
                ParkMode::NoPark => {}
                ParkMode::Timeout(timeout) => {
                    mode = ParkMode::Timeout(timeout.min(duration));
                }
                ParkMode::NextCompletion => {
                    mode = ParkMode::Timeout(duration);
                }
            }
        }
        self.inner.park(mode)
    }

    fn enter(&self) -> Self::Guard {
        let handle = self.handle();
        let guard = context::Context::enter(handle);
        (guard, self.inner.enter())
    }

    fn unparker(&self) -> Self::Unparker {
        self.inner.unparker()
    }

    fn needs_park(&self) -> bool {
        self.inner.needs_park()
    }

    fn shutdown(&mut self) {
        self.wheels.shutdown();
        self.inner.shutdown()
    }
}
