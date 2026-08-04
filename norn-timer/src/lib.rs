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
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::Context;
use std::time::{Duration, Instant};

pub use clock::Clock;
pub use error::{Error, ErrorKind};
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

pin_project_lite::pin_project! {
    /// Future returned by [`Handle::sleep`].
    ///
    /// This future will resolve once the specified duration has elapsed,
    /// or the time driver is shut down.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Sleep {
        #[pin]
        inner: entry::Sleep<Rc<wheels::Wheels>>,
        clock: Clock,
    }
}

impl std::fmt::Debug for Sleep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Sleep").finish()
    }
}

impl Future for Sleep {
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> std::task::Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl Sleep {
    /// Reset the timer.
    ///
    /// This will clear any timer state and reset it
    /// for its initial duration on the next poll.
    ///
    /// This can be used to implement retry logic without
    /// having to reallocate the timer.
    pub fn reset(&mut self) {
        self.inner.reset();
    }
}

impl Handle {
    /// Create a new timer with the specified duration.
    ///
    /// Once the duration has elapsed, the timer will fire.
    pub fn sleep(&self, duration: Duration) -> Sleep {
        let inner = entry::Sleep::new(self.wheels.clone(), duration);
        Sleep {
            inner,
            clock: self.clock.clone(),
        }
    }

    /// Create a new timer that completes at an absolute deadline.
    ///
    /// If the deadline has already elapsed, the sleep completes on its next
    /// poll. The deadline uses the same clock as this handle, so simulated
    /// clocks can be advanced after constructing the sleep without changing
    /// its target time.
    pub fn sleep_until(&self, deadline: Instant) -> Sleep {
        let inner = entry::Sleep::new_at(self.wheels.clone(), self.clock.instant_to_tick(deadline));
        Sleep {
            inner,
            clock: self.clock.clone(),
        }
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
            if let ParkMode::Timeout(timeout) = mode {
                mode = ParkMode::Timeout(timeout.min(duration));
            } else {
                mode = ParkMode::Timeout(duration)
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
