use std::cell::{Cell, RefCell};
use std::pin::Pin;
use std::ptr;
use std::rc::Rc;
use std::time::Duration;

use cordyceps::List;

use crate::{entry, error, level, MAX_DURATION, NUM_LEVELS};

pub(crate) struct Wheels {
    elapsed: Cell<u64>,
    shutdown: Cell<bool>,
    wheels: RefCell<Vec<level::Level>>,
    next_expiration_hint: Cell<Option<level::Expiration>>,
    next_expiration_dirty: Cell<bool>,
}

impl entry::TimerList for Rc<Wheels> {
    fn remove(&self, entry: ptr::NonNull<entry::Entry>) {
        Wheels::remove(self, entry);
    }

    fn add(&self, entry: Pin<&mut entry::Entry>, duration: Duration) {
        if duration.is_zero() {
            entry.as_ref().fire(Ok(()));
            return;
        }
        let expiration = self.elapsed() + duration.as_millis() as u64;
        entry.set_registered(expiration);
        let entry = unsafe { ptr::NonNull::from(entry.get_unchecked_mut()) };
        Wheels::insert(self, entry);
    }
}

impl Wheels {
    pub(crate) fn new() -> Self {
        let levels = (0..NUM_LEVELS).map(level::Level::new).collect();
        Self {
            elapsed: Cell::new(0),
            shutdown: Cell::new(false),
            wheels: RefCell::new(levels),
            next_expiration_hint: Cell::new(None),
            next_expiration_dirty: Cell::new(false),
        }
    }

    /// Expensive operation to count the number of registered timers.
    ///
    /// This should only be used for debugging or tests.
    #[cfg(test)]
    fn num_registered(&self) -> usize {
        self.wheels
            .borrow()
            .iter()
            .map(|w| w.num_registered())
            .sum()
    }

    pub(crate) fn advance(&self, now: u64) -> (usize, Option<level::Expiration>) {
        let mut pending = List::<entry::Entry>::new();
        let mut fired = 0;

        let mut next_expiration = self.next_expiration();
        while let Some(expiration) = next_expiration {
            if expiration.deadline() > now {
                break;
            }
            let entries = self.take_entries(&expiration);
            for entry in entries {
                let entry_expiration = unsafe { entry.as_ref().expiration() };
                if entry_expiration > now {
                    assert_ne!(expiration.level(), 0, "should not reschedule 0th level");
                    pending.push_front(entry);
                } else {
                    fired += 1;
                    unsafe { entry.as_ref().fire(Ok(())) };
                }
            }
            self.set_elapsed(expiration.deadline());
            next_expiration = self.next_expiration();
        }
        self.set_elapsed(now);
        let needs_reschedule = !pending.is_empty();
        for entry in pending.into_iter() {
            self.insert(entry);
        }
        if needs_reschedule {
            next_expiration = self.next_expiration();
        }
        (fired, next_expiration)
    }

    pub(crate) fn elapsed(&self) -> u64 {
        self.elapsed.get()
    }

    fn insert(&self, entry: ptr::NonNull<entry::Entry>) {
        if self.shutdown.get() {
            unsafe { entry.as_ref().fire(Err(error::Error::shutdown())) };
            return;
        }
        let expiration = unsafe { entry.as_ref().expiration() };
        if expiration <= self.elapsed() {
            unsafe { entry.as_ref().fire(Ok(())) };
        } else {
            let wheel = wheel_for(self.elapsed(), expiration);
            let slot = self.wheels.borrow_mut()[wheel].add_entry(entry);
            if !self.next_expiration_dirty.get() {
                if let Some(current) = self.next_expiration_hint.get() {
                    // A newly inserted timer may only invalidate the hint if
                    // it expires no later than the hinted deadline.
                    if expiration <= current.deadline() {
                        self.next_expiration_dirty.set(true);
                    }
                } else {
                    let candidate = level::expiration_for_slot(wheel, slot, self.elapsed());
                    self.next_expiration_hint.set(Some(candidate));
                }
            }
        }
    }

    fn remove(&self, entry: ptr::NonNull<entry::Entry>) -> Option<ptr::NonNull<entry::Entry>> {
        let expiration = unsafe { entry.as_ref().expiration() };
        let wheel = wheel_for(self.elapsed(), expiration);
        let slot = level::slot_for(expiration, wheel);
        let removed = unsafe { self.wheels.borrow_mut()[wheel].remove_entry(entry) };
        if removed.is_some() {
            if let Some(next) = self.next_expiration_hint.get() {
                if next.level() == wheel && next.slot() == slot {
                    self.next_expiration_dirty.set(true);
                }
            }
        }
        removed
    }

    fn next_expiration(&self) -> Option<level::Expiration> {
        if !self.next_expiration_dirty.get() {
            return self.next_expiration_hint.get();
        }
        let next = self.scan_next_expiration();
        self.next_expiration_hint.set(next);
        self.next_expiration_dirty.set(false);
        next
    }

    fn scan_next_expiration(&self) -> Option<level::Expiration> {
        let now = self.elapsed.get();
        let wheels = self.wheels.borrow();
        for level in 0..NUM_LEVELS {
            if let Some(expiration) = wheels[level].next_expiration(now) {
                return Some(expiration);
            }
        }
        None
    }

    /// Set the amount of ticks which have elapsed since the creation of this Wheel, if `when`
    /// is greater than the current number of ticks which have elapsed.
    ///
    /// The internal tick counter will be maintained to keep it monotonically increasing.
    fn set_elapsed(&self, when: u64) {
        assert!(
            self.elapsed.get() <= when,
            "elapsed={:?} > when={:?}",
            self.elapsed,
            when
        );
        if when > self.elapsed.get() {
            self.elapsed.set(when);
            // Advancing time alone does not invalidate the hint while it still
            // points to a future deadline and no structural wheel changes were
            // made (insert/remove/take mark the hint dirty themselves).
            if let Some(next) = self.next_expiration_hint.get() {
                if when >= next.deadline() {
                    self.next_expiration_dirty.set(true);
                }
            }
        }
    }

    fn take_entries(&self, expiration: &level::Expiration) -> cordyceps::List<entry::Entry> {
        self.next_expiration_dirty.set(true);
        self.wheels.borrow_mut()[expiration.level()].take_slot(expiration.slot())
    }

    pub(crate) fn shutdown(&self) {
        self.shutdown.set(true);
        while let Some(exp) = self.next_expiration() {
            let entries = self.take_entries(&exp);
            for entry in entries {
                unsafe { entry.as_ref().fire(Err(error::Error::shutdown())) };
            }
        }
    }
}

/// Gets the wheel for a timer based on the elapsed time and
/// the expiration time of the timer.
const fn wheel_for(elapsed: u64, when: u64) -> usize {
    const SLOT_MASK: u64 = (1 << NUM_LEVELS) - 1;

    // Mask in the trailing bits ignored by the level calculation in order to cap
    // the possible leading zeros
    let mut masked = elapsed ^ when | SLOT_MASK;

    if masked >= MAX_DURATION {
        // Fudge the timer into the top level
        masked = MAX_DURATION - 1;
    }

    // Find MSB set
    let leading_zeros = masked.leading_zeros() as usize;
    let significant = 63 - leading_zeros;

    significant / NUM_LEVELS
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Poll;
    use std::time::Duration;

    use crate::clock::Clock;
    use crate::Driver;

    use super::*;

    #[test]
    fn wheel_for_time() {
        assert_eq!(wheel_for(0, 0), 0);
        assert_eq!(wheel_for(0, 63), 0);
        assert_eq!(wheel_for(0, 64), 1);

        assert_eq!(wheel_for(0, 1 << 12), 2);
        assert_eq!(wheel_for(0, 1 << 18), 3);
    }

    #[test]
    fn timer_fires_immediately() {
        let mut cx = futures_test::task::noop_context();
        let timer = Driver::new((), Clock::system());
        let handle = timer.handle();
        let sleep = pin!(handle.sleep(Duration::from_millis(0)));
        assert!(sleep.poll(&mut cx).is_ready());
    }

    #[test]
    fn future_timer_does_not_fire() {
        let mut cx = futures_test::task::noop_context();
        let timer = Driver::new((), Clock::system());
        let handle = timer.handle();
        let mut sleep = pin!(handle.sleep(Duration::from_millis(1)));
        assert!(sleep.as_mut().poll(&mut cx).is_pending());
        drop(timer);
        drop(handle);
        let poll = sleep.as_mut().poll(&mut cx);
        match poll {
            Poll::Ready(Err(err)) => {
                assert!(
                    err.to_string().contains("shut down"),
                    "unexpected timer drop error: {}",
                    err
                );
            }
            other => panic!("expected shutdown error after timer drop, got: {other:?}"),
        }
    }

    #[test]
    fn timer_drop() {
        let mut cx = futures_test::task::noop_context();
        let timer = Driver::new((), Clock::system());
        let handle = timer.handle();
        {
            let mut sleep = pin!(handle.sleep(Duration::from_millis(1)));
            assert!(sleep.as_mut().poll(&mut cx).is_pending());
        }
        assert_eq!(timer.wheels.num_registered(), 0);
    }

    #[test]
    fn timer_fire() {
        let mut cx = futures_test::task::noop_context();
        let timer = Driver::new((), Clock::system());
        let handle = timer.handle();
        let mut sleep = pin!(handle.sleep(Duration::from_millis(20)));
        assert!(sleep.as_mut().poll(&mut cx).is_pending());

        let (expired, next) = timer.wheels.advance(10);
        assert_eq!(expired, 0);
        assert!(next.unwrap().deadline() == 20);
        let (expired, next) = timer.wheels.advance(50);
        assert_eq!(expired, 1);
        assert!(next.is_none());
        assert!(sleep.as_mut().poll(&mut cx).is_ready());
    }
}
