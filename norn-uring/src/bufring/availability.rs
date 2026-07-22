use std::cell::{Cell, RefCell};

use crate::util::notify::Notify;

use super::ledger::RingFault;

/// A broadcast generation used to rearm consumers after ring capacity changes.
#[derive(Debug, Default)]
pub(super) struct Availability {
    generation: Cell<u64>,
    notify: Notify,
    fault: RefCell<Option<RingFault>>,
}

impl Availability {
    pub(super) fn generation(&self) -> u64 {
        self.generation.get()
    }

    pub(super) fn advance(&self) {
        self.generation.set(self.generation.get().wrapping_add(1));
        self.notify.notify(usize::MAX);
    }

    pub(super) fn fail(&self, fault: RingFault) {
        if self.fault.borrow().is_none() {
            *self.fault.borrow_mut() = Some(fault);
        }
        self.advance();
    }

    pub(super) fn check(&self) -> Result<(), RingFault> {
        match self.fault.borrow().as_ref() {
            Some(fault) => Err(fault.clone()),
            None => Ok(()),
        }
    }

    pub(super) async fn changed_since(&self, observed: u64) -> Result<(), RingFault> {
        loop {
            self.check()?;
            if self.generation() != observed {
                return Ok(());
            }

            // `Notify` registers when first polled. Because the runtime is local,
            // checking the generation immediately before awaiting this future is
            // enough to make registration and the recheck one indivisible poll.
            self.notify.wait().await;
        }
    }

    pub(super) async fn failed(&self) -> RingFault {
        loop {
            if let Err(fault) = self.check() {
                return fault;
            }
            self.notify.wait().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::pin;
    use std::task::Poll;

    use futures_test::task::noop_context;

    use super::*;

    #[test]
    fn change_before_registration_is_observed() {
        let availability = Availability::default();
        let observed = availability.generation();
        availability.advance();

        let mut changed = pin!(availability.changed_since(observed));
        let mut cx = noop_context();
        assert_eq!(changed.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }

    #[test]
    fn change_after_registration_wakes_every_waiter() {
        let availability = Availability::default();
        let observed = availability.generation();
        let mut first = pin!(availability.changed_since(observed));
        let mut second = pin!(availability.changed_since(observed));
        let mut cx = noop_context();

        assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Pending);
        availability.advance();
        assert_eq!(first.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }

    #[test]
    fn later_generation_does_not_satisfy_a_new_snapshot() {
        let availability = Availability::default();
        availability.advance();
        let observed = availability.generation();
        let mut changed = pin!(availability.changed_since(observed));
        let mut cx = noop_context();

        assert_eq!(changed.as_mut().poll(&mut cx), Poll::Pending);
        availability.advance();
        assert_eq!(changed.as_mut().poll(&mut cx), Poll::Ready(Ok(())));
    }

    #[test]
    fn fault_wakes_all_waiters_terminally() {
        let availability = Availability::default();
        let observed = availability.generation();
        let mut first = pin!(availability.changed_since(observed));
        let mut second = pin!(availability.changed_since(observed));
        let mut cx = noop_context();
        assert_eq!(first.as_mut().poll(&mut cx), Poll::Pending);
        assert_eq!(second.as_mut().poll(&mut cx), Poll::Pending);

        availability.fail(RingFault::new("test quarantine"));
        assert!(matches!(first.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
        assert!(matches!(second.as_mut().poll(&mut cx), Poll::Ready(Err(_))));
    }
}
