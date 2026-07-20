//! Ring-level coordination for registered fixed buffers.

use std::any::Any;
use std::cell::{Cell, RefCell};
use std::{io, mem};

use log::warn;

use crate::driver::RingInner;

const LOG: &str = "norn_uring::registered_buffers";

pub(crate) type Generation = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum State {
    Empty,
    Registering(Generation),
    RegisteringKernel(Generation),
    Registered(Generation),
    Released(Generation),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReserveError {
    DriverStopped,
    TableInUse,
    GenerationExhausted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Release {
    Unregistered,
    RingGone,
}

#[derive(Debug)]
pub(crate) enum ReleaseError {
    StateMismatch,
    DriverBorrowed,
    Io(io::Error),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Retention {
    Retained,
    RingGone,
    Leaked,
}

/// Coordinates the single registered fixed-buffer table owned by one ring.
///
/// This field must be owned by the same object as the ring and dropped after
/// it. Retained storage may still be referenced by the kernel when explicit
/// unregistration fails, so ring destruction must precede its release.
pub(crate) struct Registry {
    retained: RefCell<Vec<(Generation, Box<dyn Any>)>>,
    state: Cell<State>,
    generation: Cell<Generation>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            retained: RefCell::new(Vec::new()),
            state: Cell::new(State::Empty),
            generation: Cell::new(0),
        }
    }

    /// Reserve the ring's fixed-buffer table for a registration attempt.
    pub(crate) fn reserve(&self, ring: &RefCell<RingInner>) -> Result<Generation, ReserveError> {
        self.try_release_retained(ring);
        let retained_storage = self
            .retained
            .try_borrow()
            .map_or(true, |retained| !retained.is_empty());
        if self.state.get() != State::Empty || retained_storage {
            return Err(ReserveError::TableInUse);
        }

        let generation = self
            .generation
            .get()
            .checked_add(1)
            .ok_or(ReserveError::GenerationExhausted)?;
        self.generation.set(generation);
        self.state.set(State::Registering(generation));
        Ok(generation)
    }

    pub(crate) fn arm_kernel_call(&self, generation: Generation) {
        assert_eq!(
            self.state.get(),
            State::Registering(generation),
            "fixed-buffer reservation changed before kernel registration"
        );
        self.state.set(State::RegisteringKernel(generation));
    }

    pub(crate) fn kernel_call_failed(&self, generation: Generation) {
        assert_eq!(
            self.state.get(),
            State::RegisteringKernel(generation),
            "fixed-buffer reservation changed after failed kernel registration"
        );
        self.state.set(State::Registering(generation));
    }

    pub(crate) fn commit(&self, generation: Generation) {
        assert_eq!(
            self.state.get(),
            State::RegisteringKernel(generation),
            "fixed-buffer reservation changed before commit"
        );
        self.state.set(State::Registered(generation));
    }

    pub(crate) fn rollback(&self, generation: Generation) {
        if self.state.get() == State::Registering(generation) {
            self.state.set(State::Empty);
        }
    }

    pub(crate) fn unregister(
        &self,
        ring: &RefCell<RingInner>,
        generation: Generation,
    ) -> Result<Release, ReleaseError> {
        let state = self.state.get();
        if !matches!(
            state,
            State::RegisteringKernel(active) | State::Registered(active) if active == generation
        ) {
            return Err(ReleaseError::StateMismatch);
        }

        let result = loop {
            let ring = ring
                .try_borrow()
                .map_err(|_| ReleaseError::DriverBorrowed)?;
            match ring.with_submitter(|submitter| submitter.unregister_buffers()) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        };
        if let Err(err) = result {
            if state == State::RegisteringKernel(generation)
                && err.raw_os_error() == Some(libc::ENXIO)
            {
                self.mark_released(generation);
                return Ok(Release::Unregistered);
            }
            return Err(ReleaseError::Io(err));
        }

        if self.state.get() != state {
            return Err(ReleaseError::StateMismatch);
        }
        self.mark_released(generation);
        Ok(Release::Unregistered)
    }

    /// Retain storage which may still be referenced by the kernel.
    pub(crate) fn retain(&self, generation: Generation, storage: Box<dyn Any>) -> Retention {
        let storage = mem::ManuallyDrop::new(storage);
        let Ok(mut retained) = self.retained.try_borrow_mut() else {
            return Retention::Leaked;
        };
        if retained.try_reserve(1).is_err() {
            return Retention::Leaked;
        }
        retained.push((generation, mem::ManuallyDrop::into_inner(storage)));
        Retention::Retained
    }

    /// Retry a best-effort pool-drop release before rejecting a new table.
    fn try_release_retained(&self, ring: &RefCell<RingInner>) {
        let retained_generation = match self.retained.try_borrow() {
            Ok(retained) if retained.is_empty() => return,
            Ok(retained) => {
                let generation = retained[0].0;
                if retained
                    .iter()
                    .any(|(candidate, _)| *candidate != generation)
                {
                    warn!(target: LOG, "retry_unregister.retained_generation_mismatch");
                    return;
                }
                generation
            }
            Err(_) => return,
        };

        match self.state.get() {
            // Retained storage paired with no active generation indicates an
            // invariant mismatch. Keep it until ring destruction; assuming
            // that Empty means the kernel has forgotten its pointers would
            // defeat the conservative fail-safe.
            State::Empty | State::Released(_) => return,
            state @ (State::RegisteringKernel(generation) | State::Registered(generation)) => {
                if retained_generation != generation {
                    warn!(target: LOG, "retry_unregister.active_generation_mismatch");
                    return;
                }
                let result = {
                    let Ok(ring) = ring.try_borrow() else {
                        return;
                    };
                    loop {
                        match ring.with_submitter(|submitter| submitter.unregister_buffers()) {
                            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                            result => break result,
                        }
                    }
                };
                if let Err(err) = result {
                    if state == State::RegisteringKernel(generation)
                        && err.raw_os_error() == Some(libc::ENXIO)
                    {
                        self.mark_released(generation);
                        self.release_retained_storage();
                        return;
                    }
                    warn!(target: LOG, "retry_unregister.failed {err:?}");
                    return;
                }
                if self.state.get() != state {
                    warn!(target: LOG, "retry_unregister.state_mismatch");
                    return;
                }
                self.mark_released(generation);
            }
            State::Registering(_) => return,
        }

        self.release_retained_storage();
    }

    fn mark_released(&self, generation: Generation) {
        self.state.set(State::Released(generation));
        self.state.set(State::Empty);
    }

    fn release_retained_storage(&self) {
        // No kernel reference remains. If a nested borrow prevents immediate
        // reclamation, leaving the values in this registry is safe; a later
        // registration or registry drop will reclaim them.
        let Ok(mut retained) = self.retained.try_borrow_mut() else {
            return;
        };
        let released = mem::take(&mut *retained);
        drop(retained);
        drop(released);
    }

    #[cfg(test)]
    pub(crate) fn test_state(&self) -> State {
        self.state.get()
    }

    #[cfg(test)]
    pub(crate) fn test_forget(&self) {
        self.state.set(State::Empty);
    }

    #[cfg(test)]
    pub(crate) fn test_retained_len(&self) -> usize {
        self.retained.borrow().len()
    }
}

impl std::fmt::Debug for Registry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Registry")
            .field("state", &self.state.get())
            .field("generation", &self.generation.get())
            .field("retained", &self.retained.borrow().len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use super::*;

    struct DropTracked(Rc<Cell<usize>>);

    impl Drop for DropTracked {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    #[test]
    fn retained_storage_cannot_unregister_a_different_generation() -> io::Result<()> {
        let registry = Registry::new();
        let ring = RefCell::new(RingInner::Base(io_uring::IoUring::builder().build(8)?));

        let first = registry
            .reserve(&ring)
            .expect("first generation should reserve");
        registry.rollback(first);
        let second = registry
            .reserve(&ring)
            .expect("second generation should reserve");

        let mut registered = Box::new([0u8; 8]);
        let iovec = libc::iovec {
            iov_base: registered.as_mut_ptr().cast(),
            iov_len: registered.len(),
        };
        registry.arm_kernel_call(second);
        // Safety: `registered` remains alive and fixed in its box through the
        // successful unregistration below.
        unsafe {
            ring.borrow()
                .with_submitter(|submitter| submitter.register_buffers(&[iovec]))?
        };
        registry.commit(second);

        assert!(matches!(
            registry.unregister(&ring, first),
            Err(ReleaseError::StateMismatch)
        ));
        let drops = Rc::new(Cell::new(0));
        assert!(matches!(
            registry.retain(first, Box::new(DropTracked(Rc::clone(&drops)))),
            Retention::Retained
        ));

        assert_eq!(registry.reserve(&ring), Err(ReserveError::TableInUse));
        assert_eq!(registry.test_state(), State::Registered(second));
        assert_eq!(drops.get(), 0);

        assert!(matches!(
            registry.unregister(&ring, second),
            Ok(Release::Unregistered)
        ));
        drop(registered);
        drop(ring);
        drop(registry);
        assert_eq!(drops.get(), 1);
        Ok(())
    }
}
