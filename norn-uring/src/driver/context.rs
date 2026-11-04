//! Thread local context for the driver.

use std::cell::RefCell;

use crate::Handle;

thread_local! {
    static CURRENT: DriverContext = DriverContext::new();
}

pub(crate) struct DriverContext {
    handle: RefCell<Option<Handle>>,
}

impl DriverContext {
    fn new() -> Self {
        Self {
            handle: Default::default(),
        }
    }

    pub(crate) fn enter(handle: Handle) -> DriverContextGuard {
        CURRENT.with(|current| {
            let mut old = current.handle.borrow_mut();
            assert!(
                old.is_none(),
                "an io_uring driver is already entered on this thread; drivers cannot be nested"
            );
            *old = Some(handle);
        });
        DriverContextGuard {
            entered: true,
            prev: None,
        }
    }

    /// Install `handle` for teardown.
    ///
    /// Unlike [`DriverContext::enter`], this tolerates an existing context,
    /// which is restored when the guard drops, and thread-local storage that
    /// has already been destroyed (during thread exit), in which case no
    /// context is installed.
    pub(crate) fn enter_for_shutdown(handle: Handle) -> DriverContextGuard {
        match CURRENT.try_with(|current| current.handle.replace(Some(handle))) {
            Ok(prev) => DriverContextGuard {
                entered: true,
                prev,
            },
            Err(_) => DriverContextGuard {
                entered: false,
                prev: None,
            },
        }
    }

    /// Returns a reference to the current executor.
    pub(crate) fn handle() -> Option<Handle> {
        CURRENT.with(|c| c.handle.borrow().clone())
    }
}

#[derive(Debug)]
// This type is exposed only as `Park::ContextGuard`; callers never construct it.
#[allow(unnameable_types)]
pub struct DriverContextGuard {
    /// Whether this guard installed a handle that it must remove.
    entered: bool,
    /// The handle to restore when this guard drops.
    prev: Option<Handle>,
}

impl Drop for DriverContextGuard {
    fn drop(&mut self) {
        if !self.entered {
            return;
        }
        let prev = self.prev.take();
        let _current = CURRENT.try_with(|current| {
            let mut executor = current.handle.borrow_mut();
            assert!(executor.is_some(), "driver not set");
            std::mem::replace(&mut *executor, prev)
        });
    }
}
