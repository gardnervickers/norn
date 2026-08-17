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
            assert!(old.is_none(), "driver already set");
            *old = Some(handle);
        });
        DriverContextGuard {}
    }

    /// Returns a reference to the current executor.
    pub(crate) fn handle() -> Option<Handle> {
        CURRENT.with(|c| c.handle.borrow().clone())
    }
}

#[derive(Debug)]
// This type is exposed only as `Park::ContextGuard`; callers never construct it.
#[allow(unnameable_types)]
pub struct DriverContextGuard;

impl Drop for DriverContextGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            let mut executor = current.handle.borrow_mut();
            assert!(executor.is_some(), "driver not set");
            *executor = None;
        });
    }
}
