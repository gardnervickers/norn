use std::cell::UnsafeCell;

use crate::Handle;

thread_local! {
    static CURRENT: Context = Context::new();
}

pub(crate) struct Context {
    handle: UnsafeCell<Option<Handle>>,
}

impl Context {
    fn new() -> Self {
        Self {
            handle: UnsafeCell::new(None),
        }
    }

    pub(crate) fn enter(handle: Handle) -> ContextGuard {
        CURRENT.with(|current| {
            // Safety: This context is thread-local and only accessed on the
            // current thread.
            let old = unsafe { &mut *current.handle.get() };
            assert!(old.is_none(), "executor already set");
            *old = Some(handle);
        });
        ContextGuard {}
    }

    /// Returns a reference to the current executor.
    pub(crate) fn handle() -> Option<Handle> {
        CURRENT.with(|c| {
            // Safety: See [`Context::enter`].
            unsafe { (*c.handle.get()).clone() }
        })
    }
}

#[derive(Debug)]
pub struct ContextGuard;

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            // Safety: See [`Context::enter`].
            let handle = unsafe { &mut *current.handle.get() };
            assert!(handle.is_some(), "timer not set");
            *handle = None;
        });
    }
}
