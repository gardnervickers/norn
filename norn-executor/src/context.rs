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
        Self::with_handle(Clone::clone)
    }

    /// Invoke `f` with the current executor handle without cloning it.
    pub(crate) fn with_handle<T>(f: impl FnOnce(&Handle) -> T) -> Option<T> {
        CURRENT.with(|current| {
            // Safety: the slot is thread-local. The context guard cannot clear
            // it until this closure returns, and `f` receives shared access.
            unsafe { (&*current.handle.get()).as_ref().map(f) }
        })
    }
}

#[derive(Debug)]
pub(crate) struct ContextGuard;

impl Drop for ContextGuard {
    fn drop(&mut self) {
        CURRENT.with(|current| {
            // Safety: See [`Context::enter`].
            let executor = unsafe { &mut *current.handle.get() };
            assert!(executor.is_some(), "executor not set");
            *executor = None;
        });
    }
}
