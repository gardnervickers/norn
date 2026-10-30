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
        CURRENT.with(|current| {
            // Safety: the slot is thread-local and this borrow ends before
            // any user code runs.
            unsafe { (*current.handle.get()).clone() }
        })
    }

    /// Invoke `f` with the current executor handle.
    ///
    /// The handle is cloned out of the slot first. `f` can run user
    /// destructors (for example, spawning onto a closed queue cancels the
    /// future), and those may enter or leave another executor context,
    /// which takes a mutable borrow of this slot.
    pub(crate) fn with_handle<T>(f: impl FnOnce(&Handle) -> T) -> Option<T> {
        Self::handle().as_ref().map(f)
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
