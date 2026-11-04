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
            assert!(
                old.is_none(),
                "an executor is already running on this thread; `block_on` cannot be nested"
            );
            *old = Some(handle);
        });
        ContextGuard {
            entered: true,
            prev: None,
        }
    }

    /// Install `handle` for executor teardown.
    ///
    /// Unlike [`Context::enter`], this tolerates an existing context, which
    /// is restored when the guard drops, and thread-local storage that has
    /// already been destroyed (during thread exit), in which case no context
    /// is installed.
    pub(crate) fn enter_for_shutdown(handle: Handle) -> ContextGuard {
        let prev = CURRENT.try_with(|current| {
            // Safety: See [`Context::enter`]. The previous handle is moved
            // out and dropped only after this borrow ends.
            let slot = unsafe { &mut *current.handle.get() };
            slot.replace(handle)
        });
        match prev {
            Ok(prev) => ContextGuard {
                entered: true,
                prev,
            },
            Err(_) => ContextGuard {
                entered: false,
                prev: None,
            },
        }
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
pub(crate) struct ContextGuard {
    /// Whether this guard installed a handle that it must remove.
    entered: bool,
    /// The handle to restore when this guard drops.
    prev: Option<Handle>,
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        if !self.entered {
            return;
        }
        let prev = self.prev.take();
        let _current = CURRENT.try_with(|current| {
            // Safety: See [`Context::enter`]. The removed handle is returned
            // and dropped only after this borrow ends.
            let executor = unsafe { &mut *current.handle.get() };
            assert!(executor.is_some(), "executor not set");
            std::mem::replace(executor, prev)
        });
    }
}
