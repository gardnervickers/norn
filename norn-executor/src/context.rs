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
        ContextGuard { _private: () }
    }

    /// Install `handle` for executor teardown.
    ///
    /// Unlike [`Context::enter`], this tolerates an existing context, which
    /// is restored when the guard drops, and thread-local storage that has
    /// already been destroyed (during thread exit), in which case no context
    /// is installed.
    pub(crate) fn enter_for_shutdown(handle: Handle) -> ShutdownContextGuard {
        let prev = CURRENT.try_with(|current| {
            // Safety: See [`Context::enter`]. The previous handle is moved
            // out and dropped only after this borrow ends.
            let slot = unsafe { &mut *current.handle.get() };
            slot.replace(handle)
        });
        match prev {
            Ok(prev) => ShutdownContextGuard {
                entered: true,
                prev,
            },
            Err(_) => ShutdownContextGuard {
                entered: false,
                prev: None,
            },
        }
    }

    /// Returns a reference to the current executor.
    ///
    /// Returns `None` when no executor is set, including when thread-local
    /// storage has already been destroyed (a destructor running at thread exit).
    pub(crate) fn handle() -> Option<Handle> {
        CURRENT
            .try_with(|current| {
                // Safety: the slot is thread-local and this borrow ends before
                // any user code runs.
                unsafe { (*current.handle.get()).clone() }
            })
            .ok()
            .flatten()
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

/// Guard returned by [`Context::enter`]; clears the context when dropped.
///
/// `enter` runs once per `block_on`, so this guard is zero-sized and has
/// nothing to restore: `enter` asserts the slot was empty.
#[derive(Debug)]
pub(crate) struct ContextGuard {
    _private: (),
}

impl Drop for ContextGuard {
    fn drop(&mut self) {
        let _removed = CURRENT.try_with(|current| {
            // Safety: See [`Context::enter`]. The removed handle is returned
            // and dropped only after this borrow ends.
            let executor = unsafe { &mut *current.handle.get() };
            assert!(executor.is_some(), "executor not set");
            executor.take()
        });
    }
}

/// Guard returned by [`Context::enter_for_shutdown`]; restores the previous
/// context, if any, when dropped.
#[derive(Debug)]
pub(crate) struct ShutdownContextGuard {
    /// Whether this guard installed a handle that it must remove.
    entered: bool,
    /// The handle to restore when this guard drops.
    prev: Option<Handle>,
}

impl Drop for ShutdownContextGuard {
    fn drop(&mut self) {
        if !self.entered {
            return;
        }
        let prev = self.prev.take();
        let _removed = CURRENT.try_with(|current| {
            // Safety: See [`Context::enter`]. The removed handle is returned
            // and dropped only after this borrow ends.
            let executor = unsafe { &mut *current.handle.get() };
            assert!(executor.is_some(), "executor not set");
            std::mem::replace(executor, prev)
        });
    }
}
