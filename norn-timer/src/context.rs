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
                "a timer driver is already entered on this thread; drivers cannot be nested"
            );
            *old = Some(handle);
        });
        ContextGuard {
            entered: true,
            prev: None,
        }
    }

    /// Install `handle` for teardown.
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
        CURRENT.with(|c| {
            // Safety: See [`Context::enter`].
            unsafe { (*c.handle.get()).clone() }
        })
    }
}

#[derive(Debug)]
pub struct ContextGuard {
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
            let handle = unsafe { &mut *current.handle.get() };
            assert!(handle.is_some(), "timer not set");
            std::mem::replace(handle, prev)
        });
    }
}
