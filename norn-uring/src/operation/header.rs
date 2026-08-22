use std::cell::{Cell, RefCell};
use std::ptr::NonNull;
use std::task::Waker;

use super::CQEResult;

/// The header is the first field in every operation. The reactor uses it to
/// route completions without knowing the operation's concrete type.
///
/// Multiple references to the header may be outstanding, so all state uses
/// interior mutability.
pub(crate) struct Header {
    refcount: Cell<usize>,
    waker: RefCell<Option<Waker>>,
    complete: Cell<bool>,
    pub(crate) vtable: &'static VTable,
}

pub(crate) struct VTable {
    /// Called when a handle to the [`Header`] is dropped.
    ///
    /// The implementation must call [`Header::dec_refcount`] and destroy the
    /// operation only when the last reference is dropped.
    ///
    /// # Safety
    ///
    /// The pointer must reference a live [`Header`].
    pub(crate) drop_ref: unsafe fn(NonNull<Header>),

    /// Called when a handle to the [`Header`] is cloned.
    ///
    /// The implementation must call [`Header::inc_refcount`].
    ///
    /// # Safety
    ///
    /// The pointer must reference a live [`Header`].
    pub(crate) clone_ref: unsafe fn(NonNull<Header>),

    /// Called when a kernel or synthetic completion is reaped for the operation.
    ///
    /// An operation may receive multiple completions. [`CQEResult::more`]
    /// reports whether another completion will follow.
    ///
    /// If [`CQEResult::more`] returns false, the implementation must call
    /// [`Header::set_complete`] after queuing the owned completion.
    ///
    /// # Safety
    ///
    /// The pointer must reference the operation that produced `result`, and the
    /// completion must not have been reaped before.
    pub(crate) reap: unsafe fn(NonNull<Header>, result: CQEResult),
}

impl Header {
    /// Create a new [`Header`] with the given vtable.
    ///
    /// The header will have a refcount of 1 initially.
    pub(crate) fn new(vtable: &'static VTable) -> Self {
        Self {
            refcount: Cell::new(1),
            waker: Default::default(),
            complete: Cell::new(false),
            vtable,
        }
    }

    /// Increment the refcount of the header.
    pub(crate) fn inc_refcount(&self) {
        assert!(self.refcount.get() > 0);
        self.refcount.set(self.refcount.get() + 1);
    }

    /// Decrement the refcount of the header.
    ///
    /// Returns `true` if the refcount is now zero.
    pub(crate) fn dec_refcount(&self) -> bool {
        assert!(self.refcount.get() > 0);
        self.refcount.set(self.refcount.get() - 1);
        self.refcount.get() == 0
    }

    /// Returns the current refcount of the header.
    pub(crate) fn refcount(&self) -> usize {
        self.refcount.get()
    }

    /// Returns true if there are no more completions to be received.
    pub(crate) fn is_complete(&self) -> bool {
        self.complete.get()
    }

    /// Set the complete flag.
    ///
    /// # Safety
    ///
    /// The terminal completion must already be queued, and [`CQEResult::more`]
    /// must have returned false for it.
    pub(crate) unsafe fn set_complete(&self) {
        self.complete.set(true);
    }

    /// Take the waker from the header.
    pub(crate) fn take_waker(&self) -> Option<Waker> {
        self.waker.borrow_mut().take()
    }

    /// Set the waker for the header.
    ///
    /// Existing wakers will be overwritten.
    pub(crate) fn set_waker(&self, waker: &Waker) {
        *self.waker.borrow_mut() = Some(waker.clone());
    }
}
