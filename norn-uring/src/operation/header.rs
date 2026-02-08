use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::ptr::NonNull;
use std::task::Waker;

use super::CQEResult;

/// Header is the first field in every operation. It is the handle
/// through which the reactor completes operations.
///
/// There will be multiple references to the header outstanding, so
/// it is important that all fields in the header support interior
/// mutability.
pub(crate) struct Header {
    refcount: Cell<usize>,
    waker: RefCell<Option<Waker>>,
    completions: RefCell<VecDeque<CQEResult>>,
    complete: Cell<bool>,
    pub(crate) vtable: &'static VTable,
}

pub(crate) struct VTable {
    /// Called when a handle to the [`Header`] is dropped.
    ///
    /// This should call [`Header::dec_refcount`] and obey
    /// the return value. Only dropping the operation if
    /// the last reference was dropped.
    ///
    /// # Safety:
    /// Callers must ensure that the pointer is valid and points
    /// to a valid [`Header`].
    pub(crate) drop_ref: unsafe fn(NonNull<Header>),

    /// Called when a handle to the [`Header`] is cloned.
    ///
    /// This should call [`Header::inc_refcount`].
    ///
    /// # Safety:
    /// Callers must ensure that the pointer is valid and points
    /// to a valid [`Header`].
    pub(crate) clone_ref: unsafe fn(NonNull<Header>),

    /// Called when a completion is received for the operation.
    ///
    /// Note that an operation may receive multiple completions.
    /// The CQEResult more flag will be set to indicate if there
    /// are additional completions.
    ///
    /// If CQEResult::more returns false, ensure that Header::set_complete
    /// is called.
    ///
    /// # Safety:
    /// Callers must ensure that the pointer is valid and points
    /// to a valid [`Header`].
    pub(crate) complete: unsafe fn(NonNull<Header>, result: CQEResult) -> bool,
}

impl Header {
    /// Create a new [`Header`] with the given vtable.
    ///
    /// The header will have a refcount of 1 initially.
    pub(crate) fn new(vtable: &'static VTable) -> Self {
        Self {
            refcount: Cell::new(1),
            waker: Default::default(),
            completions: RefCell::new(VecDeque::new()),
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

    /// Returns a reference to the completion list.
    pub(crate) fn completions(&self) -> &RefCell<VecDeque<CQEResult>> {
        &self.completions
    }

    /// Returns a mutable reference to the completion list.
    pub(crate) fn completions_mut(&mut self) -> &mut RefCell<VecDeque<CQEResult>> {
        &mut self.completions
    }

    /// Returns true if there are no more completions to be received.
    ///
    /// This should be called
    pub(crate) fn is_complete(&self) -> bool {
        self.complete.get()
    }

    /// Set the complete flag.
    ///
    /// # Safety
    /// This should **only** be called if CQEResult::more returns false.
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
