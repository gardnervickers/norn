use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::mem;
use std::ptr::NonNull;
use std::task::Waker;

use super::CQEResult;

/// A FIFO completion queue with an allocation-free common path.
///
/// Singleshot operations and multishot operations whose consumer keeps pace
/// store their sole pending completion inline. Once an operation has observed
/// a backlog, the deque retains its high-water capacity so a steady producer
/// and consumer do not repeatedly allocate.
pub(crate) struct CompletionQueue {
    storage: CompletionStorage,
}

enum CompletionStorage {
    Empty,
    One(CQEResult),
    Many(VecDeque<CQEResult>),
}

impl CompletionQueue {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            storage: CompletionStorage::Empty,
        }
    }

    #[inline]
    pub(crate) fn push(&mut self, result: CQEResult) {
        match &mut self.storage {
            CompletionStorage::Empty => self.storage = CompletionStorage::One(result),
            CompletionStorage::One(_) => {
                let CompletionStorage::One(first) =
                    mem::replace(&mut self.storage, CompletionStorage::Empty)
                else {
                    unreachable!()
                };
                let mut overflow = VecDeque::with_capacity(2);
                overflow.push_back(first);
                overflow.push_back(result);
                self.storage = CompletionStorage::Many(overflow);
            }
            CompletionStorage::Many(overflow) => overflow.push_back(result),
        }
    }

    #[inline]
    pub(crate) fn pop_front(&mut self) -> Option<CQEResult> {
        match &mut self.storage {
            CompletionStorage::Empty => None,
            CompletionStorage::One(_) => {
                let CompletionStorage::One(result) =
                    mem::replace(&mut self.storage, CompletionStorage::Empty)
                else {
                    unreachable!()
                };
                Some(result)
            }
            CompletionStorage::Many(overflow) => overflow.pop_front(),
        }
    }

    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub(crate) fn len(&self) -> usize {
        match &self.storage {
            CompletionStorage::Empty => 0,
            CompletionStorage::One(_) => 1,
            CompletionStorage::Many(overflow) => overflow.len(),
        }
    }
}

impl Default for CompletionQueue {
    fn default() -> Self {
        Self::new()
    }
}

pub(crate) enum CompletionQueueIntoIter {
    Inline(std::option::IntoIter<CQEResult>),
    Overflow(std::collections::vec_deque::IntoIter<CQEResult>),
}

impl Iterator for CompletionQueueIntoIter {
    type Item = CQEResult;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Inline(iter) => iter.next(),
            Self::Overflow(iter) => iter.next(),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match self {
            Self::Inline(iter) => iter.size_hint(),
            Self::Overflow(iter) => iter.size_hint(),
        }
    }
}

impl ExactSizeIterator for CompletionQueueIntoIter {}

impl IntoIterator for CompletionQueue {
    type Item = CQEResult;
    type IntoIter = CompletionQueueIntoIter;

    fn into_iter(self) -> Self::IntoIter {
        match self.storage {
            CompletionStorage::Empty => CompletionQueueIntoIter::Inline(None.into_iter()),
            CompletionStorage::One(result) => {
                CompletionQueueIntoIter::Inline(Some(result).into_iter())
            }
            CompletionStorage::Many(overflow) => {
                CompletionQueueIntoIter::Overflow(overflow.into_iter())
            }
        }
    }
}

/// Header is the first field in every operation. It is the handle
/// through which the reactor completes operations.
///
/// There will be multiple references to the header outstanding, so
/// it is important that all fields in the header support interior
/// mutability.
pub(crate) struct Header {
    refcount: Cell<usize>,
    waker: RefCell<Option<Waker>>,
    completions: RefCell<CompletionQueue>,
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

    /// Called after the SQE has been written but before the submission tail is published.
    ///
    /// # Safety
    /// Callers must ensure exclusive access to the operation data.
    pub(crate) on_submit: unsafe fn(NonNull<Header>) -> std::io::Result<()>,

    /// Roll back state changed by `on_submit` when the submission tail will not be published.
    ///
    /// # Safety
    /// Callers must ensure exclusive access to the operation data.
    pub(crate) rollback_submit: unsafe fn(NonNull<Header>),

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
            completions: RefCell::new(CompletionQueue::new()),
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
    pub(crate) fn completions(&self) -> &RefCell<CompletionQueue> {
        &self.completions
    }

    /// Returns a mutable reference to the completion list.
    pub(crate) fn completions_mut(&mut self) -> &mut RefCell<CompletionQueue> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn completion(value: u32) -> CQEResult {
        CQEResult::new(Ok(value), 0)
    }

    #[test]
    fn one_completion_stays_inline() {
        let mut queue = CompletionQueue::new();
        queue.push(completion(7));
        assert!(matches!(queue.storage, CompletionStorage::One(_)));
        assert_eq!(queue.pop_front().unwrap().into_result().unwrap(), 7);
        assert!(matches!(queue.storage, CompletionStorage::Empty));
    }

    #[test]
    fn overflow_is_fifo_and_retains_high_water_storage() {
        let mut queue = CompletionQueue::new();
        for value in 0..128 {
            queue.push(completion(value));
        }
        let capacity = match &queue.storage {
            CompletionStorage::Many(overflow) => overflow.capacity(),
            _ => panic!("completion backlog did not use overflow storage"),
        };

        for expected in 0..128 {
            assert_eq!(queue.pop_front().unwrap().into_result().unwrap(), expected);
        }
        assert!(queue.is_empty());
        assert_eq!(
            match &queue.storage {
                CompletionStorage::Many(overflow) => overflow.capacity(),
                _ => panic!("completion queue discarded overflow storage"),
            },
            capacity
        );
    }
}
