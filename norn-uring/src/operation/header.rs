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
    queue: Cell<QueueState>,
    /// Whether the link from this operation to the next one in its batch is
    /// an `IO_HARDLINK`.
    hard_link: Cell<bool>,
    pub(crate) vtable: &'static VTable,
}

/// Whether an operation's entry waits in the driver's overflow queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueState {
    /// The entry is not in the overflow queue: it is unconfigured, in the SQ
    /// or the kernel, or done.
    Idle,
    /// The entry waits in the overflow queue for SQ space.
    Queued,
    /// The operation was cancelled while its entry waited there, so the entry
    /// completes with `ECANCELED` instead of reaching the kernel.
    Cancelled,
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
            queue: Cell::new(QueueState::Idle),
            hard_link: Cell::new(false),
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

    pub(crate) fn queue_state(&self) -> QueueState {
        self.queue.get()
    }

    pub(crate) fn set_queue_state(&self, state: QueueState) {
        self.queue.set(state);
    }

    pub(crate) fn hard_link(&self) -> bool {
        self.hard_link.get()
    }

    pub(crate) fn set_hard_link(&self) {
        self.hard_link.set(true);
    }

    /// Take the waker from the header.
    pub(crate) fn take_waker(&self) -> Option<Waker> {
        self.waker.borrow_mut().take()
    }

    /// Set the waker for the header.
    ///
    /// Existing wakers will be overwritten.
    pub(crate) fn set_waker(&self, waker: &Waker) {
        let new = waker.clone();
        // Dropping a waker runs arbitrary code that may re-enter this header,
        // so release the borrow before the replaced waker is dropped.
        let old = self.waker.borrow_mut().replace(new);
        drop(old);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::task::Wake;

    use super::*;

    static VTABLE: VTable = VTable {
        drop_ref: |_| {},
        clone_ref: |_| {},
        reap: |_, _| {},
    };

    struct ReentrantDrop {
        header: *const Header,
        drops: Arc<AtomicUsize>,
    }

    // Safety: the test waker is only dropped synchronously on the thread that
    // owns `header`, which outlives it.
    unsafe impl Send for ReentrantDrop {}
    unsafe impl Sync for ReentrantDrop {}

    // The drop-time re-entry is the point of this waker.
    #[allow(clippy::manual_noop_waker)]
    impl Wake for ReentrantDrop {
        fn wake(self: Arc<Self>) {}
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::Relaxed);
            // Re-enter the header while its old waker is being dropped.
            drop(unsafe { (*self.header).take_waker() });
        }
    }

    #[test]
    fn replaced_waker_is_dropped_outside_the_waker_borrow() {
        let header = Header::new(&VTABLE);
        let drops = Arc::new(AtomicUsize::new(0));
        let old = Waker::from(Arc::new(ReentrantDrop {
            header: &header,
            drops: Arc::clone(&drops),
        }));
        let (new, _) = futures_test::task::new_count_waker();

        header.set_waker(&old);
        drop(old);
        header.set_waker(&new);

        assert_eq!(drops.load(Ordering::Relaxed), 1);
        assert!(
            header.take_waker().is_none(),
            "the old waker was not dropped after the new one was stored"
        );
    }
}
