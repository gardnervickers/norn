use std::cell::Cell;
use std::future::Future;
use std::mem::{self, ManuallyDrop};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, RawWaker, RawWakerVTable, Waker};

pub(super) type RootNotifier = Rc<Cell<bool>>;

pub(super) fn root_notifier() -> RootNotifier {
    Rc::new(Cell::new(true))
}

/// [`FutureHarness`] wraps a pinned future
/// with a waker and provides a way to poll it.
pub(super) struct FutureHarness<'a, F> {
    task: Pin<&'a mut F>,
    poll_root: Rc<Cell<bool>>,
    waker: std::task::Waker,
}

impl<'a, F> FutureHarness<'a, F>
where
    F: Future,
{
    /// Construct a new [`FutureHarness`] from a pinned future.
    pub(crate) fn new(future: Pin<&'a mut F>, poll_root: RootNotifier) -> Self {
        poll_root.set(true);
        let waker = waker_fn(Rc::clone(&poll_root));
        Self {
            task: future,
            poll_root,
            waker,
        }
    }

    /// Attempt to poll the inner future, returning the result if ready.
    pub(crate) fn try_poll(&mut self) -> Option<F::Output> {
        if !self.is_notified() {
            return None;
        }
        self.poll_root.set(false);
        match self
            .task
            .as_mut()
            .poll(&mut std::task::Context::from_waker(&self.waker))
        {
            Poll::Ready(res) => Some(res),
            Poll::Pending => None,
        }
    }

    /// Returns true if the future is ready to be polled.
    pub(crate) fn is_notified(&self) -> bool {
        self.poll_root.get()
    }

    /// Reclaim the root notifier when no clone of this root future's waker
    /// escaped. An escaped waker retains its own notifier generation so waking
    /// it cannot spuriously notify a later `block_on` call.
    pub(crate) fn into_reusable_notifier(self) -> Option<RootNotifier> {
        let Self {
            task: _,
            poll_root,
            waker,
        } = self;
        drop(waker);
        (Rc::strong_count(&poll_root) == 1).then_some(poll_root)
    }
}

/// Creates a waker from a wake function.
///
/// The function gets called every time the waker is woken.
fn waker_fn(f: Rc<Cell<bool>>) -> Waker {
    let raw = Rc::into_raw(f).cast::<()>();
    let vtable = &Helper::VTABLE;
    unsafe { Waker::from_raw(RawWaker::new(raw, vtable)) }
}

#[derive(Clone)]
struct Helper();

impl Helper {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_waker,
        Self::wake,
        Self::wake_by_ref,
        Self::drop_waker,
    );

    #[allow(clippy::redundant_clone, clippy::forget_non_drop)]
    unsafe fn clone_waker(ptr: *const ()) -> RawWaker {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr.cast::<Cell<bool>>()));
        mem::forget(rc.clone());
        RawWaker::new(ptr, &Self::VTABLE)
    }

    unsafe fn wake(ptr: *const ()) {
        let rc = Rc::from_raw(ptr.cast::<Cell<bool>>());
        rc.set(true);
    }

    unsafe fn wake_by_ref(ptr: *const ()) {
        let rc = ManuallyDrop::new(Rc::from_raw(ptr.cast::<Cell<bool>>()));
        rc.set(true);
    }

    unsafe fn drop_waker(ptr: *const ()) {
        drop(Rc::from_raw(ptr.cast::<Cell<bool>>()));
    }
}
