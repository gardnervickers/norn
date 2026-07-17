use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Poll, RawWaker, RawWakerVTable, Waker};

use crate::park::Unpark;

pub(super) type RootNotifier<U> = Arc<Notifier<U>>;

pub(super) fn root_notifier<U>(unparker: U) -> RootNotifier<U> {
    Arc::new(Notifier {
        generation: AtomicUsize::new(1),
        owner: std::thread::current().id(),
        unparker,
    })
}

pub(super) struct Notifier<U> {
    generation: AtomicUsize,
    owner: std::thread::ThreadId,
    unparker: U,
}

impl<U> Notifier<U>
where
    U: Unpark,
{
    fn notify_local(&self) {
        self.generation.fetch_add(1, Ordering::Release);
    }

    fn notify_owned(&self) {
        self.notify_local();
        if self.owner != std::thread::current().id() {
            self.unparker.unpark();
        }
    }
}

struct WakerVTable<U>(PhantomData<U>);

impl<U> WakerVTable<U>
where
    U: Unpark + Send + Sync + 'static,
{
    const BORROWED_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_owned,
        Self::wake_borrowed,
        Self::wake_borrowed_by_ref,
        Self::drop_borrowed,
    );
    const OWNED_VTABLE: RawWakerVTable = RawWakerVTable::new(
        Self::clone_owned,
        Self::wake_owned,
        Self::wake_owned_by_ref,
        Self::drop_owned,
    );

    unsafe fn clone_owned(ptr: *const ()) -> RawWaker {
        Arc::increment_strong_count(ptr.cast::<Notifier<U>>());
        RawWaker::new(ptr, &Self::OWNED_VTABLE)
    }

    unsafe fn wake_borrowed(ptr: *const ()) {
        (*ptr.cast::<Notifier<U>>()).notify_local();
    }

    unsafe fn wake_borrowed_by_ref(ptr: *const ()) {
        (*ptr.cast::<Notifier<U>>()).notify_local();
    }

    unsafe fn drop_borrowed(_: *const ()) {}

    unsafe fn wake_owned(ptr: *const ()) {
        let notifier = Arc::from_raw(ptr.cast::<Notifier<U>>());
        notifier.notify_owned();
    }

    unsafe fn wake_owned_by_ref(ptr: *const ()) {
        let notifier = ManuallyDrop::new(Arc::from_raw(ptr.cast::<Notifier<U>>()));
        notifier.notify_owned();
    }

    unsafe fn drop_owned(ptr: *const ()) {
        Arc::decrement_strong_count(ptr.cast::<Notifier<U>>());
    }
}

unsafe fn borrowed_waker<U>(notifier: &RootNotifier<U>) -> ManuallyDrop<Waker>
where
    U: Unpark + Send + Sync + 'static,
{
    let raw = RawWaker::new(
        Arc::as_ptr(notifier).cast(),
        &WakerVTable::<U>::BORROWED_VTABLE,
    );
    ManuallyDrop::new(Waker::from_raw(raw))
}

/// [`FutureHarness`] wraps a pinned future
/// with a waker and provides a way to poll it.
pub(super) struct FutureHarness<'a, F, U> {
    task: Pin<&'a mut F>,
    poll_root: Arc<Notifier<U>>,
    waker: ManuallyDrop<Waker>,
    observed_generation: usize,
}

impl<'a, F, U> FutureHarness<'a, F, U>
where
    F: Future,
    U: Unpark + Send + Sync + 'static,
{
    /// Construct a new [`FutureHarness`] from a pinned future.
    pub(crate) fn new(future: Pin<&'a mut F>, poll_root: RootNotifier<U>) -> Self {
        let observed_generation = poll_root.generation.load(Ordering::Relaxed).wrapping_sub(1);
        // Safety: `poll_root` keeps the allocation alive for the harness, and
        // the borrowed waker is not dropped. Owned clones use the Arc-backed
        // vtable and therefore keep the allocation alive independently.
        let waker = unsafe { borrowed_waker(&poll_root) };
        Self {
            task: future,
            poll_root,
            waker,
            observed_generation,
        }
    }

    /// Attempt to poll the inner future, returning the result if ready.
    pub(crate) fn try_poll(&mut self) -> Option<F::Output> {
        let generation = self.poll_root.generation.load(Ordering::Acquire);
        if generation == self.observed_generation {
            return None;
        }
        self.observed_generation = generation;
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
        self.poll_root.generation.load(Ordering::Acquire) != self.observed_generation
    }

    /// Reclaim the root notifier when no clone of this root future's waker
    /// escaped. An escaped waker retains its own notifier generation so waking
    /// it cannot spuriously notify a later `block_on` call.
    pub(crate) fn into_reusable_notifier(self) -> Option<RootNotifier<U>> {
        let Self {
            task: _,
            poll_root,
            waker: _,
            observed_generation: _,
        } = self;
        (Arc::strong_count(&poll_root) == 1).then_some(poll_root)
    }
}
