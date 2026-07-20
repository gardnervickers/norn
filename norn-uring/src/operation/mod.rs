use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll, Waker};
use std::{io, mem};

mod header;
mod raw;

use io_uring::types::CancelBuilder;
pub use raw::CQEResult;
pub(crate) use raw::RawOpRef;

use io_uring::squeue::Flags;
use smallvec::SmallVec;

use crate::driver::{PushFuture, TryPush};
use crate::error::SubmitError;
use header::CompletionQueue;

/// Low-level request customization for advanced io_uring users.
///
/// # Safety
///
/// Implementing this trait asserts that every entry returned by [`Operation::configure`]
/// remains valid for the complete lifetime of the kernel operation. In particular:
///
/// - every pointer, file descriptor, fixed-resource index, and other resource referenced by
///   the entry must remain valid for every access the opcode permits, through the terminal CQE;
/// - memory that the kernel may read or write must remain allocated at a stable address and
///   must obey Rust's aliasing rules for the entire period of kernel access. An implementation
///   must not expose references that conflict with those accesses;
/// - the operation must account for every CQE the entry can produce. The runtime treats the
///   first CQE without `IORING_CQE_F_MORE` as terminal, so the entry must not permit any later
///   CQE or kernel access associated with the operation;
/// - requesting cancellation does not end the operation's lifetime. All referenced resources
///   must remain valid until the original operation produces its terminal CQE; and
/// - [`Operation::cleanup`] must correctly dispose of resources represented by each unconsumed
///   CQE. It may be called more than once and must handle success, failure, and cancellation
///   results without double-freeing or otherwise invalidating resources.
///
/// The runtime places the operation at a stable address before calling
/// [`Operation::configure`] and does not move it while the entry may be submitted or accessed by
/// the kernel. After the operation is known not to be in flight, it may be moved into its
/// completion handler. The runtime overwrites the SQE's `user_data` field for its own tracking
/// and cannot verify any of the requirements above.
pub unsafe trait Operation {
    /// Configure a new [`io_uring::squeue::Entry`] for this operation.
    ///
    /// Configuration failures are delivered through the operation's normal completion path;
    /// the operation is not submitted to the kernel.
    ///
    /// The address of `self` remains stable while the returned entry may be accessed by the
    /// kernel. Implementations may store pointers to fields of `self` in the entry, but must not
    /// invalidate the pointed-to storage during that period.
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry>;

    /// Release resources represented by an unconsumed completion.
    ///
    /// When an application drops a submitted operation, the runtime continues reaping its
    /// completions. Once the terminal completion has been reaped, this method is called once
    /// for each completion the application did not consume, in completion order. It can
    /// therefore be called multiple times for one operation. It is also called with a synthetic
    /// error completion when configuration or submission fails; in either case the kernel never
    /// saw the entry.
    ///
    /// Implementations should use this hook to release per-completion resources created or
    /// selected by the kernel, such as provided buffers or file descriptors.
    fn cleanup(&mut self, result: CQEResult);
}

/// A singleshot request that resolves to one final output.
pub trait Singleshot: Operation {
    /// The value returned once the final completion is observed.
    type Output;

    /// Complete can be called multiple times in the case of a multi-shot operation.
    fn complete(self, result: CQEResult) -> Self::Output;

    /// Called when a cqe with the more flag set is received.
    fn update(&mut self, result: CQEResult) {
        panic!("unhandled update called on a singleshot operation: {result:?}. Implement update.")
    }
}

/// A multishot request that can yield many items from one submission.
pub trait Multishot: Operation {
    /// The item yielded by each completion.
    type Item;

    /// Handle a non-terminal completion.
    fn update(&mut self, result: CQEResult) -> Self::Item;

    /// Called when the final completion for this operation is received.
    ///
    /// The final completion is identified by `!result.more()`.
    fn complete(self, result: CQEResult) -> Option<Self::Item>
    where
        Self: Sized,
    {
        debug_assert!(!result.more());
        let _ = result;
        None
    }
}

pub(crate) struct ConfiguredEntry {
    entry: io_uring::squeue::Entry,
    handle: RawOpRef,
}

impl ConfiguredEntry {
    pub(crate) fn into_entry_with_flags(self, flags: Flags) -> io_uring::squeue::Entry {
        self.entry
            .flags(flags)
            .user_data(self.handle.into_raw_usize() as u64)
    }

    pub(crate) fn new(handle: RawOpRef, entry: io_uring::squeue::Entry) -> Self {
        Self { entry, handle }
    }
}

pin_project_lite::pin_project! {
    /// A lazily-submitted io_uring operation.
    #[must_use = "future does nothing unless you `.await` or poll them"]
    pub struct Op<T>
    where
        T: 'static,
    {
        #[pin]
        submit: Option<PushFuture>,
        state: State<T>,
        reactor: crate::Handle,
        completed: bool,
    }

    impl<T> PinnedDrop for Op<T> where T: 'static {
        fn drop(me: Pin<&mut Self>) {
            let this = me.project();
            match this.state {
                State::Submitted { inner } => {
                    if !*this.completed {
                        let user_data = inner.inner.inner.as_raw_usize();
                        let criteria = CancelBuilder::user_data(user_data as u64);
                        let _ = this.reactor.cancel(criteria, false);
                    }
                }
                State::Prepared { .. }
                | State::ConfigureFailed { .. }
                | State::Waiting { .. }
                | State::Done => {}
            }
        }
    }
}

enum State<T>
where
    T: 'static,
{
    Prepared {
        handle: Option<TypedHandle<T>>,
        entry: Option<ConfiguredEntry>,
    },
    ConfigureFailed {
        handle: Option<TypedHandle<T>>,
        error: Option<io::Error>,
    },
    Waiting {
        handle: Option<TypedHandle<T>>,
    },
    Submitted {
        inner: SubmittedOp<T>,
    },
    Done,
}

impl<T> State<T>
where
    T: Operation + 'static,
{
    fn prepare_batch(&mut self, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let state = mem::replace(self, State::Done);
        match state {
            State::Prepared {
                mut handle,
                mut entry,
            } => {
                let entry = entry.take().expect("entry already prepared");
                *self = State::Waiting {
                    handle: Some(handle.take().expect("handle missing")),
                };
                batch.push(entry);
                true
            }
            State::ConfigureFailed {
                mut handle,
                mut error,
            } => {
                let handle = handle.take().expect("handle missing");
                handle.untyped().complete(CQEResult::synthetic(Err(error
                    .take()
                    .expect("configuration error missing"))));
                *self = State::Submitted {
                    inner: SubmittedOp { inner: handle },
                };
                false
            }
            state => {
                *self = state;
                false
            }
        }
    }

    fn start_submit(&mut self) -> Option<ConfiguredEntry> {
        // Keep ordinary submissions on the direct path. Linked requests use
        // `prepare_batch`, which carries the configuration-failure semantics.
        let state = mem::replace(self, State::Done);
        match state {
            State::Prepared {
                mut handle,
                mut entry,
            } => {
                let entry = entry.take().expect("entry missing");
                *self = State::Waiting {
                    handle: Some(handle.take().expect("handle missing")),
                };
                Some(entry)
            }
            state => {
                *self = state;
                None
            }
        }
    }

    fn cancel_unsubmitted(&mut self) -> bool {
        let state = mem::replace(self, State::Done);
        let handle = match state {
            State::Prepared { mut handle, entry } => {
                drop(entry);
                handle.take().expect("handle missing")
            }
            State::ConfigureFailed { mut handle, error } => {
                drop(error);
                handle.take().expect("handle missing")
            }
            state => {
                *self = state;
                return false;
            }
        };
        handle
            .untyped()
            .complete(CQEResult::synthetic(Err(io::Error::from_raw_os_error(
                libc::ECANCELED,
            ))));
        *self = State::Submitted {
            inner: SubmittedOp { inner: handle },
        };
        true
    }

    fn finish_submit(&mut self) {
        let state = mem::replace(self, State::Done);
        *self = match state {
            State::Waiting { mut handle } => State::Submitted {
                inner: SubmittedOp {
                    inner: handle.take().expect("handle missing"),
                },
            },
            state => state,
        };
    }

    fn fail_submit(&mut self, err: &SubmitError) {
        let state = mem::replace(self, State::Done);
        *self = match state {
            State::Waiting { mut handle } => {
                let handle = handle.take().expect("handle missing");
                handle
                    .untyped()
                    .complete(CQEResult::synthetic(Err(err.to_io_error())));
                State::Submitted {
                    inner: SubmittedOp { inner: handle },
                }
            }
            state => state,
        };
    }
}

impl<T> Op<T>
where
    T: Operation + 'static,
{
    pub(crate) fn new(data: T, reactor: crate::Handle) -> Self {
        let mut handle = TypedHandle::new(data);
        // Safety: The handle was just created and no other references to its operation data
        // exist. `RawOp` keeps the data at a stable address until the operation completes.
        let data = unsafe { handle.data_mut().expect("operation already completed") };

        let entry = match T::configure(data) {
            Ok(entry) => entry,
            Err(err) => return Self::configure_failed(handle, err, reactor),
        };
        let entry = ConfiguredEntry::new(handle.untyped(), entry);

        Self {
            submit: None,
            state: State::Prepared {
                handle: Some(handle),
                entry: Some(entry),
            },
            reactor,
            completed: false,
        }
    }

    #[cold]
    #[inline(never)]
    fn configure_failed(handle: TypedHandle<T>, error: io::Error, reactor: crate::Handle) -> Self {
        Self {
            submit: None,
            state: State::ConfigureFailed {
                handle: Some(handle),
                error: Some(error),
            },
            reactor,
            completed: false,
        }
    }

    pub(crate) fn handle(&self) -> &crate::Handle {
        &self.reactor
    }

    pub(crate) fn prepare_batch(
        mut self: Pin<&mut Self>,
        batch: &mut SmallVec<[ConfiguredEntry; 4]>,
    ) -> bool {
        let this = self.as_mut().project();
        let can_continue = this.state.prepare_batch(batch);
        if !can_continue {
            *this.completed = true;
        }
        can_continue
    }

    pub(crate) fn cancel_unsubmitted(mut self: Pin<&mut Self>) {
        let this = self.as_mut().project();
        if this.state.cancel_unsubmitted() {
            *this.completed = true;
        }
    }

    pub(crate) fn finish_submit(mut self: Pin<&mut Self>) {
        let this = self.as_mut().project();
        this.state.finish_submit();
    }

    pub(crate) fn fail_submit(mut self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.as_mut().project();
        this.state.fail_submit(err);
    }

    pub(crate) fn cancel_unfinished(mut self: Pin<&mut Self>) {
        let this = self.as_mut().project();
        if let State::Submitted { inner } = this.state {
            if !*this.completed {
                let user_data = inner.inner.inner.as_raw_usize();
                let criteria = CancelBuilder::user_data(user_data as u64);
                let _ = self.reactor.cancel(criteria, false);
            }
        }
    }

    fn poll_submit(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        {
            let mut this = self.as_mut().project();
            if this.submit.is_none() && matches!(this.state, State::ConfigureFailed { .. }) {
                let mut batch = SmallVec::new();
                let can_continue = this.state.prepare_batch(&mut batch);
                debug_assert!(!can_continue);
                debug_assert!(batch.is_empty());
                *this.completed = true;
            }
            if this.submit.is_none() && matches!(this.state, State::Prepared { .. }) {
                let entry = this
                    .state
                    .start_submit()
                    .expect("prepared operation missing entry");
                match this.reactor.try_push(entry) {
                    TryPush::Submitted => this.state.finish_submit(),
                    TryPush::Full(entry) => this.submit.set(Some(this.reactor.push(entry))),
                    TryPush::Failed(err) => this.state.fail_submit(&err),
                }
            }
        }

        let mut this = self.as_mut().project();
        let Some(fut) = this.submit.as_mut().as_pin_mut() else {
            return Poll::Ready(());
        };

        match ready!(fut.poll(cx)) {
            Ok(()) => this.state.finish_submit(),
            Err(err) => this.state.fail_submit(&err),
        }
        this.submit.set(None);
        Poll::Ready(())
    }
}

impl<T> Future for Op<T>
where
    T: Singleshot + 'static,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.as_mut().poll_submit(cx));
        let this = self.project();
        let State::Submitted { inner } = this.state else {
            unreachable!("operation not submitted");
        };
        if let Some(result) = inner.try_complete() {
            *this.completed = true;
            return Poll::Ready(result);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

impl<T> futures_core::Stream for Op<T>
where
    T: Multishot + 'static,
{
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ready!(self.as_mut().poll_submit(cx));
        let this = self.project();
        let State::Submitted { inner } = this.state else {
            unreachable!("operation not submitted");
        };
        if let Some(result) = inner.try_next() {
            return Poll::Ready(Some(result));
        }
        if inner.inner.is_complete() {
            *this.completed = true;
            return Poll::Ready(None);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

/// [`TypedHandle`] is a reference to an operation that is in progress.
pub(crate) struct TypedHandle<T> {
    inner: RawOpRef,
    _marker: std::marker::PhantomData<T>,
}

impl<T> TypedHandle<T>
where
    T: Operation + 'static,
{
    pub(crate) fn new(data: T) -> Self {
        let ptr = raw::RawOp::<T>::allocate(data);
        Self {
            inner: RawOpRef::from(ptr),
            _marker: std::marker::PhantomData,
        }
    }

    /// Returns an untyped [`Handle`] to this operation.
    ///
    /// The untyped handle can be used to complete the operation
    /// without knowing the type of the operation.
    #[inline]
    pub(crate) fn untyped(&self) -> RawOpRef {
        self.inner.clone()
    }

    fn take_completions(&self) -> CompletionQueue {
        let mut completions = self.inner.header().completions().borrow_mut();
        mem::take(&mut *completions)
    }

    fn pop_completion(&self) -> Option<CQEResult> {
        let mut completions = self.inner.header().completions().borrow_mut();
        if completions.is_empty() {
            None
        } else {
            Some(completions.remove(0))
        }
    }

    /// Returns true if this operation is complete.
    fn is_complete(&self) -> bool {
        self.inner.is_complete()
    }

    /// Returns a mutable reference to the data associated with this operation.
    unsafe fn data_mut(&mut self) -> Option<&mut T> {
        unsafe {
            let mut raw = raw::RawOp::<T>::from_raw_header(self.inner.inner());
            let opref = raw.as_mut();
            opref.data_mut().as_mut()
        }
    }

    /// Attempt to take the data from this operation.
    ///
    /// This will succeed if the operation is complete.
    ///
    /// # Safety
    /// Callers must ensure that there are no other references to the data (such as Self::data_mut).
    unsafe fn try_take(&self) -> Option<T> {
        if !self.is_complete() {
            return None;
        }
        unsafe {
            let mut raw = raw::RawOp::<T>::from_raw_header(self.inner.inner());
            let opref = raw.as_mut();
            opref.data_mut().take()
        }
    }

    fn register_waker(&self, waker: &Waker) {
        let header = self.inner.header();
        header.set_waker(waker);
    }
}

/// Complete an operation.
///
/// ### Safety
///
/// This should only be called by the reactor when it reaps a completion. It should not
/// be called multiple times for the same completion. `user_data` must be the raw
/// operation reference installed by this runtime.
#[inline]
pub(crate) unsafe fn complete_operation(
    user_data: u64,
    result: i32,
    flags: u32,
    extra: Option<[u64; 2]>,
) {
    assert!(user_data > 1024);
    let handle = RawOpRef::from_raw_usize(user_data as usize);
    let result = if result >= 0 {
        Ok(result as u32)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    };
    let result = match extra {
        Some(extra) => CQEResult::new_big(result, flags, extra),
        None => CQEResult::new(result, flags),
    };
    handle.complete(result);
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
pub(crate) struct SubmittedOp<T> {
    inner: TypedHandle<T>,
}

impl<T> SubmittedOp<T>
where
    T: Operation + 'static,
{
    fn try_complete(&mut self) -> Option<T::Output>
    where
        T: Singleshot,
    {
        if !self.inner.is_complete() {
            return None;
        }
        let results = self.inner.take_completions();
        let mut data = unsafe { self.inner.try_take() }.expect("operation already completed");
        let last_idx = results.len() - 1;
        for (idx, result) in results.into_iter().enumerate() {
            if idx == last_idx {
                assert!(!result.more());
                return Some(data.complete(result));
            } else {
                assert!(result.more());
                data.update(result);
            }
        }
        panic!("no final completion");
    }

    fn try_next(&mut self) -> Option<T::Item>
    where
        T: Multishot,
    {
        let completion = self.inner.pop_completion()?;
        if completion.more() {
            let data = unsafe { self.inner.data_mut() }.expect("operation already completed");
            return Some(data.update(completion));
        }
        let data = unsafe { self.inner.try_take() }.expect("operation already completed");
        data.complete(completion)
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::task::{Poll, Wake, Waker};

    use super::*;

    #[derive(Debug, Default)]
    struct TestOp(Vec<CQEResult>);
    unsafe impl Operation for TestOp {
        fn cleanup(&mut self, result: CQEResult) {
            self.0.push(result);
        }

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            unimplemented!()
        }
    }

    #[test]
    fn complete_op() {
        let mut op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        handle.complete(CQEResult::new(Ok(0), 0));

        assert!(op.is_complete());
        unsafe { assert!(op.data_mut().unwrap().0.is_empty()) };
    }

    #[test]
    fn complete_op_through_usize() {
        let mut op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        let handle_usize = handle.into_raw_usize();
        let handle = unsafe { RawOpRef::from_raw_usize(handle_usize) };
        handle.complete(CQEResult::new(Ok(0), 0));

        assert!(op.is_complete());
        unsafe { assert!(op.data_mut().unwrap().0.is_empty()) };
    }

    #[test]
    fn drop_op_through_usize() {
        let op = TypedHandle::new(TestOp::default());
        let handle = op.untyped();

        let handle_usize = handle.into_raw_usize();
        let handle = unsafe { RawOpRef::from_raw_usize(handle_usize) };
        drop(handle);
    }

    #[derive(Debug, Default)]
    struct TestMultishot;

    unsafe impl Operation for TestMultishot {
        fn cleanup(&mut self, _: CQEResult) {}

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            unimplemented!()
        }
    }

    impl Multishot for TestMultishot {
        type Item = u32;

        fn update(&mut self, result: CQEResult) -> Self::Item {
            result.result.unwrap()
        }

        fn complete(self, result: CQEResult) -> Option<Self::Item> {
            Some(result.result.unwrap())
        }
    }

    fn more_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::more(*flags))
            .expect("missing CQE more flag")
    }

    thread_local! {
        static WAKE_ACTION: RefCell<Option<Box<dyn FnMut()>>> = RefCell::new(None);
    }

    struct TestWake;

    impl Wake for TestWake {
        fn wake(self: Arc<Self>) {
            Self::run();
        }

        fn wake_by_ref(self: &Arc<Self>) {
            Self::run();
        }
    }

    impl TestWake {
        fn run() {
            WAKE_ACTION.with(|action| {
                action.borrow_mut().as_mut().expect("wake action missing")();
            });
        }
    }

    fn test_waker(action: impl FnMut() + 'static) -> Waker {
        WAKE_ACTION.with(|slot| {
            assert!(slot.borrow().is_none(), "wake action already installed");
            *slot.borrow_mut() = Some(Box::new(action));
        });
        Waker::from(Arc::new(TestWake))
    }

    fn clear_wake_action() {
        WAKE_ACTION.with(|slot| *slot.borrow_mut() = None);
    }

    #[test]
    fn multishot_completions_are_fifo() {
        let typed = TypedHandle::new(TestMultishot);
        let more = more_flag();
        typed.untyped().complete(CQEResult::new(Ok(10), more));
        typed.untyped().complete(CQEResult::new(Ok(20), more));
        typed.untyped().complete(CQEResult::new(Ok(30), 0));

        let mut submitted = SubmittedOp { inner: typed };

        assert_eq!(submitted.try_next(), Some(10));
        assert_eq!(submitted.try_next(), Some(20));
        assert_eq!(submitted.try_next(), Some(30));
        assert_eq!(submitted.try_next(), None);
        assert!(submitted.inner.is_complete());
    }

    #[test]
    fn multishot_more_completion_survives_panicking_waker() {
        let typed = TypedHandle::new(TestMultishot);
        let kernel_ref = typed.untyped().into_raw_usize();
        let waker = test_waker(|| panic!("wake panic"));
        typed.register_waker(&waker);

        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let kernel_ref = unsafe { RawOpRef::from_raw_usize(kernel_ref) };
            kernel_ref.complete(CQEResult::new(Ok(10), more_flag()));
        }));
        assert!(result.is_err());
        clear_wake_action();

        assert_eq!(typed.inner.header().refcount(), 2);

        let terminal_ref = unsafe { RawOpRef::from_raw_usize(kernel_ref) };
        terminal_ref.complete(CQEResult::new(Ok(20), 0));
        assert_eq!(typed.inner.header().refcount(), 1);

        let mut submitted = SubmittedOp { inner: typed };
        assert_eq!(submitted.try_next(), Some(10));
        assert_eq!(submitted.try_next(), Some(20));
        assert_eq!(submitted.try_next(), None);
    }

    #[test]
    fn multishot_waker_can_poll_synchronously() {
        let typed = TypedHandle::new(TestMultishot);
        let kernel_ref = typed.untyped().into_raw_usize();
        let submitted = Rc::new(RefCell::new(SubmittedOp { inner: typed }));
        let observed = Rc::new(Cell::new(None));
        let waker = test_waker({
            let submitted = Rc::clone(&submitted);
            let observed = Rc::clone(&observed);
            move || observed.set(submitted.borrow_mut().try_next())
        });
        submitted.borrow().inner.register_waker(&waker);

        let more_ref = unsafe { RawOpRef::from_raw_usize(kernel_ref) };
        more_ref.complete(CQEResult::new(Ok(10), more_flag()));
        clear_wake_action();

        assert_eq!(observed.get(), Some(10));
        assert_eq!(submitted.borrow().inner.inner.header().refcount(), 2);

        let terminal_ref = unsafe { RawOpRef::from_raw_usize(kernel_ref) };
        terminal_ref.complete(CQEResult::new(Ok(20), 0));
        assert_eq!(submitted.borrow_mut().try_next(), Some(20));
        assert_eq!(submitted.borrow_mut().try_next(), None);
        assert_eq!(submitted.borrow().inner.inner.header().refcount(), 1);
    }

    #[test]
    fn submit_failure_returns_error_instead_of_panicking() {
        #[derive(Debug)]
        struct SubmitFailureOp;

        unsafe impl Operation for SubmitFailureOp {
            fn cleanup(&mut self, _: CQEResult) {}

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }
        }

        impl Singleshot for SubmitFailureOp {
            type Output = (bool, io::Result<()>);

            fn complete(self, result: CQEResult) -> Self::Output {
                let synthetic = result.is_synthetic();
                (synthetic, result.result.map(drop))
            }
        }

        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        norn_executor::park::Park::shutdown(&mut driver);

        let mut op = std::pin::pin!(handle.submit(SubmitFailureOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let poll_result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Future::poll(op.as_mut(), &mut cx)
        }))
        .expect("poll should not panic");

        match poll_result {
            Poll::Ready((true, Err(_))) => {}
            other => panic!("expected ready error, got: {other:?}"),
        }
    }

    #[test]
    fn configuration_failure_completes_without_submitting() {
        #[derive(Debug)]
        struct ConfigurationFailureOp;

        unsafe impl Operation for ConfigurationFailureOp {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Err(io::Error::new(io::ErrorKind::InvalidInput, "invalid op"))
            }

            fn cleanup(&mut self, _: CQEResult) {}
        }

        impl Singleshot for ConfigurationFailureOp {
            type Output = (bool, io::Result<()>);

            fn complete(self, result: CQEResult) -> Self::Output {
                let synthetic = result.is_synthetic();
                (synthetic, result.result.map(drop))
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut op = std::pin::pin!(handle.submit(ConfigurationFailureOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        let Poll::Ready((synthetic, Err(err))) = Future::poll(op.as_mut(), &mut cx) else {
            panic!("configuration failure should complete immediately")
        };
        assert!(synthetic);
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn first_poll_submits_without_allocating_backpressure_future() {
        #[derive(Debug)]
        struct NopOp;

        unsafe impl Operation for NopOp {
            fn cleanup(&mut self, _: CQEResult) {}

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }
        }

        impl Singleshot for NopOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut op = std::pin::pin!(handle.submit(NopOp));
        let waker = futures_test::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);

        assert!(Future::poll(op.as_mut(), &mut cx).is_pending());
        let op = op.as_ref().get_ref();
        assert!(op.submit.is_none());
        assert!(matches!(op.state, State::Submitted { .. }));
    }

    #[test]
    fn multishot_terminal_completion_is_not_sent_to_update() {
        #[derive(Debug)]
        struct TerminalMultishot {
            updates: Rc<Cell<usize>>,
            complete: Rc<Cell<usize>>,
        }

        unsafe impl Operation for TerminalMultishot {
            fn cleanup(&mut self, _: CQEResult) {}

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                unimplemented!()
            }
        }

        impl Multishot for TerminalMultishot {
            type Item = ();

            fn update(&mut self, result: CQEResult) -> Self::Item {
                assert!(result.more(), "terminal completion must not call update");
                self.updates.set(self.updates.get() + 1);
            }

            fn complete(self, result: CQEResult) -> Option<Self::Item> {
                assert!(!result.more());
                self.complete.set(self.complete.get() + 1);
                None
            }
        }

        let updates = Rc::new(Cell::new(0));
        let complete = Rc::new(Cell::new(0));
        let typed = TypedHandle::new(TerminalMultishot {
            updates: Rc::clone(&updates),
            complete: Rc::clone(&complete),
        });
        typed.untyped().complete(CQEResult::new(
            Err(io::Error::from_raw_os_error(libc::ECANCELED)),
            0,
        ));

        let mut submitted = SubmittedOp { inner: typed };
        assert_eq!(submitted.try_next(), None);
        assert_eq!(updates.get(), 0);
        assert_eq!(complete.get(), 1);
        assert!(submitted.inner.is_complete());
    }
}
