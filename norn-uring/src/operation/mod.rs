use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll, Waker};
use std::{io, mem};

mod header;
mod raw;

use io_uring::types::CancelBuilder;
pub(crate) use raw::{CQEResult, RawOpRef};

use io_uring::squeue::Flags;
use smallvec::SmallVec;

use crate::driver::PushFuture;
use crate::error::SubmitError;

/// Low-level request customization for advanced io_uring users.
pub trait Operation {
    /// Configure a new [`io_uring::squeue::Entry`] for this operation.
    ///
    /// Self will be pinned for the duration of the operation.
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry;

    /// Called (potentially multiple) times when the operation is dropped by the application.
    ///
    /// This should be used to cleanup resources created by the kernel, such as buffers and
    /// file descriptors.
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
                State::Prepared { .. } | State::Waiting { .. } | State::Done => {}
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
    fn prepare_batch(&mut self, batch: &mut SmallVec<[ConfiguredEntry; 4]>) {
        let state = mem::replace(self, State::Done);
        *self = match state {
            State::Prepared {
                mut handle,
                mut entry,
            } => {
                batch.push(entry.take().expect("entry already prepared"));
                State::Waiting {
                    handle: Some(handle.take().expect("handle missing")),
                }
            }
            state => state,
        };
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
                    .complete(CQEResult::new(Err(err.to_io_error()), 0));
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
        let data =
            // Safety: We will not move `data` after this point, so it is safe to create a pin. The only time
            // we will move `data` is when the operation is complete. That will only happen after all sqes have
            // been submitted and completed.
            unsafe { Pin::new_unchecked(handle.data_mut().expect("operation already completed")) };

        let entry = T::configure(data);
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

    pub(crate) fn handle(&self) -> &crate::Handle {
        &self.reactor
    }

    pub(crate) fn prepare_batch(
        mut self: Pin<&mut Self>,
        batch: &mut SmallVec<[ConfiguredEntry; 4]>,
    ) {
        let this = self.as_mut().project();
        this.state.prepare_batch(batch);
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
            if this.submit.is_none() && matches!(this.state, State::Prepared { .. }) {
                let mut batch = SmallVec::new();
                this.state.prepare_batch(&mut batch);
                this.submit.set(Some(this.reactor.push_batch(batch)));
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

    fn take_completions(&self) -> VecDeque<CQEResult> {
        let handle = self.untyped();
        let mut completions = handle.header().completions().borrow_mut();
        mem::take(&mut *completions)
    }

    fn pop_completion(&self) -> Option<CQEResult> {
        let handle = self.untyped();
        let mut completions = handle.header().completions().borrow_mut();
        completions.pop_front()
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

/// Complete an operations.
///
/// ### Safety
///
/// This should only be called by the reactor when it reaps a completion. It should not
/// be called multiple times for the same completion.
#[inline]
pub(crate) unsafe fn complete_operation(entry: &io_uring::cqueue::Entry) {
    assert!(entry.user_data() > 1024);
    let handle = RawOpRef::from_raw_usize(entry.user_data() as usize);
    let result = entry.result();
    let result = if result >= 0 {
        Ok(result as u32)
    } else {
        Err(io::Error::from_raw_os_error(-result))
    };
    let result = CQEResult::new(result, entry.flags());
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
    use std::cell::Cell;
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::Poll;

    use super::*;

    #[derive(Debug, Default)]
    struct TestOp(Vec<CQEResult>);
    impl Operation for TestOp {
        fn cleanup(&mut self, result: CQEResult) {
            self.0.push(result);
        }

        fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
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

    impl Operation for TestMultishot {
        fn cleanup(&mut self, _: CQEResult) {}

        fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
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
    fn submit_failure_returns_error_instead_of_panicking() {
        #[derive(Debug)]
        struct SubmitFailureOp;

        impl Operation for SubmitFailureOp {
            fn cleanup(&mut self, _: CQEResult) {}

            fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
                io_uring::opcode::Nop::new().build()
            }
        }

        impl Singleshot for SubmitFailureOp {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
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
            Poll::Ready(Err(_)) => {}
            other => panic!("expected ready error, got: {other:?}"),
        }
    }

    #[test]
    fn multishot_terminal_completion_is_not_sent_to_update() {
        #[derive(Debug)]
        struct TerminalMultishot {
            updates: Rc<Cell<usize>>,
            complete: Rc<Cell<usize>>,
        }

        impl Operation for TerminalMultishot {
            fn cleanup(&mut self, _: CQEResult) {}

            fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
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
