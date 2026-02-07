use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll, Waker};
use std::{io, mem};

mod header;
mod raw;

use io_uring::types::CancelBuilder;
pub(crate) use raw::{CQEResult, RawOpRef};

use futures_core::Stream;

use crate::driver::PushFuture;

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

pub trait Singleshot: Operation {
    type Output;

    /// Complete can be called multiple times in the case of a multi-shot operation.
    fn complete(self, result: CQEResult) -> Self::Output;

    /// Called when a cqe with the more flag set is received.
    fn update(&mut self, result: CQEResult) {
        panic!("unhandled update called on a singleshot operation: {result:?}. Implement update.")
    }
}

pub trait Multishot: Operation {
    type Item;

    fn update(&mut self, result: CQEResult) -> Self::Item;
}

pub(crate) struct ConfiguredEntry {
    entry: io_uring::squeue::Entry,
    handle: RawOpRef,
}

impl ConfiguredEntry {
    pub(crate) fn into_entry(self) -> io_uring::squeue::Entry {
        self.entry.user_data(self.handle.into_raw_usize() as u64)
    }

    pub(crate) fn new(handle: RawOpRef, entry: io_uring::squeue::Entry) -> Self {
        Self { entry, handle }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "future does nothing unless you `.await` or poll them"]
    pub struct Op<T>
    where
        T: 'static,
    {
        #[pin]
        stage: Stage<T>,
        reactor: crate::Handle,
        completed: bool
    }

    impl<T> PinnedDrop for Op<T> where T: 'static {
        fn drop(me: Pin<&mut Self>) {
            let this = me.project();
            // Safety: We are not moving out of the pinned `this.stage` field.
            match unsafe {this.stage.get_unchecked_mut()} {
                Stage::Unsubmitted { .. } => {
                },
                Stage::Submitted { inner } => {
                    if !*this.completed {
                        // Ignore the result, it is possible that the future
                        // completed before or concurrently with the Op drop.
                        let user_data = inner.inner.inner.as_raw_usize();
                        let criteria = CancelBuilder::user_data(user_data as u64);
                        let _ = this.reactor.cancel(criteria, false);
                    }
                },
            }
        }
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
        let submit_future = reactor.push(entry);
        let inner = Stage::new(handle, submit_future);

        Self {
            stage: inner,
            reactor,
            completed: false,
        }
    }
}

impl<T> Future for Op<T>
where
    T: Singleshot + 'static,
{
    type Output = T::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        let res = ready!(this.stage.poll(cx));
        *this.completed = true;
        Poll::Ready(res)
    }
}

impl<T> futures_core::Stream for Op<T>
where
    T: Multishot + 'static,
{
    type Item = T::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let res = ready!(this.stage.poll_next(cx));
        if res.is_none() {
            *this.completed = true;
        }
        Poll::Ready(res)
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

    fn take_completions(&self) -> smallvec::SmallVec<[CQEResult; 4]> {
        let handle = self.untyped();
        let mut completions = handle.header().completions().borrow_mut();
        mem::take(&mut *completions)
    }

    fn pop_completion(&self) -> Option<CQEResult> {
        let handle = self.untyped();
        let mut completions = handle.header().completions().borrow_mut();
        completions.pop()
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

pin_project_lite::pin_project! {
    #[project = StageProj]
    enum Stage<T> where T: 'static {
        Unsubmitted{#[pin] unsubmitted: UnsubmittedOp<T>},
        Submitted{inner: SubmittedOp<T>},
    }
}

impl<T> Stage<T>
where
    T: Operation + 'static,
{
    fn new(handle: TypedHandle<T>, future: PushFuture) -> Self {
        Self::Unsubmitted {
            unsubmitted: UnsubmittedOp {
                handle: Some(handle),
                future,
            },
        }
    }

    fn poll_submit(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.as_mut().project();
        match this {
            StageProj::Unsubmitted { unsubmitted } => {
                let handle = ready!(unsubmitted.poll(cx));
                Pin::set(
                    &mut self,
                    Stage::Submitted {
                        inner: SubmittedOp { inner: handle },
                    },
                );
                Poll::Ready(())
            }
            StageProj::Submitted { .. } => Poll::Ready(()),
        }
    }
}

pin_project_lite::pin_project! {
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    struct UnsubmittedOp<T> where T: 'static {
        handle: Option<TypedHandle<T>>,
        #[pin]
        future: PushFuture,
    }
}

impl<T> Future for UnsubmittedOp<T>
where
    T: Operation + 'static,
{
    type Output = TypedHandle<T>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        if let Err(err) = ready!(this.future.poll(cx)) {
            // Submission failed (typically during driver shutdown). Synthesize a completion
            // error so the op follows normal completion/drop lifetimes instead of panicking.
            let op = this
                .handle
                .as_ref()
                .expect("operation handle must exist until submission completes")
                .untyped();
            op.complete(CQEResult::new(Err(err.into()), 0));
        }
        Poll::Ready(this.handle.take().unwrap())
    }
}

#[must_use = "futures do nothing unless you `.await` or poll them"]
struct SubmittedOp<T> {
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
        let data = unsafe { self.inner.data_mut() }.expect("operation already completed");
        Some(data.update(completion))
    }
}

impl<T> Future for Stage<T>
where
    T: Singleshot + 'static,
{
    type Output = T::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        ready!(self.as_mut().poll_submit(cx));
        let this = self.as_mut().project();
        let inner = match this {
            StageProj::Unsubmitted { .. } => unreachable!("unsubmitted"),
            StageProj::Submitted { inner } => inner,
        };
        if let Some(result) = inner.try_complete() {
            return Poll::Ready(result);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

impl<T> Stream for Stage<T>
where
    T: Multishot + 'static,
{
    type Item = T::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        ready!(self.as_mut().poll_submit(cx));
        let this = self.as_mut().project();
        let inner = match this {
            StageProj::Unsubmitted { .. } => unreachable!("unsubmitted"),
            StageProj::Submitted { inner } => inner,
        };
        if let Some(result) = inner.try_next() {
            return Poll::Ready(Some(result));
        }
        if inner.inner.is_complete() {
            return Poll::Ready(None);
        }
        inner.inner.register_waker(cx.waker());
        Poll::Pending
    }
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;

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
}
