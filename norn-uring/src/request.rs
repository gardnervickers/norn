#![allow(private_interfaces)]

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use smallvec::SmallVec;

use crate::driver::PushFuture;
use crate::error::SubmitError;
use crate::operation::{CQEResult, ConfiguredEntry, Op, Operation, Singleshot};

mod private {
    use super::*;

    pub trait Chainable: Future {
        fn reactor(&self) -> &crate::Handle;
        /// Prepare entries and return whether later linked requests may be submitted.
        fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool;
        fn cancel_unsubmitted(self: Pin<&mut Self>);
        fn finish_submit(self: Pin<&mut Self>);
        fn fail_submit(self: Pin<&mut Self>, err: &SubmitError);
        fn cancel_unfinished(self: Pin<&mut Self>);
    }
}

/// A lazy request that can be linked with other requests before submission.
pub trait Request: Future + Sized + private::Chainable {
    /// Link another request and return both results together.
    ///
    /// If an operation fails during configuration, earlier configured operations are still
    /// submitted, while that operation reports its configuration error and later linked
    /// operations complete with `ECANCELED` without reaching the kernel.
    fn then<R>(self, next: R) -> Then<Self, R>
    where
        R: Request,
    {
        Then::new(self, next)
    }

    /// Link another request but discard its output.
    ///
    /// Configuration failures short-circuit later linked operations in the same way as
    /// [`Request::then`].
    fn then_aux<R>(self, next: R) -> ThenAux<Self, R>
    where
        R: Request,
    {
        ThenAux::new(self, next)
    }

    /// Transform the resolved output without changing the underlying request batch.
    fn map<F, U>(self, f: F) -> Map<Self, F>
    where
        F: FnOnce(Self::Output) -> U,
    {
        Map::new(self, f)
    }

    /// Append a terminal linked timeout to this request chain.
    ///
    /// The returned future resolves to this request's output. If the timeout
    /// expires first, the linked request is canceled and its output reflects
    /// that cancellation.
    fn timeout(self, duration: Duration) -> WithTimeout<Self> {
        WithTimeout::new(self, duration)
    }
}

impl<T> Request for T where T: Future + Sized + private::Chainable {}

pin_project_lite::pin_project! {
    /// A request future with a terminal linked timeout.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct WithTimeout<R>
    where
        R: Request,
    {
        #[pin]
        inner: ThenAux<R, Op<LinkTimeoutOp>>,
    }
}

impl<R> std::fmt::Debug for WithTimeout<R>
where
    R: Request,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithTimeout").finish()
    }
}

impl<R> WithTimeout<R>
where
    R: Request,
{
    fn new(inner: R, duration: Duration) -> Self {
        let reactor = inner.reactor().clone();
        let timeout = Op::new(LinkTimeoutOp::new(duration), reactor);
        Self {
            inner: ThenAux::new(inner, timeout),
        }
    }
}

impl<R> Future for WithTimeout<R>
where
    R: Request,
{
    type Output = R::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

#[derive(Debug)]
struct LinkTimeoutOp {
    timespec: io_uring::types::Timespec,
}

impl LinkTimeoutOp {
    fn new(duration: Duration) -> Self {
        Self {
            timespec: duration.into(),
        }
    }
}

// Safety: the timeout specification is stored inline in this pinned operation,
// so its SQE pointer remains valid until the terminal completion.
unsafe impl Operation for LinkTimeoutOp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::LinkTimeout::new(&self.timespec).build())
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for LinkTimeoutOp {
    type Output = std::io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

pin_project_lite::pin_project! {
    /// A linked request that yields both inner results.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Then<A, B>
    where
        A: Request,
        B: Request,
    {
        #[pin]
        state: ThenState<A, B>,
    }
}

impl<A, B> std::fmt::Debug for Then<A, B>
where
    A: Request,
    B: Request,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Then").finish()
    }
}

pin_project_lite::pin_project! {
    #[project = ThenStateProj]
    enum ThenState<A, B>
    where
        A: Request,
        B: Request,
    {
        Pending {
            #[pin]
            left: A,
            #[pin]
            right: B,
            #[pin]
            submit: Option<PushFuture>,
            left_output: Option<A::Output>,
            right_output: Option<B::Output>,
            submitted: bool,
        },
        Complete,
    }
}

impl<A, B> Then<A, B>
where
    A: Request,
    B: Request,
{
    fn new(left: A, right: B) -> Self {
        assert!(
            left.reactor().same_driver(right.reactor()),
            "linked requests must target the same driver"
        );
        Self {
            state: ThenState::Pending {
                left,
                right,
                submit: None,
                left_output: None,
                right_output: None,
                submitted: false,
            },
        }
    }
}

impl<A, B> Future for Then<A, B>
where
    A: Request,
    B: Request,
{
    type Output = (A::Output, B::Output);

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut this = self.as_mut().project();
            let ThenStateProj::Pending {
                mut left,
                mut right,
                mut submit,
                submitted,
                ..
            } = this.state.as_mut().project()
            else {
                panic!("cannot poll future after completion");
            };

            if !*submitted {
                if submit.is_none() {
                    let mut batch = SmallVec::new();
                    if !left.as_mut().prepare_batch(&mut batch) {
                        right.as_mut().cancel_unsubmitted();
                    } else {
                        right.as_mut().prepare_batch(&mut batch);
                    }
                    let reactor = left.as_ref().get_ref().reactor().clone();
                    submit.set(Some(reactor.push_batch(batch)));
                }

                let fut = submit
                    .as_mut()
                    .as_pin_mut()
                    .expect("submit future must exist");
                match ready!(fut.poll(cx)) {
                    Ok(()) => {
                        left.as_mut().finish_submit();
                        right.as_mut().finish_submit();
                    }
                    Err(err) => {
                        left.as_mut().fail_submit(&err);
                        right.as_mut().fail_submit(&err);
                    }
                }
                submit.set(None);
                *submitted = true;
            }
        }

        let mut this = self.as_mut().project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            left_output,
            right_output,
            ..
        } = this.state.as_mut().project()
        else {
            panic!("cannot poll future after completion");
        };

        if left_output.is_none() {
            if let Poll::Ready(output) = Future::poll(left.as_mut(), cx) {
                *left_output = Some(output);
            }
        }
        if right_output.is_none() {
            if let Poll::Ready(output) = Future::poll(right.as_mut(), cx) {
                *right_output = Some(output);
            }
        }

        if left_output.is_some() && right_output.is_some() {
            let left = left_output.take().expect("left output missing");
            let right = right_output.take().expect("right output missing");
            this.state.set(ThenState::Complete);
            return Poll::Ready((left, right));
        }
        Poll::Pending
    }
}

pin_project_lite::pin_project! {
    /// A linked request that discards the auxiliary request output.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct ThenAux<A, B>
    where
        A: Request,
        B: Request,
    {
        #[pin]
        state: ThenAuxState<A, B>,
    }
}

impl<A, B> std::fmt::Debug for ThenAux<A, B>
where
    A: Request,
    B: Request,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ThenAux").finish()
    }
}

pin_project_lite::pin_project! {
    #[project = ThenAuxStateProj]
    enum ThenAuxState<A, B>
    where
        A: Request,
        B: Request,
    {
        Pending {
            #[pin]
            left: A,
            #[pin]
            right: B,
            #[pin]
            submit: Option<PushFuture>,
            left_output: Option<A::Output>,
            right_done: bool,
            submitted: bool,
        },
        Complete,
    }
}

impl<A, B> ThenAux<A, B>
where
    A: Request,
    B: Request,
{
    fn new(left: A, right: B) -> Self {
        assert!(
            left.reactor().same_driver(right.reactor()),
            "linked requests must target the same driver"
        );
        Self {
            state: ThenAuxState::Pending {
                left,
                right,
                submit: None,
                left_output: None,
                right_done: false,
                submitted: false,
            },
        }
    }
}

impl<A, B> Future for ThenAux<A, B>
where
    A: Request,
    B: Request,
{
    type Output = A::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        {
            let mut this = self.as_mut().project();
            let ThenAuxStateProj::Pending {
                mut left,
                mut right,
                mut submit,
                submitted,
                ..
            } = this.state.as_mut().project()
            else {
                panic!("cannot poll future after completion");
            };

            if !*submitted {
                if submit.is_none() {
                    let mut batch = SmallVec::new();
                    if !left.as_mut().prepare_batch(&mut batch) {
                        right.as_mut().cancel_unsubmitted();
                    } else {
                        right.as_mut().prepare_batch(&mut batch);
                    }
                    let reactor = left.as_ref().get_ref().reactor().clone();
                    submit.set(Some(reactor.push_batch(batch)));
                }

                let fut = submit
                    .as_mut()
                    .as_pin_mut()
                    .expect("submit future must exist");
                match ready!(fut.poll(cx)) {
                    Ok(()) => {
                        left.as_mut().finish_submit();
                        right.as_mut().finish_submit();
                    }
                    Err(err) => {
                        left.as_mut().fail_submit(&err);
                        right.as_mut().fail_submit(&err);
                    }
                }
                submit.set(None);
                *submitted = true;
            }
        }

        let mut this = self.as_mut().project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            left_output,
            right_done,
            ..
        } = this.state.as_mut().project()
        else {
            panic!("cannot poll future after completion");
        };

        if left_output.is_none() {
            if let Poll::Ready(output) = Future::poll(left.as_mut(), cx) {
                *left_output = Some(output);
            }
        }
        if !*right_done && Future::poll(right.as_mut(), cx).is_ready() {
            *right_done = true;
        }

        if *right_done && left_output.is_some() {
            let left = left_output.take().expect("left output missing");
            this.state.set(ThenAuxState::Complete);
            return Poll::Ready(left);
        }
        Poll::Pending
    }
}

pin_project_lite::pin_project! {
    /// A lazy output transform over another request.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Map<R, F>
    where
        R: Request,
    {
        #[pin]
        inner: R,
        f: Option<F>,
    }
}

impl<R, F> std::fmt::Debug for Map<R, F>
where
    R: Request,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Map").finish()
    }
}

impl<R, F> Map<R, F>
where
    R: Request,
{
    fn new(inner: R, f: F) -> Self {
        Self { inner, f: Some(f) }
    }
}

impl<R, F, U> Future for Map<R, F>
where
    R: Request,
    F: FnOnce(R::Output) -> U,
{
    type Output = U;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let value = ready!(Future::poll(this.inner.as_mut(), cx));
        let f = this.f.take().expect("cannot poll future after completion");
        Poll::Ready(f(value))
    }
}

impl<T> private::Chainable for Op<T>
where
    T: Singleshot + 'static,
{
    fn reactor(&self) -> &crate::Handle {
        self.handle()
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        Op::prepare_batch(self, batch)
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        Op::cancel_unsubmitted(self);
    }

    fn finish_submit(self: Pin<&mut Self>) {
        Op::finish_submit(self);
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        Op::fail_submit(self, err);
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        Op::cancel_unfinished(self);
    }
}

impl<A, B> private::Chainable for Then<A, B>
where
    A: Request,
    B: Request,
{
    fn reactor(&self) -> &crate::Handle {
        match &self.state {
            ThenState::Pending { left, .. } => left.reactor(),
            ThenState::Complete => panic!("completed request has no reactor"),
        }
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            panic!("cannot prepare completed request");
        };
        if !left.as_mut().prepare_batch(batch) {
            right.as_mut().cancel_unsubmitted();
            false
        } else {
            right.as_mut().prepare_batch(batch)
        }
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            return;
        };
        left.as_mut().cancel_unsubmitted();
        right.as_mut().cancel_unsubmitted();
    }

    fn finish_submit(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            submitted,
            ..
        } = this.state.project()
        else {
            panic!("cannot submit completed request");
        };
        left.as_mut().finish_submit();
        right.as_mut().finish_submit();
        *submitted = true;
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            submitted,
            ..
        } = this.state.project()
        else {
            panic!("cannot fail completed request");
        };
        left.as_mut().fail_submit(err);
        right.as_mut().fail_submit(err);
        *submitted = true;
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            return;
        };
        left.as_mut().cancel_unfinished();
        right.as_mut().cancel_unfinished();
    }
}

impl<A, B> private::Chainable for ThenAux<A, B>
where
    A: Request,
    B: Request,
{
    fn reactor(&self) -> &crate::Handle {
        match &self.state {
            ThenAuxState::Pending { left, .. } => left.reactor(),
            ThenAuxState::Complete => panic!("completed request has no reactor"),
        }
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            panic!("cannot prepare completed request");
        };
        if !left.as_mut().prepare_batch(batch) {
            right.as_mut().cancel_unsubmitted();
            false
        } else {
            right.as_mut().prepare_batch(batch)
        }
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            return;
        };
        left.as_mut().cancel_unsubmitted();
        right.as_mut().cancel_unsubmitted();
    }

    fn finish_submit(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            submitted,
            ..
        } = this.state.project()
        else {
            panic!("cannot submit completed request");
        };
        left.as_mut().finish_submit();
        right.as_mut().finish_submit();
        *submitted = true;
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            submitted,
            ..
        } = this.state.project()
        else {
            panic!("cannot fail completed request");
        };
        left.as_mut().fail_submit(err);
        right.as_mut().fail_submit(err);
        *submitted = true;
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            return;
        };
        left.as_mut().cancel_unfinished();
        right.as_mut().cancel_unfinished();
    }
}

impl<R, F, U> private::Chainable for Map<R, F>
where
    R: Request,
    F: FnOnce(R::Output) -> U,
{
    fn reactor(&self) -> &crate::Handle {
        self.inner.reactor()
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        self.project().inner.prepare_batch(batch)
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        self.project().inner.cancel_unsubmitted();
    }

    fn finish_submit(self: Pin<&mut Self>) {
        self.project().inner.finish_submit();
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        self.project().inner.fail_submit(err);
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        self.project().inner.cancel_unfinished();
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::rc::Rc;
    use std::time::Duration;

    use norn_executor::LocalExecutor;

    use super::*;

    #[derive(Debug)]
    struct TaggedNop(u8);

    unsafe impl Operation for TaggedNop {
        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(io_uring::opcode::Nop::new().build())
        }

        fn cleanup(&mut self, _: CQEResult) {}
    }

    impl Singleshot for TaggedNop {
        type Output = std::io::Result<u8>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.result.map(|_| self.0)
        }
    }

    #[derive(Debug)]
    struct ConfigureFailed(&'static str);

    unsafe impl Operation for ConfigureFailed {
        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Err(io::Error::new(io::ErrorKind::InvalidInput, self.0))
        }

        fn cleanup(&mut self, _: CQEResult) {}
    }

    impl Singleshot for ConfigureFailed {
        type Output = std::io::Result<()>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.result.map(drop)
        }
    }

    fn assert_canceled<T>(result: &io::Result<T>) {
        let Err(error) = result else {
            panic!("request should be canceled");
        };
        assert_eq!(error.raw_os_error(), Some(libc::ECANCELED));
    }

    #[test]
    fn configure_failure_cancels_later_linked_request() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let (failed, later) = ex.block_on(async {
            handle
                .submit(ConfigureFailed("first configure failed"))
                .then(handle.submit(TaggedNop(2)))
                .await
        });

        assert_eq!(
            failed.expect_err("configuration should fail").to_string(),
            "first configure failed"
        );
        assert_canceled(&later);
    }

    #[test]
    fn middle_configure_failure_submits_prefix_and_cancels_suffix() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let ((prefix, failed), suffix) = ex.block_on(async {
            handle
                .submit(TaggedNop(1))
                .then(handle.submit(ConfigureFailed("middle configure failed")))
                .then(handle.submit(TaggedNop(3)))
                .await
        });

        assert_eq!(prefix.unwrap(), 1);
        assert_eq!(
            failed.expect_err("configuration should fail").to_string(),
            "middle configure failed"
        );
        assert_canceled(&suffix);
    }

    #[test]
    fn earlier_configure_failure_dominates_later_configure_failure() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let (first, second) = ex.block_on(async {
            handle
                .submit(ConfigureFailed("first configure failed"))
                .then(handle.submit(ConfigureFailed("second configure failed")))
                .await
        });

        assert_eq!(
            first
                .expect_err("first configuration should fail")
                .to_string(),
            "first configure failed"
        );
        assert_canceled(&second);
    }

    #[test]
    fn configure_failure_propagates_through_map_and_then_aux() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);
        let aux_error = Rc::new(Cell::new(None));
        let aux_error_seen = Rc::clone(&aux_error);

        let failed = ex.block_on(async move {
            handle
                .submit(ConfigureFailed("mapped configure failed"))
                .map(|result| result)
                .then_aux(handle.submit(TaggedNop(9)).map(move |result| {
                    aux_error_seen.set(
                        result
                            .expect_err("auxiliary request should be canceled")
                            .raw_os_error(),
                    );
                }))
                .await
        });

        assert_eq!(
            failed.expect_err("configuration should fail").to_string(),
            "mapped configure failed"
        );
        assert_eq!(aux_error.get(), Some(libc::ECANCELED));
    }

    #[test]
    fn dropping_canceled_unsubmitted_request_runs_cleanup() {
        #[derive(Debug)]
        struct CleanupTracked(Rc<Cell<Option<i32>>>);

        unsafe impl Operation for CleanupTracked {
            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            fn cleanup(&mut self, result: CQEResult) {
                self.0.set(
                    result
                        .result
                        .expect_err("cleanup should see cancellation")
                        .raw_os_error(),
                );
            }
        }

        impl Singleshot for CleanupTracked {
            type Output = io::Result<()>;

            fn complete(self, result: CQEResult) -> Self::Output {
                result.result.map(drop)
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let cleaned = Rc::new(Cell::new(None));
        let mut op = Box::pin(driver.handle().submit(CleanupTracked(Rc::clone(&cleaned))));

        op.as_mut().cancel_unsubmitted();
        drop(op);

        assert_eq!(cleaned.get(), Some(libc::ECANCELED));
    }

    #[test]
    fn nested_then_composes_outputs() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let output = ex.block_on(async {
            handle
                .submit(TaggedNop(1))
                .then(
                    handle
                        .submit(TaggedNop(2))
                        .then(handle.submit(TaggedNop(3))),
                )
                .await
        });

        let (left, (middle, right)) = output;
        assert_eq!(left.unwrap(), 1);
        assert_eq!(middle.unwrap(), 2);
        assert_eq!(right.unwrap(), 3);
    }

    #[test]
    fn then_aux_waits_for_auxiliary_request_and_map_transforms_output() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);
        let aux_ran = Rc::new(Cell::new(false));
        let aux_seen = Rc::clone(&aux_ran);

        let output = ex.block_on(async move {
            handle
                .submit(TaggedNop(4))
                .map(|result| result.map(|value| value + 1))
                .then_aux(handle.submit(TaggedNop(9)).map(move |result| {
                    assert_eq!(result.unwrap(), 9);
                    aux_seen.set(true);
                }))
                .await
        });

        assert_eq!(output.unwrap(), 5);
        assert!(
            aux_ran.get(),
            "auxiliary request should be polled to completion"
        );
    }

    #[test]
    fn chained_maps_run_in_order() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let output = ex.block_on(async {
            handle
                .submit(TaggedNop(7))
                .map(|result| result.expect("request should succeed"))
                .map(|value| value as u16 + 5)
                .await
        });

        assert_eq!(output, 12);
    }

    #[test]
    fn timeout_returns_primary_output_when_request_completes_first() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let output = ex.block_on(async {
            handle
                .submit(TaggedNop(11))
                .timeout(Duration::from_secs(1))
                .await
        });

        assert_eq!(output.unwrap(), 11);
    }

    #[test]
    #[should_panic(expected = "linked requests must target the same driver")]
    fn linking_requests_from_different_drivers_panics() {
        let left_driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let right_driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();

        drop(
            left_driver
                .handle()
                .submit(TaggedNop(1))
                .then(right_driver.handle().submit(TaggedNop(2))),
        );
    }
}
