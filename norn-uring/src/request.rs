#![allow(private_interfaces)]

use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};
use std::time::Duration;

use smallvec::SmallVec;

use crate::error::SubmitError;
use crate::operation::{ConfiguredEntry, Op, Singleshot};

mod timeout;

pub use timeout::{
    LinkedTimeoutControl, Timeout, TimeoutControl, TimeoutOutcome, TimeoutRemove, TimeoutUpdate,
    WithTimeout,
};

mod private {
    use super::*;

    // Public solely so it can seal `Request`; the private module prevents external use.
    #[allow(unnameable_types)]
    pub trait Chainable: Future {
        fn reactor(&self) -> &crate::Handle;
        /// Wait until batch preparation can proceed without consuming entries.
        fn poll_ready_to_prepare(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
            Poll::Ready(())
        }
        /// Prepare entries and return whether later linked requests may be submitted.
        fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool;
        fn cancel_unsubmitted(self: Pin<&mut Self>);
        fn finish_submit(self: Pin<&mut Self>);
        fn fail_submit(self: Pin<&mut Self>, err: &SubmitError);
    }

    // Public solely so it can seal `WithOptions`.
    #[allow(unnameable_types)]
    pub trait AcceptsOptions<O> {
        fn set_options(&mut self, opts: O);
    }

    // Public solely so it can seal `WithOptions`.
    #[allow(unnameable_types)]
    pub trait OptionsSealed {}
}

pub(crate) use private::{AcceptsOptions, OptionsSealed};

/// A request or future that accepts per-operation options before it is
/// first polled.
///
/// Each operation family has one options type, such as
/// [`fs::ReadOptions`](crate::fs::ReadOptions) for positional file reads.
/// Options apply to this request only. A request whose options are never set
/// behaves the same as one given default options. Most implementors are
/// [`Request`]s; a few operations that first try a synchronous system call,
/// such as [`UdpSocket::recv_from`](crate::net::UdpSocket::recv_from), are plain
/// futures.
///
/// Set options before linking the request or adding a timeout, since those
/// return a different request type:
///
/// ```no_run
/// # async fn f(file: &norn_uring::fs::File) -> std::io::Result<()> {
/// use std::time::Duration;
/// use norn_uring::{fs::WriteOptions, Request, WithOptions};
///
/// const DURABLE: WriteOptions = WriteOptions::new().dsync(true);
/// let (n, buf) = file
///     .write_at(b"record".to_vec(), 0)
///     .options(DURABLE)
///     .timeout(Duration::from_secs(1))
///     .await
///     .buf_ok()?;
/// # Ok(())
/// # }
/// ```
pub trait WithOptions<O>: private::OptionsSealed + Sized {
    /// Replace this request's options.
    ///
    /// # Panics
    ///
    /// Panics if the request has already been polled.
    #[must_use]
    fn options(self, opts: O) -> Self;
}

impl<T: 'static> private::OptionsSealed for Op<T> {}

impl<T, O> WithOptions<O> for Op<T>
where
    T: Singleshot + private::AcceptsOptions<O> + 'static,
{
    #[track_caller]
    fn options(self, opts: O) -> Self {
        self.modify_prepared(|op| op.set_options(opts))
    }
}

/// A lazy request that can be linked with other requests before submission.
pub trait Request: Future + Sized + private::Chainable {
    /// Link another request and return both results together.
    ///
    /// If an operation fails during configuration, earlier configured operations are still
    /// submitted, while that operation reports its configuration error and later linked
    /// operations complete with `ECANCELED` without reaching the kernel.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `next` target different drivers.
    fn then<R>(self, next: R) -> Then<Self, R>
    where
        R: Request,
    {
        Then::new(self, next)
    }

    /// Link another request that runs even if this one fails.
    ///
    /// The requests are linked with `IOSQE_IO_HARDLINK`: the kernel issues
    /// `next` once this request completes, whatever its result, for example
    /// to close a descriptor after a write. `next` also runs if this request
    /// fails configuration. Both results are returned, as with
    /// [`Request::then`].
    ///
    /// Dropping the returned request cancels both requests, as it does for
    /// [`Request::then`]. Cancelling this request does not break a hard link,
    /// so the kernel still issues `next`, and `next` is cancelled once it has
    /// been issued. An operation that completes as soon as it is issued, such
    /// as a close, can therefore still run.
    ///
    /// # Panics
    ///
    /// Panics if `self` and `next` target different drivers.
    fn then_always<R>(self, next: R) -> Then<Self, R>
    where
        R: Request,
    {
        Then::new_linked(self, next, true)
    }

    /// Link another request but discard its output.
    ///
    /// Configuration failures short-circuit later linked operations in the same way as
    /// [`Request::then`].
    ///
    /// # Panics
    ///
    /// Panics if `self` and `next` target different drivers.
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
    ///
    /// The timeout bounds only the **final** request in a chain.
    /// `IORING_OP_LINK_TIMEOUT` applies to the entry immediately before it,
    /// and the kernel starts its timer only once that entry is issued. In
    /// `a.then(b).timeout(d)`, a never-completing `a` is not bounded: `b` is
    /// never issued and the timer never starts. To bound a whole chain, race
    /// it against [`Handle::timeout`](crate::Handle::timeout) and drop the
    /// chain on expiry. Call [`WithTimeout::control`] before moving the
    /// future if the deadline may need to be reset while the request is active.
    fn timeout(self, duration: Duration) -> WithTimeout<Self> {
        WithTimeout::new(self, duration)
    }
}

impl<T> Request for T where T: Future + Sized + private::Chainable {}

/// Configure two linked requests into `batch`, and return whether requests
/// linked after them may be submitted.
///
/// After a soft link, a configuration failure in `left` cancels `right`
/// without the kernel. After a hard link, `right` is configured regardless and
/// linked to whatever `left` configured.
fn prepare_pair<A, B>(
    mut left: Pin<&mut A>,
    mut right: Pin<&mut B>,
    hard: bool,
    batch: &mut SmallVec<[ConfiguredEntry; 4]>,
) -> bool
where
    A: Request,
    B: Request,
{
    let left_configured = left.as_mut().prepare_batch(batch);
    if hard {
        if let Some(last) = batch.last() {
            last.set_hard_link();
        }
        return right.as_mut().prepare_batch(batch);
    }
    if left_configured {
        right.as_mut().prepare_batch(batch)
    } else {
        right.as_mut().cancel_unsubmitted();
        false
    }
}

/// Configure two linked requests and hand them to the driver as one batch.
///
/// The driver pushes the batch whole, or queues it until the SQ has room, so
/// both requests count as submitted once this returns.
fn submit_pair<A, B>(mut left: Pin<&mut A>, mut right: Pin<&mut B>, hard: bool)
where
    A: Request,
    B: Request,
{
    let mut batch = SmallVec::new();
    prepare_pair(left.as_mut(), right.as_mut(), hard, &mut batch);
    let submitted = if batch.is_empty() {
        Ok(())
    } else {
        left.as_ref().get_ref().reactor().submit_batch(batch)
    };
    match submitted {
        Ok(()) => {
            left.as_mut().finish_submit();
            right.as_mut().finish_submit();
        }
        Err(err) => {
            left.as_mut().fail_submit(&err);
            right.as_mut().fail_submit(&err);
        }
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
        // Whether `right` is hard-linked behind `left` (`then_always`).
        hard: bool,
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
        Self::new_linked(left, right, false)
    }

    fn new_linked(left: A, right: B, hard: bool) -> Self {
        assert!(
            left.reactor().same_driver(right.reactor()),
            "linked requests must target the same driver"
        );
        Self {
            state: ThenState::Pending {
                left,
                right,
                left_output: None,
                right_output: None,
                submitted: false,
            },
            hard,
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
            let hard = *this.hard;
            let ThenStateProj::Pending {
                mut left,
                mut right,
                submitted,
                ..
            } = this.state.as_mut().project()
            else {
                panic!("cannot poll future after completion");
            };

            if !*submitted {
                ready!(left.as_mut().poll_ready_to_prepare(cx));
                ready!(right.as_mut().poll_ready_to_prepare(cx));
                submit_pair(left.as_mut(), right.as_mut(), hard);
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
                submitted,
                ..
            } = this.state.as_mut().project()
            else {
                panic!("cannot poll future after completion");
            };

            if !*submitted {
                ready!(left.as_mut().poll_ready_to_prepare(cx));
                ready!(right.as_mut().poll_ready_to_prepare(cx));
                submit_pair(left.as_mut(), right.as_mut(), false);
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

    fn poll_ready_to_prepare(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        let ThenStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            panic!("cannot prepare completed request");
        };
        ready!(left.as_mut().poll_ready_to_prepare(cx));
        right.as_mut().poll_ready_to_prepare(cx)
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
        prepare_pair(left.as_mut(), right.as_mut(), *this.hard, batch)
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

    fn poll_ready_to_prepare(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.project();
        let ThenAuxStateProj::Pending {
            mut left,
            mut right,
            ..
        } = this.state.project()
        else {
            panic!("cannot prepare completed request");
        };
        ready!(left.as_mut().poll_ready_to_prepare(cx));
        right.as_mut().poll_ready_to_prepare(cx)
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
        prepare_pair(left.as_mut(), right.as_mut(), false, batch)
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
}

impl<R, F, U> private::Chainable for Map<R, F>
where
    R: Request,
    F: FnOnce(R::Output) -> U,
{
    fn reactor(&self) -> &crate::Handle {
        self.inner.reactor()
    }

    fn poll_ready_to_prepare(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        self.project().inner.poll_ready_to_prepare(cx)
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
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;
    use std::io;
    use std::rc::Rc;
    use std::time::Duration;

    use norn_executor::LocalExecutor;

    use super::*;
    use crate::operation::{CQEResult, Operation};

    #[derive(Debug)]
    struct TaggedNop(u8);

    unsafe impl Operation for TaggedNop {
        type Completion = CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(io_uring::opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }
    }

    impl Singleshot for TaggedNop {
        type Output = std::io::Result<u8>;

        fn complete(self, completion: Self::Completion) -> Self::Output {
            completion.result.map(|_| self.0)
        }
    }

    #[derive(Debug)]
    struct ConfigureFailed(&'static str);

    unsafe impl Operation for ConfigureFailed {
        type Completion = CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Err(io::Error::new(io::ErrorKind::InvalidInput, self.0))
        }

        unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
            result
        }
    }

    impl Singleshot for ConfigureFailed {
        type Output = std::io::Result<()>;

        fn complete(self, completion: Self::Completion) -> Self::Output {
            completion.result.map(drop)
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
    fn then_always_runs_next_after_a_configure_failure() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut ex = LocalExecutor::new(driver);

        let ((failed, always), later) = ex.block_on(async {
            handle
                .submit(ConfigureFailed("first configure failed"))
                .then_always(handle.submit(TaggedNop(2)))
                .then(handle.submit(TaggedNop(3)))
                .await
        });

        assert!(failed.is_err());
        assert_eq!(always.unwrap(), 2, "a hard link runs after a failure");
        assert_eq!(later.unwrap(), 3, "a soft link after it follows its result");
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
    fn canceled_unsubmitted_request_reaps_before_typed_completion_drop() {
        #[derive(Debug)]
        struct TrackedCompletion {
            result: Option<io::Result<u32>>,
            dropped: Rc<Cell<Option<i32>>>,
        }

        impl TrackedCompletion {
            fn into_result(mut self) -> io::Result<u32> {
                self.result.take().expect("completion result missing")
            }
        }

        impl Drop for TrackedCompletion {
            fn drop(&mut self) {
                let Some(result) = self.result.take() else {
                    return;
                };
                self.dropped.set(
                    result
                        .expect_err("abandoned completion should contain cancellation")
                        .raw_os_error(),
                );
            }
        }

        #[derive(Debug)]
        struct ReapTracked {
            reaped: Rc<Cell<Option<i32>>>,
            dropped: Rc<Cell<Option<i32>>>,
        }

        unsafe impl Operation for ReapTracked {
            type Completion = TrackedCompletion;

            fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
                Ok(io_uring::opcode::Nop::new().build())
            }

            unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
                self.reaped.set(
                    result
                        .result
                        .as_ref()
                        .err()
                        .and_then(io::Error::raw_os_error),
                );
                TrackedCompletion {
                    result: Some(result.result),
                    dropped: Rc::clone(&self.dropped),
                }
            }
        }

        impl Singleshot for ReapTracked {
            type Output = io::Result<()>;

            fn complete(self, completion: Self::Completion) -> Self::Output {
                completion.into_result().map(drop)
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let reaped = Rc::new(Cell::new(None));
        let dropped = Rc::new(Cell::new(None));
        let mut op = Box::pin(driver.handle().submit(ReapTracked {
            reaped: Rc::clone(&reaped),
            dropped: Rc::clone(&dropped),
        }));

        op.as_mut().cancel_unsubmitted();
        assert_eq!(reaped.get(), Some(libc::ECANCELED));
        assert_eq!(dropped.get(), None);

        drop(op);

        assert_eq!(dropped.get(), Some(libc::ECANCELED));
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
