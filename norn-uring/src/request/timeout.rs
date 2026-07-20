use std::cell::{Cell, RefCell};
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use smallvec::SmallVec;

use super::{private, Request, ThenAux};
use crate::error::SubmitError;
use crate::operation::{CQEResult, ConfiguredEntry, Op, OpTarget, Operation, Singleshot};

/// The terminal state of a standalone timeout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimeoutOutcome {
    /// The configured duration elapsed.
    Expired,
    /// The timeout was canceled before it elapsed.
    Canceled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutLifecycle {
    Prepared,
    Queued,
    Submitted,
    CanceledBeforeSubmit,
    Complete,
}

#[derive(Debug)]
struct TimeoutState {
    lifecycle: Cell<TimeoutLifecycle>,
    duration: Cell<Duration>,
    timespec: Cell<io_uring::types::Timespec>,
    waiters: RefCell<Vec<Waker>>,
}

impl TimeoutState {
    fn new(duration: Duration) -> Self {
        Self {
            lifecycle: Cell::new(TimeoutLifecycle::Prepared),
            duration: Cell::new(duration),
            timespec: Cell::new(duration.into()),
            waiters: RefCell::new(Vec::new()),
        }
    }

    fn set_duration(&self, duration: Duration) {
        assert!(
            matches!(
                self.lifecycle.get(),
                TimeoutLifecycle::Prepared | TimeoutLifecycle::Queued
            ),
            "cannot update inactive timeout storage"
        );
        self.duration.set(duration);
        self.timespec.set(duration.into());
    }

    fn mark_submitted(&self) {
        match self.lifecycle.get() {
            TimeoutLifecycle::Prepared | TimeoutLifecycle::Queued => {
                self.lifecycle.set(TimeoutLifecycle::Submitted);
                self.wake_waiters();
            }
            TimeoutLifecycle::Submitted => {}
            TimeoutLifecycle::CanceledBeforeSubmit | TimeoutLifecycle::Complete => {
                panic!("cannot submit an inactive timeout")
            }
        }
    }

    fn mark_queued(&self) {
        match self.lifecycle.get() {
            TimeoutLifecycle::Prepared => self.lifecycle.set(TimeoutLifecycle::Queued),
            TimeoutLifecycle::Queued => {}
            TimeoutLifecycle::Submitted
            | TimeoutLifecycle::CanceledBeforeSubmit
            | TimeoutLifecycle::Complete => panic!("cannot queue an inactive timeout"),
        }
    }

    fn mark_complete(&self) {
        self.lifecycle.set(TimeoutLifecycle::Complete);
        self.wake_waiters();
    }

    fn mark_canceled_before_submit(&self) {
        self.lifecycle.set(TimeoutLifecycle::CanceledBeforeSubmit);
        self.wake_waiters();
    }

    fn register_waiter(&self, waker: &Waker) {
        self.waiters.borrow_mut().push(waker.clone());
    }

    fn unregister_waiter(&self, waker: &Waker) {
        let mut waiters = self.waiters.borrow_mut();
        if let Some(position) = waiters
            .iter()
            .position(|registered| registered.will_wake(waker))
        {
            waiters.swap_remove(position);
        }
    }

    fn wake_waiters(&self) {
        for waker in self.waiters.take() {
            waker.wake();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeoutKind {
    Standalone,
    Linked,
}

#[derive(Clone)]
struct TimeoutTarget {
    state: Rc<TimeoutState>,
    operation: OpTarget,
    reactor: crate::Handle,
    kind: TimeoutKind,
}

impl std::fmt::Debug for TimeoutTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutTarget")
            .field("kind", &self.kind)
            .field("lifecycle", &self.state.lifecycle.get())
            .finish_non_exhaustive()
    }
}

/// A cloneable controller for a standalone [`Timeout`].
///
/// The controller retains the timeout's opaque io_uring identity, so a cancel
/// or reset request can never accidentally target a newer operation that reused
/// the same allocation address.
#[derive(Clone)]
pub struct TimeoutControl {
    target: TimeoutTarget,
}

impl std::fmt::Debug for TimeoutControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutControl")
            .field("lifecycle", &self.target.state.lifecycle.get())
            .finish_non_exhaustive()
    }
}

impl TimeoutControl {
    /// Cancel the timeout.
    ///
    /// The returned request resolves to `Ok(true)` when this call canceled a
    /// prepared or active timeout. It resolves to `Ok(false)` when no matching
    /// timeout could be removed, including when it had already completed or its
    /// completion was already in progress.
    ///
    /// Canceling before the [`Timeout`] is first polled is handled locally; the
    /// timeout will never be submitted later.
    pub fn cancel(&self) -> TimeoutRemove {
        TimeoutRemove::new(self.target.clone())
    }

    /// Reset the timeout to expire after `duration` from the update.
    ///
    /// The returned request resolves to `Ok(true)` when the new duration was
    /// applied. It resolves to `Ok(false)` when no matching timeout could be
    /// updated, including when it had already completed or its completion was
    /// already in progress.
    ///
    /// Resetting before the [`Timeout`] is first polled updates its initial
    /// duration locally and does not submit an unnecessary control request.
    pub fn reset(&self, duration: Duration) -> TimeoutUpdate {
        TimeoutUpdate::new(self.target.clone(), duration)
    }
}

/// A cloneable update controller for the timeout attached by [`Request::timeout`].
///
/// Linked timeouts intentionally do not expose [`TimeoutControl::cancel`]: a
/// linked timeout participates in the lifetime of its request chain and uses a
/// different kernel update mode from a standalone timeout.
#[derive(Clone)]
pub struct LinkedTimeoutControl {
    target: TimeoutTarget,
}

impl std::fmt::Debug for LinkedTimeoutControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkedTimeoutControl")
            .field("lifecycle", &self.target.state.lifecycle.get())
            .finish_non_exhaustive()
    }
}

impl LinkedTimeoutControl {
    /// Reset the linked timeout to expire after `duration` from the update.
    ///
    /// The returned request resolves to `Ok(true)` when the new duration was
    /// applied and `Ok(false)` when no matching linked timeout could be updated,
    /// including when it had already completed or its completion was already in
    /// progress.
    pub fn reset(&self, duration: Duration) -> TimeoutUpdate {
        TimeoutUpdate::new(self.target.clone(), duration)
    }
}

pin_project_lite::pin_project! {
    /// A lazy, standalone io_uring timeout.
    ///
    /// Construct one with [`crate::Handle::timeout`]. Normal expiration resolves
    /// to [`TimeoutOutcome::Expired`] rather than surfacing the kernel's expected
    /// `ETIME` completion as an error. Use [`Timeout::control`] to cancel or reset
    /// it safely.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct Timeout {
        #[pin]
        inner: Op<TimeoutOp>,
        control: TimeoutControl,
        done: bool,
    }

    impl PinnedDrop for Timeout {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if matches!(
                this.control.target.state.lifecycle.get(),
                TimeoutLifecycle::Prepared
                    | TimeoutLifecycle::Queued
                    | TimeoutLifecycle::CanceledBeforeSubmit
            ) {
                this.control.target.state.mark_complete();
            }
        }
    }
}

impl std::fmt::Debug for Timeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Timeout")
            .field("duration", &self.control.target.state.duration.get())
            .field("lifecycle", &self.control.target.state.lifecycle.get())
            .finish_non_exhaustive()
    }
}

impl Timeout {
    fn new(reactor: crate::Handle, duration: Duration) -> Self {
        let state = Rc::new(TimeoutState::new(duration));
        let (inner, operation) = Op::new_with_target(
            TimeoutOp {
                state: Rc::clone(&state),
            },
            reactor.clone(),
        );
        let control = TimeoutControl {
            target: TimeoutTarget {
                state,
                operation,
                reactor,
                kind: TimeoutKind::Standalone,
            },
        };
        Self {
            inner,
            control,
            done: false,
        }
    }

    /// Return a controller that can cancel or reset this timeout.
    pub fn control(&self) -> TimeoutControl {
        self.control.clone()
    }
}

impl Future for Timeout {
    type Output = io::Result<TimeoutOutcome>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        assert!(!*this.done, "cannot poll timeout after completion");

        if this.control.target.state.lifecycle.get() == TimeoutLifecycle::CanceledBeforeSubmit {
            *this.done = true;
            return Poll::Ready(Ok(TimeoutOutcome::Canceled));
        }

        match Future::poll(this.inner.as_mut(), cx) {
            Poll::Ready(output) => {
                *this.done = true;
                Poll::Ready(output)
            }
            Poll::Pending => {
                if this.inner.as_ref().get_ref().is_submitted() {
                    this.control.target.state.mark_submitted();
                } else {
                    this.control.target.state.mark_queued();
                }
                Poll::Pending
            }
        }
    }
}

impl private::Chainable for Timeout {
    fn reactor(&self) -> &crate::Handle {
        &self.control.target.reactor
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Prepared => {
                let can_continue = this.inner.prepare_batch(batch);
                if can_continue {
                    this.control.target.state.mark_queued();
                } else {
                    this.control.target.state.mark_complete();
                }
                can_continue
            }
            TimeoutLifecycle::CanceledBeforeSubmit => true,
            TimeoutLifecycle::Queued | TimeoutLifecycle::Submitted | TimeoutLifecycle::Complete => {
                panic!("cannot prepare timeout more than once")
            }
        }
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Prepared => {
                this.control.target.state.mark_canceled_before_submit();
            }
            TimeoutLifecycle::CanceledBeforeSubmit | TimeoutLifecycle::Complete => {}
            TimeoutLifecycle::Queued | TimeoutLifecycle::Submitted => {
                panic!("cannot cancel a submitted timeout as unsubmitted")
            }
        }
    }

    fn finish_submit(self: Pin<&mut Self>) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Queued => {
                this.inner.finish_submit();
                this.control.target.state.mark_submitted();
            }
            TimeoutLifecycle::CanceledBeforeSubmit => {}
            TimeoutLifecycle::Prepared
            | TimeoutLifecycle::Submitted
            | TimeoutLifecycle::Complete => {
                panic!("cannot submit timeout more than once")
            }
        }
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Queued => {
                this.inner.fail_submit(err);
                this.control.target.state.mark_complete();
            }
            TimeoutLifecycle::CanceledBeforeSubmit => {}
            TimeoutLifecycle::Prepared
            | TimeoutLifecycle::Submitted
            | TimeoutLifecycle::Complete => {
                panic!("cannot fail timeout submission more than once")
            }
        }
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Prepared => this.control.target.state.mark_canceled_before_submit(),
            TimeoutLifecycle::Queued => {}
            TimeoutLifecycle::Submitted => this.inner.cancel_unfinished(),
            TimeoutLifecycle::CanceledBeforeSubmit | TimeoutLifecycle::Complete => {}
        }
    }
}

impl crate::Handle {
    /// Create a lazy standalone timeout that expires after `duration`.
    ///
    /// This is a completion-queue timer. For a deadline that cancels one request
    /// chain, use [`Request::timeout`] instead.
    pub fn timeout(&self, duration: Duration) -> Timeout {
        Timeout::new(self.clone(), duration)
    }
}

#[derive(Debug)]
struct TimeoutOp {
    state: Rc<TimeoutState>,
}

// Safety: `state` owns the stable timespec allocation referenced by the SQE.
// The lifecycle only permits changing it before the entry is submitted, and
// every timeout/control handle retains `state` through terminal completion.
unsafe impl Operation for TimeoutOp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Timeout::new(self.state.timespec.as_ptr())
            .flags(io_uring::types::TimeoutFlags::ETIME_SUCCESS)
            .build())
    }

    fn cleanup(&mut self, _: CQEResult) {
        self.state.mark_complete();
    }
}

impl Singleshot for TimeoutOp {
    type Output = io::Result<TimeoutOutcome>;

    fn complete(self, result: CQEResult) -> Self::Output {
        self.state.mark_complete();
        match result.into_result() {
            Err(err) if err.raw_os_error() == Some(libc::ETIME) => Ok(TimeoutOutcome::Expired),
            Err(err) if err.raw_os_error() == Some(libc::ECANCELED) => Ok(TimeoutOutcome::Canceled),
            Err(err) => Err(err),
            Ok(result) => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("standalone timeout completed with unexpected result {result}"),
            )),
        }
    }
}

pin_project_lite::pin_project! {
    struct LinkedTimeout {
        #[pin]
        inner: Op<LinkTimeoutOp>,
        control: LinkedTimeoutControl,
        done: bool,
    }

    impl PinnedDrop for LinkedTimeout {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if matches!(
                this.control.target.state.lifecycle.get(),
                TimeoutLifecycle::Prepared | TimeoutLifecycle::Queued
            ) {
                this.control.target.state.mark_complete();
            }
        }
    }
}

impl LinkedTimeout {
    fn new(reactor: crate::Handle, duration: Duration) -> Self {
        let state = Rc::new(TimeoutState::new(duration));
        let (inner, operation) = Op::new_with_target(
            LinkTimeoutOp {
                state: Rc::clone(&state),
            },
            reactor.clone(),
        );
        let control = LinkedTimeoutControl {
            target: TimeoutTarget {
                state,
                operation,
                reactor,
                kind: TimeoutKind::Linked,
            },
        };
        Self {
            inner,
            control,
            done: false,
        }
    }

    fn control(&self) -> LinkedTimeoutControl {
        self.control.clone()
    }
}

impl Future for LinkedTimeout {
    type Output = io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        assert!(!*this.done, "cannot poll linked timeout after completion");
        match Future::poll(this.inner.as_mut(), cx) {
            Poll::Ready(output) => {
                *this.done = true;
                Poll::Ready(output)
            }
            Poll::Pending => {
                this.control.target.state.mark_submitted();
                Poll::Pending
            }
        }
    }
}

impl private::Chainable for LinkedTimeout {
    fn reactor(&self) -> &crate::Handle {
        &self.control.target.reactor
    }

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        let this = self.project();
        let can_continue = this.inner.prepare_batch(batch);
        if can_continue {
            this.control.target.state.mark_queued();
        } else {
            this.control.target.state.mark_complete();
        }
        can_continue
    }

    fn cancel_unsubmitted(self: Pin<&mut Self>) {
        let this = self.project();
        this.inner.cancel_unsubmitted();
        this.control.target.state.mark_complete();
    }

    fn finish_submit(self: Pin<&mut Self>) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Queued => {
                this.inner.finish_submit();
                this.control.target.state.mark_submitted();
            }
            TimeoutLifecycle::Complete => {}
            TimeoutLifecycle::Prepared
            | TimeoutLifecycle::Submitted
            | TimeoutLifecycle::CanceledBeforeSubmit => {
                panic!("cannot submit linked timeout more than once")
            }
        }
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        let this = self.project();
        match this.control.target.state.lifecycle.get() {
            TimeoutLifecycle::Queued => {
                this.inner.fail_submit(err);
                this.control.target.state.mark_complete();
            }
            TimeoutLifecycle::Complete => {}
            TimeoutLifecycle::Prepared
            | TimeoutLifecycle::Submitted
            | TimeoutLifecycle::CanceledBeforeSubmit => {
                panic!("cannot fail linked timeout submission more than once")
            }
        }
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        self.project().inner.cancel_unfinished();
    }
}

#[derive(Debug)]
struct LinkTimeoutOp {
    state: Rc<TimeoutState>,
}

// Safety: the shared timeout state retains a stable timespec for the full
// lifetime of the linked request. Resets mutate it only before submission or
// use a separate `TimeoutUpdateOp` after submission.
unsafe impl Operation for LinkTimeoutOp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::LinkTimeout::new(self.state.timespec.as_ptr()).build())
    }

    fn cleanup(&mut self, _: CQEResult) {
        self.state.mark_complete();
    }
}

impl Singleshot for LinkTimeoutOp {
    type Output = io::Result<()>;

    fn complete(self, result: CQEResult) -> Self::Output {
        self.state.mark_complete();
        result.into_result().map(drop)
    }
}

pin_project_lite::pin_project! {
    /// A request future with a terminal linked timeout.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct WithTimeout<R>
    where
        R: Request,
    {
        #[pin]
        inner: ThenAux<R, LinkedTimeout>,
        control: LinkedTimeoutControl,
    }
}

impl<R> std::fmt::Debug for WithTimeout<R>
where
    R: Request,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WithTimeout")
            .field("control", &self.control)
            .finish_non_exhaustive()
    }
}

impl<R> WithTimeout<R>
where
    R: Request,
{
    pub(super) fn new(inner: R, duration: Duration) -> Self {
        let reactor = inner.reactor().clone();
        let timeout = LinkedTimeout::new(reactor, duration);
        let control = timeout.control();
        Self {
            inner: ThenAux::new(inner, timeout),
            control,
        }
    }

    /// Return a controller that can reset this request's linked deadline.
    pub fn control(&self) -> LinkedTimeoutControl {
        self.control.clone()
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

pin_project_lite::pin_project! {
    /// A lazy request that cancels a standalone timeout.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimeoutRemove {
        target: TimeoutTarget,
        #[pin]
        inner: Option<Op<TimeoutRemoveOp>>,
        initialized: bool,
        local_result: Option<bool>,
        waiting: Option<Waker>,
        done: bool,
    }

    impl PinnedDrop for TimeoutRemove {
        fn drop(this: Pin<&mut Self>) {
            let this = this.project();
            if let Some(waiter) = this.waiting.take() {
                this.target.state.unregister_waiter(&waiter);
            }
        }
    }
}

impl std::fmt::Debug for TimeoutRemove {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutRemove")
            .field("target", &self.target)
            .field("initialized", &self.initialized)
            .finish_non_exhaustive()
    }
}

impl TimeoutRemove {
    fn new(target: TimeoutTarget) -> Self {
        debug_assert_eq!(target.kind, TimeoutKind::Standalone);
        Self {
            target,
            inner: None,
            initialized: false,
            local_result: None,
            waiting: None,
            done: false,
        }
    }

    fn initialize(self: Pin<&mut Self>) -> bool {
        let mut this = self.project();
        if *this.initialized {
            return true;
        }

        if this.target.operation.is_complete() {
            this.target.state.mark_complete();
        }

        match this.target.state.lifecycle.get() {
            TimeoutLifecycle::Prepared => {
                this.target.state.mark_canceled_before_submit();
                *this.local_result = Some(true);
            }
            TimeoutLifecycle::Submitted => {
                this.inner.set(Some(Op::new(
                    TimeoutRemoveOp {
                        target: this.target.operation.clone(),
                    },
                    this.target.reactor.clone(),
                )));
            }
            TimeoutLifecycle::Queued => {
                return false;
            }
            TimeoutLifecycle::CanceledBeforeSubmit | TimeoutLifecycle::Complete => {
                *this.local_result = Some(false);
            }
        }
        *this.initialized = true;
        true
    }
}

impl Future for TimeoutRemove {
    type Output = io::Result<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.as_ref().get_ref().done,
            "cannot poll timeout removal after completion"
        );
        if !self.as_mut().initialize() {
            let this = self.as_mut().project();
            let replace = match this.waiting.as_ref() {
                Some(registered) => !registered.will_wake(cx.waker()),
                None => true,
            };
            if replace {
                if let Some(registered) = this.waiting.take() {
                    this.target.state.unregister_waiter(&registered);
                }
                this.target.state.register_waiter(cx.waker());
                *this.waiting = Some(cx.waker().clone());
            }
            return Poll::Pending;
        }
        let mut this = self.project();
        this.waiting.take();

        if let Some(result) = this.local_result.take() {
            *this.done = true;
            return Poll::Ready(Ok(result));
        }

        let inner = this
            .inner
            .as_mut()
            .as_pin_mut()
            .expect("initialized removal missing operation");
        match Future::poll(inner, cx) {
            Poll::Ready(output) => {
                *this.done = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl private::Chainable for TimeoutRemove {
    fn reactor(&self) -> &crate::Handle {
        &self.target.reactor
    }

    fn prepare_batch(mut self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        if !self.as_ref().get_ref().initialized
            && self.as_ref().get_ref().target.state.lifecycle.get() == TimeoutLifecycle::Queued
        {
            let target = self.as_ref().get_ref().target.operation.user_data();
            if let Some(position) = batch
                .iter()
                .position(|entry| entry.target_user_data() == target)
            {
                batch.remove(position);

                let this = self.as_mut().project();
                this.target.state.mark_canceled_before_submit();
                *this.local_result = Some(true);
                *this.initialized = true;
                return true;
            }

            // The target is waiting for capacity in a different submission.
            // Keep this request chainable by issuing a normal kernel removal;
            // its result accurately reports whether the target reached the
            // kernel before this batch did.
            let operation = self.as_ref().get_ref().target.operation.clone();
            let reactor = self.as_ref().get_ref().target.reactor.clone();
            let mut this = self.as_mut().project();
            this.inner.set(Some(Op::new(
                TimeoutRemoveOp { target: operation },
                reactor,
            )));
            *this.initialized = true;
        }

        let initialized = self.as_mut().initialize();
        debug_assert!(
            initialized,
            "queued removal should initialize during batch preparation"
        );
        self.project()
            .inner
            .as_pin_mut()
            .is_none_or(|inner| inner.prepare_batch(batch))
    }

    fn cancel_unsubmitted(mut self: Pin<&mut Self>) {
        if self.as_ref().get_ref().inner.is_none() {
            let operation = self.as_ref().get_ref().target.operation.clone();
            let reactor = self.as_ref().get_ref().target.reactor.clone();
            let mut this = self.as_mut().project();
            this.inner.set(Some(Op::new(
                TimeoutRemoveOp { target: operation },
                reactor,
            )));
            *this.local_result = None;
            *this.initialized = true;
        }
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.cancel_unsubmitted();
        }
    }

    fn finish_submit(self: Pin<&mut Self>) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.finish_submit();
        }
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.fail_submit(err);
        }
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.cancel_unfinished();
        }
    }
}

#[derive(Debug)]
struct TimeoutRemoveOp {
    target: OpTarget,
}

// Safety: the SQE contains only the target's copied `user_data` value. Owning
// `OpTarget` prevents that identity from being reused until this request's
// terminal completion has been reaped.
unsafe impl Operation for TimeoutRemoveOp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::TimeoutRemove::new(self.target.user_data()).build())
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for TimeoutRemoveOp {
    type Output = io::Result<bool>;

    fn complete(self, result: CQEResult) -> Self::Output {
        control_result("timeout removal", result)
    }
}

pin_project_lite::pin_project! {
    /// A lazy request that resets a standalone or linked timeout.
    #[must_use = "futures do nothing unless you `.await` or poll them"]
    pub struct TimeoutUpdate {
        target: TimeoutTarget,
        duration: Duration,
        #[pin]
        inner: Option<Op<TimeoutUpdateOp>>,
        initialized: bool,
        local_result: Option<bool>,
        done: bool,
    }
}

impl std::fmt::Debug for TimeoutUpdate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeoutUpdate")
            .field("target", &self.target)
            .field("duration", &self.duration)
            .field("initialized", &self.initialized)
            .finish_non_exhaustive()
    }
}

impl TimeoutUpdate {
    fn new(target: TimeoutTarget, duration: Duration) -> Self {
        Self {
            target,
            duration,
            inner: None,
            initialized: false,
            local_result: None,
            done: false,
        }
    }

    fn initialize(self: Pin<&mut Self>) {
        let mut this = self.project();
        if *this.initialized {
            return;
        }

        if this.target.operation.is_complete() {
            this.target.state.mark_complete();
        }

        match this.target.state.lifecycle.get() {
            TimeoutLifecycle::Prepared => {
                this.target.state.set_duration(*this.duration);
                *this.local_result = Some(true);
            }
            TimeoutLifecycle::Queued => {
                this.target.state.set_duration(*this.duration);
                *this.local_result = Some(true);
            }
            TimeoutLifecycle::Submitted => {
                this.inner.set(Some(Op::new(
                    TimeoutUpdateOp {
                        target: this.target.operation.clone(),
                        timespec: (*this.duration).into(),
                        kind: this.target.kind,
                    },
                    this.target.reactor.clone(),
                )));
            }
            TimeoutLifecycle::CanceledBeforeSubmit | TimeoutLifecycle::Complete => {
                *this.local_result = Some(false);
            }
        }
        *this.initialized = true;
    }
}

impl Future for TimeoutUpdate {
    type Output = io::Result<bool>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        assert!(
            !self.as_ref().get_ref().done,
            "cannot poll timeout update after completion"
        );
        self.as_mut().initialize();
        let mut this = self.project();

        if let Some(result) = this.local_result.take() {
            *this.done = true;
            return Poll::Ready(Ok(result));
        }

        let inner = this
            .inner
            .as_mut()
            .as_pin_mut()
            .expect("initialized update missing operation");
        match Future::poll(inner, cx) {
            Poll::Ready(output) => {
                if matches!(output, Ok(true)) {
                    this.target.state.duration.set(*this.duration);
                }
                *this.done = true;
                Poll::Ready(output)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl private::Chainable for TimeoutUpdate {
    fn reactor(&self) -> &crate::Handle {
        &self.target.reactor
    }

    fn prepare_batch(mut self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) -> bool {
        self.as_mut().initialize();
        self.project()
            .inner
            .as_pin_mut()
            .is_none_or(|inner| inner.prepare_batch(batch))
    }

    fn cancel_unsubmitted(mut self: Pin<&mut Self>) {
        if self.as_ref().get_ref().inner.is_none() {
            let target = self.as_ref().get_ref().target.clone();
            let duration = self.as_ref().get_ref().duration;
            let mut this = self.as_mut().project();
            this.inner.set(Some(Op::new(
                TimeoutUpdateOp {
                    target: target.operation,
                    timespec: duration.into(),
                    kind: target.kind,
                },
                target.reactor,
            )));
            *this.local_result = None;
            *this.initialized = true;
        }
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.cancel_unsubmitted();
        }
    }

    fn finish_submit(self: Pin<&mut Self>) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.finish_submit();
        }
    }

    fn fail_submit(self: Pin<&mut Self>, err: &SubmitError) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.fail_submit(err);
        }
    }

    fn cancel_unfinished(self: Pin<&mut Self>) {
        if let Some(inner) = self.project().inner.as_pin_mut() {
            inner.cancel_unfinished();
        }
    }
}

#[derive(Debug)]
struct TimeoutUpdateOp {
    target: OpTarget,
    timespec: io_uring::types::Timespec,
    kind: TimeoutKind,
}

// Safety: the update timespec is stored inline in this stable operation and
// remains alive through terminal completion. `OpTarget` keeps the target's
// `user_data` identity allocated for the same period.
unsafe impl Operation for TimeoutUpdateOp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let flags = match self.kind {
            TimeoutKind::Standalone => io_uring::types::TimeoutFlags::empty(),
            TimeoutKind::Linked => io_uring::types::TimeoutFlags::LINK_TIMEOUT_UPDATE,
        };
        Ok(
            io_uring::opcode::TimeoutUpdate::new(self.target.user_data(), &self.timespec)
                .flags(flags)
                .build(),
        )
    }

    fn cleanup(&mut self, _: CQEResult) {}
}

impl Singleshot for TimeoutUpdateOp {
    type Output = io::Result<bool>;

    fn complete(self, result: CQEResult) -> Self::Output {
        control_result("timeout update", result)
    }
}

fn control_result(action: &'static str, result: CQEResult) -> io::Result<bool> {
    match result.into_result() {
        Ok(0) => Ok(true),
        Err(err)
            if matches!(
                err.raw_os_error(),
                Some(libc::ENOENT | libc::EALREADY | libc::EBUSY)
            ) =>
        {
            Ok(false)
        }
        Err(err) => Err(err),
        Ok(result) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{action} completed with unexpected result {result}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::task::Poll;

    use futures_test::task::noop_waker;
    use norn_executor::park::{Park, ParkMode};
    use norn_executor::LocalExecutor;

    use super::*;

    #[derive(Debug)]
    struct Nop;

    unsafe impl Operation for Nop {
        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(io_uring::opcode::Nop::new().build())
        }

        fn cleanup(&mut self, _: CQEResult) {}
    }

    impl Singleshot for Nop {
        type Output = io::Result<()>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.into_result().map(drop)
        }
    }

    #[derive(Debug)]
    struct ConfigureFails;

    unsafe impl Operation for ConfigureFails {
        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        }

        fn cleanup(&mut self, _: CQEResult) {}
    }

    impl Singleshot for ConfigureFails {
        type Output = io::Result<()>;

        fn complete(self, result: CQEResult) -> Self::Output {
            result.into_result().map(drop)
        }
    }

    fn poll_ready<F: Future>(future: Pin<&mut F>) -> F::Output {
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);
        match Future::poll(future, &mut cx) {
            Poll::Ready(output) => output,
            Poll::Pending => panic!("future should complete locally"),
        }
    }

    #[test]
    fn cancel_before_first_poll_prevents_submission() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        let original_target = control.target.operation.user_data();

        let mut cancel = std::pin::pin!(control.cancel());
        assert!(poll_ready(cancel.as_mut()).unwrap());

        let mut timeout = std::pin::pin!(timeout);
        assert_eq!(
            poll_ready(timeout.as_mut()).unwrap(),
            TimeoutOutcome::Canceled
        );

        let newer = driver.handle().timeout(Duration::from_secs(60));
        assert_ne!(
            original_target,
            newer.control.target.operation.user_data(),
            "a live control handle must retain the canceled target allocation"
        );
    }

    #[test]
    fn reset_before_first_poll_updates_initial_duration() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();

        let mut reset = std::pin::pin!(control.reset(Duration::from_millis(25)));
        assert!(poll_ready(reset.as_mut()).unwrap());
        assert_eq!(
            timeout.control.target.state.duration.get(),
            Duration::from_millis(25)
        );

        let mut cancel = std::pin::pin!(control.cancel());
        assert!(poll_ready(cancel.as_mut()).unwrap());
        let mut timeout = std::pin::pin!(timeout);
        assert_eq!(
            poll_ready(timeout.as_mut()).unwrap(),
            TimeoutOutcome::Canceled
        );
    }

    #[test]
    fn repeated_local_cancel_is_idempotent() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();

        let mut first = std::pin::pin!(control.cancel());
        let mut second = std::pin::pin!(control.cancel());
        assert!(poll_ready(first.as_mut()).unwrap());
        assert!(!poll_ready(second.as_mut()).unwrap());
    }

    #[test]
    fn all_local_control_chain_needs_no_submission_entries() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut executor = LocalExecutor::new(driver);

        let (canceled, reset) =
            executor.block_on(control.cancel().then(control.reset(Duration::from_secs(1))));
        assert!(canceled.unwrap());
        assert!(!reset.unwrap());
        drop(timeout);
    }

    #[test]
    fn configure_failure_cancels_later_timeout_locally() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let timeout = handle.timeout(Duration::from_secs(60));
        let mut executor = LocalExecutor::new(driver);

        let (failed, timeout) = executor.block_on(handle.submit(ConfigureFails).then(timeout));
        assert_eq!(failed.unwrap_err().raw_os_error(), Some(libc::EINVAL));
        assert_eq!(timeout.unwrap(), TimeoutOutcome::Canceled);
    }

    #[test]
    fn configure_failure_cancels_later_timeout_controls() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut executor = LocalExecutor::new(driver);

        let (failed, reset) = executor.block_on(
            handle
                .submit(ConfigureFails)
                .then(control.reset(Duration::from_secs(1))),
        );
        assert_eq!(failed.unwrap_err().raw_os_error(), Some(libc::EINVAL));
        assert_eq!(reset.unwrap_err().raw_os_error(), Some(libc::ECANCELED));

        let (failed, cancel) =
            executor.block_on(handle.submit(ConfigureFails).then(control.cancel()));
        assert_eq!(failed.unwrap_err().raw_os_error(), Some(libc::EINVAL));
        assert_eq!(cancel.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        drop(timeout);
    }

    #[test]
    fn configure_failure_before_linked_timeout_returns_the_error() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let request = handle
            .submit(ConfigureFails)
            .timeout(Duration::from_secs(60));
        let control = request.control();
        let mut executor = LocalExecutor::new(driver);

        let error = executor.block_on(request).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Complete
        );

        let mut reset = std::pin::pin!(control.reset(Duration::from_secs(1)));
        assert!(!poll_ready(reset.as_mut()).unwrap());
    }

    #[test]
    fn partial_batch_configure_failure_cancels_linked_timeout() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let request = handle
            .submit(Nop)
            .then(handle.submit(ConfigureFails))
            .timeout(Duration::from_secs(60));
        let control = request.control();
        let mut executor = LocalExecutor::new(driver);

        let (nop, failed) = executor.block_on(request);
        nop.unwrap();
        assert_eq!(failed.unwrap_err().raw_os_error(), Some(libc::EINVAL));
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Complete
        );
    }

    #[test]
    fn controller_observes_timeout_dropped_before_submission() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        drop(timeout);

        let mut cancel = std::pin::pin!(control.cancel());
        let mut reset = std::pin::pin!(control.reset(Duration::from_secs(1)));
        assert!(!poll_ready(cancel.as_mut()).unwrap());
        assert!(!poll_ready(reset.as_mut()).unwrap());
    }

    #[test]
    fn linked_reset_before_submission_updates_initial_deadline() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let request = driver.handle().submit(Nop).timeout(Duration::from_secs(60));
        let control = request.control();

        let mut reset = std::pin::pin!(control.reset(Duration::from_millis(25)));
        assert!(poll_ready(reset.as_mut()).unwrap());
        assert_eq!(
            control.target.state.duration.get(),
            Duration::from_millis(25)
        );
        drop(request);

        let mut reset_after_drop = std::pin::pin!(control.reset(Duration::from_secs(1)));
        assert!(!poll_ready(reset_after_drop.as_mut()).unwrap());
    }

    #[test]
    fn timeout_chain_rejects_oversized_batch_atomically() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);

        let (timeout, nop) = executor.block_on(
            handle
                .timeout(Duration::from_secs(60))
                .then(handle.submit(Nop)),
        );
        assert_eq!(timeout.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(nop.unwrap_err().kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn timeout_expiration_keeps_dependent_link_running() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let mut executor = LocalExecutor::new(driver);

        let (timeout, nop) = executor.block_on(
            handle
                .timeout(Duration::from_millis(1))
                .then(handle.submit(Nop)),
        );
        assert_eq!(timeout.unwrap(), TimeoutOutcome::Expired);
        nop.unwrap();
    }

    #[test]
    fn same_batch_cancel_removes_the_prepared_timeout_entry() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut executor = LocalExecutor::new(driver);

        let (timeout, canceled) = executor.block_on(timeout.then(control.cancel()));
        assert_eq!(timeout.unwrap(), TimeoutOutcome::Canceled);
        assert!(canceled.unwrap());
    }

    #[test]
    fn cancel_before_timeout_in_same_batch_stays_local() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut executor = LocalExecutor::new(driver);

        let (canceled, timeout) = executor.block_on(control.cancel().then(timeout));
        assert!(canceled.unwrap());
        assert_eq!(timeout.unwrap(), TimeoutOutcome::Canceled);
    }

    #[test]
    fn same_batch_reset_updates_the_prepared_timeout_entry() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let timeout = driver.handle().timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut executor = LocalExecutor::new(driver);

        let (timeout, reset) =
            executor.block_on(timeout.then(control.reset(Duration::from_millis(1))));
        assert_eq!(timeout.unwrap(), TimeoutOutcome::Expired);
        assert!(reset.unwrap());
    }

    #[test]
    fn cancel_waits_while_timeout_is_queued_for_sq_capacity() {
        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut filler = std::pin::pin!(handle.submit(Nop));
        assert!(Future::poll(filler.as_mut(), &mut cx).is_pending());

        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut timeout = std::pin::pin!(timeout);
        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Queued
        );

        let mut abandoned_cancel = Box::pin(control.cancel());
        assert!(Future::poll(abandoned_cancel.as_mut(), &mut cx).is_pending());
        assert_eq!(control.target.state.waiters.borrow().len(), 1);
        drop(abandoned_cancel);
        assert!(control.target.state.waiters.borrow().is_empty());

        let mut cancel = std::pin::pin!(control.cancel());
        assert!(Future::poll(cancel.as_mut(), &mut cx).is_pending());
        assert_eq!(control.target.state.waiters.borrow().len(), 1);

        driver.park(ParkMode::NoPark).unwrap();
        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Submitted
        );
        assert!(control.target.state.waiters.borrow().is_empty());

        assert!(Future::poll(cancel.as_mut(), &mut cx).is_pending());

        driver.park(ParkMode::NoPark).unwrap();
        assert!(Future::poll(cancel.as_mut(), &mut cx).is_pending());

        let mut cancel_output = None;
        let mut timeout_output = None;
        for _ in 0..3 {
            driver.park(ParkMode::NextCompletion).unwrap();
            if cancel_output.is_none() {
                if let Poll::Ready(output) = Future::poll(cancel.as_mut(), &mut cx) {
                    cancel_output = Some(output);
                }
            }
            if timeout_output.is_none() {
                if let Poll::Ready(output) = Future::poll(timeout.as_mut(), &mut cx) {
                    timeout_output = Some(output);
                }
            }
            if cancel_output.is_some() && timeout_output.is_some() {
                break;
            }
        }

        assert!(cancel_output
            .expect("cancellation did not complete")
            .unwrap());
        assert_eq!(
            timeout_output.expect("timeout did not complete").unwrap(),
            TimeoutOutcome::Canceled
        );
    }

    #[test]
    fn queued_timeout_cancel_remains_chainable_from_another_batch() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut filler = std::pin::pin!(handle.submit(Nop));
        assert!(Future::poll(filler.as_mut(), &mut cx).is_pending());

        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut timeout = std::pin::pin!(timeout);
        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Queued
        );

        let mut cancel = std::pin::pin!(control.cancel());
        let mut batch = SmallVec::new();
        private::Chainable::prepare_batch(cancel.as_mut(), &mut batch);
        assert_eq!(batch.len(), 1, "kernel removal should remain chainable");
    }

    #[test]
    fn reset_updates_timeout_waiting_for_sq_capacity() {
        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 1).unwrap();
        let handle = driver.handle();
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        let mut filler = std::pin::pin!(handle.submit(Nop));
        assert!(Future::poll(filler.as_mut(), &mut cx).is_pending());

        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut timeout = std::pin::pin!(timeout);
        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        assert_eq!(
            control.target.state.lifecycle.get(),
            TimeoutLifecycle::Queued
        );

        let mut reset = std::pin::pin!(control.reset(Duration::from_millis(1)));
        assert!(poll_ready(reset.as_mut()).unwrap());
        assert_eq!(
            control.target.state.duration.get(),
            Duration::from_millis(1)
        );

        driver.park(ParkMode::NoPark).unwrap();
        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());

        let mut timeout_output = None;
        for _ in 0..3 {
            driver.park(ParkMode::NextCompletion).unwrap();
            if let Poll::Ready(output) = Future::poll(timeout.as_mut(), &mut cx) {
                timeout_output = Some(output);
                break;
            }
        }
        assert_eq!(
            timeout_output
                .expect("reset timeout did not complete")
                .unwrap(),
            TimeoutOutcome::Expired
        );
    }

    #[test]
    fn concurrent_timeout_control_completion_is_not_an_error() {
        for errno in [libc::ENOENT, libc::EALREADY, libc::EBUSY] {
            let result = CQEResult::synthetic(Err(io::Error::from_raw_os_error(errno)));
            assert!(!control_result("timeout control", result).unwrap());
        }
    }

    #[test]
    fn shutdown_cancels_a_live_timeout_and_completes_its_controls() {
        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut timeout = std::pin::pin!(timeout);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        driver.park(ParkMode::NoPark).unwrap();
        Park::shutdown(&mut driver);

        assert_eq!(
            poll_ready(timeout.as_mut()).unwrap(),
            TimeoutOutcome::Canceled
        );
        let mut cancel = std::pin::pin!(control.cancel());
        let mut reset = std::pin::pin!(control.reset(Duration::from_secs(1)));
        assert!(!poll_ready(cancel.as_mut()).unwrap());
        assert!(!poll_ready(reset.as_mut()).unwrap());
    }

    #[test]
    fn dropping_a_submitted_timeout_is_reclaimed_during_shutdown() {
        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let timeout = handle.timeout(Duration::from_secs(60));
        let control = timeout.control();
        let mut timeout = Box::pin(timeout);
        let waker = noop_waker();
        let mut cx = Context::from_waker(&waker);

        assert!(Future::poll(timeout.as_mut(), &mut cx).is_pending());
        driver.park(ParkMode::NoPark).unwrap();
        drop(timeout);
        Park::shutdown(&mut driver);

        let mut cancel = std::pin::pin!(control.cancel());
        let mut reset = std::pin::pin!(control.reset(Duration::from_secs(1)));
        assert!(!poll_ready(cancel.as_mut()).unwrap());
        assert!(!poll_ready(reset.as_mut()).unwrap());
    }
}
