#![allow(private_interfaces)]

use std::future::Future;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use smallvec::SmallVec;

use crate::driver::PushFuture;
use crate::error::SubmitError;
use crate::operation::{ConfiguredEntry, Op, Singleshot};

mod private {
    use super::*;

    pub trait Chainable: Future {
        fn reactor(&self) -> &crate::Handle;
        fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>);
        fn finish_submit(self: Pin<&mut Self>);
        fn fail_submit(self: Pin<&mut Self>, err: &SubmitError);
        fn cancel_unfinished(self: Pin<&mut Self>);
    }
}

/// A lazy request that can be linked with other requests before submission.
pub trait Request: Future + Sized + private::Chainable {
    /// Link another request and return both results together.
    fn then<R>(self, next: R) -> Then<Self, R>
    where
        R: Request,
    {
        Then::new(self, next)
    }

    /// Link another request but discard its output.
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
}

impl<T> Request for T where T: Future + Sized + private::Chainable {}

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
                    left.as_mut().prepare_batch(&mut batch);
                    right.as_mut().prepare_batch(&mut batch);
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
                    left.as_mut().prepare_batch(&mut batch);
                    right.as_mut().prepare_batch(&mut batch);
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

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) {
        Op::prepare_batch(self, batch);
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

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) {
        let this = self.project();
        let ThenStateProj::Pending { left, right, .. } = this.state.project() else {
            panic!("cannot prepare completed request");
        };
        left.prepare_batch(batch);
        right.prepare_batch(batch);
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

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) {
        let this = self.project();
        let ThenAuxStateProj::Pending { left, right, .. } = this.state.project() else {
            panic!("cannot prepare completed request");
        };
        left.prepare_batch(batch);
        right.prepare_batch(batch);
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

    fn prepare_batch(self: Pin<&mut Self>, batch: &mut SmallVec<[ConfiguredEntry; 4]>) {
        self.project().inner.prepare_batch(batch);
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
