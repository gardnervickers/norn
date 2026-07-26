use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{ready, Context, Poll};

use smallvec::SmallVec;

use crate::driver::{Shared, Status};
use crate::error::SubmitError;
use crate::operation::ConfiguredEntry;
use crate::util::notify::Notified;

use super::LOG;

fn into_static_shared(shared: Rc<Shared>) -> &'static Shared {
    let shared = Rc::into_raw(shared);
    // Safety: we leaked the Rc via into_raw and only reconstruct it from this
    // pointer in the associated Drop impl.
    unsafe { &*shared }
}

fn drop_static_shared(shared: &'static Shared) {
    // Safety: this pointer came from Rc::into_raw in into_static_shared.
    let shared = unsafe { Rc::from_raw(shared) };
    drop(shared);
}

pin_project_lite::pin_project! {
    struct PushFutureInner<'a, P> {
        shared: &'a Shared,
        #[pin]
        notify: Option<Notified<'a>>,
        pending: P,
    }
}

pub(crate) struct SingleSubmission(Option<ConfiguredEntry>);

pub(crate) struct BatchSubmission(SmallVec<[ConfiguredEntry; 4]>);

trait PendingSubmission {
    fn try_submit(&mut self, shared: &Shared) -> Result<bool, SubmitError>;
}

impl PendingSubmission for SingleSubmission {
    fn try_submit(&mut self, shared: &Shared) -> Result<bool, SubmitError> {
        let entry = self.0.take().expect("entry already submitted");
        match shared.try_push(entry) {
            Ok(()) => Ok(true),
            Err(entry) => {
                self.0 = Some(entry);
                Ok(false)
            }
        }
    }
}

impl PendingSubmission for BatchSubmission {
    fn try_submit(&mut self, shared: &Shared) -> Result<bool, SubmitError> {
        shared.validate_batch_len(self.0.len())?;
        Ok(shared.try_push_batch(&mut self.0))
    }
}

impl PushFutureImpl<SingleSubmission> {
    pub(super) fn new(shared: Rc<Shared>, entry: ConfiguredEntry) -> Self {
        let shared = into_static_shared(shared);
        let inner = PushFutureInner {
            shared,
            notify: Some(shared.backpressure.wait()),
            pending: SingleSubmission(Some(entry)),
        };
        PushFutureImpl {
            shared: Some(shared),
            fut: Some(inner),
        }
    }
}

impl PushFutureImpl<BatchSubmission> {
    pub(super) fn new_batch(shared: Rc<Shared>, entries: SmallVec<[ConfiguredEntry; 4]>) -> Self {
        let shared = into_static_shared(shared);
        let inner = PushFutureInner {
            shared,
            notify: None,
            pending: BatchSubmission(entries),
        };
        PushFutureImpl {
            shared: Some(shared),
            fut: Some(inner),
        }
    }
}

impl<P> Future for PushFutureInner<'_, P>
where
    P: PendingSubmission,
{
    type Output = Result<(), SubmitError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();

        loop {
            if this.shared.status() != Status::Running {
                log::trace!(target: LOG, "ring.push.sutting_down");
                if let Some(err) = this.shared.health_error() {
                    return Poll::Ready(Err(SubmitError::broken(err)));
                }
                return Poll::Ready(Err(SubmitError::shutting_down()));
            }
            if let Some(notify) = this.notify.as_mut().as_pin_mut() {
                ready!(notify.poll(cx));
                Pin::set(&mut this.notify, None);
            }

            match this.pending.try_submit(this.shared) {
                Ok(true) => {
                    log::trace!(target: LOG, "ring.push.ok");
                    return Poll::Ready(Ok(()));
                }
                Ok(false) => {
                    log::trace!(target: LOG, "ring.push.full");
                    Pin::set(&mut this.notify, Some(this.shared.backpressure.wait()));
                }
                Err(err) => return Poll::Ready(Err(err)),
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// A future which guarantees that the reactor will not be dropped
    pub(crate) struct PushFutureImpl<P> {
        shared: Option<&'static Shared>,
        #[pin]
        fut: Option<PushFutureInner<'static, P>>,
    }

    impl<P> PinnedDrop for PushFutureImpl<P> {
        fn drop(this: Pin<&mut Self>) {
            let mut me = this.project();
            me.fut.set(None);
            if let Some(shared) = me.shared.take() {
                drop_static_shared(shared);
            }
        }
    }
}

impl<P> Future for PushFutureImpl<P>
where
    P: PendingSubmission,
{
    type Output = Result<(), SubmitError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.as_mut().project();
        let fut = this
            .fut
            .as_pin_mut()
            .expect("cannot poll future after completion");
        fut.poll(cx)
    }
}

pub(crate) type PushFuture = PushFutureImpl<SingleSubmission>;
pub(crate) type PushBatchFuture = PushFutureImpl<BatchSubmission>;

#[cfg(test)]
mod tests {
    use super::{PushBatchFuture, PushFuture};

    #[test]
    fn single_entry_waiter_does_not_reserve_batch_storage() {
        assert!(
            std::mem::size_of::<PushFuture>() * 2 < std::mem::size_of::<PushBatchFuture>(),
            "single-entry submission should remain materially smaller than batch submission"
        );
    }
}
