use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{ready, Context, Poll};

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
    struct PushFutureInner<'a> {
        shared: &'a Shared,
        #[pin]
        notify: Option<Notified<'a>>,
        entry: Option<ConfiguredEntry>,
    }
}

impl PushFuture {
    pub(super) fn new(shared: Rc<Shared>, entry: ConfiguredEntry) -> Self {
        let shared = into_static_shared(shared);
        let inner = PushFutureInner {
            shared,
            notify: None,
            entry: Some(entry),
        };
        PushFuture {
            shared: Some(shared),
            fut: Some(inner),
        }
    }
}

impl Future for PushFutureInner<'_> {
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

            if let Err(entry) = this
                .shared
                .try_push(this.entry.take().expect("entry already submitted"))
            {
                // Put the entry back
                *this.entry = Some(entry);
                // Wait for the submission queue to have space
                log::trace!(target: LOG, "ring.push.full");
                Pin::set(&mut this.notify, Some(this.shared.backpressure.wait()));
                continue;
            }
            log::trace!(target: LOG, "ring.push.ok");
            return Poll::Ready(Ok(()));
        }
    }
}

pin_project_lite::pin_project! {
    /// A future which guarantees that the reactor will not be dropped
    pub(crate) struct PushFuture {
        shared: Option<&'static Shared>,
        #[pin]
        fut: Option<PushFutureInner<'static>>,
    }

    impl PinnedDrop for PushFuture {
        fn drop(this: Pin<&mut Self>) {
            let mut me = this.project();
            me.fut.set(None);
            if let Some(shared) = me.shared.take() {
                drop_static_shared(shared);
            }
        }
    }
}

impl Future for PushFuture {
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
