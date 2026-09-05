//! Regression coverage for bounded nursery child polling.

use std::cell::Cell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

struct SelfWake {
    polls: Rc<Cell<usize>>,
    stop_at: usize,
}

impl Future for SelfWake {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let polls = self.polls.get() + 1;
        self.polls.set(polls);
        if polls == self.stop_at {
            Poll::Ready(())
        } else {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct WakeCounter(AtomicUsize);

impl Wake for WakeCounter {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn nursery_yields_and_notifies_while_children_keep_rescheduling() {
    let polls = Rc::new(Cell::new(0));
    let polls_by_child = Rc::clone(&polls);
    let mut nursery = Box::pin(async move {
        norn_nursery::scope!(|scope| {
            std::mem::drop(scope.spawn(async move |_| {
                SelfWake {
                    polls: polls_by_child,
                    stop_at: 1_024,
                }
                .await
            }));
        })
        .await;
    });
    let wake_count = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake_count));
    let mut cx = Context::from_waker(&waker);

    let first_poll = nursery.as_mut().poll(&mut cx);
    assert!(
        first_poll.is_pending() && polls.get() < 1_024,
        "one nursery poll drained all {} child polls without yielding",
        polls.get(),
    );
    assert_eq!(
        wake_count.0.load(Ordering::Relaxed),
        1,
        "bounded child work must notify the outer scope waker",
    );
}
