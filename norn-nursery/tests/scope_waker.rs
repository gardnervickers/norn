//! Regression coverage for replacing the scope waker.

use std::cell::RefCell;
use std::future::{pending, poll_fn, Future};
use std::pin::pin;
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

thread_local! {
    static CHILD_WAKER: RefCell<Option<Waker>> = const { RefCell::new(None) };
}

/// A scope waker whose destructor wakes one of the scope's children, for
/// example because dropping the last waker handle closes a channel the child
/// is waiting on.
struct WakeChildOnDrop;

// Not a no-op: the behavior under test lives in `Drop`.
#[allow(clippy::manual_noop_waker)]
impl Wake for WakeChildOnDrop {
    fn wake(self: Arc<Self>) {}
}

impl Drop for WakeChildOnDrop {
    fn drop(&mut self) {
        if let Some(waker) = CHILD_WAKER.with(|slot| slot.borrow_mut().take()) {
            waker.wake();
        }
    }
}

/// Replacing the stored scope waker must not drop the previous waker while
/// the slot is borrowed: its destructor may schedule a child, which
/// re-enters the scope waker slot.
#[test]
fn replacing_scope_waker_allows_old_waker_destructor_to_wake_a_child() {
    let mut scope = pin!(norn_nursery::scope!(|scope| {
        std::mem::drop(scope.spawn(async move |_| {
            poll_fn(|cx: &mut Context<'_>| {
                CHILD_WAKER.with(|slot| *slot.borrow_mut() = Some(cx.waker().clone()));
                Poll::<()>::Pending
            })
            .await
        }));
        pending::<()>().await
    }));

    // The first poll stores a clone of `old` as the scope waker.
    let old = Waker::from(Arc::new(WakeChildOnDrop));
    assert!(scope
        .as_mut()
        .poll(&mut Context::from_waker(&old))
        .is_pending());
    drop(old);
    assert!(CHILD_WAKER.with(|slot| slot.borrow().is_some()));

    // Polling with a different waker replaces, and drops, the old one.
    assert!(scope
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert!(CHILD_WAKER.with(|slot| slot.borrow().is_none()));
}
