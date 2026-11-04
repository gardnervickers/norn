//! A panicking scheduler `unbind` must still wake the join handle.
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_task::{RegisteredTask, Runnable, Schedule, TaskSet};

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct PanicOnUnbind;

impl Schedule for PanicOnUnbind {
    fn schedule(&self, _runnable: Runnable) {}
    fn unbind(&self, _registered: &RegisteredTask) {
        panic!("unbind panicked");
    }
}

/// A panicking `unbind` must not drop the join waker without waking it.
#[test]
fn unbind_panic_still_wakes_join_handle() {
    let set = TaskSet::new();
    let (runnable, mut join) = unsafe { set.bind(async {}, PanicOnUnbind) };
    let counter = Arc::new(CountWake(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&counter));
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    let ran = panic::catch_unwind(AssertUnwindSafe(|| runnable.unwrap().run()));
    assert!(ran.is_err());
    assert_eq!(
        counter.0.load(Ordering::SeqCst),
        1,
        "join waker was dropped without being woken"
    );
    drop(join);
    drop(set);
}
