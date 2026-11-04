//! A panicking scheduler `unbind` must still wake the join handle.
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_task::{RegisteredTask, Runnable, Schedule, TaskSet};

struct CountWake(AtomicUsize);

impl Wake for CountWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Panics on the first `unbind` only, so dropping the set can unlink the task.
struct PanicOnUnbind(AtomicBool);

impl Schedule for PanicOnUnbind {
    fn schedule(&self, _runnable: Runnable) {}
    fn unbind(&self, _registered: &RegisteredTask) {
        if self.0.swap(false, Ordering::SeqCst) {
            panic!("unbind panicked");
        }
    }
}

/// A panicking `unbind` must not drop the join waker without waking it.
#[test]
fn unbind_panic_still_wakes_join_handle() {
    let set = TaskSet::new();
    let (runnable, mut join) = unsafe { set.bind(async {}, PanicOnUnbind(AtomicBool::new(true))) };
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
