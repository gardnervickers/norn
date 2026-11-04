//! Completing a task must release its scheduler registration even when the
//! join waker it notifies panics.
use std::cell::Cell;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_task::{RegisteredTask, Runnable, Schedule, TaskSet};

struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("join waker panicked");
    }
}

struct CountingUnbind {
    set: Rc<TaskSet>,
    unbinds: Rc<Cell<usize>>,
}

impl Schedule for CountingUnbind {
    fn schedule(&self, _runnable: Runnable) {}

    fn unbind(&self, registered: &RegisteredTask) {
        self.unbinds.set(self.unbinds.get() + 1);
        unsafe { self.set.remove(registered) };
    }
}

#[test]
fn completing_task_is_unbound_even_if_its_join_waker_panics() {
    let set = Rc::new(TaskSet::new());
    let unbinds = Rc::new(Cell::new(0));
    // Safety: the future and output are 'static.
    let (runnable, mut join) = unsafe {
        set.bind(
            async {},
            CountingUnbind {
                set: Rc::clone(&set),
                unbinds: Rc::clone(&unbinds),
            },
        )
    };
    let waker = Waker::from(Arc::new(PanicOnWake));
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());

    let ran = panic::catch_unwind(AssertUnwindSafe(|| runnable.unwrap().run()));
    assert!(ran.is_err(), "the join waker's panic propagates");
    assert_eq!(unbinds.get(), 1, "a completed task was never unbound");

    // The output is still delivered to the join handle.
    let noop = Waker::noop();
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(noop))
        .is_ready());
}
