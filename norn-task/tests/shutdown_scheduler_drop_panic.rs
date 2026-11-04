//! A panicking scheduler destructor must not stop `TaskSet` shutdown.
use std::cell::Cell;
use std::future::pending;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;

use norn_task::{RegisteredTask, Runnable, Schedule, TaskSet};

struct CountDrop(Rc<Cell<usize>>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

/// A scheduler whose destructor panics when armed.
struct PanicOnDropScheduler {
    armed: bool,
}

impl Schedule for PanicOnDropScheduler {
    fn schedule(&self, _runnable: Runnable) {}
    fn unbind(&self, _registered: &RegisteredTask) {}
}

impl Drop for PanicOnDropScheduler {
    fn drop(&mut self) {
        if self.armed {
            panic!("scheduler drop panicked");
        }
    }
}

/// The `RegisteredTask` popped in `TaskSet::shutdown` is dropped
/// outside `catch_unwind`. If it is the last reference, deallocation drops the
/// scheduler, and a panic there skips the remaining tasks.
#[test]
fn scheduler_drop_panic_during_shutdown_still_destroys_remaining_futures() {
    let set = TaskSet::new();
    let (runnable, join) =
        unsafe { set.bind(pending::<()>(), PanicOnDropScheduler { armed: true }) };
    drop(runnable);
    join.detach();

    let drops = Rc::new(Cell::new(0));
    let guard = CountDrop(Rc::clone(&drops));
    let (second_runnable, _second) = unsafe {
        set.bind(
            async move {
                let _guard = guard;
                pending::<()>().await;
            },
            PanicOnDropScheduler { armed: false },
        )
    };

    let dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(set)));
    assert!(dropped.is_err());
    assert_eq!(drops.get(), 1, "second future outlived the set");
    drop(second_runnable);
}
