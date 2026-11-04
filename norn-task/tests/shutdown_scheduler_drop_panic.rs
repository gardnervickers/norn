//! A panicking scheduler destructor must not stop `TaskSet` shutdown.
use std::cell::{Cell, RefCell};
use std::future::{pending, Future};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_task::{JoinHandle, RegisteredTask, Runnable, Schedule, TaskSet};

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

thread_local! {
    static JOIN: RefCell<Option<JoinHandle<()>>> = const { RefCell::new(None) };
}

/// A join waker that drops the last other reference to its task, then panics.
struct DropJoinThenPanic;

impl Wake for DropJoinThenPanic {
    fn wake(self: Arc<Self>) {
        let join = JOIN.with(|join| join.borrow_mut().take());
        drop(join);
        panic!("join waker panicked");
    }
}

struct AlwaysPanicOnDrop;

impl Schedule for AlwaysPanicOnDrop {
    fn schedule(&self, _runnable: Runnable) {}
    fn unbind(&self, _registered: &RegisteredTask) {}
}

impl Drop for AlwaysPanicOnDrop {
    fn drop(&mut self) {
        panic!("scheduler drop panicked");
    }
}

/// When `shutdown()` panics and the scheduler's destructor panics too, the
/// second panic must not unwind through the first and abort the process.
#[test]
fn join_waker_panic_and_scheduler_drop_panic_do_not_abort() {
    let set = TaskSet::new();
    let (runnable, mut join) = unsafe { set.bind(pending::<()>(), AlwaysPanicOnDrop) };
    drop(runnable);
    let waker = Waker::from(Arc::new(DropJoinThenPanic));
    assert!(Pin::new(&mut join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
    drop(waker);
    JOIN.with(|slot| *slot.borrow_mut() = Some(join));

    let result = panic::catch_unwind(AssertUnwindSafe(|| drop(set)));
    assert!(result.is_err());
}
