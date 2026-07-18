use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;

use futures::FutureExt;

use crate::{RegisteredTask, Runnable, Schedule, TaskSet};

use super::{TestFuture, TestState};

struct PanicOnUnbind {
    _tasks: Rc<TaskSet>,
}

impl Schedule for PanicOnUnbind {
    fn schedule(&self, _runnable: Runnable) {
        panic!("completed task must not be rescheduled");
    }

    fn unbind(&self, _registered: &RegisteredTask) {
        panic!("unbind panic");
    }
}

#[test]
fn panic_during_poll() {
    let _e = TestState::enter();
    TestState::with(|v| {
        v.panic_on_run = true;
    });

    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    spawner.next().unwrap().run();

    TestState::with(|v| {
        assert_eq!(v.num_polls, 1);
        assert!(v.task_dropped);
        assert!(!v.output_dropped);
    });
    assert!(handle.now_or_never().unwrap().is_err());
}

#[test]
fn panic_during_poll_abort() {
    let _e = TestState::enter();
    TestState::with(|v| {
        v.panic_on_run = true;
    });

    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(TestFuture);
    spawner.next().unwrap().run();
    handle.abort();
    TestState::with(|v| {
        assert_eq!(v.num_polls, 1);
        assert!(v.task_dropped);
        assert!(!v.output_dropped);
    });
    assert!(handle.now_or_never().unwrap().is_err());
}

#[test]
fn panic_during_unbind_preserves_registered_reference() {
    let tasks = Rc::new(TaskSet::new());
    let scheduler = PanicOnUnbind {
        _tasks: Rc::clone(&tasks),
    };
    // Safety: the future and its output are both 'static.
    let (runnable, handle) = unsafe { tasks.bind(async {}, scheduler) };

    let panic = panic::catch_unwind(AssertUnwindSafe(|| runnable.unwrap().run()));
    assert!(panic.is_err());

    drop(handle);
    tasks.shutdown();
    assert_eq!(Rc::strong_count(&tasks), 1);
}
