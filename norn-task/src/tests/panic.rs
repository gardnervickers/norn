use std::cell::Cell;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use futures::FutureExt;

use crate::{RegisteredTask, Runnable, Schedule, TaskSet};

use super::{TestFuture, TestState};

struct PanicOnUnbind {
    _tasks: Rc<TaskSet>,
}

struct PanicOnSchedule {
    tasks: Rc<TaskSet>,
}

impl Schedule for PanicOnSchedule {
    fn schedule(&self, _runnable: Runnable) {
        panic!("schedule panic");
    }

    fn unbind(&self, registered: &RegisteredTask) {
        unsafe { self.tasks.remove(registered) };
    }
}

struct WakeAndPending(Rc<Cell<usize>>);

impl Future for WakeAndPending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

impl Drop for WakeAndPending {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
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
fn panic_payload_round_trips_through_join_handle() {
    let spawner = super::TestSpawner::new();
    let handle = spawner.spawn(async {
        panic::panic_any(String::from("boom"));
    });

    spawner.next().unwrap().run();
    let error = handle.now_or_never().unwrap().unwrap_err();
    let payload = error.into_panic().expect("expected a panic payload");

    assert_eq!(*payload.downcast::<String>().unwrap(), "boom");
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

#[test]
fn panic_during_reschedule_drops_transferred_runnable_reference_once() {
    let tasks = Rc::new(TaskSet::new());
    let future_drops = Rc::new(Cell::new(0));
    let scheduler = PanicOnSchedule {
        tasks: Rc::clone(&tasks),
    };
    // Safety: the future and its output are both 'static.
    let (runnable, handle) =
        unsafe { tasks.bind(WakeAndPending(Rc::clone(&future_drops)), scheduler) };

    let panic = panic::catch_unwind(AssertUnwindSafe(|| runnable.unwrap().run()));
    assert!(panic.is_err());
    assert_eq!(future_drops.get(), 0);

    drop(handle);
    tasks.shutdown();
    assert_eq!(future_drops.get(), 1);
    assert_eq!(Rc::strong_count(&tasks), 1);
}
