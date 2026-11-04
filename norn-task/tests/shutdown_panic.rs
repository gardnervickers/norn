//! A join waker that panics while a task set or queue shuts down one task
//! must not stop the remaining tasks from being shut down.
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::future::{pending, Future};
use std::panic::{self, AssertUnwindSafe};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};

use norn_task::{JoinHandle, RegisteredTask, Runnable, Schedule, TaskQueue, TaskSet};

struct PanicOnWake;

impl Wake for PanicOnWake {
    fn wake(self: Arc<Self>) {
        panic!("join waker panicked");
    }
}

struct CountDrop(Rc<Cell<usize>>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

fn register_panicking_join_waker<T: Unpin>(join: &mut JoinHandle<T>) {
    let waker = Waker::from(Arc::new(PanicOnWake));
    assert!(Pin::new(join)
        .poll(&mut Context::from_waker(&waker))
        .is_pending());
}

#[test]
fn dropping_last_queue_handle_cancels_every_task_even_if_a_join_waker_panics() {
    let queue = TaskQueue::new();
    let mut first = queue.spawn(pending::<()>());
    register_panicking_join_waker(&mut first);

    let second_drops = Rc::new(Cell::new(0));
    let guard = CountDrop(Rc::clone(&second_drops));
    queue
        .spawn(async move {
            let _guard = guard;
            pending::<()>().await;
        })
        .detach();

    let dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(queue)));
    assert!(dropped.is_err(), "the join waker's panic propagates");

    // The final external handle is gone, so the queue must have shut down
    // every task rather than leak it through the queue/task reference cycle.
    assert_eq!(
        second_drops.get(),
        1,
        "the second task's future was never destroyed"
    );
}

struct Scheduler {
    runqueue: Rc<RefCell<VecDeque<Runnable>>>,
}

impl Schedule for Scheduler {
    fn schedule(&self, runnable: Runnable) {
        self.runqueue.borrow_mut().push_back(runnable);
    }

    fn unbind(&self, _registered: &RegisteredTask) {
        // The tasks in this test never complete normally.
    }
}

#[test]
fn dropping_task_set_destroys_incomplete_futures_even_if_a_join_waker_panics() {
    let runqueue = Rc::new(RefCell::new(VecDeque::new()));
    let second_drops = Rc::new(Cell::new(0));
    let set = TaskSet::new();

    // Safety: both futures and outputs are 'static.
    let (first_runnable, mut first) = unsafe {
        set.bind(
            pending::<()>(),
            Scheduler {
                runqueue: Rc::clone(&runqueue),
            },
        )
    };
    drop(first_runnable);
    register_panicking_join_waker(&mut first);

    let guard = CountDrop(Rc::clone(&second_drops));
    let (second_runnable, _second) = unsafe {
        set.bind(
            async move {
                let _guard = guard;
                pending::<()>().await;
            },
            Scheduler {
                runqueue: Rc::clone(&runqueue),
            },
        )
    };
    // The runnable is queued but has not run yet.
    runqueue.borrow_mut().push_back(second_runnable.unwrap());

    let dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(set)));
    assert!(dropped.is_err(), "the join waker's panic propagates");

    // `TaskSet::bind` documents that dropping the set destroys every
    // incomplete future even if a runnable still references the task.
    assert_eq!(
        second_drops.get(),
        1,
        "the set is gone but the second task's future is still alive"
    );
    // The queued runnable keeps its scheduler, and so this runqueue, alive.
    runqueue.borrow_mut().clear();
}

/// The caller follows the `TaskSet::bind` contract: the borrowed value
/// outlives the set. Without unwind-safe shutdown, the borrowing future
/// survives the set and is polled after the value is freed, which Miri
/// reports as a use-after-free.
#[test]
fn scoped_future_does_not_outlive_task_set_after_join_waker_panic() {
    let runqueue = Rc::new(RefCell::new(VecDeque::new()));
    {
        let data = Box::new(17_u64);
        let set = TaskSet::new();

        // Safety: `data` outlives `set`, which is dropped below.
        let (first_runnable, mut first) = unsafe {
            set.bind(
                pending::<()>(),
                Scheduler {
                    runqueue: Rc::clone(&runqueue),
                },
            )
        };
        drop(first_runnable);
        register_panicking_join_waker(&mut first);

        let data_ref: &u64 = &data;
        let reads_borrow = std::future::poll_fn(move |_| {
            assert_eq!(*data_ref, 17);
            std::task::Poll::<()>::Pending
        });
        // Safety: as above.
        let (second_runnable, second) = unsafe {
            set.bind(
                reads_borrow,
                Scheduler {
                    runqueue: Rc::clone(&runqueue),
                },
            )
        };
        runqueue.borrow_mut().push_back(second_runnable.unwrap());

        let dropped = panic::catch_unwind(AssertUnwindSafe(|| drop(set)));
        assert!(dropped.is_err());
        drop(first);
        drop(second);
        // `data` is freed here, after the set.
    }
    let runnable = runqueue.borrow_mut().pop_front().unwrap();
    runnable.run();
}
