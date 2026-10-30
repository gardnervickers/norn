use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll, Wake, Waker};

use futures::FutureExt;

use crate::{JoinHandle, TaskQueue};

struct CountDrop(Rc<Cell<usize>>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
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

struct ReenterOnDrop {
    queue: TaskQueue,
    drops: Rc<Cell<usize>>,
    nested_drops: Rc<Cell<usize>>,
}

impl Drop for ReenterOnDrop {
    fn drop(&mut self) {
        self.drops.set(self.drops.get() + 1);

        let nested = CountDrop(Rc::clone(&self.nested_drops));
        self.queue
            .spawn(async move {
                let _nested = nested;
                std::future::pending::<()>().await;
            })
            .detach();
        self.queue.shutdown();
    }
}

#[test]
fn shutdown_allows_destructor_to_spawn_and_shutdown() {
    let queue = TaskQueue::new();
    let drops = Rc::new(Cell::new(0));
    let nested_drops = Rc::new(Cell::new(0));
    let other_drops = Rc::new(Cell::new(0));
    let guard = ReenterOnDrop {
        queue: queue.clone(),
        drops: Rc::clone(&drops),
        nested_drops: Rc::clone(&nested_drops),
    };
    let handle = queue.spawn(async move {
        let _guard = guard;
        std::future::pending::<()>().await;
    });
    let other = CountDrop(Rc::clone(&other_drops));
    let other_handle = queue.spawn(async move {
        let _other = other;
        std::future::pending::<()>().await;
    });

    queue.shutdown();
    queue.shutdown();

    assert_eq!(drops.get(), 1);
    assert_eq!(nested_drops.get(), 1);
    assert_eq!(other_drops.get(), 1);
    assert_eq!(queue.runnable(), 0);
    assert!(handle.now_or_never().unwrap().unwrap_err().is_cancelled());
    assert!(other_handle
        .now_or_never()
        .unwrap()
        .unwrap_err()
        .is_cancelled());
}

#[test]
fn shutdown_allows_pending_task_to_own_its_join_handle() {
    let queue = TaskQueue::new();
    let drops = Rc::new(Cell::new(0));
    let join = Rc::new(RefCell::new(None));
    let task_join = Rc::clone(&join);
    let guard = CountDrop(Rc::clone(&drops));

    let handle: JoinHandle<()> = queue.spawn(async move {
        let _guard = guard;
        let _task_join = task_join;
        std::future::pending::<()>().await;
    });
    join.borrow_mut().replace(handle);

    // Leave the task as the sole owner of the cell containing its join handle.
    // Cancelling the task drops the future, then the cell, then the handle.
    drop(join);

    queue.shutdown();
    queue.shutdown();

    assert_eq!(drops.get(), 1);
    assert_eq!(queue.runnable(), 0);
}

#[test]
fn dropping_completed_output_allows_reentrant_shutdown() {
    let queue = TaskQueue::new();
    let drops = Rc::new(Cell::new(0));
    let nested_drops = Rc::new(Cell::new(0));
    let output = ReenterOnDrop {
        queue: queue.clone(),
        drops: Rc::clone(&drops),
        nested_drops: Rc::clone(&nested_drops),
    };
    let handle = queue.spawn(async move { output });

    queue.next().unwrap().run();
    drop(handle);

    assert_eq!(drops.get(), 1);
    assert_eq!(nested_drops.get(), 1);
    assert_eq!(queue.runnable(), 0);
}

#[test]
fn running_task_can_shutdown_its_queue_and_cancellation_wins() {
    let queue = TaskQueue::new();
    let task_queue = queue.clone();
    let output_drops = Rc::new(Cell::new(0));
    let task_output_drops = Rc::clone(&output_drops);
    let handle = queue.spawn(async move {
        task_queue.shutdown();
        CountDrop(task_output_drops)
    });

    queue.next().unwrap().run();

    assert_eq!(queue.runnable(), 0);
    assert_eq!(output_drops.get(), 1);
    let Err(error) = handle.now_or_never().unwrap() else {
        panic!("self-shutdown must cancel the task output");
    };
    assert!(error.is_cancelled());
}

#[test]
fn pending_task_can_shutdown_its_queue_and_is_cancelled_after_poll() {
    let queue = TaskQueue::new();
    let task_queue = queue.clone();
    let future_drops = Rc::new(Cell::new(0));
    let task_future_drops = Rc::clone(&future_drops);
    let handle = queue.spawn(async move {
        let _guard = CountDrop(task_future_drops);
        task_queue.shutdown();
        std::future::pending::<()>().await;
    });

    queue.next().unwrap().run();

    assert_eq!(queue.runnable(), 0);
    assert_eq!(future_drops.get(), 1);
    let Err(error) = handle.now_or_never().unwrap() else {
        panic!("self-shutdown must cancel the pending task");
    };
    assert!(error.is_cancelled());
}

#[test]
fn shutdown_wakes_previously_polled_join() {
    let queue = TaskQueue::new();
    let mut handle = Box::pin(queue.spawn(std::future::pending::<()>()));
    let wake_counter = Arc::new(WakeCounter(AtomicUsize::new(0)));
    let waker = Waker::from(Arc::clone(&wake_counter));
    let mut cx = Context::from_waker(&waker);

    assert!(matches!(handle.as_mut().poll(&mut cx), Poll::Pending));
    queue.shutdown();

    assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
    let Poll::Ready(result) = handle.as_mut().poll(&mut cx) else {
        panic!("join must be ready after shutdown");
    };
    let Err(error) = result else {
        panic!("shutdown must cancel the joined task");
    };
    assert!(error.is_cancelled());

    queue.shutdown();
    assert_eq!(wake_counter.0.load(Ordering::Relaxed), 1);
}

/// A scheduler that runs tasks inline and then reads its own state, which
/// lives inside the task allocation it just ran.
#[test]
fn inline_scheduler_may_run_the_last_task_reference() {
    use crate::{RegisteredTask, Runnable, Schedule, TaskSet};

    struct Inline {
        set: TaskSet,
        runs: Cell<usize>,
    }

    struct InlineScheduler(Rc<Inline>);

    impl Schedule for InlineScheduler {
        fn schedule(&self, runnable: Runnable) {
            runnable.run();
            self.0.runs.set(self.0.runs.get() + 1);
        }

        fn unbind(&self, registered: &RegisteredTask) {
            unsafe { self.0.set.remove(registered) };
        }
    }

    let waker = Rc::new(RefCell::new(None::<Waker>));
    let task_waker = Rc::clone(&waker);
    let mut first = true;
    let future = std::future::poll_fn(move |cx| {
        if std::mem::take(&mut first) {
            *task_waker.borrow_mut() = Some(cx.waker().clone());
            Poll::Pending
        } else {
            Poll::Ready(())
        }
    });

    let inline = Rc::new(Inline {
        set: TaskSet::new(),
        runs: Cell::new(0),
    });
    let (runnable, handle) =
        unsafe { inline.set.bind(future, InlineScheduler(Rc::clone(&inline))) };
    drop(handle);
    runnable.unwrap().run();

    // Waking by value transfers the waker's reference into the runnable.
    // The scheduler completes the task inline before it returns.
    let waker = waker.borrow_mut().take().unwrap();
    waker.wake();
    assert_eq!(inline.runs.get(), 1);
}
