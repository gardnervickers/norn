use std::cell::{Cell, RefCell};
use std::rc::Rc;

use futures::FutureExt;

use crate::{JoinHandle, TaskQueue};

struct CountDrop(Rc<Cell<usize>>);

impl Drop for CountDrop {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
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
