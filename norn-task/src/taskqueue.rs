use std::cell::{Cell, UnsafeCell};
use std::collections::VecDeque;
use std::future::Future;
use std::panic::{self, AssertUnwindSafe};
use std::rc::Rc;
use std::sync::Arc;

use norn_metrics::Padded;

use crate::metrics::TaskCounters;
use crate::{JoinHandle, Runnable, Schedule, TaskMetrics, TaskSet};

/// [`TaskQueue`] provides a way to spawn and run tasks.
///
/// ```rust
/// let tq = norn_task::TaskQueue::new();
///
/// tq.spawn(async { println!("Hello world") }).detach();
/// while let Some(runnable) = tq.next() {
///     runnable.run();
/// }
/// ```
///
/// Cloning a queue creates another external owner. Dropping the final external
/// owner shuts down the queue, even though pending tasks retain strong scheduler
/// references internally. A queue clone captured by one of its tasks still
/// counts as an external owner; call [`TaskQueue::shutdown`] explicitly when
/// the queue must be cancelled while such captured handles remain alive.
pub struct TaskQueue {
    shared: Rc<Shared>,
}

impl Clone for TaskQueue {
    fn clone(&self) -> Self {
        let external_handles = self.shared.external_handles.get();
        self.shared.external_handles.set(
            external_handles
                .checked_add(1)
                .expect("TaskQueue handle count overflow"),
        );
        Self {
            shared: Rc::clone(&self.shared),
        }
    }
}

impl Default for TaskQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for TaskQueue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskQueue").finish()
    }
}

struct Shared {
    runqueue: UnsafeCell<VecDeque<Runnable>>,
    taskset: TaskSet,
    external_handles: Cell<usize>,
    counters: Arc<Padded<TaskCounters>>,
}

impl TaskQueue {
    /// Construct a new [`TaskQueue`].
    pub fn new() -> Self {
        let shared = Shared {
            runqueue: UnsafeCell::new(VecDeque::with_capacity(1024)),
            taskset: TaskSet::default(),
            external_handles: Cell::new(1),
            counters: Arc::default(),
        };
        Self {
            shared: Rc::new(shared),
        }
    }

    /// Spawn a [`Future`] onto the [`TaskQueue`].
    ///
    /// The future will immediately be queued for execution. Returns a [`JoinHandle`]
    /// which can be used to await the result of the future.
    ///
    /// If the queue has already been shut down, the returned [`JoinHandle`]
    /// resolves immediately with cancellation.
    ///
    /// # Aborts
    ///
    /// The task belongs to the thread that runs it. Cloning, waking or dropping
    /// its waker from any other thread aborts the process (after printing a
    /// message), so a future must not hand its waker to another thread or to
    /// something completed there, such as a `std::thread`, or a channel or oneshot
    /// from another runtime (for example `futures::channel` or `tokio::sync`) whose
    /// sender completes on another thread. `norn-channel` is safe: it wakes receivers
    /// only on their own runtime thread. The process also aborts, instead of
    /// unwinding, if dropping the
    /// future or its output panics, or if the task's reference count overflows.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        let sched = Rc::clone(&self.shared);
        // Safety: The 'static bound on the future is required to ensure that the future does not reference
        //         data which can be dropped before the future. 'static guarantees that the future outlives
        //         all references it captures.
        let (runnable, handle) = unsafe { self.shared.taskset.bind(future, sched) };
        if let Some(runnable) = runnable {
            self.shared.counters.spawned.incr();
            self.shared.push_runnable(runnable);
        }
        handle
    }

    /// Returns the next [`Runnable`] to be executed.
    #[inline]
    pub fn next(&self) -> Option<Runnable> {
        self.shared.pop_runnable()
    }

    /// Returns the number of [`Runnable`]s in the queue.
    pub fn runnable(&self) -> usize {
        self.shared.runnable_len()
    }

    /// A view of this queue's task counts that any thread can read.
    pub fn metrics(&self) -> TaskMetrics {
        TaskMetrics(Arc::clone(&self.shared.counters))
    }

    /// Shutdown the [`TaskQueue`].
    ///
    /// Cancels all tasks and drops their [`Future`]s.
    pub fn shutdown(&self) {
        self.shared.shutdown();
    }
}

impl Drop for TaskQueue {
    fn drop(&mut self) {
        let external_handles = self.shared.external_handles.get();
        let remaining_handles = external_handles
            .checked_sub(1)
            .expect("TaskQueue handle count underflow");
        self.shared.external_handles.set(remaining_handles);
        if remaining_handles == 0 {
            self.shared.shutdown();
        }
    }
}

impl Schedule for Rc<Shared> {
    #[inline]
    fn schedule(&self, runnable: Runnable) {
        self.push_runnable(runnable);
    }

    fn unbind(&self, registered: &crate::RegisteredTask) {
        // A task that shutdown already took from the set was counted there.
        if unsafe { self.taskset.remove(registered) }.is_some() {
            self.counters.finished.incr();
        }
    }
}

impl Shared {
    /// Shutdown all tasks and release their queued runnable references.
    fn shutdown(&self) {
        // Queued runnables reference this queue through their scheduler. Clear
        // them even if a join waker panics, or the cycle leaks every task.
        // Shutdown cancels every task in the set, and none can join it once
        // it is closed.
        let cancelled = self.taskset.len();
        let result = panic::catch_unwind(AssertUnwindSafe(|| self.taskset.shutdown()));
        self.counters.finished.add(cancelled as u64);
        self.clear_runqueue();
        if let Err(payload) = result {
            panic::resume_unwind(payload);
        }
    }

    /// Push a runnable into the queue.
    ///
    /// # Safety
    /// `TaskQueue` is single-threaded and these methods only create short-lived
    /// mutable references that do not escape the function.
    #[inline]
    fn push_runnable(&self, runnable: Runnable) {
        unsafe {
            (*self.runqueue.get()).push_back(runnable);
        }
    }

    /// Pop the next runnable from the queue.
    ///
    /// # Safety
    /// See [`Shared::push_runnable`].
    #[inline]
    fn pop_runnable(&self) -> Option<Runnable> {
        unsafe { (*self.runqueue.get()).pop_front() }
    }

    /// Return the number of queued runnables.
    ///
    /// # Safety
    /// See [`Shared::push_runnable`].
    #[inline]
    fn runnable_len(&self) -> usize {
        unsafe { (*self.runqueue.get()).len() }
    }

    /// Clear the runqueue and drop all queued runnables.
    ///
    /// # Safety
    /// See [`Shared::push_runnable`].
    #[inline]
    fn clear_runqueue(&self) {
        // Pop each runnable under a short-lived queue borrow. Dropping a
        // runnable performs task refcount and possible deallocation work, which
        // must remain outside the runqueue's aliasing invariant.
        while let Some(runnable) = self.pop_runnable() {
            drop(runnable);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};
    use std::future::Future;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll};

    use super::TaskQueue;
    use crate::JoinHandle;

    struct CountDrop(Rc<Cell<usize>>);

    impl Drop for CountDrop {
        fn drop(&mut self) {
            self.0.set(self.0.get() + 1);
        }
    }

    struct SelfWoken {
        _guard: CountDrop,
    }

    impl Future for SelfWoken {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }

    #[test]
    fn dropping_final_queue_destroys_queued_detached_task() {
        let queue = TaskQueue::new();
        let shared = Rc::downgrade(&queue.shared);
        let drops = Rc::new(Cell::new(0));
        let guard = CountDrop(Rc::clone(&drops));
        queue
            .spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .detach();

        assert_eq!(queue.runnable(), 1);
        drop(queue);

        assert_eq!(drops.get(), 1);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn dropping_final_queue_destroys_pending_task() {
        let queue = TaskQueue::new();
        let shared = Rc::downgrade(&queue.shared);
        let drops = Rc::new(Cell::new(0));
        let guard = CountDrop(Rc::clone(&drops));
        queue
            .spawn(async move {
                let _guard = guard;
                std::future::pending::<()>().await;
            })
            .detach();
        queue.next().unwrap().run();

        assert_eq!(queue.runnable(), 0);
        drop(queue);

        assert_eq!(drops.get(), 1);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn dropping_final_queue_destroys_self_woken_task() {
        let queue = TaskQueue::new();
        let shared = Rc::downgrade(&queue.shared);
        let drops = Rc::new(Cell::new(0));
        queue
            .spawn(SelfWoken {
                _guard: CountDrop(Rc::clone(&drops)),
            })
            .detach();
        queue.next().unwrap().run();

        assert_eq!(queue.runnable(), 1);
        drop(queue);

        assert_eq!(drops.get(), 1);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn dropping_final_queue_breaks_task_join_handle_cycle() {
        let queue = TaskQueue::new();
        let shared = Rc::downgrade(&queue.shared);
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
        drop(join);
        drop(queue);

        assert_eq!(drops.get(), 1);
        assert!(shared.upgrade().is_none());
    }

    #[test]
    fn running_task_can_drop_final_external_queue_handle() {
        let queue = TaskQueue::new();
        let shared = Rc::downgrade(&queue.shared);
        let drops = Rc::new(Cell::new(0));
        let task_queue = queue.clone();
        let guard = CountDrop(Rc::clone(&drops));
        queue
            .spawn(async move {
                let _guard = guard;
                drop(task_queue);
                std::future::pending::<()>().await;
            })
            .detach();
        let runnable = queue.next().unwrap();
        drop(queue);

        assert!(shared.upgrade().is_some());
        runnable.run();

        assert_eq!(drops.get(), 1);
        assert!(shared.upgrade().is_none());
    }

    /// Every task is counted once when it finishes, whether it completed or
    /// shutdown cancelled it, and the counts stay readable after the queue
    /// is gone.
    #[test]
    fn metrics_count_spawned_and_finished_tasks() {
        let queue = TaskQueue::new();
        let metrics = queue.metrics();
        for _ in 0..3 {
            queue.spawn(async {}).detach();
        }
        let drops = Rc::new(Cell::new(0));
        for _ in 0..2 {
            queue
                .spawn(SelfWoken {
                    _guard: CountDrop(Rc::clone(&drops)),
                })
                .detach();
        }
        // One pass: the three empty tasks complete, the self-waking ones
        // stay alive.
        for _ in 0..5 {
            queue.next().unwrap().run();
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.spawned, 5);
        assert_eq!(snapshot.finished, 3);
        assert_eq!(snapshot.alive(), 2);

        drop(queue);
        assert_eq!(drops.get(), 2);
        let snapshot = metrics.snapshot();
        assert_eq!((snapshot.spawned, snapshot.finished), (5, 5));
        assert_eq!(snapshot.alive(), 0);
    }

    /// A task spawned after shutdown is never accepted, so it is not counted.
    #[test]
    fn metrics_skip_tasks_spawned_after_shutdown() {
        let queue = TaskQueue::new();
        queue.shutdown();
        queue.spawn(async {}).detach();
        let snapshot = queue.metrics().snapshot();
        assert_eq!((snapshot.spawned, snapshot.finished), (0, 0));
    }
}
