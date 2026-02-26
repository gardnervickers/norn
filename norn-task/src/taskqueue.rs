use std::cell::UnsafeCell;
use std::collections::VecDeque;
use std::future::Future;
use std::rc::Rc;

use crate::{JoinHandle, Runnable, Schedule, TaskSet};

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
#[derive(Clone)]
pub struct TaskQueue {
    shared: Rc<Shared>,
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
}

impl TaskQueue {
    /// Construct a new [`TaskQueue`].
    pub fn new() -> Self {
        let shared = Shared {
            runqueue: UnsafeCell::new(VecDeque::with_capacity(1024)),
            taskset: TaskSet::default(),
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
            self.shared.push_runnable(runnable);
        }
        handle
    }

    /// Returns the next [`Runnable`] to be executed.
    pub fn next(&self) -> Option<Runnable> {
        self.shared.pop_runnable()
    }

    /// Returns the number of [`Runnable`]s in the queue.
    pub fn runnable(&self) -> usize {
        self.shared.runnable_len()
    }

    /// Shutdown the [`TaskQueue`].
    ///
    /// Cancels all tasks and drops their [`Future`]s.
    pub fn shutdown(&self) {
        self.shared.taskset.shutdown();
        self.shared.clear_runqueue();
    }
}

impl Schedule for Rc<Shared> {
    fn schedule(&self, runnable: Runnable) {
        self.push_runnable(runnable);
    }

    fn unbind(&self, registered: &crate::RegisteredTask) {
        unsafe { self.taskset.remove(registered) };
    }
}

impl Shared {
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
        unsafe {
            (*self.runqueue.get()).clear();
        }
    }
}
