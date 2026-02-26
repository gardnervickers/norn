//! A set of futures which can be polled to completion.
use std::cell::RefCell;
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use norn_task::{JoinHandle, Runnable, Schedule, TaskSet};

/// [`PollSet`] provides a way to spawn and run tasks within
/// a scope.
///
/// Polling a `PollSet` drives all currently runnable tasks and then returns
/// [`Poll::Pending`]. It is intended to be embedded in another future that
/// controls lifecycle and shutdown.
#[must_use = "futures do nothing unless awaited or polled"]
pub struct PollSet {
    shared: Rc<Shared>,
}

struct Shared {
    waker: RefCell<Option<Waker>>,
    runqueue: RefCell<VecDeque<Runnable>>,
    taskset: TaskSet,
}

struct Scheduler {
    shared: Rc<Shared>,
}

impl PollSet {
    /// Create an empty [`PollSet`].
    pub fn new() -> Self {
        let shared = Shared {
            waker: RefCell::new(None),
            runqueue: RefCell::new(VecDeque::new()),
            taskset: TaskSet::default(),
        };
        Self {
            shared: Rc::new(shared),
        }
    }

    /// Spawn a task into this poll set and return a [`JoinHandle`] for its output.
    ///
    /// The task is scheduled on this poll set's local queue and will be driven when
    /// the [`PollSet`] future is polled.
    pub fn spawn<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future + 'static,
        F::Output: 'static,
    {
        // Safety: `F` is `'static` and `F::Output` is `'static`.
        unsafe { self.spawn_unchecked(future) }
    }

    pub(crate) unsafe fn spawn_unchecked<F>(&self, future: F) -> JoinHandle<F::Output>
    where
        F: Future,
        F::Output: 'static,
    {
        let sched = self.scheduler();
        let (runnable, handle) = unsafe { self.shared.taskset.bind(future, sched) };
        if let Some(runnable) = runnable {
            self.shared.runqueue.borrow_mut().push_back(runnable);
        }
        handle
    }

    pub(crate) fn clear(&self) {
        self.shared.runqueue.borrow_mut().clear();
        drop(self.shared.runqueue.take());
    }

    pub(crate) fn poll_tasks(&self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            if let Some(runnable) = self.next() {
                runnable.run();
            } else {
                *self.shared.waker.borrow_mut() = Some(cx.waker().clone());
                return Poll::Pending;
            }
        }
    }

    fn next(&self) -> Option<Runnable> {
        self.shared.runqueue.borrow_mut().pop_front()
    }

    fn scheduler(&self) -> Scheduler {
        Scheduler {
            shared: self.shared.clone(),
        }
    }
}

impl std::future::Future for PollSet {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.poll_tasks(cx)
    }
}

impl Default for PollSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Schedule for Scheduler {
    fn schedule(&self, runnable: Runnable) {
        self.shared.runqueue.borrow_mut().push_back(runnable);
        if let Some(waker) = self.shared.waker.borrow_mut().take() {
            waker.wake();
        }
    }

    fn unbind(&self, registered: &norn_task::RegisteredTask) {
        unsafe { self.shared.taskset.remove(registered) };
    }
}

impl Drop for PollSet {
    fn drop(&mut self) {
        self.clear()
    }
}
