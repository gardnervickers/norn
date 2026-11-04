use std::cell::UnsafeCell;
use std::future::Future;
use std::marker::PhantomData;
use std::panic::{self, AssertUnwindSafe};

use crate::header::Header;
use crate::{JoinHandle, RegisteredTask, Runnable, Schedule};

/// [`TaskSet`] tracks a set of registered tasks.
///
/// It can be used to cancel all tasks, such as during shutdown.
pub struct TaskSet {
    inner: UnsafeCell<Inner>,
    // !Send
    _m: PhantomData<*const ()>,
}

impl std::fmt::Debug for TaskSet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskSet").finish()
    }
}

struct Inner {
    list: cordyceps::List<Header>,
    closed: bool,
}

impl TaskSet {
    /// Construct a new [`TaskSet`].
    pub fn new() -> Self {
        Self {
            inner: UnsafeCell::new(Inner {
                closed: false,
                list: cordyceps::List::new(),
            }),
            _m: PhantomData,
        }
    }

    /// Bind a future to this [`TaskSet`], returning a [`Runnable`] and a [`JoinHandle`].
    ///
    /// If the [`TaskSet`] is closed, the [`Runnable`] will be `None` and the [`JoinHandle`]
    /// will immediately resolve to `Err(crate::TaskError)`.
    ///
    /// # Safety
    /// Callers must ensure that the provided [`Future`] remains valid until it
    /// completes or this task set is closed or dropped. Its output must remain
    /// valid until the returned [`JoinHandle`] is consumed or dropped.
    ///
    /// An easy way to guarantee this is to require that the future is `'static` along with its output,
    /// however shorter lifetimes are also valid. For example, if you can prove that the [`TaskSet`]
    /// outlives all captures of a future, then you can safely bind that future to the [`TaskSet`].
    ///
    /// Closing or dropping the [`TaskSet`] destroys every incomplete future,
    /// even if a runnable or waker still references its task allocation. A
    /// completed output remains owned by its [`JoinHandle`]; dropping the handle
    /// destroys that output immediately, even if task wakers still exist.
    /// This holds even if a join waker panics during shutdown.
    pub unsafe fn bind<T, S>(
        &self,
        future: T,
        scheduler: S,
    ) -> (Option<Runnable>, JoinHandle<T::Output>)
    where
        S: Schedule,
        T: Future,
    {
        let (task, bound, handle) = crate::task_cell::TaskCell::allocate(future, scheduler);
        // Safety: `TaskSet` is `!Send` and only accessed from one thread. The
        // borrow ends before shutdown, which may drop arbitrary user state and
        // re-enter this task set.
        let closed = unsafe { (*self.inner.get()).closed };
        if closed {
            task.shutdown();
            return (None, JoinHandle::from(handle));
        }
        // Safety: no user code runs between checking `closed` and inserting the
        // task, so the task set cannot be re-entered between these operations.
        unsafe { (*self.inner.get()).list.push_back(bound) };
        (Some(Runnable::from(task)), JoinHandle::from(handle))
    }

    /// Shutdown all tasks in the [`TaskSet`].
    ///
    /// This will prevent any new tasks from being added to the [`TaskSet`].
    ///
    /// # Panics
    /// If a join waker panics, every remaining task is still shut down before
    /// the first panic is propagated.
    pub fn shutdown(&self) {
        // Safety: `TaskSet` is `!Send` and only accessed from one thread. Each
        // mutable borrow ends before a task is cancelled or dropped, since both
        // operations may run arbitrary user destructors and re-enter this set.
        unsafe { (*self.inner.get()).closed = true };

        // Waking a join handle runs user code, which may panic. Keep going so
        // no incomplete future outlives the set, since callers of `bind` may
        // free borrowed data as soon as shutdown returns.
        let mut first_panic = None;
        loop {
            let task = unsafe { (*self.inner.get()).list.pop_front() };
            let Some(task) = task else {
                break;
            };
            if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(move || task.shutdown())) {
                first_panic.get_or_insert(payload);
            }
        }
        if let Some(payload) = first_panic {
            panic::resume_unwind(payload);
        }
    }

    /// Remove a task from the [`TaskSet`].
    ///
    /// # Safety
    /// The provided [`RegisteredTask`] must have been returned from a previous call to [`TaskSet::bind`]
    /// on this [`TaskSet`]. Do not use [`RegisteredTask`]s from other [`TaskSet`]s.
    pub unsafe fn remove(&self, task: &RegisteredTask) -> Option<RegisteredTask> {
        // Safety: `TaskSet` is `!Send` and only accessed from one thread.
        let inner = unsafe { &mut *self.inner.get() };
        unsafe { inner.list.remove(task.as_ptr()) }
    }
}

impl Default for TaskSet {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for TaskSet {
    fn drop(&mut self) {
        self.shutdown();
    }
}
