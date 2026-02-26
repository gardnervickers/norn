use std::ptr;
use std::rc::Rc;

use crate::header::{self, Header};
use crate::task_cell::{self};

/// [`Schedule`] provides a way to schedule a [`Runnable`] to be executed later.
///
/// Typically this will be a task queue implementation.
pub trait Schedule: Sized + 'static {
    /// Schedule a [`Runnable`] to be executed later.
    ///
    /// This is invoked whenever a task is woken up. The [`Runnable`] should
    /// be stored and executed at some point in the future via [`Runnable::run`].
    fn schedule(&self, runnable: Runnable);

    /// Unbind a [`RegisteredTask`] from this [`Schedule`].
    ///
    /// This should delegate to [`crate::TaskSet::remove`], unregistering
    /// the task from the [`crate::TaskSet`].
    fn unbind(&self, registered: &RegisteredTask);
}

impl<T> Schedule for Rc<T>
where
    T: Schedule,
{
    fn schedule(&self, runnable: Runnable) {
        self.as_ref().schedule(runnable);
    }

    fn unbind(&self, registered: &RegisteredTask) {
        self.as_ref().unbind(registered);
    }
}

/// [`Runnable`] is a handle to a task that can be executed.
///
/// Callers should invoke [`Runnable::run`] to execute the task.
pub struct Runnable(task_cell::TaskRef);

impl std::fmt::Debug for Runnable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Runnable").finish()
    }
}

impl Runnable {
    /// Run the task.
    ///
    /// This will advance the task to completion, or until it is parked.
    pub fn run(self) {
        self.0.run();
    }
}

impl From<task_cell::TaskRef> for Runnable {
    fn from(task: task_cell::TaskRef) -> Self {
        Runnable(task)
    }
}

/// [`RegisteredTask`] is a handle to a task that has been registered
/// with a [`Schedule`].
pub struct RegisteredTask {
    inner: task_cell::TaskRef,
}

impl std::fmt::Debug for RegisteredTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredTask").finish()
    }
}

impl RegisteredTask {
    /// Cancel the task.
    pub fn shutdown(&self) {
        self.inner.shutdown();
    }

    pub(crate) fn as_ptr(&self) -> std::ptr::NonNull<header::Header> {
        self.inner.as_ptr()
    }
}

impl From<task_cell::TaskRef> for RegisteredTask {
    fn from(task: task_cell::TaskRef) -> Self {
        RegisteredTask { inner: task }
    }
}

unsafe impl cordyceps::Linked<cordyceps::list::Links<Header>> for Header {
    type Handle = RegisteredTask;

    fn into_ptr(r: Self::Handle) -> ptr::NonNull<Self> {
        r.inner.into_ptr()
    }

    unsafe fn from_ptr(ptr: ptr::NonNull<Self>) -> Self::Handle {
        let taskref = task_cell::TaskRef::from_ptr(ptr);
        RegisteredTask { inner: taskref }
    }

    unsafe fn links(ptr: ptr::NonNull<Self>) -> ptr::NonNull<cordyceps::list::Links<Header>> {
        let me = ptr.as_ptr();
        let links = &raw mut (*me).links;
        ptr::NonNull::new_unchecked(links)
    }
}
