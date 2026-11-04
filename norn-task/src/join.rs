use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use crate::future_cell::TaskError;
use crate::task_cell::JoinHandleRef;

/// A handle to the spawned task.
///
/// [`JoinHandle`] provides a handle to the spawned task that can be awaited. If
/// the [`JoinHandle`] is dropped while the task is running, it will continue
/// running in the background.
///
/// Like any future, a [`JoinHandle`] must not be polled again after it returns
/// [`Poll::Ready`]; doing so panics.
///
/// # Aborts
///
/// The task belongs to the thread that runs it. Cloning, waking or dropping
/// its waker from any other thread aborts the process (after printing a
/// message), so a future must not hand its waker to another thread or to
/// something completed there, such as a `std::thread` or a cross-thread
/// channel. The process also aborts, instead of unwinding, if dropping the
/// future or its output panics, or if the task's reference count overflows.
#[must_use = "futures do nothing unless you `.await` or poll them"]
pub struct JoinHandle<T> {
    inner: JoinHandleRef<T>,
}

impl<T: std::fmt::Debug> std::fmt::Debug for JoinHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("JoinHandle").finish()
    }
}

impl<T> JoinHandle<T> {
    /// Abort the task associated with this [`JoinHandle`].
    ///
    /// Aborting the task will eventually stop the task from running. If the
    /// task already completed, it's result will be returned. If the task has
    /// not yet completed, it will be cancelled and the [`JoinHandle`] will
    /// resolve to an error.
    pub fn abort(&self) {
        self.inner.abort();
    }

    /// Detach the task from this [`JoinHandle`].
    ///
    /// This is a convenience method that will drop the [`JoinHandle`] without
    /// cancelling the task. This signals intent to the reader that the task
    /// result is not needed. If the task has already completed, its stored
    /// output is dropped immediately.
    pub fn detach(self) {}
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, TaskError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.inner.poll_result(cx.waker())
    }
}

impl<T> From<JoinHandleRef<T>> for JoinHandle<T> {
    fn from(handle: JoinHandleRef<T>) -> Self {
        Self { inner: handle }
    }
}
