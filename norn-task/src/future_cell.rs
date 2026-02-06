//! [`FutureCell`] contains a [`Future`] or its output. It is used to store
//! the [`Future`] associated with a task.
//!
//! It offers a way to poll a future safely, handling panics and the lifecycle
//! of the future.
//!
//! [`Future`]: std::future::Future
use std::any::Any;
use std::cell::RefCell;
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, mem};

use crate::util::abort_on_panic;

const ABORT_ON_DROP_PANIC: bool = true;

fn safe_drop_state<F>(value: State<F>)
where
    F: Future,
{
    if ABORT_ON_DROP_PANIC {
        abort_on_panic(|| drop(value));
    } else if let Err(panic) = panic::catch_unwind(panic::AssertUnwindSafe(|| drop(value))) {
        eprintln!("drop panic: {panic:?}");
    }
}

pub(crate) struct FutureCell<F>
where
    F: Future,
{
    inner: RefCell<State<F>>,
}

impl<F> FutureCell<F>
where
    F: Future,
{
    pub(crate) fn new(future: F) -> Self {
        Self {
            inner: RefCell::new(State::Future(future)),
        }
    }

    /// Cancels the future, dropping it or its output.
    ///
    /// This will set the contents of the [`FutureCell`] to an error indicating
    /// that the future was cancelled.
    pub(crate) fn cancel(&self) {
        let this = &mut *self.inner.borrow_mut();
        let old = mem::replace(this, State::FutureResult(Err(TaskError::cancelled())));
        safe_drop_state(old);
    }

    /// Drops the future or its output.
    ///
    /// # Abort
    /// This will abort if dropping the future or its output panics.
    pub(crate) fn destroy(&self) {
        let this = &mut *self.inner.borrow_mut();
        let old = mem::replace(this, State::Empty);
        safe_drop_state(old);
    }

    /// Take the output of the future, if it has been polled to completion.
    ///
    /// # Panic
    /// If the future has not been polled to completion, or if it has been destroyed,
    /// this method will panic.
    pub(crate) fn take_output(&self) -> Result<F::Output, TaskError> {
        let this = &mut *self.inner.borrow_mut();
        match mem::replace(this, State::Empty) {
            State::FutureResult(res) => res,
            _ => panic!("future not polled to completion"),
        }
    }

    /// Perform the poll operation on the future.
    ///
    /// # Panic
    /// This method will panic if the future has already been polled to completion,
    /// or has been dropped.
    ///
    /// # Safety
    /// Callers must ensure that this [`FutureCell`] is pinned in memory.
    /// This method will make a pin projection of the [`FutureCell`] for the
    /// contained future.
    pub(crate) unsafe fn poll(&self, cx: Context<'_>) -> Poll<()> {
        let this = &mut *self.inner.borrow_mut();
        unsafe { this.poll(cx) }
    }
}

enum State<F>
where
    F: Future,
{
    Future(F),
    FutureResult(Result<F::Output, TaskError>),
    Empty,
}

impl<F> State<F>
where
    F: Future,
{
    /// Poll the inner future to completion.
    ///
    /// # Safety
    /// See [`FutureCell::poll`] for safety requirements.
    unsafe fn poll(&mut self, mut cx: Context<'_>) -> Poll<()> {
        match self {
            State::Future(fut) => {
                // Safety: We require that the caller has pinned this FutureCell in memory,
                //         so it is safe to make a pinned projection to the inner future. We
                //         will not move it.
                let fut = unsafe { Pin::new_unchecked(fut) };
                match panic::catch_unwind(panic::AssertUnwindSafe(|| fut.poll(&mut cx))) {
                    Ok(Poll::Ready(result)) => {
                        let old = mem::replace(self, State::FutureResult(Ok(result)));
                        safe_drop_state(old);
                        Poll::Ready(())
                    }
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(err) => {
                        let old =
                            mem::replace(self, State::FutureResult(Err(TaskError::panic(err))));
                        safe_drop_state(old);
                        Poll::Ready(())
                    }
                }
            }
            State::FutureResult(_) => unreachable!("FutureCell is complete"),
            State::Empty => unreachable!("FutureCell is empty"),
        }
    }
}

/// [`TaskError`] indicates a failure in a task.
///
/// Tasks can fail for one of two reasons. Either the task was cancelled, or
/// the task panicked. Users can check which of these two reasons caused the
/// failure via the [`TaskError::is_cancelled`] and [`TaskError::is_panic`]
pub struct TaskError {
    inner: Kind,
}

impl TaskError {
    /// Returns `true` if the task panicked.
    pub fn is_panic(&self) -> bool {
        matches!(self.inner, Kind::Panic(_))
    }

    /// Returns `true` if the task was cancelled.
    pub fn is_cancelled(&self) -> bool {
        matches!(self.inner, Kind::Cancelled)
    }
}

enum Kind {
    Cancelled,
    #[allow(dead_code)]
    Panic(Box<dyn Any + Send + 'static>),
}

impl TaskError {
    pub(crate) fn cancelled() -> TaskError {
        TaskError {
            inner: Kind::Cancelled,
        }
    }

    pub(crate) fn panic(err: Box<dyn Any + Send + 'static>) -> TaskError {
        TaskError {
            inner: Kind::Panic(err),
        }
    }
}

impl std::error::Error for TaskError {}

impl fmt::Debug for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Kind::Cancelled => write!(f, "TaskError::Cancelled"),
            Kind::Panic(_) => write!(f, "TaskError::Panic(...)"),
        }
    }
}

impl fmt::Display for TaskError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.inner {
            Kind::Cancelled => write!(f, "task was cancelled"),
            Kind::Panic(_) => write!(f, "task panicked"),
        }
    }
}
