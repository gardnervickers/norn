//! [`FutureCell`] contains a [`Future`] or its output. It is used to store
//! the [`Future`] associated with a task.
//!
//! It offers a way to poll a future safely, handling panics and the lifecycle
//! of the future.
//!
//! [`Future`]: std::future::Future
use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{fmt, mem::ManuallyDrop, ptr};

use crate::util::abort_on_panic;

pub(crate) struct FutureCell<F>
where
    F: Future,
{
    // Task state is the borrow discipline for this cell: only the running task
    // may poll, and cancellation/destruction of a running task is deferred.
    // `state` lives outside `storage`, so lifecycle checks never create a
    // reference covering a future or output whose destructor may be running.
    // The `Dropping` state makes destructor re-entry leave storage untouched.
    state: Cell<State>,
    storage: UnsafeCell<Storage<F>>,
}

impl<F> FutureCell<F>
where
    F: Future,
{
    pub(crate) fn new(future: F) -> Self {
        Self {
            state: Cell::new(State::Future),
            storage: UnsafeCell::new(Storage {
                future: ManuallyDrop::new(future),
            }),
        }
    }

    /// Cancels the future, dropping it or its output.
    ///
    /// This will set the contents of the [`FutureCell`] to an error indicating
    /// that the future was cancelled.
    pub(crate) fn cancel(&self) {
        let state = self.begin_drop();
        match state {
            State::Future => self.drop_future(),
            State::FutureResult => self.drop_output(),
            State::Empty => {}
            State::Dropping => return,
        }
        self.finish_result(Err(TaskError::cancelled()));
    }

    /// Drops the future or its output.
    ///
    /// # Abort
    /// This will abort if dropping the future or its output panics.
    pub(crate) fn destroy(&self) {
        let state = self.begin_drop();
        match state {
            State::Future => self.drop_future(),
            State::FutureResult => self.drop_output(),
            State::Empty | State::Dropping => return,
        }
        self.state.set(State::Empty);
    }

    /// Take the output of the future, if it has been polled to completion.
    ///
    /// Returns `None` while cancellation is still destroying the future. Its
    /// destructor may run user code that polls the task's join handle before
    /// the cancellation result has been stored.
    ///
    /// # Panic
    /// If the future has not been polled to completion, or if the output was
    /// already taken (a [`JoinHandle`](crate::JoinHandle) polled again after it
    /// returned `Ready`), this method will panic.
    pub(crate) fn take_output(&self) -> Option<Result<F::Output, TaskError>> {
        match self.state.get() {
            State::FutureResult => {}
            State::Dropping => return None,
            State::Future => panic!("task output requested before the task completed"),
            State::Empty => panic!(
                "task output already taken: a JoinHandle was polled again after returning Ready"
            ),
        }
        self.state.set(State::Empty);
        // Safety: `FutureResult` means storage holds an initialized output
        // and no destructor is running. The single-thread task contract
        // excludes concurrent access.
        Some(unsafe { ManuallyDrop::take(&mut (*self.storage.get()).output) })
    }

    /// Perform the poll operation on the future.
    ///
    /// # Safety
    ///
    /// The caller must hold the task's exclusive running state for the entire
    /// poll. Cancellation and destruction must be deferred until this method
    /// returns.
    ///
    /// # Panic
    /// This method will panic if the future has already been polled to completion,
    /// or has been dropped.
    ///
    pub(crate) unsafe fn poll(&self, mut cx: Context<'_>) -> Poll<()> {
        match self.state.get() {
            State::Future => {}
            State::FutureResult => unreachable!("FutureCell is complete"),
            State::Empty => unreachable!("FutureCell is empty"),
            State::Dropping => unreachable!("FutureCell is transitioning"),
        }
        // Safety: the caller guarantees exclusive access to the future while
        // the task is running, and the future is never moved out of storage.
        let fut = unsafe { Pin::new_unchecked(&mut *(*self.storage.get()).future) };
        let result = match panic::catch_unwind(panic::AssertUnwindSafe(|| fut.poll(&mut cx))) {
            Ok(Poll::Ready(output)) => Ok(output),
            Ok(Poll::Pending) => return Poll::Pending,
            Err(err) => Err(TaskError::panic(err)),
        };
        self.state.set(State::Dropping);
        self.drop_future();
        self.finish_result(result);
        Poll::Ready(())
    }

    fn begin_drop(&self) -> State {
        let state = self.state.get();
        if matches!(state, State::Future | State::FutureResult) {
            self.state.set(State::Dropping);
        }
        state
    }

    fn drop_future(&self) {
        // Take only a raw pointer across user destructor execution so
        // re-entrant lifecycle calls do not alias a live Rust reference.
        let storage = self.storage.get();
        abort_on_panic(|| unsafe {
            ManuallyDrop::drop(&mut (*storage).future);
        });
    }

    fn drop_output(&self) {
        // See `drop_future`: `Dropping` prevents re-entry from touching the
        // storage while its destructor is running.
        let storage = self.storage.get();
        abort_on_panic(|| unsafe {
            ManuallyDrop::drop(&mut (*storage).output);
        });
    }

    fn finish_result(&self, result: Result<F::Output, TaskError>) {
        debug_assert_eq!(self.state.get(), State::Dropping);
        // Safety: the preceding poll/drop transition owns the lifecycle and
        // left the cell in `Dropping`, so no other access to storage is live.
        unsafe {
            ptr::write(
                &raw mut (*self.storage.get()).output,
                ManuallyDrop::new(result),
            );
        }
        self.state.set(State::FutureResult);
    }
}

impl<F: Future> Drop for FutureCell<F> {
    fn drop(&mut self) {
        let storage = self.storage.get_mut();
        match self.state.get() {
            State::Future => abort_on_panic(|| unsafe {
                ManuallyDrop::drop(&mut storage.future);
            }),
            State::FutureResult => abort_on_panic(|| unsafe {
                ManuallyDrop::drop(&mut storage.output);
            }),
            State::Empty | State::Dropping => {}
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum State {
    Future,
    FutureResult,
    Empty,
    Dropping,
}

union Storage<F: Future> {
    future: ManuallyDrop<F>,
    output: ManuallyDrop<Result<F::Output, TaskError>>,
}

/// [`TaskError`] indicates a failure in a task.
///
/// Tasks can fail for one of two reasons. Either the task was cancelled, or
/// the task panicked. Users can check which of these two reasons caused the
/// failure via the [`TaskError::is_cancelled`] and [`TaskError::is_panic`]
/// methods.
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

    /// Consumes this error and returns the panic payload, if the task panicked.
    ///
    /// The payload can be inspected with `Box::downcast` or passed to
    /// [`std::panic::resume_unwind`] to resume the original panic. This returns
    /// `None` when the task was cancelled.
    pub fn into_panic(self) -> Option<Box<dyn Any + Send + 'static>> {
        match self.inner {
            Kind::Panic(payload) => Some(payload),
            Kind::Cancelled => None,
        }
    }
}

enum Kind {
    Cancelled,
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

#[cfg(test)]
mod tests {
    use super::FutureCell;
    use std::cell::Cell;
    use std::future::Future;
    use std::marker::PhantomPinned;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::task::{Context, Poll, Waker};

    struct AddressChecked {
        polled_at: Rc<Cell<*const Self>>,
        moved_on_drop: Rc<Cell<bool>>,
        _pin: PhantomPinned,
    }

    impl Future for AddressChecked {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polled_at.set(self.as_ref().get_ref() as *const Self);
            Poll::Pending
        }
    }

    impl Drop for AddressChecked {
        fn drop(&mut self) {
            self.moved_on_drop
                .set(!std::ptr::eq(self.polled_at.get(), self));
        }
    }

    #[test]
    fn drops_a_polled_future_in_place() {
        let polled_at = Rc::new(Cell::new(std::ptr::null()));
        let moved_on_drop = Rc::new(Cell::new(false));
        let cell = Box::pin(FutureCell::new(AddressChecked {
            polled_at: Rc::clone(&polled_at),
            moved_on_drop: Rc::clone(&moved_on_drop),
            _pin: PhantomPinned,
        }));
        let cx = Context::from_waker(Waker::noop());

        assert!(unsafe { cell.as_ref().get_ref().poll(cx) }.is_pending());
        cell.as_ref().get_ref().cancel();

        assert!(!moved_on_drop.get());
    }
}
