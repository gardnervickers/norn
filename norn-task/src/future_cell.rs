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
use std::{fmt, mem::ManuallyDrop, ptr};

use crate::util::abort_on_panic;

pub(crate) struct FutureCell<F>
where
    F: Future,
{
    inner: RefCell<Inner<F>>,
}

impl<F> FutureCell<F>
where
    F: Future,
{
    pub(crate) fn new(future: F) -> Self {
        Self {
            inner: RefCell::new(Inner::new(future)),
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
        let this = &mut *self.inner.borrow_mut();
        this.state = State::Empty;
    }

    /// Take the output of the future, if it has been polled to completion.
    ///
    /// # Panic
    /// If the future has not been polled to completion, or if it has been destroyed,
    /// this method will panic.
    pub(crate) fn take_output(&self) -> Result<F::Output, TaskError> {
        let this = &mut *self.inner.borrow_mut();
        if this.state != State::FutureResult {
            panic!("future not polled to completion");
        }
        this.state = State::Empty;
        unsafe { ManuallyDrop::take(&mut this.storage.output) }
    }

    /// Perform the poll operation on the future.
    ///
    /// # Panic
    /// This method will panic if the future has already been polled to completion,
    /// or has been dropped.
    ///
    pub(crate) unsafe fn poll(&self, cx: Context<'_>) -> Poll<()> {
        let result = {
            let this = &mut *self.inner.borrow_mut();
            unsafe { this.poll(cx) }
        };

        match result {
            Poll::Ready(result) => {
                self.drop_future();
                self.finish_result(result);
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn begin_drop(&self) -> State {
        let this = &mut *self.inner.borrow_mut();
        let state = this.state;
        if matches!(state, State::Future | State::FutureResult) {
            this.state = State::Dropping;
        }
        state
    }

    fn drop_future(&self) {
        let storage = {
            let this = &mut *self.inner.borrow_mut();
            &mut this.storage as *mut Storage<F>
        };
        abort_on_panic(|| unsafe {
            ManuallyDrop::drop(&mut (*storage).future);
        });
    }

    fn drop_output(&self) {
        let storage = {
            let this = &mut *self.inner.borrow_mut();
            &mut this.storage as *mut Storage<F>
        };
        abort_on_panic(|| unsafe {
            ManuallyDrop::drop(&mut (*storage).output);
        });
    }

    fn finish_result(&self, result: Result<F::Output, TaskError>) {
        let this = &mut *self.inner.borrow_mut();
        debug_assert_eq!(this.state, State::Dropping);
        unsafe {
            ptr::write(&mut this.storage.output, ManuallyDrop::new(result));
        }
        this.state = State::FutureResult;
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

struct Inner<F: Future> {
    state: State,
    storage: Storage<F>,
}

impl<F: Future> Inner<F> {
    fn new(future: F) -> Self {
        Self {
            state: State::Future,
            storage: Storage {
                future: ManuallyDrop::new(future),
            },
        }
    }

    /// Poll the inner future to completion.
    ///
    unsafe fn poll(&mut self, mut cx: Context<'_>) -> Poll<Result<F::Output, TaskError>> {
        match self.state {
            State::Future => {
                let fut = unsafe { Pin::new_unchecked(&mut *self.storage.future) };
                match panic::catch_unwind(panic::AssertUnwindSafe(|| fut.poll(&mut cx))) {
                    Ok(Poll::Ready(result)) => {
                        self.state = State::Dropping;
                        Poll::Ready(Ok(result))
                    }
                    Ok(Poll::Pending) => Poll::Pending,
                    Err(err) => {
                        self.state = State::Dropping;
                        Poll::Ready(Err(TaskError::panic(err)))
                    }
                }
            }
            State::FutureResult => unreachable!("FutureCell is complete"),
            State::Empty => unreachable!("FutureCell is empty"),
            State::Dropping => unreachable!("FutureCell is transitioning"),
        }
    }
}

impl<F: Future> Drop for Inner<F> {
    fn drop(&mut self) {
        match self.state {
            State::Future => abort_on_panic(|| unsafe {
                ManuallyDrop::drop(&mut self.storage.future);
            }),
            State::FutureResult => abort_on_panic(|| unsafe {
                ManuallyDrop::drop(&mut self.storage.output);
            }),
            State::Empty | State::Dropping => {}
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
