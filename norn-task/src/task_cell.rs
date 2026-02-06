//! The core module works by using the [`TaskCell`] to allocate a task
//! and build a vtable. The vtable is then stored in the header.
//!
//! This allows us to use the header pointer to access the vtable
//! and call any [`TaskCell`] methods via the type-erased `NonNull<Header>` pointer.
use std::future::Future;
use std::marker::PhantomData;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::{mem, ptr};

use crate::future_cell;
use crate::header;
use crate::state::{self, DropRefResult, StateCell};
use crate::Schedule;

/// [`TaskCell`] serves a the allocated type which olds the [`Future`] and
/// all related data.
#[repr(C)]
pub(crate) struct TaskCell<S, F>
where
    F: Future,
{
    header: header::Header,
    scheduler: S,
    future: future_cell::FutureCell<F>,
}

impl<S, F> TaskCell<S, F>
where
    F: Future,
    S: Schedule,
{
    const TASK_VTABLE: VTable = Self::vtable();
    const WAKER_VTABLE: RawWakerVTable = Self::waker_vtable();

    pub(crate) fn allocate(future: F, scheduler: S) -> (TaskRef, JoinHandleRef<F::Output>) {
        let state = StateCell::new();
        let header = header::Header::new(state, &Self::TASK_VTABLE);
        let future = future_cell::FutureCell::new(future);
        let task = TaskCell {
            header,
            scheduler,
            future,
        };
        let boxed = Box::new(task);
        let raw = Box::into_raw(boxed);
        // Safety: `raw` is a valid pointer to a `TaskCell`, which is `repr(C)`.
        //         This means that `raw` is a valid pointer to a `Header`.
        unsafe {
            let r1 = TaskRef(NonNull::new_unchecked(raw.cast()));
            let r2 = TaskRef(NonNull::new_unchecked(raw.cast()));
            let r2 = JoinHandleRef(r2, PhantomData);
            (r1, r2)
        }
    }

    const fn vtable() -> VTable {
        VTable {
            dealloc: Self::dealloc,
            abort: Self::abort,
            poll: Self::poll,
            try_read_output: Self::try_read_output,
            shutdown: Self::shutdown,
        }
    }

    const fn waker_vtable() -> RawWakerVTable {
        RawWakerVTable::new(
            Self::waker_clone,
            Self::waker_wake_val,
            Self::waker_wake_ref,
            Self::waker_drop,
        )
    }

    #[inline]
    fn scheduler(&self) -> &S {
        &self.scheduler
    }

    #[inline]
    fn header(&self) -> &header::Header {
        &self.header
    }

    #[inline]
    fn state(&self) -> &StateCell {
        self.header.state()
    }

    #[inline]
    fn future_cell(&self) -> &future_cell::FutureCell<F> {
        &self.future
    }

    unsafe fn waker_clone(ptr: *const ()) -> RawWaker {
        let ptr = ptr::NonNull::new_unchecked(ptr as *mut header::Header);
        Self::check_caller(ptr);
        let this = Self::from_raw_header(ptr);
        let state = this.as_ref().state();
        state.update(state::State::clone_ref);
        RawWaker::new(ptr.as_ptr() as _, &Self::WAKER_VTABLE)
    }

    unsafe fn waker_wake_val(ptr: *const ()) {
        // This is a combined wake and drop.
        let p = ptr::NonNull::new_unchecked(ptr as *mut header::Header);
        Self::check_caller(p);
        let this = Self::from_raw_header(p);
        let state = this.as_ref().state();
        match state.update(state::State::notify) {
            state::NotifyResult::DoNothing => {
                Self::waker_drop(ptr);
            }
            state::NotifyResult::SubmitTask => {
                // `wake` consumes this waker, so we can transfer its refcount
                // directly into the scheduled runnable instead of clone+drop.
                let taskref = TaskRef::from_ptr(p);
                let scheduler = this.as_ref().scheduler();
                scheduler.schedule(crate::Runnable::from(taskref));
            }
        }
    }

    unsafe fn waker_wake_ref(ptr: *const ()) {
        let ptr = ptr::NonNull::new_unchecked(ptr as *mut header::Header);
        Self::check_caller(ptr);
        let this = Self::from_raw_header(ptr);
        let state = this.as_ref().state();
        match state.update(state::State::notify_and_clone) {
            state::NotifyResult::DoNothing => (),
            state::NotifyResult::SubmitTask => {
                let taskref = TaskRef::from_ptr(ptr);
                let scheduler = this.as_ref().scheduler();
                scheduler.schedule(crate::Runnable::from(taskref));
            }
        }
    }

    unsafe fn waker_drop(ptr: *const ()) {
        let ptr = ptr::NonNull::new_unchecked(ptr as *mut header::Header);
        Self::check_caller(ptr);
        let this = Self::from_raw_header(ptr);
        match this.as_ref().state().update(state::State::drop_ref) {
            DropRefResult::DropTask =>
            // Safety: The reference count is zero so it is safe to drop the task.
            unsafe { (this.as_ref().header().vtable().dealloc)(ptr) },
            DropRefResult::KeepTask => {}
        }
    }

    /// Create a borrowed [`Waker`] from the provided `ptr`.
    ///
    /// Unlike an owned waker, this does not adjust the task refcount. This is
    /// used only while polling the future and never moved out of this scope.
    ///
    /// # Safety
    /// Callers must ensure that `ptr` is valid for the lifetime of the returned
    /// waker and that the returned waker is not moved out and dropped.
    unsafe fn borrowed_waker(ptr: NonNull<header::Header>) -> ManuallyDrop<Waker> {
        let raw = RawWaker::new(ptr.as_ptr() as _, &Self::WAKER_VTABLE);
        ManuallyDrop::new(Waker::from_raw(raw))
    }

    unsafe fn poll(ptr: NonNull<header::Header>) {
        let this = Self::from_raw_header(ptr);
        // First, we set the poll state.
        let state = this.as_ref().state();
        let result = match state.update(state::State::prepare_poll) {
            state::PreparePollResult::Complete => PollResult::Done,
            state::PreparePollResult::Cancelled => {
                this.as_ref().future_cell().cancel();
                PollResult::Complete
            }
            state::PreparePollResult::Ok => {
                let waker = Self::borrowed_waker(ptr);
                let cx = Context::from_waker(&waker);
                let future_cell = this.as_ref().future_cell();

                if future_cell.poll(cx).is_ready() {
                    PollResult::Complete
                } else {
                    match this
                        .as_ref()
                        .state()
                        .update(state::State::complete_poll_and_clone)
                    {
                        state::CompletePollResult::NotifiedDuringPoll => PollResult::Notified,
                        state::CompletePollResult::Cancelled => {
                            this.as_ref().future_cell().cancel();
                            PollResult::Complete
                        }
                        state::CompletePollResult::Ok => PollResult::Done,
                    }
                }
            }
        };

        match result {
            PollResult::Complete => {
                Self::complete_task(ptr);
            }
            PollResult::Done => {}
            PollResult::Notified => {
                let taskref = TaskRef::from_ptr(ptr);
                let scheduler = this.as_ref().scheduler();
                scheduler.schedule(crate::Runnable::from(taskref));
            }
        }
    }

    unsafe fn complete_task(ptr: NonNull<header::Header>) {
        let this = Self::from_raw_header(ptr);
        let state = this.as_ref().state();
        let header = this.as_ref().header();
        match state.update(state::State::complete_task) {
            state::CompleteTaskResult::NotifyJoinHandle => {
                header.notify_join_handle();
            }
            state::CompleteTaskResult::DropOutput => {
                let future_cell = this.as_ref().future_cell();
                future_cell.destroy();
            }
        }

        // Finally we need to unbind the task from the scheduler.
        // We can directly convert this taskref into a registered task
        // for the unbind step without bumping the refcount, as unbind
        // takes a reference. We just forget it later to avoid running
        // the drop logic.
        let taskref = crate::RegisteredTask::from(TaskRef::from_ptr(ptr));
        let scheduler = this.as_ref().scheduler();
        scheduler.unbind(&taskref);
        mem::forget(taskref);
    }

    unsafe fn dealloc(ptr: NonNull<header::Header>) {
        let this = Self::from_raw_header(ptr);
        drop(Box::from_raw(this.as_ptr()));
    }

    unsafe fn abort(ptr: NonNull<header::Header>) {
        let this = Self::from_raw_header(ptr);
        match this.as_ref().state().update(state::State::abort_and_clone) {
            state::AbortResult::DoNothing => (),
            state::AbortResult::SubmitTask => {
                let taskref = TaskRef::from_ptr(ptr);
                let scheduler = this.as_ref().scheduler();
                scheduler.schedule(crate::Runnable::from(taskref));
            }
        }
    }

    unsafe fn shutdown(ptr: NonNull<header::Header>) {
        let this = Self::from_raw_header(ptr);
        let state = this.as_ref().state();
        match state.update(state::State::shutdown) {
            state::ShutdownResult::AlreadyCompleted => {
                // Nothing to do!
            }
            state::ShutdownResult::NotCompleted => {
                this.as_ref().future_cell().cancel();
            }
        }
    }

    unsafe fn try_read_output(ptr: NonNull<header::Header>, target: *mut (), waker: &Waker) {
        let out = &mut *target.cast::<Poll<Result<F::Output, crate::TaskError>>>();
        let this = Self::from_raw_header(ptr);
        let state = this.as_ref().state();

        if state.is_complete() {
            let future_cell = this.as_ref().future_cell();
            let output = future_cell.take_output();
            *out = Poll::Ready(output);
        } else {
            // Set the waker.
            let header = this.as_ref().header();
            header.set_waker(waker);
        }
    }

    #[inline]
    unsafe fn from_raw_header(ptr: NonNull<header::Header>) -> NonNull<Self> {
        ptr.cast()
    }

    #[inline]
    unsafe fn check_caller(ptr: NonNull<header::Header>) {
        if let Some(tid) = ptr.as_ref().thread {
            let caller_thread = std::thread::current().id();
            if tid != caller_thread {
                std::process::abort();
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum PollResult {
    /// The task has completed, call complete.
    Complete,
    /// The poll operation has finished. Nothing to do.
    Done,
    /// The task has been notified during poll. Resubmit it.
    Notified,
}

pub(crate) struct TaskRef(NonNull<header::Header>);

pub(crate) struct JoinHandleRef<T>(TaskRef, PhantomData<T>);

impl TaskRef {
    fn state(&self) -> &StateCell {
        self.header().state()
    }

    fn header(&self) -> &header::Header {
        // Safety: TaskRef is only constructed from a NonNull<header::Header>,
        //         and it is guaranteed to be valid as long as we have a TaskRef
        //         due to refcounting.
        unsafe { self.0.as_ref() }
    }

    fn vtable(&self) -> &'static VTable {
        self.header().vtable()
    }

    pub(crate) fn from_ptr(ptr: NonNull<header::Header>) -> Self {
        Self(ptr)
    }

    pub(crate) fn run(self) {
        // Safety: The task is valid as long as we have a TaskRef.
        unsafe {
            (self.vtable().poll)(self.0);
        }
    }

    pub(crate) fn as_ptr(&self) -> NonNull<header::Header> {
        self.0
    }

    pub(crate) fn into_ptr(self) -> NonNull<header::Header> {
        let ptr = self.0;
        std::mem::forget(self);
        ptr
    }

    pub(crate) fn shutdown(&self) {
        // Safety: The task is valid as long as we have a TaskRef.
        unsafe {
            (self.vtable().shutdown)(self.0);
        }
    }
}

impl Clone for TaskRef {
    fn clone(&self) -> Self {
        self.state().update(state::State::clone_ref);
        Self(self.0)
    }
}

impl Drop for TaskRef {
    fn drop(&mut self) {
        match self.state().update(state::State::drop_ref) {
            DropRefResult::DropTask => {
                // Safety: The task is valid as long as we have a TaskRef. We are the
                // last TaskRef as indicated by the return value of `drop_ref`. So we
                // can safely deallocate the task.
                unsafe { (self.0.as_ref().vtable().dealloc)(self.0) }
            }
            DropRefResult::KeepTask => {}
        }
    }
}

impl<T> JoinHandleRef<T> {
    pub(crate) fn poll_result(&self, waker: &Waker) -> Poll<Result<T, crate::TaskError>> {
        let mut retval: Poll<Result<T, crate::TaskError>> = Poll::Pending;
        let vtable = self.0.vtable();
        // Safety: The task is valid as long as we have a TaskRef. It is always
        // safe to poll the task.
        unsafe {
            let target = &raw mut retval;
            let target = target.cast::<()>();
            (vtable.try_read_output)(self.0 .0, target, waker);
        };
        retval
    }

    pub(crate) fn abort(&self) {
        let vtable = self.0.vtable();
        // Safety: The task is valid as long as we have a TaskRef.
        unsafe { (vtable.abort)(self.0 .0) }
    }
}

impl<T> Drop for JoinHandleRef<T> {
    fn drop(&mut self) {
        self.0.state().update(state::State::drop_join_handle);
    }
}

pub(crate) struct VTable {
    pub(crate) dealloc: unsafe fn(NonNull<header::Header>),
    pub(crate) abort: unsafe fn(NonNull<header::Header>),
    pub(crate) poll: unsafe fn(NonNull<header::Header>),
    pub(crate) try_read_output: unsafe fn(NonNull<header::Header>, *mut (), &Waker),
    pub(crate) shutdown: unsafe fn(NonNull<header::Header>),
}
