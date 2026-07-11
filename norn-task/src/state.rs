//! Task state transitions.

use std::cell::Cell;

bitflags::bitflags! {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    struct Flags: u8 {
        /// The task has been notified and should be polled.
        const NOTIFIED    = 1<<0;
        /// The task is currently running.
        const RUNNING     = 1<<1;
        /// The task has completed with a result.
        const COMPLETE    = 1<<2;
        /// Indicates the presence of a [`JoinHandle`].
        const JOIN_HANDLE = 1<<3;
        /// The task has been cancelled.
        const CANCELLED   = 1<<4;
    }
}
/// [`StateCell`] is a cell that tracks the state of a task.
///
/// [`StateCell`] combines a refcount with a set of flags to track
/// the state of a task.
pub(crate) struct StateCell {
    state: Cell<State>,
}

impl StateCell {
    /// Build a new [`StateCell`] with the [`JOIN_HANDLE`] and [`NOTIFIED`] flags set.
    ///
    /// The initial reference count is `initial_refcount`.
    pub(crate) fn new(initial_refcount: u32) -> Self {
        Self {
            state: Cell::new(State::new(initial_refcount)),
        }
    }

    /// Update the state in this [`StateCell`].
    ///
    /// The task system relies on the state to maintain memory safety. As a result,
    /// any invalid updates to the state will abort the program.
    #[inline]
    pub(crate) fn update<U>(&self, f: impl FnOnce(&mut State) -> U) -> U {
        let mut state = self.state.get();
        crate::util::abort_on_panic(|| {
            let res = f(&mut state);
            self.state.set(state);

            res
        })
    }

    /// Returns true if the task is complete.
    #[inline]
    pub(crate) fn is_complete(&self) -> bool {
        let state = self.state.get();
        state.flags.contains(Flags::COMPLETE)
    }
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct State {
    flags: Flags,
    refcount: u32,
}

impl State {
    /// Create a new [`State`] with the [`JOIN_HANDLE`] and [`NOTIFIED`]
    /// flags set and a reference count of `initial_refcount`.
    fn new(initial_refcount: u32) -> Self {
        assert!(initial_refcount > 0);
        let mut flags = Flags::empty();
        flags.insert(Flags::JOIN_HANDLE);
        flags.insert(Flags::NOTIFIED);
        Self {
            flags,
            refcount: initial_refcount,
        }
    }

    /// Prepare the task for polling.
    ///
    /// This consumes prior notifications and sets
    /// the [`RUNNING`] flag, informing future notifications
    /// that the task is already running
    #[inline]
    pub(crate) fn prepare_poll(&mut self) -> PreparePollResult {
        assert!(self.refcount > 0);
        assert!(self.flags.contains(Flags::NOTIFIED));
        assert!(!self.flags.contains(Flags::RUNNING));

        if self.flags.contains(Flags::COMPLETE) {
            return PreparePollResult::Complete;
        }

        self.flags.insert(Flags::RUNNING);
        self.flags.remove(Flags::NOTIFIED);
        if self.flags.contains(Flags::CANCELLED) {
            return PreparePollResult::Cancelled;
        }
        PreparePollResult::Ok
    }

    /// Conclude polling for a task.
    ///
    /// This clears the [`RUNNING`] flag and returns
    /// whether the task was notified during polling or if it is cancelled.
    #[inline]
    pub(crate) fn complete_poll(&mut self) -> CompletePollResult {
        assert!(self.flags.contains(Flags::RUNNING));
        assert!(self.refcount > 0);

        if self.flags.contains(Flags::CANCELLED) {
            return CompletePollResult::Cancelled;
        }
        self.flags.remove(Flags::RUNNING);
        if self.flags.contains(Flags::NOTIFIED) {
            return CompletePollResult::NotifiedDuringPoll;
        }
        CompletePollResult::Ok
    }

    /// Conclude polling for a task and clone the task ref if it must be rescheduled.
    #[inline]
    pub(crate) fn complete_poll_and_clone(&mut self) -> CompletePollResult {
        let res = self.complete_poll();
        if matches!(res, CompletePollResult::NotifiedDuringPoll) {
            self.clone_ref();
        }
        res
    }

    /// Mark the task as complete.
    ///
    /// Returns whether the task output should be dropped or if the join
    /// handle should be notified that the task is complete.
    #[inline]
    pub(crate) fn complete_task(&mut self) -> CompleteTaskResult {
        assert!(self.refcount > 0);
        assert!(self.flags.contains(Flags::RUNNING));
        assert!(!self.flags.contains(Flags::COMPLETE));
        self.flags.insert(Flags::COMPLETE);
        self.flags.remove(Flags::RUNNING);

        if self.flags.contains(Flags::JOIN_HANDLE) {
            CompleteTaskResult::NotifyJoinHandle
        } else {
            CompleteTaskResult::DropOutput
        }
    }

    /// Decrement the reference count.
    ///
    /// Returns whether the task should be dropped.
    #[inline]
    pub(crate) fn drop_ref(&mut self) -> DropRefResult {
        assert!(self.refcount > 0);
        self.refcount -= 1;
        if self.refcount == 0 {
            assert!(!self.flags.contains(Flags::JOIN_HANDLE));
            return DropRefResult::DropTask;
        }
        DropRefResult::KeepTask
    }

    #[inline]
    pub(crate) fn clone_ref(&mut self) {
        assert!(self.refcount > 0);
        self.refcount = self.refcount.checked_add(1).expect("overflow");
    }

    /// Notify the task.
    ///
    /// Returns instructions if the task should be submitted to the executor.
    #[inline]
    pub(crate) fn notify(&mut self) -> NotifyResult {
        assert!(self.refcount > 0);
        if self.flags.contains(Flags::COMPLETE) || self.flags.contains(Flags::NOTIFIED) {
            return NotifyResult::DoNothing;
        }

        self.flags.insert(Flags::NOTIFIED);
        if self.flags.contains(Flags::RUNNING) {
            // Don't re-submit the task if it is running already.
            NotifyResult::DoNothing
        } else {
            NotifyResult::SubmitTask
        }
    }

    /// Notify the task and clone the task ref if it should be submitted.
    #[inline]
    pub(crate) fn notify_and_clone(&mut self) -> NotifyResult {
        let res = self.notify();
        if matches!(res, NotifyResult::SubmitTask) {
            self.clone_ref();
        }
        res
    }

    /// Shutdown the task.
    ///
    /// This is to be called from the executor during shutdown. As a result,
    /// we enforce that the task is not running.
    #[inline]
    pub(crate) fn shutdown(&mut self) -> ShutdownResult {
        assert!(self.refcount > 0);
        assert!(!self.flags.contains(Flags::RUNNING));
        self.flags.insert(Flags::CANCELLED);
        if self.flags.contains(Flags::COMPLETE) {
            ShutdownResult::AlreadyCompleted
        } else {
            self.flags.insert(Flags::COMPLETE);
            ShutdownResult::NotCompleted
        }
    }

    /// Abort the task.
    ///
    /// This is called directly from the [`JoinHandle`] by the application.
    #[inline]
    pub(crate) fn abort(&mut self) -> AbortResult {
        assert!(self.refcount > 0);
        assert!(self.flags.contains(Flags::JOIN_HANDLE));

        if self.flags.contains(Flags::CANCELLED) || self.flags.contains(Flags::COMPLETE) {
            return AbortResult::DoNothing;
        }

        self.flags.insert(Flags::CANCELLED);
        if self.flags.contains(Flags::RUNNING) {
            // The task was cancelled. while it was running. Just set the notified
            // flag so that the task is run again and the executor notices that
            // it was cancelled.
            self.flags.insert(Flags::NOTIFIED);
            return AbortResult::DoNothing;
        }
        if self.flags.contains(Flags::NOTIFIED) {
            // If the task was notified, then there is nothing to do. We will
            // notice that the task was cancelled when we poll it.
            return AbortResult::DoNothing;
        }
        self.flags.insert(Flags::NOTIFIED);
        AbortResult::SubmitTask
    }

    /// Abort the task and clone the task ref if it should be submitted.
    #[inline]
    pub(crate) fn abort_and_clone(&mut self) -> AbortResult {
        let res = self.abort();
        if matches!(res, AbortResult::SubmitTask) {
            self.clone_ref();
        }
        res
    }

    /// Mark the task as having it's [`JoinHandle`] dropped.
    #[inline]
    pub(crate) fn drop_join_handle(&mut self) {
        assert!(self.refcount > 0);
        assert!(self.flags.contains(Flags::JOIN_HANDLE));
        self.flags.remove(Flags::JOIN_HANDLE);
    }
}

#[must_use = "this `ShutdownResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum PreparePollResult {
    Ok,
    Complete,
    Cancelled,
}

#[must_use = "this `AbortResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum CompletePollResult {
    NotifiedDuringPoll,
    Cancelled,
    Ok,
}

#[must_use = "this `CompleteTaskResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum CompleteTaskResult {
    NotifyJoinHandle,
    DropOutput,
}

#[must_use = "this `DropRefResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum DropRefResult {
    DropTask,
    KeepTask,
}

#[must_use = "this `NotifyResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum NotifyResult {
    DoNothing,
    SubmitTask,
}

#[must_use = "this `ShutdownResult` must be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum ShutdownResult {
    AlreadyCompleted,
    NotCompleted,
}

#[must_use = "this `AbortResult` may be a `SubmitTask` variant, which should be handled"]
#[derive(Debug, Copy, Clone)]
pub(crate) enum AbortResult {
    DoNothing,
    SubmitTask,
}
