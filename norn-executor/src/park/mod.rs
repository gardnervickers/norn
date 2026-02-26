//! Contains the [`Park`] and [`Unpark`] traits.
//!
//! The [`LocalExecutor`] uses a [`Park`] to delegate
//! control of the thread when there are no more tasks
//! to run.
//!
//! [`Park`] implementations are expected to utilize this
//! to process external events or execute work items.
//!
//! [`LocalExecutor`]: crate::LocalExecutor
//! [`Park`]: crate::park::Park
//! [`Unpark`]: crate::park::Unpark
use std::io;
use std::sync::Arc;
use std::time::Duration;

mod spin;
mod thread;

pub use spin::SpinPark;
pub use thread::ThreadPark;

/// Indicates under what conditions a [`Park`] operation
/// should return.
///
/// Note it is always valid to return from a [`Park`] operation
/// early.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParkMode {
    /// The [`Park`] operation should return immediately.
    NoPark,

    /// The [`Park`] operation should return when there is
    /// some new work to do at a higher layer. Examples of
    /// this might be when timers fire or I/O completes.
    NextCompletion,

    /// The [`Park`] operation should return before the
    /// specified duration has elapsed.
    Timeout(Duration),
}

/// The [`Park`] trait provides a way to share control flow
/// between different layers in a runtime.
///
/// As each layer exhausts the work it can perform, it calls
/// [`Park::park`] to pass control to the next layer.
///
/// A [`ParkMode`] is passed to [`Park::park`] to indicate
/// under what conditions control flow must be passed back.
///
/// ## Unparker
///
/// The [`Park::unparker`] method returns a [`Unpark`] that
/// can be used to force a wakeup from another thread.
pub trait Park {
    /// The [`Park::Unparker`] associated with this [`Park`] instance.
    type Unparker: Unpark + Clone + Send + Sync + 'static;

    /// The [`Park::Guard`] associated with this [`Park`] instance.
    ///
    /// Guard objects will be dropped once all tasks have stopped.
    /// This is a good place to put cleanup logic for things like
    /// handles in thread-local storage.
    type Guard;

    /// Trigger a park operation, passing control to the next layer.
    ///
    /// Layers must respect the [`ParkMode`] passed to this method.
    fn park(&mut self, mode: ParkMode) -> Result<(), io::Error>;

    /// Get a [`Park::Guard`] for this [`Park`] instance.
    ///
    /// This will be called before any calls to [`Park::park`],
    /// and can be used to setup any thread-local state needed
    /// to handle requests.
    fn enter(&self) -> Self::Guard;

    /// Returns an unparker associated with this [`Park`] instance.
    fn unparker(&self) -> Self::Unparker;

    /// Signals the executor that a park is needed by a lower layer.
    ///
    /// This is an optimistic call. The executor may choose to ignore
    /// it but generally it will be called on each task invocation so
    /// it is important that this is a cheap operation.
    ///
    /// One use case is to detect when a layer has a large backlog of
    /// work.
    fn needs_park(&self) -> bool;

    /// Shutdown the park layer.
    ///
    /// Callers should not use any services provided by the park layer
    /// after calling shutdown. There is no guarantee that wakers
    /// registered with the park layer will be invoked after shutdown
    /// is triggered.
    fn shutdown(&mut self);
}

/// The [`Unpark`] trait provides a way to force a wakeup
/// of a thread which is blocked in a [`Park::park`] operation.
pub trait Unpark {
    /// Unpark the associated [`Park`] instance.
    fn unpark(&self);
}

impl<T> Unpark for &T
where
    T: Unpark,
{
    fn unpark(&self) {
        (**self).unpark()
    }
}

impl<T> Unpark for &mut T
where
    T: Unpark,
{
    fn unpark(&self) {
        (**self).unpark()
    }
}

impl<T> Unpark for Arc<T>
where
    T: Unpark,
{
    fn unpark(&self) {
        (**self).unpark()
    }
}
