//! Provides a task abstraction for driving [Futures] to completion.
//!
//! This is inspired by the approaches taken by [Tokio] and [async-std], where
//! tasks store a [Future] in a single reference counted allocation.
//!
//! [`norn_task`] is orientated towards the use case of a single-threaded event loop. Futures
//! cannot be moved, polled, or woken from other threads.
//!
//! # Components
//! - [`TaskQueue`]: spawn and run local tasks from a run queue.
//! - [`TaskSet`]: intrusive set used to track and shut down tasks.
//! - [`JoinHandle`]: awaitable task result handle.
//! - [`Schedule`], [`Runnable`], and [`RegisteredTask`]: scheduling primitives.
//!
//! [Futures]: std::future::Future
//! [Future]: std::future::Future
//! [Tokio]: https://github.com/tokio-rs/tokio
//! [async-std]: https://github.com/async-rs/async-std
//! [`norn_task`]: crate
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    clippy::missing_safety_doc
)]
mod future_cell;
mod header;
mod join;
mod schedule;
mod state;
mod task_cell;
mod taskqueue;
mod tasks;
mod util;

pub use taskqueue::TaskQueue;
pub use tasks::TaskSet;

#[cfg(test)]
mod tests;

pub use future_cell::TaskError;
pub use join::JoinHandle;
pub use schedule::{RegisteredTask, Runnable, Schedule};
