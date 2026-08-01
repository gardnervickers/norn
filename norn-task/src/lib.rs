//! Local task primitives for driving [`Future`]s to completion.
//!
//! Tasks store a future and its state in one reference-counted allocation. The
//! task model is deliberately single-threaded: futures cannot be moved, polled,
//! scheduled, or woken from another thread.
//!
//! Most users interact with [`TaskQueue`] and [`JoinHandle`]. Runtime authors
//! can use [`TaskSet`], [`Schedule`], [`Runnable`], and [`RegisteredTask`] to
//! build a different local scheduler.
//!
//! # Example
//!
//! ```
//! use std::cell::Cell;
//! use std::rc::Rc;
//!
//! let queue = norn_task::TaskQueue::new();
//! let completed = Rc::new(Cell::new(false));
//! let completed_by_task = Rc::clone(&completed);
//! queue
//!     .spawn(async move { completed_by_task.set(true) })
//!     .detach();
//!
//! while let Some(task) = queue.next() {
//!     task.run();
//! }
//!
//! assert!(completed.get());
//! ```
//!
//! The allocation and scheduling design is informed by
//! [Tokio](https://github.com/tokio-rs/tokio) and
//! [async-std](https://github.com/async-rs/async-std).
//!
//! [`Future`]: std::future::Future
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    rustdoc::bare_urls,
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
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
