//! A [`Park`] implementation providing a driver for
//! [io_uring].
//!
//! Most operations in this crate require running inside an active `Driver`
//! context (typically via [`norn_executor::LocalExecutor`] with this driver as
//! the park layer). APIs that depend on context will panic if called outside
//! that runtime context.
//!
//! # Modules
//! - `buf`: stable buffer traits and adapters used by I/O operations.
//! - `bufring`: registered io_uring buffer-ring support.
//! - `fixedbuf`: caller-owned buffers registered for fixed I/O.
//! - `fs`: asynchronous filesystem operations.
//! - `net`: asynchronous TCP and UDP networking.
//!
//! [`Park`]: norn_executor::park::Park
//! [io_uring]: https://kernel.dk/io_uring.pdf
#![cfg(target_os = "linux")]
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    clippy::missing_safety_doc
)]

pub(crate) mod driver;
pub(crate) mod error;
pub(crate) mod fd;
pub(crate) mod operation;
mod registered_buffers;
mod request;
#[cfg(test)]
mod test_util;
pub(crate) mod util;

/// Stable buffer traits and adapters for io_uring operations.
pub mod buf;
/// Registered io_uring buffer-ring support.
pub mod bufring;
/// Owned caller-provided buffers registered for fixed I/O.
pub mod fixedbuf;
/// Asynchronous filesystem operations.
pub mod fs;
/// Asynchronous TCP and UDP networking.
pub mod net;

pub use driver::{Driver, DriverOptions, Handle};
pub use operation::{CQEResult, Multishot, Op, Operation, Singleshot};
pub use request::{
    LinkedTimeoutControl, Map, Request, Then, ThenAux, Timeout, TimeoutControl, TimeoutOutcome,
    TimeoutRemove, TimeoutUpdate, WithTimeout,
};
pub use util::noop;
