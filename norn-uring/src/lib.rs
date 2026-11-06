//! Linux `io_uring` support for Norn runtimes.
//!
//! [`Driver`] implements [`Park`] and can be placed directly under
//! [`norn_executor::LocalExecutor`]. Most operations require an active driver
//! context (typically via [`norn_executor::LocalExecutor`] with this driver as
//! the park layer). APIs that depend on context will panic if called outside
//! that runtime context.
//!
//! I/O methods take ownership of their buffers while the kernel may access
//! them and return those buffers with the completion result. See [`buf`] for
//! the stable-address contracts that make this safe.
//!
//! # Example
//!
//! ```no_run
//! use norn_executor::LocalExecutor;
//! use norn_uring::fs::File;
//!
//! let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64)?;
//! let mut executor = LocalExecutor::new(driver);
//! executor.block_on(async {
//!     let file = File::open("data.bin").await?;
//!     let (result, buffer) = file.read_at(vec![0_u8; 4096], 0).await;
//!     let bytes_read = result?;
//!     file.close().await?;
//!     assert!(bytes_read <= buffer.len());
//!     Ok::<(), std::io::Error>(())
//! })?;
//! # Ok::<(), std::io::Error>(())
//! ```
//!
//! # Dropping an operation
//!
//! Dropping an operation future cancels it, and what that means depends on
//! how far the operation got:
//!
//! - Never polled: nothing happened, and nothing is submitted.
//! - Waiting for SQ space in the driver's queue: the operation is cancelled
//!   there with `ECANCELED` and never reaches the kernel.
//! - Held by the kernel: the driver queues an `IORING_OP_ASYNC_CANCEL`. The
//!   kernel may complete the operation first, and its result is then
//!   discarded: a receive's data is lost, and a send or write may have
//!   happened. To keep a result, keep polling the operation instead.
//!
//! The kernel cannot cancel some operations once it holds them: a zero-copy
//! send waiting for its notification, an in-flight `uring_cmd`, and a linked
//! timeout, which ends with the request it bounds. They keep their buffers
//! until the kernel finishes them, and driver shutdown waits for them.
//!
//! # Modules
//!
//! - [`buf`]: stable buffer traits and adapters used by I/O operations.
//! - [`block`]: typed commands for Linux block devices.
//! - [`bufring`]: registered `io_uring` buffer-ring support.
//! - [`epoll`]: epoll instances controlled and waited on through `io_uring`.
//! - [`fixedbuf`]: caller-owned buffers registered for fixed I/O.
//! - [`fs`]: asynchronous filesystem operations.
//! - [`net`]: asynchronous TCP and UDP networking.
//! - [`ring`]: which `io_uring` opcodes the kernel implements.
//! - [`uring_cmd`]: the low-level device-command extension contract.
//!
//! # Low-level extension APIs
//!
//! [`Request`] and its combinator types are public because filesystem and
//! networking methods return lazy requests that applications may transform or
//! attach timeouts to. [`Operation`], [`Op`], [`Singleshot`], [`Multishot`],
//! and [`CQEResult`] form the lower-level interface for implementing custom
//! `io_uring` operations. Implementing [`Operation`] is unsafe because
//! implementors must uphold its address-stability, kernel-ownership, and
//! completion-reaping contracts.
//!
//! [`Park`]: norn_executor::park::Park
//! [io_uring]: https://kernel.dk/io_uring.pdf
#![cfg(target_os = "linux")]
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    rustdoc::bare_urls,
    rustdoc::broken_intra_doc_links,
    unnameable_types,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
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

/// Commands for Linux block devices.
pub mod block;
/// Stable buffer traits and adapters for io_uring operations.
pub mod buf;
/// Registered io_uring buffer-ring support.
pub mod bufring;
/// Epoll instances driven through io_uring.
pub mod epoll;
/// Owned caller-provided buffers registered for fixed I/O.
pub mod fixedbuf;
/// Asynchronous filesystem operations.
pub mod fs;
/// Asynchronous TCP and UDP networking.
pub mod net;
/// Which `io_uring` opcodes the kernel implements.
pub mod ring;
/// Device-specific commands submitted through `IORING_OP_URING_CMD`.
pub mod uring_cmd;

pub use driver::{Driver, DriverOptions, Handle};
pub use fd::UringFd;
pub use operation::{CQEResult, Multishot, Op, Operation, Singleshot};
pub use request::{
    LinkedTimeoutControl, Map, Request, Then, ThenAux, Timeout, TimeoutControl, TimeoutOutcome,
    TimeoutRemove, TimeoutUpdate, WithOptions, WithTimeout,
};
pub use util::noop;
