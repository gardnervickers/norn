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
//! # Modules
//!
//! - [`buf`]: stable buffer traits and adapters used by I/O operations.
//! - [`bufring`]: registered `io_uring` buffer-ring support.
//! - [`fixedbuf`]: caller-owned buffers registered for fixed I/O.
//! - [`fs`]: asynchronous filesystem operations.
//! - [`net`]: asynchronous TCP and UDP networking.
//!
//! # Low-level extension APIs
//!
//! [`Request`] and its combinator types are public because filesystem and
//! networking methods return lazy requests that applications may transform or
//! attach timeouts to. [`Operation`], [`Op`], [`Singleshot`], [`Multishot`],
//! and [`CQEResult`] form the lower-level interface for implementing custom
//! `io_uring` operations. Implementing [`Operation`] is unsafe; its rustdoc
//! states the address-stability, kernel-ownership, and cleanup requirements.
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
