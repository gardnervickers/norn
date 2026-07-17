//! Bounded cross-thread channels for Norn executors.
//!
//! Norn tasks and their wakers are thread-affine. This crate preserves that
//! property: remote senders only enqueue messages and unpark the destination
//! executor. A [`Driver`] running on the destination thread observes ready
//! channels and invokes receiver wakers locally.
//!
//! The initial channel flavor is a bounded, multi-producer, single-consumer
//! queue with bounded bulk receive support in [`mpsc`].
//!
//! # Application shape
//!
//! A sharded application can run one Norn executor per disk scheduler and give
//! each scheduler its own receiver. Frontend threads retain cloned senders and
//! route requests to a scheduler shard. Sending only touches thread-safe queue
//! and unpark state; task wakers remain on the scheduler's executor thread.
//!
//! ```no_run
//! use std::thread;
//!
//! use norn_channel::{mpsc, Driver};
//! use norn_executor::park::ThreadPark;
//! use norn_executor::LocalExecutor;
//!
//! #[derive(Debug)]
//! struct DiskRequest(u64);
//!
//! let driver = Driver::new(ThreadPark::default());
//! let (frontend_tx, mut disk_rx) = mpsc::bounded(&driver.handle(), 4_096);
//! let mut disk_executor = LocalExecutor::new(driver);
//!
//! let frontend = thread::spawn(move || {
//!     frontend_tx.try_send(DiskRequest(7)).unwrap();
//! });
//!
//! disk_executor.block_on(async move {
//!     let mut batch = Vec::with_capacity(32);
//!     while disk_rx.recv_many(&mut batch, 32).await != 0 {
//!         for request in batch.drain(..) {
//!             // Submit `request` to this shard's local I/O scheduler.
//!             let _ = request.0;
//!         }
//!     }
//! });
//! frontend.join().unwrap();
//! ```
#![deny(
    missing_docs,
    missing_debug_implementations,
    rust_2018_idioms,
    clippy::missing_safety_doc
)]

mod driver;
pub mod mpsc;

pub use driver::{Driver, Handle};
