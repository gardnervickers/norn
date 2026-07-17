//! Bounded cross-thread channels for Norn executors.
//!
//! Norn tasks and their wakers are thread-affine. This crate preserves that
//! property: remote senders only enqueue messages and unpark the destination
//! executor. A [`Driver`] running on the destination thread observes ready
//! channels and invokes receiver wakers locally.
//!
//! The initial channel flavors are a bounded multi-producer queue and an
//! explicitly sharded fan-in queue. Both provide bounded bulk receive support
//! in [`mpsc`].
//!
//! # Application shape
//!
//! A sharded application can run one Norn executor per disk scheduler and give
//! each scheduler its own receiver. A scheduler can give each frontend thread
//! a separate ingress lane, avoiding shared producer state in the steady state.
//! Sending only touches thread-safe queue and unpark state; task wakers remain
//! on the scheduler's executor thread.
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
//! let (frontend_txs, mut disk_rx) =
//!     mpsc::bounded_sharded(&driver.handle(), 4_096, 4);
//! let mut disk_executor = LocalExecutor::new(driver);
//!
//! let frontends: Vec<_> = frontend_txs
//!     .into_iter()
//!     .enumerate()
//!     .map(|(frontend, tx)| {
//!         thread::spawn(move || {
//!             tx.try_send(DiskRequest(frontend as u64)).unwrap();
//!         })
//!     })
//!     .collect();
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
//! for frontend in frontends {
//!     frontend.join().unwrap();
//! }
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
