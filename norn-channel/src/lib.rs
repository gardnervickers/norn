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
//! # Shutdown
//!
//! Dropping the last sender is the lossless way to end a channel. Every value
//! accepted by a send (`Ok`) happens-before the final sender drop, so a
//! receiver that drains until [`recv`](mpsc::Receiver::recv) returns `None`
//! sees all of them. To shut down without losing accepted values:
//!
//! 1. Stop producing out of band (a flag, a cancellation token, joining the
//!    producer threads) and drop every sender, including clones.
//! 2. Keep receiving until `recv` returns `None` (or `try_recv` returns
//!    [`Closed`](mpsc::TryRecvError::Closed)).
//!
//! [`Receiver::close`](mpsc::Receiver::close),
//! [`ShardedReceiver::close`](mpsc::ShardedReceiver::close), and driver
//! shutdown (dropping a [`Driver`] or `Park::shutdown`) only refuse sends that
//! observe the close. They do not wait for sends already past their closed
//! check, so a racing send may return `Ok` after the receiver has reported
//! `Closed`. Such a value is returned by a later receive call or dropped
//! exactly once when the receiver is dropped; it is never leaked, and `Closed`
//! is not sticky.
//!
//! This differs from `tokio::sync::mpsc`, where `close()` followed by draining
//! to `None` is lossless: every send first takes a permit that `close()`
//! accounts for, so a send is either rejected or received before `None`.
//! Here, `close()` followed by draining to `None` is lossless only if no send
//! can race it, for example because all senders have already been dropped or
//! stopped. Do not use `close()` as a drain handshake with live producers.
//!
//! Dropping the receiver is separate: it discards buffered values, and a send
//! racing with the drop may return `Ok` while the channel drops the value
//! exactly once.
//!
//! # Application shape
//!
//! A sharded application can assemble its channels before starting any Norn
//! executors. Each detached receiver then moves to its disk scheduler thread
//! and attaches to that thread's driver. A scheduler can give each frontend
//! thread a separate ingress lane, avoiding shared producer state in the steady
//! state. Sending only touches thread-safe queue and unpark state; task wakers
//! remain on the scheduler's executor thread.
//!
//! ```no_run
//! use std::thread;
//!
//! use norn_channel::{mpsc, DriverBuilder};
//! use norn_executor::park::ThreadPark;
//! use norn_executor::LocalExecutor;
//!
//! #[derive(Debug)]
//! struct DiskRequest(u64);
//!
//! // Assemble the topology before any runtime thread starts.
//! let disk_driver = DriverBuilder::new();
//! let (frontend_txs, disk_rx) =
//!     mpsc::bounded_sharded::<DiskRequest>(disk_driver.endpoint(), 4_096, 4);
//! let disk = thread::spawn(move || {
//!     let driver = disk_driver.build(ThreadPark::default());
//!     let mut disk_rx = disk_rx.attach(&driver.handle());
//!     let mut disk_executor = LocalExecutor::new(driver);
//!
//!     disk_executor.block_on(async move {
//!         let mut batch = Vec::with_capacity(32);
//!         while disk_rx.recv_many(&mut batch, 32).await != 0 {
//!             for request in batch.drain(..) {
//!                 // Submit `request` to this shard's local I/O scheduler.
//!                 let _ = request.0;
//!             }
//!         }
//!     });
//! });
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
//! for frontend in frontends {
//!     frontend.join().unwrap();
//! }
//! disk.join().unwrap();
//! ```
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

mod driver;
mod metrics;
pub mod mpsc;

pub use driver::{Driver, DriverBuilder, Endpoint, Handle};
pub use metrics::{ChannelMetrics, ChannelSnapshot};
