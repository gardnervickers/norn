//! An in-memory Memcached-binary server used to exercise Norn networking.
//!
//! Storage remains synchronous and local so the benchmark isolates TCP,
//! framing, scheduling, and response construction.
#![deny(
    missing_docs,
    rust_2018_idioms,
    rustdoc::bare_urls,
    rustdoc::broken_intra_doc_links,
    unreachable_pub,
    clippy::doc_markdown,
    clippy::missing_errors_doc,
    clippy::missing_safety_doc
)]

pub mod codec;
pub mod handler;
pub mod memory;
pub mod protocol;

#[cfg(target_os = "linux")]
pub mod server;
#[cfg(target_os = "linux")]
pub mod sharded;
