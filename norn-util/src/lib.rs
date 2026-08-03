//! Utilities for composing and polling local Norn tasks.
//!
//! [`PollSet`] embeds a local task queue in another future. Polling the set
//! drains runnable child tasks; dropping it cancels any children that remain.
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

mod pollset;
pub use pollset::PollSet;
