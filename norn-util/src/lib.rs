//! Utilities for composing and polling local Norn tasks.
//!
//! # Modules
//! - [`PollSet`]: scoped local task orchestration helper.
#![deny(missing_docs, rust_2018_idioms, clippy::missing_safety_doc)]

mod pollset;
pub use pollset::PollSet;
