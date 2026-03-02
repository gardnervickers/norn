//! Filesystem operations.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.

mod dir;
mod file;
mod opts;

pub use dir::{
    create_dir, hard_link, metadata, read_link, remove_dir, remove_file, rename, statx, symlink,
};
pub use file::{pipe, File, PipeReader, PipeWriter};
pub use opts::OpenOptions;
