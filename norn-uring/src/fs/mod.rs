//! Filesystem operations.
//!
//! Unless otherwise documented, functions and methods in this module require
//! an active [`crate::Driver`] context.

mod dir;
mod file;
mod opts;

use std::io;

use crate::buf::{set_init_checked, StableBufMut};

pub use dir::{
    create_dir, get_xattr, hard_link, metadata, remove_dir, remove_file, rename, set_xattr, statx,
    symlink,
};
pub use file::{pipe, File, PipeReader, PipeWriter};
pub use opts::OpenOptions;

fn complete_get_xattr<B>(buf: &mut B, submitted_len: u32, result: u32) -> io::Result<usize>
where
    B: StableBufMut,
{
    let result = result as usize;
    let submitted_len = submitted_len as usize;

    // A zero-length getxattr call is a size query. The kernel reports the
    // required size without writing any bytes into the destination.
    if submitted_len == 0 {
        return Ok(result);
    }

    set_init_checked(buf, submitted_len, result, "getxattr")?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::complete_get_xattr;

    #[test]
    fn getxattr_size_query_does_not_initialize_bytes() {
        let mut buf = Vec::new();
        assert_eq!(complete_get_xattr(&mut buf, 0, 8).unwrap(), 8);
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 0);
    }

    #[test]
    fn getxattr_rejects_oversized_completion() {
        let mut buf = Vec::with_capacity(1);
        let err = complete_get_xattr(&mut buf, 1, 2).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(buf.is_empty());
    }
}
