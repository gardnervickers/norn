//! Commands for Linux block devices.

use std::io;

use crate::operation::CQEResult;
use crate::uring_cmd::{Command16, UringCommand16};

// `_IO(0x12, 0)` from `include/uapi/linux/blkdev.h`.
const BLOCK_URING_CMD_DISCARD: u32 = 0x1200;

/// Discard a byte range on a block device.
///
/// The kernel may execute multiple discard commands concurrently with each
/// other and with ordinary I/O. Callers must ensure that no reads or writes
/// overlap the range until the command completes. Page-cache invalidation is
/// best effort, so the same exclusion must hold for buffered I/O.
///
/// This command requires Linux 6.12 or newer and a block device that supports
/// discard.
///
/// # Example
///
/// ```no_run
/// use norn_uring::block::Discard;
/// use norn_uring::fs::File;
///
/// # async fn discard(file: &File) -> std::io::Result<()> {
/// file.as_uring_fd()
///     .submit_command(Discard::new(0, 1024 * 1024))
///     .await
/// # }
/// ```
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Discard {
    offset: u64,
    length: u64,
}

impl Discard {
    /// Create a discard command for `length` bytes starting at `offset`.
    pub const fn new(offset: u64, length: u64) -> Self {
        Self { offset, length }
    }

    /// Return the starting byte offset.
    pub const fn offset(&self) -> u64 {
        self.offset
    }

    /// Return the number of bytes to discard.
    pub const fn length(&self) -> u64 {
        self.length
    }
}

// Safety: discard encodes only two copied integers, references no userspace
// memory, and produces one terminal completion.
unsafe impl UringCommand16 for Discard {
    type Output = io::Result<()>;

    fn encode(&mut self) -> io::Result<Command16> {
        let mut data = [0; 16];
        data[..8].copy_from_slice(&self.length.to_ne_bytes());
        Ok(Command16::new(BLOCK_URING_CMD_DISCARD, data).with_address(self.offset))
    }

    fn complete(self, result: CQEResult) -> Self::Output {
        result.into_result().map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn discard_encodes_byte_range() {
        let mut discard = Discard::new(0x0102_0304_0506_0708, 0x1112_1314_1516_1718);

        let command = discard.encode().unwrap();

        assert_eq!(command.command_op(), BLOCK_URING_CMD_DISCARD);
        assert_eq!(command.address(), Some(0x0102_0304_0506_0708));
        assert_eq!(
            &command.data()[..8],
            &0x1112_1314_1516_1718_u64.to_ne_bytes()
        );
        assert_eq!(&command.data()[8..], &[0; 8]);
    }
}
