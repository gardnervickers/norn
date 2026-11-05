//! Memory advice operations.
//!
//! [`advise`] submits `IORING_OP_MADVISE`, which requires Linux 5.6 or newer
//! and a kernel built with `CONFIG_ADVISE_SYSCALLS`. The kernel runs the
//! request on an `io_uring` worker thread, so a slow hint such as
//! [`Advice::WillNeed`] on a file mapping does not block the event loop.
//!
//! Only advice that cannot change memory contents, sharing, or process
//! semantics is exposed. Advice such as `MADV_DONTNEED`, `MADV_FREE`,
//! `MADV_REMOVE`, `MADV_DONTFORK`, and `MADV_WIPEONFORK` would let safe code
//! invalidate memory that Rust still references and is intentionally omitted.

use std::io;

use io_uring::opcode;

use crate::buf::StableBuf;
use crate::operation::{CQEResult, Operation, Singleshot};
use crate::Handle;

/// Access-pattern advice for a memory range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Advice {
    /// No special treatment (`MADV_NORMAL`).
    Normal,
    /// Expect page references in random order (`MADV_RANDOM`).
    Random,
    /// Expect page references in sequential order (`MADV_SEQUENTIAL`).
    Sequential,
    /// Expect access in the near future (`MADV_WILLNEED`).
    ///
    /// For file mappings this starts readahead of the backing pages.
    WillNeed,
}

impl Advice {
    fn as_raw(self) -> libc::c_int {
        match self {
            Self::Normal => libc::MADV_NORMAL,
            Self::Random => libc::MADV_RANDOM,
            Self::Sequential => libc::MADV_SEQUENTIAL,
            Self::WillNeed => libc::MADV_WILLNEED,
        }
    }
}

/// Advise the kernel about the expected use of a buffer's memory.
///
/// This is equivalent to `madvise(2)` applied to every page that lies entirely
/// within the buffer's initialized bytes. Partial pages at either end are not
/// advised, so a buffer smaller than one page completes successfully without
/// effect. Advice is most useful for page-aligned regions such as memory
/// mappings exposed through a custom [`StableBuf`] implementation.
///
/// The request owns the buffer until completion and returns it with the
/// result. The returned request is lazy and can be linked or given a timeout.
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if the advised range exceeds
/// `u32::MAX` bytes, `ENOMEM` if part of the range is not mapped, `EINVAL` or
/// `EOPNOTSUPP` if the kernel does not support the request, or another error
/// reported by `madvise(2)`.
///
/// # Panics
///
/// Panics if called outside an active [`Driver`](crate::Driver) context.
pub fn advise<B>(buf: B, advice: Advice) -> impl crate::Request<Output = (io::Result<()>, B)>
where
    B: StableBuf,
{
    Handle::current().submit(Madvise { buf, advice })
}

struct Madvise<B> {
    buf: B,
    advice: Advice,
}

// Safety: advice does not read or write the buffer's contents, and the owned
// buffer keeps the advised range allocated through the terminal CQE. The
// request owns no kernel resource after completion.
unsafe impl<B> Operation for Madvise<B>
where
    B: StableBuf,
{
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let (addr, len) = contained_pages(
            self.buf.stable_ptr() as usize,
            self.buf.bytes_init(),
            page_size(),
        );
        // The SQE length field is 32 bits wide.
        let len = u32::try_from(len).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "madvise range exceeds u32::MAX bytes",
            )
        })?;
        Ok(opcode::Madvise::new(
            addr as *const libc::c_void,
            libc::off_t::from(len),
            self.advice.as_raw(),
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result.into_result().map(drop)
    }
}

impl<B> Singleshot for Madvise<B>
where
    B: StableBuf,
{
    type Output = (io::Result<()>, B);

    fn complete(self, completion: Self::Completion) -> Self::Output {
        (completion, self.buf)
    }
}

fn page_size() -> usize {
    // Safety: `sysconf` has no memory-safety preconditions.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(size).expect("page size is positive")
}

/// Return the page-aligned start and length of the pages fully inside
/// `addr..addr + len`.
fn contained_pages(addr: usize, len: usize, page: usize) -> (usize, usize) {
    debug_assert!(page.is_power_of_two());
    let mask = page - 1;
    let end = addr.saturating_add(len) & !mask;
    match addr.checked_add(mask).map(|start| start & !mask) {
        Some(start) if start < end => (start, end - start),
        Some(start) => (start, 0),
        None => (end, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: usize = 4096;

    #[test]
    fn advises_only_whole_pages_inside_buffer() {
        assert_eq!(contained_pages(0x1000, 0x2000, PAGE), (0x1000, 0x2000));
        assert_eq!(contained_pages(0x1001, 0x2000, PAGE), (0x2000, 0x1000));
        assert_eq!(contained_pages(0x1000, 0x1fff, PAGE), (0x1000, 0x1000));
        assert_eq!(contained_pages(0x1001, 0x1fff, PAGE), (0x2000, 0x1000));
        assert_eq!(contained_pages(0x1001, 0x1ffe, PAGE), (0x2000, 0));
        assert_eq!(contained_pages(0x1010, 0x10, PAGE), (0x2000, 0));
        assert_eq!(contained_pages(0x1000, 0, PAGE), (0x1000, 0));
        assert_eq!(
            contained_pages(usize::MAX - 2, 2, PAGE),
            (usize::MAX & !0xfff, 0)
        );
    }

    #[test]
    fn configures_madvise_opcode() {
        let page = page_size();
        let storage = vec![0_u8; page * 3];
        let base = storage.as_ptr() as usize;
        let mut op = Madvise {
            buf: storage,
            advice: Advice::Sequential,
        };

        let entry = op.configure().unwrap();

        assert_eq!(entry.get_opcode(), opcode::Madvise::CODE.into());
        let bytes: [u8; 64] = unsafe { std::mem::transmute_copy(&entry) };
        let (start, len) = contained_pages(base, page * 3, page);
        assert_eq!(i32::from_ne_bytes(bytes[4..8].try_into().unwrap()), -1);
        assert_eq!(u64::from_ne_bytes(bytes[8..16].try_into().unwrap()), 0);
        assert_eq!(
            u64::from_ne_bytes(bytes[16..24].try_into().unwrap()),
            start as u64
        );
        assert_eq!(
            u32::from_ne_bytes(bytes[24..28].try_into().unwrap()),
            len as u32
        );
        assert_eq!(
            i32::from_ne_bytes(bytes[28..32].try_into().unwrap()),
            libc::MADV_SEQUENTIAL
        );
    }

    #[test]
    fn advice_maps_to_kernel_values() {
        assert_eq!(Advice::Normal.as_raw(), libc::MADV_NORMAL);
        assert_eq!(Advice::Random.as_raw(), libc::MADV_RANDOM);
        assert_eq!(Advice::Sequential.as_raw(), libc::MADV_SEQUENTIAL);
        assert_eq!(Advice::WillNeed.as_raw(), libc::MADV_WILLNEED);
    }
}
