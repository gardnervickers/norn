//! Support for io_uring fixed buffers used by `ReadFixed`/`WriteFixed`.
//!
//! Fixed buffers are especially useful with direct I/O (`O_DIRECT`) because
//! registration pins the memory range once instead of repeatedly pinning and
//! unpinning for each I/O. `O_DIRECT` is a performance option, not a default:
//! callers are responsible for alignment constraints and for avoiding mixed
//! buffered/direct I/O patterns on overlapping ranges.

use std::cell::Cell;
use std::fmt;
use std::io;
use std::ops::Range;
use std::rc::Rc;

use io_uring::Submitter;
use log::warn;

use crate::Handle;

/// Registry for fixed buffers registered with the current io_uring driver.
///
/// The registry owns the provided buffers and unregisters them when the last
/// clone is dropped. Buffers are leased via [`RegisteredBufSlice`].
#[derive(Clone)]
pub struct FixedBufRegistry {
    rc: Rc<Inner>,
}

impl fmt::Debug for FixedBufRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBufRegistry")
            .field("buffer_count", &self.rc.slots.len())
            .finish()
    }
}

/// A typed handle to a selected range of a registered fixed buffer.
///
/// This type is intentionally not `Clone`: only one active lease is allowed
/// per registered slot, and dropping this handle releases that slot.
pub struct RegisteredBufSlice {
    registry: FixedBufRegistry,
    slot: u16,
    offset: usize,
    len: usize,
    init_len: usize,
}

impl fmt::Debug for RegisteredBufSlice {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisteredBufSlice")
            .field("buf_index", &self.slot)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .field("init_len", &self.init_len)
            .finish()
    }
}

impl FixedBufRegistry {
    /// Register user-provided buffers as io_uring fixed buffers.
    ///
    /// All buffers must be non-empty. The maximum number of buffers is `u16::MAX`
    /// to match io_uring fixed-buffer index width.
    pub fn register(buffers: Vec<Vec<u8>>) -> io::Result<Self> {
        if buffers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer registry requires at least one buffer",
            ));
        }
        if buffers.len() > usize::from(u16::MAX) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer registry exceeds u16::MAX entries",
            ));
        }

        let mut slots = Vec::with_capacity(buffers.len());
        for (slot_id, buf) in buffers.into_iter().enumerate() {
            if buf.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("buffer at index {slot_id} is empty"),
                ));
            }
            slots.push(Slot {
                slot_id: slot_id as u16,
                buf,
                leased: Cell::new(false),
            });
        }

        let mut iovecs = Vec::with_capacity(slots.len());
        for slot in &mut slots {
            iovecs.push(libc::iovec {
                iov_base: slot.buf.as_mut_ptr() as *mut libc::c_void,
                iov_len: slot.buf.len(),
            });
        }

        let handle = Handle::current();
        if let Err(err) = handle.with_submitter(|submitter| register_buffers(submitter, &iovecs)) {
            return Err(map_register_error(err));
        }

        Ok(Self {
            rc: Rc::new(Inner { handle, slots }),
        })
    }

    /// Number of registered buffers.
    pub fn len(&self) -> usize {
        self.rc.slots.len()
    }

    /// Returns `true` if no buffers are registered.
    pub fn is_empty(&self) -> bool {
        self.rc.slots.is_empty()
    }

    /// Lease a typed slice of a registered fixed buffer.
    ///
    /// Returns `InvalidInput` if the index/range is invalid and `WouldBlock`
    /// when the indexed buffer is already leased.
    pub fn slice(&self, index: usize, range: Range<usize>) -> io::Result<RegisteredBufSlice> {
        let slot = self.rc.slots.get(index).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("fixed buffer index {index} is out of bounds"),
            )
        })?;
        let len = slot.buf.len();
        if range.start > range.end || range.end > len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "fixed buffer range {}..{} is out of bounds for buffer length {}",
                    range.start, range.end, len
                ),
            ));
        }
        let selected = range.end - range.start;
        if selected == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer range must not be empty",
            ));
        }
        if selected > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "fixed buffer range exceeds u32::MAX",
            ));
        }
        if slot.leased.replace(true) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                format!("fixed buffer index {index} is currently leased"),
            ));
        }

        Ok(RegisteredBufSlice {
            registry: self.clone(),
            slot: slot.slot_id,
            offset: range.start,
            len: selected,
            init_len: selected,
        })
    }

    fn slot(&self, slot: u16) -> &Slot {
        &self.rc.slots[usize::from(slot)]
    }

    fn release_slot(&self, slot: u16) {
        self.slot(slot).leased.set(false);
    }
}

impl RegisteredBufSlice {
    /// Returns the fixed buffer index used for io_uring `buf_index`.
    pub fn buf_index(&self) -> u16 {
        self.slot
    }

    /// Returns the selected byte range within the registered buffer.
    pub fn range(&self) -> Range<usize> {
        self.offset..self.offset + self.len
    }

    /// Returns the selected capacity in bytes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` when the selected range length is zero.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns initialized length tracked by this handle.
    pub fn bytes_init(&self) -> usize {
        self.init_len
    }

    /// Set initialized length tracked by this handle.
    ///
    /// This does not alter registration metadata; it only updates the logical
    /// initialized window exposed by [`RegisteredBufSlice::as_slice`].
    pub fn set_init(&mut self, init_len: usize) -> io::Result<()> {
        if init_len > self.len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "initialized length {} exceeds selected range length {}",
                    init_len, self.len
                ),
            ));
        }
        self.init_len = init_len;
        Ok(())
    }

    /// Returns initialized bytes as an immutable slice.
    pub fn as_slice(&self) -> &[u8] {
        let ptr = self.ptr();
        unsafe { std::slice::from_raw_parts(ptr, self.init_len) }
    }

    /// Returns the full selected range as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let ptr = self.ptr_mut();
        unsafe { std::slice::from_raw_parts_mut(ptr, self.len) }
    }

    pub(crate) fn clear_init(&mut self) {
        self.init_len = 0;
    }

    pub(crate) fn set_init_after_read(&mut self, init_len: usize) {
        debug_assert!(init_len <= self.len);
        self.init_len = init_len.min(self.len);
    }

    pub(crate) fn ptr(&self) -> *const u8 {
        let base = self.registry.slot(self.slot).buf.as_ptr();
        unsafe { base.add(self.offset) }
    }

    pub(crate) fn ptr_mut(&mut self) -> *mut u8 {
        self.ptr() as *mut u8
    }

    pub(crate) fn len_u32(&self) -> u32 {
        self.len as u32
    }
}

impl Drop for RegisteredBufSlice {
    fn drop(&mut self) {
        self.registry.release_slot(self.slot);
    }
}

struct Slot {
    // Slot id is kept explicitly so sparse/update APIs can evolve without
    // changing external handle layout.
    slot_id: u16,
    buf: Vec<u8>,
    leased: Cell<bool>,
}

struct Inner {
    handle: Handle,
    slots: Vec<Slot>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Err(err) = self
            .handle
            .with_submitter(|submitter| submitter.unregister_buffers())
        {
            warn!(target: "norn_uring::fixedbuf", "unregister.failed: {}", err);
        }
    }
}

fn register_buffers(submitter: &Submitter<'_>, iovecs: &[libc::iovec]) -> io::Result<()> {
    unsafe { submitter.register_buffers(iovecs) }
}

fn map_register_error(err: io::Error) -> io::Error {
    match err.raw_os_error() {
        Some(errno)
            if errno == libc::ENOSYS || errno == libc::EOPNOTSUPP || errno == libc::ENOTSUP =>
        {
            io::Error::new(
                io::ErrorKind::Unsupported,
                format!("fixed buffer registration is unsupported: {err}"),
            )
        }
        _ => err,
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::io;

    use norn_executor::LocalExecutor;

    use super::FixedBufRegistry;

    fn with_test_env<T, F>(f: impl FnOnce() -> F) -> io::Result<T>
    where
        F: Future<Output = io::Result<T>>,
    {
        let builder = io_uring::IoUring::builder();
        let driver = crate::Driver::new(builder, 32)?;
        let mut ex = LocalExecutor::new(driver);
        ex.block_on(f())
    }

    #[test]
    fn range_validation_and_index_bounds() -> io::Result<()> {
        with_test_env(|| async {
            let registry = FixedBufRegistry::register(vec![vec![0u8; 16]])?;

            let err = registry.slice(2, 0..1).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

            let err = registry.slice(0, 4..18).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

            let err = registry.slice(0, 8..8).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

            let slice = registry.slice(0, 4..8)?;
            assert_eq!(slice.buf_index(), 0);
            assert_eq!(slice.range(), 4..8);
            Ok(())
        })
    }

    #[test]
    fn lease_lifecycle_releases_slot() -> io::Result<()> {
        with_test_env(|| async {
            let registry = FixedBufRegistry::register(vec![vec![0u8; 32]])?;

            let lease = registry.slice(0, 0..8)?;
            let err = registry.slice(0, 0..8).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

            drop(lease);

            let _lease = registry.slice(0, 0..8)?;
            Ok(())
        })
    }
}
