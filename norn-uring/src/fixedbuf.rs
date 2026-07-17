//! Owned fixed buffers registered with an io_uring driver.
//!
//! A [`FixedBufPool`](crate::fixedbuf::FixedBufPool) owns a dense immutable
//! table of caller-provided buffers. Each registered slot can be acquired as
//! one non-cloneable [`FixedBuf`](crate::fixedbuf::FixedBuf) which fixed I/O
//! operations consume and return.
//!
//! `Vec<u8>` and `BytesMut` expose only their initialized lengths. Resize them
//! before registration; `Vec::with_capacity(N)` and
//! `BytesMut::with_capacity(N)` expose empty regions and are rejected.
//!
//! ```no_run
//! use norn_uring::{fs::File, Handle};
//!
//! # async fn example(file: &File) -> Result<(), Box<dyn std::error::Error>> {
//! let pool = Handle::current().register_fixed_buffers(vec![vec![0u8; 4096]])?;
//! let buf = pool.try_acquire()?;
//!
//! let (result, mut buf) = file.read_fixed_at(buf, 0).await;
//! let read = result?;
//! consume(&buf[..read]);
//!
//! buf.set_payload(b"hello")?;
//! let mut offset = 4096;
//! while !buf.is_empty() {
//!     let (result, returned) = file.write_fixed_at(buf, offset).await;
//!     let written = result?;
//!     buf = returned;
//!     if written == 0 {
//!         return Err(std::io::Error::from(std::io::ErrorKind::WriteZero).into());
//!     }
//!     buf.consume(written);
//!     offset += written as u64;
//! }
//! drop(buf);
//!
//! let original = pool.unregister()?;
//! assert_eq!(original.len(), 1);
//! # Ok(())
//! # }
//! # fn consume(_: &[u8]) {}
//! ```

use std::any::Any;
use std::cell::{Cell, UnsafeCell};
use std::error::Error;
use std::fmt;
use std::io;
use std::ops::{Deref, DerefMut, Range};
use std::ptr::NonNull;
use std::rc::Rc;

use crate::buf::{StableBuf, StableBufMut};
use crate::driver::{
    FixedBufDriver, FixedBufGeneration, FixedBufRelease, FixedBufReleaseError, ReserveFixedBufError,
};
use crate::Handle;

pub use crate::buf::FixedBuffer;

const MAX_FIXED_BUFFERS: usize = 1 << 14;

/// An owned dense table of buffers registered with one io_uring driver.
///
/// The pool is not cloneable. Borrow it to acquire multiple slots, and call
/// [`FixedBufPool::unregister`] to recover the original buffers.
///
/// Local tasks can share acquisition through `Rc<FixedBufPool<_>>` and recover
/// the pool with `Rc::try_unwrap` before unregistering. Alternatively, acquire
/// buffers up front and move each [`FixedBuf`] into its task.
///
/// Dropping the pool performs safe best-effort release. Use explicit
/// [`FixedBufPool::unregister`] when release errors or ownership recovery matter.
pub struct FixedBufPool<B: 'static> {
    inner: Rc<Inner<B>>,
}

/// Exclusive ownership of one slot in a [`FixedBufPool`].
///
/// This type is not cloneable. Dropping it returns its whole slot to the pool.
/// The underlying `B` remains inaccessible while registered; keep application
/// metadata externally and associate it with [`FixedBuf::index`].
pub struct FixedBuf<B: 'static> {
    inner: Rc<Inner<B>>,
    ptr: NonNull<u8>,
    range_start: u32,
    range_len: u32,
    len: u32,
    index: u16,
}

struct Inner<B: 'static> {
    driver: FixedBufDriver,
    generation: FixedBufGeneration,
    registered: Cell<bool>,
    storage: Option<Box<Storage<B>>>,
    slots: Box<[SlotMeta]>,
    free_head: Cell<Option<usize>>,
}

struct Storage<B> {
    buffers: Box<[UnsafeCell<B>]>,
}

struct SlotMeta {
    ptr: NonNull<u8>,
    len: u32,
    acquired: Cell<bool>,
    next_free: Cell<Option<usize>>,
}

/// Why fixed-buffer registration failed.
#[derive(Debug)]
pub enum RegisterErrorKind {
    /// No buffers were supplied.
    EmptyPool,
    /// One buffer exposed an empty registered region.
    EmptyBuffer {
        /// Index of the empty buffer.
        index: usize,
    },
    /// The dense table exceeds the kernel's fixed-buffer limit.
    TooManyBuffers {
        /// Number of buffers supplied.
        count: usize,
        /// Maximum supported count.
        max: usize,
    },
    /// A registered region cannot be represented by fixed I/O SQEs.
    BufferTooLarge {
        /// Index of the oversized buffer.
        index: usize,
        /// Length exposed by the buffer.
        len: usize,
        /// Maximum representable length.
        max: usize,
    },
    /// Two caller-provided values selected overlapping memory regions.
    OverlappingBuffers {
        /// Index of the earlier-addressed overlapping region.
        first: usize,
        /// Index of the other overlapping region.
        second: usize,
    },
    /// This ring already has a fixed-buffer registration in progress or active.
    AlreadyRegistered,
    /// The driver is no longer accepting registrations.
    DriverStopped,
    /// The driver's registration generation counter was exhausted.
    GenerationExhausted,
    /// The running kernel does not support this registration operation.
    Unsupported(io::Error),
    /// The kernel could not reserve the required resources.
    ResourceExhausted(io::Error),
    /// Another operating-system error occurred.
    Io(io::Error),
}

impl fmt::Display for RegisterErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPool => f.write_str("fixed-buffer pool must not be empty"),
            Self::EmptyBuffer { index } => {
                write!(f, "fixed buffer at index {index} exposes an empty region")
            }
            Self::TooManyBuffers { count, max } => write!(
                f,
                "fixed-buffer pool contains {count} buffers, exceeding the limit of {max}"
            ),
            Self::BufferTooLarge { index, len, max } => write!(
                f,
                "fixed buffer at index {index} is {len} bytes, exceeding the limit of {max}"
            ),
            Self::OverlappingBuffers { first, second } => write!(
                f,
                "fixed buffers at indices {first} and {second} expose overlapping regions"
            ),
            Self::AlreadyRegistered => f.write_str("this driver already has a fixed-buffer table"),
            Self::DriverStopped => f.write_str("the driver is not running"),
            Self::GenerationExhausted => {
                f.write_str("the fixed-buffer registration generation was exhausted")
            }
            Self::Unsupported(err) => write!(f, "fixed buffers are unsupported: {err}"),
            Self::ResourceExhausted(err) => write!(
                f,
                "fixed-buffer resources are exhausted (check locked-memory limits): {err}"
            ),
            Self::Io(err) => write!(f, "fixed-buffer registration failed: {err}"),
        }
    }
}

impl Error for RegisterErrorKind {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unsupported(err) | Self::ResourceExhausted(err) | Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// A registration failure which retains the original buffers.
pub struct RegisterError<B> {
    kind: RegisterErrorKind,
    buffers: Vec<B>,
}

impl<B> RegisterError<B> {
    /// Return the reason registration failed.
    pub fn kind(&self) -> &RegisterErrorKind {
        &self.kind
    }

    /// Recover the original buffers in their input order.
    pub fn into_buffers(self) -> Vec<B> {
        self.buffers
    }

    /// Split this error into its reason and original buffers.
    pub fn into_parts(self) -> (RegisterErrorKind, Vec<B>) {
        (self.kind, self.buffers)
    }
}

impl<B> fmt::Debug for RegisterError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegisterError")
            .field("kind", &self.kind)
            .field("buffer_count", &self.buffers.len())
            .finish()
    }
}

impl<B> fmt::Display for RegisterError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl<B> Error for RegisterError<B> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

/// Failure to acquire a registered slot.
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
pub enum AcquireError {
    /// No registered slot is currently free.
    #[error("no fixed buffer is currently available")]
    Exhausted,
    /// The requested table index does not exist.
    #[error("fixed-buffer index {index} is out of bounds for a pool of length {len}")]
    InvalidIndex {
        /// Requested index.
        index: usize,
        /// Number of registered slots.
        len: usize,
    },
    /// The requested table index is already acquired.
    #[error("fixed-buffer index {index} is already acquired")]
    InUse {
        /// Requested index.
        index: usize,
    },
}

/// An invalid view range for a [`FixedBuf`].
#[derive(Debug, thiserror::Error, Clone, PartialEq, Eq)]
#[error("fixed-buffer range {start}..{end} is invalid for a region of length {capacity}")]
pub struct RangeError {
    start: usize,
    end: usize,
    capacity: usize,
}

impl RangeError {
    /// Return the rejected range.
    pub fn range(&self) -> Range<usize> {
        self.start..self.end
    }

    /// Return the full registered-region capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// An invalid logical payload length for a [`FixedBuf`].
#[derive(Debug, thiserror::Error, Clone, Copy, PartialEq, Eq)]
#[error("fixed-buffer payload length {len} exceeds the selected capacity {capacity}")]
pub struct LengthError {
    len: usize,
    capacity: usize,
}

impl LengthError {
    /// Return the rejected payload length.
    pub fn requested(&self) -> usize {
        self.len
    }

    /// Return the selected view's capacity.
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

/// Why explicit fixed-buffer unregistration failed.
#[derive(Debug)]
pub enum UnregisterErrorKind {
    /// One or more [`FixedBuf`] values still own pool slots.
    Busy {
        /// Number of acquired slots, including buffers owned by operations.
        acquired: usize,
    },
    /// The driver's active table did not match this pool's generation.
    StateMismatch,
    /// The driver was temporarily borrowed by another operation.
    DriverBorrowed,
    /// The kernel rejected unregistration.
    Io(io::Error),
}

impl fmt::Display for UnregisterErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Busy { acquired } => {
                write!(f, "{acquired} fixed buffer(s) are still acquired")
            }
            Self::StateMismatch => {
                f.write_str("the driver's fixed-buffer generation does not match this pool")
            }
            Self::DriverBorrowed => {
                f.write_str("the io_uring driver is currently borrowed; retry unregistration")
            }
            Self::Io(err) => write!(f, "fixed-buffer unregistration failed: {err}"),
        }
    }
}

impl Error for UnregisterErrorKind {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// An unregistration failure which retains the intact pool.
pub struct UnregisterError<B: 'static> {
    kind: UnregisterErrorKind,
    pool: FixedBufPool<B>,
}

impl<B: 'static> UnregisterError<B> {
    /// Return the reason unregistration failed.
    pub fn kind(&self) -> &UnregisterErrorKind {
        &self.kind
    }

    /// Recover the intact pool so unregistration can be retried.
    pub fn into_pool(self) -> FixedBufPool<B> {
        self.pool
    }

    /// Split this error into its reason and intact pool.
    pub fn into_parts(self) -> (UnregisterErrorKind, FixedBufPool<B>) {
        (self.kind, self.pool)
    }
}

impl<B: 'static> fmt::Debug for UnregisterError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnregisterError")
            .field("kind", &self.kind)
            .field("pool", &self.pool)
            .finish()
    }
}

impl<B: 'static> fmt::Display for UnregisterError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl<B: 'static> Error for UnregisterError<B> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

impl Handle {
    /// Register a caller-owned dense table of fixed buffers with this driver.
    ///
    /// On failure, the returned [`RegisterError`] retains every input buffer.
    /// `Vec<u8>` and `BytesMut` values must be resized to their intended
    /// registered lengths before this call; spare capacity is not registered.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] for invalid regions, a stopped or already
    /// registered driver, unsupported kernels, or OS resource failures.
    pub fn register_fixed_buffers<B>(
        &self,
        buffers: Vec<B>,
    ) -> Result<FixedBufPool<B>, RegisterError<B>>
    where
        B: FixedBuffer + 'static,
    {
        // Safety: `FixedBuffer` implementations provide the long-lived region
        // guarantees required by `register_fixed_buffers_with`.
        unsafe { self.register_fixed_buffers_with(buffers, FixedBuffer::fixed_region) }
    }

    /// Register buffers using a caller-provided region projection.
    ///
    /// This is useful for third-party types which cannot implement
    /// [`FixedBuffer`] because of Rust's orphan rules. The projection is called
    /// at most once per value, in input order, after all values have reached
    /// final storage. On success it has been called once for every value.
    ///
    /// Projection is not a pinning event. If registration never becomes
    /// active, cached pointers are discarded and the values may move while
    /// being returned in [`RegisterError`].
    ///
    /// # Safety
    ///
    /// Every returned slice must obey the complete [`FixedBuffer`] safety
    /// contract until the returned pool is successfully unregistered or its
    /// io_uring is destroyed. Regions selected from different values must not
    /// overlap; registration also checks and rejects overlapping intervals.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] under the same conditions as
    /// [`Handle::register_fixed_buffers`], retaining every input value. This
    /// includes regions which overlap another selected region in the pool.
    pub unsafe fn register_fixed_buffers_with<B, F>(
        &self,
        buffers: Vec<B>,
        mut region: F,
    ) -> Result<FixedBufPool<B>, RegisterError<B>>
    where
        B: 'static,
        F: for<'a> FnMut(&'a mut B) -> &'a mut [u8],
    {
        if buffers.is_empty() {
            return Err(RegisterError {
                kind: RegisterErrorKind::EmptyPool,
                buffers,
            });
        }
        if buffers.len() > MAX_FIXED_BUFFERS {
            return Err(RegisterError {
                kind: RegisterErrorKind::TooManyBuffers {
                    count: buffers.len(),
                    max: MAX_FIXED_BUFFERS,
                },
                buffers,
            });
        }

        // Claim the driver's single dense table before allocating metadata or
        // invoking user projection code. The reservation guard rolls this back
        // on every validation error or unwind before commit.
        let reservation = match self.reserve_fixed_buffers() {
            Ok(reservation) => reservation,
            Err(err) => {
                return Err(RegisterError {
                    kind: map_reserve_error(err),
                    buffers,
                });
            }
        };

        let cells = buffers
            .into_iter()
            .map(UnsafeCell::new)
            .collect::<Vec<_>>()
            .into_boxed_slice();
        let mut storage = Box::new(Storage { buffers: cells });
        let mut slots = Vec::with_capacity(storage.buffers.len());
        let mut iovecs = Vec::with_capacity(storage.buffers.len());
        let mut intervals = Vec::with_capacity(storage.buffers.len());

        let buffer_count = storage.buffers.len();
        for (index, cell) in storage.buffers.iter_mut().enumerate() {
            let selected = region(cell.get_mut());
            if selected.is_empty() {
                return Err(RegisterError {
                    kind: RegisterErrorKind::EmptyBuffer { index },
                    buffers: (*storage).into_buffers(),
                });
            }
            if selected.len() > u32::MAX as usize {
                return Err(RegisterError {
                    kind: RegisterErrorKind::BufferTooLarge {
                        index,
                        len: selected.len(),
                        max: u32::MAX as usize,
                    },
                    buffers: (*storage).into_buffers(),
                });
            }

            let ptr = NonNull::from(&mut selected[0]);
            let start = ptr.as_ptr() as usize;
            let end = start
                .checked_add(selected.len())
                .expect("FixedBuffer returned an address range which wraps usize");
            intervals.push((start, end, index));
            slots.push(SlotMeta {
                ptr,
                len: selected.len() as u32,
                acquired: Cell::new(false),
                next_free: Cell::new((index + 1 < buffer_count).then_some(index + 1)),
            });
            iovecs.push(libc::iovec {
                iov_base: ptr.as_ptr().cast(),
                iov_len: selected.len(),
            });
        }

        intervals.sort_unstable_by_key(|&(start, _, _)| start);
        if let Some(pair) = intervals.windows(2).find(|pair| pair[1].0 < pair[0].1) {
            let first = pair[0].2.min(pair[1].2);
            let second = pair[0].2.max(pair[1].2);
            return Err(RegisterError {
                kind: RegisterErrorKind::OverlappingBuffers { first, second },
                buffers: (*storage).into_buffers(),
            });
        }

        let generation = reservation.generation();
        let inner = Rc::new(Inner {
            driver: self.fixed_buf_driver(),
            generation,
            registered: Cell::new(false),
            storage: Some(storage),
            slots: slots.into_boxed_slice(),
            free_head: Cell::new(Some(0)),
        });

        // Arm conservative retention before entering the syscall wrapper. If
        // that wrapper ever unwinds after the kernel accepts registration, the
        // backing storage cannot be freed.
        inner.registered.set(true);
        reservation.arm_kernel_call();
        let register = reservation.with_submitter(|submitter| {
            // Safety: the iovecs point into `storage`, which has reached its
            // final allocation and is retained by `inner` from this point.
            unsafe { submitter.register_buffers(&iovecs) }
        });
        if let Err(err) = register {
            inner.registered.set(false);
            reservation.kernel_call_failed();
            drop(reservation);
            let inner = unwrap_unregistered(inner);
            return Err(RegisterError {
                kind: map_register_io_error(err),
                buffers: inner.into_buffers(),
            });
        }

        let _ = reservation.commit();

        Ok(FixedBufPool { inner })
    }
}

impl<B: 'static> FixedBufPool<B> {
    /// Acquire any currently free registered slot.
    ///
    /// # Errors
    ///
    /// Returns [`AcquireError::Exhausted`] when every slot is acquired.
    pub fn try_acquire(&self) -> Result<FixedBuf<B>, AcquireError> {
        let inner = Rc::clone(&self.inner);
        let index = inner.free_head.get().ok_or(AcquireError::Exhausted)?;
        let slot = &inner.slots[index];
        inner.free_head.set(slot.next_free.get());
        slot.next_free.set(None);
        assert!(!slot.acquired.replace(true), "free-list slot was acquired");
        let ptr = slot.ptr;
        let len = slot.len;
        Ok(FixedBuf::new(inner, index as u16, ptr, len))
    }

    /// Acquire a specific registered slot by kernel-table index.
    ///
    /// This searches the free list and is O(pool length). Prefer
    /// [`FixedBufPool::try_acquire`] on request hot paths.
    ///
    /// # Errors
    ///
    /// Returns [`AcquireError::InvalidIndex`] for an unknown index or
    /// [`AcquireError::InUse`] when that slot is already acquired.
    pub fn try_acquire_at(&self, index: usize) -> Result<FixedBuf<B>, AcquireError> {
        let Some(slot) = self.inner.slots.get(index) else {
            return Err(AcquireError::InvalidIndex {
                index,
                len: self.inner.slots.len(),
            });
        };
        if slot.acquired.get() {
            return Err(AcquireError::InUse { index });
        }

        let inner = Rc::clone(&self.inner);
        let next = slot.next_free.get();
        if inner.free_head.get() == Some(index) {
            inner.free_head.set(next);
        } else {
            let mut cursor = inner
                .free_head
                .get()
                .expect("unacquired fixed-buffer slot missing from free list");
            loop {
                let cursor_slot = &inner.slots[cursor];
                if cursor_slot.next_free.get() == Some(index) {
                    cursor_slot.next_free.set(next);
                    break;
                }
                cursor = cursor_slot
                    .next_free
                    .get()
                    .expect("unacquired fixed-buffer slot missing from free list");
            }
        }
        slot.next_free.set(None);
        slot.acquired.set(true);
        let ptr = slot.ptr;
        let len = slot.len;
        Ok(FixedBuf::new(inner, index as u16, ptr, len))
    }

    /// Unregister the table and recover the original buffers in input order.
    ///
    /// # Errors
    ///
    /// [`UnregisterErrorKind::Busy`] is retryable after dropping the reported
    /// acquired buffers or operations. [`UnregisterErrorKind::DriverBorrowed`]
    /// is transient and can be retried later. OS errors retain the intact pool
    /// for caller-directed retry. A state mismatch is an invariant failure;
    /// dropping the returned pool conservatively retains its storage until the
    /// ring is destroyed.
    pub fn unregister(self) -> Result<Vec<B>, UnregisterError<B>> {
        let inner = match Rc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(inner) => {
                let acquired = Rc::strong_count(&inner).saturating_sub(1);
                return Err(UnregisterError {
                    kind: UnregisterErrorKind::Busy { acquired },
                    pool: Self { inner },
                });
            }
        };
        let inner = inner;

        match inner.driver.unregister(inner.generation) {
            Ok(FixedBufRelease::Unregistered | FixedBufRelease::RingGone) => {
                inner.registered.set(false);
                Ok(inner.into_buffers())
            }
            Err(err) => {
                let kind = map_release_error(err);
                Err(UnregisterError {
                    kind,
                    pool: Self {
                        inner: Rc::new(inner),
                    },
                })
            }
        }
    }

    /// Return the number of registered slots.
    pub fn len(&self) -> usize {
        self.inner.slots.len()
    }

    /// Return whether this pool contains no slots.
    pub fn is_empty(&self) -> bool {
        self.inner.slots.is_empty()
    }
}

impl<B: 'static> FixedBuf<B> {
    fn new(inner: Rc<Inner<B>>, index: u16, ptr: NonNull<u8>, registered_len: u32) -> Self {
        Self {
            inner,
            ptr,
            index,
            range_start: 0,
            range_len: registered_len,
            len: registered_len,
        }
    }

    /// Select a non-empty range relative to the whole registered slot.
    ///
    /// Selecting a range clears the logical payload length.
    ///
    /// # Errors
    ///
    /// Returns [`RangeError`] for an empty, reversed, or out-of-bounds range.
    pub fn set_range(&mut self, range: Range<usize>) -> Result<(), RangeError> {
        let registered_len = self.registered_capacity();
        if range.start >= range.end || range.end > registered_len {
            return Err(RangeError {
                start: range.start,
                end: range.end,
                capacity: registered_len,
            });
        }
        self.range_start = range.start as u32;
        self.range_len = (range.end - range.start) as u32;
        self.len = 0;
        Ok(())
    }

    /// Select the whole registered slot and clear the logical payload length.
    pub fn reset_range(&mut self) {
        self.range_start = 0;
        self.range_len = self.registered_capacity() as u32;
        self.len = 0;
    }

    /// Return the selected range relative to the whole registered slot.
    pub fn range(&self) -> Range<usize> {
        let start = self.range_start as usize;
        start..start + self.range_len as usize
    }

    /// Return the logical payload length used by fixed writes.
    pub fn len(&self) -> usize {
        self.len as usize
    }

    /// Return whether the logical payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the selected capacity used by fixed reads.
    pub fn capacity(&self) -> usize {
        self.range_len as usize
    }

    /// Clear the logical payload without changing the selected range.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Set the logical payload length used by fixed writes.
    ///
    /// This changes metadata only. It does not initialize, clear, or overwrite
    /// bytes; newly exposed payload bytes retain their previous contents.
    ///
    /// # Errors
    ///
    /// Returns [`LengthError`] when `len` exceeds the selected capacity.
    pub fn set_len(&mut self, len: usize) -> Result<(), LengthError> {
        if len > self.range_len as usize {
            return Err(LengthError {
                len,
                capacity: self.range_len as usize,
            });
        }
        self.len = len as u32;
        Ok(())
    }

    /// Copy a payload into the selected region and set its logical length.
    ///
    /// # Errors
    ///
    /// Returns [`LengthError`] without modifying the buffer when `payload` is
    /// larger than the selected capacity.
    pub fn set_payload(&mut self, payload: &[u8]) -> Result<(), LengthError> {
        if payload.len() > self.range_len as usize {
            return Err(LengthError {
                len: payload.len(),
                capacity: self.range_len as usize,
            });
        }
        self.as_full_slice_mut()[..payload.len()].copy_from_slice(payload);
        self.len = payload.len() as u32;
        Ok(())
    }

    /// Advance past up to `n` logical payload bytes.
    ///
    /// This saturates at the logical payload length, matching
    /// [`crate::buf::BufCursor::consume`]. The selected range start advances,
    /// while both selected capacity and logical length shrink by the consumed
    /// amount. It is intended for retrying short fixed writes.
    pub fn consume(&mut self, n: usize) {
        let consumed = n.min(self.len as usize) as u32;
        self.range_start += consumed;
        self.range_len -= consumed;
        self.len -= consumed;
    }

    /// Return the logical payload.
    pub fn as_slice(&self) -> &[u8] {
        // Safety: the pool retains the initialized registered storage and this
        // exclusive FixedBuf owns the whole slot.
        unsafe { std::slice::from_raw_parts(self.view_ptr(), self.len as usize) }
    }

    /// Return the logical payload mutably.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: this non-cloneable FixedBuf is the only owner of the slot.
        unsafe { std::slice::from_raw_parts_mut(self.view_ptr_mut(), self.len as usize) }
    }

    /// Return the entire selected initialized region.
    pub fn as_full_slice(&self) -> &[u8] {
        // Safety: the complete v1 region is initialized by contract.
        unsafe { std::slice::from_raw_parts(self.view_ptr(), self.range_len as usize) }
    }

    /// Return the entire selected initialized region mutably.
    pub fn as_full_slice_mut(&mut self) -> &mut [u8] {
        // Safety: this non-cloneable FixedBuf is the only owner of the slot.
        unsafe { std::slice::from_raw_parts_mut(self.view_ptr_mut(), self.range_len as usize) }
    }

    /// Return this buffer's kernel-table index.
    pub fn index(&self) -> usize {
        self.index as usize
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.inner.driver.same_driver(handle)
    }

    fn registered_capacity(&self) -> usize {
        self.inner.slots[self.index as usize].len as usize
    }

    pub(crate) fn kernel_index(&self) -> u16 {
        self.index
    }

    pub(crate) fn read_capacity_u32(&self) -> u32 {
        self.range_len
    }

    pub(crate) fn write_len_u32(&self) -> u32 {
        self.len
    }

    pub(crate) fn fixed_ptr(&self) -> *const u8 {
        self.view_ptr()
    }

    pub(crate) fn fixed_ptr_mut(&mut self) -> *mut u8 {
        self.view_ptr_mut()
    }

    pub(crate) fn set_len_after_read(&mut self, len: usize) -> io::Result<()> {
        if len > self.range_len as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "kernel reported {len} bytes for a fixed read with capacity {}",
                    self.range_len
                ),
            ));
        }
        self.len = len as u32;
        Ok(())
    }

    fn view_ptr(&self) -> *const u8 {
        // Safety: range validation guarantees the offset stays within the
        // registered object, including the one-past rule.
        unsafe { self.ptr.as_ptr().add(self.range_start as usize) }
    }

    fn view_ptr_mut(&mut self) -> *mut u8 {
        self.view_ptr().cast_mut()
    }
}

impl<B: 'static> Drop for FixedBuf<B> {
    fn drop(&mut self) {
        let index = self.index as usize;
        let slot = &self.inner.slots[index];
        debug_assert!(slot.acquired.get());
        slot.next_free.set(self.inner.free_head.get());
        slot.acquired.set(false);
        self.inner.free_head.set(Some(index));
    }
}

impl<B: 'static> Deref for FixedBuf<B> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<B: 'static> DerefMut for FixedBuf<B> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.as_mut_slice()
    }
}

impl<B: 'static> AsRef<[u8]> for FixedBuf<B> {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl<B: 'static> AsMut<[u8]> for FixedBuf<B> {
    fn as_mut(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// Safety: the pool keeps the registered region stable and initialized, while
// this non-cloneable value owns its whole slot.
unsafe impl<B: 'static> StableBuf for FixedBuf<B> {
    fn stable_ptr(&self) -> *const u8 {
        self.view_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len as usize
    }
}

// Safety: the pool keeps the selected region stable and this non-cloneable
// value provides exclusive access to its whole slot.
unsafe impl<B: 'static> StableBufMut for FixedBuf<B> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.view_ptr_mut()
    }

    fn bytes_remaining(&self) -> usize {
        self.range_len as usize
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(
            init_len <= self.range_len as usize,
            "initialized length exceeds capacity"
        );
        self.len = init_len as u32;
    }
}

impl<B: 'static> fmt::Debug for FixedBufPool<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBufPool")
            .field("len", &self.len())
            .finish()
    }
}

impl<B: 'static> fmt::Debug for FixedBuf<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuf")
            .field("index", &self.index)
            .field("range", &self.range())
            .field("len", &self.len)
            .finish()
    }
}

impl<B: 'static> Storage<B> {
    fn into_buffers(self) -> Vec<B> {
        let Self { buffers } = self;
        Vec::from(buffers)
            .into_iter()
            .map(UnsafeCell::into_inner)
            .collect()
    }
}

impl<B: 'static> Inner<B> {
    fn into_buffers(mut self) -> Vec<B> {
        self.registered.set(false);
        (*self.storage.take().expect("fixed-buffer storage missing")).into_buffers()
    }
}

impl<B: 'static> Drop for Inner<B> {
    fn drop(&mut self) {
        if !self.registered.get() {
            return;
        }

        let release = self.driver.unregister(self.generation);
        if matches!(
            release,
            Ok(FixedBufRelease::Unregistered | FixedBufRelease::RingGone)
        ) {
            self.registered.set(false);
            return;
        }

        let Some(storage) = self.storage.take() else {
            return;
        };
        self.registered.set(false);
        let erased: Box<dyn Any> = storage;
        let retention = self.driver.retain(self.generation, erased);
        log::warn!(
            target: "norn_uring::fixedbuf",
            "unregister.failed error={release:?} retention={retention:?}"
        );
    }
}

fn unwrap_unregistered<B: 'static>(inner: Rc<Inner<B>>) -> Inner<B> {
    match Rc::try_unwrap(inner) {
        Ok(inner) => inner,
        Err(_) => unreachable!("unpublished fixed-buffer pool was shared"),
    }
}

fn map_reserve_error(err: ReserveFixedBufError) -> RegisterErrorKind {
    match err {
        ReserveFixedBufError::DriverStopped => RegisterErrorKind::DriverStopped,
        ReserveFixedBufError::TableInUse => RegisterErrorKind::AlreadyRegistered,
        ReserveFixedBufError::GenerationExhausted => RegisterErrorKind::GenerationExhausted,
    }
}

fn map_register_io_error(err: io::Error) -> RegisterErrorKind {
    match err.raw_os_error() {
        Some(libc::ENOSYS | libc::EOPNOTSUPP) => RegisterErrorKind::Unsupported(err),
        Some(libc::ENOMEM | libc::EPERM) => RegisterErrorKind::ResourceExhausted(err),
        _ => RegisterErrorKind::Io(err),
    }
}

fn map_release_error(err: FixedBufReleaseError) -> UnregisterErrorKind {
    match err {
        FixedBufReleaseError::StateMismatch => UnregisterErrorKind::StateMismatch,
        FixedBufReleaseError::DriverBorrowed => UnregisterErrorKind::DriverBorrowed,
        FixedBufReleaseError::Io(err) => UnregisterErrorKind::Io(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SharedRegion {
        bytes: Rc<UnsafeCell<[u8; 32]>>,
        range: Range<usize>,
    }

    unsafe fn project_shared_region(buffer: &mut SharedRegion) -> &mut [u8] {
        let ptr = unsafe { (*buffer.bytes.get()).as_mut_ptr().add(buffer.range.start) };
        unsafe { std::slice::from_raw_parts_mut(ptr, buffer.range.len()) }
    }

    struct DropTracked {
        bytes: [u8; 8],
        drops: Rc<Cell<usize>>,
    }

    // Safety: `bytes` is inline and the pool captures its address after final
    // placement without exposing `DropTracked` again until release.
    unsafe impl FixedBuffer for DropTracked {
        fn fixed_region(&mut self) -> &mut [u8] {
            &mut self.bytes
        }
    }

    impl Drop for DropTracked {
        fn drop(&mut self) {
            self.drops.set(self.drops.get() + 1);
        }
    }

    #[test]
    fn register_errors_recover_original_buffers() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let buffers = vec![Vec::new()];
        let err = driver.handle().register_fixed_buffers(buffers).unwrap_err();
        assert!(matches!(
            err.kind(),
            RegisterErrorKind::EmptyBuffer { index: 0 }
        ));
        assert_eq!(err.into_buffers(), vec![Vec::<u8>::new()]);
    }

    #[test]
    fn registration_rejects_exact_and_partial_overlap() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();

        for ranges in [[0..16, 0..16], [0..16, 8..24]] {
            let bytes = Rc::new(UnsafeCell::new([0u8; 32]));
            let buffers = ranges
                .into_iter()
                .map(|range| SharedRegion {
                    bytes: Rc::clone(&bytes),
                    range,
                })
                .collect();
            // Safety: the test projection produces stable owned regions, but
            // intentionally violates pairwise disjointness to verify rejection.
            let err = unsafe {
                handle.register_fixed_buffers_with(buffers, |buffer| project_shared_region(buffer))
            }
            .unwrap_err();
            assert!(matches!(
                err.kind(),
                RegisterErrorKind::OverlappingBuffers {
                    first: 0,
                    second: 1
                }
            ));
            assert_eq!(err.into_buffers().len(), 2);
        }
    }

    #[test]
    fn fixed_buffer_lease_stays_compact() {
        assert_eq!(std::mem::size_of::<FixedBuf<[u8; 1]>>(), 32);
    }

    #[test]
    fn acquired_view_state_and_recycling() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let pool = driver
            .handle()
            .register_fixed_buffers(vec![[0u8; 16], [0u8; 16]])
            .map_err(io::Error::other)?;

        let first = pool.try_acquire().unwrap();
        assert_eq!(first.index(), 0);
        assert_eq!(first.len(), 16);
        assert_eq!(
            pool.try_acquire_at(0).unwrap_err(),
            AcquireError::InUse { index: 0 }
        );

        let mut second = pool.try_acquire().unwrap();
        assert_eq!(second.index(), 1);
        assert_eq!(pool.try_acquire().unwrap_err(), AcquireError::Exhausted);
        second.set_range(4..12).unwrap();
        assert_eq!(second.range(), 4..12);
        assert_eq!(second.capacity(), 8);
        assert_eq!(second.len(), 0);
        second.as_full_slice_mut()[..3].copy_from_slice(b"abc");
        second.set_len(3).unwrap();
        assert_eq!(&*second, b"abc");
        second.consume(0);
        assert_eq!(second.range(), 4..12);
        assert_eq!(&*second, b"abc");
        second.consume(2);
        assert_eq!(second.range(), 6..12);
        assert_eq!(second.capacity(), 6);
        assert_eq!(second.len(), 1);
        assert_eq!(&*second, b"c");
        second.consume(1);
        assert_eq!(second.range(), 7..12);
        assert_eq!(second.capacity(), 5);
        assert!(second.is_empty());
        second.consume(usize::MAX);
        assert_eq!(second.range(), 7..12);
        assert_eq!(second.capacity(), 5);

        drop(first);
        assert_eq!(pool.try_acquire().unwrap().index(), 0);
        drop(second);
        Ok(())
    }

    #[test]
    fn state_mismatch_retains_storage_until_ring_destruction() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let drops = Rc::new(Cell::new(0));
        let pool = handle
            .register_fixed_buffers(vec![DropTracked {
                bytes: [0; 8],
                drops: Rc::clone(&drops),
            }])
            .map_err(|err| io::Error::other(err.to_string()))?;

        handle.test_forget_fixed_buffers();
        drop(pool);
        assert_eq!(drops.get(), 0);
        assert_eq!(handle.test_retained_fixed_buffers(), 1);

        let replacement = handle.register_fixed_buffers(vec![[0u8; 8]]).unwrap_err();
        assert!(matches!(
            replacement.kind(),
            RegisterErrorKind::AlreadyRegistered
        ));
        assert_eq!(drops.get(), 0);

        drop(handle);
        drop(driver);
        assert_eq!(drops.get(), 1);
        Ok(())
    }

    #[test]
    fn a_later_registration_retries_retained_pool_release() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let drops = Rc::new(Cell::new(0));
        let pool = handle
            .register_fixed_buffers(vec![DropTracked {
                bytes: [0; 8],
                drops: Rc::clone(&drops),
            }])
            .map_err(|err| io::Error::other(err.to_string()))?;

        // Holding a mutable ring borrow forces implicit pool release down its
        // conservative retention path.
        handle.test_with_ring_borrowed_mut(|| drop(pool));
        assert_eq!(drops.get(), 0);
        assert_eq!(handle.test_retained_fixed_buffers(), 1);

        let replacement = handle
            .register_fixed_buffers(vec![[0u8; 8]])
            .map_err(io::Error::other)?;
        assert_eq!(drops.get(), 1);
        assert_eq!(handle.test_retained_fixed_buffers(), 0);
        replacement
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn projection_panic_rolls_back_the_driver_reservation() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| unsafe {
            let _ =
                handle.register_fixed_buffers_with(vec![[0u8; 8]], |_| panic!("projection panic"));
        }));
        assert!(panic.is_err());

        let replacement = handle
            .register_fixed_buffers(vec![[0u8; 8]])
            .map_err(io::Error::other)?;
        replacement
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn armed_registration_releases_safely_before_commit() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let drops = Rc::new(Cell::new(0));
        let reservation = handle.reserve_fixed_buffers().unwrap();
        let generation = reservation.generation();
        let mut storage = Box::new(Storage {
            buffers: vec![UnsafeCell::new(DropTracked {
                bytes: [0; 8],
                drops: Rc::clone(&drops),
            })]
            .into_boxed_slice(),
        });
        let selected = storage.buffers[0].get_mut().fixed_region();
        let ptr = NonNull::from(&mut selected[0]);
        let iovec = libc::iovec {
            iov_base: ptr.as_ptr().cast(),
            iov_len: selected.len(),
        };
        let inner = Rc::new(Inner {
            driver: handle.fixed_buf_driver(),
            generation,
            registered: Cell::new(true),
            storage: Some(storage),
            slots: vec![SlotMeta {
                ptr,
                len: iovec.iov_len as u32,
                acquired: Cell::new(false),
                next_free: Cell::new(None),
            }]
            .into_boxed_slice(),
            free_head: Cell::new(Some(0)),
        });

        reservation.arm_kernel_call();
        reservation.with_submitter(|submitter| unsafe {
            submitter.register_buffers(std::slice::from_ref(&iovec))
        })?;

        // This is the unwind order after the syscall wrapper succeeds but
        // before the reservation is committed: storage guard, then reservation.
        drop(inner);
        drop(reservation);
        assert_eq!(drops.get(), 1);
        assert_eq!(handle.test_retained_fixed_buffers(), 0);

        let replacement = handle
            .register_fixed_buffers(vec![[0u8; 8]])
            .map_err(io::Error::other)?;
        replacement
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn pool_recovers_after_driver_destruction_with_an_outstanding_lease() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let pool = handle
            .register_fixed_buffers(vec![[0u8; 8]])
            .map_err(io::Error::other)?;
        let lease = pool.try_acquire().unwrap();

        drop(handle);
        drop(driver);
        drop(lease);

        let buffers = pool
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        assert_eq!(buffers, vec![[0u8; 8]]);
        Ok(())
    }
}
