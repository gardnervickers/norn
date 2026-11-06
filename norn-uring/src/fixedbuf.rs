//! Owned fixed buffers registered with an io_uring driver.
//!
//! A [`FixedBufTable`] owns a table of caller-provided buffers registered
//! with one driver. Each installed slot can be acquired as one non-cloneable
//! [`FixedBuf`] which fixed I/O operations consume and return, and buffers can
//! be installed, replaced, and removed one slot at a time while other slots
//! stay in use. [`Handle::register_fixed_buffers`] registers a table with
//! every slot installed; [`Handle::register_sparse_fixed_buffers`] registers
//! one with every slot vacant. A driver holds at most one table at a time.
//!
//! `Vec<u8>` and `BytesMut` expose only their initialized lengths. Resize them
//! before registration; `Vec::with_capacity(N)` and
//! `BytesMut::with_capacity(N)` expose empty regions and are rejected.
//!
//! ```no_run
//! use norn_uring::{fs::File, Handle};
//!
//! # async fn example(file: &File) -> Result<(), Box<dyn std::error::Error>> {
//! let table = Handle::current().register_fixed_buffers(vec![vec![0u8; 4096]])?;
//! let buf = table.try_acquire()?;
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
//! let original = table.unregister()?;
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
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut, Range};
use std::ptr::NonNull;
use std::rc::Rc;

use crate::buf::{StableBuf, StableBufMut};
use crate::driver::{
    FixedBufDriver, FixedBufGeneration, FixedBufRelease, FixedBufReleaseError, FixedBufUpdateError,
    ReserveFixedBufError,
};
use crate::Handle;

pub use crate::buf::FixedBuffer;

const MAX_FIXED_BUFFERS: usize = 1 << 14;

/// Exclusive ownership of one slot in a [`FixedBufTable`].
///
/// This type is not cloneable. Dropping it returns its whole slot to the table.
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

/// Separately allocated values installed in a [`FixedBufTable`]'s slots.
struct Storage<B> {
    slots: Box<[Cell<Option<SlotValue<B>>>]>,
    /// A replacement whose region was passed to a kernel update that has not
    /// returned. It is only left here if that update unwinds.
    staged: Cell<Option<SlotValue<B>>>,
}

/// One separately allocated table value.
///
/// The allocation is held as a raw pointer rather than a `Box`, so moving the
/// owner never reasserts uniqueness over a region the kernel may write.
struct SlotValue<B>(NonNull<UnsafeCell<B>>, PhantomData<B>);

struct SlotMeta {
    ptr: Cell<NonNull<u8>>,
    /// Registered length; zero for a vacant table slot.
    len: Cell<u32>,
    acquired: Cell<bool>,
    next_free: Cell<Option<usize>>,
}

fn fixed_region_ptr(region: &mut [u8]) -> NonNull<u8> {
    // Derive permission for the entire region, rather than just its first byte.
    NonNull::new(region.as_mut_ptr()).expect("slice pointers are non-null")
}

/// Why fixed-buffer registration failed.
#[derive(Debug)]
pub enum RegisterErrorKind {
    /// No buffers were supplied, or a sparse table of zero slots was requested.
    Empty,
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
            Self::Empty => f.write_str("a fixed-buffer table must have at least one slot"),
            Self::EmptyBuffer { index } => {
                write!(f, "fixed buffer at index {index} exposes an empty region")
            }
            Self::TooManyBuffers { count, max } => write!(
                f,
                "fixed-buffer table has {count} slots, exceeding the limit of {max}"
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
    #[error("fixed-buffer index {index} is out of bounds for a table of length {len}")]
    InvalidIndex {
        /// Requested index.
        index: usize,
        /// Number of registered slots.
        len: usize,
    },
    /// The requested table index is already acquired.
    ///
    /// A slot whose install or replacement is in progress, as seen from
    /// inside [`FixedBuffer::fixed_region`], also counts as acquired.
    #[error("fixed-buffer index {index} is already acquired")]
    InUse {
        /// Requested index.
        index: usize,
    },
    /// The requested [`FixedBufTable`] slot has no installed buffer.
    #[error("fixed-buffer index {index} has no installed buffer")]
    Vacant {
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
    /// One or more [`FixedBuf`] values still own table slots.
    Busy {
        /// Number of acquired slots, including buffers owned by operations.
        acquired: usize,
    },
    /// The driver's active table did not match this table's generation.
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
                f.write_str("the driver's fixed-buffer generation does not match this table")
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

impl Handle {
    /// Register a table of caller-owned fixed buffers with this driver, with
    /// one installed slot per buffer, in input order.
    ///
    /// All buffers are registered in one `io_uring_register(2)` call. Slots
    /// can later be replaced or removed through the returned table. On failure, the returned [`RegisterError`] retains every input buffer.
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
    ) -> Result<FixedBufTable<B>, RegisterError<B>>
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
    /// contract until the returned table is successfully unregistered or its
    /// `io_uring` is destroyed. Regions selected from different values must not
    /// overlap; registration also checks and rejects overlapping intervals.
    ///
    /// # Errors
    ///
    /// Returns [`RegisterError`] under the same conditions as
    /// [`Handle::register_fixed_buffers`], retaining every input value. This
    /// includes regions which overlap another selected region in the table.
    ///
    /// The projection applies only to the buffers registered here. Installing
    /// or replacing a buffer later requires `B: FixedBuffer`.
    pub unsafe fn register_fixed_buffers_with<B, F>(
        &self,
        buffers: Vec<B>,
        mut region: F,
    ) -> Result<FixedBufTable<B>, RegisterError<B>>
    where
        B: 'static,
        F: for<'a> FnMut(&'a mut B) -> &'a mut [u8],
    {
        if buffers.is_empty() {
            return Err(RegisterError {
                kind: RegisterErrorKind::Empty,
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

        let buffer_count = buffers.len();
        let storage = Box::new(Storage::filled(buffers));
        let mut slots = Vec::with_capacity(buffer_count);
        let mut iovecs = Vec::with_capacity(buffer_count);
        let mut intervals = Vec::with_capacity(buffer_count);

        for (index, cell) in storage.slots.iter().enumerate() {
            let mut value = cell
                .take()
                .expect("filled fixed-buffer storage has a vacant slot");
            // The region is derived from the value's own allocation, so it
            // stays valid when the value goes back into its slot.
            let selected = value.project(&mut region);
            let selected_len = selected.len();
            let ptr = (!selected.is_empty()).then(|| fixed_region_ptr(selected));
            cell.set(Some(value));
            let Some(ptr) = ptr else {
                return Err(RegisterError {
                    kind: RegisterErrorKind::EmptyBuffer { index },
                    buffers: (*storage).into_values(),
                });
            };
            if selected_len > u32::MAX as usize {
                return Err(RegisterError {
                    kind: RegisterErrorKind::BufferTooLarge {
                        index,
                        len: selected_len,
                        max: u32::MAX as usize,
                    },
                    buffers: (*storage).into_values(),
                });
            }

            let start = ptr.as_ptr() as usize;
            let end = start
                .checked_add(selected_len)
                .expect("FixedBuffer returned an address range which wraps usize");
            intervals.push((start, end, index));
            slots.push(SlotMeta {
                ptr: Cell::new(ptr),
                len: Cell::new(selected_len as u32),
                acquired: Cell::new(false),
                next_free: Cell::new((index + 1 < buffer_count).then_some(index + 1)),
            });
            iovecs.push(libc::iovec {
                iov_base: ptr.as_ptr().cast(),
                iov_len: selected_len,
            });
        }

        intervals.sort_unstable_by_key(|&(start, _, _)| start);
        if let Some(pair) = intervals.windows(2).find(|pair| pair[1].0 < pair[0].1) {
            let first = pair[0].2.min(pair[1].2);
            let second = pair[0].2.max(pair[1].2);
            return Err(RegisterError {
                kind: RegisterErrorKind::OverlappingBuffers { first, second },
                buffers: (*storage).into_values(),
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
                buffers: inner.into_values(),
            });
        }

        let _ = reservation.commit();

        Ok(FixedBufTable { inner })
    }
}

/// A table of fixed buffers registered with one `io_uring` driver.
///
/// [`Handle::register_fixed_buffers`] creates a table with one installed slot
/// per buffer, and [`Handle::register_sparse_fixed_buffers`] creates one whose
/// `len` slots are all vacant. Buffers can be installed, replaced, or removed
/// one slot at a time, without unregistering the table or returning buffers
/// held in other slots. Each installed slot can be acquired as one
/// [`FixedBuf`].
///
/// The table is not cloneable. Local tasks can share acquisition through
/// `Rc<FixedBufTable<_>>` and recover the table with `Rc::try_unwrap` before
/// unregistering. Alternatively, acquire buffers up front and move each
/// [`FixedBuf`] into its task.
///
/// A slot can be replaced or removed only while it is not acquired. Fixed I/O
/// operations own their [`FixedBuf`] until the kernel's terminal completion,
/// so no submitted operation can observe a slot change: the kernel resolves a
/// slot when it issues an operation, which may be after submission.
///
/// Each value is allocated separately, so its region stays put while other
/// slots change. Slot changes are synchronous `io_uring_register(2)` calls
/// and require Linux 5.13; sparse tables require Linux 5.19.
///
/// The table occupies the driver's single fixed-buffer table. Dropping it
/// performs safe best-effort release; use [`FixedBufTable::unregister`] when
/// release errors or ownership recovery matter.
pub struct FixedBufTable<B: 'static> {
    inner: Rc<Inner<B>>,
}

/// Why a [`FixedBufTable`] slot operation failed.
#[derive(Debug)]
pub enum SlotErrorKind {
    /// The slot index does not exist.
    InvalidIndex {
        /// Requested index.
        index: usize,
        /// Number of table slots.
        len: usize,
    },
    /// The slot already holds a buffer.
    Occupied {
        /// Requested index.
        index: usize,
    },
    /// The slot holds no buffer.
    Vacant {
        /// Requested index.
        index: usize,
    },
    /// The slot is acquired by a [`FixedBuf`], possibly owned by an operation.
    ///
    /// A slot whose install or replacement is in progress, as seen from
    /// inside [`FixedBuffer::fixed_region`], also counts as acquired.
    InUse {
        /// Requested index.
        index: usize,
    },
    /// The new buffer exposed an empty region.
    EmptyBuffer,
    /// The new buffer's region cannot be represented by fixed I/O SQEs.
    BufferTooLarge {
        /// Length exposed by the buffer.
        len: usize,
        /// Maximum representable length.
        max: usize,
    },
    /// The new buffer's region overlaps the buffer installed in another slot.
    OverlappingBuffers {
        /// Index of the overlapping installed slot.
        other: usize,
    },
    /// The driver is no longer accepting registrations.
    DriverStopped,
    /// The driver was temporarily borrowed by another operation.
    DriverBorrowed,
    /// The driver's active table did not match this table's generation, or
    /// an earlier slot update unwound. The table should be dropped.
    StateMismatch,
    /// The kernel could not reserve the required resources.
    ResourceExhausted(io::Error),
    /// Another operating-system error occurred.
    Io(io::Error),
}

impl fmt::Display for SlotErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidIndex { index, len } => write!(
                f,
                "fixed-buffer index {index} is out of bounds for a table of length {len}"
            ),
            Self::Occupied { index } => {
                write!(f, "fixed-buffer slot {index} already holds a buffer")
            }
            Self::Vacant { index } => write!(f, "fixed-buffer slot {index} holds no buffer"),
            Self::InUse { index } => write!(f, "fixed-buffer slot {index} is acquired"),
            Self::EmptyBuffer => f.write_str("fixed buffer exposes an empty region"),
            Self::BufferTooLarge { len, max } => write!(
                f,
                "fixed buffer is {len} bytes, exceeding the limit of {max}"
            ),
            Self::OverlappingBuffers { other } => write!(
                f,
                "fixed buffer overlaps the region installed in slot {other}"
            ),
            Self::DriverStopped => f.write_str("the driver is not running"),
            Self::DriverBorrowed => {
                f.write_str("the io_uring driver is currently borrowed; retry the slot update")
            }
            Self::StateMismatch => {
                f.write_str("the driver's fixed-buffer table does not match this table")
            }
            Self::ResourceExhausted(err) => write!(
                f,
                "fixed-buffer resources are exhausted (check locked-memory limits): {err}"
            ),
            Self::Io(err) => write!(f, "fixed-buffer slot update failed: {err}"),
        }
    }
}

impl Error for SlotErrorKind {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::ResourceExhausted(err) | Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

/// A failed install or replacement which returns the rejected buffer.
pub struct SlotError<B> {
    kind: SlotErrorKind,
    buffer: B,
}

impl<B> SlotError<B> {
    /// Return the reason the slot operation failed.
    pub fn kind(&self) -> &SlotErrorKind {
        &self.kind
    }

    /// Recover the buffer which was not installed.
    pub fn into_buffer(self) -> B {
        self.buffer
    }

    /// Split this error into its reason and rejected buffer.
    pub fn into_parts(self) -> (SlotErrorKind, B) {
        (self.kind, self.buffer)
    }
}

impl<B> fmt::Debug for SlotError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlotError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl<B> fmt::Display for SlotError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl<B> Error for SlotError<B> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.kind.source()
    }
}

/// A table unregistration failure which retains the intact table.
pub struct UnregisterError<B: 'static> {
    kind: UnregisterErrorKind,
    table: FixedBufTable<B>,
}

impl<B: 'static> UnregisterError<B> {
    /// Return the reason unregistration failed.
    pub fn kind(&self) -> &UnregisterErrorKind {
        &self.kind
    }

    /// Recover the intact table so unregistration can be retried.
    pub fn into_table(self) -> FixedBufTable<B> {
        self.table
    }

    /// Split this error into its reason and intact table.
    pub fn into_parts(self) -> (UnregisterErrorKind, FixedBufTable<B>) {
        (self.kind, self.table)
    }
}

impl<B: 'static> fmt::Debug for UnregisterError<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UnregisterError")
            .field("kind", &self.kind)
            .field("table", &self.table)
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
    /// Register an empty table of `len` replaceable fixed-buffer slots.
    ///
    /// Install buffers with [`FixedBufTable::install`]. Requires Linux 5.19
    /// (`IORING_RSRC_REGISTER_SPARSE`).
    ///
    /// # Errors
    ///
    /// Returns [`RegisterErrorKind::Unsupported`] when the kernel rejects
    /// sparse registration with `EINVAL`, [`RegisterErrorKind::Empty`] for
    /// `len == 0`,
    /// [`RegisterErrorKind::TooManyBuffers`] above the kernel's 16384-slot
    /// limit, and otherwise the same driver and kernel errors as
    /// [`Handle::register_fixed_buffers`].
    pub fn register_sparse_fixed_buffers<B: 'static>(
        &self,
        len: usize,
    ) -> Result<FixedBufTable<B>, RegisterErrorKind> {
        if len == 0 {
            return Err(RegisterErrorKind::Empty);
        }
        if len > MAX_FIXED_BUFFERS {
            return Err(RegisterErrorKind::TooManyBuffers {
                count: len,
                max: MAX_FIXED_BUFFERS,
            });
        }

        let reservation = self.reserve_fixed_buffers().map_err(map_reserve_error)?;
        let slots = (0..len)
            .map(|_| SlotMeta {
                ptr: Cell::new(NonNull::dangling()),
                len: Cell::new(0),
                acquired: Cell::new(false),
                next_free: Cell::new(None),
            })
            .collect();
        let inner = Rc::new(Inner {
            driver: self.fixed_buf_driver(),
            generation: reservation.generation(),
            registered: Cell::new(true),
            storage: Some(Box::new(Storage::new(len))),
            slots,
            free_head: Cell::new(None),
        });

        reservation.arm_kernel_call();
        let register =
            reservation.with_submitter(|submitter| submitter.register_buffers_sparse(len as u32));
        if let Err(err) = register {
            inner.registered.set(false);
            reservation.kernel_call_failed();
            // Kernels before 5.19 reject the sparse flag, and kernels before
            // 5.13 the `IORING_REGISTER_BUFFERS2` opcode, with EINVAL.
            return Err(match err.raw_os_error() {
                Some(libc::EINVAL) => RegisterErrorKind::Unsupported(err),
                _ => map_register_io_error(err),
            });
        }
        let _ = reservation.commit();

        Ok(FixedBufTable { inner })
    }
}

impl<B: 'static> FixedBufTable<B> {
    /// Install `buffer` in a vacant slot.
    ///
    /// The region is projected once, after the value reaches its final
    /// allocation. On failure the slot stays vacant and the buffer is
    /// returned.
    ///
    /// # Errors
    ///
    /// Returns [`SlotError`] for an invalid or occupied slot (including one
    /// whose install is in progress, as seen from inside
    /// [`FixedBuffer::fixed_region`]), an invalid or overlapping region, a stopped or borrowed driver, or a kernel failure.
    pub fn install(&self, index: usize, buffer: B) -> Result<(), SlotError<B>>
    where
        B: FixedBuffer,
    {
        let inner = &*self.inner;
        let storage = inner.storage();
        let Some(slot) = inner.slots.get(index) else {
            return Err(SlotError {
                kind: self.invalid_index(index),
                buffer,
            });
        };
        // The storage check also covers a value left behind by an install
        // whose kernel update unwound. A vacant slot is acquired only while
        // another install of it is projecting or updating the kernel.
        if slot.len.get() != 0 || slot.acquired.get() || Storage::is_occupied(&storage.slots[index])
        {
            return Err(SlotError {
                kind: SlotErrorKind::Occupied { index },
                buffer,
            });
        }
        if !inner.driver.is_running() {
            return Err(SlotError {
                kind: SlotErrorKind::DriverStopped,
                buffer,
            });
        }

        let mut value = SlotValue::new(buffer);
        // `fixed_region` may call back into this table. Hold the slot so such
        // calls cannot install, replace, or remove it before this install
        // stores its value.
        slot.acquired.set(true);
        let hold = HeldSlot {
            inner,
            index,
            installed: false,
        };
        let (ptr, len) = match self.project(index, &mut value) {
            Ok(region) => region,
            Err(kind) => {
                drop(hold);
                return Err(SlotError {
                    kind,
                    buffer: value.into_inner(),
                });
            }
        };
        // The slot stays held through the kernel call; if the call unwinds,
        // the slot stays unavailable.
        mem::forget(hold);

        // Keep the value in table storage across the kernel call, so an
        // unwind cannot free a region the kernel may already reference.
        store_vacant(&storage.slots[index], value);
        // Safety: `FixedBuffer` guarantees the region, which table storage
        // owns until this slot is updated again or the table is released. The
        // slot is vacant and held, so no operation uses it.
        let result = unsafe {
            inner
                .driver
                .update_slot(inner.generation, index as u32, &region_iovec(ptr, len))
        };
        slot.acquired.set(false);
        match result {
            Ok(()) => {
                slot.ptr.set(ptr);
                slot.len.set(len);
                inner.push_free(index);
                Ok(())
            }
            Err(err) => {
                let value = storage.slots[index]
                    .take()
                    .expect("installing fixed-buffer value disappeared");
                Err(SlotError {
                    kind: map_update_error(err),
                    buffer: value.into_inner(),
                })
            }
        }
    }

    /// Replace the buffer in an installed, unacquired slot.
    ///
    /// The kernel switches the slot to the new region in one update, and the
    /// previous buffer is returned. On failure the previous buffer stays
    /// installed and the new buffer is returned.
    ///
    /// # Errors
    ///
    /// Returns [`SlotError`] for an invalid, vacant, or acquired slot, an
    /// invalid or overlapping region, a stopped or borrowed driver, or a
    /// kernel failure. A slot being installed or replaced counts as acquired.
    pub fn replace(&self, index: usize, buffer: B) -> Result<B, SlotError<B>>
    where
        B: FixedBuffer,
    {
        let inner = &*self.inner;
        let storage = inner.storage();
        if let Err(kind) = self.check_releasable(index) {
            return Err(SlotError { kind, buffer });
        }
        if !inner.driver.is_running() {
            return Err(SlotError {
                kind: SlotErrorKind::DriverStopped,
                buffer,
            });
        }
        if Storage::is_occupied(&storage.staged) {
            return Err(SlotError {
                kind: SlotErrorKind::StateMismatch,
                buffer,
            });
        }

        // Hold the slot out of circulation while the new region is projected
        // and during the kernel call. `fixed_region` may call back into this
        // table, and a held slot cannot be acquired, replaced, or removed. If
        // the kernel call unwinds, the slot stays unavailable and both values
        // stay owned by table storage until the table is released.
        let slot = &inner.slots[index];
        inner.unlink_free(index);
        slot.acquired.set(true);
        let hold = HeldSlot {
            inner,
            index,
            installed: true,
        };

        let mut value = SlotValue::new(buffer);
        let projected = self.project(index, &mut value).and_then(|region| {
            // A reentrant replacement of another slot may have unwound and
            // left its value staged.
            if Storage::is_occupied(&storage.staged) {
                Err(SlotErrorKind::StateMismatch)
            } else {
                Ok(region)
            }
        });
        let (ptr, len) = match projected {
            Ok(region) => region,
            Err(kind) => {
                drop(hold);
                return Err(SlotError {
                    kind,
                    buffer: value.into_inner(),
                });
            }
        };
        mem::forget(hold);
        store_vacant(&storage.staged, value);
        // Safety: as for `install`; the slot is held unacquirable, so no
        // operation uses it.
        let result = unsafe {
            inner
                .driver
                .update_slot(inner.generation, index as u32, &region_iovec(ptr, len))
        };
        let value = storage
            .staged
            .take()
            .expect("staged fixed-buffer value disappeared");
        slot.acquired.set(false);
        match result {
            Ok(()) => {
                let previous = storage.slots[index]
                    .replace(Some(value))
                    .expect("installed fixed-buffer slot has no value");
                slot.ptr.set(ptr);
                slot.len.set(len);
                inner.push_free(index);
                Ok(previous.into_inner())
            }
            Err(err) => {
                inner.push_free(index);
                Err(SlotError {
                    kind: map_update_error(err),
                    buffer: value.into_inner(),
                })
            }
        }
    }

    /// Remove and return the buffer in an installed, unacquired slot.
    ///
    /// Removal is permitted after driver shutdown. Once the ring has been
    /// destroyed, the buffer is returned without a kernel call.
    ///
    /// # Errors
    ///
    /// Returns [`SlotErrorKind`] for an invalid, vacant, or acquired slot, a
    /// borrowed driver, or a kernel failure. The buffer stays installed. A
    /// slot being installed or replaced counts as acquired
    /// ([`SlotErrorKind::InUse`]), not vacant.
    pub fn remove(&self, index: usize) -> Result<B, SlotErrorKind> {
        let inner = &*self.inner;
        self.check_releasable(index)?;

        let slot = &inner.slots[index];
        inner.unlink_free(index);
        slot.acquired.set(true);
        let empty = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        // Safety: an empty update installs no region. The slot is held
        // unacquirable, so no operation uses it.
        let result = unsafe {
            inner
                .driver
                .update_slot(inner.generation, index as u32, &empty)
        };
        slot.acquired.set(false);
        match result {
            Ok(()) | Err(FixedBufUpdateError::RingGone) => {
                let value = inner.storage().slots[index]
                    .take()
                    .expect("installed fixed-buffer slot has no value");
                slot.len.set(0);
                slot.ptr.set(NonNull::dangling());
                Ok(value.into_inner())
            }
            Err(err) => {
                inner.push_free(index);
                Err(map_update_error(err))
            }
        }
    }

    /// Acquire any installed slot which is not currently acquired.
    ///
    /// # Errors
    ///
    /// Returns [`AcquireError::Exhausted`] when no such slot exists.
    pub fn try_acquire(&self) -> Result<FixedBuf<B>, AcquireError> {
        self.inner.try_acquire()
    }

    /// Acquire a specific installed slot by kernel-table index.
    ///
    /// This searches the free list and is O(table length).
    ///
    /// # Errors
    ///
    /// Returns [`AcquireError::InvalidIndex`] for an unknown index,
    /// [`AcquireError::Vacant`] for a slot without a buffer, or
    /// [`AcquireError::InUse`] when that slot is already acquired. A slot
    /// being installed or replaced, as seen from inside
    /// [`FixedBuffer::fixed_region`], counts as acquired, not vacant.
    pub fn try_acquire_at(&self, index: usize) -> Result<FixedBuf<B>, AcquireError> {
        self.inner.try_acquire_at(index)
    }

    /// Return whether a buffer is installed in `index`.
    pub fn is_installed(&self, index: usize) -> bool {
        self.inner
            .slots
            .get(index)
            .is_some_and(|slot| slot.len.get() != 0)
    }

    /// Unregister the table and recover each slot's buffer by index.
    ///
    /// # Errors
    ///
    /// [`UnregisterErrorKind::Busy`] is retryable after dropping the reported
    /// acquired buffers or operations. [`UnregisterErrorKind::DriverBorrowed`]
    /// is transient and can be retried later. OS errors retain the intact
    /// table for caller-directed retry. A state mismatch is an invariant
    /// failure; dropping the returned table conservatively retains its storage
    /// until the ring is destroyed.
    pub fn unregister(self) -> Result<Vec<Option<B>>, UnregisterError<B>> {
        let inner = match Rc::try_unwrap(self.inner) {
            Ok(inner) => inner,
            Err(inner) => {
                let acquired = Rc::strong_count(&inner).saturating_sub(1);
                return Err(UnregisterError {
                    kind: UnregisterErrorKind::Busy { acquired },
                    table: Self { inner },
                });
            }
        };

        match inner.driver.unregister(inner.generation) {
            Ok(FixedBufRelease::Unregistered | FixedBufRelease::RingGone) => {
                inner.registered.set(false);
                Ok(inner.into_slots())
            }
            Err(err) => Err(UnregisterError {
                kind: map_release_error(err),
                table: Self {
                    inner: Rc::new(inner),
                },
            }),
        }
    }

    /// Return the number of slots.
    pub fn len(&self) -> usize {
        self.inner.slots.len()
    }

    /// Return whether this table has no slots.
    pub fn is_empty(&self) -> bool {
        self.inner.slots.is_empty()
    }

    fn invalid_index(&self, index: usize) -> SlotErrorKind {
        SlotErrorKind::InvalidIndex {
            index,
            len: self.inner.slots.len(),
        }
    }

    /// Check that `index` holds an installed buffer that may leave the kernel table.
    fn check_releasable(&self, index: usize) -> Result<(), SlotErrorKind> {
        let Some(slot) = self.inner.slots.get(index) else {
            return Err(self.invalid_index(index));
        };
        if slot.acquired.get() {
            return Err(SlotErrorKind::InUse { index });
        }
        if slot.len.get() == 0 {
            return Err(SlotErrorKind::Vacant { index });
        }
        Ok(())
    }

    /// Project and validate a value destined for `index`.
    ///
    /// Regions must not overlap a buffer installed in any other slot.
    fn project(
        &self,
        index: usize,
        value: &mut SlotValue<B>,
    ) -> Result<(NonNull<u8>, u32), SlotErrorKind>
    where
        B: FixedBuffer,
    {
        let selected = value.project(FixedBuffer::fixed_region);
        if selected.is_empty() {
            return Err(SlotErrorKind::EmptyBuffer);
        }
        let len = u32::try_from(selected.len()).map_err(|_| SlotErrorKind::BufferTooLarge {
            len: selected.len(),
            max: u32::MAX as usize,
        })?;
        let ptr = fixed_region_ptr(selected);
        let start = ptr.as_ptr() as usize;
        let end = start
            .checked_add(len as usize)
            .expect("FixedBuffer returned an address range which wraps usize");
        let overlapping = self.inner.slots.iter().enumerate().find(|(other, slot)| {
            let other_start = slot.ptr.get().as_ptr() as usize;
            let other_end = other_start + slot.len.get() as usize;
            *other != index && slot.len.get() != 0 && start < other_end && other_start < end
        });
        if let Some((other, _)) = overlapping {
            return Err(SlotErrorKind::OverlappingBuffers { other });
        }
        Ok((ptr, len))
    }
}

impl<B: 'static> fmt::Debug for FixedBufTable<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let installed = (0..self.len())
            .filter(|&index| self.is_installed(index))
            .count();
        f.debug_struct("FixedBufTable")
            .field("len", &self.len())
            .field("installed", &installed)
            .finish()
    }
}

fn region_iovec(ptr: NonNull<u8>, len: u32) -> libc::iovec {
    libc::iovec {
        iov_base: ptr.as_ptr().cast(),
        iov_len: len as usize,
    }
}

fn map_update_error(err: FixedBufUpdateError) -> SlotErrorKind {
    match err {
        FixedBufUpdateError::RingGone => SlotErrorKind::DriverStopped,
        FixedBufUpdateError::StateMismatch => SlotErrorKind::StateMismatch,
        FixedBufUpdateError::DriverBorrowed => SlotErrorKind::DriverBorrowed,
        FixedBufUpdateError::Io(err) => match err.raw_os_error() {
            Some(libc::ENOMEM | libc::EPERM) => SlotErrorKind::ResourceExhausted(err),
            _ => SlotErrorKind::Io(err),
        },
    }
}

/// Store `value` in a storage cell which must be empty.
///
/// A value already there may be referenced by the kernel, so it is leaked
/// rather than dropped before panicking.
fn store_vacant<B>(cell: &Cell<Option<SlotValue<B>>>, value: SlotValue<B>) {
    if let Some(stale) = cell.replace(Some(value)) {
        mem::forget(stale);
        panic!("fixed-buffer storage cell was already occupied");
    }
}

/// A slot held acquired while a new value's region is projected.
///
/// Dropping it, on a projection error or unwind, returns the slot to
/// circulation: unacquired, and on the free list if it is installed.
struct HeldSlot<'a, B: 'static> {
    inner: &'a Inner<B>,
    index: usize,
    installed: bool,
}

impl<B: 'static> Drop for HeldSlot<'_, B> {
    fn drop(&mut self) {
        self.inner.slots[self.index].acquired.set(false);
        if self.installed {
            self.inner.push_free(self.index);
        }
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
        // Safety: the table retains the initialized registered storage and this
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
        self.inner.slots[self.index as usize].len.get() as usize
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
        slot.acquired.set(false);
        self.inner.push_free(index);
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

// Safety: the table keeps the registered region stable and initialized, while
// this non-cloneable value owns its whole slot.
unsafe impl<B: 'static> StableBuf for FixedBuf<B> {
    fn stable_ptr(&self) -> *const u8 {
        self.view_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len as usize
    }
}

// Safety: the table keeps the selected region stable and this non-cloneable
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

impl<B: 'static> fmt::Debug for FixedBuf<B> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FixedBuf")
            .field("index", &self.index)
            .field("range", &self.range())
            .field("len", &self.len)
            .finish()
    }
}

impl<B> Storage<B> {
    fn new(len: usize) -> Self {
        Self {
            slots: (0..len).map(|_| Cell::new(None)).collect(),
            staged: Cell::new(None),
        }
    }

    /// Storage with one installed value per slot, in order.
    fn filled(values: Vec<B>) -> Self {
        Self {
            slots: values
                .into_iter()
                .map(|value| Cell::new(Some(SlotValue::new(value))))
                .collect(),
            staged: Cell::new(None),
        }
    }

    fn into_slots(self) -> Vec<Option<B>> {
        // A staged replacement exists only after an unwound kernel update. It
        // is dropped here, once the table is no longer registered.
        Vec::from(self.slots)
            .into_iter()
            .map(|slot| slot.into_inner().map(SlotValue::into_inner))
            .collect()
    }

    /// Recover the values of storage created by [`Storage::filled`] whose
    /// registration never became active.
    fn into_values(self) -> Vec<B> {
        self.into_slots()
            .into_iter()
            .map(|value| value.expect("filled fixed-buffer storage lost a value"))
            .collect()
    }

    fn is_occupied(cell: &Cell<Option<SlotValue<B>>>) -> bool {
        let value = cell.take();
        let occupied = value.is_some();
        cell.set(value);
        occupied
    }
}

impl<B> SlotValue<B> {
    fn new(value: B) -> Self {
        Self(
            NonNull::from(Box::leak(Box::new(UnsafeCell::new(value)))),
            PhantomData,
        )
    }

    /// Project the value's fixed region.
    ///
    /// The region is derived from the raw allocation, so it stays valid while
    /// this value is moved, until the allocation is accessed through another
    /// path or freed.
    fn project<F>(&mut self, region: F) -> &mut [u8]
    where
        F: for<'a> FnOnce(&'a mut B) -> &'a mut [u8],
    {
        // Safety: this value exclusively owns the live allocation.
        region(unsafe { &mut *UnsafeCell::raw_get(self.0.as_ptr()) })
    }

    fn into_inner(self) -> B {
        let ptr = self.0;
        mem::forget(self);
        // Safety: the pointer came from `Box::leak`, and ownership moves out
        // of `self`, which is not dropped.
        unsafe { Box::from_raw(ptr.as_ptr()) }.into_inner()
    }
}

impl<B> Drop for SlotValue<B> {
    fn drop(&mut self) {
        // Safety: the pointer came from `Box::leak` and is released once.
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}

impl<B: 'static> Inner<B> {
    fn into_values(mut self) -> Vec<B> {
        self.registered.set(false);
        (*self.storage.take().expect("fixed-buffer storage missing")).into_values()
    }

    fn into_slots(mut self) -> Vec<Option<B>> {
        self.registered.set(false);
        (*self.storage.take().expect("fixed-buffer storage missing")).into_slots()
    }

    fn storage(&self) -> &Storage<B> {
        self.storage
            .as_deref()
            .expect("fixed-buffer table storage missing")
    }

    fn try_acquire(self: &Rc<Self>) -> Result<FixedBuf<B>, AcquireError> {
        let index = self.free_head.get().ok_or(AcquireError::Exhausted)?;
        let slot = &self.slots[index];
        self.free_head.set(slot.next_free.get());
        slot.next_free.set(None);
        assert!(!slot.acquired.replace(true), "free-list slot was acquired");
        Ok(FixedBuf::new(
            Rc::clone(self),
            index as u16,
            slot.ptr.get(),
            slot.len.get(),
        ))
    }

    fn try_acquire_at(self: &Rc<Self>, index: usize) -> Result<FixedBuf<B>, AcquireError> {
        let Some(slot) = self.slots.get(index) else {
            return Err(AcquireError::InvalidIndex {
                index,
                len: self.slots.len(),
            });
        };
        if slot.acquired.get() {
            return Err(AcquireError::InUse { index });
        }
        if slot.len.get() == 0 {
            return Err(AcquireError::Vacant { index });
        }

        self.unlink_free(index);
        slot.acquired.set(true);
        Ok(FixedBuf::new(
            Rc::clone(self),
            index as u16,
            slot.ptr.get(),
            slot.len.get(),
        ))
    }

    fn push_free(&self, index: usize) {
        self.slots[index].next_free.set(self.free_head.get());
        self.free_head.set(Some(index));
    }

    /// Remove an installed, unacquired slot from the free list in O(len).
    fn unlink_free(&self, index: usize) {
        let slot = &self.slots[index];
        let next = slot.next_free.get();
        if self.free_head.get() == Some(index) {
            self.free_head.set(next);
        } else {
            let mut cursor = self
                .free_head
                .get()
                .expect("unacquired fixed-buffer slot missing from free list");
            loop {
                let cursor_slot = &self.slots[cursor];
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
        Err(_) => unreachable!("unpublished fixed-buffer table was shared"),
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

    #[test]
    fn fixed_region_pointer_covers_whole_projection() {
        fn check<B: FixedBuffer>(buffer: &mut B) {
            let region = &mut buffer.fixed_region()[2..6];
            let len = region.len();
            let ptr = fixed_region_ptr(region);
            // Safety: the projection remains exclusively borrowed and live;
            // these are the same whole-region views used by FixedBuf.
            unsafe {
                let full = std::slice::from_raw_parts_mut(ptr.as_ptr(), len);
                full.copy_from_slice(b"norn");
                assert_eq!(std::slice::from_raw_parts(ptr.as_ptr(), len), b"norn");
                std::slice::from_raw_parts_mut(ptr.as_ptr().add(1), len - 1)
                    .copy_from_slice(b"ice");
            }
            assert_eq!(&buffer.fixed_region()[2..6], b"nice");
        }

        check(&mut vec![0; 8]);
        check(&mut [0; 8]);
    }

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

    // Safety: `bytes` is inline and the table captures its address after final
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

        // Holding a mutable ring borrow forces implicit table release down its
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
        let storage = Box::new(Storage::filled(vec![DropTracked {
            bytes: [0; 8],
            drops: Rc::clone(&drops),
        }]));
        let mut value = storage.slots[0].take().unwrap();
        let selected = value.project(FixedBuffer::fixed_region);
        let ptr = fixed_region_ptr(selected);
        let iovec = libc::iovec {
            iov_base: ptr.as_ptr().cast(),
            iov_len: selected.len(),
        };
        storage.slots[0].set(Some(value));
        let inner = Rc::new(Inner {
            driver: handle.fixed_buf_driver(),
            generation,
            registered: Cell::new(true),
            storage: Some(storage),
            slots: vec![SlotMeta {
                ptr: Cell::new(ptr),
                len: Cell::new(iovec.iov_len as u32),
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
    fn table_recovers_after_driver_destruction_with_an_outstanding_lease() -> io::Result<()> {
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
        assert_eq!(buffers, vec![Some([0u8; 8])]);
        Ok(())
    }

    fn tracked(drops: &Rc<Cell<usize>>) -> DropTracked {
        DropTracked {
            bytes: [0; 8],
            drops: Rc::clone(drops),
        }
    }

    #[test]
    fn failed_sparse_registration_rolls_back_the_driver_reservation() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let mut foreign = Box::new([0u8; 8]);
        let iovec = libc::iovec {
            iov_base: foreign.as_mut_ptr().cast(),
            iov_len: foreign.len(),
        };
        // Occupy the kernel table behind the registry's back.
        // Safety: `foreign` outlives the explicit unregistration below.
        handle.test_with_submitter(|submitter| unsafe { submitter.register_buffers(&[iovec]) })?;

        let err = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(2)
            .unwrap_err();
        assert!(
            matches!(&err, RegisterErrorKind::Io(err) if err.raw_os_error() == Some(libc::EBUSY))
        );

        handle.test_with_submitter(|submitter| submitter.unregister_buffers())?;
        drop(foreign);
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(2)
            .map_err(io::Error::other)?;
        table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn sparse_registration_validates_length() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        assert!(matches!(
            handle.register_sparse_fixed_buffers::<Vec<u8>>(0),
            Err(RegisterErrorKind::Empty)
        ));
        assert!(matches!(
            handle.register_sparse_fixed_buffers::<Vec<u8>>(MAX_FIXED_BUFFERS + 1),
            Err(RegisterErrorKind::TooManyBuffers { .. })
        ));
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(MAX_FIXED_BUFFERS)
            .map_err(io::Error::other)?;
        assert_eq!(table.len(), MAX_FIXED_BUFFERS);
        table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn sparse_table_installs_replaces_and_removes_slots() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(3)
            .map_err(io::Error::other)?;

        assert_eq!(table.try_acquire().unwrap_err(), AcquireError::Exhausted);
        assert_eq!(
            table.try_acquire_at(1).unwrap_err(),
            AcquireError::Vacant { index: 1 }
        );
        assert!(matches!(
            table.remove(1),
            Err(SlotErrorKind::Vacant { index: 1 })
        ));
        let err = table.install(3, vec![0; 8]).unwrap_err();
        assert!(matches!(
            err.kind(),
            SlotErrorKind::InvalidIndex { index: 3, len: 3 }
        ));
        let err = table.install(1, Vec::new()).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::EmptyBuffer));
        assert!(!table.is_installed(1));

        table.install(1, vec![1; 8]).map_err(io::Error::other)?;
        assert!(table.is_installed(1));
        let err = table.install(1, vec![9; 8]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::Occupied { index: 1 }));
        assert_eq!(err.into_buffer(), vec![9; 8]);

        let lease = table.try_acquire().unwrap();
        assert_eq!(lease.index(), 1);
        assert_eq!(lease.capacity(), 8);
        assert_eq!(lease.as_full_slice(), &[1; 8]);
        let err = table.replace(1, vec![2; 4]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::InUse { index: 1 }));
        assert!(matches!(
            table.remove(1),
            Err(SlotErrorKind::InUse { index: 1 })
        ));
        drop(lease);

        assert_eq!(
            table.replace(1, vec![2; 4]).map_err(io::Error::other)?,
            vec![1; 8]
        );
        let lease = table.try_acquire_at(1).unwrap();
        assert_eq!(lease.as_full_slice(), &[2; 4]);
        drop(lease);

        table.install(0, vec![3; 16]).map_err(io::Error::other)?;
        assert_eq!(table.remove(1).map_err(io::Error::other)?, vec![2; 4]);
        assert!(!table.is_installed(1));
        assert_eq!(table.try_acquire().unwrap().index(), 0);

        let slots = table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        assert_eq!(slots, vec![Some(vec![3; 16]), None, None]);
        Ok(())
    }

    #[test]
    fn failed_slot_updates_keep_the_previous_slot_state() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(2)
            .map_err(io::Error::other)?;

        handle.test_fail_next_fixed_buffer_update(libc::ENOMEM);
        let err = table.install(0, vec![1; 8]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::ResourceExhausted(_)));
        assert_eq!(err.into_buffer(), vec![1; 8]);
        assert!(!table.is_installed(0));
        assert_eq!(table.try_acquire().unwrap_err(), AcquireError::Exhausted);

        table.install(0, vec![1; 8]).map_err(io::Error::other)?;
        handle.test_fail_next_fixed_buffer_update(libc::EFAULT);
        let err = table.replace(0, vec![2; 4]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::Io(_)));
        assert_eq!(err.into_buffer(), vec![2; 4]);
        let lease = table.try_acquire().unwrap();
        assert_eq!(lease.as_full_slice(), &[1; 8]);
        drop(lease);

        handle.test_fail_next_fixed_buffer_update(libc::EFAULT);
        assert!(matches!(table.remove(0), Err(SlotErrorKind::Io(_))));
        assert_eq!(table.try_acquire_at(0).unwrap().capacity(), 8);

        let slots = table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        assert_eq!(slots, vec![Some(vec![1; 8]), None]);
        Ok(())
    }

    #[test]
    fn sparse_table_rejects_regions_overlapping_another_slot() -> io::Result<()> {
        struct SharedSlice(SharedRegion);

        // Safety: the regions are stable and initialized while the test runs;
        // the test intentionally overlaps them to verify rejection.
        unsafe impl FixedBuffer for SharedSlice {
            fn fixed_region(&mut self) -> &mut [u8] {
                unsafe { project_shared_region(&mut self.0) }
            }
        }

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let table = driver
            .handle()
            .register_sparse_fixed_buffers::<SharedSlice>(2)
            .map_err(io::Error::other)?;
        let bytes = Rc::new(UnsafeCell::new([0u8; 32]));
        let region = |range| {
            SharedSlice(SharedRegion {
                bytes: Rc::clone(&bytes),
                range,
            })
        };

        table
            .install(0, region(0..16))
            .map_err(|err| io::Error::other(err.to_string()))?;
        let err = table.install(1, region(8..24)).unwrap_err();
        assert!(matches!(
            err.kind(),
            SlotErrorKind::OverlappingBuffers { other: 0 }
        ));
        table
            .install(1, region(16..32))
            .map_err(|err| io::Error::other(err.to_string()))?;
        // A replacement may reuse its own slot's region, but not another's.
        table
            .replace(0, region(0..16))
            .map_err(|err| io::Error::other(err.to_string()))?;
        let Err(err) = table.replace(0, region(12..20)) else {
            panic!("overlapping replacement succeeded");
        };
        assert!(matches!(
            err.kind(),
            SlotErrorKind::OverlappingBuffers { other: 1 }
        ));
        table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn sparse_table_and_dense_pool_share_the_single_driver_table() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(1)
            .map_err(io::Error::other)?;
        let err = handle.register_fixed_buffers(vec![[0u8; 8]]).unwrap_err();
        assert!(matches!(err.kind(), RegisterErrorKind::AlreadyRegistered));

        let busy = table.try_acquire().err();
        assert_eq!(busy, Some(AcquireError::Exhausted));
        table.install(0, vec![0; 8]).map_err(io::Error::other)?;
        let lease = table.try_acquire().unwrap();
        let (kind, table) = table.unregister().unwrap_err().into_parts();
        assert!(matches!(kind, UnregisterErrorKind::Busy { acquired: 1 }));
        drop(lease);
        table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;

        let pool = handle
            .register_fixed_buffers(vec![[0u8; 8]])
            .map_err(io::Error::other)?;
        assert!(matches!(
            handle.register_sparse_fixed_buffers::<Vec<u8>>(1),
            Err(RegisterErrorKind::AlreadyRegistered)
        ));
        pool.unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        Ok(())
    }

    #[test]
    fn dropped_table_retains_installed_storage_until_ring_destruction() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let drops = Rc::new(Cell::new(0));
        let table = handle
            .register_sparse_fixed_buffers::<DropTracked>(2)
            .map_err(io::Error::other)?;
        table
            .install(1, tracked(&drops))
            .map_err(|err| io::Error::other(err.to_string()))?;

        handle.test_with_ring_borrowed_mut(|| drop(table));
        assert_eq!(drops.get(), 0);
        assert_eq!(handle.test_retained_fixed_buffers(), 1);

        drop(handle);
        drop(driver);
        assert_eq!(drops.get(), 1);
        Ok(())
    }

    #[test]
    fn stopped_driver_rejects_installs_but_releases_slots() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let table = handle
            .register_sparse_fixed_buffers::<Vec<u8>>(3)
            .map_err(io::Error::other)?;
        table.install(0, vec![1; 8]).map_err(io::Error::other)?;
        table.install(1, vec![2; 8]).map_err(io::Error::other)?;

        // Shut down with the handle keeping the ring alive.
        drop(driver);
        let err = table.install(2, vec![3; 8]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::DriverStopped));
        let err = table.replace(0, vec![3; 8]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::DriverStopped));
        assert_eq!(table.remove(0).map_err(io::Error::other)?, vec![1; 8]);

        // Once the ring is destroyed, removal needs no kernel call.
        drop(handle);
        assert_eq!(table.remove(1).map_err(io::Error::other)?, vec![2; 8]);
        let slots = table
            .unregister()
            .map_err(|err| io::Error::other(err.to_string()))?;
        assert_eq!(slots, vec![None, None, None]);
        Ok(())
    }

    #[test]
    fn state_mismatch_rejects_slot_updates() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let drops = Rc::new(Cell::new(0));
        let table = handle
            .register_sparse_fixed_buffers::<DropTracked>(2)
            .map_err(io::Error::other)?;
        table
            .install(0, tracked(&drops))
            .map_err(|err| io::Error::other(err.to_string()))?;

        handle.test_forget_fixed_buffers();
        let err = table.install(1, tracked(&drops)).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::StateMismatch));
        drop(err);
        assert_eq!(drops.get(), 1);
        assert!(matches!(table.remove(0), Err(SlotErrorKind::StateMismatch)));

        drop(table);
        assert_eq!(drops.get(), 1);
        assert_eq!(handle.test_retained_fixed_buffers(), 1);
        drop(handle);
        drop(driver);
        assert_eq!(drops.get(), 2);
        Ok(())
    }
}
