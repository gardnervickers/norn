//! Traits for I/O buffers.
//!
//! IoUring requires that buffers used for I/O operations do not
//! move for the lifetime of the operation. In other words, the
//! operation takes ownership of the buffer.
//!
//! Some buffer types, such as `Bytes` or `Vec<u8>`, can move due
//! to reallocation. To avoid this, we use the StableBuf trait
//! which restricts the buffer to a stable pointer into memory.
use std::{cmp, io};

use bytes::{Bytes, BytesMut};

/// [`StableBuf`] is a trait for types which expose a
/// stable pointer into initialized memory.
///
/// ### Safety
/// Implementors of this trait must ensure that the pointer returned by
/// `stable_ptr` is valid and points to initialized memory of at least
/// `bytes_init` bytes.
///
/// Furthermore, the pointer must remain valid for the lifetime of the
/// request it is used in, and must not be moved.
pub unsafe trait StableBuf: Unpin + 'static {
    /// Returns a pointer to the stable memory location.
    fn stable_ptr(&self) -> *const u8;

    /// Returns the number of initialized bytes.
    fn bytes_init(&self) -> usize;

    /// Returns a slice of the initialized bytes.
    fn as_slice(&self) -> &[u8] {
        // Safety: `bytes_init` returns the number of initialized bytes.
        unsafe { std::slice::from_raw_parts(self.stable_ptr(), self.bytes_init()) }
    }

    /// Convert this buffer into a [`BufCursor`].
    ///
    /// [`BufCursor`] is a wrapper around a [`StableBuf`] which allows
    /// for maintaining an index into the initialized bytes.
    fn into_cursor(self) -> BufCursor<Self>
    where
        Self: Sized,
    {
        BufCursor::new(self)
    }

    /// Limit the number of initialized bytes.
    ///
    /// [`BufLimit`] is a wrapper around a [`StableBuf`] which limits
    /// the number of initialized bytes.
    fn limit(self, limit: usize) -> BufLimit<Self>
    where
        Self: Sized,
    {
        BufLimit::new(self, limit)
    }
}

/// A buffer that exposes a stable, exclusively writable memory region to an
/// I/O operation.
///
/// # Safety
///
/// Implementing this trait allows safe Norn APIs to give a raw pointer to the
/// kernel. Implementors must uphold all of the following requirements:
///
/// - [`StableBufMut::stable_ptr_mut`] returns a non-null, properly aligned
///   pointer. The pointer must be valid for writes of at least
///   [`StableBufMut::bytes_remaining`] consecutive bytes. This non-null and
///   alignment requirement also applies when `bytes_remaining()` is zero.
/// - The pointer, allocation, and writable length reported for an operation
///   remain unchanged from operation configuration until its terminal
///   completion or cleanup. Moving the owning buffer value during that interval
///   must not move, free, or shrink the exposed region.
/// - While an operation owns the buffer, no independent alias may read from or
///   write to the exposed region. In particular, an implementation backed by
///   shared or interior-mutable storage must prevent access through every other
///   handle while the kernel may write to the region.
/// - Every `init_len` accepted by [`StableBufMut::set_init`] must be within the
///   region previously reported by `bytes_remaining()`. The method must make at
///   least `0..init_len` observable as initialized without reading any bytes
///   that the caller did not report as initialized. It may retain a longer
///   initialized prefix that existed before the operation.
///
/// Norn does not access the buffer through Rust references while the kernel may
/// use the pointer and calls `set_init` only after terminal completion. Norn
/// also checks successful completion lengths against the exact writable length
/// submitted to the kernel before calling `set_init`.
///
/// # Example
///
/// A custom implementation can use separately allocated, exclusively owned
/// storage so moving the wrapper does not move the exposed region:
///
/// ```
/// use norn_uring::buf::StableBufMut;
///
/// struct CustomBuf {
///     storage: Box<[u8]>,
///     initialized: usize,
/// }
///
/// // Safety: the box keeps its allocation stable when `CustomBuf` moves. The
/// // wrapper is not cloneable, so ownership of it provides exclusive access to
/// // the entire writable allocation.
/// unsafe impl StableBufMut for CustomBuf {
///     fn stable_ptr_mut(&mut self) -> *mut u8 {
///         self.storage.as_mut_ptr()
///     }
///
///     fn bytes_remaining(&self) -> usize {
///         self.storage.len()
///     }
///
///     unsafe fn set_init(&mut self, init_len: usize) {
///         assert!(init_len <= self.storage.len());
///         self.initialized = self.initialized.max(init_len);
///     }
/// }
///
/// let mut buf = CustomBuf {
///     storage: vec![0; 16].into_boxed_slice(),
///     initialized: 0,
/// };
/// let ptr = buf.stable_ptr_mut();
/// // Model a completed kernel write of one byte.
/// unsafe {
///     ptr.write(42);
///     buf.set_init(1);
/// }
/// assert_eq!(buf.initialized, 1);
/// assert_eq!(buf.storage[0], 42);
/// ```
pub unsafe trait StableBufMut: Unpin + 'static {
    /// Return the start of the stable writable region.
    ///
    /// The returned pointer and the length from [`Self::bytes_remaining`] form
    /// one coherent region and must satisfy the trait's safety contract.
    fn stable_ptr_mut(&mut self) -> *mut u8;

    /// Return the number of consecutive bytes writable through
    /// [`Self::stable_ptr_mut`].
    ///
    /// The value must not exceed the allocation available from the returned
    /// pointer and must remain valid for the complete operation lifetime.
    fn bytes_remaining(&self) -> usize;

    /// Record that the kernel initialized the first `init_len` bytes.
    ///
    /// # Safety
    ///
    /// The caller must ensure that kernel access to the buffer has ended, that
    /// every byte in `0..init_len` is initialized, and that `init_len` is no
    /// greater than the writable length submitted for the completed operation.
    unsafe fn set_init(&mut self, init_len: usize);
}

pub(crate) fn set_init_checked<B>(
    buf: &mut B,
    submitted_len: usize,
    initialized_len: usize,
    operation: &'static str,
) -> io::Result<()>
where
    B: StableBufMut,
{
    if initialized_len > submitted_len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{operation} completion exceeded the submitted buffer length"),
        ));
    }

    // Safety: callers invoke this helper only after terminal completion. The
    // bound above proves that the initialized prefix fits the submitted region.
    unsafe { buf.set_init(initialized_len) };
    Ok(())
}

/// A fully initialized, writable memory region suitable for long-lived kernel
/// registration.
///
/// Unlike [`StableBufMut`], this trait describes a region whose address and
/// length may be cached beyond a single I/O operation. Implementations may
/// choose any region owned by the buffer; it need not be the buffer's spare
/// capacity or its entire backing allocation.
///
/// # Safety
///
/// The region returned by [`FixedBuffer::fixed_region`] is inspected only after
/// `self` has been placed in its final storage. The guarantees below apply while
/// the registration attempt may pass the region to the kernel. If registration
/// becomes active, they continue until the registration is successfully released
/// or the owning ring is destroyed:
///
/// - the region's address and length do not change;
/// - every byte remains allocated, initialized, and writable;
/// - the region does not overlap memory exposed independently by the value;
/// - no hidden alias reads or writes the region while the kernel or the
///   registered-buffer owner may write it; and
/// - no interior-mutation path can expose, free, resize, or relocate the
///   region.
///
/// The registration attempt owns the value exclusively and does not move it,
/// call other methods on it, or expose another reference during those intervals.
/// If an attempt fails before becoming active, every cached pointer is discarded
/// before the value is moved or returned to the caller. Moving the value is
/// permitted before `fixed_region` is called, after a failed attempt returns,
/// and after an active registration is released. Registration additionally
/// verifies that regions returned by different values in one pool are pairwise
/// disjoint.
pub unsafe trait FixedBuffer {
    /// Return the exact initialized region to register.
    fn fixed_region(&mut self) -> &mut [u8];
}

// Safety: the initialized portion of a `Vec` remains allocated and at a stable
// address while the vector is not moved, resized, or otherwise accessed.
unsafe impl FixedBuffer for Vec<u8> {
    fn fixed_region(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// Safety: a boxed slice has a fixed address and length for its lifetime, and
// exclusive ownership of the box gives exclusive access to the elements.
unsafe impl FixedBuffer for Box<[u8]> {
    fn fixed_region(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

// Safety: the initialized portion of `BytesMut` remains allocated and at a
// stable address while the value is not moved, resized, or otherwise accessed.
unsafe impl FixedBuffer for BytesMut {
    fn fixed_region(&mut self) -> &mut [u8] {
        self.as_mut()
    }
}

// Safety: the array is placed in its final storage before this method is
// called, and its address and length then remain fixed until registration ends.
unsafe impl<const N: usize> FixedBuffer for [u8; N] {
    fn fixed_region(&mut self) -> &mut [u8] {
        self.as_mut_slice()
    }
}

// Safety: `Box` keeps its pointee at a stable address, and the delegated
// implementation supplies the remaining guarantees for its selected region.
unsafe impl<T> FixedBuffer for Box<T>
where
    T: FixedBuffer + ?Sized,
{
    fn fixed_region(&mut self) -> &mut [u8] {
        (**self).fixed_region()
    }
}

unsafe impl StableBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// Safety: moving a `Vec` does not move its allocation. Exclusive ownership of
// the vector prevents aliases, and no method here reallocates its storage.
unsafe impl StableBufMut for Vec<u8> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(
            init_len <= self.capacity(),
            "initialized length exceeds capacity"
        );
        self.set_len(init_len);
    }
}

unsafe impl StableBuf for Box<[u8]> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// Safety: a boxed slice has a fixed allocation and length, and exclusive
// ownership of the box provides exclusive access to every element.
unsafe impl StableBufMut for Box<[u8]> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.len()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(
            init_len <= self.len(),
            "initialized length exceeds capacity"
        );
    }
}

unsafe impl StableBuf for &'static [u8] {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl StableBuf for &'static str {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        str::len(self)
    }
}

unsafe impl StableBuf for Bytes {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl StableBuf for BytesMut {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

// Safety: moving `BytesMut` does not move its backing allocation. Operation
// ownership prevents aliases, and these methods do not resize the allocation.
unsafe impl StableBufMut for BytesMut {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(
            init_len <= self.capacity(),
            "initialized length exceeds capacity"
        );
        if self.len() < init_len {
            self.set_len(init_len)
        }
    }
}

/// [`BufCursor`] is a wrapper around a [`StableBuf`] which allows
///  for advancing the initialized bytes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufCursor<B> {
    buf: B,
    pos: usize,
}

impl<B> BufCursor<B>
where
    B: StableBuf,
{
    /// Create a new [`BufCursor`] from a [`StableBuf`].
    ///
    /// The cursor is initialized at the start of the buffer.
    pub fn new(buf: B) -> Self {
        Self { buf, pos: 0 }
    }

    /// Advance the cursor by `n` bytes.
    pub fn consume(&mut self, n: usize) {
        self.pos = self.pos.saturating_add(n);
    }

    /// Return the inner [`StableBuf`].
    pub fn into_inner(self) -> B {
        self.buf
    }
}

unsafe impl<B> StableBuf for BufCursor<B>
where
    B: StableBuf,
{
    fn stable_ptr(&self) -> *const u8 {
        let offset = cmp::min(self.buf.bytes_init(), self.pos);
        // Safety: `offset` is always in bounds as it is the min of the initialized
        // bytes and the current position.
        unsafe { self.buf.stable_ptr().add(offset) }
    }

    fn bytes_init(&self) -> usize {
        self.buf.bytes_init().saturating_sub(self.pos)
    }
}

/// [`BufLimit`] is a wrapper around a [`StableBuf`] which limits
/// the number of initialized bytes.
#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BufLimit<B> {
    buf: B,
    limit: usize,
}

impl<B> BufLimit<B>
where
    B: StableBuf,
{
    /// Create a new [`BufLimit`] from a [`StableBuf`].
    pub fn new(buf: B, limit: usize) -> Self {
        Self { buf, limit }
    }
}

unsafe impl<B> StableBuf for BufLimit<B>
where
    B: StableBuf,
{
    fn stable_ptr(&self) -> *const u8 {
        self.buf.stable_ptr()
    }

    fn bytes_init(&self) -> usize {
        cmp::min(self.buf.bytes_init(), self.limit)
    }
}

#[cfg(test)]
mod tests {
    use super::{set_init_checked, BufCursor, StableBuf};

    #[test]
    fn cursor_consume_saturates_remaining_len() {
        let mut cursor = BufCursor::new(vec![1_u8, 2, 3]);
        cursor.consume(5);
        assert_eq!(cursor.bytes_init(), 0);
        assert!(cursor.as_slice().is_empty());
    }

    #[test]
    fn cursor_consume_saturates_position_addition() {
        let mut cursor = BufCursor::new(vec![1_u8, 2, 3]);
        cursor.consume(usize::MAX);
        cursor.consume(usize::MAX);
        assert_eq!(cursor.bytes_init(), 0);
        assert!(cursor.as_slice().is_empty());
    }

    #[test]
    fn checked_init_rejects_oversized_completions() {
        let mut buf = Vec::with_capacity(1);
        let err = set_init_checked(&mut buf, 1, 2, "test I/O").unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(buf.is_empty());
    }
}
