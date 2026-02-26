//! Traits for I/O buffers.
//!
//! IoUring requires that buffers used for I/O operations do not
//! move for the lifetime of the operation. In other words, the
//! operation takes ownership of the buffer.
//!
//! Some buffer types, such as Bytes or Vec<u8> can move due
//! to reallocation. To avoid this, we use the StableBuf trait
//! which restricts the buffer to a stable pointer into memory.
use std::cmp;

use bytes::{Bytes, BytesMut};

/// [`StableBuf`] is a trait for types which expose a
/// stable pointer into initialized memory.
///
/// ### Safety
/// Implementors of this trait must ensure that the pointer returned by
/// stable_ptr is valid and points to initialized memory of at least
/// bytes_init bytes.
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

/// [`StableBufMut`] is a trait for types which expose a
/// stable pointer into memory.
///
/// ### Safety
/// Implementors of this trait must ensure that the pointer returned by
/// stable_ptr_mut is valid.
pub unsafe trait StableBufMut: Unpin + 'static {
    /// Returns a mutable pointer to the stable memory location.
    fn stable_ptr_mut(&mut self) -> *mut u8;

    /// Returns the capacity of the buffer.
    fn bytes_remaining(&self) -> usize;

    /// Set the number of initialized bytes.
    ///
    /// ### Safety
    /// Callers should ensure that all bytes from 0..init_len are initialized.
    unsafe fn set_init(&mut self, init_len: usize);
}

unsafe impl StableBuf for Vec<u8> {
    fn stable_ptr(&self) -> *const u8 {
        self.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len()
    }
}

unsafe impl StableBufMut for Vec<u8> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
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

unsafe impl StableBufMut for Box<[u8]> {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.len()
    }

    unsafe fn set_init(&mut self, _: usize) {}
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

unsafe impl StableBufMut for BytesMut {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
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
    use super::{BufCursor, StableBuf};

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
}
