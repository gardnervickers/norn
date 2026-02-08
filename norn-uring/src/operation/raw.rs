//! Contains the raw operation handle which is used to
//! track an in-flight request. This module is pretty much
//! entirely unsafe.

use std::cell::UnsafeCell;
use std::ptr::NonNull;
use std::{io, mem};

use crate::operation::header::{Header, VTable};
use crate::operation::Operation;

#[repr(C)]
pub(crate) struct RawOp<T> {
    header: Header,
    data: UnsafeCell<Option<T>>,
}

impl<T> RawOp<T>
where
    T: Operation + 'static,
{
    const VTABLE: VTable = VTable {
        drop_ref: Self::drop_ref,
        clone_ref: Self::clone_ref,
        complete: Self::complete,
    };

    pub(crate) fn allocate(data: T) -> NonNull<Header> {
        let header = Header::new(&Self::VTABLE);
        let raw = RawOp {
            header,
            data: UnsafeCell::new(Some(data)),
        };
        let ptr = Box::into_raw(Box::new(raw));
        // Safety: `ptr` is a valid pointer to a `Header`. RawOp is also
        // repr(C), so the pointer to the header is the same as the pointer
        // to the whole struct.
        unsafe { NonNull::new_unchecked(ptr as *mut Header) }
    }

    unsafe fn drop_ref(ptr: NonNull<Header>) {
        let this = Self::from_raw_header(ptr);
        if this.as_ref().header.dec_refcount() {
            Self::destroy(ptr)
        }
    }

    /// Destroy the [`RawOp`] and its associated data.
    ///
    /// # Safety
    /// This should only ever be called when the reference count is 0.
    unsafe fn destroy(ptr: NonNull<Header>) {
        let mut raw = Self::from_raw_header(ptr);
        // The refcount should be 0 now, so we are the only owner. We can
        // thus borrow mutably.
        let this = raw.as_mut();
        debug_assert!(this.header.refcount() == 0);

        if let Some(mut data) = this.data_mut().take() {
            let completions = this.header.completions_mut().get_mut();
            for result in completions.drain(..) {
                data.cleanup(result);
            }
        } else if !this.header.completions().borrow().is_empty() {
            panic!("operation state cone without waiting for all completions");
        }
        // Safety: We can cast to Box<RawTask> because we allocated it with Box::new,
        // and Header is the first field in RawTask (which is repr(C)).
        drop(Box::from_raw(this as *mut Self));
    }

    pub(crate) unsafe fn data_mut(&mut self) -> &mut Option<T> {
        &mut *self.data.get()
    }

    unsafe fn clone_ref(ptr: NonNull<Header>) {
        let this = Self::from_raw_header(ptr);
        this.as_ref().header.inc_refcount();
    }

    unsafe fn complete(ptr: NonNull<Header>, result: CQEResult) -> bool {
        let more = result.more();
        let this = Self::from_raw_header(ptr);
        let header = &this.as_ref().header;
        assert!(!header.is_complete());
        let mut completions = header.completions().borrow_mut();
        completions.push_back(result);
        if !more {
            header.set_complete();
        }
        if let Some(waker) = header.take_waker() {
            waker.wake();
        }
        true
    }

    #[inline]
    pub(crate) unsafe fn from_raw_header(ptr: NonNull<Header>) -> NonNull<Self> {
        ptr.cast()
    }
}

#[derive(Debug)]
pub struct CQEResult {
    pub(crate) result: io::Result<u32>,
    pub(crate) flags: u32,
}

impl CQEResult {
    pub(crate) fn new(result: io::Result<u32>, flags: u32) -> Self {
        Self { result, flags }
    }

    pub(crate) fn more(&self) -> bool {
        io_uring::cqueue::more(self.flags)
    }
}

/// [`RawOpHandle`] is a reference to an operation that is in
/// progress.
pub(crate) struct RawOpRef {
    inner: NonNull<Header>,
}

impl From<NonNull<Header>> for RawOpRef {
    fn from(inner: NonNull<Header>) -> Self {
        RawOpRef { inner }
    }
}

impl RawOpRef {
    /// Returns a reference to the [`Header`] for this operation.
    #[inline]
    pub(crate) fn header(&self) -> &Header {
        // Safety: `inner` is a valid pointer to a `Header`.
        //          We only ever access the header through immutable references.
        unsafe { self.inner.as_ref() }
    }

    pub(crate) fn inner(&self) -> NonNull<Header> {
        self.inner
    }

    pub(crate) fn complete(self, result: CQEResult) -> bool {
        let more = result.more();
        let header = self.header();
        let res = unsafe { (header.vtable.complete)(self.inner, result) };
        if more {
            // We need to keep the handle alive.
            mem::forget(self);
        }
        res
    }

    fn as_raw(&self) -> *const () {
        self.inner.as_ptr() as *const ()
    }

    pub(crate) fn as_raw_usize(&self) -> usize {
        sptr::Strict::expose_addr(self.as_raw())
    }

    /// Returns the inner pointer.
    ///
    /// This will **not** decrement the reference count. It is the callers responsibility
    /// to ensure that the returned pointer is passed to Handle::from_raw later.
    #[inline]
    pub(crate) fn into_raw(self) -> *const () {
        let raw = self.inner.as_ptr();
        mem::forget(self);
        raw as *const ()
    }

    /// Creates a new [Handle] from a raw pointer.
    ///
    /// ### Safety
    /// The caller must ensure that the pointer was previously obtained from a call
    /// to [Handle::into_raw]. The caller must also ensure that the allocation backing
    /// the operation referenced by this [Handle] has not been dropped.
    #[inline]
    unsafe fn from_raw(ptr: *const ()) -> Self {
        let inner = NonNull::new_unchecked(ptr as *mut Header);
        RawOpRef { inner }
    }

    /// Returns a usize representing the raw pointer for the operation.
    ///
    /// This consumes the [Handle] and does not decrement the reference count.
    #[inline]
    pub(crate) fn into_raw_usize(self) -> usize {
        sptr::Strict::expose_addr(self.into_raw())
    }

    /// Creates a new [Handle] from a usize representing a raw pointer.
    ///
    /// ### Safety
    /// The caller must ensure that the pointer was previously obtained from a call
    /// to [Handle::into_raw]. The caller must also ensure that the allocation backing
    /// the operation referenced by this [Handle] has not been dropped.
    #[inline]
    pub(crate) unsafe fn from_raw_usize(addr: usize) -> Self {
        let ptr = sptr::from_exposed_addr(addr);
        Self::from_raw(ptr)
    }

    #[inline]
    pub(crate) fn is_complete(&self) -> bool {
        let header = self.header();
        header.is_complete()
    }
}

impl Drop for RawOpRef {
    fn drop(&mut self) {
        let header = self.header();
        unsafe { (header.vtable.drop_ref)(self.inner) }
    }
}

impl Clone for RawOpRef {
    fn clone(&self) -> Self {
        let header = self.header();
        unsafe { (header.vtable.clone_ref)(self.inner) }
        RawOpRef { inner: self.inner }
    }
}
