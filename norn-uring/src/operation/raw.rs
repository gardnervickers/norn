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

#[repr(C)]
struct RawOpStorage<T, G> {
    // Keep this prefix first so typed handles can access operation data without
    // knowing whether this allocation carries a terminal guard.
    op: RawOp<T>,
    terminal_guard: UnsafeCell<G>,
}

trait TerminalGuardSlot: 'static {
    fn release(&mut self);
}

struct NoTerminalGuard;

impl TerminalGuardSlot for NoTerminalGuard {
    fn release(&mut self) {}
}

struct SomeTerminalGuard<G>(Option<G>);

impl<G: 'static> TerminalGuardSlot for SomeTerminalGuard<G> {
    fn release(&mut self) {
        self.0.take();
    }
}

pub(crate) fn allocate<T>(data: T) -> NonNull<Header>
where
    T: Operation + 'static,
{
    RawOpStorage::<T, NoTerminalGuard>::allocate(data, NoTerminalGuard)
}

pub(crate) fn allocate_guarded<T, G>(data: T, terminal_guard: G) -> NonNull<Header>
where
    T: Operation + 'static,
    G: 'static,
{
    RawOpStorage::<T, SomeTerminalGuard<G>>::allocate(data, SomeTerminalGuard(Some(terminal_guard)))
}

impl<T> RawOp<T> {
    pub(crate) unsafe fn data_mut(&mut self) -> &mut Option<T> {
        &mut *self.data.get()
    }

    #[inline]
    pub(crate) unsafe fn from_raw_header(ptr: NonNull<Header>) -> NonNull<Self> {
        ptr.cast()
    }
}

impl<T, G> RawOpStorage<T, G>
where
    T: Operation + 'static,
    G: TerminalGuardSlot,
{
    const VTABLE: VTable = VTable {
        drop_ref: Self::drop_ref,
        clone_ref: Self::clone_ref,
        complete: Self::complete,
    };

    fn allocate(data: T, terminal_guard: G) -> NonNull<Header> {
        let header = Header::new(&Self::VTABLE);
        let raw = Self {
            op: RawOp {
                header,
                data: UnsafeCell::new(Some(data)),
            },
            terminal_guard: UnsafeCell::new(terminal_guard),
        };
        let ptr = Box::into_raw(Box::new(raw));
        // Safety: `ptr` is a valid pointer to a `Header`. RawOp is also
        // repr(C), and both of its prefix fields are first, so the pointer to
        // the header is the same as the pointer to the whole allocation.
        unsafe { NonNull::new_unchecked(ptr as *mut Header) }
    }

    unsafe fn drop_ref(ptr: NonNull<Header>) {
        let this = Self::from_raw_header(ptr);
        if this.as_ref().op.header.dec_refcount() {
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
        debug_assert!(this.op.header.refcount() == 0);

        if let Some(mut data) = this.op.data_mut().take() {
            let completions = this.op.header.completions_mut().get_mut();
            for result in mem::take(completions) {
                data.cleanup(result);
            }
        } else if !this.op.header.completions().borrow().is_empty() {
            panic!("operation state cone without waiting for all completions");
        }
        // Safety: this pointer has the exact `RawOpStorage<T, G>` type used by
        // `allocate`, and the reference count proves this is the last owner.
        drop(Box::from_raw(this as *mut Self));
    }

    unsafe fn clone_ref(ptr: NonNull<Header>) {
        let this = Self::from_raw_header(ptr);
        this.as_ref().op.header.inc_refcount();
    }

    unsafe fn complete(ptr: NonNull<Header>, result: CQEResult) -> bool {
        let more = result.more();
        let this = Self::from_raw_header(ptr);
        let raw = this.as_ref();
        assert!(!raw.op.header.is_complete());
        let header = &raw.op.header;
        {
            let mut completions = header.completions().borrow_mut();
            completions.push(result);
        }
        if !more {
            header.set_complete();
            // Safety: the single-threaded driver serializes completion, and
            // this is the only early-release path for the terminal guard.
            (*raw.terminal_guard.get()).release();
        }
        if let Some(waker) = header.take_waker() {
            waker.wake();
        }
        true
    }

    unsafe fn from_raw_header(ptr: NonNull<Header>) -> NonNull<Self> {
        ptr.cast()
    }
}

#[cfg(test)]
mod layout_tests {
    use super::*;

    #[test]
    fn unguarded_operations_pay_no_terminal_guard_storage() {
        assert_eq!(
            std::mem::size_of::<RawOpStorage<(), NoTerminalGuard>>(),
            std::mem::size_of::<RawOp<()>>()
        );
        assert!(
            std::mem::size_of::<RawOpStorage<(), SomeTerminalGuard<[usize; 2]>>>()
                > std::mem::size_of::<RawOpStorage<(), NoTerminalGuard>>()
        );
    }
}

/// The result value and flags from one io_uring completion queue entry.
///
/// Values of this type are created by the runtime and passed to
/// [`Operation`](crate::Operation), [`Singleshot`](crate::Singleshot), and
/// [`Multishot`](crate::Multishot) implementations.
#[derive(Debug)]
pub struct CQEResult {
    pub(crate) result: io::Result<u32>,
    pub(crate) flags: u32,
    source: CompletionSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionSource {
    Kernel,
    Synthetic,
}

impl CQEResult {
    pub(crate) fn new(result: io::Result<u32>, flags: u32) -> Self {
        Self {
            result,
            flags,
            source: CompletionSource::Kernel,
        }
    }

    pub(crate) fn synthetic(result: io::Result<u32>) -> Self {
        Self {
            result,
            flags: 0,
            source: CompletionSource::Synthetic,
        }
    }

    pub(crate) fn is_synthetic(&self) -> bool {
        self.source == CompletionSource::Synthetic
    }

    /// Consume the completion and return its result value.
    pub fn into_result(self) -> io::Result<u32> {
        self.result
    }

    /// Consume the completion and return its result value and CQE flags.
    pub fn into_parts(self) -> (io::Result<u32>, u32) {
        (self.result, self.flags)
    }

    /// Return the raw CQE flags supplied by the kernel.
    pub fn flags(&self) -> u32 {
        self.flags
    }

    /// Return whether the kernel will produce more completions for this operation.
    pub fn more(&self) -> bool {
        io_uring::cqueue::more(self.flags)
    }

    /// Return whether this is a zero-copy notification completion.
    pub fn is_notification(&self) -> bool {
        self.notif()
    }

    pub(crate) fn notif(&self) -> bool {
        io_uring::cqueue::notif(self.flags)
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
        let inner = self.inner;
        if more {
            // Preserve the kernel-owned reference before invoking any operation callback. A
            // callback may wake arbitrary safe code, including code that panics or polls the
            // operation synchronously. In either case the kernel still owns this reference
            // until it reports a terminal completion.
            mem::forget(self);
        }
        let header = unsafe { inner.as_ref() };
        unsafe { (header.vtable.complete)(inner, result) }
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
