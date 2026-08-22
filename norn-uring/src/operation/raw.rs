//! Type-erased storage and reference-counted handles for in-flight operations.

use std::cell::RefCell;
use std::ptr::NonNull;
use std::{io, mem};

use crate::operation::header::{Header, VTable};
use crate::operation::queue::CompletionQueue;
use crate::operation::Operation;

#[inline]
fn abort_on_panic<R>(f: impl FnOnce() -> R) -> R {
    struct Bomb;

    impl Drop for Bomb {
        fn drop(&mut self) {
            std::process::abort();
        }
    }

    let bomb = Bomb;
    let result = f();
    mem::forget(bomb);
    result
}

#[repr(C)]
pub(crate) struct RawOp<T>
where
    T: Operation,
{
    header: Header,
    state: RefCell<RawState<T>>,
}

pub(crate) struct RawState<T>
where
    T: Operation,
{
    // Drop queued owned completions before the operation data that reaped them.
    pub(crate) completions: CompletionQueue<T::Completion>,
    pub(crate) data: Option<T>,
}

impl<T> RawOp<T>
where
    T: Operation + 'static,
{
    const VTABLE: VTable = VTable {
        drop_ref: Self::drop_ref,
        clone_ref: Self::clone_ref,
        reap: Self::reap,
    };

    pub(crate) fn allocate(data: T) -> NonNull<Header> {
        let header = Header::new(&Self::VTABLE);
        let raw = RawOp {
            header,
            state: RefCell::new(RawState {
                completions: CompletionQueue::new(),
                data: Some(data),
            }),
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
    ///
    /// The reference count must be zero.
    unsafe fn destroy(ptr: NonNull<Header>) {
        let raw = Self::from_raw_header(ptr);
        // The refcount should be 0 now, so we are the only owner. We can
        // thus borrow mutably.
        let mut this = Box::from_raw(raw.as_ptr());
        debug_assert!(this.header.refcount() == 0);

        let state = this.state.get_mut();
        while let Some(completion) = state.completions.pop_front() {
            drop(completion);
        }
        debug_assert!(state.completions.is_empty());
        drop(this);
    }

    pub(crate) fn state(&self) -> &RefCell<RawState<T>> {
        &self.state
    }

    unsafe fn clone_ref(ptr: NonNull<Header>) {
        let this = Self::from_raw_header(ptr);
        this.as_ref().header.inc_refcount();
    }

    unsafe fn reap(ptr: NonNull<Header>, result: CQEResult) {
        let more = result.more();
        let this = Self::from_raw_header(ptr);
        let this = this.as_ref();
        let header = &this.header;
        abort_on_panic(|| {
            assert!(!header.is_complete());
            let mut state = this.state.borrow_mut();
            let data = state
                .data
                .as_mut()
                .expect("operation data missing while reaping completion");
            // Safety: the caller associates this unreaped kernel or synthetic completion with
            // `ptr`'s operation.
            let completion = unsafe { data.reap(result) };
            state.completions.push(completion);
        });
        if !more {
            header.set_complete();
        }
        if let Some(waker) = header.take_waker() {
            waker.wake();
        }
    }

    #[inline]
    pub(crate) unsafe fn from_raw_header(ptr: NonNull<Header>) -> NonNull<Self> {
        ptr.cast()
    }
}

/// The result value and flags for one kernel or runtime-synthesized completion.
///
/// Values are created by the runtime and passed to [`Operation::reap`].
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

    /// Return whether the runtime generated this completion without submitting the operation.
    pub fn is_synthetic(&self) -> bool {
        self.source == CompletionSource::Synthetic
    }

    /// Consume the completion and return its result value.
    ///
    /// # Errors
    ///
    /// Returns the error reported by the kernel or generated while preparing, submitting, or
    /// canceling the operation.
    pub fn into_result(self) -> io::Result<u32> {
        self.result
    }

    /// Consume the completion and return its result value and CQE flags.
    pub fn into_parts(self) -> (io::Result<u32>, u32) {
        (self.result, self.flags)
    }

    /// Return the raw CQE flags supplied by the kernel.
    ///
    /// Synthetic completions have no CQE flags and return zero.
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

/// A reference-counted handle to an in-flight operation.
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

    /// Reap one completion belonging to this operation.
    ///
    /// # Safety
    ///
    /// `result` must be an unreaped kernel or synthetic completion produced for the operation
    /// referenced by `self`. Supplying another operation's completion can manufacture ownership
    /// of resources that this operation does not own.
    pub(crate) unsafe fn reap(self, result: CQEResult) {
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
        // Safety: upheld by this method's caller; `inner` is retained by `self`
        // (or deliberately retained above for a non-terminal completion).
        unsafe { (header.vtable.reap)(inner, result) }
    }

    fn as_raw(&self) -> *const () {
        self.inner.as_ptr() as *const ()
    }

    pub(crate) fn as_raw_usize(&self) -> usize {
        sptr::Strict::expose_addr(self.as_raw())
    }

    /// Return the inner pointer without decrementing the reference count.
    ///
    /// The caller is responsible for eventually passing the pointer to
    /// [`RawOpRef::from_raw`].
    #[inline]
    pub(crate) fn into_raw(self) -> *const () {
        let raw = self.inner.as_ptr();
        mem::forget(self);
        raw as *const ()
    }

    /// Create a [`RawOpRef`] from a pointer returned by [`RawOpRef::into_raw`].
    ///
    /// # Safety
    ///
    /// The allocation referenced by `ptr` must still be live, and `ptr` must represent an
    /// outstanding reference produced by [`RawOpRef::into_raw`].
    #[inline]
    unsafe fn from_raw(ptr: *const ()) -> Self {
        let inner = NonNull::new_unchecked(ptr as *mut Header);
        RawOpRef { inner }
    }

    /// Return the operation pointer as a `usize` without decrementing the reference count.
    ///
    /// The caller is responsible for eventually passing the value to
    /// [`RawOpRef::from_raw_usize`].
    #[inline]
    pub(crate) fn into_raw_usize(self) -> usize {
        sptr::Strict::expose_addr(self.into_raw())
    }

    /// Create a [`RawOpRef`] from a value returned by [`RawOpRef::into_raw_usize`].
    ///
    /// # Safety
    ///
    /// The allocation referenced by `addr` must still be live, and `addr` must represent an
    /// outstanding reference produced by [`RawOpRef::into_raw_usize`].
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
