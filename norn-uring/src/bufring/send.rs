//! Provided-buffer rings that supply payload to bundle sends.
//!
//! A receive ring publishes empty buffers for the kernel to fill. A send ring
//! is the reverse: the application fills buffers, publishes them in order, and
//! a bundle send transmits a prefix of the published queue.
//!
//! The kernel commits every buffer it selects for a send when it selects it,
//! but a completion reports only the transmitted byte count and the first
//! selected buffer ID. Accounting from byte counts is therefore exact only when
//! every selected buffer was transmitted in full. One bundle send owns a ring
//! at a time; when a completion is ambiguous, its terminal completion reads the
//! kernel's consumption head with `IORING_REGISTER_PBUF_STATUS` and releases
//! exactly the consumed buffers. If that fails, the ring is quarantined:
//! published buffers stay unavailable until the ring is unregistered.
//!
//! A committed buffer cannot be put back, so a request that ends before
//! transmitting everything it selected, because the socket failed or the
//! request was canceled or dropped, consumes payload it never sent. The byte
//! stream then has a gap, and the ring is quarantined as well, so that no
//! later send continues the stream past the gap. The request's own result
//! reports the gap even when nobody observes it.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::error::Error;
use std::rc::Rc;
use std::sync::atomic::{self, AtomicU16};
use std::{fmt, io, ops};

use io_uring::types::{self, BufRingEntry};

use super::{register_ring_memory, AnonymousMmap, Bgid, Bid, KernelBuffer};
use crate::buf::{StableBuf, StableBufMut};
use crate::driver::BufRingRegistration;
use crate::Handle;

const NO_BID: Bid = Bid::MAX;

/// A registered buffer ring that supplies payload to bundle sends.
///
/// Acquire a [`SendBuf`], fill it, and [`publish`](Self::publish) it. A bundle
/// send such as [`TcpSocket::send_bundle`](crate::net::TcpSocket::send_bundle)
/// transmits published buffers in publication order and returns each
/// transmitted buffer to the ring for reuse.
///
/// The kernel selects payload from the ring head, not from a particular
/// connection, so a send ring should feed one byte stream. Clones share the
/// same ring. Dropping the last clone, buffer, and request unregisters the
/// ring and discards buffers that remain published.
///
/// # Example
///
/// ```no_run
/// use norn_uring::bufring::SendBufRing;
/// use norn_uring::net::TcpSocket;
///
/// # async fn send(socket: &TcpSocket) -> std::io::Result<()> {
/// let ring = SendBufRing::builder(9).buf_cnt(16).buf_len(4096).build()?;
/// for message in [&b"hello "[..], b"world"] {
///     let mut buf = ring.try_acquire().expect("free send buffer");
///     buf.extend_from_slice(message);
///     ring.publish(buf).map_err(|err| std::io::Error::other(err.to_string()))?;
/// }
/// let sent = socket.send_bundle(&ring).await?;
/// assert_eq!(sent.bytes_sent(), 11);
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct SendBufRing {
    rc: Rc<Inner>,
}

struct Inner {
    // Dropped before `storage`'s last owner: the registry retains the storage
    // until the kernel has released the ring.
    registration: BufRingRegistration,
    storage: Rc<SendRingStorage>,
}

impl fmt::Debug for SendBufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let state = self.rc.storage.state.borrow();
        f.debug_struct("SendBufRing")
            .field("bgid", &self.rc.storage.bgid)
            .field("buf_cnt", &self.rc.storage.buf_cnt)
            .field("buf_len", &self.rc.storage.buf_len)
            .field("queued", &state.queued.len())
            .field("available", &state.free.len())
            .finish()
    }
}

impl SendBufRing {
    /// Create a new builder with the given buffer group ID.
    ///
    /// The ID must not be used by another buffer ring on the same driver.
    pub fn builder(bgid: Bgid) -> SendBufRingBuilder {
        SendBufRingBuilder {
            bgid,
            buf_cnt: 64,
            buf_len: 4096,
        }
    }

    /// Acquire an unpublished buffer, or return `None` when every buffer is
    /// published or held by the application.
    pub fn try_acquire(&self) -> Option<SendBuf> {
        let bid = self.rc.storage.state.borrow_mut().free.pop()?;
        Some(SendBuf {
            ring: self.clone(),
            bid,
            len: 0,
        })
    }

    /// Queue a filled buffer for the next bundle send.
    ///
    /// Buffers are transmitted in publication order. Publication may race
    /// with an in-flight bundle send, which can then include the buffer.
    ///
    /// # Errors
    ///
    /// Returns the buffer unchanged when it is empty, belongs to another ring,
    /// or this ring is quarantined.
    pub fn publish(&self, mut buf: SendBuf) -> Result<(), PublishError> {
        let kind = if !Rc::ptr_eq(&buf.ring.rc, &self.rc) {
            Some(PublishErrorKind::ForeignRing)
        } else if buf.len == 0 {
            Some(PublishErrorKind::Empty)
        } else if self.rc.storage.state.borrow().quarantine.is_some() {
            Some(PublishErrorKind::Quarantined)
        } else {
            None
        };
        if let Some(kind) = kind {
            return Err(PublishError { kind, buf });
        }
        let bid = std::mem::replace(&mut buf.bid, NO_BID);
        // The length fits: buffer capacities are limited to `i32::MAX`.
        let len = buf.len as u32;
        drop(buf);
        self.rc.storage.publish(bid, len);
        Ok(())
    }

    /// Return the capacity of each buffer.
    pub fn buf_capacity(&self) -> usize {
        self.rc.storage.buf_len
    }

    /// Return the number of buffers owned by the ring.
    pub fn buf_count(&self) -> u16 {
        self.rc.storage.buf_cnt
    }

    /// Return the number of buffers that can be acquired now.
    pub fn available(&self) -> usize {
        self.rc.storage.state.borrow().free.len()
    }

    /// Return the number of published buffers not yet known to be consumed.
    pub fn queued(&self) -> usize {
        self.rc.storage.state.borrow().queued.len()
    }

    /// Return the payload bytes in published buffers not yet known to be
    /// consumed.
    pub fn queued_bytes(&self) -> usize {
        self.rc.storage.state.borrow().queued_bytes
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.storage.bgid
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.registration.same_driver(handle)
    }

    pub(crate) fn quarantine(&self) -> Option<Quarantine> {
        self.rc.storage.state.borrow().quarantine
    }

    /// Reserve the ring for one bundle send.
    ///
    /// Exclusivity makes the kernel head after a terminal completion
    /// attributable to that request alone.
    pub(crate) fn claim_sender(&self) -> io::Result<SendClaim> {
        let mut state = self.rc.storage.state.borrow_mut();
        if let Some(reason) = state.quarantine {
            return Err(quarantined_error(reason, None));
        }
        if state.sending {
            return Err(io::Error::from_raw_os_error(libc::EBUSY));
        }
        state.sending = true;
        Ok(SendClaim {
            ring: self.clone(),
            request: RequestAccounting::default(),
        })
    }
}

/// Why a send ring was quarantined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Quarantine {
    /// A terminal completion's consumed buffers could not be identified.
    Unidentified,
    /// A request consumed payload it did not transmit.
    Gap,
}

fn quarantined_error(reason: Quarantine, cause: Option<io::Error>) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        QuarantineError { reason, cause },
    )
}

fn is_quarantine_error(err: &io::Error) -> bool {
    err.get_ref().is_some_and(|err| err.is::<QuarantineError>())
}

// The payload of an `InvalidData` error for a quarantined ring. It keeps the
// socket error, if any, that the quarantining completion reported.
#[derive(Debug)]
struct QuarantineError {
    reason: Quarantine,
    cause: Option<io::Error>,
}

impl fmt::Display for QuarantineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self.reason {
            Quarantine::Unidentified => {
                "send buffer ring is quarantined because consumed buffers could not be identified"
            }
            Quarantine::Gap => {
                "send buffer ring is quarantined because a bundle send consumed payload it did \
                 not transmit, leaving a gap in the byte stream"
            }
        })
    }
}

impl Error for QuarantineError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.cause.as_ref().map(|cause| cause as _)
    }
}

/// Builder for a [`SendBufRing`].
#[derive(Copy, Clone, Debug)]
pub struct SendBufRingBuilder {
    bgid: Bgid,
    buf_cnt: u16,
    buf_len: usize,
}

impl SendBufRingBuilder {
    /// The number of buffers to allocate, from 1 to 32768. Defaults to 64.
    pub fn buf_cnt(mut self, buf_cnt: u16) -> Self {
        self.buf_cnt = buf_cnt;
        self
    }

    /// The capacity of each buffer, from 1 to `i32::MAX`. Defaults to 4096.
    pub fn buf_len(mut self, buf_len: usize) -> Self {
        self.buf_len = buf_len;
        self
    }

    /// Allocate and register the ring.
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid configuration, allocation failure,
    /// duplicate buffer-group ID, or kernel registration failure.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub fn build(&self) -> io::Result<SendBufRing> {
        let handle = Handle::current();
        let storage = Rc::new(SendRingStorage::new(self.bgid, self.buf_cnt, self.buf_len)?);
        let reservation = handle.reserve_buf_ring(self.bgid, Rc::clone(&storage) as _)?;
        reservation.register(|submitter| {
            register_ring_memory(
                submitter,
                &storage.ring_start,
                storage.ring_entries(),
                storage.bgid,
            )
        })?;
        Ok(SendBufRing {
            rc: Rc::new(Inner {
                registration: reservation.commit(),
                storage,
            }),
        })
    }
}

/// An unpublished buffer owned by the application.
///
/// The full capacity is always initialized, so the logical length may be set
/// freely within it. Dropping the buffer returns it to its ring unpublished.
pub struct SendBuf {
    ring: SendBufRing,
    bid: Bid,
    len: usize,
}

impl SendBuf {
    /// Return the logical payload length that publication will send.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the logical payload is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the buffer capacity.
    pub fn capacity(&self) -> usize {
        self.ring.rc.storage.buf_len
    }

    /// Clear the logical payload.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Set the logical payload length.
    ///
    /// This changes metadata only; newly exposed bytes keep their previous
    /// contents.
    ///
    /// # Panics
    ///
    /// Panics if `len` exceeds the capacity.
    pub fn set_len(&mut self, len: usize) {
        assert!(
            len <= self.capacity(),
            "send buffer length exceeds capacity"
        );
        self.len = len;
    }

    /// Append as much of `data` as fits and return the number of bytes copied.
    pub fn extend_from_slice(&mut self, data: &[u8]) -> usize {
        let start = self.len;
        let n = data.len().min(self.capacity() - start);
        self.as_full_slice_mut()[start..start + n].copy_from_slice(&data[..n]);
        self.len += n;
        n
    }

    /// Return the whole initialized buffer, regardless of logical length.
    pub fn as_full_slice(&self) -> &[u8] {
        // Safety: the buffer is fully initialized, and this unpublished value
        // exclusively owns its BID; the kernel reads only published buffers.
        unsafe { std::slice::from_raw_parts(self.ptr(), self.capacity()) }
    }

    /// Return the whole initialized buffer mutably, regardless of logical
    /// length.
    pub fn as_full_slice_mut(&mut self) -> &mut [u8] {
        // Safety: as for `as_full_slice`; `&mut self` makes the access unique.
        unsafe { std::slice::from_raw_parts_mut(self.ptr().cast_mut(), self.capacity()) }
    }

    fn ptr(&self) -> *const u8 {
        self.ring.rc.storage.buf_ptr(self.bid)
    }
}

impl Drop for SendBuf {
    fn drop(&mut self) {
        if self.bid != NO_BID {
            self.ring.rc.storage.state.borrow_mut().free.push(self.bid);
        }
    }
}

impl fmt::Debug for SendBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBuf")
            .field("bgid", &self.ring.rc.storage.bgid)
            .field("bid", &self.bid)
            .field("len", &self.len)
            .field("cap", &self.capacity())
            .finish()
    }
}

impl ops::Deref for SendBuf {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        &self.as_full_slice()[..self.len]
    }
}

impl ops::DerefMut for SendBuf {
    fn deref_mut(&mut self) -> &mut [u8] {
        let len = self.len;
        &mut self.as_full_slice_mut()[..len]
    }
}

impl AsRef<[u8]> for SendBuf {
    fn as_ref(&self) -> &[u8] {
        self
    }
}

impl AsMut<[u8]> for SendBuf {
    fn as_mut(&mut self) -> &mut [u8] {
        self
    }
}

// Safety: the storage allocation outlives every `SendBuf` through its ring
// reference and never moves. An unpublished buffer is exclusively owned by
// this value, so neither the kernel nor another handle accesses it.
unsafe impl StableBuf for SendBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.ptr()
    }

    fn bytes_init(&self) -> usize {
        self.len
    }
}

// Safety: as for `StableBuf`; the whole capacity is initialized and writable,
// and `set_init` only records a length within it.
unsafe impl StableBufMut for SendBuf {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.ptr().cast_mut()
    }

    fn bytes_remaining(&self) -> usize {
        self.capacity()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        self.set_len(init_len);
    }
}

/// Why a buffer could not be published.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishErrorKind {
    /// The buffer has no payload. The kernel cannot select empty send buffers.
    Empty,
    /// The buffer was acquired from another ring.
    ForeignRing,
    /// Ownership accounting failed, or a bundle send left a gap in the byte
    /// stream, so the ring accepts no more payload.
    Quarantined,
}

impl fmt::Display for PublishErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Empty => "send buffer has no payload",
            Self::ForeignRing => "send buffer belongs to another ring",
            Self::Quarantined => "send buffer ring is quarantined",
        })
    }
}

/// A rejected publication, which returns the buffer.
pub struct PublishError {
    kind: PublishErrorKind,
    buf: SendBuf,
}

impl PublishError {
    /// Return why publication failed.
    pub fn kind(&self) -> PublishErrorKind {
        self.kind
    }

    /// Recover the unpublished buffer.
    pub fn into_buf(self) -> SendBuf {
        self.buf
    }

    /// Return the failure and the unpublished buffer.
    pub fn into_parts(self) -> (PublishErrorKind, SendBuf) {
        (self.kind, self.buf)
    }
}

impl fmt::Debug for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PublishError")
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

impl fmt::Display for PublishError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.kind, f)
    }
}

impl Error for PublishError {}

/// The progress of a bundle send.
///
/// A successful request returns it directly; a failed request reports the
/// progress it made before failing through [`SendBundleError::progress`].
///
/// A successful request transmitted every byte it released. Released payload
/// is consumed from the ring even if it was not transmitted, for example when
/// the socket fails partway through a bundle (Linux then reports the
/// transmitted byte count instead of the error). Such a request fails and
/// quarantines the ring, and its progress shows `bytes_released() >
/// bytes_sent()`: the byte stream has a gap, and the connection should be
/// abandoned.
#[must_use = "the transmitted and released byte counts must be checked"]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SendBundleResult {
    bytes_sent: usize,
    buffers_released: usize,
    bytes_released: usize,
}

impl SendBundleResult {
    /// Return the number of payload bytes the kernel transmitted.
    pub fn bytes_sent(self) -> usize {
        self.bytes_sent
    }

    /// Return the number of published buffers returned to the ring.
    pub fn buffers_released(self) -> usize {
        self.buffers_released
    }

    /// Return the payload bytes in buffers returned to the ring.
    pub fn bytes_released(self) -> usize {
        self.bytes_released
    }

    fn add(&mut self, other: Self) {
        self.bytes_sent += other.bytes_sent;
        self.buffers_released += other.buffers_released;
        self.bytes_released += other.bytes_released;
    }
}

/// A failed bundle send, with the progress it made before failing.
///
/// One request can transmit several bundles before a later completion fails,
/// and it releases the buffers the kernel consumed even when it fails.
/// [`progress`](Self::progress) accounts for every completion of the request.
/// When `progress().bytes_released() > progress().bytes_sent()`, the request
/// left a gap in the byte stream and quarantined the ring. Payload that stays
/// published was not consumed.
///
/// Display and [`source`](Error::source) are those of [`error`](Self::error).
/// Converting into [`io::Error`] returns that error and discards the progress.
#[derive(Debug)]
pub struct SendBundleError {
    error: io::Error,
    progress: SendBundleResult,
    quarantined: bool,
}

impl SendBundleError {
    /// Return the error the request failed with.
    ///
    /// When [`is_quarantined`](Self::is_quarantined) is true this is
    /// [`io::ErrorKind::InvalidData`], and its [`source`](Error::source) is the
    /// socket error reported by the terminal completion, if any.
    pub fn error(&self) -> &io::Error {
        &self.error
    }

    /// Return the kind of [`error`](Self::error).
    pub fn kind(&self) -> io::ErrorKind {
        self.error.kind()
    }

    /// Return the payload the request transmitted and released before it
    /// failed.
    pub fn progress(&self) -> SendBundleResult {
        self.progress
    }

    /// Return whether the request failed because the ring is quarantined.
    ///
    /// The request quarantined the ring because it could not identify the
    /// buffers it consumed or because it consumed payload it did not transmit,
    /// or it found the ring already quarantined. Buffers that remain published
    /// are then unavailable until the ring is dropped, and publication and
    /// further bundle sends fail.
    pub fn is_quarantined(&self) -> bool {
        self.quarantined
    }

    /// Return the error, discarding the progress.
    pub fn into_error(self) -> io::Error {
        self.error
    }
}

impl fmt::Display for SendBundleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl Error for SendBundleError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.error.source()
    }
}

impl From<SendBundleError> for io::Error {
    fn from(error: SendBundleError) -> Self {
        error.error
    }
}

/// Progress accumulated across the completions of one bundle send request.
#[derive(Debug, Default)]
pub(crate) struct SendBundleProgress {
    progress: SendBundleResult,
    quarantine: Option<Quarantine>,
}

impl SendBundleProgress {
    /// Record a request that found the ring already quarantined.
    pub(crate) fn mark_quarantined(&mut self, reason: Quarantine) {
        self.quarantine = Some(reason);
    }

    /// Record the accounting of one completion.
    pub(crate) fn record(&mut self, progress: SendBundleResult, quarantine: Option<Quarantine>) {
        self.progress.add(progress);
        self.quarantine = self.quarantine.or(quarantine);
    }

    /// Resolve the request from its terminal completion result, after that
    /// completion has been recorded.
    pub(crate) fn finish(
        self,
        result: io::Result<u32>,
    ) -> Result<SendBundleResult, SendBundleError> {
        let error = match (result, self.quarantine) {
            (Ok(_), None) => return Ok(self.progress),
            (Ok(_), Some(reason)) => quarantined_error(reason, None),
            // A failed claim already reports the quarantine.
            (Err(err), Some(reason)) if !is_quarantine_error(&err) => {
                quarantined_error(reason, Some(err))
            }
            (Err(err), _) => err,
        };
        Err(SendBundleError {
            error,
            progress: self.progress,
            quarantined: self.quarantine.is_some(),
        })
    }
}

/// Exclusive use of a send ring by one bundle send request.
///
/// The operation owns the claim until its terminal completion, even after the
/// request is dropped; dropping the claim then permits another bundle send.
pub(crate) struct SendClaim {
    ring: SendBufRing,
    request: RequestAccounting,
}

/// What one bundle send request has consumed and transmitted so far.
#[derive(Debug, Default)]
struct RequestAccounting {
    // A completion did not identify exactly which buffers were consumed.
    uncertain: bool,
    bytes_sent: usize,
    bytes_released: usize,
}

impl SendClaim {
    /// Account for one kernel completion of the owning bundle send.
    ///
    /// This runs in `reap`, so it also accounts for an abandoned request.
    /// Returns the progress attributable to the completion and why the ring
    /// was quarantined while accounting for it, if it was.
    pub(crate) fn complete(
        &mut self,
        result: &io::Result<u32>,
        flags: u32,
    ) -> (SendBundleResult, Option<Quarantine>) {
        let inner = &self.ring.rc;
        let terminal = !io_uring::cqueue::more(flags);
        let selected = io_uring::cqueue::buffer_select(flags);
        let mut state = inner.storage.state.borrow_mut();
        state.complete(result, selected, terminal, &mut self.request, || {
            inner.registration.kernel_head(inner.storage.bgid)
        })
    }
}

impl Drop for SendClaim {
    fn drop(&mut self) {
        self.ring.rc.storage.state.borrow_mut().sending = false;
    }
}

impl fmt::Debug for SendClaim {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendClaim")
            .field("bgid", &self.ring.bgid())
            .field("request", &self.request)
            .finish()
    }
}

#[derive(Debug, Clone, Copy)]
struct Published {
    bid: Bid,
    len: u32,
}

#[derive(Debug)]
struct SendRingState {
    // The next unpublished ring-tail value.
    local_tail: u16,
    // The ring position of `queued.front()`. It trails or equals the kernel
    // head; buffers between the two are consumed but not yet attributed.
    head: u16,
    queued: VecDeque<Published>,
    queued_bytes: usize,
    free: Vec<Bid>,
    sending: bool,
    quarantine: Option<Quarantine>,
}

impl SendRingState {
    fn new(buf_cnt: u16) -> Self {
        Self {
            local_tail: 0,
            head: 0,
            queued: VecDeque::with_capacity(usize::from(buf_cnt)),
            queued_bytes: 0,
            // Pop from the end so low BIDs are acquired first.
            free: (0..buf_cnt).rev().collect(),
            sending: false,
            quarantine: None,
        }
    }

    fn release_front(&mut self, progress: &mut SendBundleResult) -> Option<Published> {
        let entry = self.queued.pop_front()?;
        self.head = self.head.wrapping_add(1);
        self.queued_bytes -= entry.len as usize;
        self.free.push(entry.bid);
        progress.buffers_released += 1;
        progress.bytes_released += entry.len as usize;
        Some(entry)
    }

    fn complete(
        &mut self,
        result: &io::Result<u32>,
        selected: Option<Bid>,
        terminal: bool,
        request: &mut RequestAccounting,
        kernel_head: impl FnOnce() -> io::Result<u16>,
    ) -> (SendBundleResult, Option<Quarantine>) {
        let mut progress = SendBundleResult::default();
        if let Ok(n) = result {
            progress.bytes_sent = *n as usize;
        }
        match (result, selected) {
            // The kernel transmitted `n` bytes starting at the first selected
            // buffer. Transmitted buffers are a prefix of the selection, so
            // releasing them never releases a buffer the kernel may still read.
            (Ok(n), Some(bid))
                if *n > 0
                    && !request.uncertain
                    && self.queued.front().map(|e| e.bid) == Some(bid) =>
            {
                let mut released = 0usize;
                while released < *n as usize {
                    let Some(entry) = self.release_front(&mut progress) else {
                        break;
                    };
                    released += entry.len as usize;
                }
                // A partial final buffer, or a count beyond the queue, means
                // the selection is not known from the byte count.
                if released != *n as usize {
                    request.uncertain = true;
                }
            }
            // These errors arise before the kernel selects a buffer, such as
            // an empty ring or a kernel without bundle sends, and the
            // completion names no buffer, so nothing was consumed.
            (Err(err), None)
                if matches!(
                    err.raw_os_error(),
                    Some(
                        libc::ENOBUFS
                            | libc::EINVAL
                            | libc::ENOMEM
                            | libc::EBADF
                            | libc::ENOTSOCK
                            | libc::EOPNOTSUPP
                    )
                ) => {}
            // Other errors and cancellations may follow selection without
            // identifying it; a mismatched BID means earlier consumption was
            // not attributed.
            _ => request.uncertain = true,
        }

        // Buffers can remain queued after a successful terminal completion:
        // ones published after the kernel's last selection, and, after a short
        // final bundle, ones it consumed without transmitting. Only the kernel
        // head can tell them apart.
        let mut quarantine = None;
        if terminal
            && self.quarantine.is_none()
            && (request.uncertain || (result.is_ok() && !self.queued.is_empty()))
        {
            match kernel_head() {
                Ok(head) => {
                    let consumed = usize::from(head.wrapping_sub(self.head));
                    if consumed <= self.queued.len() {
                        for _ in 0..consumed {
                            self.release_front(&mut progress);
                        }
                        request.uncertain = false;
                    } else {
                        quarantine = Some(Quarantine::Unidentified);
                    }
                }
                Err(_) => quarantine = Some(Quarantine::Unidentified),
            }
        }
        request.bytes_sent += progress.bytes_sent;
        request.bytes_released += progress.bytes_released;
        // Released payload that was never transmitted is a gap in the byte
        // stream. Quarantine the ring so that no later send continues past it,
        // even if nobody observes this request's result.
        if terminal
            && self.quarantine.is_none()
            && quarantine.is_none()
            && request.bytes_released > request.bytes_sent
        {
            quarantine = Some(Quarantine::Gap);
        }
        if quarantine.is_some() {
            self.quarantine = quarantine;
        }
        (progress, quarantine)
    }
}

pub(crate) struct SendRingStorage {
    bgid: Bgid,
    ring_entries_mask: u16,
    buf_cnt: u16,
    buf_len: usize,
    ring_start: AnonymousMmap,
    buf_list: Vec<KernelBuffer>,
    state: RefCell<SendRingState>,
}

impl SendRingStorage {
    fn new(bgid: Bgid, buf_cnt: u16, buf_len: usize) -> io::Result<Self> {
        // Entry lengths must not exceed the kernel's per-request limit;
        // otherwise it truncates the entry in place.
        if buf_cnt == 0 || buf_cnt > (1 << 15) || buf_len == 0 || buf_len > i32::MAX as usize {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }
        let ring_entries = buf_cnt.next_power_of_two();
        let ring_start =
            AnonymousMmap::new(std::mem::size_of::<BufRingEntry>() * usize::from(ring_entries))?;
        let buf_list = (0..buf_cnt)
            .map(|_| KernelBuffer::new(buf_len))
            .collect::<io::Result<Vec<_>>>()?;
        Ok(Self {
            bgid,
            ring_entries_mask: ring_entries - 1,
            buf_cnt,
            buf_len,
            ring_start,
            buf_list,
            state: RefCell::new(SendRingState::new(buf_cnt)),
        })
    }

    fn ring_entries(&self) -> u16 {
        self.ring_entries_mask + 1
    }

    fn buf_ptr(&self, bid: Bid) -> *const u8 {
        self.buf_list[usize::from(bid)].as_ptr()
    }

    fn publish(&self, bid: Bid, len: u32) {
        let mut state = self.state.borrow_mut();
        // At most `buf_cnt <= ring_entries` buffers are outstanding, so this
        // slot is not in the kernel-visible range `[head, tail)`.
        assert!(state.queued.len() < usize::from(self.buf_cnt));
        let slot = state.local_tail & self.ring_entries_mask;
        let entries = self.ring_start.as_ptr_mut() as *mut BufRingEntry;
        // Safety: `slot` is within the mapped entry array and is not visible
        // to the kernel until the tail store below.
        let entry = unsafe { &mut *entries.add(usize::from(slot)) };
        entry.set_addr(self.buf_ptr(bid) as u64);
        entry.set_len(len);
        entry.set_bid(bid);
        state.local_tail = state.local_tail.wrapping_add(1);
        state.queued.push_back(Published { bid, len });
        state.queued_bytes += len as usize;

        // Safety: the tail field lies within the first mapped entry; the
        // release store publishes the entry written above.
        let tail = unsafe { types::BufRingEntry::tail(self.ring_start.as_ptr() as *const _) }
            as *const AtomicU16;
        unsafe { (*tail).store(state.local_tail, atomic::Ordering::Release) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state_with(lens: &[u32]) -> SendRingState {
        let mut state = SendRingState::new(lens.len() as u16 + 2);
        for &len in lens {
            let bid = state.free.pop().unwrap();
            state.local_tail = state.local_tail.wrapping_add(1);
            state.queued.push_back(Published { bid, len });
            state.queued_bytes += len as usize;
        }
        state
    }

    fn no_head() -> io::Result<u16> {
        panic!("exact accounting must not query the kernel head")
    }

    fn errno(code: i32) -> io::Result<u32> {
        Err(io::Error::from_raw_os_error(code))
    }

    #[test]
    fn exact_completion_releases_transmitted_prefix() {
        let mut state = state_with(&[4, 6, 8]);
        let mut request = RequestAccounting::default();
        let (progress, quarantine) = state.complete(&Ok(10), Some(0), false, &mut request, no_head);
        assert_eq!(quarantine, None);
        assert!(!request.uncertain);
        assert_eq!(progress.bytes_sent(), 10);
        assert_eq!(progress.buffers_released(), 2);
        assert_eq!(progress.bytes_released(), 10);
        assert_eq!(state.head, 2);
        assert_eq!(state.queued_bytes, 8);

        // The terminal completion drains the queue, so no status query.
        let (progress, quarantine) = state.complete(&Ok(8), Some(2), true, &mut request, no_head);
        assert_eq!(quarantine, None);
        assert_eq!(progress.buffers_released(), 1);
        assert!(state.queued.is_empty());
        assert_eq!(state.free.len(), 5);
    }

    #[test]
    fn partial_buffer_defers_to_kernel_head_and_is_a_gap() {
        let mut state = state_with(&[4, 6, 8]);
        let mut request = RequestAccounting::default();
        // Bytes end inside the second buffer: it is consumed but partial.
        let (progress, _) = state.complete(&Ok(7), Some(0), false, &mut request, no_head);
        assert!(request.uncertain);
        assert_eq!(progress.buffers_released(), 2);

        // The kernel also consumed the third buffer before failing.
        let (progress, quarantine) =
            state.complete(&errno(libc::EPIPE), Some(2), true, &mut request, || Ok(3));
        assert!(!request.uncertain);
        assert_eq!(progress.buffers_released(), 1);
        assert_eq!(progress.bytes_released(), 8);
        assert!(state.queued.is_empty());
        // 18 bytes were consumed and 7 transmitted.
        assert_eq!(quarantine, Some(Quarantine::Gap));
        assert_eq!(state.quarantine, Some(Quarantine::Gap));
    }

    #[test]
    fn boundary_aligned_short_send_is_found_at_terminal_completion() {
        let mut state = state_with(&[4, 6, 8]);
        let mut request = RequestAccounting::default();
        // The kernel selected all three buffers but transmitted two before a
        // swallowed socket error; the byte count matches a buffer boundary.
        let (progress, quarantine) = state.complete(&Ok(10), Some(0), true, &mut request, || Ok(3));
        assert_eq!(progress.bytes_sent(), 10);
        assert_eq!(progress.buffers_released(), 3);
        assert_eq!(progress.bytes_released(), 18);
        assert_eq!(quarantine, Some(Quarantine::Gap));
    }

    #[test]
    fn newly_published_buffers_survive_the_head_check() {
        let mut state = state_with(&[4, 6]);
        let mut request = RequestAccounting::default();
        let (progress, quarantine) = state.complete(&Ok(4), Some(0), true, &mut request, || Ok(1));
        assert_eq!(quarantine, None);
        assert_eq!(progress.buffers_released(), 1);
        assert_eq!(state.queued.len(), 1);
    }

    #[test]
    fn mismatched_selection_is_reconciled_by_head_with_wraparound() {
        let mut state = state_with(&[4, 6, 8]);
        state.head = u16::MAX;
        state.local_tail = 2;
        let mut request = RequestAccounting::default();
        let (progress, quarantine) = state.complete(&Ok(18), Some(2), true, &mut request, || Ok(2));
        assert_eq!(quarantine, None);
        assert_eq!(progress.bytes_sent(), 18);
        assert_eq!(progress.buffers_released(), 3);
        assert_eq!(state.head, 2);
    }

    #[test]
    fn errors_before_selection_need_no_head() {
        let mut state = state_with(&[]);
        let mut request = RequestAccounting::default();
        let (progress, quarantine) =
            state.complete(&errno(libc::ENOBUFS), None, true, &mut request, no_head);
        assert_eq!(quarantine, None);
        assert_eq!(progress, SendBundleResult::default());

        // A kernel without bundle sends rejects the request at preparation,
        // so published buffers stay queued and the ring stays usable.
        let mut state = state_with(&[4, 6]);
        let mut request = RequestAccounting::default();
        let (progress, quarantine) =
            state.complete(&errno(libc::EINVAL), None, true, &mut request, no_head);
        assert_eq!(quarantine, None);
        assert_eq!(progress, SendBundleResult::default());
        assert_eq!(state.queued.len(), 2);
    }

    #[test]
    fn cancellation_after_selection_is_a_gap() {
        let mut state = state_with(&[4, 6, 8]);
        let mut request = RequestAccounting::default();
        let (progress, quarantine) =
            state.complete(&errno(libc::ECANCELED), Some(0), true, &mut request, || {
                Ok(3)
            });
        assert_eq!(progress.buffers_released(), 3);
        assert_eq!(progress.bytes_sent(), 0);
        assert_eq!(quarantine, Some(Quarantine::Gap));
        assert_eq!(state.quarantine, Some(Quarantine::Gap));
    }

    // Accounts for each completion, then resolves the request as
    // `SendBundle` does.
    fn run_request(
        state: &mut SendRingState,
        completions: Vec<(io::Result<u32>, Option<Bid>, io::Result<u16>)>,
    ) -> Result<SendBundleResult, SendBundleError> {
        let mut progress = SendBundleProgress::default();
        let mut request = RequestAccounting::default();
        let last = completions.len() - 1;
        for (index, (result, selected, head)) in completions.into_iter().enumerate() {
            let terminal = index == last;
            let (completion, quarantine) =
                state.complete(&result, selected, terminal, &mut request, || head);
            progress.record(completion, quarantine);
            if terminal {
                return progress.finish(result);
            }
        }
        unreachable!()
    }

    fn unused_head() -> io::Result<u16> {
        Err(io::Error::other("head not expected"))
    }

    fn source_errno(error: &SendBundleError) -> Option<i32> {
        error
            .source()
            .and_then(|err| err.downcast_ref::<io::Error>())
            .and_then(io::Error::raw_os_error)
    }

    #[test]
    fn terminal_error_keeps_progress_of_earlier_bundles() {
        let mut state = state_with(&[4, 6, 8, 5]);
        let error = run_request(
            &mut state,
            vec![
                // Two full bundles post `F_MORE` completions.
                (Ok(10), Some(0), unused_head()),
                (Ok(8), Some(2), unused_head()),
                // The third selects the last buffer and fails.
                (errno(libc::ECONNRESET), None, Ok(4)),
            ],
        )
        .unwrap_err();
        // The last buffer was consumed without being transmitted.
        assert!(error.is_quarantined());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(source_errno(&error), Some(libc::ECONNRESET));
        let progress = error.progress();
        assert_eq!(progress.bytes_sent(), 18);
        assert_eq!(progress.buffers_released(), 4);
        assert_eq!(progress.bytes_released(), 23);
        assert!(state.queued.is_empty());
    }

    #[test]
    fn errors_without_a_gap_report_their_own_progress() {
        // Nothing was published.
        let mut state = state_with(&[]);
        let error = run_request(
            &mut state,
            vec![(errno(libc::ENOBUFS), None, unused_head())],
        )
        .unwrap_err();
        assert_eq!(error.error().raw_os_error(), Some(libc::ENOBUFS));
        assert_eq!(error.progress(), SendBundleResult::default());
        assert!(!error.is_quarantined());

        // Two full bundles, then the socket fails before selecting more.
        let mut state = state_with(&[4, 6, 8]);
        let error = run_request(
            &mut state,
            vec![
                (Ok(10), Some(0), unused_head()),
                (errno(libc::EPIPE), None, Ok(2)),
            ],
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert!(!error.is_quarantined());
        assert_eq!(error.progress().bytes_sent(), 10);
        assert_eq!(error.progress().bytes_released(), 10);
        assert_eq!(state.queued.len(), 1);
        assert_eq!(state.quarantine, None);

        let error: io::Error = error.into();
        assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
    }

    #[test]
    fn gap_quarantine_reports_the_socket_error_and_blocks_the_ring() {
        // The request selected two buffers and transmitted nothing.
        let mut state = state_with(&[4, 6, 8]);
        let error = run_request(&mut state, vec![(errno(libc::EPIPE), None, Ok(2))]).unwrap_err();
        assert!(error.is_quarantined());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(source_errno(&error), Some(libc::EPIPE));
        assert_eq!(error.progress().bytes_sent(), 0);
        assert_eq!(error.progress().buffers_released(), 2);
        assert_eq!(error.progress().bytes_released(), 10);
        assert_eq!(state.queued.len(), 1);
        assert_eq!(state.quarantine, Some(Quarantine::Gap));

        // A swallowed socket error leaves a successful terminal completion.
        let mut state = state_with(&[4, 6]);
        let error = run_request(&mut state, vec![(Ok(4), Some(0), Ok(2))]).unwrap_err();
        assert!(error.is_quarantined());
        assert!(error.source().is_none());
        assert_eq!(error.progress().bytes_sent(), 4);
        assert_eq!(error.progress().bytes_released(), 10);
    }

    #[test]
    fn quarantine_keeps_progress_and_the_socket_error() {
        let mut state = state_with(&[4, 6, 8]);
        let error = run_request(
            &mut state,
            vec![
                (Ok(4), Some(0), unused_head()),
                (
                    errno(libc::ECANCELED),
                    None,
                    Err(io::Error::from_raw_os_error(libc::EINVAL)),
                ),
            ],
        )
        .unwrap_err();
        assert!(error.is_quarantined());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(source_errno(&error), Some(libc::ECANCELED));
        assert_eq!(error.progress().bytes_sent(), 4);
        assert_eq!(error.progress().bytes_released(), 4);
        assert_eq!(state.queued.len(), 2);
        assert_eq!(state.quarantine, Some(Quarantine::Unidentified));

        // A successful terminal completion can also quarantine.
        let mut state = state_with(&[4, 6]);
        let error = run_request(&mut state, vec![(Ok(4), Some(0), Ok(3))]).unwrap_err();
        assert!(error.is_quarantined());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.source().is_none());
        assert_eq!(error.progress().bytes_sent(), 4);

        // A request that finds the ring quarantined keeps the claim error.
        let mut request = SendBundleProgress::default();
        request.mark_quarantined(Quarantine::Gap);
        request.record(SendBundleResult::default(), None);
        let error = request
            .finish(Err(quarantined_error(Quarantine::Gap, None)))
            .unwrap_err();
        assert!(error.is_quarantined());
        assert!(is_quarantine_error(error.error()));
        assert!(error.source().is_none());
    }

    #[test]
    fn impossible_or_unknown_head_quarantines() {
        for head in [Ok(4), Err(io::Error::from_raw_os_error(libc::EINVAL))] {
            let mut state = state_with(&[4, 6, 8]);
            let mut request = RequestAccounting::default();
            let (progress, quarantine) =
                state.complete(&errno(libc::ECANCELED), None, true, &mut request, || head);
            assert_eq!(quarantine, Some(Quarantine::Unidentified));
            assert_eq!(state.quarantine, Some(Quarantine::Unidentified));
            assert_eq!(progress.buffers_released(), 0);
            assert_eq!(
                state.queued.len(),
                3,
                "unknown consumption must retain buffers"
            );
        }
    }
}
