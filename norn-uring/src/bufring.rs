//! Support for io_uring registered buffer rings.
//!
//! Copied from the test code here
//! https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/tests/register_buf_ring.rs

use std::cell::{Cell, RefCell, UnsafeCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{self, AtomicU16};
use std::{fmt, io, marker::PhantomData, ops, ptr};

use io_uring::types::{self, BufRingEntry};
use io_uring::Submitter;
use log::warn;

use crate::Handle;

/// [`RecvBufRing`] is a reference counted buffer ring which can be registered
/// with io_uring to provide buffers for read operations.
///
/// # Example
///
/// ```no_run
/// use norn_uring::bufring::RecvBufRing;
/// use norn_uring::net::UdpSocket;
///
/// # async fn receive() -> std::io::Result<()> {
/// let ring = RecvBufRing::builder(7)
///     .buf_cnt(32)
///     .buf_len(2048)
///     .build()?;
/// let socket = UdpSocket::bind("127.0.0.1:8080".parse().unwrap()).await?;
///
/// let (buffer, peer) = socket.recv_from_ring(&ring).await?;
/// println!("received {} bytes from {peer}", buffer.len());
/// # Ok(())
/// # }
/// ```
#[derive(Clone)]
pub struct RecvBufRing {
    // The RecvBufRing is reference counted because each buffer handed out has a reference back to
    // its buffer group, or in this case, to its buffer ring.
    rc: Rc<InnerBufRing>,
}

/// [`SendBufRing`] is a reference counted buffer ring used to stage outbound buffers for
/// `SendBundle`.
#[derive(Clone)]
pub struct SendBufRing {
    rc: Rc<InnerSendBufRing>,
}

/// [`SendBundleBatch`] is a one-shot collection of send buffers staged for a single
/// `SendBundle` request and UDP datagram.
#[derive(Debug)]
pub struct SendBundleBatch {
    rc: Rc<InnerSendBundleBatch>,
}

impl fmt::Debug for RecvBufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvBufRing")
            .field("bgid", &self.rc.bgid())
            .field("ring_entries", &self.rc.ring_entries())
            .field("buf_cnt", &self.rc.buf_cnt)
            .field("buf_len", &self.rc.buf_len)
            .finish()
    }
}

impl RecvBufRing {
    fn new(buf_ring: InnerBufRing) -> Self {
        RecvBufRing {
            rc: Rc::new(buf_ring),
        }
    }

    /// Create a new Builder with the given buffer group ID.
    pub fn builder(id: Bgid) -> Builder<Self> {
        Builder::new(id)
    }

    /// Returns the capacity of each buffer in the buffer ring.
    pub fn buf_capacity(&self) -> usize {
        self.rc.buf_capacity()
    }

    /// Returns the number of buffers in the buffer ring.
    pub fn buf_count(&self) -> u16 {
        self.rc.buf_cnt
    }

    pub(crate) fn get_buf(&self, res: u32, flags: u32) -> io::Result<BufRingBuf> {
        self.rc.get_buf(self.clone(), res, flags)
    }

    pub(crate) fn get_buf_bundle(&self, res: u32, flags: u32) -> io::Result<BufRingBufBundle> {
        self.rc.get_buf_bundle(self.clone(), res, flags)
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.bgid
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.handle.same_driver(handle)
    }
}

impl fmt::Debug for SendBufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBufRing")
            .field("bgid", &self.rc.bgid())
            .field("ring_entries", &self.rc.ring_entries())
            .field("buf_cnt", &self.rc.buf_cnt)
            .field("buf_len", &self.rc.buf_len)
            .finish()
    }
}

impl SendBufRing {
    fn new(buf_ring: InnerSendBufRing) -> Self {
        Self {
            rc: Rc::new(buf_ring),
        }
    }

    /// Create a new Builder with the given buffer group ID.
    pub fn builder(id: Bgid) -> Builder<Self> {
        Builder::new(id)
    }

    /// Start a new bundle batch on this send ring.
    ///
    /// Only one batch may be active per send ring at a time.
    pub fn batch(&self) -> io::Result<SendBundleBatch> {
        let id = self.rc.begin_batch()?;
        Ok(SendBundleBatch {
            rc: Rc::new(InnerSendBundleBatch::new(
                self.clone(),
                id,
                Some(SendBundleBatch::MAX_SEGMENTS),
            )),
        })
    }

    /// Returns the number of available buffers that can be checked out.
    pub fn available_buffers(&self) -> usize {
        self.rc.available_buffers()
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.bgid()
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.handle.same_driver(handle)
    }
}

impl SendBundleBatch {
    /// Maximum number of buffers that can be committed to one UDP datagram batch.
    pub const MAX_SEGMENTS: usize = 256;

    /// Check out a writable buffer from this batch.
    pub fn checkout(&self) -> io::Result<SendBuf> {
        let bid = self.rc.ring.rc.checkout_bid(self.rc.id)?;
        Ok(SendBuf::new(Rc::clone(&self.rc), bid))
    }

    /// Returns the total queued byte length across all committed send buffers in this batch.
    pub fn queued_len(&self) -> usize {
        self.rc.ring.rc.queued_len(self.rc.id)
    }

    /// Returns the number of committed send buffers in this batch.
    pub fn queued_buffers(&self) -> usize {
        self.rc.ring.rc.queued_buffers(self.rc.id)
    }

    /// Returns the number of available buffers left in the underlying send ring.
    pub fn available_buffers(&self) -> usize {
        self.rc.ring.available_buffers()
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.ring.bgid()
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.ring.same_driver(handle)
    }

    pub(crate) fn validate_send(&self) -> io::Result<()> {
        self.rc.ring.rc.validate_send(self.rc.id)
    }

    pub(crate) fn on_submit(&self) -> io::Result<()> {
        self.rc.on_submit()
    }

    pub(crate) fn on_submit_rollback(&self) {
        self.rc.on_submit_rollback();
    }

    pub(crate) fn complete_send(&self, result: crate::operation::CQEResult) -> io::Result<usize> {
        if !self.rc.submitted.get() {
            debug_assert!(!result.more());
            self.rc.ring.rc.abandon_batch(self.rc.id);
            return result.result.map(|bytes| bytes as usize);
        }
        let terminal = !result.more();
        let out = self.rc.ring.rc.complete_udp_send(self.rc.id, result);
        if terminal {
            self.rc.publish_start_tail.set(None);
            self.rc.submitted.set(false);
        }
        out
    }
}

/// [`BufRingBuf`] is a reference to a buffer in a buffer ring.
///
/// It is reference counted and will be returned to the buffer ring when dropped.
/// Users should be careful to drop the buffer as soon as possible to avoid
/// exhausting the buffer ring.
pub struct BufRingBuf {
    bufgroup: RecvBufRing,
    len: usize,
    bid: Bid,
}

/// [`SendBuf`] is a checked-out writable buffer from a [`SendBundleBatch`].
pub struct SendBuf {
    batch: Rc<InnerSendBundleBatch>,
    len: usize,
    bid: Bid,
    committed: bool,
}

/// [`BufRingBufBundle`] is a collection of one or more buffers selected from a buffer ring.
///
/// This is primarily used by recv bundle operations that may consume multiple provided buffers
/// for a single completion.
#[derive(Debug)]
pub struct BufRingBufBundle {
    bufs: Vec<BufRingBuf>,
    len: usize,
}

impl BufRingBufBundle {
    fn new(bufs: Vec<BufRingBuf>, len: usize) -> Self {
        Self { bufs, len }
    }

    /// Returns the total number of initialized bytes across all buffers in this bundle.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if this bundle contains no initialized bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the number of ring buffers contained in this bundle.
    pub fn buffer_count(&self) -> usize {
        self.bufs.len()
    }

    /// Returns an iterator over payload slices for each buffer in this bundle.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        self.bufs.iter().map(BufRingBuf::as_slice)
    }

    /// Consumes this bundle and returns the underlying ring buffers.
    pub fn into_bufs(self) -> Vec<BufRingBuf> {
        self.bufs
    }
}

impl fmt::Debug for SendBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SendBuf")
            .field("bgid", &self.batch.ring.bgid())
            .field("bid", &self.bid)
            .field("len", &self.len)
            .field("cap", &self.batch.ring.rc.buf_capacity())
            .field("committed", &self.committed)
            .finish()
    }
}

impl fmt::Debug for BufRingBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufRingBuf")
            .field("bgid", &self.bufgroup.rc.bgid())
            .field("bid", &self.bid)
            .field("len", &self.len)
            .field("cap", &self.bufgroup.rc.buf_capacity())
            .finish()
    }
}

impl BufRingBuf {
    fn new(bufgroup: RecvBufRing, bid: Bid, len: usize) -> Self {
        assert!(len <= bufgroup.rc.buf_len);

        Self { bufgroup, len, bid }
    }

    /// Return the number of bytes initialized in this buffer.
    ///
    /// This is the length reported by the kernel for the completed operation.
    pub fn len(&self) -> usize {
        self.len as _
    }

    /// Return `true` if this buffer contains no initialized bytes.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the total capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.bufgroup.rc.buf_capacity()
    }

    /// Return this buffer as a byte slice.
    pub fn as_slice(&self) -> &[u8] {
        let p = self.bufgroup.rc.stable_ptr(self.bid);
        unsafe { std::slice::from_raw_parts(p, self.len) }
    }
}

impl SendBuf {
    fn new(batch: Rc<InnerSendBundleBatch>, bid: Bid) -> Self {
        Self {
            batch,
            len: 0,
            bid,
            committed: false,
        }
    }

    /// Returns this buffer as a writable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        let p = self.batch.ring.rc.stable_ptr_mut(self.bid);
        unsafe { std::slice::from_raw_parts_mut(p, self.capacity()) }
    }

    /// Returns the total capacity of this buffer.
    pub fn capacity(&self) -> usize {
        self.batch.ring.rc.buf_capacity()
    }

    /// Returns the committed data length for this buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no bytes are currently marked for sending.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Sets the number of initialized bytes in this buffer.
    pub fn set_len(&mut self, len: usize) -> io::Result<()> {
        if len > self.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer length exceeds capacity",
            ));
        }
        self.len = len;
        Ok(())
    }

    /// Commits this buffer into the owning batch as one bundle segment.
    pub fn commit(mut self) -> io::Result<()> {
        self.batch.commit_segment(self.bid, self.len)?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for BufRingBuf {
    fn drop(&mut self) {
        // Add the buffer back to the bufgroup, for the kernel to reuse.
        unsafe { self.bufgroup.rc.dropping_bid(self.bid) };
    }
}

impl Drop for SendBuf {
    fn drop(&mut self) {
        if !self.committed {
            self.batch.ring.rc.release_checkout(self.bid);
        }
    }
}

/// Identifier for a registered buffer group.
pub type Bgid = u16;

/// Identifier for a buffer within a registered buffer group.
pub type Bid = u16;

fn selected_bid_from_flags(flags: u32) -> io::Result<Bid> {
    io_uring::cqueue::buffer_select(flags).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "completion did not include a selected buffer id",
        )
    })
}

/// [`Builder`] is used to create a new receive or send buffer ring.
#[derive(Clone, Debug)]
pub struct Builder<T> {
    bgid: Bgid,
    ring_entries: u16,
    buf_cnt: u16,
    buf_len: usize,
    target: PhantomData<fn() -> T>,
}

#[derive(Copy, Clone, Debug)]
struct NormalizedBuilder {
    bgid: Bgid,
    ring_entries: u16,
    buf_cnt: u16,
    buf_len: usize,
}

impl<T> Builder<T> {
    // Create a new Builder with the given buffer group ID and defaults.
    //
    // The buffer group ID, `bgid`, is the id the kernel uses to identify the buffer group to use
    // for a given read operation that has been placed into an sqe.
    //
    // The caller is responsible for picking a bgid that does not conflict with other buffer
    // groups that have been registered with the same uring interface.
    fn new(bgid: Bgid) -> Self {
        Builder {
            bgid,
            ring_entries: 128,
            buf_cnt: 0, // 0 indicates buf_cnt is taken from ring_entries
            buf_len: 4096,
            target: PhantomData,
        }
    }

    /// The number of ring entries to create for the buffer ring.
    ///
    /// The number will be made a power of 2, and will be the maximum of the ring_entries setting
    /// and the buf_cnt setting. The interface will enforce a maximum of 2^15 (32768).
    pub fn ring_entries(mut self, ring_entries: u16) -> Self {
        self.ring_entries = ring_entries;
        self
    }

    /// The number of buffers to allocate. If left zero, the ring_entries value will be used.
    pub fn buf_cnt(mut self, buf_cnt: u16) -> Self {
        self.buf_cnt = buf_cnt;
        self
    }

    /// The length to be preallocated for each buffer.
    pub fn buf_len(mut self, buf_len: usize) -> Self {
        self.buf_len = buf_len;
        self
    }

    fn normalized(&self) -> io::Result<NormalizedBuilder> {
        let mut b = NormalizedBuilder {
            bgid: self.bgid,
            ring_entries: self.ring_entries,
            buf_cnt: self.buf_cnt,
            buf_len: self.buf_len,
        };

        if b.buf_cnt == 0 || b.ring_entries < b.buf_cnt {
            let max = std::cmp::max(b.ring_entries, b.buf_cnt);
            b.buf_cnt = max;
            b.ring_entries = max;
        }

        if b.ring_entries > (1 << 15) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "ring_entries exceeded 32768",
            ));
        }

        b.ring_entries = b.ring_entries.next_power_of_two();
        Ok(b)
    }
}

impl Builder<RecvBufRing> {
    /// Return a RecvBufRing.
    pub fn build(&self) -> io::Result<RecvBufRing> {
        let b = self.normalized()?;

        let handle = crate::Handle::current();
        let inner =
            InnerBufRing::new(b.bgid, b.ring_entries, b.buf_cnt, b.buf_len, handle.clone())?;
        handle.with_submitter(|s| inner.register(s))?;
        Ok(RecvBufRing::new(inner))
    }
}

impl Builder<SendBufRing> {
    /// Return a send-side buffer ring used to stage outbound buffers for `SendBundle`.
    pub fn build(&self) -> io::Result<SendBufRing> {
        let b = self.normalized()?;

        let handle = crate::Handle::current();
        let inner =
            InnerSendBufRing::new(b.bgid, b.ring_entries, b.buf_cnt, b.buf_len, handle.clone())?;
        handle.with_submitter(|s| inner.register(s))?;
        Ok(SendBufRing::new(inner))
    }
}

struct InnerBufRing {
    handle: Handle,

    // True only after this instance has successfully registered its BGID with
    // the kernel. A failed candidate must never unregister another live ring
    // that happens to use the same BGID.
    registered: Cell<bool>,

    // All remaining fields are constant once the struct is instantiated except the Cell fields.
    bgid: Bgid,

    ring_entries_mask: u16, // Invariant one less than ring_entries which is > 0, power of 2, max 2^15 (32768).

    buf_cnt: u16,   // Invariants: > 0, <= ring_entries.
    buf_len: usize, // Invariant: > 0.

    // `ring_start` holds the memory allocated for the buf_ring, the ring of entries describing
    // the buffers being made available to the uring interface for this buf group id.
    ring_start: AnonymousMmap,

    buf_list: Vec<Vec<u8>>,

    // `local_tail` is the copy of the tail index that we update when a buffer is dropped and
    // therefore its buffer id is released and added back to the ring. It also serves for adding
    // buffers to the ring during init but that's not as interesting.
    local_tail: Cell<u16>,

    // `shared_tail` points to the u16 memory inside the rings that the uring interface uses as the
    // tail field. It is where the application writes new tail values and the kernel reads the tail
    // value from time to time. The address could be computed from ring_start when needed. This
    // might be here for no good reason any more.
    shared_tail: *const AtomicU16,

    // Cached consume head used for recv bundle operations. This tracks the next ring slot expected
    // to be consumed by bundle-aware receives.
    bundle_head: Cell<u16>,
}

struct InnerSendBufRing {
    handle: Handle,
    registered: Cell<bool>,
    bgid: Bgid,
    ring_entries_mask: u16,
    buf_cnt: u16,
    buf_len: usize,
    ring_start: AnonymousMmap,
    // Each BID is handed out exclusively by `SendQueueState`. The cell makes that
    // per-BID interior mutability explicit while the ring is shared through `Rc`.
    buf_list: Vec<UnsafeCell<Vec<u8>>>,
    local_tail: Cell<u16>,
    shared_tail: *const AtomicU16,
    state: RefCell<SendQueueState>,
}

#[derive(Debug)]
struct InnerSendBundleBatch {
    ring: SendBufRing,
    id: u64,
    segment_limit: Option<usize>,
    submitted: Cell<bool>,
    publish_start_tail: Cell<Option<u16>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QueuedDatagram {
    bid: Bid,
    len: usize,
}

#[derive(Debug)]
struct SendQueueState {
    free_bids: Vec<Bid>,
    queued: VecDeque<QueuedDatagram>,
    next_batch_id: u64,
    active_batch: Option<u64>,
    inflight: bool,
    poisoned: bool,
}

impl InnerBufRing {
    fn new(
        bgid: Bgid,
        ring_entries: u16,
        buf_cnt: u16,
        buf_len: usize,
        handle: Handle,
    ) -> io::Result<InnerBufRing> {
        // Check that none of the important args are zero and the ring_entries is at least large
        // enough to hold all the buffers and that ring_entries is a power of 2.
        if (buf_cnt == 0)
            || (buf_cnt > ring_entries)
            || (buf_len == 0)
            || ((ring_entries & (ring_entries - 1)) != 0)
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        // entry_size is 16 bytes.
        let entry_size = std::mem::size_of::<BufRingEntry>();
        assert_eq!(entry_size, 16);
        let ring_size = entry_size * (ring_entries as usize);

        // The memory is required to be page aligned and zero-filled by the uring buf_ring
        // interface. Anonymous mmap promises both of those things.
        // https://man7.org/linux/man-pages/man2/mmap.2.html
        let ring_start = AnonymousMmap::new(ring_size)?;

        // Probably some functional way to do this.
        let buf_list: Vec<Vec<u8>> = {
            let mut bp = Vec::with_capacity(buf_cnt as _);
            for _ in 0..buf_cnt {
                bp.push(vec![0; buf_len]);
            }
            bp
        };

        let shared_tail =
            unsafe { types::BufRingEntry::tail(ring_start.as_ptr() as *const BufRingEntry) }
                as *const AtomicU16;

        let ring_entries_mask = ring_entries - 1;
        assert!((ring_entries & ring_entries_mask) == 0);

        let buf_ring = InnerBufRing {
            handle,
            registered: Cell::new(false),
            bgid,
            ring_entries_mask,
            buf_cnt,
            buf_len,
            ring_start,
            buf_list,
            local_tail: Cell::new(0),
            shared_tail,
            bundle_head: Cell::new(0),
        };

        Ok(buf_ring)
    }

    fn register(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        let bgid = self.bgid;

        let res = unsafe {
            submitter.register_buf_ring_with_flags(
                self.ring_start.as_ptr() as _,
                self.ring_entries(),
                bgid,
                0,
            )
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    // using buf_ring requires kernel 5.19 or greater.
                    return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("buf_ring.register returned {}, most likely indicating this kernel is not 5.19+", e),
                            ));
                }
                Some(libc::EEXIST) => {
                    // Registering a duplicate bgid is not allowed. There is an `unregister`
                    // operations that can remove the first, but care must be taken that there
                    // are no outstanding operations that will still return a buffer from that
                    // one.
                    return Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!(
                                "buf_ring.register returned `{}`, indicating the attempted buffer group id {} was already registered",
                            e,
                            bgid),
                        ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("buf_ring.register returned `{}` for group id {}", e, bgid),
                    ));
                }
            }
        };

        // From this point on, Drop owns the matching unregister operation. Set
        // the state before initializing userspace entries so unwinding cannot
        // leave a successful kernel registration behind.
        self.registered.set(true);

        for bid in 0..self.buf_cnt {
            self.buf_ring_push(bid);
        }
        self.buf_ring_sync();

        res
    }

    fn unregister(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        let bgid = self.bgid;
        submitter.unregister_buf_ring(bgid)
    }

    // Safety: dropping a duplicate bid is likely to cause undefined behavior
    // as the kernel could use the same buffer for different data concurrently.
    unsafe fn dropping_bid(&self, bid: Bid) {
        self.buf_ring_push(bid);
        self.buf_ring_sync();
    }

    // Returns the buffer group id.
    fn bgid(&self) -> Bgid {
        self.bgid
    }

    // Returns the buffer the uring interface picked from the buf_ring for the completion result
    // represented by the res and flags.
    fn get_buf(&self, buf_ring: RecvBufRing, res: u32, flags: u32) -> io::Result<BufRingBuf> {
        // This fn does the odd thing of having self as the RecvBufRing and taking an argument that
        // is the same RecvBufRing but wrapped in Rc<_> so the wrapped buf_ring can be passed to the
        // outgoing GBuf.
        let bid = selected_bid_from_flags(flags)?;
        if bid >= self.buf_cnt {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "completion selected buffer id {} outside ring bounds (buf_cnt={})",
                    bid, self.buf_cnt
                ),
            ));
        }

        let len = res as usize;

        assert!(len <= self.buf_len);

        // Best effort: keep bundle head in sync when single-buffer CQEs arrive in-order.
        let expected = self.bid_at_ring_index(self.bundle_head.get());
        if expected == bid {
            self.bundle_head.set(self.bundle_head.get().wrapping_add(1));
        }

        Ok(BufRingBuf::new(buf_ring, bid, len))
    }

    fn get_buf_bundle(
        &self,
        buf_ring: RecvBufRing,
        res: u32,
        flags: u32,
    ) -> io::Result<BufRingBufBundle> {
        let total_len = res as usize;
        let Some(first_bid) = io_uring::cqueue::buffer_select(flags) else {
            if total_len == 0 {
                return Ok(BufRingBufBundle::new(Vec::new(), 0));
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "bundle completion did not include a selected buffer id",
            ));
        };
        if first_bid >= self.buf_cnt {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bundle completion selected buffer id {} outside ring bounds (buf_cnt={})",
                    first_bid, self.buf_cnt
                ),
            ));
        }

        let needed = if total_len == 0 {
            1
        } else {
            total_len.div_ceil(self.buf_len)
        };
        if needed > usize::from(self.buf_cnt) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bundle completion requires {} buffers but ring only has {}",
                    needed, self.buf_cnt
                ),
            ));
        }

        let head = self.bundle_head.get();
        let head_bid = self.bid_at_ring_index(head);
        if head_bid != first_bid {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "bundle completion selected bid {} but bundle head expected bid {}",
                    first_bid, head_bid
                ),
            ));
        }

        let mut bufs = Vec::with_capacity(needed);
        let mut remaining = total_len;
        for i in 0..needed {
            let ring_index = head.wrapping_add(i as u16);
            let bid = self.bid_at_ring_index(ring_index);
            if bid >= self.buf_cnt {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "bundle completion consumed invalid bid {} (buf_cnt={})",
                        bid, self.buf_cnt
                    ),
                ));
            }
            let len = if i + 1 == needed {
                remaining
            } else {
                self.buf_len
            };
            bufs.push(BufRingBuf::new(buf_ring.clone(), bid, len));
            remaining = remaining.saturating_sub(len);
        }

        self.bundle_head.set(head.wrapping_add(needed as u16));
        Ok(BufRingBufBundle::new(bufs, total_len))
    }

    fn buf_capacity(&self) -> usize {
        self.buf_len as _
    }

    fn stable_ptr(&self, bid: Bid) -> *const u8 {
        self.buf_list[bid as usize].as_ptr()
    }

    fn bid_at_ring_index(&self, index: u16) -> Bid {
        let idx = index & self.mask();
        let entries = self.ring_start.as_ptr() as *const BufRingEntry;
        unsafe { (*entries.add(idx as usize)).bid() }
    }

    fn ring_entries(&self) -> u16 {
        self.ring_entries_mask + 1
    }

    fn mask(&self) -> u16 {
        self.ring_entries_mask
    }

    // Push the `bid` buffer to the buf_ring tail.
    // This test version does not safeguard against a duplicate
    // `bid` being pushed.
    fn buf_ring_push(&self, bid: Bid) {
        assert!(bid < self.buf_cnt);

        // N.B. The uring buf_ring indexing mechanism calls for the tail values to exceed the
        // actual number of ring entries. This allows the uring interface to distinguish between
        // empty and full buf_rings. As a result, the ring mask is only applied to the index used
        // for computing the ring entry, not to the tail value itself.

        let old_tail = self.local_tail.get();
        self.local_tail.set(old_tail.wrapping_add(1));
        let ring_idx = old_tail & self.mask();

        let entries = self.ring_start.as_ptr_mut() as *mut BufRingEntry;
        let re = unsafe { &mut *entries.add(ring_idx as usize) };

        re.set_addr(self.stable_ptr(bid) as _);
        re.set_len(self.buf_len as _);
        re.set_bid(bid);

        // Also note, we have not updated the tail as far as the kernel is concerned.
        // That is done with buf_ring_sync.
    }

    // Make 'local_tail' visible to the kernel. Called after buf_ring_push() has been
    // called to fill in new buffers.
    fn buf_ring_sync(&self) {
        unsafe {
            (*self.shared_tail).store(self.local_tail.get(), atomic::Ordering::Release);
        }
    }
}

impl SendQueueState {
    fn new(buf_cnt: u16) -> Self {
        let mut free_bids = Vec::with_capacity(buf_cnt as usize);
        for bid in (0..buf_cnt).rev() {
            free_bids.push(bid);
        }
        Self {
            free_bids,
            queued: VecDeque::new(),
            next_batch_id: 0,
            active_batch: None,
            inflight: false,
            poisoned: false,
        }
    }

    fn ensure_not_poisoned(&self) -> io::Result<()> {
        if self.poisoned {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "send buffer ring is poisoned",
            ))
        } else {
            Ok(())
        }
    }

    fn poison<T>(&mut self, message: impl Into<String>) -> io::Result<T> {
        self.poisoned = true;
        Err(io::Error::new(io::ErrorKind::InvalidData, message.into()))
    }

    fn ensure_active_batch(&self, batch_id: u64) -> io::Result<()> {
        if self.active_batch == Some(batch_id) {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer belongs to an inactive bundle batch",
            ))
        }
    }

    fn checkout_bid(&mut self, batch_id: u64) -> io::Result<Bid> {
        self.ensure_not_poisoned()?;
        self.ensure_active_batch(batch_id)?;
        self.free_bids.pop().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring has no available buffers",
            )
        })
    }

    fn release_checkout(&mut self, bid: Bid) {
        self.free_bids.push(bid);
    }

    fn begin_batch(&mut self) -> io::Result<u64> {
        self.ensure_not_poisoned()?;
        if self.active_batch.is_some() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring already has an active bundle batch",
            ));
        }
        let batch_id = self.next_batch_id;
        self.next_batch_id = self.next_batch_id.wrapping_add(1);
        self.active_batch = Some(batch_id);
        Ok(batch_id)
    }

    fn abandon_batch(&mut self, batch_id: u64) {
        if self.active_batch != Some(batch_id) {
            return;
        }
        self.inflight = false;
        self.active_batch = None;
        while let Some(segment) = self.queued.pop_front() {
            self.free_bids.push(segment.bid);
        }
    }

    fn commit_datagram(&mut self, batch_id: u64, bid: Bid, len: usize) -> io::Result<()> {
        self.ensure_not_poisoned()?;
        self.ensure_active_batch(batch_id)?;
        if self.inflight {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send bundle batch is already in flight",
            ));
        }
        if len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send bundle segments must contain at least one byte",
            ));
        }
        self.queued.push_back(QueuedDatagram { bid, len });
        Ok(())
    }

    fn queued_len(&self, batch_id: u64) -> usize {
        if self.active_batch == Some(batch_id) {
            self.queued.iter().map(|dgram| dgram.len).sum()
        } else {
            0
        }
    }

    fn queued_buffers(&self, batch_id: u64) -> usize {
        if self.active_batch == Some(batch_id) {
            self.queued.len()
        } else {
            0
        }
    }

    fn available_buffers(&self) -> usize {
        self.free_bids.len()
    }

    fn validate_send(&self, batch_id: u64) -> io::Result<()> {
        self.ensure_not_poisoned()?;
        self.ensure_active_batch(batch_id)?;
        if self.inflight {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send bundle already in flight for this ring",
            ));
        }
        if self.queued.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send bundle requires at least one committed buffer",
            ));
        }
        Ok(())
    }

    fn mark_submitted(&mut self, batch_id: u64) -> io::Result<()> {
        self.validate_send(batch_id)?;
        self.inflight = true;
        Ok(())
    }

    fn rollback_submit(&mut self, batch_id: u64) {
        if self.active_batch == Some(batch_id) {
            self.inflight = false;
        }
    }

    fn complete_udp_send(
        &mut self,
        batch_id: u64,
        result: crate::operation::CQEResult,
    ) -> io::Result<usize> {
        self.ensure_active_batch(batch_id)?;
        let more = result.more();
        let out = if self.poisoned {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "send buffer ring is poisoned",
            ))
        } else {
            match result.result {
                Ok(bytes) => self.reconcile_udp_send(bytes as usize, result.flags, more),
                Err(err) => {
                    // A failed kernel send may leave some or all published entries available in
                    // the kernel-side ring. Their ownership cannot be reconstructed from an error
                    // CQE, so prevent any later batch from mutating or republishing those buffers.
                    self.poisoned = true;
                    Err(err)
                }
            }
        };
        if out.is_err() {
            // Any completion we cannot reconcile leaves published entries with unknown kernel
            // ownership. Keep the original error, but make the ring permanently unavailable.
            self.poisoned = true;
        }
        if !more {
            self.inflight = false;
            self.active_batch = None;
        }
        out
    }

    fn reconcile_udp_send(&mut self, bytes: usize, flags: u32, more: bool) -> io::Result<usize> {
        let start_bid = selected_bid_from_flags(flags)?;
        let Some(front) = self.queued.front() else {
            return self.poison("send bundle completion arrived with no queued segments");
        };
        if front.bid != start_bid {
            return self.poison(format!(
                "send bundle completion selected bid {} but queue expected bid {}",
                start_bid, front.bid
            ));
        }

        // Validate the entire completion before releasing any BID. If the kernel reports an
        // impossible boundary, none of the potentially affected buffers become reusable.
        let mut remaining = bytes;
        let mut consumed = 0;
        for segment in &self.queued {
            if remaining == 0 {
                break;
            }
            if remaining < segment.len {
                return self.poison(format!(
                    "send bundle completion stopped mid-segment (remaining={} segment_len={})",
                    remaining, segment.len
                ));
            }
            remaining -= segment.len;
            consumed += 1;
        }
        if remaining != 0 {
            return self.poison(format!(
                "send bundle completion consumed {} bytes beyond queued segments",
                remaining
            ));
        }
        if consumed == 0 {
            return self.poison("send bundle completion consumed no queued segments");
        }

        let queued_after = self.queued.len() - consumed;
        if more && queued_after == 0 {
            return self.poison(
                "send bundle completion promised another CQE after consuming the active batch",
            );
        }
        if !more && queued_after != 0 {
            return self.poison(
                "terminal send bundle completion left queued segments in the active batch",
            );
        }

        for _ in 0..consumed {
            let segment = self
                .queued
                .pop_front()
                .expect("validated send bundle segment missing");
            self.free_bids.push(segment.bid);
        }

        Ok(bytes)
    }
}

impl InnerSendBufRing {
    fn new(
        bgid: Bgid,
        ring_entries: u16,
        buf_cnt: u16,
        buf_len: usize,
        handle: Handle,
    ) -> io::Result<Self> {
        if (buf_cnt == 0)
            || (buf_cnt > ring_entries)
            || (buf_len == 0)
            || ((ring_entries & (ring_entries - 1)) != 0)
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let entry_size = std::mem::size_of::<BufRingEntry>();
        assert_eq!(entry_size, 16);
        let ring_size = entry_size * (ring_entries as usize);
        let ring_start = AnonymousMmap::new(ring_size)?;

        let mut buf_list = Vec::with_capacity(buf_cnt as usize);
        for _ in 0..buf_cnt {
            buf_list.push(UnsafeCell::new(vec![0; buf_len]));
        }

        let shared_tail =
            unsafe { types::BufRingEntry::tail(ring_start.as_ptr() as *const BufRingEntry) }
                as *const AtomicU16;
        let ring_entries_mask = ring_entries - 1;

        Ok(Self {
            handle,
            registered: Cell::new(false),
            bgid,
            ring_entries_mask,
            buf_cnt,
            buf_len,
            ring_start,
            buf_list,
            local_tail: Cell::new(0),
            shared_tail,
            state: RefCell::new(SendQueueState::new(buf_cnt)),
        })
    }

    fn register(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        let bgid = self.bgid;

        let res = unsafe {
            submitter.register_buf_ring_with_flags(
                self.ring_start.as_ptr() as _,
                self.ring_entries(),
                bgid,
                0,
            )
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    return Err(io::Error::other(format!(
                        "buf_ring.register returned {}, most likely indicating this kernel is not 5.19+",
                        e
                    )));
                }
                Some(libc::EEXIST) => {
                    return Err(io::Error::other(format!(
                        "buf_ring.register returned `{}`, indicating the attempted buffer group id {} was already registered",
                        e, bgid
                    )));
                }
                _ => {
                    return Err(io::Error::other(format!(
                        "buf_ring.register returned `{}` for group id {}",
                        e, bgid
                    )));
                }
            }
        }

        self.registered.set(true);
        res
    }

    fn unregister(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        submitter.unregister_buf_ring(self.bgid)
    }

    fn bgid(&self) -> Bgid {
        self.bgid
    }

    fn begin_batch(&self) -> io::Result<u64> {
        self.state.borrow_mut().begin_batch()
    }

    fn abandon_batch(&self, batch_id: u64) {
        self.state.borrow_mut().abandon_batch(batch_id);
    }

    fn checkout_bid(&self, batch_id: u64) -> io::Result<Bid> {
        self.state.borrow_mut().checkout_bid(batch_id)
    }

    fn release_checkout(&self, bid: Bid) {
        self.state.borrow_mut().release_checkout(bid);
    }

    fn commit_bid(&self, batch_id: u64, bid: Bid, len: usize) -> io::Result<()> {
        if bid >= self.buf_cnt {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer bid exceeds ring bounds",
            ));
        }
        if len > self.buf_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send buffer length exceeds ring capacity",
            ));
        }
        self.state
            .borrow_mut()
            .commit_datagram(batch_id, bid, len)?;
        Ok(())
    }

    fn queued_len(&self, batch_id: u64) -> usize {
        self.state.borrow().queued_len(batch_id)
    }

    fn queued_buffers(&self, batch_id: u64) -> usize {
        self.state.borrow().queued_buffers(batch_id)
    }

    fn available_buffers(&self) -> usize {
        self.state.borrow().available_buffers()
    }

    fn validate_send(&self, batch_id: u64) -> io::Result<()> {
        self.state.borrow().validate_send(batch_id)
    }

    fn publish_for_send(&self, batch_id: u64) -> io::Result<()> {
        {
            let mut state = self.state.borrow_mut();
            state.mark_submitted(batch_id)?;
            for segment in state.queued.iter().copied() {
                self.buf_ring_push_with_len(segment.bid, segment.len);
            }
        }
        self.buf_ring_sync();
        Ok(())
    }

    fn rollback_publish(&self, batch_id: u64, start_tail: u16) {
        // The driver invokes this only while the matching SQE is still hidden behind the SQ tail.
        // A send ring has one active batch, so no kernel request can have consumed these entries.
        self.local_tail.set(start_tail);
        unsafe {
            (*self.shared_tail).store(start_tail, atomic::Ordering::Release);
        }
        self.state.borrow_mut().rollback_submit(batch_id);
    }

    fn complete_udp_send(
        &self,
        batch_id: u64,
        result: crate::operation::CQEResult,
    ) -> io::Result<usize> {
        self.state.borrow_mut().complete_udp_send(batch_id, result)
    }

    fn buf_capacity(&self) -> usize {
        self.buf_len
    }

    fn stable_ptr_mut(&self, bid: Bid) -> *mut u8 {
        // Safety: `SendQueueState` removes a BID from `free_bids` before creating
        // its sole, non-cloneable `SendBuf`. Committing consumes that `SendBuf`,
        // and the BID is not made free again until the kernel completion is
        // reconciled. The vectors are never resized after construction, so their
        // allocations remain stable for registration and in-flight I/O.
        unsafe { (*self.buf_list[bid as usize].get()).as_mut_ptr() }
    }

    fn ring_entries(&self) -> u16 {
        self.ring_entries_mask + 1
    }

    fn mask(&self) -> u16 {
        self.ring_entries_mask
    }

    fn buf_ring_push_with_len(&self, bid: Bid, len: usize) {
        assert!(bid < self.buf_cnt);
        assert!(len <= self.buf_len);

        let old_tail = self.local_tail.get();
        self.local_tail.set(old_tail.wrapping_add(1));
        let ring_idx = old_tail & self.mask();
        let entries = self.ring_start.as_ptr_mut() as *mut BufRingEntry;
        let re = unsafe { &mut *entries.add(ring_idx as usize) };

        re.set_addr(self.stable_ptr_mut(bid) as _);
        re.set_len(len as _);
        re.set_bid(bid);
    }

    fn buf_ring_sync(&self) {
        unsafe {
            (*self.shared_tail).store(self.local_tail.get(), atomic::Ordering::Release);
        }
    }
}

impl InnerSendBundleBatch {
    fn new(ring: SendBufRing, id: u64, segment_limit: Option<usize>) -> Self {
        Self {
            ring,
            id,
            segment_limit,
            submitted: Cell::new(false),
            publish_start_tail: Cell::new(None),
        }
    }

    fn commit_segment(&self, bid: Bid, len: usize) -> io::Result<()> {
        if let Some(limit) = self.segment_limit {
            if self.ring.rc.queued_buffers(self.id) >= limit {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("UDP send bundle batches support at most {limit} committed buffers"),
                ));
            }
        }
        self.ring.rc.commit_bid(self.id, bid, len)
    }

    fn on_submit(&self) -> io::Result<()> {
        if self.submitted.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "send bundle submission hook ran more than once",
            ));
        }
        let start_tail = self.ring.rc.local_tail.get();
        self.publish_start_tail.set(Some(start_tail));
        self.ring.rc.publish_for_send(self.id)?;
        self.submitted.set(true);
        Ok(())
    }

    fn on_submit_rollback(&self) {
        let Some(start_tail) = self.publish_start_tail.take() else {
            return;
        };
        self.ring.rc.rollback_publish(self.id, start_tail);
        self.submitted.set(false);
    }
}

impl Drop for InnerSendBundleBatch {
    fn drop(&mut self) {
        self.ring.rc.abandon_batch(self.id);
    }
}

impl Drop for InnerBufRing {
    fn drop(&mut self) {
        if !self.registered.replace(false) {
            return;
        }

        // Best-effort unregister on drop. If this fails we prefer logging over panicking
        // during teardown; the process can still exit safely.
        if let Err(err) = self.handle.with_submitter(|s| self.unregister(s)) {
            warn!(target: "norn_uring::bufring", "unregister.failed: {}", err);
        }
    }
}

impl Drop for InnerSendBufRing {
    fn drop(&mut self) {
        if !self.registered.replace(false) {
            return;
        }
        if let Err(err) = self.handle.with_submitter(|s| self.unregister(s)) {
            warn!(target: "norn_uring::bufring", "send.unregister.failed: {}", err);
        }
    }
}

/// An anonymous region of memory mapped using `mmap(2)`, not backed by a file
/// but that is guaranteed to be page-aligned and zero-filled.
struct AnonymousMmap {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

impl AnonymousMmap {
    /// Creates a new anonymous mapping of `len` bytes.
    fn new(len: usize) -> io::Result<Self> {
        Self::new_with_madvise(len, |addr, len| {
            match unsafe { libc::madvise(addr.as_ptr(), len, libc::MADV_DONTFORK) } {
                0 => Ok(()),
                _ => Err(io::Error::last_os_error()),
            }
        })
    }

    fn new_with_madvise(
        len: usize,
        madvise: impl FnOnce(ptr::NonNull<libc::c_void>, usize) -> io::Result<()>,
    ) -> io::Result<Self> {
        let addr = unsafe {
            match libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_POPULATE,
                0,
                0,
            ) {
                libc::MAP_FAILED => return Err(io::Error::last_os_error()),
                addr => ptr::NonNull::new_unchecked(addr),
            }
        };
        let mmap = Self { addr, len };
        madvise(mmap.addr, mmap.len)?;
        Ok(mmap)
    }

    /// Get a pointer to the memory.
    #[inline]
    fn as_ptr(&self) -> *const libc::c_void {
        self.addr.as_ptr()
    }

    /// Get a mut pointer to the memory.
    #[inline]
    fn as_ptr_mut(&self) -> *mut libc::c_void {
        self.addr.as_ptr()
    }
}

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}

impl ops::Deref for BufRingBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        BufRingBuf::as_slice(self)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        selected_bid_from_flags, AnonymousMmap, InnerSendBufRing, SendBufRing, SendQueueState,
    };
    use std::cell::Cell;
    use std::io;
    use std::ptr;
    use std::sync::atomic;

    fn selected_bid_flag(bid: u16) -> u32 {
        const IORING_CQE_F_BUFFER: u32 = 1;
        const IORING_CQE_BUFFER_SHIFT: u32 = 16;

        IORING_CQE_F_BUFFER | ((bid as u32) << IORING_CQE_BUFFER_SHIFT)
    }

    fn more_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::more(*flags))
            .expect("missing CQE more flag")
    }

    #[test]
    fn selected_bid_requires_buffer_select_flag() {
        let err = selected_bid_from_flags(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn anonymous_mmap_is_unmapped_when_madvise_fails() {
        let len = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mapped_addr = Cell::new(ptr::null_mut());
        let result = AnonymousMmap::new_with_madvise(len, |addr, len| {
            mapped_addr.set(addr.as_ptr());

            let mut residency = 0;
            let result = unsafe { libc::mincore(addr.as_ptr(), len, &mut residency) };
            assert_eq!(result, 0, "mapping must be live while madvise runs");

            Err(io::Error::from_raw_os_error(libc::EINVAL))
        });
        let Err(err) = result else {
            panic!("injected madvise failure unexpectedly succeeded");
        };
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));

        let mut residency = 0;
        let result = unsafe { libc::mincore(mapped_addr.get(), len, &mut residency) };
        assert_eq!(result, -1);
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ENOMEM)
        );
    }

    #[test]
    fn send_ring_checkout_commit_updates_counts() {
        let mut state = SendQueueState::new(2);
        let batch = state.begin_batch().unwrap();
        let bid = state.checkout_bid(batch).unwrap();
        assert_eq!(bid, 0);
        state.commit_datagram(batch, bid, 4).unwrap();

        assert_eq!(state.available_buffers(), 1);
        assert_eq!(state.queued_buffers(batch), 1);
        assert_eq!(state.queued_len(batch), 4);
    }

    #[test]
    fn send_buf_checkout_preserves_exclusive_bid_storage() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let ring = SendBufRing::new(InnerSendBufRing::new(32, 2, 2, 64, driver.handle()).unwrap());
        let batch = ring.batch().unwrap();

        let mut committed = batch.checkout().unwrap();
        let committed_ptr = committed.as_mut_slice().as_mut_ptr();
        committed.set_len(1).unwrap();
        committed.commit().unwrap();

        let mut checkout = batch.checkout().unwrap();
        let checkout_ptr = checkout.as_mut_slice().as_mut_ptr();
        assert_ne!(checkout_ptr, committed_ptr);
        assert_eq!(
            batch.checkout().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        drop(checkout);
        let mut replacement = batch.checkout().unwrap();
        assert_eq!(replacement.as_mut_slice().as_mut_ptr(), checkout_ptr);
        assert_ne!(replacement.as_mut_slice().as_mut_ptr(), committed_ptr);
    }

    #[test]
    fn udp_send_bundle_full_consumes_all_segments() {
        let mut state = SendQueueState::new(3);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        let second = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 3).unwrap();
        state.commit_datagram(batch, second, 5).unwrap();
        state.validate_send(batch).unwrap();
        state.mark_submitted(batch).unwrap();

        let sent = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(8), selected_bid_flag(first)),
            )
            .unwrap();
        assert_eq!(sent, 8);
        assert!(state.queued.is_empty());
        assert_eq!(state.available_buffers(), 3);
        assert!(!state.inflight);
        assert!(state.active_batch.is_none());
    }

    #[test]
    fn udp_send_bundle_more_reconciles_prefix_but_keeps_batch_inflight() {
        let mut state = SendQueueState::new(3);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        let second = state.checkout_bid(batch).unwrap();
        let third = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 3).unwrap();
        state.commit_datagram(batch, second, 5).unwrap();
        state.commit_datagram(batch, third, 7).unwrap();
        state.mark_submitted(batch).unwrap();

        let first_cqe = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(8), selected_bid_flag(first) | more_flag()),
            )
            .unwrap();

        assert_eq!(first_cqe, 8);
        assert_eq!(state.queued_buffers(batch), 1);
        assert_eq!(state.queued_len(batch), 7);
        assert_eq!(state.available_buffers(), 2);
        assert!(state.inflight);
        assert_eq!(state.active_batch, Some(batch));
        assert_eq!(
            state.begin_batch().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );

        let terminal_cqe = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(7), selected_bid_flag(third)),
            )
            .unwrap();

        assert_eq!(terminal_cqe, 7);
        assert_eq!(state.available_buffers(), 3);
        assert!(!state.inflight);
        assert!(state.active_batch.is_none());
    }

    #[test]
    fn udp_send_bundle_more_error_keeps_ownership_until_terminal_cqe() {
        let mut state = SendQueueState::new(2);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        let second = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 3).unwrap();
        state.commit_datagram(batch, second, 5).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(3), selected_bid_flag(second) | more_flag()),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
        assert!(state.inflight);
        assert_eq!(state.active_batch, Some(batch));
        assert_eq!(state.available_buffers(), 0);

        let terminal_err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(5), selected_bid_flag(second)),
            )
            .unwrap_err();
        assert_eq!(terminal_err.kind(), io::ErrorKind::InvalidData);
        assert!(!state.inflight);
        assert!(state.active_batch.is_none());
        assert_eq!(state.available_buffers(), 0);
    }

    #[test]
    fn send_bundle_batch_keeps_submission_state_until_terminal_cqe() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let ring = SendBufRing::new(InnerSendBufRing::new(37, 2, 2, 64, driver.handle()).unwrap());
        let batch = ring.batch().unwrap();
        let mut first = batch.checkout().unwrap();
        first.set_len(3).unwrap();
        first.commit().unwrap();
        let mut second = batch.checkout().unwrap();
        second.set_len(5).unwrap();
        second.commit().unwrap();
        let [first_bid, second_bid] = {
            let state = ring.rc.state.borrow();
            [state.queued[0].bid, state.queued[1].bid]
        };

        batch.on_submit().unwrap();
        batch
            .complete_send(crate::operation::CQEResult::new(
                Ok(3),
                selected_bid_flag(first_bid) | more_flag(),
            ))
            .unwrap();

        assert!(batch.rc.submitted.get());
        assert!(batch.rc.publish_start_tail.get().is_some());
        assert_eq!(batch.queued_buffers(), 1);
        assert_eq!(ring.available_buffers(), 1);

        batch
            .complete_send(crate::operation::CQEResult::new(
                Ok(5),
                selected_bid_flag(second_bid),
            ))
            .unwrap();

        assert!(!batch.rc.submitted.get());
        assert!(batch.rc.publish_start_tail.get().is_none());
        assert_eq!(ring.available_buffers(), 2);
    }

    #[test]
    fn udp_send_bundle_short_completion_poisons_ring() {
        let mut state = SendQueueState::new(3);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        let second = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 3).unwrap();
        state.commit_datagram(batch, second, 5).unwrap();
        state.validate_send(batch).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(3), selected_bid_flag(first)),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(state.queued.len(), 2);
        assert_eq!(state.available_buffers(), 1);
        assert!(state.poisoned);
    }

    #[test]
    fn udp_send_bundle_mid_segment_poisons_ring() {
        let mut state = SendQueueState::new(2);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 4).unwrap();
        state.validate_send(batch).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(2), selected_bid_flag(first)),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
    }

    #[test]
    fn send_ring_selected_bid_mismatch_poisons_ring() {
        let mut state = SendQueueState::new(3);
        let batch = state.begin_batch().unwrap();
        let first = state.checkout_bid(batch).unwrap();
        let second = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, first, 4).unwrap();
        state.commit_datagram(batch, second, 4).unwrap();
        state.validate_send(batch).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(Ok(4), selected_bid_flag(second)),
            )
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
    }

    #[test]
    fn abandon_batch_releases_buffers_and_clears_flags() {
        let mut state = SendQueueState::new(2);
        let batch = state.begin_batch().unwrap();
        let bid = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, bid, 4).unwrap();
        state.validate_send(batch).unwrap();
        state.mark_submitted(batch).unwrap();

        state.abandon_batch(batch);

        assert!(!state.inflight);
        assert!(state.active_batch.is_none());
        assert_eq!(state.queued_buffers(batch), 0);
        assert_eq!(state.available_buffers(), 2);
        state.begin_batch().unwrap();
    }

    #[test]
    fn begin_batch_rejects_second_active_batch() {
        let mut state = SendQueueState::new(2);
        state.begin_batch().unwrap();

        let err = state.begin_batch().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);
    }

    #[test]
    fn stale_batch_cannot_commit_into_next_batch() {
        let mut state = SendQueueState::new(2);
        let first_batch = state.begin_batch().unwrap();
        let stale_bid = state.checkout_bid(first_batch).unwrap();
        let sent_bid = state.checkout_bid(first_batch).unwrap();
        state.commit_datagram(first_batch, sent_bid, 4).unwrap();
        state.mark_submitted(first_batch).unwrap();
        state
            .complete_udp_send(
                first_batch,
                crate::operation::CQEResult::new(Ok(4), selected_bid_flag(sent_bid)),
            )
            .unwrap();

        let second_batch = state.begin_batch().unwrap();
        let err = state
            .commit_datagram(first_batch, stale_bid, 4)
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(state.active_batch, Some(second_batch));
        assert_eq!(state.queued_buffers(second_batch), 0);
        state.release_checkout(stale_bid);
    }

    #[test]
    fn stale_batch_teardown_does_not_abandon_next_batch() {
        let mut state = SendQueueState::new(3);
        let first_batch = state.begin_batch().unwrap();
        let stale_bid = state.checkout_bid(first_batch).unwrap();
        let sent_bid = state.checkout_bid(first_batch).unwrap();
        state.commit_datagram(first_batch, sent_bid, 4).unwrap();
        state.mark_submitted(first_batch).unwrap();
        state
            .complete_udp_send(
                first_batch,
                crate::operation::CQEResult::new(Ok(4), selected_bid_flag(sent_bid)),
            )
            .unwrap();

        let second_batch = state.begin_batch().unwrap();
        let second_bid = state.checkout_bid(second_batch).unwrap();
        state.commit_datagram(second_batch, second_bid, 5).unwrap();

        state.abandon_batch(first_batch);

        assert_eq!(state.active_batch, Some(second_batch));
        assert_eq!(state.queued_buffers(second_batch), 1);
        assert_eq!(state.queued_len(second_batch), 5);
        state.validate_send(second_batch).unwrap();
        state.release_checkout(stale_bid);
    }

    #[test]
    fn kernel_error_poisons_published_send_ring() {
        let mut state = SendQueueState::new(1);
        let batch = state.begin_batch().unwrap();
        let bid = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, bid, 4).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(
                batch,
                crate::operation::CQEResult::new(
                    Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                    0,
                ),
            )
            .unwrap_err();

        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
        assert!(state.poisoned);
        assert!(state.active_batch.is_none());
        assert!(state.begin_batch().is_err());
    }

    #[test]
    fn submit_hook_rollback_restores_send_ring_publication() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let ring = SendBufRing::new(InnerSendBufRing::new(33, 2, 2, 64, driver.handle()).unwrap());
        let batch = ring.batch().unwrap();
        let mut buf = batch.checkout().unwrap();
        buf.as_mut_slice()[..4].copy_from_slice(b"test");
        buf.set_len(4).unwrap();
        buf.commit().unwrap();
        let start_tail = ring.rc.local_tail.get();

        batch.on_submit().unwrap();
        assert!(batch.rc.submitted.get());
        assert_eq!(ring.rc.local_tail.get(), start_tail.wrapping_add(1));
        assert_eq!(
            unsafe { (*ring.rc.shared_tail).load(atomic::Ordering::Acquire) },
            start_tail.wrapping_add(1)
        );

        batch.on_submit_rollback();
        assert!(!batch.rc.submitted.get());
        assert_eq!(ring.rc.local_tail.get(), start_tail);
        assert_eq!(
            unsafe { (*ring.rc.shared_tail).load(atomic::Ordering::Acquire) },
            start_tail
        );
        batch.validate_send().unwrap();

        batch.on_submit().unwrap();
        batch.on_submit_rollback();
        drop(batch);
        assert_eq!(ring.available_buffers(), 2);
    }

    #[test]
    fn missing_selected_bid_poisons_published_send_ring() {
        let mut state = SendQueueState::new(1);
        let batch = state.begin_batch().unwrap();
        let bid = state.checkout_bid(batch).unwrap();
        state.commit_datagram(batch, bid, 4).unwrap();
        state.mark_submitted(batch).unwrap();

        let err = state
            .complete_udp_send(batch, crate::operation::CQEResult::new(Ok(4), 0))
            .unwrap_err();

        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
        assert!(state.active_batch.is_none());
        assert!(state.begin_batch().is_err());
    }
}
