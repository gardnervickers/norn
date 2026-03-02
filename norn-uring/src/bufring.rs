//! Support for io_uring registered buffer rings.
//!
//! Copied from the test code here
//! https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/tests/register_buf_ring.rs

use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::atomic::{self, AtomicU16};
use std::{fmt, io, ops, ptr};

use io_uring::types::{self, BufRingEntry};
use io_uring::Submitter;
use log::warn;

use crate::Handle;

/// [`BufRing`] is a reference counted buffer ring which can be registered
/// with io_uring to provide buffers for read operations.
#[derive(Clone)]
pub struct BufRing {
    // The BufRing is reference counted because each buffer handed out has a reference back to its
    // buffer group, or in this case, to its buffer ring.
    rc: Rc<InnerBufRing>,
}

/// [`SendBufRing`] is a reference counted buffer ring used to stage outbound buffers for
/// `SendBundle`.
#[derive(Clone)]
pub struct SendBufRing {
    rc: Rc<InnerSendBufRing>,
}

/// [`SendBundleBatch`] is a one-shot collection of send buffers staged for a single
/// `SendBundle` request.
#[derive(Debug)]
pub struct SendBundleBatch {
    rc: Rc<InnerSendBundleBatch>,
}

impl fmt::Debug for BufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufRing")
            .field("bgid", &self.rc.bgid())
            .field("ring_entries", &self.rc.ring_entries())
            .field("buf_cnt", &self.rc.buf_cnt)
            .field("buf_len", &self.rc.buf_len)
            .finish()
    }
}

impl BufRing {
    fn new(buf_ring: InnerBufRing) -> Self {
        BufRing {
            rc: Rc::new(buf_ring),
        }
    }

    /// Create a new Builder with the given buffer group ID.
    pub fn builder(id: Bgid) -> Builder {
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

    /// Start a new bundle batch on this send ring.
    ///
    /// Only one batch may be active per send ring at a time.
    pub fn batch(&self) -> io::Result<SendBundleBatch> {
        self.rc.begin_batch()?;
        Ok(SendBundleBatch {
            rc: Rc::new(InnerSendBundleBatch::new(self.clone())),
        })
    }

    /// Returns the number of available buffers that can be checked out.
    pub fn available_buffers(&self) -> usize {
        self.rc.available_buffers()
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.bgid()
    }
}

impl SendBundleBatch {
    /// Check out a writable buffer from this batch.
    pub fn checkout(&self) -> io::Result<SendBuf> {
        let bid = self.rc.ring.rc.checkout_bid()?;
        Ok(SendBuf::new(Rc::clone(&self.rc), bid))
    }

    /// Returns the total queued byte length across all committed send buffers in this batch.
    pub fn queued_len(&self) -> usize {
        self.rc.ring.rc.queued_len()
    }

    /// Returns the number of committed send buffers in this batch.
    pub fn queued_buffers(&self) -> usize {
        self.rc.ring.rc.queued_buffers()
    }

    /// Returns the number of available buffers left in the underlying send ring.
    pub fn available_buffers(&self) -> usize {
        self.rc.ring.available_buffers()
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.ring.bgid()
    }

    pub(crate) fn validate_send(&self) -> io::Result<()> {
        self.rc.ring.rc.validate_send()
    }

    pub(crate) fn on_submit(&self) {
        self.rc.on_submit();
    }

    pub(crate) fn finish_send(&self, result: crate::operation::CQEResult) -> io::Result<usize> {
        self.rc.ring.rc.finish_send(result)
    }
}

/// [`BufRingBuf`] is a reference to a buffer in a buffer ring.
///
/// It is reference counted and will be returned to the buffer ring when dropped.
/// Users should be careful to drop the buffer as soon as possible to avoid
/// exhausting the buffer ring.
pub struct BufRingBuf {
    bufgroup: BufRing,
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
    fn new(bufgroup: BufRing, bid: Bid, len: usize) -> Self {
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
        self.batch.ring.rc.commit_bid(self.bid, self.len)?;
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

/// [`Builder`] is used to create a new [`BufRing`].
#[derive(Copy, Clone, Debug)]
pub struct Builder {
    bgid: Bgid,
    ring_entries: u16,
    buf_cnt: u16,
    buf_len: usize,
}

impl Builder {
    // Create a new Builder with the given buffer group ID and defaults.
    //
    // The buffer group ID, `bgid`, is the id the kernel uses to identify the buffer group to use
    // for a given read operation that has been placed into an sqe.
    //
    // The caller is responsible for picking a bgid that does not conflict with other buffer
    // groups that have been registered with the same uring interface.
    fn new(bgid: Bgid) -> Builder {
        Builder {
            bgid,
            ring_entries: 128,
            buf_cnt: 0, // 0 indicates buf_cnt is taken from ring_entries
            buf_len: 4096,
        }
    }

    /// The number of ring entries to create for the buffer ring.
    ///
    /// The number will be made a power of 2, and will be the maximum of the ring_entries setting
    /// and the buf_cnt setting. The interface will enforce a maximum of 2^15 (32768).
    pub fn ring_entries(mut self, ring_entries: u16) -> Builder {
        self.ring_entries = ring_entries;
        self
    }

    /// The number of buffers to allocate. If left zero, the ring_entries value will be used.
    pub fn buf_cnt(mut self, buf_cnt: u16) -> Builder {
        self.buf_cnt = buf_cnt;
        self
    }

    /// The length to be preallocated for each buffer.
    pub fn buf_len(mut self, buf_len: usize) -> Builder {
        self.buf_len = buf_len;
        self
    }

    fn normalized(&self) -> io::Result<Builder> {
        let mut b: Builder = *self;

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

    /// Return a BufRing.
    pub fn build(&self) -> io::Result<BufRing> {
        let b = self.normalized()?;

        let handle = crate::Handle::current();
        let inner =
            InnerBufRing::new(b.bgid, b.ring_entries, b.buf_cnt, b.buf_len, handle.clone())?;
        handle.with_submitter(|s| inner.register(s))?;
        Ok(BufRing::new(inner))
    }

    /// Return a send-side buffer ring used to stage outbound buffers for `SendBundle`.
    pub fn build_send(&self) -> io::Result<SendBufRing> {
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

    // All these fields are constant once the struct is instantiated except the one of type Cell<u16>.
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
    bgid: Bgid,
    ring_entries_mask: u16,
    buf_cnt: u16,
    buf_len: usize,
    ring_start: AnonymousMmap,
    buf_list: Vec<Vec<u8>>,
    local_tail: Cell<u16>,
    shared_tail: *const AtomicU16,
    state: RefCell<SendQueueState>,
}

#[derive(Debug)]
struct InnerSendBundleBatch {
    ring: SendBufRing,
    submitted: Cell<bool>,
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
    active_batch: bool,
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
            submitter.register_buf_ring(self.ring_start.as_ptr() as _, self.ring_entries(), bgid)
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
    fn get_buf(&self, buf_ring: BufRing, res: u32, flags: u32) -> io::Result<BufRingBuf> {
        // This fn does the odd thing of having self as the BufRing and taking an argument that is
        // the same BufRing but wrapped in Rc<_> so the wrapped buf_ring can be passed to the
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
        buf_ring: BufRing,
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
        unsafe { (&*entries.add(idx as usize)).bid() }
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
            active_batch: false,
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

    fn checkout_bid(&mut self) -> io::Result<Bid> {
        self.ensure_not_poisoned()?;
        if !self.active_batch {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring has no active bundle batch",
            ));
        }
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

    fn begin_batch(&mut self) -> io::Result<()> {
        self.ensure_not_poisoned()?;
        if self.active_batch {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring already has an active bundle batch",
            ));
        }
        self.active_batch = true;
        Ok(())
    }

    fn abandon_batch(&mut self) {
        self.inflight = false;
        self.active_batch = false;
        while let Some(segment) = self.queued.pop_front() {
            self.free_bids.push(segment.bid);
        }
    }

    fn commit_datagram(&mut self, bid: Bid, len: usize) -> io::Result<()> {
        self.ensure_not_poisoned()?;
        if !self.active_batch {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring has no active bundle batch",
            ));
        }
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

    fn queued_len(&self) -> usize {
        self.queued.iter().map(|dgram| dgram.len).sum()
    }

    fn queued_buffers(&self) -> usize {
        self.queued.len()
    }

    fn available_buffers(&self) -> usize {
        self.free_bids.len()
    }

    fn validate_send(&self) -> io::Result<()> {
        self.ensure_not_poisoned()?;
        if !self.active_batch {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "send buffer ring has no active bundle batch",
            ));
        }
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

    fn mark_submitted(&mut self) {
        debug_assert!(!self.poisoned);
        debug_assert!(self.active_batch);
        debug_assert!(!self.inflight);
        debug_assert!(!self.queued.is_empty());
        self.inflight = true;
    }

    fn finish_send(&mut self, result: crate::operation::CQEResult) -> io::Result<usize> {
        let out = match result.result {
            Ok(bytes) => self.complete_udp_send(bytes as usize, result.flags),
            Err(err) => Err(err),
        };
        self.inflight = false;
        self.active_batch = false;
        out
    }

    fn complete_udp_send(&mut self, bytes: usize, flags: u32) -> io::Result<usize> {
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

        let mut remaining = bytes;
        while remaining > 0 {
            let Some(front) = self.queued.front() else {
                return self.poison(format!(
                    "send bundle completion consumed {} bytes beyond queued segments",
                    remaining
                ));
            };
            if remaining < front.len {
                return self.poison(format!(
                    "send bundle completion stopped mid-segment (remaining={} segment_len={})",
                    remaining, front.len
                ));
            }
            remaining -= front.len;
            let bid = front.bid;
            self.queued.pop_front();
            self.free_bids.push(bid);
        }

        if !self.queued.is_empty() {
            return self
                .poison("send bundle completion stopped before the end of the active batch");
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
            buf_list.push(vec![0; buf_len]);
        }

        let shared_tail =
            unsafe { types::BufRingEntry::tail(ring_start.as_ptr() as *const BufRingEntry) }
                as *const AtomicU16;
        let ring_entries_mask = ring_entries - 1;

        Ok(Self {
            handle,
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
            submitter.register_buf_ring(self.ring_start.as_ptr() as _, self.ring_entries(), bgid)
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "buf_ring.register returned {}, most likely indicating this kernel is not 5.19+",
                            e
                        ),
                    ));
                }
                Some(libc::EEXIST) => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!(
                            "buf_ring.register returned `{}`, indicating the attempted buffer group id {} was already registered",
                            e, bgid
                        ),
                    ));
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("buf_ring.register returned `{}` for group id {}", e, bgid),
                    ));
                }
            }
        }

        res
    }

    fn unregister(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        submitter.unregister_buf_ring(self.bgid)
    }

    fn bgid(&self) -> Bgid {
        self.bgid
    }

    fn begin_batch(&self) -> io::Result<()> {
        self.state.borrow_mut().begin_batch()
    }

    fn abandon_batch(&self) {
        self.state.borrow_mut().abandon_batch();
    }

    fn checkout_bid(&self) -> io::Result<Bid> {
        self.state.borrow_mut().checkout_bid()
    }

    fn release_checkout(&self, bid: Bid) {
        self.state.borrow_mut().release_checkout(bid);
    }

    fn commit_bid(&self, bid: Bid, len: usize) -> io::Result<()> {
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
        self.state.borrow_mut().commit_datagram(bid, len)?;
        Ok(())
    }

    fn queued_len(&self) -> usize {
        self.state.borrow().queued_len()
    }

    fn queued_buffers(&self) -> usize {
        self.state.borrow().queued_buffers()
    }

    fn available_buffers(&self) -> usize {
        self.state.borrow().available_buffers()
    }

    fn validate_send(&self) -> io::Result<()> {
        self.state.borrow().validate_send()
    }

    fn publish_for_send(&self) {
        let queued: Vec<_> = {
            let mut state = self.state.borrow_mut();
            state.mark_submitted();
            state.queued.iter().copied().collect()
        };
        for segment in queued {
            self.buf_ring_push_with_len(segment.bid, segment.len);
        }
        self.buf_ring_sync();
    }

    fn finish_send(&self, result: crate::operation::CQEResult) -> io::Result<usize> {
        self.state.borrow_mut().finish_send(result)
    }

    fn buf_capacity(&self) -> usize {
        self.buf_len
    }

    fn stable_ptr_mut(&self, bid: Bid) -> *mut u8 {
        self.buf_list[bid as usize].as_ptr() as *mut u8
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
    fn new(ring: SendBufRing) -> Self {
        Self {
            ring,
            submitted: Cell::new(false),
        }
    }

    fn on_submit(&self) {
        if self.submitted.replace(true) {
            return;
        }
        self.ring.rc.publish_for_send();
    }
}

impl Drop for InnerSendBundleBatch {
    fn drop(&mut self) {
        self.ring.rc.abandon_batch();
    }
}

impl Drop for InnerBufRing {
    fn drop(&mut self) {
        // Best-effort unregister on drop. If this fails we prefer logging over panicking
        // during teardown; the process can still exit safely.
        if let Err(err) = self.handle.with_submitter(|s| self.unregister(s)) {
            warn!(target: "norn_uring::bufring", "unregister.failed: {}", err);
        }
    }
}

impl Drop for InnerSendBufRing {
    fn drop(&mut self) {
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
        match unsafe { libc::madvise(addr.as_ptr(), len, libc::MADV_DONTFORK) } {
            0 => {
                let mmap = Self { addr, len };
                Ok(mmap)
            }
            _ => Err(io::Error::last_os_error()),
        }
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
    use super::{selected_bid_from_flags, SendQueueState};
    use std::io;

    fn selected_bid_flag(bid: u16) -> u32 {
        const IORING_CQE_F_BUFFER: u32 = 1;
        const IORING_CQE_BUFFER_SHIFT: u32 = 16;

        IORING_CQE_F_BUFFER | ((bid as u32) << IORING_CQE_BUFFER_SHIFT)
    }

    #[test]
    fn selected_bid_requires_buffer_select_flag() {
        let err = selected_bid_from_flags(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn send_ring_checkout_commit_updates_counts() {
        let mut state = SendQueueState::new(2);
        state.begin_batch().unwrap();
        let bid = state.checkout_bid().unwrap();
        assert_eq!(bid, 0);
        state.commit_datagram(bid, 4).unwrap();

        assert_eq!(state.available_buffers(), 1);
        assert_eq!(state.queued_buffers(), 1);
        assert_eq!(state.queued_len(), 4);
    }

    #[test]
    fn udp_send_bundle_full_consumes_all_segments() {
        let mut state = SendQueueState::new(3);
        state.begin_batch().unwrap();
        let first = state.checkout_bid().unwrap();
        let second = state.checkout_bid().unwrap();
        state.commit_datagram(first, 3).unwrap();
        state.commit_datagram(second, 5).unwrap();
        state.validate_send().unwrap();
        state.mark_submitted();

        let sent = state
            .complete_udp_send(8, selected_bid_flag(first))
            .unwrap();
        assert_eq!(sent, 8);
        assert_eq!(state.queued_buffers(), 0);
        assert_eq!(state.available_buffers(), 3);
    }

    #[test]
    fn udp_send_bundle_short_completion_poisons_ring() {
        let mut state = SendQueueState::new(3);
        state.begin_batch().unwrap();
        let first = state.checkout_bid().unwrap();
        let second = state.checkout_bid().unwrap();
        state.commit_datagram(first, 3).unwrap();
        state.commit_datagram(second, 5).unwrap();
        state.validate_send().unwrap();
        state.mark_submitted();

        let err = state
            .complete_udp_send(3, selected_bid_flag(first))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(state.queued_buffers(), 1);
        assert_eq!(state.available_buffers(), 2);
        assert!(state.poisoned);
    }

    #[test]
    fn udp_send_bundle_mid_segment_poisons_ring() {
        let mut state = SendQueueState::new(2);
        state.begin_batch().unwrap();
        let first = state.checkout_bid().unwrap();
        state.commit_datagram(first, 4).unwrap();
        state.validate_send().unwrap();
        state.mark_submitted();

        let err = state
            .complete_udp_send(2, selected_bid_flag(first))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
    }

    #[test]
    fn send_ring_selected_bid_mismatch_poisons_ring() {
        let mut state = SendQueueState::new(3);
        state.begin_batch().unwrap();
        let first = state.checkout_bid().unwrap();
        let second = state.checkout_bid().unwrap();
        state.commit_datagram(first, 4).unwrap();
        state.commit_datagram(second, 4).unwrap();
        state.validate_send().unwrap();
        state.mark_submitted();

        let err = state
            .complete_udp_send(4, selected_bid_flag(second))
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(state.poisoned);
    }

    #[test]
    fn abandon_batch_releases_buffers_and_clears_flags() {
        let mut state = SendQueueState::new(2);
        state.begin_batch().unwrap();
        let bid = state.checkout_bid().unwrap();
        state.commit_datagram(bid, 4).unwrap();
        state.validate_send().unwrap();
        state.mark_submitted();

        state.abandon_batch();

        assert!(!state.inflight);
        assert!(!state.active_batch);
        assert_eq!(state.queued_buffers(), 0);
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
}
