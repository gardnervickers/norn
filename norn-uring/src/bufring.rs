//! Support for io_uring registered bufer ring.
//!
//! Copied from the test code here
//! https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/tests/register_buf_ring.rs

use std::cell::Cell;
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

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.bgid
    }
}

/// [`BufRingBuf`] is a reference to a buffer in a buffer ring.
///
/// It is reference counted and will be returned to the buffer ring when dropped.
/// Users should be careful to drop the buffer as soon as possible to avoid
/// exausting the buffer ring.
pub struct BufRingBuf {
    bufgroup: BufRing,
    len: usize,
    bid: Bid,
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

    // Return the number of bytes initialized.
    //
    // This value initially came from the kernel, as reported in the cqe. This value may have been
    // modified with a call to the IoBufMut::set_init method.
    #[allow(dead_code)]
    fn len(&self) -> usize {
        self.len as _
    }

    // Return true if this represents an empty buffer. The length reported by the kernel was 0.
    #[allow(dead_code)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    // Return the capacity of this buffer.
    #[allow(dead_code)]
    fn cap(&self) -> usize {
        self.bufgroup.rc.buf_capacity()
    }

    // Return a byte slice reference.
    fn as_slice(&self) -> &[u8] {
        let p = self.bufgroup.rc.stable_ptr(self.bid);
        unsafe { std::slice::from_raw_parts(p, self.len) }
    }
}

impl Drop for BufRingBuf {
    fn drop(&mut self) {
        // Add the buffer back to the bufgroup, for the kernel to reuse.
        unsafe { self.bufgroup.rc.dropping_bid(self.bid) };
    }
}

/// [Bgid] is used to identify a buffer group.
pub(crate) type Bgid = u16;

/// [Bid] is used to identify a buffer within a buffer group.
pub(crate) type Bid = u16;

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

    /// Return a BufRing.
    pub fn build(&self) -> io::Result<BufRing> {
        let mut b: Builder = *self;

        // Two cases where both buf_cnt and ring_entries are set to the max of the two.
        if b.buf_cnt == 0 || b.ring_entries < b.buf_cnt {
            let max = std::cmp::max(b.ring_entries, b.buf_cnt);
            b.buf_cnt = max;
            b.ring_entries = max;
        }

        // Don't allow the next_power_of_two calculation to be done if already larger than 2^15
        // because 2^16 reads back as 0 in a u16. The interface doesn't allow for ring_entries
        // larger than 2^15 anyway, so this is a good place to catch it. Here we return a unique
        // error that is more descriptive than the InvalidArg that would come from the interface.
        if b.ring_entries > (1 << 15) {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                "ring_entries exceeded 32768",
            ));
        }

        // Requirement of the interface is the ring entries is a power of two, making its and our
        // wrap calculation trivial.
        b.ring_entries = b.ring_entries.next_power_of_two();

        let handle = crate::Handle::current();
        let inner =
            InnerBufRing::new(b.bgid, b.ring_entries, b.buf_cnt, b.buf_len, handle.clone())?;
        handle.with_submitter(|s| inner.register(s))?;
        Ok(BufRing::new(inner))
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

        Ok(BufRingBuf::new(buf_ring, bid, len))
    }

    fn buf_capacity(&self) -> usize {
        self.buf_len as _
    }

    fn stable_ptr(&self, bid: Bid) -> *const u8 {
        self.buf_list[bid as usize].as_ptr()
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

impl Drop for InnerBufRing {
    fn drop(&mut self) {
        // Best-effort unregister on drop. If this fails we prefer logging over panicking
        // during teardown; the process can still exit safely.
        if let Err(err) = self.handle.with_submitter(|s| self.unregister(s)) {
            warn!(target: "norn_uring::bufring", "unregister.failed: {}", err);
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
    use super::selected_bid_from_flags;
    use std::io;

    #[test]
    fn selected_bid_requires_buffer_select_flag() {
        let err = selected_bid_from_flags(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
