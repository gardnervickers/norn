//! Support for io_uring registered buffer rings.
//!
//! Copied from the test code here
//! https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/tests/register_buf_ring.rs

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::{fmt, io, ops};

use log::warn;

use crate::Handle;

mod availability;
mod ledger;
mod registered;
pub(crate) mod send;

use availability::Availability;
use ledger::{BufferToken, PublicationLedger, PublishAction, RingFault};
use registered::{RegisteredBufRing, RegisteredEntry};

pub use send::{SendBuf, SendBufRing, SendBufRingBuilder};

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
    // The RecvBufRing is reference counted because each buffer handed out has a reference back to its
    // buffer group, or in this case, to its buffer ring.
    rc: Rc<RecvRing>,
}

impl fmt::Debug for RecvBufRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvBufRing")
            .field("bgid", &self.rc.bgid())
            .field("ring_entries", &self.rc.ring_entries())
            .field("buf_cnt", &self.rc.buf_count())
            .field("buf_len", &self.rc.buf_capacity())
            .finish()
    }
}

impl RecvBufRing {
    fn new(buf_ring: RecvRing) -> Self {
        RecvBufRing {
            rc: Rc::new(buf_ring),
        }
    }

    /// Create a new RecvBufRingBuilder with the given buffer group ID.
    pub fn builder(id: Bgid) -> RecvBufRingBuilder {
        RecvBufRingBuilder::new(id)
    }

    /// Returns the capacity of each buffer in the buffer ring.
    pub fn buf_capacity(&self) -> usize {
        self.rc.buf_capacity()
    }

    /// Returns the number of buffers in the buffer ring.
    pub fn buf_count(&self) -> u16 {
        self.rc.buf_count()
    }

    pub(crate) fn get_buf(&self, res: u32, flags: u32) -> io::Result<RecvBuf> {
        self.rc.get_buf(self.clone(), res, flags)
    }

    pub(crate) fn get_buf_bundle(&self, res: u32, flags: u32) -> io::Result<RecvBufBundle> {
        self.rc.get_buf_bundle(self.clone(), res, flags)
    }

    pub(crate) fn bgid(&self) -> Bgid {
        self.rc.bgid()
    }

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.registered.same_driver(handle)
    }

    pub(crate) fn availability_generation(&self) -> u64 {
        self.rc.availability.generation()
    }

    pub(crate) fn check_health(&self) -> io::Result<()> {
        self.rc.availability.check().map_err(ring_fault_error)
    }

    pub(crate) async fn wait_for_availability_since(&self, observed: u64) -> io::Result<()> {
        self.rc
            .availability
            .changed_since(observed)
            .await
            .map_err(ring_fault_error)
    }

    pub(crate) async fn wait_for_fault(&self) -> io::Error {
        ring_fault_error(self.rc.availability.failed().await)
    }
}

/// [`RecvBuf`] is a reference to a buffer in a buffer ring.
///
/// It is reference counted and will be returned to the buffer ring when dropped.
/// Users should be careful to drop the buffer as soon as possible to avoid
/// exhausting the buffer ring.
pub struct RecvBuf {
    bufgroup: RecvBufRing,
    start: usize,
    len: usize,
    token: BufferToken,
}

/// [`RecvBufBundle`] is a collection of one or more buffers selected from a buffer ring.
///
/// This is primarily used by recv bundle operations that may consume multiple provided buffers
/// for a single completion.
#[derive(Debug)]
pub struct RecvBufBundle {
    bufs: VecDeque<RecvBuf>,
    len: usize,
}

/// Compatibility alias for an owned buffer selected from a receive ring.
pub type BufRingBuf = RecvBuf;

/// Compatibility alias for a bundle of buffers selected from a receive ring.
pub type BufRingBufBundle = RecvBufBundle;

impl RecvBufBundle {
    fn new(bufs: Vec<RecvBuf>, len: usize) -> Self {
        Self {
            bufs: bufs.into(),
            len,
        }
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
        self.bufs.iter().map(RecvBuf::as_slice)
    }

    /// Returns the first contiguous slice of remaining stream bytes.
    pub fn chunk(&self) -> &[u8] {
        self.bufs.front().map_or(&[], RecvBuf::as_slice)
    }

    /// Discards `count` bytes from the front of the bundle.
    ///
    /// Fully consumed buffers are immediately returned to their receive ring.
    ///
    /// # Panics
    ///
    /// Panics if `count` exceeds [`len`](Self::len).
    pub fn advance(&mut self, mut count: usize) {
        assert!(count <= self.len, "advanced beyond receive bundle length");
        self.len -= count;
        while count != 0 {
            let front = self.bufs.front_mut().expect("nonempty bundle lost buffers");
            let consumed = count.min(front.len());
            front.advance(consumed);
            count -= consumed;
            if front.is_empty() {
                self.bufs.pop_front();
            }
        }
    }

    /// Consumes this bundle and returns the underlying ring buffers.
    pub fn into_bufs(self) -> Vec<RecvBuf> {
        self.bufs.into()
    }
}

impl fmt::Debug for RecvBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RecvBuf")
            .field("bgid", &self.bufgroup.rc.bgid())
            .field("bid", &self.token.bid)
            .field("start", &self.start)
            .field("len", &self.len)
            .field("cap", &self.bufgroup.rc.buf_capacity())
            .finish()
    }
}

impl RecvBuf {
    fn new(bufgroup: RecvBufRing, token: BufferToken, len: usize) -> Self {
        assert!(len <= bufgroup.rc.buf_capacity());

        Self {
            bufgroup,
            start: 0,
            len,
            token,
        }
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
        let p = unsafe { self.bufgroup.rc.stable_ptr(self.token.bid).add(self.start) };
        unsafe { std::slice::from_raw_parts(p, self.len) }
    }

    fn advance(&mut self, count: usize) {
        assert!(count <= self.len);
        self.start += count;
        self.len -= count;
    }
}

impl Drop for RecvBuf {
    fn drop(&mut self) {
        self.bufgroup.rc.return_buffer(self.token);
    }
}

/// Identifier for a registered buffer group.
pub type Bgid = u16;

/// Identifier for a buffer within a registered buffer group.
pub type Bid = u16;

#[cfg(test)]
fn selected_bid_from_flags(flags: u32) -> io::Result<Bid> {
    io_uring::cqueue::buffer_select(flags).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "completion did not include a selected buffer id",
        )
    })
}

/// [`RecvBufRingBuilder`] is used to create a new [`RecvBufRing`].
#[derive(Copy, Clone, Debug)]
pub struct RecvBufRingBuilder {
    bgid: Bgid,
    ring_entries: u16,
    buf_cnt: u16,
    buf_len: usize,
}

impl RecvBufRingBuilder {
    // Create a new RecvBufRingBuilder with the given buffer group ID and defaults.
    //
    // The buffer group ID, `bgid`, is the id the kernel uses to identify the buffer group to use
    // for a given read operation that has been placed into an sqe.
    //
    // The caller is responsible for picking a bgid that does not conflict with other buffer
    // groups that have been registered with the same uring interface.
    fn new(bgid: Bgid) -> RecvBufRingBuilder {
        RecvBufRingBuilder {
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
    pub fn ring_entries(mut self, ring_entries: u16) -> RecvBufRingBuilder {
        self.ring_entries = ring_entries;
        self
    }

    /// The number of buffers to allocate. If left zero, the ring_entries value will be used.
    pub fn buf_cnt(mut self, buf_cnt: u16) -> RecvBufRingBuilder {
        self.buf_cnt = buf_cnt;
        self
    }

    /// The length to be preallocated for each buffer.
    pub fn buf_len(mut self, buf_len: usize) -> RecvBufRingBuilder {
        self.buf_len = buf_len;
        self
    }

    /// Return a [`RecvBufRing`].
    pub fn build(&self) -> io::Result<RecvBufRing> {
        let mut b: RecvBufRingBuilder = *self;

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
            return Err(io::Error::other("ring_entries exceeded 32768"));
        }

        // Requirement of the interface is the ring entries is a power of two, making its and our
        // wrap calculation trivial.
        b.ring_entries = b.ring_entries.next_power_of_two();

        let handle = crate::Handle::current();
        let inner = RecvRing::new(b.bgid, b.ring_entries, b.buf_cnt, b.buf_len, handle.clone())?;
        handle.with_submitter(|s| inner.register(s))?;
        Ok(RecvBufRing::new(inner))
    }
}

struct RecvRing {
    registered: RegisteredBufRing,
    ledger: RefCell<PublicationLedger>,
    initial_publish: RefCell<Option<Vec<PublishAction>>>,
    availability: Availability,
}

impl RecvRing {
    fn new(
        bgid: Bgid,
        ring_entries: u16,
        buf_cnt: u16,
        buf_len: usize,
        handle: Handle,
    ) -> io::Result<RecvRing> {
        let registered = RegisteredBufRing::new(bgid, ring_entries, buf_cnt, buf_len, handle)?;
        let ledger_window = usize::from(ring_entries)
            .checked_mul(2)
            .expect("buffer ring ledger window overflowed");
        let (ledger, initial_publish) = PublicationLedger::new(buf_cnt, ledger_window);

        let buf_ring = RecvRing {
            registered,
            ledger: RefCell::new(ledger),
            initial_publish: RefCell::new(Some(initial_publish)),
            availability: Availability::default(),
        };

        Ok(buf_ring)
    }

    fn register(&self, submitter: &io_uring::Submitter<'_>) -> io::Result<()> {
        self.registered.register(submitter)?;
        let initial = self
            .initial_publish
            .borrow_mut()
            .take()
            .expect("buffer ring registered more than once");
        self.publish(&initial);

        Ok(())
    }

    fn return_buffer(&self, token: BufferToken) {
        let returned = self.ledger.borrow_mut().return_buffer(token);
        match returned {
            Ok(returned) => self.publish_returned(&returned.publish),
            Err(fault) => {
                self.availability.fail(fault.clone());
                warn!(target: "norn_uring::bufring", "buffer_return.quarantined: {}", fault.message());
            }
        }
    }

    // Returns the buffer group id.
    fn bgid(&self) -> Bgid {
        self.registered.bgid()
    }

    // Returns the buffer the uring interface picked from the buf_ring for the completion result
    // represented by the res and flags.
    fn get_buf(&self, buf_ring: RecvBufRing, res: u32, flags: u32) -> io::Result<RecvBuf> {
        // This fn does the odd thing of having self as the RecvBufRing and taking an argument that is
        // the same RecvBufRing but wrapped in Rc<_> so the wrapped buf_ring can be passed to the
        // outgoing GBuf.
        let len = res as usize;
        let bid = match io_uring::cqueue::buffer_select(flags) {
            Some(bid) => bid,
            None if len == 0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            None => return Err(self.quarantine("completion omitted its selected buffer id")),
        };
        if len > self.buf_capacity() {
            return Err(self.quarantine("single-buffer completion exceeded buffer capacity"));
        }
        let claim = self.ledger.borrow_mut().claim_range(bid, 1);
        let claim = claim.map_err(|fault| self.record_fault(fault))?;
        self.publish_returned(&claim.publish);
        let token = claim.tokens.into_iter().next().expect("single claim empty");
        Ok(RecvBuf::new(buf_ring, token, len))
    }

    fn get_buf_bundle(
        &self,
        buf_ring: RecvBufRing,
        res: u32,
        flags: u32,
    ) -> io::Result<RecvBufBundle> {
        let total_len = res as usize;
        let Some(first_bid) = io_uring::cqueue::buffer_select(flags) else {
            if total_len == 0 {
                return Ok(RecvBufBundle::new(Vec::new(), 0));
            }
            return Err(self.quarantine("bundle completion omitted its selected buffer id"));
        };
        let needed = if total_len == 0 {
            1
        } else {
            total_len.div_ceil(self.buf_capacity())
        };
        if needed > usize::from(self.buf_count()) {
            return Err(self.quarantine(format!(
                "bundle completion requires {needed} buffers but ring only has {}",
                self.buf_count()
            )));
        }

        let claim = self.ledger.borrow_mut().claim_range(first_bid, needed);
        let claim = claim.map_err(|fault| self.record_fault(fault))?;
        self.publish_returned(&claim.publish);

        let mut bufs = Vec::with_capacity(needed);
        let mut remaining = total_len;
        for (i, token) in claim.tokens.into_iter().enumerate() {
            let len = if i + 1 == needed {
                remaining
            } else {
                self.buf_capacity()
            };
            bufs.push(RecvBuf::new(buf_ring.clone(), token, len));
            remaining = remaining.saturating_sub(len);
        }

        Ok(RecvBufBundle::new(bufs, total_len))
    }

    fn buf_capacity(&self) -> usize {
        self.registered.buf_capacity()
    }

    fn buf_count(&self) -> u16 {
        self.registered.buf_count()
    }

    fn stable_ptr(&self, bid: Bid) -> *const u8 {
        self.registered.stable_ptr(bid)
    }

    fn ring_entries(&self) -> u16 {
        self.registered.ring_entries()
    }

    fn publish(&self, actions: &[PublishAction]) {
        let entries: Vec<_> = actions
            .iter()
            .map(|action| RegisteredEntry {
                position: action.position,
                bid: action.bid,
                len: self.buf_capacity(),
            })
            .collect();
        self.registered.publish(&entries);
    }

    fn publish_returned(&self, actions: &[PublishAction]) {
        if actions.is_empty() {
            return;
        }
        self.publish(actions);
        self.availability.advance();
    }

    fn quarantine(&self, message: impl Into<Rc<str>>) -> io::Error {
        let fault = self.ledger.borrow_mut().quarantine(message);
        self.record_fault(fault)
    }

    fn record_fault(&self, fault: RingFault) -> io::Error {
        self.availability.fail(fault.clone());
        ring_fault_error(fault)
    }
}

fn ring_fault_error(fault: RingFault) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, fault.message().to_owned())
}

impl ops::Deref for RecvBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        RecvBuf::as_slice(self)
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
