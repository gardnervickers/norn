//! Support for io_uring registered buffer rings.
//!
//! The original registration setup was derived from the `io-uring` crate's
//! [registered buffer ring test](https://github.com/tokio-rs/io-uring/blob/master/io-uring-test/src/tests/register_buf_ring.rs).

use std::cell::{Cell, RefCell, UnsafeCell};
use std::rc::Rc;
use std::sync::atomic::{self, AtomicU16};
use std::{fmt, io, ops, ptr};

use crate::buf::StableBuf;
use crate::driver::BufRingRegistration;
use crate::Handle;
use io_uring::types::{self, BufRingEntry};
use io_uring::{IoUring, Submitter};
use log::warn;

mod tracker;

use tracker::{BufferToken, BundleClaim, PublicationTracker, TrackerError};

/// [`RecvBufRing`] is a reference counted buffer ring which can be registered
/// with `io_uring` to provide buffers for read operations.
///
/// Clones may be shared by receive operations on the same driver. Publication
/// ownership is tracked ring-wide, so completions may be reaped in a different
/// order from kernel buffer selection.
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
    fn new(registration: BufRingRegistration, storage: Rc<BufRingStorage>) -> Self {
        RecvBufRing {
            rc: Rc::new(InnerBufRing {
                registration,
                storage,
            }),
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

    pub(crate) fn same_driver(&self, handle: &Handle) -> bool {
        self.rc.registration.same_driver(handle)
    }

    pub(crate) fn ensure_healthy(&self) -> io::Result<()> {
        self.rc.ensure_healthy()
    }
}

/// [`BufRingBuf`] is a reference to a buffer in a buffer ring.
///
/// The value retains its ring and normally republishes its selected buffer when
/// dropped. If ownership accounting has quarantined the ring, the buffer
/// remains unavailable instead. Users should drop the buffer as soon as
/// possible to avoid exhausting the buffer ring.
///
/// The buffer implements [`StableBuf`], so it can be moved directly into send
/// operations. The selected buffer ID remains unavailable to receive operations
/// until the send returns the buffer and that value is dropped.
pub struct BufRingBuf {
    bufgroup: RecvBufRing,
    len: usize,
    token: BufferToken,
}

/// [`BufRingBufBundle`] is a collection of one or more buffers selected from a buffer ring.
///
/// This is primarily used by recv bundle operations that may consume multiple provided buffers
/// for a single completion. Dropping the bundle normally republishes every
/// selected buffer. If ownership accounting has quarantined the ring, those
/// buffers remain unavailable instead.
pub struct BufRingBufBundle {
    bufgroup: Option<RecvBufRing>,
    claim: Option<BundleClaim>,
    len: usize,
}

impl BufRingBufBundle {
    fn empty() -> Self {
        Self {
            bufgroup: None,
            claim: None,
            len: 0,
        }
    }

    fn new(bufgroup: RecvBufRing, claim: BundleClaim, len: usize) -> Self {
        Self {
            bufgroup: Some(bufgroup),
            claim: Some(claim),
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
        self.claim.as_ref().map_or(0, BundleClaim::len)
    }

    /// Returns an iterator over payload slices for each buffer in this bundle.
    pub fn iter(&self) -> impl Iterator<Item = &[u8]> + '_ {
        let count = self.buffer_count();
        self.claim
            .iter()
            .flat_map(BundleClaim::iter)
            .enumerate()
            .map(move |(index, token)| {
                let ring = self
                    .bufgroup
                    .as_ref()
                    .expect("non-empty bundle must retain its buffer ring");
                let len = self.buffer_len(index, count, ring.rc.buf_len);
                let ptr = ring.rc.stable_ptr(token.bid());
                // Safety: the bundle exclusively owns this BID until drop, and the
                // initialized length is bounded by the registered buffer size.
                unsafe { std::slice::from_raw_parts(ptr, len) }
            })
    }

    /// Consumes this bundle and returns the underlying ring buffers.
    pub fn into_bufs(mut self) -> Vec<BufRingBuf> {
        let count = self.buffer_count();
        let mut bufs = Vec::with_capacity(count);
        let Some(ring) = self.bufgroup.as_ref() else {
            return bufs;
        };
        let claim = self
            .claim
            .as_ref()
            .expect("non-empty bundle must retain its tracker claim");
        for (index, token) in claim.iter().enumerate() {
            assert!(token.bid() < ring.rc.buf_cnt);
            assert!(self.buffer_len(index, count, ring.rc.buf_len) <= ring.rc.buf_len);
        }

        let mut bufgroup = self.bufgroup.take();
        for (index, token) in claim.iter().enumerate() {
            let ring = bufgroup
                .as_ref()
                .expect("bundle ring must remain available through materialization");
            let len = self.buffer_len(index, count, ring.rc.buf_len);
            let owner = if index + 1 == count {
                bufgroup
                    .take()
                    .expect("final buffer must take the bundle ring")
            } else {
                ring.clone()
            };
            bufs.push(BufRingBuf::new(owner, token, len));
        }
        bufs
    }

    fn buffer_len(&self, index: usize, count: usize, capacity: usize) -> usize {
        if index + 1 == count {
            self.len - capacity * (count - 1)
        } else {
            capacity
        }
    }
}

impl Drop for BufRingBufBundle {
    fn drop(&mut self) {
        let (Some(ring), Some(claim)) = (&self.bufgroup, &self.claim) else {
            return;
        };
        ring.rc.return_bundle(claim);
    }
}

impl fmt::Debug for BufRingBufBundle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufRingBufBundle")
            .field("bgid", &self.bufgroup.as_ref().map(RecvBufRing::bgid))
            .field("buffer_count", &self.buffer_count())
            .field("len", &self.len)
            .finish()
    }
}

impl fmt::Debug for BufRingBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BufRingBuf")
            .field("bgid", &self.bufgroup.rc.bgid())
            .field("bid", &self.token.bid())
            .field("len", &self.len)
            .field("cap", &self.bufgroup.rc.buf_capacity())
            .finish()
    }
}

impl BufRingBuf {
    fn new(bufgroup: RecvBufRing, token: BufferToken, len: usize) -> Self {
        assert!(len <= bufgroup.rc.buf_len);

        Self {
            bufgroup,
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
        let p = self.bufgroup.rc.stable_ptr(self.token.bid());
        unsafe { std::slice::from_raw_parts(p, self.len) }
    }
}

// Safety: `BufRingBuf` exclusively owns its selected BID. `InnerBufRing` keeps
// the mmap allocation live and immovable, and the BID is not returned to the
// kernel until this value is dropped. Moving the wrapper therefore cannot
// invalidate the pointer or permit the kernel to reuse the exposed bytes while
// a send operation owns the buffer.
unsafe impl StableBuf for BufRingBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.bufgroup.rc.stable_ptr(self.token.bid())
    }

    fn bytes_init(&self) -> usize {
        self.len
    }
}

impl Drop for BufRingBuf {
    fn drop(&mut self) {
        self.bufgroup.rc.return_buffer(self.token);
    }
}

/// Identifier for a registered buffer group.
pub type Bgid = u16;

/// Identifier for a buffer within a registered buffer group.
pub(crate) type Bid = u16;

const REGISTRY_LOG: &str = "norn_uring::bufring::registry";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistrationState {
    Registering,
    RegisteringKernel,
    Registered,
    ReleaseRequested,
    Unregistered,
}

pub(crate) struct Registration {
    bgid: Bgid,
    state: Cell<RegistrationState>,
    // The registry owns the backing storage before the kernel can observe it.
    // This lets drop request release without allocating or risking reclamation
    // while an unregister attempt is deferred.
    _storage: Rc<BufRingStorage>,
}

impl Registration {
    pub(crate) fn state(&self) -> RegistrationState {
        self.state.get()
    }

    pub(crate) fn arm_kernel_call(&self) {
        assert_eq!(
            self.state.replace(RegistrationState::RegisteringKernel),
            RegistrationState::Registering,
            "provided-buffer-ring registration changed before the kernel call"
        );
    }

    pub(crate) fn kernel_call_failed(&self) {
        assert_eq!(
            self.state.replace(RegistrationState::Registering),
            RegistrationState::RegisteringKernel,
            "provided-buffer-ring registration changed after a failed kernel call"
        );
    }

    pub(crate) fn commit(&self) {
        assert_eq!(
            self.state.replace(RegistrationState::Registered),
            RegistrationState::RegisteringKernel,
            "provided-buffer-ring registration changed before commit"
        );
    }

    fn request_release(&self) -> Result<(), ReleaseError> {
        match self.state.get() {
            RegistrationState::Registered | RegistrationState::RegisteringKernel => {
                self.state.set(RegistrationState::ReleaseRequested);
                Ok(())
            }
            RegistrationState::ReleaseRequested | RegistrationState::Unregistered => Ok(()),
            RegistrationState::Registering => Err(ReleaseError::StateMismatch),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReserveError {
    RegistryBorrowed,
    DuplicateBgid,
    OutOfMemory,
}

#[derive(Debug)]
pub(crate) enum ReleaseError {
    RegistryBorrowed,
    RingBorrowed,
    StateMismatch,
    Io(io::Error),
}

impl fmt::Display for ReleaseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RegistryBorrowed => f.write_str("buffer-ring registry is already borrowed"),
            Self::RingBorrowed => f.write_str("io_uring is already borrowed"),
            Self::StateMismatch => f.write_str("buffer-ring registration state mismatch"),
            Self::Io(err) => write!(f, "buffer-ring unregistration failed: {err}"),
        }
    }
}

impl std::error::Error for ReleaseError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            Self::RegistryBorrowed | Self::RingBorrowed | Self::StateMismatch => None,
        }
    }
}

/// Driver-owned registrations for provided buffer rings.
///
/// The driver owns each backing allocation before registration and drops this
/// registry after its `IoUring`. A buffer-ring handle can therefore request
/// release without allocating; failed attempts remain owned and are retried.
pub(crate) struct Registry {
    registrations: RefCell<Vec<Rc<Registration>>>,
    retry_on_park: Cell<bool>,
    #[cfg(test)]
    release_failures: RefCell<std::collections::VecDeque<i32>>,
}

impl Registry {
    pub(crate) fn new() -> Self {
        Self {
            registrations: RefCell::new(Vec::new()),
            retry_on_park: Cell::new(false),
            #[cfg(test)]
            release_failures: RefCell::new(std::collections::VecDeque::new()),
        }
    }

    pub(crate) fn reserve(
        &self,
        ring: &RefCell<IoUring>,
        storage: Rc<BufRingStorage>,
    ) -> Result<Rc<Registration>, ReserveError> {
        if let Err(err) = self.release_requested(ring) {
            warn!(target: REGISTRY_LOG, "release.retry_failed: {err:?}");
        }

        let mut registrations = self
            .registrations
            .try_borrow_mut()
            .map_err(|_| ReserveError::RegistryBorrowed)?;
        if registrations
            .iter()
            .any(|registration| registration.bgid == storage.bgid)
        {
            return Err(ReserveError::DuplicateBgid);
        }
        registrations
            .try_reserve(1)
            .map_err(|_| ReserveError::OutOfMemory)?;
        let registration = Rc::new(Registration {
            bgid: storage.bgid,
            state: Cell::new(RegistrationState::Registering),
            _storage: storage,
        });
        registrations.push(Rc::clone(&registration));
        Ok(registration)
    }

    pub(crate) fn rollback(
        &self,
        ring: &RefCell<IoUring>,
        registration: &Rc<Registration>,
    ) -> Result<(), ReleaseError> {
        match registration.state() {
            RegistrationState::Registering => {
                registration.state.set(RegistrationState::Unregistered);
                self.release_or_defer(ring)
            }
            RegistrationState::RegisteringKernel => {
                registration.request_release()?;
                self.release_or_defer(ring)
            }
            RegistrationState::Registered
            | RegistrationState::ReleaseRequested
            | RegistrationState::Unregistered => Err(ReleaseError::StateMismatch),
        }
    }

    pub(crate) fn release(
        &self,
        ring: &RefCell<IoUring>,
        registration: &Rc<Registration>,
    ) -> Result<(), ReleaseError> {
        registration.request_release()?;
        self.release_or_defer(ring)
    }

    pub(crate) fn retry_deferred(&self, ring: &RefCell<IoUring>) -> Result<(), ReleaseError> {
        if !self.retry_on_park.replace(false) {
            return Ok(());
        }
        // A failed retry remains safely retained, but does not turn every park
        // into another syscall. The next registration attempt retries again.
        self.release_requested(ring)
    }

    fn release_or_defer(&self, ring: &RefCell<IoUring>) -> Result<(), ReleaseError> {
        let result = self.release_requested(ring);
        if result.is_err() {
            self.retry_on_park.set(true);
        }
        result
    }

    pub(crate) fn release_requested(&self, ring: &RefCell<IoUring>) -> Result<(), ReleaseError> {
        let mut index = 0;
        let mut first_error = None;
        loop {
            let registration = {
                let registrations = self
                    .registrations
                    .try_borrow()
                    .map_err(|_| ReleaseError::RegistryBorrowed)?;
                let Some(registration) = registrations.get(index) else {
                    break;
                };
                Rc::clone(registration)
            };

            if !matches!(
                registration.state(),
                RegistrationState::ReleaseRequested | RegistrationState::Unregistered
            ) {
                index += 1;
                continue;
            }

            if let Err(err) = self.release_one(ring, &registration) {
                first_error.get_or_insert(err);
                index += 1;
            }
            // Successful release uses swap_remove, so inspect the entry which
            // moved into this index before advancing.
        }
        match first_error {
            Some(err) => Err(err),
            None => {
                self.retry_on_park.set(false);
                Ok(())
            }
        }
    }

    fn release_one(
        &self,
        ring: &RefCell<IoUring>,
        registration: &Rc<Registration>,
    ) -> Result<(), ReleaseError> {
        if registration.state() == RegistrationState::Unregistered {
            return self.remove(registration);
        }
        if registration.state() != RegistrationState::ReleaseRequested {
            return Err(ReleaseError::StateMismatch);
        }

        #[cfg(test)]
        if let Some(errno) = self.release_failures.borrow_mut().pop_front() {
            return Err(ReleaseError::Io(io::Error::from_raw_os_error(errno)));
        }

        let ring = ring.try_borrow().map_err(|_| ReleaseError::RingBorrowed)?;
        let result = loop {
            match ring.submitter().unregister_buf_ring(registration.bgid) {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        };
        if let Err(err) = result {
            // ENOENT proves that the ring no longer owns this BGID.
            if err.raw_os_error() != Some(libc::ENOENT) {
                return Err(ReleaseError::Io(err));
            }
        }
        registration.state.set(RegistrationState::Unregistered);
        drop(ring);
        self.remove(registration)
    }

    fn remove(&self, registration: &Rc<Registration>) -> Result<(), ReleaseError> {
        let mut registrations = self
            .registrations
            .try_borrow_mut()
            .map_err(|_| ReleaseError::RegistryBorrowed)?;
        let index = registrations
            .iter()
            .position(|candidate| Rc::ptr_eq(candidate, registration))
            .ok_or(ReleaseError::StateMismatch)?;
        registrations.swap_remove(index);
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn test_len(&self) -> usize {
        self.registrations.borrow().len()
    }

    #[cfg(test)]
    pub(crate) fn test_with_borrowed(&self, f: impl FnOnce()) {
        let _registrations = self.registrations.borrow_mut();
        f();
    }

    #[cfg(test)]
    pub(crate) fn test_fail_next_release(&self, errno: i32) {
        self.release_failures.borrow_mut().push_back(errno);
    }
}

fn selected_bid_from_flags(flags: u32) -> io::Result<Bid> {
    io_uring::cqueue::buffer_select(flags).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "completion did not include a selected buffer id",
        )
    })
}

/// [`Builder`] is used to create a new [`RecvBufRing`].
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
    /// The number will be made a power of 2, and will be the maximum of the `ring_entries` setting
    /// and the `buf_cnt` setting. The interface will enforce a maximum of 2^15 (32768).
    pub fn ring_entries(mut self, ring_entries: u16) -> Builder {
        self.ring_entries = ring_entries;
        self
    }

    /// The number of buffers to allocate. If left zero, the `ring_entries` value will be used.
    pub fn buf_cnt(mut self, buf_cnt: u16) -> Builder {
        self.buf_cnt = buf_cnt;
        self
    }

    /// The length to be preallocated for each buffer.
    pub fn buf_len(mut self, buf_len: usize) -> Builder {
        self.buf_len = buf_len;
        self
    }

    /// Return a [`RecvBufRing`].
    ///
    /// # Errors
    ///
    /// Returns an error for an invalid ring configuration, allocation failure,
    /// duplicate buffer-group ID, or kernel registration failure.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub fn build(&self) -> io::Result<RecvBufRing> {
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
            return Err(io::Error::other("ring_entries exceeded 32768"));
        }

        // Requirement of the interface is the ring entries is a power of two, making its and our
        // wrap calculation trivial.
        b.ring_entries = b.ring_entries.next_power_of_two();

        let handle = crate::Handle::current();
        let storage = Rc::new(BufRingStorage::new(
            b.bgid,
            b.ring_entries,
            b.buf_cnt,
            b.buf_len,
        )?);
        let reservation = handle.reserve_buf_ring(Rc::clone(&storage))?;
        reservation.register(|submitter| storage.register(submitter))?;
        let registration = reservation.commit();
        Ok(RecvBufRing::new(registration, storage))
    }
}

struct InnerBufRing {
    registration: BufRingRegistration,
    storage: Rc<BufRingStorage>,
}

impl ops::Deref for InnerBufRing {
    type Target = BufRingStorage;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

pub(crate) struct BufRingStorage {
    // All remaining fields are constant once the struct is instantiated except the Cell fields.
    bgid: Bgid,

    ring_entries_mask: u16, // Invariant one less than ring_entries which is > 0, power of 2, max 2^15 (32768).

    buf_cnt: u16,   // Invariants: > 0, <= ring_entries.
    buf_len: usize, // Invariant: > 0.

    // `ring_start` holds the memory allocated for the buf_ring, the ring of entries describing
    // the buffers being made available to the uring interface for this buf group id.
    ring_start: AnonymousMmap,

    // The kernel writes through pointers published to the buffer ring while the
    // storage is shared through `Rc`. Keep that interior-mutation boundary
    // explicit; safe slices are created only after the tracker transfers the
    // selected BID into an owned token.
    buf_list: Vec<KernelBuffer>,

    // The next unpublished ring-tail value. It advances during initial
    // publication and when authenticated owners return buffers.
    local_tail: Cell<u16>,

    // Ring-global ownership and publication order. CQEs may arrive in a
    // different order from buffer selection, so a raw BID is not itself proof
    // of ownership.
    tracker: RefCell<PublicationTracker>,
}

impl BufRingStorage {
    fn new(bgid: Bgid, ring_entries: u16, buf_cnt: u16, buf_len: usize) -> io::Result<Self> {
        // The ring must have room for every buffer, and its entry count must be
        // a nonzero power of two. Buffer lengths are published as u32 values.
        if (buf_cnt == 0)
            || (buf_cnt > ring_entries)
            || (buf_len == 0)
            || (buf_len > u32::MAX as usize)
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

        let buf_list = (0..buf_cnt).map(|_| KernelBuffer::new(buf_len)).collect();

        let ring_entries_mask = ring_entries - 1;
        assert!((ring_entries & ring_entries_mask) == 0);

        let buf_ring = Self {
            bgid,
            ring_entries_mask,
            buf_cnt,
            buf_len,
            ring_start,
            buf_list,
            local_tail: Cell::new(0),
            tracker: RefCell::new(PublicationTracker::new(buf_cnt)),
        };

        Ok(buf_ring)
    }

    fn register(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        let bgid = self.bgid;

        let res = loop {
            let result = unsafe {
                submitter.register_buf_ring_with_flags(
                    self.ring_start.as_ptr() as _,
                    self.ring_entries(),
                    bgid,
                    0,
                )
            };
            match result {
                Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
                result => break result,
            }
        };

        if let Err(e) = res {
            match e.raw_os_error() {
                Some(libc::EINVAL) => {
                    // using buf_ring requires kernel 5.19 or greater.
                    return Err(io::Error::other(format!(
                        "buf_ring.register returned {}, most likely indicating this kernel is not 5.19+",
                        e
                    )));
                }
                Some(libc::EEXIST) => {
                    // Registering a duplicate bgid is not allowed. There is an `unregister`
                    // operations that can remove the first, but care must be taken that there
                    // are no outstanding operations that will still return a buffer from that
                    // one.
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
        };

        for bid in 0..self.buf_cnt {
            self.buf_ring_push(bid);
        }
        self.buf_ring_sync();

        res
    }

    // Returns the buffer group id.
    fn bgid(&self) -> Bgid {
        self.bgid
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.tracker.borrow().is_poisoned() {
            Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "buffer ring is quarantined after an ownership accounting failure",
            ))
        } else {
            Ok(())
        }
    }

    fn tracker_error(error: TrackerError) -> io::Error {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("buffer ring ownership accounting failed: {error}"),
        )
    }

    fn poison(&self, message: impl Into<String>) -> io::Error {
        self.tracker.borrow_mut().poison();
        io::Error::new(io::ErrorKind::InvalidData, message.into())
    }

    fn return_buffer(&self, token: BufferToken) {
        let result = self.tracker.borrow_mut().return_one(token);
        if let Err(error) = result {
            warn!(
                target: "norn_uring::bufring",
                "quarantining returned buffer for group {}: {}",
                self.bgid,
                error
            );
            return;
        }

        self.buf_ring_push(token.bid());
        self.buf_ring_sync();
    }

    fn return_bundle(&self, claim: &BundleClaim) {
        let result = self.tracker.borrow_mut().return_bundle(claim);
        if let Err(error) = result {
            warn!(
                target: "norn_uring::bufring",
                "quarantining returned buffer bundle for group {}: {}",
                self.bgid,
                error
            );
            return;
        }

        for token in claim.iter() {
            self.buf_ring_push(token.bid());
        }
        self.buf_ring_sync();
    }

    // Returns the buffer the uring interface picked from the buf_ring for the completion result
    // represented by the res and flags.
    fn get_buf(&self, buf_ring: RecvBufRing, res: u32, flags: u32) -> io::Result<BufRingBuf> {
        // Claim the selected buffer and retain `buf_ring` in the returned owner.
        let bid = match selected_bid_from_flags(flags) {
            Ok(bid) => bid,
            Err(error) if res == 0 => return Err(error),
            Err(error) => {
                return Err(self.poison(format!(
                    "positive completion did not identify its selected buffer: {error}"
                )));
            }
        };
        let len = res as usize;
        if len > self.buf_len {
            return Err(self.poison(format!(
                "completion reported {len} bytes for a {}-byte selected buffer",
                self.buf_len
            )));
        }

        let token = self
            .tracker
            .borrow_mut()
            .claim_one(bid)
            .map_err(Self::tracker_error)?;
        Ok(BufRingBuf::new(buf_ring, token, len))
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
                return Ok(BufRingBufBundle::empty());
            }
            return Err(
                self.poison("positive bundle completion did not include a selected buffer id")
            );
        };

        let needed = if total_len == 0 {
            1
        } else {
            total_len.div_ceil(self.buf_len)
        };
        let claim = self
            .tracker
            .borrow_mut()
            .claim_bundle(first_bid, needed)
            .map_err(Self::tracker_error)?;
        Ok(BufRingBufBundle::new(buf_ring, claim, total_len))
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

    // Append `bid` at the local tail. The publication tracker must authenticate
    // the buffer before this method is called.
    fn buf_ring_push(&self, bid: Bid) {
        assert!(bid < self.buf_cnt);

        // N.B. The uring buf_ring indexing mechanism calls for the tail values to exceed the
        // actual number of ring entries. This allows the uring interface to distinguish between
        // empty and full buf_rings. As a result, the ring mask is only applied to the index used
        // solely for computing the ring entry; it does not apply to the tail value.

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
        let shared_tail =
            unsafe { types::BufRingEntry::tail(self.ring_start.as_ptr() as *const BufRingEntry) }
                as *const AtomicU16;
        unsafe {
            (*shared_tail).store(self.local_tail.get(), atomic::Ordering::Release);
        }
    }
}

struct KernelBuffer {
    bytes: Box<[UnsafeCell<u8>]>,
}

impl KernelBuffer {
    fn new(len: usize) -> Self {
        let mut bytes = Vec::with_capacity(len);
        bytes.resize_with(len, || UnsafeCell::new(0));
        Self {
            bytes: bytes.into_boxed_slice(),
        }
    }

    fn as_ptr(&self) -> *const u8 {
        self.bytes.as_ptr().cast::<u8>()
    }
}

/// An anonymous region of page-aligned, zero-filled memory mapped using
/// `mmap(2)` with no file backing.
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
    use super::{selected_bid_from_flags, BufRingStorage, RecvBufRing};
    use std::io;
    use std::rc::Rc;

    use norn_executor::park::{Park, ParkMode};

    use crate::Driver;

    #[test]
    fn selected_bid_requires_buffer_select_flag() {
        let err = selected_bid_from_flags(0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn buffer_length_must_fit_the_kernel_ring_entry() {
        let error = BufRingStorage::new(0, 1, 1, u32::MAX as usize + 1)
            .err()
            .expect("oversized buffer length must be rejected");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn stale_return_quarantines_without_advancing_the_kernel_tail() -> io::Result<()> {
        let storage = BufRingStorage::new(0, 1, 1, 8)?;
        let stale = storage.tracker.borrow_mut().claim_one(0).unwrap();
        storage.return_buffer(stale);
        assert_eq!(storage.local_tail.get(), 1);

        let current = storage.tracker.borrow_mut().claim_one(0).unwrap();
        storage.return_buffer(stale);
        assert!(storage.ensure_healthy().is_err());
        assert_eq!(storage.local_tail.get(), 1);

        storage.return_buffer(current);
        assert_eq!(storage.local_tail.get(), 1);
        Ok(())
    }

    #[test]
    fn borrowed_driver_defers_buffer_ring_reclamation() -> io::Result<()> {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let _guard = driver.enter();
        let ring = RecvBufRing::builder(31_800)
            .ring_entries(2)
            .buf_cnt(2)
            .buf_len(8)
            .build()?;
        let storage = Rc::downgrade(&ring.rc.storage);

        handle.test_with_ring_borrowed_mut(move || drop(ring));

        assert_eq!(handle.test_registered_buf_rings(), 1);
        assert!(storage.upgrade().is_some());

        driver.park(ParkMode::NoPark)?;

        assert_eq!(handle.test_registered_buf_rings(), 0);
        assert!(storage.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn borrowed_registry_still_records_the_release_request() -> io::Result<()> {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let _guard = driver.enter();
        let ring = RecvBufRing::builder(31_802)
            .ring_entries(2)
            .buf_cnt(2)
            .buf_len(8)
            .build()?;
        let storage = Rc::downgrade(&ring.rc.storage);

        handle.test_with_buf_ring_registry_borrowed(move || drop(ring));

        assert_eq!(handle.test_registered_buf_rings(), 1);
        assert!(storage.upgrade().is_some());

        driver.park(ParkMode::NoPark)?;

        assert_eq!(handle.test_registered_buf_rings(), 0);
        assert!(storage.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn failed_unregister_is_retried_without_abandoning_storage() -> io::Result<()> {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let _guard = driver.enter();
        let ring = RecvBufRing::builder(31_801)
            .ring_entries(2)
            .buf_cnt(2)
            .buf_len(8)
            .build()?;
        let storage = Rc::downgrade(&ring.rc.storage);
        handle.test_fail_next_buf_ring_release(libc::EIO);

        drop(ring);

        assert_eq!(handle.test_registered_buf_rings(), 1);
        assert!(storage.upgrade().is_some());

        driver.park(ParkMode::NoPark)?;

        assert_eq!(handle.test_registered_buf_rings(), 0);
        assert!(storage.upgrade().is_none());
        Ok(())
    }

    #[test]
    fn persistent_unregister_failure_waits_for_a_registration_boundary() -> io::Result<()> {
        let mut driver = Driver::new(io_uring::IoUring::builder(), 8)?;
        let handle = driver.handle();
        let _guard = driver.enter();
        let ring = RecvBufRing::builder(31_803)
            .ring_entries(2)
            .buf_cnt(2)
            .buf_len(8)
            .build()?;
        let storage = Rc::downgrade(&ring.rc.storage);
        handle.test_fail_next_buf_ring_release(libc::EIO);
        handle.test_fail_next_buf_ring_release(libc::EIO);

        drop(ring);
        driver.park(ParkMode::NoPark)?;
        assert!(storage.upgrade().is_some());

        driver.park(ParkMode::NoPark)?;
        assert!(storage.upgrade().is_some());

        let replacement = RecvBufRing::builder(31_804)
            .ring_entries(2)
            .buf_cnt(2)
            .buf_len(8)
            .build()?;
        assert!(storage.upgrade().is_none());
        assert_eq!(handle.test_registered_buf_rings(), 1);

        drop(replacement);
        assert_eq!(handle.test_registered_buf_rings(), 0);
        Ok(())
    }
}
