//! Epoll instances driven through `io_uring`.
//!
//! [`Epoll`] wraps an epoll descriptor whose interest list is modified with
//! `IORING_OP_EPOLL_CTL` (Linux 5.6) and whose ready list is collected with
//! `IORING_OP_EPOLL_WAIT` (Linux 6.15). Older kernels reject the corresponding
//! requests with `EINVAL`. One wait reports readiness across every registered
//! descriptor, which suits code organised around epoll readiness sets.
//!
//! # Example
//!
//! ```no_run
//! use norn_uring::epoll::{Epoll, Events, Interest};
//! use norn_uring::net::UdpSocket;
//!
//! # async fn run() -> std::io::Result<()> {
//! let socket = UdpSocket::bind("127.0.0.1:0".parse().unwrap()).await?;
//! let epoll = Epoll::new()?;
//! epoll.add(socket.as_uring_fd(), Interest::READABLE, 1).await?;
//!
//! let (result, events) = epoll.wait(Events::with_capacity(16)).await;
//! result?;
//! for event in events.iter() {
//!     assert_eq!(event.token(), 1);
//!     assert!(event.readiness().is_readable());
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Registration accepts only driver-bound descriptors ([`UringFd`]), which the
//! request retains until the kernel has finished with the target. As with
//! `epoll(7)`, the kernel removes a registration automatically once every
//! descriptor referring to the registered open file description is closed.
//!
//! Only portable readiness interest, edge-triggered mode, and one-shot mode
//! are exposed. `EPOLLEXCLUSIVE` and `EPOLLWAKEUP` are intentionally omitted.
//! A multishot wait is not exposed.

use std::fmt;
use std::io;
use std::ops::BitOr;
use std::ptr;

use io_uring::{opcode, types};

use crate::fd::{NornFd, UringFd};
use crate::operation::{CQEResult, Operation, Singleshot};

/// An epoll instance.
pub struct Epoll {
    fd: UringFd,
}

impl fmt::Debug for Epoll {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Epoll").finish_non_exhaustive()
    }
}

impl Epoll {
    /// Create a new epoll instance.
    ///
    /// The descriptor is created with `epoll_create1(EPOLL_CLOEXEC)` and is
    /// bound to the current driver.
    ///
    /// # Errors
    ///
    /// Returns an error if the operating system cannot create the instance.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub fn new() -> io::Result<Self> {
        let handle = crate::Handle::current();
        // Safety: `epoll_create1` has no memory-safety preconditions.
        let fd = unsafe { libc::epoll_create1(libc::EPOLL_CLOEXEC) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            fd: UringFd::from_fd_on(fd, handle),
        })
    }

    /// Register `fd` with the given interest and token.
    ///
    /// The token is returned unchanged in each [`Event`] for this registration.
    /// The returned request is lazy and can be linked or given a timeout.
    ///
    /// # Errors
    ///
    /// Returns `EEXIST` if `fd` is already registered, `EPERM` if `fd` does not
    /// support readiness notification (for example, a regular file), or another
    /// error reported by `epoll_ctl(2)`.
    pub fn add(
        &self,
        fd: &UringFd,
        interest: Interest,
        token: u64,
    ) -> impl crate::Request<Output = io::Result<()>> {
        let op = EpollCtl::new(
            self.fd.lease(),
            fd.lease(),
            libc::EPOLL_CTL_ADD,
            interest,
            token,
        );
        self.fd.submit(op)
    }

    /// Replace the interest and token of a registered `fd`.
    ///
    /// This also rearms a one-shot registration.
    ///
    /// # Errors
    ///
    /// Returns `ENOENT` if `fd` is not registered or another error reported by
    /// `epoll_ctl(2)`.
    pub fn modify(
        &self,
        fd: &UringFd,
        interest: Interest,
        token: u64,
    ) -> impl crate::Request<Output = io::Result<()>> {
        let op = EpollCtl::new(
            self.fd.lease(),
            fd.lease(),
            libc::EPOLL_CTL_MOD,
            interest,
            token,
        );
        self.fd.submit(op)
    }

    /// Remove `fd` from the interest list.
    ///
    /// # Errors
    ///
    /// Returns `ENOENT` if `fd` is not registered or another error reported by
    /// `epoll_ctl(2)`.
    pub fn delete(&self, fd: &UringFd) -> impl crate::Request<Output = io::Result<()>> {
        let op = EpollCtl::new(
            self.fd.lease(),
            fd.lease(),
            libc::EPOLL_CTL_DEL,
            Interest::NONE,
            0,
        );
        self.fd.submit(op)
    }

    /// Wait until at least one registered descriptor is ready.
    ///
    /// Up to [`Events::capacity`] ready events are written into `events`,
    /// replacing its previous contents, and the buffer is returned with the
    /// number of events received. The request does not complete with zero
    /// events; use [`Request::timeout`](crate::Request::timeout) to bound the
    /// wait.
    ///
    /// Collecting events consumes edge-triggered and one-shot notifications. If
    /// the request is dropped or cancelled after the kernel has collected
    /// events, those notifications are lost.
    ///
    /// # Errors
    ///
    /// Returns `EINVAL` if `events` has zero capacity or the kernel does not
    /// support `IORING_OP_EPOLL_WAIT`, `ECANCELED` if the request is cancelled,
    /// or another error reported by `epoll_wait(2)`.
    pub fn wait(
        &self,
        events: Events,
    ) -> impl crate::Request<Output = (io::Result<usize>, Events)> {
        self.fd.submit(EpollWait::new(self.fd.lease(), events))
    }

    /// Return the underlying driver-bound descriptor.
    pub fn as_uring_fd(&self) -> &UringFd {
        &self.fd
    }

    /// Consume this instance and return its driver-bound descriptor.
    pub fn into_uring_fd(self) -> UringFd {
        self.fd
    }

    /// Close the epoll instance.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while another owner or operation
    /// retains the descriptor, or another I/O error if close fails.
    pub async fn close(self) -> io::Result<()> {
        self.fd.close().await
    }
}

/// Readiness interest and trigger mode for an epoll registration.
///
/// Error and hang-up conditions are always reported and need not be requested.
/// Interests are combined with `|`.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Interest(u32);

impl Interest {
    /// Report only error and hang-up conditions.
    pub const NONE: Self = Self(0);
    /// Readable (`EPOLLIN`).
    pub const READABLE: Self = Self(libc::EPOLLIN as u32);
    /// Writable (`EPOLLOUT`).
    pub const WRITABLE: Self = Self(libc::EPOLLOUT as u32);
    /// Exceptional condition such as out-of-band data (`EPOLLPRI`).
    pub const PRIORITY: Self = Self(libc::EPOLLPRI as u32);
    /// The peer closed its writing half of a stream socket (`EPOLLRDHUP`).
    pub const READ_CLOSED: Self = Self(libc::EPOLLRDHUP as u32);

    /// Report readiness transitions instead of readiness levels (`EPOLLET`).
    #[must_use]
    pub const fn edge_triggered(self) -> Self {
        Self(self.0 | libc::EPOLLET as u32)
    }

    /// Disable the registration after one event until it is rearmed with
    /// [`Epoll::modify`] (`EPOLLONESHOT`).
    #[must_use]
    pub const fn oneshot(self) -> Self {
        Self(self.0 | libc::EPOLLONESHOT as u32)
    }

    /// Return the raw `epoll_event.events` bits.
    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl BitOr for Interest {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl fmt::Debug for Interest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Interest")
            .field(&format_args!("{:#x}", self.0))
            .finish()
    }
}

/// The readiness reported for one registration.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Readiness(u32);

impl Readiness {
    /// Return whether the descriptor is readable.
    pub const fn is_readable(self) -> bool {
        self.0 & libc::EPOLLIN as u32 != 0
    }

    /// Return whether the descriptor is writable.
    pub const fn is_writable(self) -> bool {
        self.0 & libc::EPOLLOUT as u32 != 0
    }

    /// Return whether an exceptional condition is pending.
    pub const fn is_priority(self) -> bool {
        self.0 & libc::EPOLLPRI as u32 != 0
    }

    /// Return whether the peer closed its writing half.
    pub const fn is_read_closed(self) -> bool {
        self.0 & libc::EPOLLRDHUP as u32 != 0
    }

    /// Return whether an error condition is pending.
    pub const fn is_error(self) -> bool {
        self.0 & libc::EPOLLERR as u32 != 0
    }

    /// Return whether the descriptor was hung up.
    pub const fn is_hangup(self) -> bool {
        self.0 & libc::EPOLLHUP as u32 != 0
    }

    /// Return the raw `epoll_event.events` bits.
    pub const fn bits(self) -> u32 {
        self.0
    }
}

impl fmt::Debug for Readiness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Readiness")
            .field(&format_args!("{:#x}", self.0))
            .finish()
    }
}

/// One ready registration reported by [`Epoll::wait`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Event {
    token: u64,
    readiness: Readiness,
}

impl Event {
    /// Return the token supplied when the descriptor was registered.
    pub const fn token(&self) -> u64 {
        self.token
    }

    /// Return the reported readiness.
    pub const fn readiness(&self) -> Readiness {
        self.readiness
    }
}

/// An owned buffer of epoll events filled by [`Epoll::wait`].
pub struct Events {
    buf: Box<[libc::epoll_event]>,
    len: usize,
}

impl Events {
    /// Create an empty buffer that can receive up to `capacity` events.
    pub fn with_capacity(capacity: usize) -> Self {
        let buf = vec![libc::epoll_event { events: 0, u64: 0 }; capacity].into_boxed_slice();
        Self { buf, len: 0 }
    }

    /// Return the maximum number of events received by one wait.
    pub fn capacity(&self) -> usize {
        self.buf.len()
    }

    /// Return the number of events received by the last wait.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the buffer holds no events.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Discard the received events.
    pub fn clear(&mut self) {
        self.len = 0;
    }

    /// Iterate over the received events.
    pub fn iter(&self) -> impl Iterator<Item = Event> + '_ {
        self.buf[..self.len].iter().map(|event| {
            // Copy out of the potentially packed C structure.
            let event = *event;
            Event {
                token: event.u64,
                readiness: Readiness(event.events),
            }
        })
    }
}

impl fmt::Debug for Events {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Events")
            .field("capacity", &self.capacity())
            .field("events", &self.iter().collect::<Vec<_>>())
            .finish()
    }
}

struct EpollCtl {
    epfd: NornFd,
    target: NornFd,
    op: libc::c_int,
    event: libc::epoll_event,
}

impl EpollCtl {
    fn new(epfd: NornFd, target: NornFd, op: libc::c_int, interest: Interest, token: u64) -> Self {
        Self {
            epfd,
            target,
            op,
            event: libc::epoll_event {
                events: interest.0,
                u64: token,
            },
        }
    }
}

// Safety: both descriptors are retained through the terminal CQE, and the
// event read by the kernel is stored inline at a stable address. A completed
// control request owns no kernel resource.
unsafe impl Operation for EpollCtl {
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let event = ptr::addr_of!(self.event).cast::<types::epoll_event>();
        Ok(opcode::EpollCtl::new(self.epfd.fd(), self.target.fd(), self.op, event).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result.into_result().map(drop)
    }
}

impl Singleshot for EpollCtl {
    type Output = io::Result<()>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

struct EpollWait {
    epfd: NornFd,
    events: Events,
}

impl EpollWait {
    fn new(epfd: NornFd, mut events: Events) -> Self {
        events.clear();
        Self { epfd, events }
    }
}

// Safety: the boxed event array is owned by the operation, so its address and
// length remain stable through the terminal CQE, and no Rust reference to it
// is created while the kernel may write it. A completion owns no resource.
unsafe impl Operation for EpollWait {
    type Completion = io::Result<usize>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let max_events = u32::try_from(self.events.capacity()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "epoll event capacity exceeds u32::MAX",
            )
        })?;
        let events = self.events.buf.as_mut_ptr().cast::<types::epoll_event>();
        Ok(opcode::EpollWait::new(self.epfd.fd(), events, max_events).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let received = result.into_result()? as usize;
        if received > self.events.capacity() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "epoll_wait reported more events than requested",
            ));
        }
        Ok(received)
    }
}

impl Singleshot for EpollWait {
    type Output = (io::Result<usize>, Events);

    fn complete(self, completion: Self::Completion) -> Self::Output {
        let mut events = self.events;
        if let Ok(received) = completion {
            events.len = received;
        }
        (completion, events)
    }
}

#[cfg(test)]
mod tests {
    use std::os::fd::RawFd;

    use super::*;

    fn pipe() -> [RawFd; 2] {
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);
        fds
    }

    fn sqe_bytes(entry: &io_uring::squeue::Entry) -> [u8; 64] {
        unsafe { std::mem::transmute_copy(entry) }
    }

    #[test]
    fn interest_bits_match_epoll_flags() {
        let interest = (Interest::READABLE | Interest::WRITABLE).edge_triggered();
        assert_eq!(
            interest.bits(),
            (libc::EPOLLIN | libc::EPOLLOUT | libc::EPOLLET) as u32
        );
        assert_eq!(
            Interest::PRIORITY.oneshot().bits(),
            (libc::EPOLLPRI | libc::EPOLLONESHOT) as u32
        );
        assert_eq!(Interest::READ_CLOSED.bits(), libc::EPOLLRDHUP as u32);
        assert_eq!(Interest::NONE.bits(), 0);
    }

    #[test]
    fn readiness_decodes_epoll_flags() {
        let readiness = Readiness((libc::EPOLLIN | libc::EPOLLHUP) as u32);
        assert!(readiness.is_readable());
        assert!(readiness.is_hangup());
        assert!(!readiness.is_writable());
        assert!(!readiness.is_error());
        assert!(!readiness.is_priority());
        assert!(!readiness.is_read_closed());
    }

    #[test]
    fn configures_epoll_ctl_opcode() {
        let [epfd, target] = pipe();
        let mut op = EpollCtl::new(
            NornFd::from_fd(epfd),
            NornFd::from_fd(target),
            libc::EPOLL_CTL_MOD,
            Interest::READABLE.oneshot(),
            0xfeed,
        );

        let entry = op.configure().unwrap();

        assert_eq!(entry.get_opcode(), opcode::EpollCtl::CODE.into());
        let bytes = sqe_bytes(&entry);
        assert_eq!(i32::from_ne_bytes(bytes[4..8].try_into().unwrap()), epfd);
        assert_eq!(
            u64::from_ne_bytes(bytes[8..16].try_into().unwrap()),
            target as u64
        );
        assert_eq!(
            u64::from_ne_bytes(bytes[16..24].try_into().unwrap()),
            ptr::addr_of!(op.event) as u64
        );
        assert_eq!(
            i32::from_ne_bytes(bytes[24..28].try_into().unwrap()),
            libc::EPOLL_CTL_MOD
        );
        let (token, flags) = (op.event.u64, op.event.events);
        assert_eq!(token, 0xfeed);
        assert_eq!(flags, (libc::EPOLLIN | libc::EPOLLONESHOT) as u32);
    }

    #[test]
    fn configures_epoll_wait_opcode_and_clears_stale_events() {
        let [epfd, other] = pipe();
        let mut events = Events::with_capacity(8);
        events.len = 3;
        let mut op = EpollWait::new(NornFd::from_fd(epfd), events);
        assert!(op.events.is_empty());

        let entry = op.configure().unwrap();

        assert_eq!(entry.get_opcode(), opcode::EpollWait::CODE.into());
        let bytes = sqe_bytes(&entry);
        assert_eq!(i32::from_ne_bytes(bytes[4..8].try_into().unwrap()), epfd);
        assert_eq!(u64::from_ne_bytes(bytes[8..16].try_into().unwrap()), 0);
        assert_eq!(
            u64::from_ne_bytes(bytes[16..24].try_into().unwrap()),
            op.events.buf.as_ptr() as u64
        );
        assert_eq!(u32::from_ne_bytes(bytes[24..28].try_into().unwrap()), 8);
        assert_eq!(u32::from_ne_bytes(bytes[28..32].try_into().unwrap()), 0);
        drop(op);
        assert_eq!(unsafe { libc::close(other) }, 0);
    }

    #[test]
    fn epoll_wait_completion_sets_received_events() {
        let [epfd, other] = pipe();
        let mut op = EpollWait::new(NornFd::from_fd(epfd), Events::with_capacity(2));
        op.events.buf[0] = libc::epoll_event {
            events: libc::EPOLLOUT as u32,
            u64: 11,
        };
        op.events.buf[1] = libc::epoll_event {
            events: libc::EPOLLIN as u32,
            u64: 12,
        };

        let completion = unsafe { op.reap(CQEResult::new(Ok(1), 0)) };
        let (result, events) = op.complete(completion);

        assert_eq!(result.unwrap(), 1);
        let received: Vec<_> = events.iter().collect();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].token(), 11);
        assert!(received[0].readiness().is_writable());
        assert_eq!(unsafe { libc::close(other) }, 0);
    }

    #[test]
    fn epoll_wait_rejects_oversized_completion() {
        let [epfd, other] = pipe();
        let mut op = EpollWait::new(NornFd::from_fd(epfd), Events::with_capacity(1));

        let completion = unsafe { op.reap(CQEResult::new(Ok(2), 0)) };
        let (result, events) = op.complete(completion);

        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(events.is_empty());
        assert_eq!(unsafe { libc::close(other) }, 0);
    }
}
