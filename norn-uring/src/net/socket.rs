//! Socket operations.
//!
//! [Socket] is the core socket type
//! used by both TCP and UDP sockets
use io_uring::opcode;
use io_uring::squeue::Flags;
use libc::{SOCK_CLOEXEC, SOCK_NONBLOCK};
use socket2::{Domain, Protocol, SockAddr, Type};
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::os::fd::{FromRawFd, IntoRawFd, OwnedFd};
use std::pin::Pin;
use std::task::{ready, Context, Poll as TaskPoll};

use crate::buf::{set_init_checked, StableBuf, StableBufMut};
use crate::bufring::{BufRingBuf, BufRingBufBundle, RecvBufRing};
use crate::fd::{NornFd, UringFd};
use crate::operation::{CQEResult, Multishot, Op, Operation, Singleshot};

pub(crate) struct RingRecvCompletion<T> {
    result: io::Result<T>,
    terminal: bool,
}

pin_project_lite::pin_project! {
    pub(crate) struct RecvRingStream {
        socket: Option<Socket>,
        ring: Option<RecvBufRing>,
        #[pin]
        current: Option<Op<RecvRingMulti>>,
        rearm: bool,
        done: bool,
    }
}

impl RecvRingStream {
    fn new(socket: &Socket, ring: &RecvBufRing) -> Self {
        let socket = socket.clone();
        let ring = ring.clone();
        let current = Some(socket.recv_ring_multi(&ring));
        Self {
            socket: Some(socket),
            ring: Some(ring),
            current,
            rearm: false,
            done: false,
        }
    }
}

impl futures_core::Stream for RecvRingStream {
    type Item = io::Result<BufRingBuf>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> TaskPoll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        if *this.done {
            return TaskPoll::Ready(None);
        }
        if this.current.is_none() && *this.rearm {
            *this.rearm = false;
            let socket = this.socket.as_ref().expect("receive socket missing");
            let ring = this.ring.as_ref().expect("receive buffer ring missing");
            this.current.set(Some(socket.recv_ring_multi(ring)));
        }
        let current = this
            .current
            .as_mut()
            .as_pin_mut()
            .expect("receive operation missing");
        match ready!(current.poll_next(cx)) {
            Some(completion) => {
                if completion.terminal {
                    this.current.set(None);
                }
                match completion.result {
                    Ok(buffer) => {
                        *this.rearm = completion.terminal;
                        TaskPoll::Ready(Some(Ok(buffer)))
                    }
                    Err(err) => {
                        if completion.terminal {
                            *this.done = true;
                            this.socket.take();
                            this.ring.take();
                        }
                        TaskPoll::Ready(Some(Err(err)))
                    }
                }
            }
            None => {
                *this.done = true;
                this.current.set(None);
                this.socket.take();
                this.ring.take();
                TaskPoll::Ready(None)
            }
        }
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct RecvRingBundleStream {
        socket: Option<Socket>,
        ring: Option<RecvBufRing>,
        flags: i32,
        #[pin]
        current: Option<Op<RecvRingBundleMulti>>,
        rearm: bool,
        done: bool,
    }
}

impl RecvRingBundleStream {
    fn new(socket: &Socket, ring: &RecvBufRing, flags: i32) -> Self {
        let socket = socket.clone();
        let ring = ring.clone();
        let current = Some(socket.recv_ring_bundle_multi_with_flags(&ring, flags));
        Self {
            socket: Some(socket),
            ring: Some(ring),
            flags,
            current,
            rearm: false,
            done: false,
        }
    }
}

impl futures_core::Stream for RecvRingBundleStream {
    type Item = io::Result<BufRingBufBundle>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> TaskPoll<Option<Self::Item>> {
        let mut this = self.as_mut().project();
        if *this.done {
            return TaskPoll::Ready(None);
        }
        if this.current.is_none() && *this.rearm {
            *this.rearm = false;
            let socket = this.socket.as_ref().expect("receive socket missing");
            let ring = this.ring.as_ref().expect("receive buffer ring missing");
            this.current.set(Some(
                socket.recv_ring_bundle_multi_with_flags(ring, *this.flags),
            ));
        }
        let current = this
            .current
            .as_mut()
            .as_pin_mut()
            .expect("bundle receive operation missing");
        match ready!(current.poll_next(cx)) {
            Some(completion) => {
                if completion.terminal {
                    this.current.set(None);
                }
                match completion.result {
                    Ok(bundle) => {
                        *this.rearm = completion.terminal;
                        TaskPoll::Ready(Some(Ok(bundle)))
                    }
                    Err(err) => {
                        if completion.terminal {
                            *this.done = true;
                            this.socket.take();
                            this.ring.take();
                        }
                        TaskPoll::Ready(Some(Err(err)))
                    }
                }
            }
            None => {
                *this.done = true;
                this.current.set(None);
                this.socket.take();
                this.ring.take();
                TaskPoll::Ready(None)
            }
        }
    }
}

fn reap_owned_fd(result: CQEResult) -> io::Result<OwnedFd> {
    result.into_result().map(|fd| {
        // Safety: successful `Socket` and `Accept` CQEs return a newly owned
        // descriptor. `reap_operation` converts only non-negative `i32` results
        // to `u32`.
        unsafe { OwnedFd::from_raw_fd(fd as i32) }
    })
}

fn bind_owned_fd(fd: OwnedFd) -> NornFd {
    NornFd::from_fd(fd.into_raw_fd())
}

fn invalid_socket_addr_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "socket operation returned a non-inet socket address",
    )
}

fn no_source_addr_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "recvmsg did not return a source socket address",
    )
}

fn invalid_zc_notification_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "zerocopy send notification completion missing primary send result",
    )
}

fn validate_recv_bundle_flags(flags: i32) -> io::Result<()> {
    if flags & libc::MSG_TRUNC != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MSG_TRUNC is unsupported for receive bundles because the completion byte count does not identify how many buffers were selected",
        ));
    }
    Ok(())
}

fn validate_recv_multi_bundle_flags(flags: i32) -> io::Result<()> {
    validate_recv_bundle_flags(flags)?;
    if flags & libc::MSG_WAITALL != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "MSG_WAITALL is unsupported for multishot receive bundles",
        ));
    }
    Ok(())
}

fn complete_recv_buffer<B>(
    buf: &mut B,
    submitted_len: usize,
    reported_len: usize,
    flags: u32,
) -> io::Result<usize>
where
    B: StableBufMut,
{
    let init_len = if reported_len <= submitted_len {
        reported_len
    } else if flags & libc::MSG_TRUNC as u32 != 0 {
        submitted_len
    } else {
        reported_len
    };

    set_init_checked(buf, submitted_len, init_len, "receive")?;
    Ok(reported_len)
}

fn as_socket_addr(addr: &SockAddr) -> io::Result<SocketAddr> {
    addr.as_socket().ok_or_else(invalid_socket_addr_error)
}

fn as_socket_addr_or_peer(
    fd: &NornFd,
    addr: &SockAddr,
    msg_namelen: libc::socklen_t,
) -> io::Result<SocketAddr> {
    if msg_namelen == 0 {
        let sock = unsafe { socket2::Socket::from_raw_fd(fd.fd().0) };
        let sock = ManuallyDrop::new(sock);
        return as_socket_addr(&sock.peer_addr()?);
    }
    as_socket_addr(addr)
}

pub(crate) struct Socket {
    fd: UringFd,
}

impl Clone for Socket {
    fn clone(&self) -> Self {
        Self {
            fd: self.fd.clone_internal(),
        }
    }
}

impl Socket {
    pub(crate) fn from_fd(fd: NornFd) -> Self {
        Self {
            fd: UringFd::from_inner(fd),
        }
    }

    pub(crate) fn from_uring_fd(fd: UringFd) -> Self {
        Self { fd }
    }

    pub(crate) fn as_uring_fd(&self) -> &UringFd {
        &self.fd
    }

    pub(crate) fn into_uring_fd(self) -> UringFd {
        self.fd
    }

    pub(crate) async fn open(
        domain: Domain,
        socket_type: Type,
        protocol: Option<Protocol>,
    ) -> io::Result<Self> {
        let handle = crate::Handle::current();
        let op = OpenSocket {
            domain,
            socket_type,
            protocol,
        };
        let fd = handle.submit(op).await?;
        let this = Self::from_fd(fd);
        Ok(this)
    }

    pub(crate) async fn bind(
        addr: SocketAddr,
        domain: Domain,
        socket_type: Type,
    ) -> io::Result<Self> {
        Self::bind_with_reuse_port(addr, domain, socket_type, false).await
    }

    pub(crate) async fn bind_with_reuse_port(
        addr: SocketAddr,
        domain: Domain,
        socket_type: Type,
        reuse_port: bool,
    ) -> io::Result<Self> {
        let socket = Self::open(domain, socket_type, None).await?;
        if reuse_port {
            socket.set_reuse_port(true).await?;
        }
        let op = BindSocket::new(socket.fd.lease(), addr);
        socket.fd.submit(op).await?;
        Ok(socket)
    }

    pub(crate) async fn listen(&self, backlog: u32) -> io::Result<()> {
        let backlog = i32::try_from(backlog).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "listen backlog exceeds i32")
        })?;
        let op = ListenSocket::new(self.fd.lease(), backlog);
        self.fd.submit(op).await
    }

    pub(crate) async fn accept(&self) -> io::Result<(Self, SocketAddr)> {
        let op = Accept::<false>::new(self.fd.lease());
        let (fd, addr) = self.fd.submit(op).await?;
        let socket = Self::from_fd(fd);
        Ok((socket, addr))
    }

    pub(crate) fn accept_multi(&self) -> Op<Accept<true>> {
        let op = Accept::<true>::new(self.fd.lease());
        self.fd.submit(op)
    }

    pub(crate) async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let op = Connect::new(self.fd.lease(), addr);
        self.fd.submit(op).await?;
        Ok(())
    }

    #[track_caller]
    fn assert_bufring_driver(&self, ring: &RecvBufRing) {
        assert!(
            ring.same_driver(self.fd.handle()),
            "buffer ring and socket must target the same driver"
        );
    }

    pub(crate) fn recv_from_ring(&self, ring: &RecvBufRing) -> Op<RecvFromRing> {
        self.assert_bufring_driver(ring);
        let op = RecvFromRing::new(self.fd.lease(), ring.clone());
        self.fd.submit(op)
    }

    pub(crate) fn recv_from_ring_multi(&self, ring: &RecvBufRing) -> Op<RecvFromRingMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvFromRingMulti::new(self.fd.lease(), ring.clone());
        self.fd.submit(op)
    }

    pub(crate) fn recv_ring_stream(&self, ring: &RecvBufRing) -> RecvRingStream {
        RecvRingStream::new(self, ring)
    }

    pub(crate) fn recv_ring_bundle_stream(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> RecvRingBundleStream {
        RecvRingBundleStream::new(self, ring, flags)
    }

    pub(crate) fn recv_ring_multi(&self, ring: &RecvBufRing) -> Op<RecvRingMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvRingMulti::new(self.fd.lease(), ring.clone(), 0);
        self.fd.submit(op)
    }

    pub(crate) fn recv_ring_bundle(&self, ring: &RecvBufRing) -> Op<RecvRingBundle> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundle::new(self.fd.lease(), ring.clone(), 0);
        self.fd.submit(op)
    }

    pub(crate) fn recv_ring_bundle_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> Op<RecvRingBundle> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundle::new(self.fd.lease(), ring.clone(), flags);
        self.fd.submit(op)
    }

    pub(crate) fn recv_ring_bundle_multi_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> Op<RecvRingBundleMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundleMulti::new(self.fd.lease(), ring.clone(), flags);
        self.fd.submit(op)
    }

    pub(crate) async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        let mut buf = buf;
        if let Some(result) = self.try_recv_from(&mut buf, 0) {
            return (result, buf);
        }
        let op = RecvFrom::new(self.fd.lease(), buf, 0);
        self.fd.submit(op).await
    }

    pub(crate) async fn send_to<B>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        if let Some(result) = self.try_send_to(&buf, Some(addr), 0) {
            return (result, buf);
        }
        let op = SendTo::new(self.fd.lease(), buf, Some(addr), 0);
        self.fd.submit(op).await
    }

    pub(crate) async fn recv_from_with_flags<B>(
        &self,
        buf: B,
        flags: i32,
    ) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        let mut buf = buf;
        if let Some(result) = self.try_recv_from(&mut buf, flags) {
            return (result, buf);
        }
        let op = RecvFrom::new(self.fd.lease(), buf, flags as u32);
        self.fd.submit(op).await
    }

    pub(crate) fn recv<B>(&self, buf: B) -> Op<Recv<B>>
    where
        B: StableBufMut + 'static,
    {
        let op = Recv::new(self.fd.lease(), buf, 0);
        self.fd.submit(op)
    }

    pub(crate) fn send<B>(&self, buf: B) -> Op<Send<B>>
    where
        B: StableBuf + 'static,
    {
        let op = Send::new(self.fd.lease(), buf, 0);
        self.fd.submit(op)
    }

    pub(crate) fn recv_with_flags<B>(&self, buf: B, flags: i32) -> Op<Recv<B>>
    where
        B: StableBufMut + 'static,
    {
        let op = Recv::new(self.fd.lease(), buf, flags);
        self.fd.submit(op)
    }

    pub(crate) fn send_zc<B>(&self, buf: B) -> Op<SendZc<B>>
    where
        B: StableBuf + 'static,
    {
        let op = SendZc::new(self.fd.lease(), buf, 0);
        self.fd.submit(op)
    }

    pub(crate) fn send_msg_zc<B>(&self, buf: B) -> Op<SendMsgZc<B>>
    where
        B: StableBuf + 'static,
    {
        let op = SendMsgZc::new(self.fd.lease(), buf, 0);
        self.fd.submit(op)
    }

    pub(crate) async fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        let op = Shutdown::new(self.fd.lease(), how);
        self.fd.submit(op).await
    }

    pub(crate) fn poll_readiness<const MULTI: bool>(&self, events: u32) -> Op<Poll<MULTI>> {
        let op = Poll::<MULTI>::new(self.fd.lease(), events);
        self.fd.submit(op)
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        as_socket_addr(&self.as_socket().local_addr()?)
    }

    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        as_socket_addr(&self.as_socket().peer_addr()?)
    }

    pub(crate) fn as_socket(&self) -> ManuallyDrop<socket2::Socket> {
        let sock = unsafe { socket2::Socket::from_raw_fd(self.fd.fd().0) };
        ManuallyDrop::new(sock)
    }

    pub(crate) async fn close(self) -> io::Result<()> {
        self.fd.close().await
    }

    pub(crate) async fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        let size = i32::try_from(size).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "receive buffer size exceeds i32",
            )
        })?;
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_RCVBUF, size)
            .await
    }

    pub(crate) async fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        let size = i32::try_from(size).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "send buffer size exceeds i32")
        })?;
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_SNDBUF, size)
            .await
    }

    pub(crate) async fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        let reuse = if reuse { 1 } else { 0 };
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_REUSEADDR, reuse)
            .await
    }

    async fn set_reuse_port(&self, reuse: bool) -> io::Result<()> {
        let reuse = if reuse { 1 } else { 0 };
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_REUSEPORT, reuse)
            .await
    }

    pub(crate) async fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        let keepalive = if keepalive { 1 } else { 0 };
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_KEEPALIVE, keepalive)
            .await
    }

    pub(crate) async fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        let nodelay = if nodelay { 1 } else { 0 };
        self.set_sock_opt(libc::IPPROTO_TCP, libc::TCP_NODELAY, nodelay)
            .await
    }

    pub(crate) async fn set_zerocopy(&self, enabled: bool) -> io::Result<()> {
        let enabled = if enabled { 1 } else { 0 };
        self.set_sock_opt(libc::SOL_SOCKET, libc::SO_ZEROCOPY, enabled)
            .await
    }

    async fn set_sock_opt<T>(&self, level: i32, optname: i32, value: T) -> io::Result<()>
    where
        T: Copy + 'static,
    {
        let op = SetSockOpt::new(self.fd.lease(), level as u32, optname as u32, value);
        self.fd.submit(op).await
    }

    fn try_send_to<B>(
        &self,
        buf: &B,
        addr: Option<SocketAddr>,
        flags: i32,
    ) -> Option<io::Result<usize>>
    where
        B: StableBuf,
    {
        let fd = self.fd.fd().0;
        let (name, namelen) = match addr {
            Some(addr) => {
                let addr = SockAddr::from(addr);
                let rc = unsafe {
                    libc::sendto(
                        fd,
                        buf.stable_ptr().cast(),
                        buf.bytes_init(),
                        flags,
                        addr.as_ptr(),
                        addr.len(),
                    )
                };
                return direct_io_result(rc).map(|res| res.map(|n| n as usize));
            }
            None => (std::ptr::null(), 0),
        };
        let rc = unsafe {
            libc::sendto(
                fd,
                buf.stable_ptr().cast(),
                buf.bytes_init(),
                flags,
                name,
                namelen,
            )
        };
        direct_io_result(rc).map(|res| res.map(|n| n as usize))
    }

    fn try_recv_from<B>(&self, buf: &mut B, flags: i32) -> Option<io::Result<(usize, SocketAddr)>>
    where
        B: StableBufMut,
    {
        let fd = self.fd.fd().0;
        let submitted_len = buf.bytes_remaining();
        let addr = unsafe {
            SockAddr::try_init(|storage, len| {
                let n = libc::recvfrom(
                    fd,
                    buf.stable_ptr_mut().cast(),
                    submitted_len,
                    flags,
                    storage.cast(),
                    len,
                );
                if n >= 0 {
                    Ok(n)
                } else {
                    Err(io::Error::last_os_error())
                }
            })
        };
        match addr {
            Ok((n, addr)) => {
                if n == 0 && addr.len() == 0 {
                    return Some(Err(no_source_addr_error()));
                }
                let reported_len =
                    match complete_recv_buffer(buf, submitted_len, n as usize, flags as u32) {
                        Ok(reported_len) => reported_len,
                        Err(err) => return Some(Err(err)),
                    };
                Some(as_socket_addr(&addr).map(|addr| (reported_len, addr)))
            }
            Err(err) if err.kind() == io::ErrorKind::WouldBlock => None,
            Err(err) => Some(Err(err)),
        }
    }
}

fn direct_io_result(rc: libc::ssize_t) -> Option<io::Result<libc::ssize_t>> {
    if rc >= 0 {
        return Some(Ok(rc));
    }
    let err = io::Error::last_os_error();
    if err.kind() == io::ErrorKind::WouldBlock {
        None
    } else {
        Some(Err(err))
    }
}

pub(crate) const READ_FLAGS: i16 = read_flags() | common_flags();
pub(crate) const WRITE_FLAGS: i16 = write_flags() | common_flags();

const fn read_flags() -> i16 {
    libc::POLLIN | libc::POLLPRI
}

const fn common_flags() -> i16 {
    libc::POLLERR | libc::POLLHUP | libc::POLLNVAL
}

const fn write_flags() -> i16 {
    libc::POLLOUT
}

struct OpenSocket {
    domain: Domain,
    socket_type: Type,
    protocol: Option<Protocol>,
}

// Safety: the socket SQE contains only copied scalar arguments. `reap` converts
// every returned descriptor to `OwnedFd` before waking application code.
unsafe impl Operation for OpenSocket {
    type Completion = io::Result<OwnedFd>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let ty: i32 = self.socket_type.into();
        let ty = ty | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
        Ok(io_uring::opcode::Socket::new(
            self.domain.into(),
            ty,
            self.protocol.map(Into::into).unwrap_or(0),
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        reap_owned_fd(result)
    }
}

impl Singleshot for OpenSocket {
    type Output = io::Result<NornFd>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.map(bind_owned_fd)
    }
}

struct SendTo<B> {
    fd: NornFd,
    buf: B,
    addr: Option<SockAddr>,
    flags: u32,
    msghdr: MaybeUninit<libc::msghdr>,
    slices: MaybeUninit<[io::IoSlice<'static>; 1]>,
}

impl<B> SendTo<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B, addr: Option<SocketAddr>, flags: u32) -> Self {
        let addr = addr.map(SockAddr::from);
        Self {
            fd,
            buf,
            addr,
            flags,
            msghdr: MaybeUninit::zeroed(),
            slices: MaybeUninit::zeroed(),
        }
    }
}

// Safety: the owned stable buffer, socket address, msghdr, and iovec storage
// remain pinned and live through the terminal CQE.
unsafe impl<B> Operation for SendTo<B>
where
    B: StableBuf,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;

        // Initialize the slice.
        {
            let slice = io::IoSlice::new(unsafe {
                std::slice::from_raw_parts(this.buf.stable_ptr(), this.buf.bytes_init())
            });
            this.slices.write([slice]);
        }

        // Next we initialize the msghdr.
        let msghdr = this.msghdr.as_mut_ptr();
        {
            let slices = unsafe { this.slices.assume_init_mut() };
            unsafe {
                (*msghdr).msg_iov = slices.as_mut_ptr() as *mut _;
                (*msghdr).msg_iovlen = slices.len() as _;
            }
        }

        // Configure the address.
        match &this.addr {
            Some(addr) => unsafe {
                (*msghdr).msg_name = addr.as_ptr() as *mut libc::c_void;
                (*msghdr).msg_namelen = addr.len() as _;
            },
            None => unsafe {
                (*msghdr).msg_name = std::ptr::null_mut();
                (*msghdr).msg_namelen = 0;
            },
        };

        let msghdr = this.msghdr.as_ptr();
        Ok(opcode::SendMsg::new(this.fd.fd(), msghdr)
            .flags(this.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for SendTo<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, B);

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        (result.result.map(|v| v as usize), self.buf)
    }
}

struct RecvFrom<B> {
    fd: NornFd,
    buf: B,
    addr: SockAddr,
    flags: u32,
    submitted_len: usize,
    msghdr: MaybeUninit<libc::msghdr>,
    slices: MaybeUninit<[io::IoSliceMut<'static>; 1]>,
}

impl<B> RecvFrom<B>
where
    B: StableBufMut,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: u32) -> Self {
        // Safety: We won't read from the socket addr until it's initialized.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        let submitted_len = buf.bytes_remaining();
        Self {
            fd,
            buf,
            addr,
            flags,
            submitted_len,
            msghdr: MaybeUninit::zeroed(),
            slices: MaybeUninit::zeroed(),
        }
    }
}

// Safety: the owned mutable stable buffer, socket address, msghdr, and iovec
// storage remain pinned and exclusive through the terminal CQE.
unsafe impl<B> Operation for RecvFrom<B>
where
    B: StableBufMut,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;

        let ptr = this.buf.stable_ptr_mut();
        let len = this.submitted_len;
        let slice = io::IoSliceMut::new(unsafe { std::slice::from_raw_parts_mut(ptr, len) });
        // First we initialize the IoVecMut slice.
        this.slices.write([slice]);
        // Safety: We just initialized the slice.
        let slices = unsafe { this.slices.assume_init_mut() };

        // Next we initialize the msghdr.
        let msghdr = this.msghdr.as_mut_ptr();
        unsafe {
            (*msghdr).msg_iov = slices.as_mut_ptr().cast();
            (*msghdr).msg_iovlen = slices.len() as _;
            (*msghdr).msg_name = this.addr.as_ptr() as *mut libc::c_void;
            (*msghdr).msg_namelen = this.addr.len() as _;
        };

        // Finally we create the operation.
        Ok(opcode::RecvMsg::new(this.fd.fd(), msghdr)
            .flags(this.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for RecvFrom<B>
where
    B: StableBufMut,
{
    type Output = (io::Result<(usize, SocketAddr)>, B);

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let mut this = self;
        match result.result {
            Ok(bytes_read) => {
                // Safety: the msghdr was initialized when the sqe was configured.
                let msg_namelen = unsafe { this.msghdr.assume_init_ref().msg_namelen };
                if msg_namelen == 0 {
                    return (Err(no_source_addr_error()), this.buf);
                }
                // Safety: the kernel wrote at most `msg_namelen` bytes into `addr`.
                unsafe { this.addr.set_length(msg_namelen) };
                let addr = match as_socket_addr(&this.addr) {
                    Ok(addr) => addr,
                    Err(err) => return (Err(err), this.buf),
                };
                let mut buf = this.buf;
                let reported_len = match complete_recv_buffer(
                    &mut buf,
                    this.submitted_len,
                    bytes_read as usize,
                    this.flags,
                ) {
                    Ok(reported_len) => reported_len,
                    Err(err) => return (Err(err), buf),
                };
                (Ok((reported_len, addr)), buf)
            }
            Err(err) => (Err(err), this.buf),
        }
    }
}

#[derive(Debug)]
pub(crate) struct RecvFromRing {
    fd: NornFd,
    ring: RecvBufRing,
    addr: SockAddr,
    msghdr: MaybeUninit<libc::msghdr>,
}

impl RecvFromRing {
    pub(crate) fn new(fd: NornFd, ring: RecvBufRing) -> Self {
        // Safety: We won't read from the socket addr until it's initialized.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        Self {
            fd,
            ring,
            addr,
            msghdr: MaybeUninit::zeroed(),
        }
    }
}

// Safety: `NornFd` and `RecvBufRing` retain the socket and registered buffer
// group; inline recvmsg metadata remains pinned, and `reap` converts every
// selected buffer into an owned completion before waking application code.
unsafe impl Operation for RecvFromRing {
    type Completion = io::Result<BufRingBuf>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        self.ring.ensure_accepting_receives()?;
        let this = self;

        // Next we initialize the msghdr.
        let msghdr = this.msghdr.as_mut_ptr();
        unsafe {
            (*msghdr).msg_iov = std::ptr::null_mut();
            (*msghdr).msg_iovlen = 0;
            (*msghdr).msg_name = this.addr.as_ptr() as *mut libc::c_void;
            (*msghdr).msg_namelen = this.addr.len() as _;
        };

        // Finally we create the operation.
        Ok(opcode::RecvMsg::new(this.fd.fd(), msghdr)
            .buf_group(this.ring.bgid())
            .build()
            .flags(Flags::BUFFER_SELECT))
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let (result, flags) = result.into_parts();
        result.and_then(|n| self.ring.get_buf(n, flags))
    }
}

impl Singleshot for RecvFromRing {
    type Output = io::Result<(BufRingBuf, SocketAddr)>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        let mut this = self;
        let buf = completion?;
        // Safety: the msghdr was initialized when the sqe was configured.
        let msg_namelen = unsafe { this.msghdr.assume_init_ref().msg_namelen };
        // Safety: the kernel wrote at most `msg_namelen` bytes into `addr`.
        unsafe { this.addr.set_length(msg_namelen) };
        let addr = as_socket_addr_or_peer(&this.fd, &this.addr, msg_namelen)?;
        Ok((buf, addr))
    }
}

/// A bufring-backed receive buffer that exposes only payload bytes for `RecvMsgMulti`.
#[derive(Debug)]
pub struct RecvMsgRingBuf {
    buf: BufRingBuf,
    payload_offset: usize,
    payload_len: usize,
}

impl std::ops::Deref for RecvMsgRingBuf {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.payload()
    }
}

impl RecvMsgRingBuf {
    fn new(buf: BufRingBuf, payload_offset: usize, payload_len: usize) -> Self {
        Self {
            buf,
            payload_offset,
            payload_len,
        }
    }

    /// Returns the payload bytes from the received message.
    pub fn payload(&self) -> &[u8] {
        &self.buf[self.payload_offset..self.payload_offset + self.payload_len]
    }

    /// Returns the underlying full buffer including recvmsg metadata prefix.
    pub fn as_raw(&self) -> &[u8] {
        &self.buf
    }
}

unsafe impl StableBuf for RecvMsgRingBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.payload().as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.payload_len
    }
}

#[derive(Debug)]
pub(crate) struct RecvFromRingMulti {
    fd: NornFd,
    ring: RecvBufRing,
    addr: SockAddr,
    msghdr: MaybeUninit<libc::msghdr>,
}

impl RecvFromRingMulti {
    pub(crate) fn new(fd: NornFd, ring: RecvBufRing) -> Self {
        // Safety: We won't read from the socket addr until it's initialized by the kernel.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        Self {
            fd,
            ring,
            addr,
            msghdr: MaybeUninit::zeroed(),
        }
    }

    fn recv_item(
        &mut self,
        completion: io::Result<BufRingBuf>,
    ) -> io::Result<(RecvMsgRingBuf, SocketAddr)> {
        let buf = completion?;
        let msghdr = unsafe { self.msghdr.assume_init_ref() };
        let recvmsg = io_uring::types::RecvMsgOut::parse(&buf, msghdr).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid recvmsg multishot completion layout",
            )
        })?;
        let addr = if recvmsg.name_data().is_empty() {
            as_socket_addr_or_peer(&self.fd, &self.addr, 0)?
        } else {
            socket_addr_from_name(recvmsg.name_data())?
        };
        let base_ptr = buf[..].as_ptr() as usize;
        let payload = recvmsg.payload_data();
        let payload_offset = payload.as_ptr() as usize - base_ptr;
        let payload_len = payload.len();
        Ok((RecvMsgRingBuf::new(buf, payload_offset, payload_len), addr))
    }
}

// Safety: `NornFd` and `RecvBufRing` retain all referenced resources through the
// multishot terminal CQE. Each selected buffer is held by an owned completion
// until it is yielded or dropped.
unsafe impl Operation for RecvFromRingMulti {
    type Completion = io::Result<BufRingBuf>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        self.ring.ensure_accepting_receives()?;
        let this = self;
        let msghdr = this.msghdr.as_mut_ptr();
        unsafe {
            (*msghdr).msg_name = this.addr.as_ptr() as *mut libc::c_void;
            (*msghdr).msg_namelen = this.addr.len() as _;
            (*msghdr).msg_control = std::ptr::null_mut();
            (*msghdr).msg_controllen = 0;
            (*msghdr).msg_iov = std::ptr::null_mut();
            (*msghdr).msg_iovlen = 0;
        };

        Ok(opcode::RecvMsgMulti::new(this.fd.fd(), msghdr, this.ring.bgid()).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let (result, flags) = result.into_parts();
        result.and_then(|n| self.ring.get_buf(n, flags))
    }
}

impl Multishot for RecvFromRingMulti {
    type Item = io::Result<(RecvMsgRingBuf, SocketAddr)>;

    fn update(&mut self, completion: Self::Completion) -> Self::Item {
        self.recv_item(completion)
    }

    fn complete(mut self, completion: Self::Completion) -> Option<Self::Item> {
        Some(self.recv_item(completion))
    }
}

#[derive(Debug)]
pub(crate) struct RecvRingMulti {
    fd: NornFd,
    ring: RecvBufRing,
    flags: i32,
}

impl RecvRingMulti {
    pub(crate) fn new(fd: NornFd, ring: RecvBufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }
}

// Safety: `NornFd` and `RecvBufRing` retain all referenced resources through the
// multishot terminal CQE. Each selected buffer is held by an owned completion
// until it is yielded or dropped.
unsafe impl Operation for RecvRingMulti {
    type Completion = RingRecvCompletion<BufRingBuf>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        self.ring.ensure_accepting_receives()?;
        let this = self;
        Ok(opcode::RecvMulti::new(this.fd.fd(), this.ring.bgid())
            .flags(this.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let (result, flags) = result.into_parts();
        let result = result.and_then(|n| {
            if n == 0 && io_uring::cqueue::buffer_select(flags).is_none() {
                Ok(BufRingBuf::empty(self.ring.clone()))
            } else {
                self.ring.get_buf(n, flags)
            }
        });
        RingRecvCompletion {
            result,
            terminal: !io_uring::cqueue::more(flags),
        }
    }
}

impl Multishot for RecvRingMulti {
    type Item = RingRecvCompletion<BufRingBuf>;

    fn update(&mut self, completion: Self::Completion) -> Self::Item {
        completion
    }

    fn complete(self, completion: Self::Completion) -> Option<Self::Item> {
        Some(completion)
    }
}

#[derive(Debug)]
pub(crate) struct RecvRingBundle {
    fd: NornFd,
    ring: RecvBufRing,
    flags: i32,
}

impl RecvRingBundle {
    pub(crate) fn new(fd: NornFd, ring: RecvBufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }
}

// Safety: `NornFd` and `RecvBufRing` retain the descriptor and registered group.
// `reap` converts every selected bundle into an owned completion before waking
// application code.
unsafe impl Operation for RecvRingBundle {
    type Completion = io::Result<BufRingBufBundle>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        validate_recv_bundle_flags(self.flags)?;
        self.ring.ensure_accepting_receives()?;
        let this = self;
        Ok(opcode::RecvBundle::new(this.fd.fd(), this.ring.bgid())
            .flags(this.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let (result, flags) = result.into_parts();
        result.and_then(|n| self.ring.get_buf_bundle(n, flags))
    }
}

impl Singleshot for RecvRingBundle {
    type Output = io::Result<BufRingBufBundle>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

#[derive(Debug)]
pub(crate) struct RecvRingBundleMulti {
    fd: NornFd,
    ring: RecvBufRing,
    flags: i32,
}

impl RecvRingBundleMulti {
    pub(crate) fn new(fd: NornFd, ring: RecvBufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }
}

// Safety: `NornFd` and `RecvBufRing` retain resources through the multishot
// terminal CQE. Each selected bundle is held by an owned completion until it is
// yielded or dropped.
unsafe impl Operation for RecvRingBundleMulti {
    type Completion = RingRecvCompletion<BufRingBufBundle>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        validate_recv_multi_bundle_flags(self.flags)?;
        self.ring.ensure_accepting_receives()?;
        let this = self;
        Ok(opcode::RecvMultiBundle::new(this.fd.fd(), this.ring.bgid())
            .flags(this.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        let (result, flags) = result.into_parts();
        let result = result.and_then(|n| {
            if n == 0 && io_uring::cqueue::buffer_select(flags).is_none() {
                Ok(BufRingBufBundle::empty())
            } else {
                self.ring.get_buf_bundle(n, flags)
            }
        });
        RingRecvCompletion {
            result,
            terminal: !io_uring::cqueue::more(flags),
        }
    }
}

impl Multishot for RecvRingBundleMulti {
    type Item = RingRecvCompletion<BufRingBufBundle>;

    fn update(&mut self, completion: Self::Completion) -> Self::Item {
        completion
    }

    fn complete(self, completion: Self::Completion) -> Option<Self::Item> {
        Some(completion)
    }
}

fn socket_addr_from_name(name: &[u8]) -> io::Result<SocketAddr> {
    if name.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recvmsg completion did not include source address",
        ));
    }
    if name.len() > std::mem::size_of::<libc::sockaddr_storage>() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "recvmsg completion source address exceeded storage size",
        ));
    }
    let mut storage = std::mem::MaybeUninit::<libc::sockaddr_storage>::zeroed();
    unsafe {
        std::ptr::copy_nonoverlapping(name.as_ptr(), storage.as_mut_ptr() as *mut u8, name.len());
        let storage = storage.assume_init();
        let addr = SockAddr::new(storage, name.len() as libc::socklen_t);
        addr.as_socket().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "recvmsg completion had unsupported address family",
            )
        })
    }
}

pub(crate) struct Accept<const MULTI: bool> {
    fd: NornFd,
    addr: SockAddr,
    addr_len: libc::socklen_t,
}

impl<const MULTI: bool> Accept<MULTI> {
    pub(crate) fn new(fd: NornFd) -> Self {
        // Safety: We won't read from the socket addr until it's initialized.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        let addr_len = addr.len();
        Self { fd, addr, addr_len }
    }
}

// Safety: `NornFd` retains the listener and the pinned socket-address storage
// remains valid for every CQE. `reap` converts every accepted descriptor to
// `OwnedFd` before waking application code.
unsafe impl<const MULTI: bool> Operation for Accept<MULTI> {
    type Completion = io::Result<OwnedFd>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        let fd = this.fd.fd();
        if MULTI {
            Ok(opcode::AcceptMulti::new(fd)
                .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                .build())
        } else {
            Ok(opcode::Accept::new(
                fd,
                this.addr.as_ptr() as *mut _,
                &mut this.addr_len as *mut _,
            )
            .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
            .build())
        }
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        reap_owned_fd(result)
    }
}

impl Singleshot for Accept<false> {
    type Output = io::Result<(NornFd, SocketAddr)>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        let mut this = self;
        let fd = completion?;
        // Safety: the kernel wrote at most `addr_len` bytes into `addr`.
        unsafe { this.addr.set_length(this.addr_len) };
        let addr = as_socket_addr(&this.addr)?;
        Ok((bind_owned_fd(fd), addr))
    }
}

impl Multishot for Accept<true> {
    type Item = io::Result<NornFd>;

    fn update(&mut self, completion: Self::Completion) -> Self::Item {
        completion.map(bind_owned_fd)
    }

    fn complete(self, completion: Self::Completion) -> Option<Self::Item> {
        Some(completion.map(bind_owned_fd))
    }
}

struct BindSocket {
    fd: NornFd,
    addr: SockAddr,
}

impl BindSocket {
    fn new(fd: NornFd, addr: SocketAddr) -> Self {
        Self {
            fd,
            addr: SockAddr::from(addr),
        }
    }
}

// Safety: `NornFd` retains the socket and the owned address storage remains
// live and pinned through completion.
unsafe impl Operation for BindSocket {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(opcode::Bind::new(
            this.fd.fd(),
            this.addr.as_ptr() as *const _,
            this.addr.len() as _,
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for BindSocket {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct ListenSocket {
    fd: NornFd,
    backlog: i32,
}

impl ListenSocket {
    fn new(fd: NornFd, backlog: i32) -> Self {
        Self { fd, backlog }
    }
}

// Safety: `NornFd` retains the only resource referenced by this SQE.
unsafe impl Operation for ListenSocket {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(opcode::Listen::new(this.fd.fd(), this.backlog).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for ListenSocket {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct SetSockOpt<T> {
    fd: NornFd,
    level: u32,
    optname: u32,
    value: T,
}

impl<T> SetSockOpt<T>
where
    T: Copy,
{
    fn new(fd: NornFd, level: u32, optname: u32, value: T) -> Self {
        Self {
            fd,
            level,
            optname,
            value,
        }
    }
}

// Safety: `NornFd` retains the socket and the pinned inline option value keeps
// the SQE pointer valid through completion.
unsafe impl<T> Operation for SetSockOpt<T>
where
    T: Copy,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        let optlen = std::mem::size_of::<T>() as u32;
        let optval = &this.value as *const T as *const libc::c_void;
        Ok(opcode::SetSockOpt::new(this.fd.fd(), this.level, this.optname, optval, optlen).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<T> Singleshot for SetSockOpt<T>
where
    T: Copy,
{
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct Connect {
    fd: NornFd,
    addr: SockAddr,
}

impl Connect {
    pub(crate) fn new(fd: NornFd, addr: SocketAddr) -> Self {
        let addr = SockAddr::from(addr);
        Self { fd, addr }
    }
}

// Safety: `NornFd` retains the socket and the owned address storage remains
// live and pinned through completion.
unsafe impl Operation for Connect {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(opcode::Connect::new(
            this.fd.fd(),
            this.addr.as_ptr() as *mut _,
            this.addr.len() as _,
        )
        .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Connect {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}

struct Shutdown {
    fd: NornFd,
    how: libc::c_int,
}

impl Shutdown {
    pub(crate) fn new(fd: NornFd, how: libc::c_int) -> Self {
        Self { fd, how }
    }
}

// Safety: `NornFd` retains the only resource referenced by this SQE.
unsafe impl Operation for Shutdown {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(opcode::Shutdown::new(this.fd.fd(), this.how).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Shutdown {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}
#[derive(Debug)]
pub(crate) struct Recv<B> {
    fd: NornFd,
    buf: B,
    flags: i32,
    submitted_len: usize,
}

impl<B> Recv<B>
where
    B: StableBufMut,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: i32) -> Self {
        let submitted_len = buf.bytes_remaining();
        Self {
            fd,
            buf,
            flags,
            submitted_len,
        }
    }
}

// Safety: `NornFd` retains the socket and the owned `StableBufMut` keeps the
// writable region stable and exclusive through completion.
unsafe impl<B> Operation for Recv<B>
where
    B: StableBufMut,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let len = checked_scalar_len(self.submitted_len, "receive buffer length")?;
        let ptr = self.buf.stable_ptr_mut();
        Ok(opcode::Recv::new(self.fd.fd(), ptr, len)
            .flags(self.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for Recv<B>
where
    B: StableBufMut,
{
    type Output = (io::Result<usize>, B);

    fn complete(mut self, result: crate::operation::CQEResult) -> Self::Output {
        match result.result {
            Ok(bytes_read) => {
                let reported_len = complete_recv_buffer(
                    &mut self.buf,
                    self.submitted_len,
                    bytes_read as usize,
                    self.flags as u32,
                );
                (reported_len, self.buf)
            }
            Err(err) => (Err(err), self.buf),
        }
    }
}

#[derive(Debug)]
pub(crate) struct Send<B> {
    fd: NornFd,
    buf: B,
    flags: i32,
}

impl<B> Send<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: i32) -> Self {
        Self { fd, buf, flags }
    }
}

// Safety: `NornFd` retains the socket and the owned `StableBuf` keeps its
// initialized bytes stable through every kernel read.
unsafe impl<B> Operation for Send<B>
where
    B: StableBuf,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let len = checked_scalar_len(self.buf.bytes_init(), "send buffer length")?;
        let ptr = self.buf.stable_ptr();
        Ok(opcode::Send::new(self.fd.fd(), ptr, len)
            .flags(self.flags)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for Send<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, B);

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        (result.result.map(|v| v as usize), self.buf)
    }
}

fn checked_scalar_len(len: usize, what: &'static str) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} exceeds u32::MAX"),
        )
    })
}

fn update_send_zc_primary(
    primary_result: &mut Option<io::Result<usize>>,
    result: crate::operation::CQEResult,
) {
    if result.notif() {
        return;
    }
    *primary_result = Some(result.result.map(|v| v as usize));
}

const SEND_ZC_REPORT_USAGE: u16 = 1 << 3;
const NOTIF_USAGE_ZC_COPIED: u32 = 1 << 31;

/// How the kernel transferred a successful zero-copy send request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendZcUsage {
    /// The kernel transferred the complete payload without copying it.
    ZeroCopy,
    /// The kernel copied at least part of the payload.
    Copied,
    /// The kernel completed the request without a usage notification.
    Unknown,
}

/// The outcome of a successful zero-copy send request.
#[must_use = "the send length and zero-copy usage must be handled"]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SendZcResult {
    bytes_sent: usize,
    usage: SendZcUsage,
}

impl SendZcResult {
    /// Return the number of payload bytes accepted by the socket.
    pub fn bytes_sent(self) -> usize {
        self.bytes_sent
    }

    /// Return how the kernel transferred the payload.
    pub fn usage(self) -> SendZcUsage {
        self.usage
    }
}

fn complete_send_zc_result(
    primary_result: Option<io::Result<usize>>,
    result: crate::operation::CQEResult,
) -> io::Result<SendZcResult> {
    if result.notif() {
        let bytes_sent = primary_result.unwrap_or_else(|| Err(invalid_zc_notification_error()))?;
        let usage_flags = result.result?;
        let usage = if usage_flags & NOTIF_USAGE_ZC_COPIED == 0 {
            SendZcUsage::ZeroCopy
        } else {
            SendZcUsage::Copied
        };
        Ok(SendZcResult { bytes_sent, usage })
    } else {
        result.result.map(|bytes_sent| SendZcResult {
            bytes_sent: bytes_sent as usize,
            usage: SendZcUsage::Unknown,
        })
    }
}

#[derive(Debug)]
pub(crate) struct SendZc<B> {
    fd: NornFd,
    buf: B,
    flags: i32,
    primary_result: Option<io::Result<usize>>,
}

impl<B> SendZc<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: i32) -> Self {
        Self {
            fd,
            buf,
            flags,
            primary_result: None,
        }
    }
}

// Safety: the owned stable buffer remains live through both the primary and
// notification CQEs; the completion state treats only the notification as final.
unsafe impl<B> Operation for SendZc<B>
where
    B: StableBuf,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        let ptr = this.buf.stable_ptr();
        let len = checked_scalar_len(this.buf.bytes_init(), "zerocopy send buffer length")?;
        Ok(opcode::SendZc::new(this.fd.fd(), ptr, len)
            .flags(this.flags)
            .zc_flags(SEND_ZC_REPORT_USAGE)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for SendZc<B>
where
    B: StableBuf,
{
    type Output = (io::Result<SendZcResult>, B);

    fn update(&mut self, result: crate::operation::CQEResult) {
        update_send_zc_primary(&mut self.primary_result, result);
    }

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let this = self;
        (
            complete_send_zc_result(this.primary_result, result),
            this.buf,
        )
    }
}

pub(crate) struct SendMsgZc<B> {
    fd: NornFd,
    buf: B,
    flags: i32,
    msghdr: MaybeUninit<libc::msghdr>,
    slices: MaybeUninit<[io::IoSlice<'static>; 1]>,
    primary_result: Option<io::Result<usize>>,
}

impl<B> std::fmt::Debug for SendMsgZc<B> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendMsgZc").finish()
    }
}

impl<B> SendMsgZc<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: i32) -> Self {
        Self {
            fd,
            buf,
            flags,
            msghdr: MaybeUninit::zeroed(),
            slices: MaybeUninit::zeroed(),
            primary_result: None,
        }
    }
}

// Safety: the owned stable buffer and pinned msghdr/iovec storage remain live
// through both zerocopy CQEs; completion state accounts for the notification.
unsafe impl<B> Operation for SendMsgZc<B>
where
    B: StableBuf,
{
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;

        let slice = io::IoSlice::new(unsafe {
            std::slice::from_raw_parts(this.buf.stable_ptr(), this.buf.bytes_init())
        });
        this.slices.write([slice]);

        let msghdr = this.msghdr.as_mut_ptr();
        let slices = unsafe { this.slices.assume_init_mut() };
        unsafe {
            (*msghdr).msg_iov = slices.as_mut_ptr() as *mut _;
            (*msghdr).msg_iovlen = slices.len() as _;
            (*msghdr).msg_name = std::ptr::null_mut();
            (*msghdr).msg_namelen = 0;
            (*msghdr).msg_control = std::ptr::null_mut();
            (*msghdr).msg_controllen = 0;
        }

        let msghdr = this.msghdr.as_ptr();
        Ok(opcode::SendMsgZc::new(this.fd.fd(), msghdr)
            .ioprio(SEND_ZC_REPORT_USAGE)
            .flags(this.flags as u32)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl<B> Singleshot for SendMsgZc<B>
where
    B: StableBuf,
{
    type Output = (io::Result<SendZcResult>, B);

    fn update(&mut self, result: crate::operation::CQEResult) {
        update_send_zc_primary(&mut self.primary_result, result);
    }

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let this = self;
        (
            complete_send_zc_result(this.primary_result, result),
            this.buf,
        )
    }
}

#[derive(Debug)]
pub(crate) struct Poll<const MULTI: bool> {
    fd: NornFd,
    events: u32,
}

impl<const MULTI: bool> Poll<MULTI> {
    pub(crate) fn new(fd: NornFd, events: u32) -> Self {
        Self { fd, events }
    }
}

// Safety: `NornFd` retains the descriptor through the single or multishot
// terminal CQE; the SQE references no userspace memory.
unsafe impl<const MULTI: bool> Operation for Poll<MULTI> {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(opcode::PollAdd::new(this.fd.fd(), this.events)
            .multi(MULTI)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Multishot for Poll<true> {
    type Item = io::Result<Event>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        let res = result.result?;
        let event = Event::new(res as i16);
        Ok(event)
    }

    fn complete(self, result: crate::operation::CQEResult) -> Option<Self::Item> {
        let res = result.result.map(|res| Event::new(res as i16));
        Some(res)
    }
}

impl Singleshot for Poll<false> {
    type Output = io::Result<Event>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let res = result.result?;
        let event = Event::new(res as i16);
        Ok(event)
    }
}

/// [`Event`] captures the notification state of a polled
/// socket.
#[derive(Debug, Clone, Copy)]
#[must_use = "events must be handled"]
pub struct Event {
    events: i16,
}

impl Event {
    fn new(events: i16) -> Self {
        Self { events }
    }

    /// Returns true if the socket is readable.
    pub fn is_readable(&self) -> bool {
        (self.events & libc::POLLIN) != 0 || (self.events & libc::POLLPRI) != 0
    }

    /// Returns true if the socket is writeable.
    pub fn is_writeable(&self) -> bool {
        (self.events & libc::POLLOUT) != 0
    }

    /// Returns true if the socket has an error.
    pub fn is_error(&self) -> bool {
        (self.events & libc::POLLERR) != 0
    }

    /// Returns true if the socket is closed for reads.
    pub fn is_read_closed(&self) -> bool {
        (self.events & libc::POLLHUP) != 0 || (self.events & libc::POLLRDHUP) != 0
    }

    /// Returns true if the socket is closed for writes.
    pub fn is_write_closed(&self) -> bool {
        (self.events & libc::POLLHUP) != 0
            || ((self.events & libc::POLLOUT) != 0 && (self.events & libc::POLLERR) != 0)
            || (self.events == libc::POLLERR)
    }

    /// Returns true if there is a priority event.
    pub fn is_priority(&self) -> bool {
        (self.events & libc::POLLPRI) != 0
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;
    use std::panic::{self, AssertUnwindSafe};
    use std::pin::pin;
    use std::time::{Duration, Instant};

    use futures_util::StreamExt;
    use norn_executor::LocalExecutor;

    use super::*;

    #[test]
    fn unconsumed_reaped_descriptor_closes_directly() {
        let mut fds = [0; 2];
        assert_eq!(unsafe { libc::pipe(fds.as_mut_ptr()) }, 0);

        let completion = reap_owned_fd(CQEResult::new(Ok(fds[0] as u32), 0)).unwrap();
        drop(completion);

        let mut pollfd = libc::pollfd {
            fd: fds[1],
            events: libc::POLLOUT,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut pollfd, 1, 0) }, 1);
        assert_ne!(pollfd.revents & libc::POLLERR, 0);
        assert_eq!(unsafe { libc::close(fds[1]) }, 0);
    }

    fn assert_accept_flags(fd: io_uring::types::Fd) {
        let status = unsafe { libc::fcntl(fd.0, libc::F_GETFL) };
        assert_ne!(status, -1);
        assert_ne!(status & libc::O_NONBLOCK, 0);

        let descriptor = unsafe { libc::fcntl(fd.0, libc::F_GETFD) };
        assert_ne!(descriptor, -1);
        assert_ne!(descriptor & libc::FD_CLOEXEC, 0);
    }

    fn connect_from_thread(addr: SocketAddr) -> std::thread::JoinHandle<io::Result<()>> {
        std::thread::spawn(move || std::net::TcpStream::connect(addr).map(drop))
    }

    async fn connected_socket_with_writer() -> io::Result<(Socket, std::net::TcpStream)> {
        let listener =
            Socket::bind("127.0.0.1:0".parse().unwrap(), Domain::IPV4, Type::STREAM).await?;
        listener.listen(1).await?;
        let addr = listener.local_addr()?;
        let connector = std::thread::spawn(move || std::net::TcpStream::connect(addr));

        let (socket, _) = listener.accept().await?;
        let writer = connector.join().expect("connector thread panicked")?;
        socket.set_nodelay(true).await?;
        writer.set_nodelay(true)?;
        listener.close().await?;
        Ok((socket, writer))
    }

    fn socket_bytes_available(socket: &Socket) -> io::Result<i32> {
        let mut available = 0;
        let result =
            unsafe { libc::ioctl(socket.fd.fd().0, libc::FIONREAD, &mut available as *mut i32) };
        if result < 0 {
            Err(io::Error::last_os_error())
        } else {
            Ok(available)
        }
    }

    fn wait_for_socket_bytes(socket: &Socket, expected: i32) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let available = socket_bytes_available(socket)?;
            if available >= expected {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "socket did not reach {expected} readable bytes; last observed {available}"
                    ),
                ));
            }
            std::thread::yield_now();
        }
    }

    async fn wait_for_socket_consumption(socket: &Socket) -> io::Result<()> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            crate::noop().await;
            let available = socket_bytes_available(socket)?;
            if available == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!("socket retained {available} readable bytes"),
                ));
            }
        }
    }

    #[test]
    fn single_accept_sets_nonblocking_and_close_on_exec() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let mut executor = LocalExecutor::new(driver);

        executor.block_on(async {
            let listener =
                Socket::bind("127.0.0.1:0".parse().unwrap(), Domain::IPV4, Type::STREAM).await?;
            listener.listen(1).await?;
            let connector = connect_from_thread(listener.local_addr()?);

            let (socket, _) = listener.accept().await?;
            assert_accept_flags(socket.fd.fd());

            connector.join().expect("connector thread panicked")?;
            socket.close().await?;
            listener.close().await
        })
    }

    #[test]
    fn multishot_accept_sets_nonblocking_and_close_on_exec() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let mut executor = LocalExecutor::new(driver);

        executor.block_on(async {
            let listener =
                Socket::bind("127.0.0.1:0".parse().unwrap(), Domain::IPV4, Type::STREAM).await?;
            listener.listen(1).await?;
            let connector = connect_from_thread(listener.local_addr()?);

            let socket = {
                let mut incoming = pin!(listener.accept_multi());
                incoming
                    .next()
                    .await
                    .expect("multishot accept ended before yielding")?
            };
            assert_accept_flags(socket.fd());

            connector.join().expect("connector thread panicked")?;
            socket.close().await?;
            listener.close().await
        })
    }

    #[test]
    fn oversized_receive_completion_requires_msg_trunc() {
        let mut buf = Vec::with_capacity(1);
        let err = complete_recv_buffer(&mut buf, 1, 2, 0).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(buf.is_empty());

        assert_eq!(
            complete_recv_buffer(&mut buf, 1, 2, libc::MSG_TRUNC as u32).unwrap(),
            2
        );
        assert_eq!(buf.len(), 1);
    }

    #[test]
    fn recv_bundle_flag_validation_matches_single_and_multishot_contracts() {
        for flags in [
            0,
            libc::MSG_WAITALL,
            libc::MSG_PEEK,
            libc::MSG_WAITALL | libc::MSG_PEEK,
        ] {
            validate_recv_bundle_flags(flags).unwrap();

            let err = validate_recv_bundle_flags(flags | libc::MSG_TRUNC).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert!(err.to_string().contains("MSG_TRUNC"));
        }

        for flags in [0, libc::MSG_PEEK] {
            validate_recv_multi_bundle_flags(flags).unwrap();
        }
        for flags in [libc::MSG_WAITALL, libc::MSG_WAITALL | libc::MSG_PEEK] {
            let err = validate_recv_multi_bundle_flags(flags).unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
            assert!(err.to_string().contains("MSG_WAITALL"));
        }
        let err =
            validate_recv_multi_bundle_flags(libc::MSG_TRUNC | libc::MSG_WAITALL).unwrap_err();
        assert!(err.to_string().contains("MSG_TRUNC"));
    }

    #[test]
    fn waitall_reorder_preserves_shared_bundle_and_scalar_ownership() -> io::Result<()> {
        let feature_probe = io_uring::IoUring::new(2)?;
        if !feature_probe.params().is_feature_recvsend_bundle() {
            return Ok(());
        }
        drop(feature_probe);

        const BUFFER_LEN: usize = 256;
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 16)?;
        let mut executor = LocalExecutor::new(driver);

        executor.block_on(async {
            let ring = RecvBufRing::builder(31_804)
                .ring_entries(4)
                .buf_cnt(4)
                .buf_len(BUFFER_LEN)
                .build()?;
            let (waitall_socket, mut waitall_writer) = connected_socket_with_writer().await?;
            let (later_socket, mut later_writer) = connected_socket_with_writer().await?;
            let (third_socket, mut third_writer) = connected_socket_with_writer().await?;

            // Make the first receive consume the first published buffer without
            // completing. Completed NOPs drive the ring while FIONREAD confirms
            // consumption before the later receive starts.
            waitall_writer.write_all(&[0xaa])?;
            wait_for_socket_bytes(&waitall_socket, 1)?;
            let mut waitall_receive =
                pin!(waitall_socket.recv_ring_bundle_with_flags(&ring, libc::MSG_WAITALL));
            assert!(futures_util::poll!(&mut waitall_receive).is_pending());
            wait_for_socket_consumption(&waitall_socket).await?;
            assert!(futures_util::poll!(&mut waitall_receive).is_pending());

            // This later scalar receive consumes and completes the second
            // publication before the earlier bundle produces its CQE.
            later_writer.write_all(&vec![0xbb; BUFFER_LEN])?;
            wait_for_socket_bytes(&later_socket, BUFFER_LEN as i32)?;
            let (later, _) = later_socket.recv_from_ring(&ring).await?;
            assert_eq!(later.len(), BUFFER_LEN);
            assert!(later.iter().all(|byte| *byte == 0xbb));

            waitall_writer.write_all(&vec![0xaa; BUFFER_LEN - 1])?;
            waitall_writer.shutdown(std::net::Shutdown::Write)?;
            let waitall = waitall_receive.await?;
            assert_eq!(waitall.len(), BUFFER_LEN);
            assert!(waitall.iter().flatten().all(|byte| *byte == 0xaa));

            // Retain both earlier owners. A third bundle must skip both of
            // their BIDs and claim the next live publication.
            third_writer.write_all(b"third")?;
            let third = third_socket.recv_ring_bundle(&ring).await?;
            assert_eq!(
                third.iter().flatten().copied().collect::<Vec<_>>(),
                b"third"
            );

            drop((third, waitall, later));
            waitall_socket.close().await?;
            later_socket.close().await?;
            third_socket.close().await
        })
    }

    fn build_test_ring(driver: &crate::Driver, bgid: u16) -> io::Result<RecvBufRing> {
        let _guard = norn_executor::park::Park::enter(driver);
        RecvBufRing::builder(bgid).buf_cnt(8).buf_len(1024).build()
    }

    fn test_socket(handle: crate::Handle) -> io::Result<Socket> {
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Socket {
            fd: UringFd::from_fd_on(fd, handle),
        })
    }

    fn assert_driver_mismatch(f: impl FnOnce()) {
        let panic = panic::catch_unwind(AssertUnwindSafe(f)).expect_err("operation must panic");
        let message = panic
            .downcast_ref::<&str>()
            .copied()
            .or_else(|| panic.downcast_ref::<String>().map(String::as_str));
        assert_eq!(
            message,
            Some("buffer ring and socket must target the same driver")
        );
    }

    fn prepare_all_ring_receives(socket: &Socket, ring: &RecvBufRing) {
        drop(socket.recv_from_ring(ring));
        drop(socket.recv_from_ring_multi(ring));
        drop(socket.recv_ring_multi(ring));
        drop(socket.recv_ring_bundle(ring));
        drop(socket.recv_ring_bundle_with_flags(ring, libc::MSG_PEEK));
        drop(socket.recv_ring_bundle_multi_with_flags(ring, 0));
        drop(socket.recv_ring_bundle_multi_with_flags(ring, libc::MSG_PEEK));
    }

    #[test]
    fn ring_receive_entrypoints_accept_same_driver_ring() -> io::Result<()> {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let ring = build_test_ring(&driver, 31)?;
        let socket = test_socket(driver.handle())?;

        prepare_all_ring_receives(&socket, &ring);
        Ok(())
    }

    #[test]
    fn ring_receive_entrypoints_reject_same_bgid_from_another_driver() -> io::Result<()> {
        let first_driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let first_ring = build_test_ring(&first_driver, 31)?;
        let second_driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let second_ring = build_test_ring(&second_driver, 31)?;
        let socket = test_socket(second_driver.handle())?;

        prepare_all_ring_receives(&socket, &second_ring);
        assert_driver_mismatch(|| drop(socket.recv_from_ring(&first_ring)));
        assert_driver_mismatch(|| drop(socket.recv_from_ring_multi(&first_ring)));
        assert_driver_mismatch(|| drop(socket.recv_ring_multi(&first_ring)));
        assert_driver_mismatch(|| drop(socket.recv_ring_bundle(&first_ring)));
        assert_driver_mismatch(|| {
            drop(socket.recv_ring_bundle_with_flags(&first_ring, libc::MSG_PEEK));
        });
        assert_driver_mismatch(|| {
            drop(socket.recv_ring_bundle_multi_with_flags(&first_ring, 0));
        });
        assert_driver_mismatch(|| {
            drop(socket.recv_ring_bundle_multi_with_flags(&first_ring, libc::MSG_PEEK));
        });
        Ok(())
    }

    fn more_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::more(*flags))
            .expect("missing CQE more flag")
    }

    fn notif_flag() -> u32 {
        (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::notif(*flags))
            .expect("missing CQE notif flag")
    }

    #[test]
    fn zc_completion_single_cqe_uses_final_result() {
        let final_cqe = crate::operation::CQEResult::new(Ok(64), 0);
        let result = complete_send_zc_result(None, final_cqe).unwrap();
        assert_eq!(result.bytes_sent(), 64);
        assert_eq!(result.usage(), SendZcUsage::Unknown);
    }

    #[test]
    fn zc_completion_final_notification_reports_zero_copy() {
        let mut primary = None;
        let update = crate::operation::CQEResult::new(Ok(32), more_flag());
        update_send_zc_primary(&mut primary, update);
        let result = complete_send_zc_result(
            primary,
            crate::operation::CQEResult::new(Ok(0), notif_flag()),
        )
        .unwrap();
        assert_eq!(result.bytes_sent(), 32);
        assert_eq!(result.usage(), SendZcUsage::ZeroCopy);
    }

    #[test]
    fn zc_completion_final_notification_reports_copy_fallback() {
        let mut primary = None;
        let update = crate::operation::CQEResult::new(Ok(32), more_flag());
        update_send_zc_primary(&mut primary, update);
        let result = complete_send_zc_result(
            primary,
            crate::operation::CQEResult::new(Ok(NOTIF_USAGE_ZC_COPIED), notif_flag()),
        )
        .unwrap();
        assert_eq!(result.bytes_sent(), 32);
        assert_eq!(result.usage(), SendZcUsage::Copied);
    }

    #[test]
    fn zc_completion_notification_without_primary_is_invalid() {
        let err =
            complete_send_zc_result(None, crate::operation::CQEResult::new(Ok(0), notif_flag()))
                .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn connected_scalar_io_rejects_lengths_above_u32_max() {
        assert_eq!(
            checked_scalar_len(u32::MAX as usize + 1, "test buffer")
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn connected_scalar_io_preserves_u32_max_length() {
        assert_eq!(
            checked_scalar_len(u32::MAX as usize, "test buffer").unwrap(),
            u32::MAX
        );
    }
}
