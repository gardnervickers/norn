//! Socket operations.
//!
//! [Socket] is the core socket type
//! used by both TCP and UDP sockets
use io_uring::squeue::Flags;
use io_uring::{opcode, types};
use libc::{SOCK_CLOEXEC, SOCK_NONBLOCK};
use socket2::{Domain, Protocol, SockAddr, Type};
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::os::fd::FromRawFd;

use crate::buf::{set_init_checked, StableBuf, StableBufMut};
use crate::bufring::{BufRing, BufRingBuf, BufRingBufBundle, SendBundleBatch};
use crate::fd::NornFd;
use crate::operation::{Multishot, Op, Operation, Singleshot};

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

fn fixed_fd_unsupported_error(context: &'static str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("fixed descriptors are not supported for {context}"),
    )
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
        let sock = match fd.kind() {
            crate::fd::FdKind::Fd(fd) => unsafe { socket2::Socket::from_raw_fd(fd.0) },
            crate::fd::FdKind::Fixed(_) => {
                return Err(fixed_fd_unsupported_error("peer address lookup"))
            }
        };
        let sock = ManuallyDrop::new(sock);
        return as_socket_addr(&sock.peer_addr()?);
    }
    as_socket_addr(addr)
}

#[derive(Clone)]
pub(crate) struct Socket {
    fd: NornFd,
    handle: crate::Handle,
}

impl Socket {
    pub(crate) fn from_fd(fd: NornFd) -> Self {
        Self {
            fd,
            handle: crate::Handle::current(),
        }
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
        let socket = Self::open(domain, socket_type, None).await?;
        let op = BindSocket::new(socket.fd.clone(), addr);
        socket.handle.submit(op).await?;
        Ok(socket)
    }

    pub(crate) async fn listen(&self, backlog: u32) -> io::Result<()> {
        let backlog = i32::try_from(backlog).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "listen backlog exceeds i32")
        })?;
        let op = ListenSocket::new(self.fd.clone(), backlog);
        self.handle.submit(op).await
    }

    pub(crate) async fn accept(&self) -> io::Result<(Self, SocketAddr)> {
        let op = Accept::<false>::new(self.fd.clone());
        let (fd, addr) = self.handle.submit(op).await?;
        let socket = Self::from_fd(fd);
        Ok((socket, addr))
    }

    pub(crate) fn accept_multi(&self) -> Op<Accept<true>> {
        let op = Accept::<true>::new(self.fd.clone());
        self.handle.submit(op)
    }

    pub(crate) async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        let op = Connect::new(self.fd.clone(), addr);
        self.handle.submit(op).await?;
        Ok(())
    }

    #[track_caller]
    fn assert_bufring_driver(&self, ring: &BufRing) {
        assert!(
            ring.same_driver(&self.handle),
            "buffer ring and socket must target the same driver"
        );
    }

    #[track_caller]
    fn assert_sendbufring_driver(&self, batch: &SendBundleBatch) {
        assert!(
            batch.same_driver(&self.handle),
            "buffer ring and socket must target the same driver"
        );
    }

    pub(crate) fn recv_from_ring(&self, ring: &BufRing) -> Op<RecvFromRing> {
        self.assert_bufring_driver(ring);
        let op = RecvFromRing::new(self.fd.clone(), ring.clone());
        self.handle.submit(op)
    }

    pub(crate) fn recv_from_ring_multi(&self, ring: &BufRing) -> Op<RecvFromRingMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvFromRingMulti::new(self.fd.clone(), ring.clone());
        self.handle.submit(op)
    }

    pub(crate) fn recv_ring_multi(&self, ring: &BufRing) -> Op<RecvRingMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvRingMulti::new(self.fd.clone(), ring.clone(), 0);
        self.handle.submit(op)
    }

    pub(crate) fn recv_ring_bundle(&self, ring: &BufRing) -> Op<RecvRingBundle> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundle::new(self.fd.clone(), ring.clone(), 0);
        self.handle.submit(op)
    }

    pub(crate) fn recv_ring_bundle_with_flags(
        &self,
        ring: &BufRing,
        flags: i32,
    ) -> Op<RecvRingBundle> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundle::new(self.fd.clone(), ring.clone(), flags);
        self.handle.submit(op)
    }

    pub(crate) fn recv_ring_bundle_multi(&self, ring: &BufRing) -> Op<RecvRingBundleMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundleMulti::new(self.fd.clone(), ring.clone(), 0);
        self.handle.submit(op)
    }

    pub(crate) fn recv_ring_bundle_multi_with_flags(
        &self,
        ring: &BufRing,
        flags: i32,
    ) -> Op<RecvRingBundleMulti> {
        self.assert_bufring_driver(ring);
        let op = RecvRingBundleMulti::new(self.fd.clone(), ring.clone(), flags);
        self.handle.submit(op)
    }

    pub(crate) async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        let mut buf = buf;
        if let Some(result) = self.try_recv_from(&mut buf, 0) {
            return (result, buf);
        }
        let op = RecvFrom::new(self.fd.clone(), buf, 0);
        self.handle.submit(op).await
    }

    pub(crate) async fn send_to<B>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        if let Some(result) = self.try_send_to(&buf, Some(addr), 0) {
            return (result, buf);
        }
        let op = SendTo::new(self.fd.clone(), buf, Some(addr), 0);
        self.handle.submit(op).await
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
        let op = RecvFrom::new(self.fd.clone(), buf, flags as u32);
        self.handle.submit(op).await
    }

    pub(crate) async fn send_to_with_flags<B>(
        &self,
        buf: B,
        addr: SocketAddr,
        flags: i32,
    ) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        if let Some(result) = self.try_send_to(&buf, Some(addr), flags) {
            return (result, buf);
        }
        let op = SendTo::new(self.fd.clone(), buf, Some(addr), flags as u32);
        self.handle.submit(op).await
    }

    pub(crate) async fn send_bundle_udp(&self, batch: SendBundleBatch) -> io::Result<usize> {
        self.send_bundle_udp_with_flags(batch, 0).await
    }

    pub(crate) async fn send_bundle_udp_with_flags(
        &self,
        batch: SendBundleBatch,
        flags: i32,
    ) -> io::Result<usize> {
        self.assert_sendbufring_driver(&batch);
        batch.validate_send()?;
        let op = SendBundleUdp::new(self.fd.clone(), batch, flags);
        self.handle.submit(op).await
    }

    pub(crate) fn recv<B>(&self, buf: B) -> Op<Recv<B>>
    where
        B: StableBufMut + 'static,
    {
        let op = Recv::new(self.fd.clone(), buf, 0);
        self.handle.submit(op)
    }

    pub(crate) fn send<B>(&self, buf: B) -> Op<Send<B>>
    where
        B: StableBuf + 'static,
    {
        let op = Send::new(self.fd.clone(), buf, 0);
        self.handle.submit(op)
    }

    pub(crate) fn recv_with_flags<B>(&self, buf: B, flags: i32) -> Op<Recv<B>>
    where
        B: StableBufMut + 'static,
    {
        let op = Recv::new(self.fd.clone(), buf, flags);
        self.handle.submit(op)
    }

    pub(crate) fn send_with_flags<B>(&self, buf: B, flags: i32) -> Op<Send<B>>
    where
        B: StableBuf + 'static,
    {
        let op = Send::new(self.fd.clone(), buf, flags);
        self.handle.submit(op)
    }

    pub(crate) fn send_zc<B>(&self, buf: B) -> Op<SendZc<B>>
    where
        B: StableBuf + 'static,
    {
        let op = SendZc::new(self.fd.clone(), buf, 0);
        self.handle.submit(op)
    }

    pub(crate) fn send_zc_with_flags<B>(&self, buf: B, flags: i32) -> Op<SendZc<B>>
    where
        B: StableBuf + 'static,
    {
        let op = SendZc::new(self.fd.clone(), buf, flags);
        self.handle.submit(op)
    }

    pub(crate) fn send_msg_zc<B>(&self, buf: B, flags: i32) -> Op<SendMsgZc<B>>
    where
        B: StableBuf + 'static,
    {
        let op = SendMsgZc::new(self.fd.clone(), buf, flags);
        self.handle.submit(op)
    }

    pub(crate) async fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        let how = match how {
            std::net::Shutdown::Read => libc::SHUT_RD,
            std::net::Shutdown::Write => libc::SHUT_WR,
            std::net::Shutdown::Both => libc::SHUT_RDWR,
        };
        let op = Shutdown::new(self.fd.clone(), how);
        self.handle.submit(op).await
    }

    pub(crate) fn poll_readiness<const MULTI: bool>(&self, events: u32) -> Op<Poll<MULTI>> {
        let op = Poll::<MULTI>::new(self.fd.clone(), events);
        self.handle.submit(op)
    }

    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        as_socket_addr(&self.as_socket()?.local_addr()?)
    }

    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        as_socket_addr(&self.as_socket()?.peer_addr()?)
    }

    pub(crate) fn as_socket(&self) -> io::Result<ManuallyDrop<socket2::Socket>> {
        match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                let sock = unsafe { socket2::Socket::from_raw_fd(fd.0) };
                Ok(ManuallyDrop::new(sock))
            }
            crate::fd::FdKind::Fixed(_) => Err(fixed_fd_unsupported_error("socket2 operations")),
        }
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
        let op = SetSockOpt::new(self.fd.clone(), level as u32, optname as u32, value);
        self.handle.submit(op).await
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
        let fd = match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => fd.0,
            crate::fd::FdKind::Fixed(_) => return None,
        };
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
        let fd = match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => fd.0,
            crate::fd::FdKind::Fixed(_) => return None,
        };
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

// Safety: the socket SQE contains only copied scalar arguments; cleanup closes
// a descriptor returned by an unconsumed successful CQE.
unsafe impl Operation for OpenSocket {
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

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(res) = result.result {
            NornFd::from_fd(res as i32);
        }
    }
}

impl Singleshot for OpenSocket {
    type Output = io::Result<NornFd>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let fd = result.result?;
        Ok(NornFd::from_fd(fd as i32))
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
        Ok(
            // Finally we create the operation.
            match this.fd.kind() {
                crate::fd::FdKind::Fd(fd) => opcode::SendMsg::new(types::Fd(fd.0), msghdr),
                crate::fd::FdKind::Fixed(fd) => opcode::SendMsg::new(types::Fixed(fd.0), msghdr),
            }
            .flags(this.flags)
            .build(),
        )
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvMsg::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::RecvMsg::new(types::Fixed(fd.0), msghdr),
        }
        .flags(this.flags)
        .build())
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
pub struct RecvFromRing {
    fd: NornFd,
    ring: BufRing,
    addr: SockAddr,
    msghdr: MaybeUninit<libc::msghdr>,
}

impl RecvFromRing {
    pub(crate) fn new(fd: NornFd, ring: BufRing) -> Self {
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

// Safety: `NornFd` and `BufRing` retain the socket and registered buffer group;
// inline recvmsg metadata remains pinned, and cleanup returns selected buffers.
unsafe impl Operation for RecvFromRing {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
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
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvMsg::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::RecvMsg::new(types::Fixed(fd.0), msghdr),
        }
        .buf_group(this.ring.bgid())
        .build()
        .flags(Flags::BUFFER_SELECT))
    }

    fn cleanup(&mut self, res: crate::operation::CQEResult) {
        if let Ok(n) = res.result {
            drop(self.ring.get_buf(n, res.flags));
        }
    }
}

impl Singleshot for RecvFromRing {
    type Output = io::Result<(BufRingBuf, SocketAddr)>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let mut this = self;
        let n = result.result?;
        let buf = this.ring.get_buf(n, result.flags)?;
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
pub struct RecvFromRingMulti {
    fd: NornFd,
    ring: BufRing,
    addr: SockAddr,
    msghdr: MaybeUninit<libc::msghdr>,
}

impl RecvFromRingMulti {
    pub(crate) fn new(fd: NornFd, ring: BufRing) -> Self {
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
        result: crate::operation::CQEResult,
    ) -> io::Result<(RecvMsgRingBuf, SocketAddr)> {
        let n = result.result?;
        let buf = self.ring.get_buf(n, result.flags)?;
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

// Safety: `NornFd` and `BufRing` retain all referenced resources through the
// multishot terminal CQE; each selected buffer is either yielded or cleaned up.
unsafe impl Operation for RecvFromRingMulti {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
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

        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::RecvMsgMulti::new(types::Fd(fd.0), msghdr, this.ring.bgid()).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::RecvMsgMulti::new(types::Fixed(fd.0), msghdr, this.ring.bgid()).build()
            }
        })
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(n) = result.result {
            drop(self.ring.get_buf(n, result.flags));
        }
    }
}

impl Multishot for RecvFromRingMulti {
    type Item = io::Result<(RecvMsgRingBuf, SocketAddr)>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        self.recv_item(result)
    }

    fn complete(mut self, result: crate::operation::CQEResult) -> Option<Self::Item> {
        Some(self.recv_item(result))
    }
}

#[derive(Debug)]
pub struct RecvRingMulti {
    fd: NornFd,
    ring: BufRing,
    flags: i32,
}

impl RecvRingMulti {
    pub(crate) fn new(fd: NornFd, ring: BufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }

    fn to_item(&self, result: crate::operation::CQEResult) -> io::Result<BufRingBuf> {
        let n = result.result?;
        self.ring.get_buf(n, result.flags)
    }
}

// Safety: `NornFd` and `BufRing` retain all referenced resources through the
// multishot terminal CQE; each selected buffer is either yielded or cleaned up.
unsafe impl Operation for RecvRingMulti {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvMulti::new(types::Fd(fd.0), this.ring.bgid())
                .flags(this.flags)
                .build(),
            crate::fd::FdKind::Fixed(fd) => {
                opcode::RecvMulti::new(types::Fixed(fd.0), this.ring.bgid())
                    .flags(this.flags)
                    .build()
            }
        })
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(n) = result.result {
            drop(self.ring.get_buf(n, result.flags));
        }
    }
}

impl Multishot for RecvRingMulti {
    type Item = io::Result<BufRingBuf>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        self.to_item(result)
    }

    fn complete(self, result: crate::operation::CQEResult) -> Option<Self::Item> {
        Some(self.to_item(result))
    }
}

#[derive(Debug)]
pub struct RecvRingBundle {
    fd: NornFd,
    ring: BufRing,
    flags: i32,
}

impl RecvRingBundle {
    pub(crate) fn new(fd: NornFd, ring: BufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }
}

// Safety: `NornFd` and `BufRing` retain the descriptor and registered group;
// completion ownership accounts for every selected buffer in the bundle.
unsafe impl Operation for RecvRingBundle {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvBundle::new(types::Fd(fd.0), this.ring.bgid())
                .flags(this.flags)
                .build(),
            crate::fd::FdKind::Fixed(fd) => {
                opcode::RecvBundle::new(types::Fixed(fd.0), this.ring.bgid())
                    .flags(this.flags)
                    .build()
            }
        })
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(n) = result.result {
            drop(self.ring.get_buf_bundle(n, result.flags));
        }
    }
}

impl Singleshot for RecvRingBundle {
    type Output = io::Result<BufRingBufBundle>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let n = result.result?;
        self.ring.get_buf_bundle(n, result.flags)
    }
}

#[derive(Debug)]
pub struct RecvRingBundleMulti {
    fd: NornFd,
    ring: BufRing,
    flags: i32,
}

impl RecvRingBundleMulti {
    pub(crate) fn new(fd: NornFd, ring: BufRing, flags: i32) -> Self {
        Self { fd, ring, flags }
    }

    fn to_item(&self, result: crate::operation::CQEResult) -> io::Result<BufRingBufBundle> {
        let n = result.result?;
        self.ring.get_buf_bundle(n, result.flags)
    }
}

// Safety: `NornFd` and `BufRing` retain resources through the multishot
// terminal CQE; yielded and unconsumed bundles are returned by completion logic.
unsafe impl Operation for RecvRingBundleMulti {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::RecvMultiBundle::new(types::Fd(fd.0), this.ring.bgid())
                    .flags(this.flags)
                    .build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::RecvMultiBundle::new(types::Fixed(fd.0), this.ring.bgid())
                    .flags(this.flags)
                    .build()
            }
        })
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(n) = result.result {
            drop(self.ring.get_buf_bundle(n, result.flags));
        }
    }
}

impl Multishot for RecvRingBundleMulti {
    type Item = io::Result<BufRingBufBundle>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        self.to_item(result)
    }

    fn complete(self, result: crate::operation::CQEResult) -> Option<Self::Item> {
        Some(self.to_item(result))
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
// remains valid for every CQE; cleanup closes unconsumed accepted descriptors.
unsafe impl<const MULTI: bool> Operation for Accept<MULTI> {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(
            // Finally we create the operation.
            match this.fd.kind() {
                crate::fd::FdKind::Fd(fd) => {
                    if MULTI {
                        opcode::AcceptMulti::new(*fd)
                            .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                            .build()
                    } else {
                        opcode::Accept::new(
                            *fd,
                            this.addr.as_ptr() as *mut _,
                            &mut this.addr_len as *mut _,
                        )
                        .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                        .build()
                    }
                }
                crate::fd::FdKind::Fixed(fd) => {
                    if MULTI {
                        opcode::AcceptMulti::new(*fd)
                            .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                            .build()
                    } else {
                        opcode::Accept::new(
                            *fd,
                            this.addr.as_ptr() as *mut _,
                            &mut this.addr_len as *mut _,
                        )
                        .flags(SOCK_NONBLOCK | SOCK_CLOEXEC)
                        .build()
                    }
                }
            },
        )
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        if let Ok(fd) = result.result {
            NornFd::from_fd(fd as i32);
        }
    }
}

impl Singleshot for Accept<false> {
    type Output = io::Result<(NornFd, SocketAddr)>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let mut this = self;
        let fd = result.result?;
        // Safety: the kernel wrote at most `addr_len` bytes into `addr`.
        unsafe { this.addr.set_length(this.addr_len) };
        let addr = as_socket_addr(&this.addr)?;
        Ok((NornFd::from_fd(fd as i32), addr))
    }
}

impl Multishot for Accept<true> {
    type Item = io::Result<NornFd>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        let fd = result.result?;
        Ok(NornFd::from_fd(fd as i32))
    }

    fn complete(self, result: crate::operation::CQEResult) -> Option<Self::Item> {
        Some(result.result.map(|fd| NornFd::from_fd(fd as i32)))
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::Bind::new(*fd, this.addr.as_ptr() as *const _, this.addr.len() as _).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::Bind::new(*fd, this.addr.as_ptr() as *const _, this.addr.len() as _).build()
            }
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Listen::new(*fd, this.backlog).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Listen::new(*fd, this.backlog).build(),
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        let optlen = std::mem::size_of::<T>() as u32;
        let optval = &this.value as *const T as *const libc::c_void;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::SetSockOpt::new(types::Fd(fd.0), this.level, this.optname, optval, optlen)
                    .build()
            }
            crate::fd::FdKind::Fixed(fd) => opcode::SetSockOpt::new(
                types::Fixed(fd.0),
                this.level,
                this.optname,
                optval,
                optlen,
            )
            .build(),
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::Connect::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
                    .build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::Connect::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
                    .build()
            }
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Shutdown::new(*fd, this.how).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Shutdown::new(*fd, this.how).build(),
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for Shutdown {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(|_| ())
    }
}
#[derive(Debug)]
pub struct Recv<B> {
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let len = checked_scalar_len(self.submitted_len, "receive buffer length")?;
        let ptr = self.buf.stable_ptr_mut();
        Ok(
            // Finally we create the operation.
            match self.fd.kind() {
                crate::fd::FdKind::Fd(fd) => opcode::Recv::new(*fd, ptr, len),
                crate::fd::FdKind::Fixed(fd) => opcode::Recv::new(*fd, ptr, len),
            }
            .flags(self.flags)
            .build(),
        )
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
pub struct Send<B> {
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let len = checked_scalar_len(self.buf.bytes_init(), "send buffer length")?;
        let ptr = self.buf.stable_ptr();
        Ok(
            // Finally we create the operation.
            match self.fd.kind() {
                crate::fd::FdKind::Fd(fd) => opcode::Send::new(*fd, ptr, len),
                crate::fd::FdKind::Fixed(fd) => opcode::Send::new(*fd, ptr, len),
            }
            .flags(self.flags)
            .build(),
        )
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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

#[derive(Debug)]
struct SendBundleUdp {
    fd: NornFd,
    batch: SendBundleBatch,
    flags: i32,
}

impl SendBundleUdp {
    fn new(fd: NornFd, batch: SendBundleBatch, flags: i32) -> Self {
        Self { fd, batch, flags }
    }
}

// Safety: `NornFd` retains the connected socket and `SendBundleBatch` retains all
// provided buffers through the terminal completion.
unsafe impl Operation for SendBundleUdp {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::SendBundle::new(types::Fd(fd.0), self.batch.bgid())
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::SendBundle::new(types::Fixed(fd.0), self.batch.bgid())
            }
        }
        .flags(self.flags)
        .len(0)
        .build())
    }

    fn on_submit(&mut self) {
        self.batch.on_submit();
    }

    fn cleanup(&mut self, result: crate::operation::CQEResult) {
        let _ = self.batch.finish_send(result);
    }
}

impl Singleshot for SendBundleUdp {
    type Output = io::Result<usize>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        self.batch.finish_send(result)
    }
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

fn complete_send_zc_result(
    primary_result: Option<io::Result<usize>>,
    result: crate::operation::CQEResult,
) -> io::Result<usize> {
    if result.notif() {
        primary_result.unwrap_or_else(|| Err(invalid_zc_notification_error()))
    } else {
        result.result.map(|v| v as usize)
    }
}

#[derive(Debug)]
pub struct SendZc<B> {
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        let ptr = this.buf.stable_ptr();
        let len = this.buf.bytes_init();
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::SendZc::new(*fd, ptr, len as _),
            crate::fd::FdKind::Fixed(fd) => opcode::SendZc::new(*fd, ptr, len as _),
        }
        .flags(this.flags)
        .build())
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl<B> Singleshot for SendZc<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, B);

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

pub struct SendMsgZc<B> {
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
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::SendMsgZc::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::SendMsgZc::new(types::Fixed(fd.0), msghdr),
        }
        .flags(this.flags as u32)
        .build())
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl<B> Singleshot for SendMsgZc<B>
where
    B: StableBuf,
{
    type Output = (io::Result<usize>, B);

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
pub struct Poll<const MULTI: bool> {
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
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let this = self;
        Ok(match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::PollAdd::new(*fd, this.events).multi(MULTI).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::PollAdd::new(*fd, this.events).multi(MULTI).build()
            }
        })
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
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
    use std::panic::{self, AssertUnwindSafe};
    use std::pin::pin;

    use futures_util::StreamExt;
    use norn_executor::LocalExecutor;

    use super::*;
    use crate::bufring::SendBufRing;

    fn assert_accept_flags(fd: &NornFd) {
        let crate::fd::FdKind::Fd(fd) = fd.kind() else {
            panic!("accepted socket used a fixed descriptor");
        };

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
            assert_accept_flags(&socket.fd);

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
            assert_accept_flags(&socket);

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

    fn build_test_ring(driver: &crate::Driver, bgid: u16) -> io::Result<BufRing> {
        let _guard = norn_executor::park::Park::enter(driver);
        BufRing::builder(bgid).buf_cnt(8).buf_len(1024).build()
    }

    fn build_test_send_ring(driver: &crate::Driver, bgid: u16) -> io::Result<SendBufRing> {
        let _guard = norn_executor::park::Park::enter(driver);
        BufRing::builder(bgid).buf_cnt(8).buf_len(1024).build_send()
    }

    fn test_socket(handle: crate::Handle) -> io::Result<Socket> {
        let fd = unsafe { libc::socket(libc::AF_INET, libc::SOCK_DGRAM | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(Socket {
            fd: NornFd::from_fd(fd),
            handle,
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

    fn prepare_all_ring_receives(socket: &Socket, ring: &BufRing) {
        drop(socket.recv_from_ring(ring));
        drop(socket.recv_from_ring_multi(ring));
        drop(socket.recv_ring_multi(ring));
        drop(socket.recv_ring_bundle(ring));
        drop(socket.recv_ring_bundle_with_flags(ring, libc::MSG_PEEK));
        drop(socket.recv_ring_bundle_multi(ring));
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
        assert_driver_mismatch(|| drop(socket.recv_ring_bundle_multi(&first_ring)));
        assert_driver_mismatch(|| {
            drop(socket.recv_ring_bundle_multi_with_flags(&first_ring, libc::MSG_PEEK));
        });
        Ok(())
    }

    #[test]
    fn send_bundle_rejects_same_bgid_from_another_driver() -> io::Result<()> {
        let first_driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let first_ring = build_test_send_ring(&first_driver, 32)?;
        let batch = first_ring.batch()?;
        let mut buf = batch.checkout()?;
        buf.as_mut_slice()[0] = 1;
        buf.set_len(1)?;
        buf.commit()?;

        let second_driver = crate::Driver::new(io_uring::IoUring::builder(), 8)?;
        let socket = test_socket(second_driver.handle())?;

        assert_driver_mismatch(|| socket.assert_sendbufring_driver(&batch));
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
        assert_eq!(result, 64);
    }

    #[test]
    fn zc_completion_final_notification_uses_primary_result() {
        let mut primary = None;
        let update = crate::operation::CQEResult::new(Ok(32), more_flag());
        update_send_zc_primary(&mut primary, update);
        let result = complete_send_zc_result(
            primary,
            crate::operation::CQEResult::new(Ok(0), notif_flag()),
        )
        .unwrap();
        assert_eq!(result, 32);
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
