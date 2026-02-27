//! Socket operations.
//!
//! [Socket] is the core socket type
//! used by both TCP and UDP sockets
use std::io;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::net::SocketAddr;
use std::os::fd::FromRawFd;
use std::pin::Pin;

use io_uring::squeue::Flags;
use io_uring::{opcode, types};
use libc::O_NONBLOCK;
use socket2::{Domain, Protocol, SockAddr, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRing, BufRingBuf};
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

    pub(crate) fn recv_from_ring(&self, ring: &BufRing) -> Op<RecvFromRing> {
        let op = RecvFromRing::new(self.fd.clone(), ring.clone());
        self.handle.submit(op)
    }

    pub(crate) async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        let op = RecvFrom::new(self.fd.clone(), buf, 0);
        self.handle.submit(op).await
    }

    pub(crate) async fn send_to<B>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
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
        let op = SendTo::new(self.fd.clone(), buf, Some(addr), flags as u32);
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

impl Operation for OpenSocket {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let ty: i32 = self.socket_type.into();
        let ty = ty | libc::SOCK_NONBLOCK | libc::SOCK_CLOEXEC;
        io_uring::opcode::Socket::new(
            self.domain.into(),
            ty,
            self.protocol.map(Into::into).unwrap_or(0),
        )
        .build()
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

impl<B> Operation for SendTo<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };

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

        // Finally we create the operation.
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::SendMsg::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::SendMsg::new(types::Fixed(fd.0), msghdr),
        }
        .flags(this.flags)
        .build()
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

impl<B> Operation for RecvFrom<B>
where
    B: StableBufMut,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };

        let ptr = this.buf.stable_ptr_mut();
        let len = this.buf.bytes_remaining();
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
        }

        // Finally we create the operation.
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvMsg::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::RecvMsg::new(types::Fixed(fd.0), msghdr),
        }
        .flags(this.flags)
        .build()
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
                unsafe { buf.set_init(bytes_read as usize) };
                (Ok((bytes_read as usize, addr)), buf)
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

impl Operation for RecvFromRing {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };

        // Next we initialize the msghdr.
        let msghdr = this.msghdr.as_mut_ptr();
        unsafe {
            (*msghdr).msg_iov = std::ptr::null_mut();
            (*msghdr).msg_iovlen = 0;
            (*msghdr).msg_name = this.addr.as_ptr() as *mut libc::c_void;
            (*msghdr).msg_namelen = this.addr.len() as _;
        }

        // Finally we create the operation.
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::RecvMsg::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::RecvMsg::new(types::Fixed(fd.0), msghdr),
        }
        .buf_group(this.ring.bgid())
        .build()
        .flags(Flags::BUFFER_SELECT)
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

impl<const MULTI: bool> Operation for Accept<MULTI> {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };

        // Finally we create the operation.
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                if MULTI {
                    opcode::AcceptMulti::new(*fd).flags(O_NONBLOCK).build()
                } else {
                    opcode::Accept::new(
                        *fd,
                        this.addr.as_ptr() as *mut _,
                        &mut this.addr_len as *mut _,
                    )
                    .flags(O_NONBLOCK)
                    .build()
                }
            }
            crate::fd::FdKind::Fixed(fd) => {
                if MULTI {
                    opcode::AcceptMulti::new(*fd).flags(O_NONBLOCK).build()
                } else {
                    opcode::Accept::new(
                        *fd,
                        this.addr.as_ptr() as *mut _,
                        &mut this.addr_len as *mut _,
                    )
                    .flags(O_NONBLOCK)
                    .build()
                }
            }
        }
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

impl Operation for BindSocket {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::Bind::new(*fd, this.addr.as_ptr() as *const _, this.addr.len() as _).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::Bind::new(*fd, this.addr.as_ptr() as *const _, this.addr.len() as _).build()
            }
        }
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

impl Operation for ListenSocket {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Listen::new(*fd, this.backlog).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Listen::new(*fd, this.backlog).build(),
        }
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

impl<T> Operation for SetSockOpt<T>
where
    T: Copy,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        let optlen = std::mem::size_of::<T>() as u32;
        let optval = &this.value as *const T as *const libc::c_void;
        match this.fd.kind() {
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
        }
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

impl Operation for Connect {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::Connect::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
                    .build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::Connect::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
                    .build()
            }
        }
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

impl Operation for Shutdown {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Shutdown::new(*fd, this.how).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Shutdown::new(*fd, this.how).build(),
        }
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
}

impl<B> Recv<B>
where
    B: StableBufMut,
{
    pub(crate) fn new(fd: NornFd, buf: B, flags: i32) -> Self {
        Self { fd, buf, flags }
    }
}

impl<B> Operation for Recv<B>
where
    B: StableBufMut,
{
    fn configure(mut self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let ptr = self.buf.stable_ptr_mut();
        let len = self.buf.bytes_remaining();

        // Finally we create the operation.
        match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Recv::new(*fd, ptr, len as _),
            crate::fd::FdKind::Fixed(fd) => opcode::Recv::new(*fd, ptr, len as _),
        }
        .flags(self.flags)
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl<B> Singleshot for Recv<B>
where
    B: StableBufMut,
{
    type Output = (io::Result<usize>, B);

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        match result.result {
            Ok(bytes_read) => {
                let mut buf = self.buf;
                unsafe { buf.set_init(bytes_read as usize) };
                (Ok(bytes_read as usize), buf)
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

impl<B> Operation for Send<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let ptr = self.buf.stable_ptr();
        let len = self.buf.bytes_init();

        // Finally we create the operation.
        match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::Send::new(*fd, ptr, len as _),
            crate::fd::FdKind::Fixed(fd) => opcode::Send::new(*fd, ptr, len as _),
        }
        .flags(self.flags)
        .build()
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

impl<B> Operation for SendZc<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        let ptr = this.buf.stable_ptr();
        let len = this.buf.bytes_init();

        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::SendZc::new(*fd, ptr, len as _),
            crate::fd::FdKind::Fixed(fd) => opcode::SendZc::new(*fd, ptr, len as _),
        }
        .flags(this.flags)
        .build()
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

impl<B> Operation for SendMsgZc<B>
where
    B: StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };

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
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => opcode::SendMsgZc::new(types::Fd(fd.0), msghdr),
            crate::fd::FdKind::Fixed(fd) => opcode::SendMsgZc::new(types::Fixed(fd.0), msghdr),
        }
        .flags(this.flags as u32)
        .build()
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

impl<const MULTI: bool> Operation for Poll<MULTI> {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = unsafe { self.get_unchecked_mut() };
        match this.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                opcode::PollAdd::new(*fd, this.events).multi(MULTI).build()
            }
            crate::fd::FdKind::Fixed(fd) => {
                opcode::PollAdd::new(*fd, this.events).multi(MULTI).build()
            }
        }
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
    use super::*;

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
}
