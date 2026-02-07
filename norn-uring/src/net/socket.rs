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
        let addr = SockAddr::from(addr);
        let socket = Self::open(domain, socket_type, None).await?;
        let s = socket.as_socket();
        s.bind(&addr)?;
        Ok(socket)
    }

    pub(crate) fn listen(&self, backlog: u32) -> io::Result<()> {
        let s = self.as_socket();
        s.listen(backlog as _)?;
        Ok(())
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
        let op = RecvFrom::new(self.fd.clone(), buf);
        self.handle.submit(op).await
    }

    pub(crate) async fn send_to<B>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        let op = SendTo::new(self.fd.clone(), buf, Some(addr));
        self.handle.submit(op).await
    }

    pub(crate) fn recv<B>(&self, buf: B) -> Op<Recv<B>>
    where
        B: StableBufMut + 'static,
    {
        let op = Recv::new(self.fd.clone(), buf);
        self.handle.submit(op)
    }

    pub(crate) fn send<B>(&self, buf: B) -> Op<Send<B>>
    where
        B: StableBuf + 'static,
    {
        let op = Send::new(self.fd.clone(), buf);
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
        Ok(self.as_socket().local_addr()?.as_socket().unwrap())
    }

    pub(crate) fn peer_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.as_socket().peer_addr()?.as_socket().unwrap())
    }

    pub(crate) fn as_socket(&self) -> ManuallyDrop<socket2::Socket> {
        match self.fd.kind() {
            crate::fd::FdKind::Fd(fd) => {
                let sock = unsafe { socket2::Socket::from_raw_fd(fd.0) };
                ManuallyDrop::new(sock)
            }
            crate::fd::FdKind::Fixed(_) => unimplemented!(),
        }
    }

    pub(crate) async fn close(self) -> io::Result<()> {
        self.fd.close().await
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
    msghdr: MaybeUninit<libc::msghdr>,
    slices: MaybeUninit<[io::IoSlice<'static>; 1]>,
}

impl<B> SendTo<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B, addr: Option<SocketAddr>) -> Self {
        let addr = addr.map(SockAddr::from);
        Self {
            fd,
            buf,
            addr,
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
    msghdr: MaybeUninit<libc::msghdr>,
    slices: MaybeUninit<[io::IoSliceMut<'static>; 1]>,
}

impl<B> RecvFrom<B>
where
    B: StableBufMut,
{
    pub(crate) fn new(fd: NornFd, buf: B) -> Self {
        // Safety: We won't read from the socket addr until it's initialized.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        Self {
            fd,
            buf,
            addr,
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
        match result.result {
            Ok(bytes_read) => {
                let addr = self.addr.as_socket().unwrap();
                let mut buf = self.buf;
                unsafe { buf.set_init(bytes_read as usize) };
                (Ok((bytes_read as usize, addr)), buf)
            }
            Err(err) => (Err(err), self.buf),
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
        let n = result.result?;
        let buf = self.ring.get_buf(n, result.flags)?;
        let addr = self.addr.as_socket().unwrap();
        Ok((buf, addr))
    }
}

pub(crate) struct Accept<const MULTI: bool> {
    fd: NornFd,
    addr: SockAddr,
}

impl<const MULTI: bool> Accept<MULTI> {
    pub(crate) fn new(fd: NornFd) -> Self {
        // Safety: We won't read from the socket addr until it's initialized.
        let addr = unsafe { SockAddr::try_init(|_, _| Ok(())) }.unwrap().1;
        Self { fd, addr }
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
                    opcode::Accept::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
                        .flags(O_NONBLOCK)
                        .build()
                }
            }
            crate::fd::FdKind::Fixed(fd) => {
                if MULTI {
                    opcode::AcceptMulti::new(*fd).flags(O_NONBLOCK).build()
                } else {
                    opcode::Accept::new(*fd, this.addr.as_ptr() as *mut _, this.addr.len() as _)
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
        let fd = result.result?;
        let addr = self.addr.as_socket().unwrap();
        Ok((NornFd::from_fd(fd as i32), addr))
    }
}

impl Multishot for Accept<true> {
    type Item = io::Result<NornFd>;

    fn update(&mut self, result: crate::operation::CQEResult) -> Self::Item {
        let fd = result.result?;
        Ok(NornFd::from_fd(fd as i32))
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
}

impl<B> Recv<B>
where
    B: StableBufMut,
{
    pub(crate) fn new(fd: NornFd, buf: B) -> Self {
        Self { fd, buf }
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
            crate::fd::FdKind::Fd(fd) => opcode::Recv::new(*fd, ptr, len as _).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Recv::new(*fd, ptr, len as _).build(),
        }
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
}

impl<B> Send<B>
where
    B: StableBuf,
{
    pub(crate) fn new(fd: NornFd, buf: B) -> Self {
        Self { fd, buf }
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
            crate::fd::FdKind::Fd(fd) => opcode::Send::new(*fd, ptr, len as _).build(),
            crate::fd::FdKind::Fixed(fd) => opcode::Send::new(*fd, ptr, len as _).build(),
        }
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
