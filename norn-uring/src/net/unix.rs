//! Unix domain sockets.
//!
//! These reuse the TCP and UDP machinery: [`UnixListener`] and [`UnixSocket`]
//! mirror [`TcpListener`](super::TcpListener) and
//! [`TcpSocket`](super::TcpSocket), [`UnixStream`] mirrors
//! [`TcpStream`](super::TcpStream), and [`UnixDatagram`] mirrors
//! [`UdpSocket`](super::UdpSocket).
//!
//! Addresses are [`std::os::unix::net::SocketAddr`]s. A socket that was never
//! bound, such as the client end of a connection or a socket from
//! [`UnixSocket::pair`], has an unnamed address. Names in the Linux abstract
//! namespace are supported through
//! [`SocketAddrExt`](std::os::linux::net::SocketAddrExt). A peer bound to a
//! path that fills all of `sun_path`, which `std` cannot represent, is
//! reported as unnamed.
use crate::BufResult;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::os::unix::net::SocketAddr;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use socket2::{Domain, SockAddr, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::fd::UringFd;
use crate::net::incoming::{Incoming, INCOMING_LIMIT};
use crate::net::ready::ReadyStream;
use crate::net::socket::{self, unix_sockaddr, Event, Unix, UnixDest};
use crate::net::{RecvOptions, SendOptions};
use crate::WithOptions;

/// A Unix domain stream listener.
///
/// Dropping or closing the listener does not remove the socket file a
/// pathname listener created; remove it before binding the path again.
pub struct UnixListener {
    socket: socket::Socket,
}

/// A Unix domain stream socket.
///
/// [`UnixSocket`] sends and receives owned buffers, like
/// [`TcpSocket`](super::TcpSocket). [`UnixSocket::into_stream`] converts it
/// into a [`UnixStream`], which implements
/// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite).
pub struct UnixSocket {
    socket: socket::Socket,
}

/// A Unix domain datagram socket.
///
/// Datagrams keep their boundaries and are never reordered or lost while
/// the receiver keeps up; a sender to a full receive queue waits.
pub struct UnixDatagram {
    inner: socket::Socket,
}

pin_project_lite::pin_project! {
    /// [`UnixStream`] represents a connected Unix domain stream socket.
    ///
    /// Bytes can be read from and written to the socket using the
    /// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite) traits.
    pub struct UnixStream {
        #[pin]
        reader: UnixStreamReader,
        #[pin]
        writer: UnixStreamWriter,
    }
}

pin_project_lite::pin_project! {
    /// [`UnixStreamReader`] is the read half of a [`UnixStream`].
    pub struct UnixStreamReader {
        #[pin]
        inner: ReadyStream,
    }
}

pin_project_lite::pin_project! {
    /// [`UnixStreamWriter`] is the write half of a [`UnixStream`].
    pub struct UnixStreamWriter {
        #[pin]
        inner: ReadyStream,
    }
}

/// Convert a path argument. Like `std`, reject NUL bytes: the kernel would
/// cut the path at the first one, and a leading one names the abstract
/// namespace instead of a file.
fn unix_path(path: &Path) -> io::Result<SockAddr> {
    use std::os::unix::ffi::OsStrExt;
    if path.as_os_str().as_bytes().contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "paths must not contain null bytes",
        ));
    }
    SockAddr::unix(path)
}

impl std::fmt::Debug for UnixListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixListener").finish()
    }
}

impl std::fmt::Debug for UnixSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixSocket").finish()
    }
}

impl std::fmt::Debug for UnixStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixStream").finish()
    }
}

impl std::fmt::Debug for UnixDatagram {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UnixDatagram").finish()
    }
}

impl UnixListener {
    /// Wrap a driver-bound descriptor as a Unix listener.
    ///
    /// This does not inspect the descriptor. The kernel validates its socket
    /// type and listening state when operations are submitted.
    pub fn from_uring_fd(fd: UringFd) -> Self {
        Self {
            socket: socket::Socket::from_uring_fd(fd),
        }
    }

    /// Return the underlying driver-bound descriptor.
    pub fn as_uring_fd(&self) -> &UringFd {
        self.socket.as_uring_fd()
    }

    /// Consume this listener and return its driver-bound descriptor.
    pub fn into_uring_fd(self) -> UringFd {
        self.socket.into_uring_fd()
    }

    /// Creates a listener bound to the socket file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is too long, if the socket cannot be
    /// created or bound (for example because `path` already exists), or if
    /// it cannot be placed in listening mode.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind(path: impl AsRef<Path>, backlog: u32) -> io::Result<UnixListener> {
        Self::bind_sockaddr(unix_path(path.as_ref())?, backlog).await
    }

    /// Creates a listener bound to `addr`, which may name a path or an
    /// abstract name.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an unnamed address, or an
    /// error if the socket cannot be created, bound, or placed in listening
    /// mode.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind_addr(addr: &SocketAddr, backlog: u32) -> io::Result<UnixListener> {
        Self::bind_sockaddr(unix_sockaddr(addr)?, backlog).await
    }

    async fn bind_sockaddr(addr: SockAddr, backlog: u32) -> io::Result<UnixListener> {
        let socket = socket::Socket::bind_sockaddr(addr, Domain::UNIX, Type::STREAM, false).await?;
        socket.listen(backlog).await?;
        Ok(UnixListener { socket })
    }

    /// Returns the local address that this listener is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.unix_local_addr()
    }

    /// Accepts a new incoming connection to this listener.
    ///
    /// The returned address is the peer's, which is unnamed unless the peer
    /// bound its socket. The returned socket is bound to the
    /// [`Driver`](crate::Driver) that owns the listener, whichever context
    /// finishes this future.
    ///
    /// # Errors
    ///
    /// Returns an error if the accept operation fails.
    pub async fn accept(&self) -> io::Result<(UnixSocket, SocketAddr)> {
        let (socket, addr) = self.socket.accept::<Unix>().await?;
        Ok((UnixSocket { socket }, addr))
    }

    /// Returns a stream of incoming connections.
    ///
    /// This behaves like [`TcpListener::incoming`](super::TcpListener::incoming):
    /// a multishot accept that pauses once 64 accepted connections wait to be
    /// yielded.
    pub fn incoming(&self) -> impl Stream<Item = io::Result<UnixSocket>> + '_ {
        self.incoming_with_limit(Some(INCOMING_LIMIT))
    }

    /// Returns a stream of incoming connections that pauses its accept once
    /// `limit` accepted connections wait to be yielded, like
    /// [`TcpListener::incoming_with_limit`](super::TcpListener::incoming_with_limit).
    pub fn incoming_with_limit(
        &self,
        limit: Option<NonZeroUsize>,
    ) -> impl Stream<Item = io::Result<UnixSocket>> + '_ {
        Incoming::new(&self.socket, limit, |socket| UnixSocket { socket })
    }

    /// Closes the listener.
    ///
    /// This behaves like [`TcpListener::close`](super::TcpListener::close),
    /// and does not remove the socket file.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while another owner or operation
    /// retains the descriptor, or another I/O error if close fails.
    pub async fn close(self) -> io::Result<()> {
        self.socket.close().await
    }
}

impl UnixSocket {
    /// Wrap a driver-bound descriptor as a Unix stream socket.
    ///
    /// This does not inspect the descriptor. The kernel validates whether it
    /// supports each requested operation.
    pub fn from_uring_fd(fd: UringFd) -> Self {
        Self {
            socket: socket::Socket::from_uring_fd(fd),
        }
    }

    /// Return the underlying driver-bound descriptor.
    pub fn as_uring_fd(&self) -> &UringFd {
        self.socket.as_uring_fd()
    }

    /// Consume this socket and return its driver-bound descriptor.
    pub fn into_uring_fd(self) -> UringFd {
        self.socket.into_uring_fd()
    }

    /// Connects to the listener bound to the socket file at `path`.
    ///
    /// While the listener's backlog is full, the connect waits for space,
    /// like a blocking `connect(2)`, in an `io_uring` worker thread.
    /// Dropping the future cancels it.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is too long, or if the socket cannot be
    /// created or connected.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn connect(path: impl AsRef<Path>) -> io::Result<UnixSocket> {
        Self::connect_sockaddr(unix_path(path.as_ref())?).await
    }

    /// Connects to the listener bound to `addr`, which may name a path or an
    /// abstract name. It waits for backlog space like
    /// [`UnixSocket::connect`].
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an unnamed address, or an
    /// error if the socket cannot be created or connected.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn connect_addr(addr: &SocketAddr) -> io::Result<UnixSocket> {
        Self::connect_sockaddr(unix_sockaddr(addr)?).await
    }

    async fn connect_sockaddr(addr: SockAddr) -> io::Result<UnixSocket> {
        // A nonblocking Unix stream connect to a listener with a full backlog
        // fails with `EAGAIN` and queues nothing, unlike TCP's
        // `EINPROGRESS`. Connect on a blocking socket so the kernel waits for
        // backlog space, as a blocking `connect(2)` does, then switch it to
        // nonblocking for the readiness-driven stream.
        let socket = socket::Socket::open_blocking(Domain::UNIX, Type::STREAM).await?;
        socket.connect(addr).await?;
        socket.set_nonblocking()?;
        Ok(UnixSocket { socket })
    }

    /// Creates a pair of connected, unnamed sockets with `socketpair(2)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the sockets cannot be created.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub fn pair() -> io::Result<(UnixSocket, UnixSocket)> {
        let (a, b) = socket::Socket::pair(Type::STREAM)?;
        Ok((UnixSocket { socket: a }, UnixSocket { socket: b }))
    }

    /// Returns the local address that this socket is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.unix_local_addr()
    }

    /// Returns the address of the connected peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer address cannot be read from the socket.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.unix_peer_addr()
    }

    /// Shuts down the read, write, or both halves of this connection.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel cannot shut down the requested half.
    pub async fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.socket.shutdown(how).await
    }

    /// Recv data into the given buffer.
    ///
    /// This takes ownership of the buffer, and returns a future which resolves
    /// to a [`BufResult`](crate::BufResult) holding the number of bytes read and the buffer. Zero
    /// bytes read means the peer shut down its write half.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`]. As on
    /// TCP, a receive with [`RecvOptions::trunc`], or with
    /// [`RecvOptions::peek`] together with [`RecvOptions::wait_all`], fails
    /// with [`io::ErrorKind::InvalidInput`] and returns the buffer unchanged.
    pub fn recv<B: StableBufMut>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + WithOptions<RecvOptions> {
        self.socket.recv_stream(buf)
    }

    /// Send data from the given buffer.
    ///
    /// This takes ownership of the buffer, and returns a future which resolves
    /// to a [`BufResult`](crate::BufResult) holding the number of bytes sent and the buffer.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`]. Unix
    /// stream sockets ignore [`SendOptions::more`].
    pub fn send<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + WithOptions<SendOptions> {
        self.socket.send(buf)
    }

    /// Convert this socket into a stream.
    ///
    /// [`UnixStream`] is a stateful wrapper around [`UnixSocket`] that implements
    /// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite).
    pub fn into_stream(self) -> UnixStream {
        let reader = UnixStreamReader {
            inner: ReadyStream::new(self.socket.clone()),
        };
        let writer = UnixStreamWriter {
            inner: ReadyStream::new(self.socket),
        };
        UnixStream { reader, writer }
    }

    /// Close the socket.
    ///
    /// This behaves like [`TcpSocket::close`](super::TcpSocket::close).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while another owner or operation
    /// retains the descriptor, or another I/O error if close fails.
    pub async fn close(self) -> io::Result<()> {
        self.socket.close().await
    }

    /// Wait for one readiness event on this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    pub fn poll_readiness(&self, events: u32) -> impl crate::Request<Output = io::Result<Event>> {
        self.socket.poll_readiness::<false>(events)
    }

    /// Return a stream of readiness events for this socket, like
    /// [`TcpSocket::poll_readiness_multi`](super::TcpSocket::poll_readiness_multi).
    pub fn poll_readiness_multi(&self, events: u32) -> impl Stream<Item = io::Result<Event>> {
        socket::ReadinessStream::new(&self.socket, events)
    }

    /// Set value for the `SO_RCVBUF` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.socket.set_recv_buffer_size(size).await
    }

    /// Set value for the `SO_SNDBUF` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.socket.set_send_buffer_size(size).await
    }
}

impl UnixStream {
    /// Split the stream into a reader and writer.
    pub fn owned_split(self) -> (UnixStreamReader, UnixStreamWriter) {
        (self.reader, self.writer)
    }
}

impl tokio::io::AsyncRead for UnixStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().reader.poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for UnixStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().writer.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().writer.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().writer.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.writer.is_write_vectored()
    }
}

impl tokio::io::AsyncRead for UnixStreamReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for UnixStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // Writes go straight to the kernel; there is nothing to flush.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        self.project().inner.poll_shutdown_write()
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        self.project().inner.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

impl UnixDatagram {
    /// Wrap a driver-bound descriptor as a Unix datagram socket.
    ///
    /// This does not inspect the descriptor. The kernel validates whether it
    /// supports each requested operation.
    pub fn from_uring_fd(fd: UringFd) -> Self {
        Self {
            inner: socket::Socket::from_uring_fd(fd),
        }
    }

    /// Return the underlying driver-bound descriptor.
    pub fn as_uring_fd(&self) -> &UringFd {
        self.inner.as_uring_fd()
    }

    /// Consume this socket and return its driver-bound descriptor.
    pub fn into_uring_fd(self) -> UringFd {
        self.inner.into_uring_fd()
    }

    /// Creates a datagram socket bound to the socket file at `path`.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is too long, or if the socket cannot be
    /// created or bound (for example because `path` already exists).
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind(path: impl AsRef<Path>) -> io::Result<UnixDatagram> {
        Self::bind_sockaddr(unix_path(path.as_ref())?).await
    }

    /// Creates a datagram socket bound to `addr`, which may name a path or an
    /// abstract name.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an unnamed address, or an
    /// error if the socket cannot be created or bound.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind_addr(addr: &SocketAddr) -> io::Result<UnixDatagram> {
        Self::bind_sockaddr(unix_sockaddr(addr)?).await
    }

    async fn bind_sockaddr(addr: SockAddr) -> io::Result<UnixDatagram> {
        let inner = socket::Socket::bind_sockaddr(addr, Domain::UNIX, Type::DGRAM, false).await?;
        Ok(UnixDatagram { inner })
    }

    /// Creates a datagram socket that is not bound to an address.
    ///
    /// It can send, and receive from a connected peer, but a receiver sees
    /// its datagrams as coming from an unnamed address it cannot reply to.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be created.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn unbound() -> io::Result<UnixDatagram> {
        let inner = socket::Socket::open(Domain::UNIX, Type::DGRAM, None).await?;
        Ok(UnixDatagram { inner })
    }

    /// Creates a pair of connected, unnamed datagram sockets with
    /// `socketpair(2)`.
    ///
    /// # Errors
    ///
    /// Returns an error if the sockets cannot be created.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub fn pair() -> io::Result<(UnixDatagram, UnixDatagram)> {
        let (a, b) = socket::Socket::pair(Type::DGRAM)?;
        Ok((UnixDatagram { inner: a }, UnixDatagram { inner: b }))
    }

    /// Connect this socket to the socket bound at `path`.
    ///
    /// A connected socket can use [`UnixDatagram::send`] and
    /// [`UnixDatagram::recv`], and receives only from its peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the path is too long or the socket cannot be
    /// connected.
    pub async fn connect(&self, path: impl AsRef<Path>) -> io::Result<()> {
        self.inner.connect(unix_path(path.as_ref())?).await
    }

    /// Connect this socket to `addr`, which may name a path or an abstract
    /// name.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] for an unnamed address, or an
    /// error if the socket cannot be connected.
    pub async fn connect_addr(&self, addr: &SocketAddr) -> io::Result<()> {
        self.inner.connect(unix_sockaddr(addr)?).await
    }

    /// Returns the local address that this socket is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.unix_local_addr()
    }

    /// Returns the address of the connected peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket is not connected or the peer address
    /// cannot be read.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.unix_peer_addr()
    }

    /// Sends a single datagram to the socket bound at `path`.
    ///
    /// On success, returns the number of bytes written. Like
    /// [`UdpSocket::send_to`](super::UdpSocket::send_to), it first tries a
    /// nonblocking `sendto` and submits to `io_uring` only if that would
    /// block. A path that is too long fails the send.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    /// Unix datagram sockets ignore [`SendOptions::more`].
    pub fn send_to<B>(
        &self,
        buf: B,
        path: impl AsRef<Path>,
    ) -> impl Future<Output = BufResult<usize, B>> + WithOptions<SendOptions> + '_
    where
        B: StableBuf + 'static,
    {
        self.inner
            .send_to(buf, UnixDest::new(unix_path(path.as_ref())))
    }

    /// Sends a single datagram to `addr`, which may name a path or an
    /// abstract name. An unnamed address fails the send with
    /// [`io::ErrorKind::InvalidInput`].
    ///
    /// Otherwise this behaves like [`UnixDatagram::send_to`].
    pub fn send_to_addr<B>(
        &self,
        buf: B,
        addr: &SocketAddr,
    ) -> impl Future<Output = BufResult<usize, B>> + WithOptions<SendOptions> + '_
    where
        B: StableBuf + 'static,
    {
        self.inner.send_to(buf, UnixDest::new(unix_sockaddr(addr)))
    }

    /// Sends a single datagram on a connected socket.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    /// Unix datagram sockets ignore [`SendOptions::more`].
    pub fn send<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + WithOptions<SendOptions>
    where
        B: StableBuf + 'static,
    {
        self.inner.send(buf)
    }

    /// Receives a single datagram. On success, returns the number of bytes
    /// read and the sender's address, which is unnamed if the sender is not
    /// bound.
    ///
    /// A datagram longer than the buffer is truncated, and the excess is
    /// discarded. Like [`UdpSocket::recv_from`](super::UdpSocket::recv_from),
    /// it first tries a nonblocking `recvfrom` and submits to `io_uring` only
    /// if that would block.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`].
    pub fn recv_from<B>(
        &self,
        buf: B,
    ) -> impl Future<Output = BufResult<(usize, SocketAddr), B>> + WithOptions<RecvOptions> + '_
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv_from::<Unix, B>(buf)
    }

    /// Receives a single datagram on a connected socket.
    ///
    /// A datagram longer than the buffer is truncated, and the excess is
    /// discarded.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`].
    pub fn recv<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + WithOptions<RecvOptions>
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv(buf)
    }

    /// Wait for one readiness event on this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    pub fn poll_readiness(&self, events: u32) -> impl crate::Request<Output = io::Result<Event>> {
        self.inner.poll_readiness::<false>(events)
    }

    /// Return a stream of readiness events for this socket, like
    /// [`UdpSocket::poll_readiness_multi`](super::UdpSocket::poll_readiness_multi).
    pub fn poll_readiness_multi(&self, events: u32) -> impl Stream<Item = io::Result<Event>> {
        socket::ReadinessStream::new(&self.inner, events)
    }

    /// Shuts down the read, write, or both halves of this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the kernel cannot shut down the requested half.
    pub async fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.inner.shutdown(how).await
    }

    /// Close the socket.
    ///
    /// This behaves like [`UdpSocket::close`](super::UdpSocket::close).
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while another owner or operation
    /// retains the descriptor, or another I/O error if close fails.
    pub async fn close(self) -> io::Result<()> {
        self.inner.close().await
    }
}
