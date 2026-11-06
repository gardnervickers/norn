use crate::BufResult;
use std::io;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_core::Stream;
use socket2::{Domain, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRingBuf, BufRingBufBundle, RecvBufRing};
use crate::fd::UringFd;
use crate::net::socket::{self, Event, SendZcResult};
use crate::net::{RecvOptions, SendOptions};

use super::incoming::{Incoming, INCOMING_LIMIT};
use super::ready::ReadyStream;

/// A TCP listener.
///
/// A `TcpListener` can be used to accept incoming TCP connections.
pub struct TcpListener {
    socket: socket::Socket,
}

/// Options applied while binding a [`TcpListener`].
///
/// The default options preserve the behavior of [`TcpListener::bind`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TcpListenerOptions {
    reuse_port: bool,
}

/// A TCP socket.
///
/// [`TcpSocket`] provides a low-level interface for configuring a socket
/// and sending or receiving owned buffers.
///
/// A [`TcpSocket`] can be converted into a [`TcpStream`] using
/// [`TcpSocket::into_stream`].
/// [`TcpStream`] is a stateful wrapper around [`TcpSocket`] that implements
/// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite).
pub struct TcpSocket {
    socket: socket::Socket,
}

pin_project_lite::pin_project! {
    /// A stream of buffers received by a multishot TCP receive operation.
    ///
    /// The stream ends when the peer performs an orderly shutdown.
    ///
    /// Buffer exhaustion is absorbed as backpressure: receiving waits for a
    /// buffer return and re-arms without yielding ENOBUFS or discarding queued data.
    /// Drop held buffers to allow progress. Ring quarantine yields a terminal
    /// error; driver shutdown ends a stream waiting for buffers. Other socket
    /// errors remain terminal.
    ///
    /// The kernel keeps the receive armed while the stream is not polled.
    /// Each receive completion takes at least one whole ring buffer, however
    /// few bytes arrived, and the stream holds it until a later `poll_next`
    /// yields it. A stream whose task stops polling, for example while one
    /// of its sends is blocked, can therefore take every buffer of a ring it
    /// shares with other streams, which then wait for a buffer return.
    /// Dropping the stream returns those buffers but discards the data they
    /// hold.
    /// [`Builder::max_unconsumed_per_receive`](crate::bufring::Builder::max_unconsumed_per_receive)
    /// bounds the buffers a stream holds: at the bound it pauses receiving,
    /// without losing data, until it is polled again.
    pub struct TcpRecvRingMulti {
        #[pin]
        inner: Option<socket::RecvRingStream>,
    }
}

impl TcpRecvRingMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing) -> Self {
        Self {
            inner: Some(socket.recv_ring_stream(ring)),
        }
    }
}

impl Stream for TcpRecvRingMulti {
    type Item = io::Result<BufRingBuf>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let Some(inner) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        match ready!(inner.poll_next(cx)) {
            Some(Ok(buffer)) if buffer.capacity() == 0 => {
                this.inner.set(None);
                Poll::Ready(None)
            }
            item @ Some(_) => Poll::Ready(item),
            None => {
                this.inner.set(None);
                Poll::Ready(None)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// A stream of buffer bundles received by a multishot TCP receive operation.
    ///
    /// The stream ends when the peer performs an orderly shutdown.
    ///
    /// Buffer exhaustion is absorbed as backpressure: receiving waits for a
    /// buffer return and re-arms without yielding ENOBUFS or discarding queued data.
    /// Drop held buffers to allow progress. Ring quarantine yields a terminal
    /// error; driver shutdown ends a stream waiting for buffers. Other socket
    /// errors remain terminal.
    ///
    /// The kernel keeps the receive armed while the stream is not polled.
    /// Each receive completion takes at least one whole ring buffer, however
    /// few bytes arrived, and the stream holds it until a later `poll_next`
    /// yields it. A stream whose task stops polling, for example while one
    /// of its sends is blocked, can therefore take every buffer of a ring it
    /// shares with other streams, which then wait for a buffer return.
    /// Dropping the stream returns those buffers but discards the data they
    /// hold.
    /// [`Builder::max_unconsumed_per_receive`](crate::bufring::Builder::max_unconsumed_per_receive)
    /// bounds the buffers a stream holds: at the bound it pauses receiving,
    /// without losing data, until it is polled again.
    pub struct TcpRecvBundleMulti {
        #[pin]
        inner: Option<socket::RecvRingBundleStream>,
    }
}

impl TcpRecvBundleMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing, flags: i32) -> Self {
        Self {
            inner: Some(socket.recv_ring_bundle_stream(ring, flags)),
        }
    }
}

impl Stream for TcpRecvBundleMulti {
    type Item = io::Result<BufRingBufBundle>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        let Some(inner) = this.inner.as_mut().as_pin_mut() else {
            return Poll::Ready(None);
        };
        match ready!(inner.poll_next(cx)) {
            Some(Ok(bundle)) if bundle.buffer_count() == 0 => {
                this.inner.set(None);
                Poll::Ready(None)
            }
            item @ Some(_) => Poll::Ready(item),
            None => {
                this.inner.set(None);
                Poll::Ready(None)
            }
        }
    }
}

pin_project_lite::pin_project! {
    /// [`TcpStream`] represents a connected TCP socket.
    ///
    /// Bytes can be read from and written to the socket using the
    /// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite) traits.
    pub struct TcpStream {
        #[pin]
        reader: TcpStreamReader,
        #[pin]
        writer: TcpStreamWriter,
    }
}

impl std::fmt::Debug for TcpListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpListener").finish()
    }
}

impl std::fmt::Debug for TcpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpSocket").finish()
    }
}

impl std::fmt::Debug for TcpStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TcpStream").finish()
    }
}

impl TcpListenerOptions {
    /// Create listener options with all optional behavior disabled.
    pub const fn new() -> Self {
        Self { reuse_port: false }
    }

    /// Configure whether `SO_REUSEPORT` is enabled before binding.
    ///
    /// Every listener sharing an address must enable this option. The kernel
    /// distributes new connections among the listeners that share the address.
    #[must_use]
    pub const fn reuse_port(mut self, reuse_port: bool) -> Self {
        self.reuse_port = reuse_port;
        self
    }
}

impl TcpListener {
    /// Wrap a driver-bound descriptor as a TCP listener.
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

    /// Creates a TCP listener bound to the specified address.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be created, bound, or placed in
    /// listening mode.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind(addr: SocketAddr, backlog: u32) -> io::Result<TcpListener> {
        Self::bind_with_options(addr, backlog, TcpListenerOptions::default()).await
    }

    /// Creates a TCP listener with options applied before the socket is bound.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be created or configured, or if
    /// it cannot be bound or placed in listening mode.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind_with_options(
        addr: SocketAddr,
        backlog: u32,
        options: TcpListenerOptions,
    ) -> io::Result<TcpListener> {
        let inner = socket::Socket::bind_with_reuse_port(
            addr,
            Domain::for_address(addr),
            Type::STREAM,
            options.reuse_port,
        )
        .await?;
        inner.listen(backlog).await?;
        Ok(TcpListener { socket: inner })
    }

    /// Set value for the `SO_REUSEADDR` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.socket.set_reuse_address(reuse).await
    }

    /// Returns the local address that this listener is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accepts a new incoming connection to this listener.
    ///
    /// The returned socket is bound to the [`Driver`](crate::Driver) that owns
    /// the listener, whichever context finishes this future.
    ///
    /// # Errors
    ///
    /// Returns an error if the accept operation fails.
    pub async fn accept(&self) -> io::Result<(TcpSocket, SocketAddr)> {
        let (socket, addr) = self.socket.accept::<socket::Inet>().await?;
        Ok((TcpSocket { socket }, addr))
    }

    /// Returns a stream of incoming connections.
    ///
    /// The stream is backed by a multishot accept, so connections are accepted
    /// without submitting a request per connection. Each socket is bound to the
    /// [`Driver`](crate::Driver) that owns the listener.
    ///
    /// The kernel keeps accepting while the stream is not polled, and each
    /// accepted connection holds a descriptor until it is yielded. Once 64
    /// wait, the stream pauses its accept, so connections that arrive after
    /// the pause takes effect stay in the listen backlog until the consumer
    /// catches up.
    ///
    /// The limit does not bound the descriptors the stream holds. The pause
    /// is a cancellation that takes effect only after the driver reaps the
    /// completions and submits it, and the kernel's multishot accept takes
    /// every connection it can until then. The stream holds the limit plus
    /// every connection accepted before the pause takes effect: the whole
    /// queue at arm time, and every arrival until the driver next reaps and
    /// submits the cancellation. That window is about one executor turn, as
    /// every turn ends in a park, which reaps and submits before any wait,
    /// but it grows if a task blocks the thread or the executor does not
    /// reach the driver. The listen backlog does not bound this (it may never hold
    /// more than one connection). Beyond that, an arm ends when descriptors
    /// run out (`EMFILE`/`ENFILE`, yielded as an error), or when the
    /// completion queue is full, at about its size in descriptors; the
    /// latter depends on the kernel, and an older one may let the
    /// completions overflow instead of ending the arm. Each
    /// re-arm after a pause can take the same again. A caller that needs a
    /// hard bound on descriptors should use [`accept`](Self::accept), which
    /// takes one connection per request.
    /// [`incoming_with_limit`](Self::incoming_with_limit) sets another limit.
    ///
    /// An error is yielded as an item and does not end the stream; the next
    /// poll accepts again. The kernel can end a multishot accept without an
    /// error, for example when it cannot post a completion to a full completion
    /// queue. The stream re-arms after an accept that produced connections, and
    /// ends (`None`) when one ends without producing any, or once the driver
    /// stops accepting work.
    pub fn incoming(&self) -> impl Stream<Item = io::Result<TcpSocket>> + '_ {
        self.incoming_with_limit(Some(INCOMING_LIMIT))
    }

    /// Returns a stream of incoming connections that pauses its accept once
    /// `limit` accepted connections wait to be yielded. With `None` it never
    /// pauses, so a consumer that falls behind holds a descriptor for every
    /// connection the kernel accepts.
    ///
    /// `limit` is when the pause is requested, not a bound on the descriptors
    /// held. The pause takes effect only after the driver reaps the
    /// completions and submits the cancellation, and the kernel's multishot
    /// accept takes every connection it can until then. The stream holds
    /// `limit` plus every connection accepted before the pause takes effect:
    /// the whole queue at arm time, and every arrival until the driver next
    /// reaps and submits the cancellation, a window of about one executor
    /// turn, longer if a task blocks the thread or the executor does not
    /// reach the driver. The listen backlog does not bound this. An arm ends
    /// when descriptors run out, or when the completion queue is full (at
    /// about its size in descriptors), which depends on the kernel: an older
    /// one may let the completions overflow instead. For a hard bound, use
    /// [`accept`](Self::accept). Otherwise it behaves like
    /// [`incoming`](Self::incoming).
    pub fn incoming_with_limit(
        &self,
        limit: Option<NonZeroUsize>,
    ) -> impl Stream<Item = io::Result<TcpSocket>> + '_ {
        Incoming::new(&self.socket, limit, |socket| TcpSocket { socket })
    }

    /// Closes the listener.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if another owner or operation still
    /// retains the descriptor. Prepared operations are not submitted or cancelled
    /// by this call; queued, submitted, and completed-but-unconsumed operations
    /// continue retaining the descriptor until they are dropped or consumed. If
    /// close is rejected, dropping the last owner still closes the descriptor.
    ///
    /// Dropping an in-flight operation (an unfinished write, or an `incoming()`
    /// stream) only requests its cancellation. The operation keeps its reference
    /// to the descriptor until the kernel reports its terminal completion, which
    /// the driver reaps on a later turn, so a `close` called right after such a
    /// drop can still return `WouldBlock`. Yield to the driver (for example with
    /// [`noop`](crate::noop)) and retry, or let the last owner's drop close the
    /// descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] while another owner or operation
    /// retains the descriptor, or another I/O error if close fails.
    pub async fn close(self) -> io::Result<()> {
        self.socket.close().await
    }
}

impl TcpSocket {
    /// Wrap a driver-bound descriptor as a TCP socket.
    ///
    /// This does not inspect the descriptor. The kernel validates whether it
    /// supports each requested TCP operation.
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

    /// Creates a TCP connection to the specified address.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be created or connected.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpSocket> {
        let domain = Domain::for_address(addr);
        let socket_type = Type::STREAM;
        let protocol = None;
        let socket = socket::Socket::open(domain, socket_type, protocol).await?;
        socket.connect(addr.into()).await?;
        Ok(TcpSocket { socket })
    }

    /// Returns the local address that this stream is bound to.
    ///
    /// # Errors
    ///
    /// Returns an error if the address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns the remote address that this stream is connected to.
    ///
    /// # Errors
    ///
    /// Returns an error if the peer address cannot be read from the socket.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
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
    /// This takes ownership of the buffer, and returns future which resolves
    /// to a [`BufResult`](crate::BufResult) holding the number of bytes read and the buffer.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`]. A
    /// receive with [`RecvOptions::trunc`] fails with
    /// [`io::ErrorKind::InvalidInput`] and returns the buffer unchanged.
    ///
    /// [`WithOptions::options`]: crate::WithOptions::options
    pub fn recv<B: StableBufMut>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + crate::WithOptions<RecvOptions> {
        self.socket.recv_stream(buf)
    }

    /// Send data from the given buffer.
    ///
    /// This takes ownership of the buffer, and returns a future which resolves
    /// to a [`BufResult`](crate::BufResult) holding the number of bytes sent and the buffer.
    ///
    /// Set per-operation [`SendOptions`] with
    /// [`WithOptions::options`](crate::WithOptions::options).
    pub fn send<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<usize, B>> + crate::WithOptions<SendOptions> {
        self.socket.send(buf)
    }

    /// Send data from the given buffer using `io_uring` zerocopy send.
    ///
    /// A successful result reports both the number of bytes sent and whether
    /// the kernel copied any part of the payload. Usage reporting requires
    /// Linux 6.2 or newer; earlier kernels reject the request with `EINVAL`.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`TcpSocket::set_zerocopy`] first.
    ///
    /// Set per-operation [`SendOptions`] with
    /// [`WithOptions::options`](crate::WithOptions::options).
    pub fn send_zc<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<SendZcResult, B>> + crate::WithOptions<SendOptions>
    {
        self.socket.send_zc(buf)
    }

    /// Send a message from the given buffer using `io_uring` zerocopy sendmsg.
    ///
    /// A successful result reports both the number of bytes sent and whether
    /// the kernel copied any part of the payload. Usage reporting requires
    /// Linux 6.2 or newer; earlier kernels reject the request with `EINVAL`.
    ///
    /// This method does not fall back to regular sendmsg if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`TcpSocket::set_zerocopy`] first.
    ///
    /// Set per-operation [`SendOptions`] with
    /// [`WithOptions::options`](crate::WithOptions::options).
    pub fn send_msg_zc<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = BufResult<SendZcResult, B>> + crate::WithOptions<SendOptions>
    {
        self.socket.send_msg_zc(buf)
    }

    /// Recv data using the given buffer ring.
    ///
    /// A buffer will be taken from the ring when the operation is completed
    /// and returned. If there are no buffers available, the operation will
    /// fail. An orderly end of stream yields an empty buffer.
    ///
    /// Use [`TcpSocket::peer_addr`] for the remote address; it cannot be read
    /// after the connection has been reset or fully closed.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_ring(
        &self,
        ring: &RecvBufRing,
    ) -> impl crate::Request<Output = io::Result<BufRingBuf>> {
        self.socket.recv_ring(ring)
    }

    /// Receive data using a multishot recv operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_ring_multi(&self, ring: &RecvBufRing) -> TcpRecvRingMulti {
        TcpRecvRingMulti::new(&self.socket, ring)
    }

    /// Receive data using a single-shot recv bundle operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle(
        &self,
        ring: &RecvBufRing,
    ) -> impl crate::Request<Output = io::Result<BufRingBufBundle>> {
        self.socket.recv_ring_bundle(ring)
    }

    /// Receive data using a multishot recv bundle operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle_multi(&self, ring: &RecvBufRing) -> TcpRecvBundleMulti {
        TcpRecvBundleMulti::new(&self.socket, ring, 0)
    }

    /// Convert this socket into a stream.
    ///
    /// [`TcpStream`] is a stateful wrapper around [`TcpSocket`] that implements
    /// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite).
    pub fn into_stream(self) -> TcpStream {
        let reader = TcpStreamReader {
            inner: ReadyStream::new(self.socket.clone()),
        };
        let writer = TcpStreamWriter {
            inner: ReadyStream::new(self.socket.clone()),
        };

        TcpStream { reader, writer }
    }

    /// Close the socket.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if another owner or operation still
    /// retains the descriptor. Prepared operations are not submitted or cancelled
    /// by this call; queued, submitted, and completed-but-unconsumed operations
    /// continue retaining the descriptor until they are dropped or consumed. If
    /// close is rejected, dropping the last owner still closes the descriptor.
    ///
    /// Dropping an in-flight operation (an unfinished write, or an `incoming()`
    /// stream) only requests its cancellation. The operation keeps its reference
    /// to the descriptor until the kernel reports its terminal completion, which
    /// the driver reaps on a later turn, so a `close` called right after such a
    /// drop can still return `WouldBlock`. Yield to the driver (for example with
    /// [`noop`](crate::noop)) and retry, or let the last owner's drop close the
    /// descriptor.
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

    /// Return a stream of readiness events for this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`. The
    /// stream is backed by a multishot poll, which the kernel can end without
    /// an error, for example when it cannot post a completion to a full
    /// completion queue. The stream then polls again, so it ends only after
    /// yielding an error, such as `ECANCELED` once the driver shuts down.
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

    /// Set value for the `SO_REUSEADDR` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.socket.set_reuse_address(reuse).await
    }

    /// Set value for the `SO_KEEPALIVE` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        self.socket.set_keepalive(keepalive).await
    }
    /// Set the value of the `TCP_NODELAY` option on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed.
    pub async fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.socket.set_nodelay(nodelay).await
    }

    /// Enable or disable `SO_ZEROCOPY` on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed or is unsupported.
    pub async fn set_zerocopy(&self, enabled: bool) -> io::Result<()> {
        self.socket.set_zerocopy(enabled).await
    }
}

impl TcpStream {
    /// Split the stream into a reader and writer.
    pub fn owned_split(self) -> (TcpStreamReader, TcpStreamWriter) {
        (self.reader, self.writer)
    }
}

impl tokio::io::AsyncRead for TcpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.project();
        this.reader.poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TcpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        this.writer.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        this.writer.poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        this.writer.poll_shutdown(cx)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        this.writer.poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.writer.is_write_vectored()
    }
}

pin_project_lite::pin_project! {
    /// [`TcpStreamReader`] is the read half of a [`TcpStream`].
    pub struct TcpStreamReader {
        #[pin]
        inner: ReadyStream,
    }
}

pin_project_lite::pin_project! {
    /// [`TcpStreamWriter`] is the write half of a [`TcpStream`].
    pub struct TcpStreamWriter {
        #[pin]
        inner: ReadyStream,
    }
}

impl tokio::io::AsyncRead for TcpStreamReader {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.project().inner.poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for TcpStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.project().inner.poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // TCP has no userspace buffer: every accepted write has already been
        // handed to the kernel, so there is nothing to flush and no reason to
        // wait for writability.
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

#[cfg(test)]
mod tests {
    use std::future::poll_fn;
    use std::pin::pin;

    use norn_executor::LocalExecutor;
    use tokio::io::AsyncWrite;

    use super::*;

    #[test]
    fn stream_write_to_closed_peer_fails_instead_of_raising_sigpipe() {
        crate::test_util::run_isolated(
            "net::tcp::tests::stream_write_to_closed_peer_fails_instead_of_raising_sigpipe",
            || {
                // Hosts that restore the default disposition die on SIGPIPE.
                unsafe { libc::signal(libc::SIGPIPE, libc::SIG_DFL) };
                let driver = crate::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
                let mut executor = LocalExecutor::new(driver);
                executor.block_on(async {
                    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8)
                        .await
                        .unwrap();
                    let peer =
                        std::net::TcpStream::connect(listener.local_addr().unwrap()).unwrap();
                    let (socket, _) = listener.accept().await.unwrap();
                    drop(peer);

                    let mut stream = pin!(socket.into_stream());
                    let mut error = None;
                    for _ in 0..64 {
                        let result = poll_fn(|cx| stream.as_mut().poll_write(cx, b"payload")).await;
                        if let Err(err) = result {
                            error = Some(err);
                            break;
                        }
                    }
                    let error = error.expect("writes to a closed peer should eventually fail");
                    assert!(
                        matches!(
                            error.kind(),
                            io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset
                        ),
                        "unexpected error: {error}"
                    );
                });
            },
        );
    }
}
