use std::io;
use std::mem::ManuallyDrop;
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
use crate::operation::Op;

use super::socket::AcceptMulti;

const LOG: &str = "norn_uring::net::tcp";

/// Linux `IOV_MAX`: the most `iovec`s one `sendmsg(2)` call accepts.
const IOV_MAX: usize = 1024;

/// Accepted connections [`TcpListener::incoming`] holds before it pauses.
const INCOMING_LIMIT: NonZeroUsize = NonZeroUsize::new(64).unwrap();

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

    /// Steer new connections on this listener's reuse-port group by the CPU
    /// that processed their SYN.
    ///
    /// The map applies to the whole group, whichever member attaches it, and
    /// replaces any program attached before. Attaching it before every
    /// listener has joined is fine: a route to a slot the group does not have
    /// yet falls back to hash selection. See [`ReuseportCpuMap`] for how
    /// slots are numbered.
    ///
    /// # Errors
    ///
    /// Returns the `setsockopt` error: `EINVAL` when the listener was not
    /// created with [`TcpListenerOptions::reuse_port`], or `ENOMEM` when the
    /// program exceeds `net.core.optmem_max`. Steering only affects
    /// locality, so callers may continue with hash selection.
    ///
    /// [`ReuseportCpuMap`]: crate::net::ReuseportCpuMap
    pub fn attach_reuseport_cpu_map(&self, map: &super::ReuseportCpuMap) -> io::Result<()> {
        map.attach(self.socket.as_uring_fd().fd().0)
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
        let (socket, addr) = self.socket.accept().await?;
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
    /// wait, the stream pauses its accept, and later connections stay in the
    /// listen backlog until the consumer catches up.
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
    /// Connections the kernel accepted before a pause took effect are still
    /// yielded, so the stream can briefly hold more than `limit`. Otherwise
    /// it behaves like [`incoming`](Self::incoming).
    pub fn incoming_with_limit(
        &self,
        limit: Option<NonZeroUsize>,
    ) -> impl Stream<Item = io::Result<TcpSocket>> + '_ {
        Incoming {
            listener: &self.socket,
            limit,
            current: None,
            accepted: false,
        }
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

pin_project_lite::pin_project! {
    /// A stream of connections accepted by a [`TcpListener`].
    struct Incoming<'a> {
        listener: &'a socket::Socket,
        limit: Option<NonZeroUsize>,
        #[pin]
        current: Option<Op<AcceptMulti>>,
        // Whether the current operation produced a connection.
        accepted: bool,
    }
}

impl Stream for Incoming<'_> {
    type Item = io::Result<TcpSocket>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        loop {
            if let Some(current) = this.current.as_mut().as_pin_mut() {
                match ready!(current.poll_next(cx)) {
                    Some(Err(err)) => {
                        this.current.set(None);
                        return Poll::Ready(Some(Err(err)));
                    }

                    Some(Ok(socket)) => {
                        *this.accepted = true;
                        let socket = socket::Socket::from_fd(socket);
                        return Poll::Ready(Some(Ok(TcpSocket { socket })));
                    }
                    None => {
                        this.current.set(None);
                        // The kernel also ends a multishot accept without an
                        // error, for example when it cannot post a completion
                        // to a full completion queue, and a pause at the limit
                        // ends one quietly. Re-arm after an operation that
                        // accepted connections; one that ended without
                        // producing anything ends the stream.
                        if !std::mem::take(this.accepted) {
                            return Poll::Ready(None);
                        }
                    }
                }
            }
            // Once the driver stops accepting work, every new accept would fail
            // at once, and a consumer that continues past errors would spin.
            if !this.listener.driver_running() {
                return Poll::Ready(None);
            }
            this.current
                .set(Some(this.listener.accept_multi(*this.limit)));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
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
        socket.connect(addr).await?;
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

    /// Return the CPU that last processed a packet received on this socket.
    ///
    /// This reads `SO_INCOMING_CPU`. It returns `None` before the kernel has
    /// recorded a CPU. For a freshly accepted socket it is the CPU that
    /// completed the handshake, which with RSS is the connection's receive
    /// queue CPU.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be read.
    pub fn incoming_cpu(&self) -> io::Result<Option<usize>> {
        let mut cpu: libc::c_int = -1;
        let mut len = std::mem::size_of::<libc::c_int>() as libc::socklen_t;
        // SAFETY: `cpu` and `len` are valid for writes of the sizes given.
        let result = unsafe {
            libc::getsockopt(
                self.socket.as_uring_fd().fd().0,
                libc::SOL_SOCKET,
                libc::SO_INCOMING_CPU,
                std::ptr::from_mut(&mut cpu).cast(),
                &mut len,
            )
        };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(usize::try_from(cpu).ok())
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
    /// to a tuple of the original buffer and the number of bytes read.
    pub fn recv<B: StableBufMut>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)> {
        self.socket.recv(buf)
    }

    /// Recv data into the given buffer with recv flags.
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] with the buffer unchanged if
    /// `flags` includes `MSG_TRUNC`. On TCP that flag discards input without
    /// initializing the buffer, so it is rejected before submission.
    pub fn recv_with_flags<B: StableBufMut>(
        &self,
        buf: B,
        flags: i32,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)> {
        self.socket.recv_tcp_with_flags(buf, flags)
    }

    /// Send data from the given buffer.
    ///
    /// This takes ownership of the buffer, and returns a future which resolves
    /// to a tuple of the original buffer and the number of bytes sent.
    pub fn send<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)> {
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
    pub fn send_zc<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<SendZcResult>, B)> {
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
    pub fn send_msg_zc<B: StableBuf>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<SendZcResult>, B)> {
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

    /// Receive data using a single-shot recv bundle operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    ///
    /// # Errors
    ///
    /// The request resolves with [`io::ErrorKind::InvalidInput`] when `flags`
    /// contains `MSG_TRUNC`, `MSG_PEEK` or `MSG_WAITALL`. `MSG_TRUNC` does not
    /// identify the number of buffers selected by a bundle receive, a kernel
    /// bundle retry peeks the start of the queue again under `MSG_PEEK`, and a
    /// partial `MSG_WAITALL` retry can commit buffers that the completion does
    /// not report.
    pub fn recv_bundle_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> impl crate::Request<Output = io::Result<BufRingBufBundle>> {
        self.socket.recv_ring_bundle_with_flags(ring, flags)
    }

    /// Receive data using a multishot recv bundle operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle_multi(&self, ring: &RecvBufRing) -> TcpRecvBundleMulti {
        TcpRecvBundleMulti::new(&self.socket, ring, 0)
    }

    /// Receive data using a multishot recv bundle operation and a provided buffer ring.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    ///
    /// # Errors
    ///
    /// The stream yields [`io::ErrorKind::InvalidInput`] and terminates when
    /// `flags` contains `MSG_TRUNC`, `MSG_PEEK`, or `MSG_WAITALL`. `MSG_TRUNC`
    /// does not identify the number of selected buffers, a kernel bundle retry
    /// peeks the start of the queue again, and multishot receives do not
    /// support `MSG_WAITALL`.
    pub fn recv_bundle_multi_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> TcpRecvBundleMulti {
        TcpRecvBundleMulti::new(&self.socket, ring, flags)
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
        let this = self.project();

        let n = ready!(this.inner.poll_op(
            cx,
            |sock| { unsafe { sock.recv(buf.unfilled_mut()) } },
            socket::READ_FLAGS as u32,
        ))?;
        unsafe {
            buf.assume_init(n);
            buf.advance(n);
        }
        Poll::Ready(Ok(()))
    }
}

impl tokio::io::AsyncWrite for TcpStreamWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.project();
        let n = ready!(this.inner.poll_op(
            cx,
            |sock| sock.send_with_flags(buf, libc::MSG_NOSIGNAL),
            socket::WRITE_FLAGS as u32
        ))?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // TCP has no userspace buffer: every accepted write has already been
        // handed to the kernel, so there is nothing to flush and no reason to
        // wait for writability.
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        // `shutdown(SHUT_WR)` does not depend on send-buffer space, so it must
        // not wait on the write readiness gate, which stays closed after an
        // abandoned `EAGAIN` write until the peer drains the buffer.
        let this = self.project();
        let sock = this.inner.project().inner.as_socket();
        Poll::Ready(sock.shutdown(std::net::Shutdown::Write))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        // The kernel rejects more than IOV_MAX iovecs with EMSGSIZE. Like
        // `std`, pass only the first IOV_MAX slices; a short write is valid.
        // Skip leading empty slices first, so non-empty input never caps down
        // to a prefix of empty slices and reports `Ok(0)`.
        let skip = bufs.iter().take_while(|buf| buf.is_empty()).count();
        let bufs = &bufs[skip..];
        let bufs = &bufs[..bufs.len().min(IOV_MAX)];
        let n = ready!(this.inner.poll_op(
            cx,
            |sock| sock.send_vectored_with_flags(bufs, libc::MSG_NOSIGNAL),
            socket::WRITE_FLAGS as u32
        ))?;
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

pin_project_lite::pin_project! {
    /// Readiness-driven nonblocking I/O on one direction of a socket.
    ///
    /// The operation is attempted first. Only after it returns `EAGAIN` is a
    /// multishot poll armed. It then stays armed for the life of the half, so
    /// later waits submit nothing; the kernel posts a completion whenever the
    /// socket becomes ready.
    ///
    /// The kernel also posts those completions while the caller keeps up
    /// without waiting. Every completion queued on the poll was reaped before
    /// the current attempt ran, so after the attempt none of them says
    /// anything the attempt has not already observed: a success leaves the
    /// half notified and the next call attempts the operation directly, and
    /// an `EAGAIN` shows the readiness they reported has been consumed. Each
    /// attempt therefore discards them without a syscall, which bounds the
    /// queue by the completions reaped between two attempts. Readiness that
    /// arrives after an `EAGAIN` wakes the socket's wait queue again and posts
    /// a new completion, so discarding loses none.
    ///
    /// This relies on completions never being reaped while a task is being
    /// polled. Only the driver's park drains the CQ, between task polls, so
    /// nothing can be reaped between the attempt and the discard that
    /// follows it. If completions were ever reaped during a poll (a drain
    /// on the submit path, or a nested driver run from inside a task), a
    /// completion for readiness that arrived after the attempt's `EAGAIN`
    /// could be queued before the discard and dropped with the stale ones,
    /// losing the wakeup.
    ///
    /// The kernel can end a multishot poll without an error, for example when
    /// it cannot post a completion to a full CQ. The next wait then arms a
    /// fresh poll, which checks the current readiness mask when it is armed,
    /// so readiness that arrived in between is not lost.
    struct ReadyStream {
        inner: socket::Socket,
        #[pin]
        armed: Option<Op<socket::Poll<true>>>,
        notified: bool,
    }
}

impl ReadyStream {
    fn new(inner: socket::Socket) -> Self {
        Self {
            inner,
            armed: None,
            notified: true,
        }
    }

    fn poll_op<U>(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        mut f: impl FnMut(ManuallyDrop<socket2::Socket>) -> io::Result<U>,
        flags: u32,
    ) -> Poll<io::Result<U>> {
        loop {
            log::trace!(target: LOG, "poll_op");
            ready!(self.as_mut().poll_ready(cx, flags))?;
            log::trace!(target: LOG, "poll_op.ready");
            let mut this = self.as_mut().project();
            let sock = this.inner.as_socket();
            let res = f(sock);
            discard_stale(this.armed.as_mut());
            match res {
                Ok(res) => {
                    log::trace!(target: LOG, "poll_op.success");
                    return Poll::Ready(Ok(res));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    log::trace!(target: LOG, "poll_op.would_block");
                    *this.notified = false;
                }
                Err(err) => {
                    log::trace!(target: LOG, "poll_op.err");
                    return Poll::Ready(Err(err));
                }
            }
        }
    }

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>, flags: u32) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            if *this.notified {
                // If the notified bit is set, return immediately.
                return Poll::Ready(Ok(()));
            }
            if let Some(armed) = this.armed.as_mut().as_pin_mut() {
                match ready!(armed.poll_next(cx)) {
                    Some(Ok(_)) => {
                        log::trace!(target: LOG, "poll_op.ready.stream_notified");
                        *this.notified = true;
                    }
                    Some(Err(err)) => {
                        // A failed poll has ended; the next wait arms a fresh
                        // one.
                        this.armed.set(None);
                        return Poll::Ready(Err(err));
                    }
                    // The kernel ended the poll without an error. Its terminal
                    // completion has been delivered, so it may be replaced.
                    None => {
                        log::trace!(target: LOG, "poll_op.ready.stream_done");
                        this.armed.set(None);
                    }
                }
            } else {
                log::trace!(target: LOG, "poll_op.ready.stream_arm");
                this.armed
                    .set(Some(this.inner.poll_readiness::<true>(flags)));
            }
        }
    }
}

/// Discard readiness events queued on `armed` without a syscall.
///
/// Called after an attempt, every queued event was reaped before the attempt
/// ran and is stale, because the driver never reaps completions while a task
/// is being polled; see [`ReadyStream`]. A poll that has ended, with or
/// without an error, is dropped so the next wait arms a fresh one.
#[inline]
fn discard_stale(armed: Pin<&mut Option<Op<socket::Poll<true>>>>) {
    if armed
        .as_ref()
        .get_ref()
        .as_ref()
        .is_some_and(Op::has_queued)
    {
        discard_stale_slow(armed);
    }
}

#[cold]
#[inline(never)]
fn discard_stale_slow(mut armed: Pin<&mut Option<Op<socket::Poll<true>>>>) {
    let Some(mut op) = armed.as_mut().as_pin_mut() else {
        return;
    };
    loop {
        match op.as_mut().poll_queued() {
            Poll::Pending => return,
            Poll::Ready(Some(Ok(_))) => log::trace!(target: LOG, "poll_op.stale"),
            Poll::Ready(Some(Err(_)) | None) => break,
        }
    }
    armed.set(None);
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
