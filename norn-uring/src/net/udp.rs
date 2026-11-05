//! UDP Protocol Socket
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use socket2::{Domain, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRingBuf, BufRingBufBundle, RecvBufRing};
use crate::fd::UringFd;
use crate::net::socket::{self, Event, RecvMsgRingBuf, SendZcResult};
use crate::net::{RecvOptions, SendOptions};
use crate::WithOptions;

/// A UDP socket.
///
/// After creating a `UdpSocket` by [`UdpSocket::bind`]ing it to a socket address, data can be
/// [sent to] and [received from] any other socket address.
pub struct UdpSocket {
    inner: socket::Socket,
}

pin_project_lite::pin_project! {
    /// A stream of datagrams received into buffers from a registered ring.
    ///
    /// Empty datagrams are yielded as empty, zero-capacity buffers.
    ///
    /// Buffer exhaustion is absorbed as backpressure: receiving waits for a
    /// buffer return and re-arms without yielding ENOBUFS or discarding queued data.
    /// Drop held buffers to allow progress. Ring quarantine yields a terminal
    /// error; driver shutdown ends a stream waiting for buffers. Other socket
    /// errors remain terminal.
    ///
    /// The kernel keeps the receive armed while the stream is not polled.
    /// Each arriving datagram takes a whole ring buffer, however small, and
    /// the stream holds that buffer until a later `poll_next` yields it. A
    /// stream whose task stops polling, for example while it awaits other
    /// work, can therefore take every buffer of a ring it shares with
    /// other streams, which then wait for a buffer return. Dropping the
    /// stream returns those buffers but discards the data they hold.
    /// [`Builder::max_unconsumed_per_receive`](crate::bufring::Builder::max_unconsumed_per_receive)
    /// bounds the buffers a stream holds: at the bound it pauses receiving,
    /// without losing data, until it is polled again.
    pub struct UdpRecvRingMulti {
        #[pin]
        inner: socket::RecvRingStream,
    }
}

impl UdpRecvRingMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing) -> Self {
        Self {
            inner: socket.recv_ring_stream(ring),
        }
    }
}

impl Stream for UdpRecvRingMulti {
    type Item = io::Result<BufRingBuf>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pin_project_lite::pin_project! {
    /// A stream of datagrams received into buffer bundles from a registered ring.
    ///
    /// Empty datagrams are yielded as empty, zero-buffer bundles.
    ///
    /// Buffer exhaustion is absorbed as backpressure: receiving waits for a
    /// buffer return and re-arms without yielding ENOBUFS or discarding queued data.
    /// Drop held buffers to allow progress. Ring quarantine yields a terminal
    /// error; driver shutdown ends a stream waiting for buffers. Other socket
    /// errors remain terminal.
    ///
    /// The kernel keeps the receive armed while the stream is not polled.
    /// Each arriving datagram takes a whole ring buffer, however small, and
    /// the stream holds that buffer until a later `poll_next` yields it. A
    /// stream whose task stops polling, for example while it awaits other
    /// work, can therefore take every buffer of a ring it shares with
    /// other streams, which then wait for a buffer return. Dropping the
    /// stream returns those buffers but discards the data they hold.
    /// [`Builder::max_unconsumed_per_receive`](crate::bufring::Builder::max_unconsumed_per_receive)
    /// bounds the buffers a stream holds: at the bound it pauses receiving,
    /// without losing data, until it is polled again.
    pub struct UdpRecvBundleMulti {
        #[pin]
        inner: socket::RecvRingBundleStream,
    }
}

impl UdpRecvBundleMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing, flags: i32) -> Self {
        Self {
            inner: socket.recv_ring_bundle_stream(ring, flags),
        }
    }
}

impl Stream for UdpRecvBundleMulti {
    type Item = io::Result<BufRingBufBundle>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

pin_project_lite::pin_project! {
    /// A persistent stream of datagrams and sender addresses from a buffer ring.
    ///
    /// Successful terminal kernel completions transparently re-arm receiving.
    /// Buffer exhaustion is absorbed as backpressure: the stream waits for a
    /// buffer return, then re-arms without yielding ENOBUFS or discarding queued data.
    /// Drop held buffers to allow progress. Ring quarantine yields a terminal
    /// error; driver shutdown ends a stream waiting for buffers. Other socket
    /// errors remain terminal.
    ///
    /// The kernel keeps the receive armed while the stream is not polled.
    /// Each arriving datagram takes a whole ring buffer, however small, and
    /// the stream holds that buffer until a later `poll_next` yields it. A
    /// stream whose task stops polling, for example while it awaits other
    /// work, can therefore take every buffer of a ring it shares with
    /// other streams, which then wait for a buffer return. Dropping the
    /// stream returns those buffers but discards the data they hold.
    /// [`Builder::max_unconsumed_per_receive`](crate::bufring::Builder::max_unconsumed_per_receive)
    /// bounds the buffers a stream holds: at the bound it pauses receiving,
    /// without losing data, until it is polled again.
    pub struct UdpRecvFromRingMulti {
        #[pin]
        inner: socket::RecvFromRingStream,
    }
}

impl Stream for UdpRecvFromRingMulti {
    type Item = io::Result<(RecvMsgRingBuf, SocketAddr)>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl std::fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpSocket").finish()
    }
}

impl UdpSocket {
    /// Wrap a driver-bound descriptor as a UDP socket.
    ///
    /// This does not inspect the descriptor. The kernel validates whether it
    /// supports each requested UDP operation.
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

    /// Creates a UDP socket from the given address.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be created or bound.
    ///
    /// # Panics
    ///
    /// Panics if called outside an active [`Driver`](crate::Driver) context.
    pub async fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let inner = socket::Socket::bind(addr, Domain::for_address(addr), Type::DGRAM).await?;
        Ok(UdpSocket { inner })
    }

    /// Connect this socket to a remote address.
    ///
    /// A connected UDP socket can use [`UdpSocket::send`] and [`UdpSocket::recv`] without passing
    /// an address for each operation.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket cannot be connected to `addr`.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.connect(addr.into()).await
    }

    /// Returns the socket address that this socket was created from.
    ///
    /// # Errors
    ///
    /// Returns an error if the local address cannot be read from the socket.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the socket address of the remote peer.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket is not connected or the peer address
    /// cannot be read.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.inner.peer_addr()
    }

    /// Sends a single datagram message on the socket to the given address.
    ///
    /// On success, returns the number of bytes written.
    ///
    /// This takes ownership of the buffer provided and will return it back
    /// once the operation has completed.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    pub fn send_to<B>(
        &self,
        buf: B,
        addr: SocketAddr,
    ) -> impl std::future::Future<Output = (io::Result<usize>, B)> + WithOptions<SendOptions> + '_
    where
        B: StableBuf + 'static,
    {
        self.inner.send_to(buf, addr)
    }

    /// Sends a single datagram message on a connected socket.
    ///
    /// This takes ownership of the buffer provided and will return it back
    /// once the operation has completed.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    pub fn send<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)> + WithOptions<SendOptions>
    where
        B: StableBuf + 'static,
    {
        self.inner.send(buf)
    }

    /// Sends a single datagram on a connected socket using `io_uring` zerocopy send.
    ///
    /// A successful result reports both the number of bytes sent and whether
    /// the kernel copied any part of the payload. Usage reporting requires
    /// Linux 6.2 or newer; earlier kernels reject the request with `EINVAL`.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`UdpSocket::set_zerocopy`] first.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    pub fn send_zc<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<SendZcResult>, B)> + WithOptions<SendOptions>
    where
        B: StableBuf + 'static,
    {
        self.inner.send_zc(buf)
    }

    /// Receives a single datagram message on the socket. On success, returns the number
    /// of bytes read and the origin.
    ///
    /// This must be called with a buf of sufficient size to hold the message. If a message
    /// is too long to fit in the supplied, buffer, excess bytes may be discarded.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`].
    pub fn recv_from<B>(
        &self,
        buf: B,
    ) -> impl std::future::Future<Output = (io::Result<(usize, SocketAddr)>, B)>
           + WithOptions<RecvOptions>
           + '_
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv_from::<socket::Inet, B>(buf)
    }

    /// Receives a single datagram message on a connected socket.
    ///
    /// This must be called with a buffer of sufficient size to hold the message. If a message
    /// is too long to fit in the supplied buffer, excess bytes may be discarded.
    ///
    /// Set per-operation [`RecvOptions`] with [`WithOptions::options`].
    pub fn recv<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<usize>, B)> + WithOptions<RecvOptions>
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv(buf)
    }

    /// Sends a message on a connected socket using `io_uring` zerocopy sendmsg.
    ///
    /// A successful result reports both the number of bytes sent and whether
    /// the kernel copied any part of the payload. Usage reporting requires
    /// Linux 6.2 or newer; earlier kernels reject the request with `EINVAL`.
    ///
    /// This method does not fall back to regular sendmsg if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`UdpSocket::set_zerocopy`] first.
    ///
    /// Set per-operation [`SendOptions`] with [`WithOptions::options`].
    pub fn send_msg_zc<B>(
        &self,
        buf: B,
    ) -> impl crate::Request<Output = (io::Result<SendZcResult>, B)> + WithOptions<SendOptions>
    where
        B: StableBuf + 'static,
    {
        self.inner.send_msg_zc(buf)
    }

    /// Receives a single datagram message on the socket using a buffer
    /// from the given ring.
    ///
    /// The buffer ring used must contain buffers of sufficient size to hold the message. If
    /// a message is too long to fit in the supplied, buffer, excess bytes may be discarded.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    ///
    /// # Errors
    ///
    /// Returns an error if the receive operation fails or its completion does
    /// not identify a selected buffer.
    pub async fn recv_from_ring(&self, ring: &RecvBufRing) -> io::Result<(BufRingBuf, SocketAddr)> {
        self.inner.recv_from_ring(ring).await
    }

    /// Wait for one readiness event on this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    pub fn poll_readiness(&self, events: u32) -> impl crate::Request<Output = io::Result<Event>> {
        self.inner.poll_readiness::<false>(events)
    }

    /// Return a stream of readiness events for this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`. The
    /// stream is backed by a multishot poll, which the kernel can end without
    /// an error, for example when it cannot post a completion to a full
    /// completion queue. The stream then polls again, so it ends only after
    /// yielding an error, such as `ECANCELED` once the driver shuts down.
    pub fn poll_readiness_multi(&self, events: u32) -> impl Stream<Item = io::Result<Event>> {
        socket::ReadinessStream::new(&self.inner, events)
    }

    /// Receives datagrams from this socket using a multishot recvmsg operation backed by the
    /// provided buffer ring.
    ///
    /// Each yielded item includes payload bytes and the sender address.
    /// See [`UdpRecvFromRingMulti`] for recovery and backpressure behavior.
    ///
    /// # Buffer sizing
    ///
    /// The kernel stores a 16-byte `io_uring_recvmsg_out` header and a 28-byte
    /// sender address field ahead of the payload in each ring buffer, so each
    /// buffer carries at most `ring.buf_capacity() - 44` payload bytes. A
    /// larger datagram is truncated to fit, and the yielded
    /// [`RecvMsgRingBuf::is_truncated`] returns `true`. Rings with buffers
    /// shorter than 44 bytes cannot receive with this operation.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_from_ring_multi(&self, ring: &RecvBufRing) -> UdpRecvFromRingMulti {
        UdpRecvFromRingMulti {
            inner: self.inner.recv_from_ring_stream(ring),
        }
    }

    /// Receives datagrams from a connected socket using a multishot recv operation backed by the
    /// provided buffer ring.
    ///
    /// Empty datagrams are yielded as empty, zero-capacity buffers. Linux ends
    /// the underlying multishot operation after such a completion, so this
    /// stream transparently submits a replacement receive before continuing.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_ring_multi(&self, ring: &RecvBufRing) -> UdpRecvRingMulti {
        UdpRecvRingMulti::new(&self.inner, ring)
    }

    /// Receives data from a connected socket using a single-shot recv bundle operation.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle(
        &self,
        ring: &RecvBufRing,
    ) -> impl crate::Request<Output = io::Result<BufRingBufBundle>> {
        self.inner.recv_ring_bundle(ring)
    }

    /// Receives data from a connected socket using a multishot recv bundle operation.
    ///
    /// Empty datagrams are yielded as empty bundles and the underlying
    /// multishot operation is transparently rearmed.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle_multi(&self, ring: &RecvBufRing) -> UdpRecvBundleMulti {
        UdpRecvBundleMulti::new(&self.inner, ring, 0)
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
        self.inner.close().await
    }

    /// Enable or disable `SO_ZEROCOPY` on this socket.
    ///
    /// # Errors
    ///
    /// Returns an error if the socket option cannot be changed or is unsupported.
    pub async fn set_zerocopy(&self, enabled: bool) -> io::Result<()> {
        self.inner.set_zerocopy(enabled).await
    }
}
