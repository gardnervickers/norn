//! UDP Protocol Socket
use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_core::Stream;
use socket2::{Domain, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRingBuf, BufRingBufBundle, RecvBufRing};
use crate::fd::UringFd;
use crate::net::socket::{self, Event, RecvMsgRingBuf};
use crate::operation::Op;

/// A UDP socket.
///
/// After creating a `UdpSocket` by [`UdpSocket::bind`]ing it to a socket address, data can be
/// [sent to] and [received from] any other socket address.
pub struct UdpSocket {
    inner: socket::Socket,
}

pin_project_lite::pin_project! {
    struct DatagramRecvRingMulti {
        socket: socket::Socket,
        ring: RecvBufRing,
        #[pin]
        current: Option<Op<socket::RecvRingMulti>>,
        rearm: bool,
    }
}

impl DatagramRecvRingMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing) -> Self {
        let socket = socket.clone();
        let ring = ring.clone();
        let current = Some(socket.recv_ring_multi(&ring, socket::ZeroByteBehavior::Datagram));
        Self {
            socket,
            ring,
            current,
            rearm: false,
        }
    }
}

impl Stream for DatagramRecvRingMulti {
    type Item = io::Result<BufRingBuf>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            let current = this
                .current
                .as_mut()
                .as_pin_mut()
                .expect("datagram receive operation missing");
            match ready!(current.poll_next(cx)) {
                Some(item) => {
                    *this.rearm = item
                        .as_ref()
                        .is_ok_and(|buffer| buffer.is_empty() && buffer.capacity() == 0);
                    return Poll::Ready(Some(item));
                }
                None if *this.rearm => {
                    *this.rearm = false;
                    this.current.set(Some(
                        this.socket
                            .recv_ring_multi(this.ring, socket::ZeroByteBehavior::Datagram),
                    ));
                }
                None => return Poll::Ready(None),
            }
        }
    }
}

pin_project_lite::pin_project! {
    struct DatagramRecvBundleMulti {
        socket: socket::Socket,
        ring: RecvBufRing,
        flags: i32,
        #[pin]
        current: Option<Op<socket::RecvRingBundleMulti>>,
        rearm: bool,
    }
}

impl DatagramRecvBundleMulti {
    fn new(socket: &socket::Socket, ring: &RecvBufRing, flags: i32) -> Self {
        let socket = socket.clone();
        let ring = ring.clone();
        let current = Some(socket.recv_ring_bundle_multi_with_flags(
            &ring,
            flags,
            socket::ZeroByteBehavior::Datagram,
        ));
        Self {
            socket,
            ring,
            flags,
            current,
            rearm: false,
        }
    }
}

impl Stream for DatagramRecvBundleMulti {
    type Item = io::Result<BufRingBufBundle>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let mut this = self.as_mut().project();
            let current = this
                .current
                .as_mut()
                .as_pin_mut()
                .expect("datagram bundle receive operation missing");
            match ready!(current.poll_next(cx)) {
                Some(item) => {
                    *this.rearm = item
                        .as_ref()
                        .is_ok_and(|bundle| bundle.is_empty() && bundle.buffer_count() == 0);
                    return Poll::Ready(Some(item));
                }
                None if *this.rearm => {
                    *this.rearm = false;
                    this.current
                        .set(Some(this.socket.recv_ring_bundle_multi_with_flags(
                            this.ring,
                            *this.flags,
                            socket::ZeroByteBehavior::Datagram,
                        )));
                }
                None => return Poll::Ready(None),
            }
        }
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
        self.inner.connect(addr).await
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
    pub async fn send_to<B>(&self, buf: B, addr: SocketAddr) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_to(buf, addr).await
    }

    /// Sends a single datagram message on the socket to the given address with the
    /// provided send flags.
    pub async fn send_to_with_flags<B>(
        &self,
        buf: B,
        addr: SocketAddr,
        flags: i32,
    ) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_to_with_flags(buf, addr, flags).await
    }

    /// Sends a single datagram message on a connected socket.
    ///
    /// This takes ownership of the buffer provided and will return it back
    /// once the operation has completed.
    pub async fn send<B>(&self, buf: B) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send(buf).await
    }

    /// Sends a single datagram message on a connected socket with the provided
    /// send flags.
    pub async fn send_with_flags<B>(&self, buf: B, flags: i32) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_with_flags(buf, flags).await
    }

    /// Sends a single datagram on a connected socket using `io_uring` zerocopy send.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`UdpSocket::set_zerocopy`] first.
    pub async fn send_zc<B>(&self, buf: B) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_zc(buf).await
    }

    /// Sends a single datagram on a connected socket using `io_uring` zerocopy send with flags.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`UdpSocket::set_zerocopy`] first.
    pub async fn send_zc_with_flags<B>(&self, buf: B, flags: i32) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_zc_with_flags(buf, flags).await
    }

    /// Receives a single datagram message on the socket. On success, returns the number
    /// of bytes read and the origin.
    ///
    /// This must be called with a buf of sufficient size to hold the message. If a message
    /// is too long to fit in the supplied, buffer, excess bytes may be discarded.
    pub async fn recv_from<B>(&self, buf: B) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv_from(buf).await
    }

    /// Receives a single datagram message on the socket with the provided recv flags.
    ///
    /// With `MSG_TRUNC`, the returned byte count is the full datagram length and may
    /// exceed the initialized length of the returned buffer.
    pub async fn recv_from_with_flags<B>(
        &self,
        buf: B,
        flags: i32,
    ) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv_from_with_flags(buf, flags).await
    }

    /// Receives a single datagram message on a connected socket.
    ///
    /// This must be called with a buffer of sufficient size to hold the message. If a message
    /// is too long to fit in the supplied buffer, excess bytes may be discarded.
    pub async fn recv<B>(&self, buf: B) -> (io::Result<usize>, B)
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv(buf).await
    }

    /// Receives a single datagram message on a connected socket with the provided
    /// recv flags.
    ///
    /// With `MSG_TRUNC`, the returned byte count is the full datagram length and may
    /// exceed the initialized length of the returned buffer.
    pub async fn recv_with_flags<B>(&self, buf: B, flags: i32) -> (io::Result<usize>, B)
    where
        B: StableBufMut + 'static,
    {
        self.inner.recv_with_flags(buf, flags).await
    }

    /// Send a message on this socket using message-style flags.
    ///
    /// If `addr` is `Some`, the datagram is sent to that destination. If `None`, the socket
    /// must already be connected.
    pub async fn send_msg<B>(
        &self,
        buf: B,
        addr: Option<SocketAddr>,
        flags: i32,
    ) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        match addr {
            Some(addr) => self.send_to_with_flags(buf, addr, flags).await,
            None => self.send_with_flags(buf, flags).await,
        }
    }

    /// Sends a message on a connected socket using `io_uring` zerocopy sendmsg.
    ///
    /// This method does not fall back to regular sendmsg if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`UdpSocket::set_zerocopy`] first.
    pub async fn send_msg_zc<B>(&self, buf: B, flags: i32) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_msg_zc(buf, flags).await
    }

    /// Receive a message from this socket using message-style flags.
    pub async fn recv_msg<B>(&self, buf: B, flags: i32) -> (io::Result<(usize, SocketAddr)>, B)
    where
        B: StableBufMut + 'static,
    {
        self.recv_from_with_flags(buf, flags).await
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
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    pub fn poll_readiness_multi(&self, events: u32) -> impl Stream<Item = io::Result<Event>> {
        self.inner.poll_readiness::<true>(events)
    }

    /// Receives datagrams from this socket using a multishot recvmsg operation backed by the
    /// provided buffer ring.
    ///
    /// Each yielded item includes payload bytes and the sender address.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_from_ring_multi(
        &self,
        ring: &RecvBufRing,
    ) -> impl Stream<Item = io::Result<(RecvMsgRingBuf, SocketAddr)>> {
        self.inner.recv_from_ring_multi(ring)
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
    pub fn recv_ring_multi(
        &self,
        ring: &RecvBufRing,
    ) -> impl Stream<Item = io::Result<BufRingBuf>> {
        DatagramRecvRingMulti::new(&self.inner, ring)
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

    /// Receives data from a connected socket using a single-shot recv bundle operation with flags.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    ///
    /// # Errors
    ///
    /// The request resolves with [`io::ErrorKind::InvalidInput`] when `flags`
    /// contains `MSG_TRUNC`, because its result does not identify the number of
    /// buffers selected by a bundle receive.
    pub fn recv_bundle_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> impl crate::Request<Output = io::Result<BufRingBufBundle>> {
        self.inner.recv_ring_bundle_with_flags(ring, flags)
    }

    /// Receives data from a connected socket using a multishot recv bundle operation.
    ///
    /// Empty datagrams are yielded as empty bundles and the underlying
    /// multishot operation is transparently rearmed.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    pub fn recv_bundle_multi(
        &self,
        ring: &RecvBufRing,
    ) -> impl Stream<Item = io::Result<BufRingBufBundle>> {
        DatagramRecvBundleMulti::new(&self.inner, ring, 0)
    }

    /// Receives data from a connected socket using a multishot recv bundle operation with flags.
    ///
    /// Empty datagrams are yielded as empty bundles and the underlying
    /// multishot operation is transparently rearmed.
    ///
    /// # Panics
    ///
    /// Panics when the buffer ring was registered with another driver.
    ///
    /// # Errors
    ///
    /// The stream yields [`io::ErrorKind::InvalidInput`] and terminates when
    /// `flags` contains `MSG_TRUNC` or `MSG_WAITALL`. `MSG_TRUNC` does not
    /// identify the number of selected buffers, and multishot receives do not
    /// support `MSG_WAITALL`.
    pub fn recv_bundle_multi_with_flags(
        &self,
        ring: &RecvBufRing,
        flags: i32,
    ) -> impl Stream<Item = io::Result<BufRingBufBundle>> {
        DatagramRecvBundleMulti::new(&self.inner, ring, flags)
    }

    /// Close the socket.
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if another owner or operation still
    /// retains the descriptor. Prepared operations are not submitted or cancelled
    /// by this call; queued, submitted, and completed-but-unconsumed operations
    /// continue retaining the descriptor until they are dropped or consumed. If
    /// close is rejected, dropping the last owner still closes the descriptor.
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
