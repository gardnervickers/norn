//! UDP Protocol Socket
use std::io;
use std::net::SocketAddr;

use socket2::{Domain, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::{BufRing, BufRingBuf};
use crate::net::socket;
use crate::operation::Op;

/// A UDP socket.
///
/// After creating a `UdpSocket` by [`bind`]ing it to a socket address, data can be
/// [sent to] and [received from] any other socket address.
pub struct UdpSocket {
    inner: socket::Socket,
}

impl std::fmt::Debug for UdpSocket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UdpSocket").finish()
    }
}

impl UdpSocket {
    /// Creates a UDP socket from the given address.
    pub async fn bind(addr: SocketAddr) -> io::Result<UdpSocket> {
        let inner = socket::Socket::bind(addr, Domain::for_address(addr), Type::DGRAM).await?;
        Ok(UdpSocket { inner })
    }

    /// Connect this socket to a remote address.
    ///
    /// A connected UDP socket can use [`send`] and [`recv`] without passing
    /// an address for each operation.
    pub async fn connect(&self, addr: SocketAddr) -> io::Result<()> {
        self.inner.connect(addr).await
    }

    /// Returns the socket address that this socket was created from.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.inner.local_addr()
    }

    /// Returns the socket address of the remote peer.
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

    /// Sends a single datagram on a connected socket using io_uring zerocopy send.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`set_zerocopy`] first.
    pub async fn send_zc<B>(&self, buf: B) -> (io::Result<usize>, B)
    where
        B: StableBuf + 'static,
    {
        self.inner.send_zc(buf).await
    }

    /// Sends a single datagram on a connected socket using io_uring zerocopy send with flags.
    ///
    /// This method does not fall back to regular send if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`set_zerocopy`] first.
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

    /// Sends a message on a connected socket using io_uring zerocopy sendmsg.
    ///
    /// This method does not fall back to regular sendmsg if zerocopy is unsupported.
    /// Callers should enable `SO_ZEROCOPY` with [`set_zerocopy`] first.
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
    pub async fn recv_from_ring(&self, ring: &BufRing) -> io::Result<(BufRingBuf, SocketAddr)> {
        self.inner.recv_from_ring(ring).await
    }

    /// Poll readiness on this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    /// When `MULTI` is `true`, the returned operation yields a stream of events.
    pub fn poll_readiness<const MULTI: bool>(&self, events: u32) -> Op<socket::Poll<MULTI>> {
        self.inner.poll_readiness(events)
    }

    /// Receives datagrams from this socket using a multishot recvmsg operation backed by the
    /// provided buffer ring.
    ///
    /// Each yielded item includes payload bytes and the sender address.
    pub fn recv_from_ring_multi(&self, ring: &BufRing) -> Op<socket::RecvFromRingMulti> {
        self.inner.recv_from_ring_multi(ring)
    }

    /// Receives datagrams from a connected socket using a multishot recv operation backed by the
    /// provided buffer ring.
    pub fn recv_ring_multi(&self, ring: &BufRing) -> Op<socket::RecvRingMulti> {
        self.inner.recv_ring_multi(ring)
    }

    /// Close the socket.
    ///
    /// This will wait for all pending operations to complete before closing the socket.
    pub async fn close(self) -> io::Result<()> {
        self.inner.close().await
    }

    /// Enable or disable `SO_ZEROCOPY` on this socket.
    pub async fn set_zerocopy(&self, enabled: bool) -> io::Result<()> {
        self.inner.set_zerocopy(enabled).await
    }
}
