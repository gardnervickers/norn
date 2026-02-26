use std::io;
use std::mem::ManuallyDrop;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_core::Stream;
use socket2::{Domain, Type};

use crate::buf::{StableBuf, StableBufMut};
use crate::bufring::BufRing;
use crate::net::socket;
use crate::operation::Op;

use super::socket::Accept;

const LOG: &str = "norn_uring::net::tcp";

/// A TCP listener.
///
/// A TcpListener can be used to accept incoming TCP connections.
pub struct TcpListener {
    socket: socket::Socket,
}

/// A TCP socket.
///
/// [`TcpSocket`] provides a low-level interface for configuring a socket
/// and sending or receiving owned buffers.
///
/// A [`TcpSocket`] can be converted into a [`TcpStream`] using [`into_stream`].
/// [`TcpStream`] is a stateful wrapper around [`TcpSocket`] that implements
/// [`AsyncRead`](tokio::io::AsyncRead) and [`AsyncWrite`](tokio::io::AsyncWrite).
pub struct TcpSocket {
    socket: socket::Socket,
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

impl TcpListener {
    /// Creates a TCP listener bound to the specified address.
    pub async fn bind(addr: SocketAddr, backlog: u32) -> io::Result<TcpListener> {
        let inner = socket::Socket::bind(addr, Domain::for_address(addr), Type::STREAM).await?;
        inner.listen(backlog)?;
        Ok(TcpListener { socket: inner })
    }

    /// Set value for the SO_REUSEADDR option on this socket.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.socket.as_socket()?.set_reuse_address(reuse)
    }

    /// Returns the local address that this listener is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Accepts a new incoming connection to this listener.
    pub async fn accept(&self) -> io::Result<(TcpSocket, SocketAddr)> {
        let (socket, addr) = self.socket.accept().await?;
        Ok((TcpSocket { socket }, addr))
    }

    /// Returns a stream of incoming connections.
    pub fn incoming(&self) -> Incoming<'_> {
        Incoming {
            listener: &self.socket,
            current: None,
        }
    }

    /// Closes the listener.
    pub async fn close(self) -> io::Result<()> {
        self.socket.close().await
    }
}

pin_project_lite::pin_project! {
    pub struct Incoming<'a> {
        listener: &'a socket::Socket,
        #[pin]
        current: Option<Op<Accept<true>>>,
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
                        let socket = socket::Socket::from_fd(socket);
                        return Poll::Ready(Some(Ok(TcpSocket { socket })));
                    }
                    None => {
                        this.current.set(None);
                        return Poll::Ready(None);
                    }
                }
            }
            this.current.set(Some(this.listener.accept_multi()));
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

impl TcpSocket {
    /// Creates a TCP connection to the specified address.
    pub async fn connect(addr: SocketAddr) -> io::Result<TcpSocket> {
        let domain = Domain::for_address(addr);
        let socket_type = Type::STREAM;
        let protocol = None;
        let socket = socket::Socket::open(domain, socket_type, protocol).await?;
        socket.connect(addr).await?;
        Ok(TcpSocket { socket })
    }

    /// Returns the local address that this stream is bound to.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    /// Returns the remote address that this stream is connected to.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.socket.peer_addr()
    }

    /// Shuts down the read, write, or both halves of this connection.
    pub async fn shutdown(&self, how: std::net::Shutdown) -> io::Result<()> {
        self.socket.shutdown(how).await
    }

    /// Recv data into the given buffer.
    ///
    /// This takes ownership of the buffer, and returns future which resolves
    /// to a tuple of the original buffer and the number of bytes read.
    pub fn recv<B: StableBufMut>(&self, buf: B) -> Op<socket::Recv<B>> {
        self.socket.recv(buf)
    }

    /// Recv data into the given buffer with recv flags.
    pub fn recv_with_flags<B: StableBufMut>(&self, buf: B, flags: i32) -> Op<socket::Recv<B>> {
        self.socket.recv_with_flags(buf, flags)
    }

    /// Send data from the given buffer.
    ///
    /// This takes ownership of the buffer, and returns a future which resolves
    /// to a tuple of the original buffer and the number of bytes sent.
    pub fn send<B: StableBuf>(&self, buf: B) -> Op<socket::Send<B>> {
        self.socket.send(buf)
    }

    /// Send data from the given buffer with send flags.
    pub fn send_with_flags<B: StableBuf>(&self, buf: B, flags: i32) -> Op<socket::Send<B>> {
        self.socket.send_with_flags(buf, flags)
    }

    /// Send a message from the given buffer with message-style flags.
    pub fn send_msg<B: StableBuf>(&self, buf: B, flags: i32) -> Op<socket::Send<B>> {
        self.send_with_flags(buf, flags)
    }

    /// Receive a message into the given buffer with message-style flags.
    pub fn recv_msg<B: StableBufMut>(&self, buf: B, flags: i32) -> Op<socket::Recv<B>> {
        self.recv_with_flags(buf, flags)
    }

    /// Recv data using the given buffer ring.
    ///
    /// A buffer will be taken from the ring when the operation is completed
    /// and returned. If there are no buffers available, the operation will
    /// fail.
    pub fn recv_ring(&self, ring: &BufRing) -> Op<socket::RecvFromRing> {
        self.socket.recv_from_ring(ring)
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
    pub async fn close(self) -> io::Result<()> {
        self.socket.close().await
    }

    /// Poll readiness on this socket.
    ///
    /// `events` uses `libc::POLL*` flags such as `POLLIN` and `POLLOUT`.
    /// When `MULTI` is `true`, the returned operation yields a stream of events.
    pub fn poll_readiness<const MULTI: bool>(&self, events: u32) -> Op<socket::Poll<MULTI>> {
        self.socket.poll_readiness(events)
    }

    /// Set value for the SO_RCVBUF option on this socket.
    pub fn set_recv_buffer_size(&self, size: usize) -> io::Result<()> {
        self.socket.as_socket()?.set_recv_buffer_size(size)
    }
    /// Set value for the SO_SNDBUF option on this socket.
    pub fn set_send_buffer_size(&self, size: usize) -> io::Result<()> {
        self.socket.as_socket()?.set_send_buffer_size(size)
    }

    /// Set value for the SO_REUSEADDR option on this socket.
    pub fn set_reuse_address(&self, reuse: bool) -> io::Result<()> {
        self.socket.as_socket()?.set_reuse_address(reuse)
    }

    /// Set value for the SO_KEEPALIVE option on this socket.
    pub fn set_keepalive(&self, keepalive: bool) -> io::Result<()> {
        self.socket.as_socket()?.set_keepalive(keepalive)
    }
    /// Set the value of the TCP_NODELAY option on this socket.
    pub fn set_nodelay(&self, nodelay: bool) -> io::Result<()> {
        self.socket.as_socket()?.set_nodelay(nodelay)
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
        let n = ready!(this
            .inner
            .poll_op(cx, |sock| sock.send(buf), socket::WRITE_FLAGS as u32))?;
        Poll::Ready(Ok(n))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        use std::io::Write;
        let this = self.project();
        ready!(this
            .inner
            .poll_op(cx, |mut sock| sock.flush(), socket::WRITE_FLAGS as u32))?;
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), io::Error>> {
        let this = self.project();
        ready!(this.inner.poll_op(
            cx,
            |sock| sock.shutdown(std::net::Shutdown::Write),
            socket::WRITE_FLAGS as u32
        ))?;
        Poll::Ready(Ok(()))
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<Result<usize, io::Error>> {
        let this = self.project();
        let n = ready!(this.inner.poll_op(
            cx,
            |sock| sock.send_vectored(bufs),
            socket::WRITE_FLAGS as u32
        ))?;
        Poll::Ready(Ok(n))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }
}

pin_project_lite::pin_project! {
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
            let this = self.as_mut().project();
            let sock = this.inner.as_socket()?;
            match f(sock) {
                Ok(res) => {
                    log::trace!(target: LOG, "poll_op.success");
                    return Poll::Ready(Ok(res));
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    log::trace!(target: LOG, "poll_op.would_block");
                    *this.notified = false;
                    ready!(self.as_mut().poll_ready(cx, flags))?;
                    continue;
                }
                Err(err) => {
                    log::trace!(target: LOG, "poll_op.err");
                    return Poll::Ready(Err(err));
                }
            }
        }
    }
}

impl ReadyStream {
    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>, flags: u32) -> Poll<io::Result<()>> {
        let mut this = self.project();
        loop {
            if *this.notified {
                // If the notified bit is set, return immediately.
                return Poll::Ready(Ok(()));
            }
            if let Some(armed) = this.armed.as_mut().as_pin_mut() {
                if let Some(res) = ready!(armed.poll_next(cx)) {
                    log::trace!(target: LOG, "poll_op.ready.stream_notified");
                    let _ = res?;
                    *this.notified = true;
                } else {
                    log::trace!(target: LOG, "poll_op.ready.stream_done");
                    this.armed.set(None);
                }
            } else {
                log::trace!(target: LOG, "poll_op.ready.stream_arm");
                this.armed.set(Some(this.inner.poll_readiness(flags)));
            }
        }
    }
}
