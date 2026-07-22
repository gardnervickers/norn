use std::fmt;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use crate::buf::StableBufMut;
use crate::bufring::send::SendRing;
use crate::bufring::{SendBuf, SendBufRing};
use crate::fd::Direction;
use crate::net::socket::{self, PumpControl, PumpError, PumpPhase, PumpTerminal};

use super::{ReadyStream, TcpStreamWriter};

type RingWait = Pin<Box<dyn Future<Output = ()>>>;

/// Why a send ring could not be attached to a TCP writer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachSendRingErrorKind {
    /// The socket and ring belong to different io_uring drivers.
    DifferentDriver,
    /// The running kernel does not advertise receive/send bundle support.
    Unsupported,
    /// An ordinary or bundled write operation is still alive on the socket.
    WriteBusy,
    /// The ring is attached, failed, or otherwise not clean.
    RingBusy,
    /// No local executor is active to own the send pump.
    NoExecutor,
    /// The active executor does not drive this socket's io_uring.
    WrongExecutor,
}

impl fmt::Display for AttachSendRingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DifferentDriver => "socket and send ring use different drivers",
            Self::Unsupported => "the kernel does not support io_uring send bundles",
            Self::WriteBusy => "the socket write side has another operation in progress",
            Self::RingBusy => "the send ring is attached or not clean",
            Self::NoExecutor => "send-ring attachment requires an active local executor",
            Self::WrongExecutor => "the active executor does not drive this socket's io_uring",
        };
        f.write_str(message)
    }
}

/// An attachment failure that returns the unchanged writer and send ring.
pub struct AttachSendRingError {
    kind: AttachSendRingErrorKind,
    writer: TcpStreamWriter,
    ring: SendBufRing,
}

impl AttachSendRingError {
    fn new(kind: AttachSendRingErrorKind, writer: TcpStreamWriter, ring: SendBufRing) -> Self {
        Self { kind, writer, ring }
    }

    /// Returns the reason attachment failed.
    pub fn kind(&self) -> AttachSendRingErrorKind {
        self.kind
    }

    /// Recovers the unchanged writer and send ring.
    pub fn into_parts(self) -> (TcpStreamWriter, SendBufRing) {
        (self.writer, self.ring)
    }
}

impl fmt::Debug for AttachSendRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttachSendRingError")
            .field("kind", &self.kind)
            .field("ring", &self.ring)
            .finish()
    }
}

impl fmt::Display for AttachSendRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for AttachSendRingError {}

/// A stable, clonable description of a send or cleanup failure.
#[derive(Debug, Clone)]
pub struct SendError {
    kind: io::ErrorKind,
    raw_os_error: Option<i32>,
    message: Rc<str>,
}

impl SendError {
    pub(crate) fn from_parts(
        kind: io::ErrorKind,
        raw_os_error: Option<i32>,
        message: String,
    ) -> Self {
        Self {
            kind,
            raw_os_error,
            message: Rc::from(message),
        }
    }

    fn from_io(error: &io::Error) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: Rc::from(error.to_string()),
        }
    }

    fn from_pump(error: PumpError) -> Self {
        Self {
            kind: error.kind(),
            raw_os_error: error.raw_os_error(),
            message: Rc::from(error.message()),
        }
    }

    /// Returns the corresponding standard I/O error.
    pub fn to_io_error(&self) -> io::Error {
        match self.raw_os_error {
            Some(code) => io::Error::from_raw_os_error(code),
            None => io::Error::new(self.kind, self.message.to_string()),
        }
    }
}

impl From<io::Error> for SendError {
    fn from(error: io::Error) -> Self {
        Self::from_io(&error)
    }
}

impl fmt::Display for SendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SendError {}

/// A failed enqueue that returns ownership of the rejected buffer.
pub struct EnqueueError {
    error: SendError,
    buffer: SendBuf,
}

impl EnqueueError {
    /// Returns the enqueue failure.
    pub fn error(&self) -> &SendError {
        &self.error
    }

    /// Recovers the rejected buffer.
    pub fn into_buffer(self) -> SendBuf {
        self.buffer
    }

    /// Recovers both the failure and rejected buffer.
    pub fn into_parts(self) -> (SendError, SendBuf) {
        (self.error, self.buffer)
    }
}

impl fmt::Debug for EnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnqueueError")
            .field("error", &self.error)
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl fmt::Display for EnqueueError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for EnqueueError {}

/// Result of explicitly draining and detaching a TCP send ring.
pub enum FinishSendRingOutcome {
    /// All accepted bytes reached the local socket and the ring is clean.
    Drained {
        /// The ordinary TCP writer.
        writer: TcpStreamWriter,
        /// The same clean send ring supplied at attachment.
        ring: SendBufRing,
    },
    /// Sending failed, but the ring was sanitized and is safe to reuse.
    SendFailed {
        /// The send failure.
        error: SendError,
        /// The sanitized send ring.
        ring: SendBufRing,
    },
    /// The ring could not be safely sanitized and was destroyed.
    CleanupFailed {
        /// The cleanup failure.
        error: SendError,
    },
}

impl fmt::Debug for FinishSendRingOutcome {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Drained { ring, .. } => f.debug_tuple("Drained").field(ring).finish(),
            Self::SendFailed { error, ring } => f
                .debug_struct("SendFailed")
                .field("error", error)
                .field("ring", ring)
                .finish(),
            Self::CleanupFailed { error } => f.debug_tuple("CleanupFailed").field(error).finish(),
        }
    }
}

/// A bounded, bundle-backed TCP byte writer.
///
/// The attached [`SendBufRing`] is the entire queue budget. The application
/// remains responsible for deciding how many rings to create and how to reuse
/// rings returned by [`finish_send_ring`](Self::finish_send_ring).
pub struct BundledTcpWriter {
    socket: socket::Socket,
    ring: Rc<SendRing>,
    attachment: u64,
    control: Rc<PumpControl>,
    wait: Option<RingWait>,
    flush_target: Option<u64>,
    shutdown: bool,
    abandon_on_drop: bool,
}

impl fmt::Debug for BundledTcpWriter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundledTcpWriter")
            .field("attachment", &self.attachment)
            .field("accepted", &self.ring.accepted())
            .field("completed", &self.ring.completed())
            .field("phase", &self.control.phase())
            .finish()
    }
}

impl TcpStreamWriter {
    /// Consume this writer and attach one application-owned outbound ring.
    ///
    /// On failure, [`AttachSendRingError::into_parts`] returns both inputs.
    pub fn attach_send_ring(
        self,
        ring: SendBufRing,
    ) -> Result<BundledTcpWriter, AttachSendRingError> {
        if !ring.same_driver(self.inner.inner.handle()) {
            return Err(AttachSendRingError::new(
                AttachSendRingErrorKind::DifferentDriver,
                self,
                ring,
            ));
        }
        if !self.inner.inner.supports_recvsend_bundle() {
            return Err(AttachSendRingError::new(
                AttachSendRingErrorKind::Unsupported,
                self,
                ring,
            ));
        }
        let Some(executor) = norn_executor::Handle::try_current() else {
            return Err(AttachSendRingError::new(
                AttachSendRingErrorKind::NoExecutor,
                self,
                ring,
            ));
        };
        let Some(current_driver) = crate::Handle::try_current() else {
            return Err(AttachSendRingError::new(
                AttachSendRingErrorKind::WrongExecutor,
                self,
                ring,
            ));
        };
        if !current_driver.same_driver(self.inner.inner.handle()) {
            return Err(AttachSendRingError::new(
                AttachSendRingErrorKind::WrongExecutor,
                self,
                ring,
            ));
        }
        let permit = match self.inner.inner.acquire_bundled(Direction::Write) {
            Ok(permit) => permit,
            Err(_) => {
                return Err(AttachSendRingError::new(
                    AttachSendRingErrorKind::WriteBusy,
                    self,
                    ring,
                ))
            }
        };
        let attachment = match ring.inner.begin_attachment() {
            Ok(attachment) => attachment,
            Err(_) => {
                drop(permit);
                return Err(AttachSendRingError::new(
                    AttachSendRingErrorKind::RingBusy,
                    self,
                    ring,
                ));
            }
        };

        let socket = self.inner.into_socket();
        let control = Rc::new(PumpControl::new());
        socket::spawn_stream_pump(
            &executor,
            socket.clone(),
            Rc::clone(&ring.inner),
            attachment,
            Rc::clone(&control),
            permit,
        );

        Ok(BundledTcpWriter {
            socket,
            ring: ring.inner,
            attachment,
            control,
            wait: None,
            flush_target: None,
            shutdown: false,
            abandon_on_drop: true,
        })
    }
}

impl BundledTcpWriter {
    /// Try to reserve one free registered send buffer without waiting.
    pub fn try_acquire(&mut self) -> io::Result<Option<SendBuf>> {
        self.ensure_open()?;
        self.ring.try_acquire(self.attachment)
    }

    /// Wait until one registered send buffer can be reserved.
    pub async fn acquire(&mut self) -> io::Result<SendBuf> {
        loop {
            self.ensure_open()?;
            let observed = self.ring.generation();
            if let Some(buffer) = self.ring.try_acquire(self.attachment)? {
                return Ok(buffer);
            }
            self.ring.changed_since(observed).await;
        }
    }

    /// Commit the initialized prefix of an owned buffer to the socket FIFO.
    pub fn enqueue(&mut self, buffer: SendBuf, initialized: usize) -> Result<(), EnqueueError> {
        if let Err(error) = self.ensure_open() {
            return Err(EnqueueError {
                error: SendError::from_io(&error),
                buffer,
            });
        }
        self.ring
            .enqueue_buffer(self.attachment, buffer, initialized)
            .map(|_| ())
            .map_err(|(error, buffer)| EnqueueError {
                error: SendError::from_io(&error),
                buffer,
            })
    }

    /// Drain accepted data and detach the ring from this socket.
    ///
    /// Dropping the returned future abandons the writer. The runtime still
    /// fences and destroys the ring, but it cannot be returned to the caller.
    pub async fn finish_send_ring(mut self) -> FinishSendRingOutcome {
        self.wait = None;
        self.control.request_finish();
        let terminal = self.control.wait_terminal().await;
        self.abandon_on_drop = false;
        match terminal {
            PumpTerminal::Drained => FinishSendRingOutcome::Drained {
                writer: TcpStreamWriter {
                    inner: ReadyStream::new(self.socket.clone()),
                },
                ring: SendBufRing {
                    inner: Rc::clone(&self.ring),
                },
            },
            PumpTerminal::Failed(error) => FinishSendRingOutcome::SendFailed {
                error: SendError::from_pump(error),
                ring: SendBufRing {
                    inner: Rc::clone(&self.ring),
                },
            },
            PumpTerminal::CleanupFailed(error) => FinishSendRingOutcome::CleanupFailed {
                error: SendError::from_pump(error),
            },
        }
    }

    fn ensure_open(&self) -> io::Result<()> {
        if self.shutdown {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "bundled TCP writer is shut down",
            ));
        }
        if self.control.phase() != PumpPhase::Open {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "bundled TCP writer is closing",
            ));
        }
        if let Some(error) = self.ring.failure() {
            return Err(error);
        }
        Ok(())
    }

    fn poll_ring_change(&mut self, cx: &mut Context<'_>, observed: u64) -> Poll<()> {
        if self.wait.is_none() {
            let ring = Rc::clone(&self.ring);
            self.wait = Some(Box::pin(async move {
                ring.changed_since(observed).await;
            }));
        }
        let wait = self.wait.as_mut().expect("ring wait missing");
        match wait.as_mut().poll(cx) {
            Poll::Ready(()) => {
                self.wait = None;
                Poll::Ready(())
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl tokio::io::AsyncWrite for BundledTcpWriter {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if bytes.is_empty() {
            return Poll::Ready(Ok(0));
        }
        loop {
            this.ensure_open()?;
            let observed = this.ring.generation();
            let Some(mut buffer) = this.ring.try_acquire(this.attachment)? else {
                match this.poll_ring_change(cx, observed) {
                    Poll::Ready(()) => continue,
                    Poll::Pending => return Poll::Pending,
                }
            };
            let len = bytes.len().min(buffer.capacity());
            buffer.spare_capacity_mut()[..len]
                .iter_mut()
                .zip(&bytes[..len])
                .for_each(|(dst, src)| {
                    dst.write(*src);
                });
            unsafe { buffer.set_init(len) };
            match this.ring.enqueue_buffer(this.attachment, buffer, len) {
                Ok(_) => return Poll::Ready(Ok(len)),
                Err((error, _buffer)) => return Poll::Ready(Err(error)),
            }
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if let Some(error) = this.ring.failure() {
                this.flush_target = None;
                return Poll::Ready(Err(error));
            }
            let target = *this
                .flush_target
                .get_or_insert_with(|| this.ring.accepted());
            if this.ring.completed() >= target {
                this.flush_target = None;
                return Poll::Ready(Ok(()));
            }
            let observed = this.ring.generation();
            match this.poll_ring_change(cx, observed) {
                Poll::Ready(()) => continue,
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if !self.shutdown {
            match self.as_mut().poll_flush(cx) {
                Poll::Ready(Ok(())) => {}
                result => return result,
            }
            self.socket
                .as_socket()?
                .shutdown(std::net::Shutdown::Write)?;
            self.shutdown = true;
        }
        Poll::Ready(Ok(()))
    }
}

impl Drop for BundledTcpWriter {
    fn drop(&mut self) {
        if self.abandon_on_drop {
            self.control.abandon();
        }
    }
}
