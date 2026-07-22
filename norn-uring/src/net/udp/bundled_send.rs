use std::cell::RefCell;
use std::fmt;
use std::io;
use std::rc::Rc;

use crate::bufring::send::SendRing;
use crate::bufring::send::SendToken;
use crate::bufring::{SendBuf, SendBufRing};
use crate::fd::Direction;
use crate::net::socket::{
    self, DatagramQueue, PumpControl, PumpError, PumpPhase, PumpTerminal, QueuedDatagram,
    QueuedSegment,
};

use super::UdpSocket;
use crate::net::SendError;

/// Maximum number of registered buffers in one bundled UDP datagram.
///
/// Linux caps one provided-buffer bundle selection at 256 entries. Enforcing
/// the same limit before publication prevents a larger logical datagram from
/// being emitted as a truncated 256-segment packet.
pub const SEND_BUNDLE_MAX_SEGMENTS: usize = 256;

/// Why a send ring could not be attached to a connected UDP socket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachUdpSendRingErrorKind {
    /// The socket and ring belong to different io_uring drivers.
    DifferentDriver,
    /// The running kernel does not advertise send-bundle support.
    Unsupported,
    /// An ordinary or bundled write operation is still alive on the socket.
    WriteBusy,
    /// The ring is attached, failed, or otherwise not clean.
    RingBusy,
    /// No local executor is active to own the send pump.
    NoExecutor,
    /// The active executor does not drive this socket's io_uring.
    WrongExecutor,
    /// UDP bundle sends require a connected socket.
    NotConnected,
}

impl fmt::Display for AttachUdpSendRingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DifferentDriver => "socket and send ring use different drivers",
            Self::Unsupported => "the kernel does not support io_uring send bundles",
            Self::WriteBusy => "the socket write side has another operation in progress",
            Self::RingBusy => "the send ring is attached or not clean",
            Self::NoExecutor => "send-ring attachment requires an active local executor",
            Self::WrongExecutor => "the active executor does not drive this socket's io_uring",
            Self::NotConnected => "UDP send bundles require a connected socket",
        };
        f.write_str(message)
    }
}

/// An attachment failure that returns the unchanged socket and send ring.
pub struct AttachUdpSendRingError {
    kind: AttachUdpSendRingErrorKind,
    socket: UdpSocket,
    ring: SendBufRing,
}

impl AttachUdpSendRingError {
    fn new(kind: AttachUdpSendRingErrorKind, socket: UdpSocket, ring: SendBufRing) -> Self {
        Self { kind, socket, ring }
    }

    /// Returns the reason attachment failed.
    pub fn kind(&self) -> AttachUdpSendRingErrorKind {
        self.kind
    }

    /// Recovers the unchanged socket and ring.
    pub fn into_parts(self) -> (UdpSocket, SendBufRing) {
        (self.socket, self.ring)
    }
}

impl fmt::Debug for AttachUdpSendRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttachUdpSendRingError")
            .field("kind", &self.kind)
            .field("ring", &self.ring)
            .finish()
    }
}

impl fmt::Display for AttachUdpSendRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for AttachUdpSendRingError {}

/// A failed datagram-segment push that returns the rejected buffer.
pub struct DatagramPushError {
    error: SendError,
    buffer: SendBuf,
}

impl DatagramPushError {
    /// Returns the validation failure.
    pub fn error(&self) -> &SendError {
        &self.error
    }

    /// Recovers the failure and rejected buffer.
    pub fn into_parts(self) -> (SendError, SendBuf) {
        (self.error, self.buffer)
    }
}

impl fmt::Debug for DatagramPushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatagramPushError")
            .field("error", &self.error)
            .field("buffer", &self.buffer)
            .finish()
    }
}

impl fmt::Display for DatagramPushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for DatagramPushError {}

/// A failed datagram commit that retains the complete private builder.
pub struct DatagramCommitError<'a> {
    error: SendError,
    builder: UdpDatagramBuilder<'a>,
}

impl<'a> DatagramCommitError<'a> {
    /// Returns the commit failure.
    pub fn error(&self) -> &SendError {
        &self.error
    }

    /// Recovers the private datagram builder.
    pub fn into_builder(self) -> UdpDatagramBuilder<'a> {
        self.builder
    }

    /// Recovers both the failure and private datagram builder.
    pub fn into_parts(self) -> (SendError, UdpDatagramBuilder<'a>) {
        (self.error, self.builder)
    }

    /// Discards the private builder and returns an owned error.
    pub fn into_error(self) -> SendError {
        self.error
    }
}

impl fmt::Debug for DatagramCommitError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DatagramCommitError")
            .field("error", &self.error)
            .field("segments", &self.builder.segments.len())
            .finish()
    }
}

impl fmt::Display for DatagramCommitError<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.error.fmt(f)
    }
}

impl std::error::Error for DatagramCommitError<'_> {}

/// Result of explicitly draining and detaching a connected UDP send ring.
pub enum FinishUdpSendRingOutcome {
    /// Every committed datagram was sent and the ring is clean.
    Drained {
        /// The ordinary connected UDP socket.
        socket: UdpSocket,
        /// The same clean send ring supplied at attachment.
        ring: SendBufRing,
    },
    /// A datagram send failed, but the ring was sanitized and is reusable.
    SendFailed {
        /// The send failure.
        error: SendError,
        /// The sanitized ring.
        ring: SendBufRing,
    },
    /// The ring could not be safely sanitized and was destroyed.
    CleanupFailed {
        /// The cleanup failure.
        error: SendError,
    },
}

impl fmt::Debug for FinishUdpSendRingOutcome {
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

/// A connected UDP socket with an attached outbound bundle ring.
pub struct BundledUdpSocket {
    socket: socket::Socket,
    ring: Rc<SendRing>,
    attachment: u64,
    control: Rc<PumpControl>,
    queue: Rc<RefCell<DatagramQueue>>,
    abandon_on_drop: bool,
}

impl fmt::Debug for BundledUdpSocket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BundledUdpSocket")
            .field("attachment", &self.attachment)
            .field("phase", &self.control.phase())
            .finish()
    }
}

impl UdpSocket {
    /// Attach one application-owned send ring to this connected UDP socket.
    pub fn attach_send_ring(
        self,
        ring: SendBufRing,
    ) -> Result<BundledUdpSocket, AttachUdpSendRingError> {
        if !ring.same_driver(self.inner.handle()) {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::DifferentDriver,
                self,
                ring,
            ));
        }
        if !self.inner.supports_recvsend_bundle() {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::Unsupported,
                self,
                ring,
            ));
        }
        if self.inner.peer_addr().is_err() {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::NotConnected,
                self,
                ring,
            ));
        }
        let Some(executor) = norn_executor::Handle::try_current() else {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::NoExecutor,
                self,
                ring,
            ));
        };
        let Some(current_driver) = crate::Handle::try_current() else {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::WrongExecutor,
                self,
                ring,
            ));
        };
        if !current_driver.same_driver(self.inner.handle()) {
            return Err(AttachUdpSendRingError::new(
                AttachUdpSendRingErrorKind::WrongExecutor,
                self,
                ring,
            ));
        }
        let permit = match self.inner.acquire_bundled(Direction::Write) {
            Ok(permit) => permit,
            Err(_) => {
                return Err(AttachUdpSendRingError::new(
                    AttachUdpSendRingErrorKind::WriteBusy,
                    self,
                    ring,
                ))
            }
        };
        let attachment = match ring.inner.begin_attachment() {
            Ok(attachment) => attachment,
            Err(_) => {
                drop(permit);
                return Err(AttachUdpSendRingError::new(
                    AttachUdpSendRingErrorKind::RingBusy,
                    self,
                    ring,
                ));
            }
        };

        let socket = self.inner;
        let control = Rc::new(PumpControl::new());
        let queue = Rc::new(RefCell::new(DatagramQueue::default()));
        socket::spawn_datagram_pump(
            &executor,
            socket.clone(),
            Rc::clone(&ring.inner),
            attachment,
            Rc::clone(&control),
            permit,
            Rc::clone(&queue),
        );
        Ok(BundledUdpSocket {
            socket,
            ring: ring.inner,
            attachment,
            control,
            queue,
            abandon_on_drop: true,
        })
    }
}

impl BundledUdpSocket {
    /// Begin staging one private datagram.
    ///
    /// The builder exclusively borrows this sender, so one datagram is staged
    /// at a time and it cannot race [`flush`](Self::flush) or
    /// [`finish_send_ring`](Self::finish_send_ring).
    pub fn datagram(&mut self) -> UdpDatagramBuilder<'_> {
        let segment_limit =
            usize::from(self.ring.registered_buf_count()).min(SEND_BUNDLE_MAX_SEGMENTS);
        UdpDatagramBuilder {
            sender: self,
            segments: Vec::new(),
            reservations: Vec::new(),
            segment_limit,
        }
    }

    /// Wait until every datagram committed before this call has completed.
    pub async fn flush(&mut self) -> io::Result<()> {
        let target = self.queue.borrow().committed();
        loop {
            if let Some(error) = self.ring.failure() {
                return Err(error);
            }
            if self.queue.borrow().completed() >= target {
                return Ok(());
            }
            let observed = self.control.generation();
            self.control.changed_since(observed).await;
        }
    }

    /// Drain committed datagrams and detach the ring.
    pub async fn finish_send_ring(mut self) -> FinishUdpSendRingOutcome {
        self.control.request_finish();
        let terminal = self.control.wait_terminal().await;
        self.abandon_on_drop = false;
        match terminal {
            PumpTerminal::Drained => FinishUdpSendRingOutcome::Drained {
                socket: UdpSocket {
                    inner: self.socket.clone(),
                },
                ring: SendBufRing {
                    inner: Rc::clone(&self.ring),
                },
            },
            PumpTerminal::Failed(error) => FinishUdpSendRingOutcome::SendFailed {
                error: send_error(error),
                ring: SendBufRing {
                    inner: Rc::clone(&self.ring),
                },
            },
            PumpTerminal::CleanupFailed(error) => FinishUdpSendRingOutcome::CleanupFailed {
                error: send_error(error),
            },
        }
    }
}

impl Drop for BundledUdpSocket {
    fn drop(&mut self) {
        if self.abandon_on_drop {
            self.control.abandon();
        }
    }
}

/// A private, not-yet-visible connected UDP datagram.
pub struct UdpDatagramBuilder<'a> {
    sender: &'a mut BundledUdpSocket,
    segments: Vec<QueuedSegment>,
    reservations: Vec<Reservation>,
    segment_limit: usize,
}

struct Reservation {
    token: SendToken,
    pushed: bool,
}

impl fmt::Debug for UdpDatagramBuilder<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UdpDatagramBuilder")
            .field("segments", &self.segments.len())
            .finish()
    }
}

impl<'a> UdpDatagramBuilder<'a> {
    /// Try to reserve another segment buffer without waiting.
    pub fn try_acquire(&mut self) -> io::Result<Option<SendBuf>> {
        self.check_capacity(false, None)?;
        self.ensure_open()?;
        let buffer = self.sender.ring.try_acquire(self.sender.attachment)?;
        if let Some(buffer) = buffer.as_ref() {
            self.reservations.push(Reservation {
                token: buffer.token(),
                pushed: false,
            });
        }
        Ok(buffer)
    }

    /// Wait to reserve another segment buffer.
    pub async fn acquire(&mut self) -> io::Result<SendBuf> {
        self.check_capacity(false, None)?;
        loop {
            self.ensure_open()?;
            let observed = self.sender.ring.generation();
            if let Some(buffer) = self.sender.ring.try_acquire(self.sender.attachment)? {
                self.reservations.push(Reservation {
                    token: buffer.token(),
                    pushed: false,
                });
                return Ok(buffer);
            }
            self.sender.ring.changed_since(observed).await;
        }
    }

    /// Add an initialized buffer prefix to this private datagram.
    pub fn push(&mut self, buffer: SendBuf, initialized: usize) -> Result<(), DatagramPushError> {
        let token = buffer.token();
        if let Err(error) = self.check_capacity(true, Some(token)) {
            return Err(DatagramPushError {
                error: SendError::from(error),
                buffer,
            });
        }
        if let Err(error) =
            self.sender
                .ring
                .validate_staged_buffer(self.sender.attachment, &buffer, initialized)
        {
            return Err(DatagramPushError {
                error: SendError::from(error),
                buffer,
            });
        }
        if let Some(reservation) = self
            .reservations
            .iter_mut()
            .find(|reservation| reservation.token == token)
        {
            reservation.pushed = true;
        }
        self.segments.push(QueuedSegment {
            buffer,
            len: initialized,
        });
        Ok(())
    }

    /// Commit exactly one datagram to the private FIFO.
    ///
    /// Empty datagrams reserve one ring buffer as their queue-capacity token
    /// until the ordinary zero-length send reaches its terminal completion.
    pub async fn commit(mut self) -> Result<(), DatagramCommitError<'a>> {
        if let Err(error) = self.ensure_open() {
            return Err(DatagramCommitError {
                error: SendError::from(error),
                builder: self,
            });
        }
        if self.segments.is_empty() && self.used_capacity() != 0 {
            return Err(DatagramCommitError {
                error: SendError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "empty datagram still has an acquired segment buffer",
                )),
                builder: self,
            });
        }
        let empty_token = if self.segments.is_empty() {
            loop {
                if let Err(error) = self.ensure_open() {
                    return Err(DatagramCommitError {
                        error: SendError::from(error),
                        builder: self,
                    });
                }
                let observed = self.sender.ring.generation();
                match self.sender.ring.try_acquire(self.sender.attachment) {
                    Ok(Some(buffer)) => break Some(buffer),
                    Ok(None) => self.sender.ring.changed_since(observed).await,
                    Err(error) => {
                        return Err(DatagramCommitError {
                            error: SendError::from(error),
                            builder: self,
                        })
                    }
                }
            }
        } else {
            None
        };
        let segments = std::mem::take(&mut self.segments);
        self.sender.queue.borrow_mut().push(QueuedDatagram {
            segments,
            empty_token,
        });
        self.sender.control.notify_work();
        Ok(())
    }

    fn segment_limit(&self) -> usize {
        self.segment_limit
    }

    fn used_capacity(&self) -> usize {
        self.segments.len()
            + self
                .reservations
                .iter()
                .filter(|reservation| !reservation.pushed)
                .count()
    }

    fn check_capacity(&self, pushing: bool, token: Option<SendToken>) -> io::Result<()> {
        let moves_existing_reservation = pushing
            && token.is_some_and(|token| {
                self.reservations
                    .iter()
                    .any(|reservation| reservation.token == token && !reservation.pushed)
            });
        if self.used_capacity() >= self.segment_limit() && !moves_existing_reservation {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "datagram reached its segment limit",
            ));
        }
        Ok(())
    }

    fn ensure_open(&self) -> io::Result<()> {
        if self.sender.control.phase() != PumpPhase::Open {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "bundled UDP socket is closing",
            ));
        }
        if let Some(error) = self.sender.ring.failure() {
            return Err(error);
        }
        Ok(())
    }
}

fn send_error(error: PumpError) -> SendError {
    SendError::from_parts(
        error.kind(),
        error.raw_os_error(),
        error.message().to_owned(),
    )
}
