use std::fmt;
use std::future::{poll_fn, Future};
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;

use crate::bufring::{RecvBufBundle, RecvBufRing};
use crate::fd::{BundledPermit, Direction};
use crate::net::socket;
use crate::operation::Op;

use super::{ReadyStream, TcpStreamReader};

type AvailabilityWait = Pin<Box<dyn Future<Output = io::Result<()>>>>;

enum TerminalReceive {
    Eof,
    Error(io::Error),
}

/// Why a receive ring could not be attached to a TCP reader.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttachRecvRingErrorKind {
    /// The socket and ring belong to different io_uring drivers.
    DifferentDriver,
    /// The running kernel does not advertise receive/send bundle support.
    Unsupported,
    /// An ordinary read operation is still alive on this socket.
    ReadBusy,
}

impl fmt::Display for AttachRecvRingErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::DifferentDriver => "socket and receive ring use different drivers",
            Self::Unsupported => "the kernel does not support io_uring receive bundles",
            Self::ReadBusy => "the socket read side has an ordinary operation in progress",
        };
        f.write_str(message)
    }
}

/// An attachment failure that returns the unchanged reader and receive ring.
pub struct AttachRecvRingError {
    kind: AttachRecvRingErrorKind,
    reader: TcpStreamReader,
    ring: RecvBufRing,
}

impl AttachRecvRingError {
    fn new(kind: AttachRecvRingErrorKind, reader: TcpStreamReader, ring: RecvBufRing) -> Self {
        Self { kind, reader, ring }
    }

    /// Returns the reason attachment failed.
    pub fn kind(&self) -> AttachRecvRingErrorKind {
        self.kind
    }

    /// Recovers the unchanged reader and receive ring.
    pub fn into_parts(self) -> (TcpStreamReader, RecvBufRing) {
        (self.reader, self.ring)
    }
}

impl fmt::Debug for AttachRecvRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AttachRecvRingError")
            .field("kind", &self.kind)
            .field("ring", &self.ring)
            .finish()
    }
}

impl fmt::Display for AttachRecvRingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.kind.fmt(f)
    }
}

impl std::error::Error for AttachRecvRingError {}

/// A TCP byte stream delivered as receive-buffer bundles.
///
/// Bundle boundaries are an implementation detail and are not TCP message
/// boundaries. Temporary receive-ring exhaustion is handled by waiting for a
/// returned buffer and transparently rearming the multishot receive.
pub struct TcpRecvBundles {
    socket: Option<socket::Socket>,
    ring: RecvBufRing,
    permit: Option<BundledPermit>,
    current: Option<Pin<Box<Op<socket::RecvRingBundleMulti>>>>,
    wait: Option<AvailabilityWait>,
    fault_wait: Option<AvailabilityWait>,
    terminal: Option<TerminalReceive>,
    stopping: bool,
    armed_generation: u64,
    ended: bool,
}

impl fmt::Debug for TcpRecvBundles {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpRecvBundles")
            .field("ring", &self.ring)
            .field("receiving", &self.current.is_some())
            .field("waiting_for_buffer", &self.wait.is_some())
            .field("stopping", &self.stopping)
            .field("ended", &self.ended)
            .finish()
    }
}

impl TcpStreamReader {
    /// Consume this reader and receive TCP bytes as bundles from `ring`.
    ///
    /// On failure, [`AttachRecvRingError::into_parts`] returns both inputs.
    pub fn recv_bundles(self, ring: RecvBufRing) -> Result<TcpRecvBundles, AttachRecvRingError> {
        if !ring.same_driver(self.inner.inner.handle()) {
            return Err(AttachRecvRingError::new(
                AttachRecvRingErrorKind::DifferentDriver,
                self,
                ring,
            ));
        }
        if !self.inner.inner.supports_recvsend_bundle() {
            return Err(AttachRecvRingError::new(
                AttachRecvRingErrorKind::Unsupported,
                self,
                ring,
            ));
        }
        let permit = match self.inner.inner.acquire_bundled(Direction::Read) {
            Ok(permit) => permit,
            Err(_) => {
                return Err(AttachRecvRingError::new(
                    AttachRecvRingErrorKind::ReadBusy,
                    self,
                    ring,
                ))
            }
        };
        let socket = self.inner.into_socket();
        let armed_generation = ring.availability_generation();
        Ok(TcpRecvBundles {
            socket: Some(socket),
            ring,
            permit: Some(permit),
            current: None,
            wait: None,
            fault_wait: None,
            terminal: None,
            stopping: false,
            armed_generation,
            ended: false,
        })
    }
}

impl TcpRecvBundles {
    fn socket(&self) -> &socket::Socket {
        self.socket.as_ref().expect("receive adapter lost socket")
    }

    fn permit(&self) -> &BundledPermit {
        self.permit
            .as_ref()
            .expect("receive adapter lost bundled permit")
    }

    fn arm(&mut self) -> io::Result<()> {
        self.ring.check_health()?;
        self.armed_generation = self.ring.availability_generation();
        self.current = Some(Box::pin(
            self.socket()
                .recv_ring_bundle_multi_bundled(&self.ring, self.permit().clone()),
        ));
        Ok(())
    }

    fn wait_for_buffer(&mut self) {
        let ring = self.ring.clone();
        let observed = self.armed_generation;
        self.wait = Some(Box::pin(async move {
            ring.wait_for_availability_since(observed).await
        }));
    }

    /// Stop receiving, wait for the active multishot request's terminal CQE,
    /// and recover the ordinary TCP reader.
    pub async fn finish(mut self) -> io::Result<TcpStreamReader> {
        self.wait = None;
        if let Some(mut current) = self.current.take() {
            current.as_mut().request_stop();
            while poll_fn(|cx| current.as_mut().poll_next(cx)).await.is_some() {}
        }

        self.permit.take();
        let socket = self.socket.take().expect("receive adapter lost socket");
        Ok(TcpStreamReader {
            inner: ReadyStream::new(socket),
        })
    }
}

impl Stream for TcpRecvBundles {
    type Item = io::Result<RecvBufBundle>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if self.terminal.is_some() {
                if !self.stopping && self.current.is_some() {
                    self.current
                        .as_mut()
                        .expect("active receive missing")
                        .as_mut()
                        .request_stop();
                    self.stopping = true;
                }
                if let Some(current) = self.current.as_mut() {
                    match current.as_mut().poll_next(cx) {
                        Poll::Pending => return Poll::Pending,
                        Poll::Ready(Some(_)) => continue,
                        Poll::Ready(None) => {
                            self.current = None;
                            self.stopping = false;
                            continue;
                        }
                    }
                }
                self.ended = true;
                return match self
                    .terminal
                    .take()
                    .expect("terminal receive state missing")
                {
                    TerminalReceive::Eof => Poll::Ready(None),
                    TerminalReceive::Error(error) => Poll::Ready(Some(Err(error))),
                };
            }

            if self.ended {
                return Poll::Ready(None);
            }

            if self.fault_wait.is_none() {
                let ring = self.ring.clone();
                self.fault_wait = Some(Box::pin(async move { Err(ring.wait_for_fault().await) }));
            }
            if let Some(wait) = self.fault_wait.as_mut() {
                match wait.as_mut().poll(cx) {
                    Poll::Pending => {}
                    Poll::Ready(Err(error)) => {
                        self.fault_wait = None;
                        self.terminal = Some(TerminalReceive::Error(error));
                        continue;
                    }
                    Poll::Ready(Ok(())) => unreachable!("fault wait completed healthy"),
                }
            }

            if let Some(wait) = self.wait.as_mut() {
                match wait.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => self.wait = None,
                    Poll::Ready(Err(err)) => {
                        self.wait = None;
                        self.ended = true;
                        return Poll::Ready(Some(Err(err)));
                    }
                }
            }

            if self.current.is_none() {
                if let Err(err) = self.arm() {
                    self.ended = true;
                    return Poll::Ready(Some(Err(err)));
                }
            }

            let result = {
                let current = self.current.as_mut().expect("receive was not armed");
                current.as_mut().poll_next(cx)
            };

            match result {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(bundle))) if bundle.is_empty() => {
                    self.terminal = Some(TerminalReceive::Eof);
                }
                Poll::Ready(Some(Ok(bundle))) => return Poll::Ready(Some(Ok(bundle))),
                Poll::Ready(Some(Err(err))) if err.raw_os_error() == Some(libc::ENOBUFS) => {
                    self.current = None;
                    self.wait_for_buffer();
                }
                Poll::Ready(Some(Err(err))) => {
                    self.terminal = Some(TerminalReceive::Error(err));
                }
                Poll::Ready(None) => {
                    self.current = None;
                    // A multishot receive may end after a non-empty terminal
                    // item without EOF (for example when the kernel elects to
                    // stop the request as ring capacity gets low). Rearm it;
                    // TCP EOF is represented by the explicit empty bundle.
                }
            }
        }
    }
}
