//! Norn TCP server for the in-memory protocol implementation.

use std::cell::Cell;
use std::future::Future;
use std::io;
use std::num::NonZeroUsize;
use std::pin::pin;
use std::rc::Rc;

use futures_util::{FutureExt, Stream, StreamExt};
use norn_executor::spawn;
use norn_uring::buf::{BufCursor, StableBuf, StableBufMut};
use norn_uring::bufring::{BufRingBuf, RecvBufRing};
use norn_uring::net::{TcpListener, TcpSocket};

use crate::codec::{decode_body, DecodeStatus, FrameDecoder, ResponseEncoder};
use crate::handler::{ConnectionAction, MemoryHandler};
use crate::protocol::{EncodeError, RequestHeader, HEADER_LEN};

const RECV_RING_BUFFERS: u16 = 8_192;
const RECV_BUFFER_LEN: usize = 8 * 1024;
/// Buffers one connection's receive may hold before it pauses. This sits
/// above the backlog a polling connection builds up between polls in the
/// benchmark (at most 32 buffers in `mixed-1w`; `mixed-4w` did not pause),
/// and 128 stalled connections are needed to exhaust the 8192-buffer ring.
const MAX_UNCONSUMED_PER_RECEIVE: NonZeroUsize = NonZeroUsize::new(64).unwrap();
const MAX_READY_RECEIVES_PER_BATCH: usize = 16;
const ACCEPT_BACKOFF: std::time::Duration = std::time::Duration::from_millis(10);
/// Longest a failed connection keeps discarding input before it is closed.
const LINGER_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);
/// Most input a failed connection discards before it is closed.
const LINGER_MAX_BYTES: usize = 16 * 1024 * 1024;

/// How the listener reacts to a failed accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AcceptRecovery {
    /// A load condition: wait briefly, then accept again.
    BackOff,
    /// Concerns only the connection being accepted: accept again at once.
    Skip,
    /// Not recoverable: stop the listener.
    Stop,
}

/// Most consecutive skipped accept errors tolerated without a successful
/// accept in between.
///
/// Skipping retries immediately, so an error that is really sticky (a security
/// module that denies every accept with `EPERM`, say) would otherwise spin the
/// listener at full CPU forever and silently.
const MAX_CONSECUTIVE_SKIPPED_ACCEPTS: u32 = 64;

/// Classify the `errno` of a failed accept.
///
/// accept(2) documents that Linux passes the pending network errors of the new
/// connection through accept and that they should be treated like `EAGAIN`:
/// `ENETDOWN`, `EPROTO`, `ENOPROTOOPT`, `EHOSTDOWN`, `ENONET`, `EHOSTUNREACH`
/// and `ENETUNREACH` are skipped. `ECONNABORTED` is a connection that died
/// before it was accepted, and `EPERM` ("Firewall rules forbid connection")
/// and `EINTR` are likewise skipped.
///
/// `EOPNOTSUPP` is on accept(2)'s retry list but is also what accept returns
/// for a listener that is not `SOCK_STREAM`, and `EACCES` is what a security
/// module returns from a check that sees only the listener. Both would repeat
/// on every accept, so they stop the listener rather than spin it.
fn classify_accept_error(errno: Option<i32>) -> AcceptRecovery {
    match errno {
        Some(libc::EMFILE | libc::ENFILE | libc::ENOBUFS | libc::ENOMEM) => AcceptRecovery::BackOff,
        Some(
            libc::ECONNABORTED
            | libc::EPROTO
            | libc::EPERM
            | libc::EINTR
            | libc::ENETDOWN
            | libc::ENOPROTOOPT
            | libc::EHOSTDOWN
            | libc::ENONET
            | libc::EHOSTUNREACH
            | libc::ENETUNREACH,
        ) => AcceptRecovery::Skip,
        _ => AcceptRecovery::Stop,
    }
}

/// Tracks accept failures so the listener can recover from transient ones.
#[derive(Debug, Default)]
pub(crate) struct AcceptErrors {
    consecutive_skips: u32,
}

impl AcceptErrors {
    /// Record a successful accept, ending any run of skipped errors.
    pub(crate) fn accepted(&mut self) {
        self.consecutive_skips = 0;
    }

    /// Decide what to do about a failed accept without waiting.
    fn classify(&mut self, error: &io::Error) -> AcceptRecovery {
        match classify_accept_error(error.raw_os_error()) {
            AcceptRecovery::Skip => {
                self.consecutive_skips += 1;
                if self.consecutive_skips > MAX_CONSECUTIVE_SKIPPED_ACCEPTS {
                    eprintln!(
                        "accept failed {} times in a row: {error}; stopping",
                        self.consecutive_skips
                    );
                    AcceptRecovery::Stop
                } else {
                    AcceptRecovery::Skip
                }
            }
            other => other,
        }
    }

    /// Decide whether an accept failure should stop the listener.
    ///
    /// Descriptor and memory exhaustion are load conditions: back off briefly
    /// so pending connections can drain instead of retrying in a hot loop.
    /// Errors that concern only the connection being accepted are skipped,
    /// unless more than [`MAX_CONSECUTIVE_SKIPPED_ACCEPTS`] arrive without a
    /// successful accept. Anything else is returned to the caller.
    pub(crate) async fn recover(&mut self, error: io::Error) -> io::Result<()> {
        match self.classify(&error) {
            AcceptRecovery::BackOff => {
                eprintln!("accept failed: {error}; backing off");
                norn_uring::Handle::current()
                    .timeout(ACCEPT_BACKOFF)
                    .await?;
                Ok(())
            }
            AcceptRecovery::Skip => Ok(()),
            AcceptRecovery::Stop => Err(error),
        }
    }
}

/// Close a connection refused by the connection limit.
///
/// A failed close affects only that connection, so it is reported and the
/// listener keeps running.
pub(crate) async fn close_rejected(socket: TcpSocket) {
    if let Err(error) = socket.close().await {
        eprintln!("closing rejected connection failed: {error}");
    }
}

/// Discard a failed connection's input before its socket is dropped.
///
/// Closing a socket with unread input makes the kernel send RST instead of
/// FIN, and the peer then discards replies it has not read yet, including
/// replies flushed just before the failure. Shutting down the write half
/// queues FIN behind those replies, and `discard` reads input until the peer
/// closes, so the caller's close is orderly. Discarding stops after
/// [`LINGER_TIMEOUT`] or [`LINGER_MAX_BYTES`], and the close may then still
/// reset the connection.
pub(crate) async fn lingering_close(socket: &TcpSocket, discard: impl Future<Output = ()>) {
    if socket.shutdown(std::net::Shutdown::Write).await.is_err() {
        return;
    }
    let timeout = norn_uring::Handle::current().timeout(LINGER_TIMEOUT);
    futures_util::future::select(pin!(discard), pin!(timeout)).await;
}

/// Discard multishot input for [`lingering_close`].
pub(crate) async fn discard_multishot<S>(incoming: &mut S)
where
    S: Stream<Item = io::Result<BufRingBuf>> + Unpin,
{
    let mut discarded = 0;
    while discarded < LINGER_MAX_BYTES {
        match incoming.next().await {
            Some(Ok(selected)) if !selected.is_empty() => discarded += selected.len(),
            _ => return,
        }
    }
}

/// Discard input with owned-buffer receives for [`lingering_close`].
async fn discard_exact(socket: &TcpSocket) {
    let mut buffer = RecvBuffer::new();
    let mut discarded = 0;
    while discarded < LINGER_MAX_BYTES {
        if buffer.reset(RECV_BUFFER_LEN).is_err() {
            return;
        }
        let (result, returned) = socket.recv(buffer).await;
        buffer = returned;
        match result {
            Ok(received) if received != 0 => discarded += received,
            _ => return,
        }
    }
}

/// Socket receive strategy used by the networking experiment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecvMode {
    /// Submit exact-sized owned-buffer receives for each frame component.
    Exact,
    /// Keep a provided-buffer multishot receive armed on every connection.
    #[default]
    Multishot,
}

/// Largest request body the wire protocol can declare.
///
/// The header's total body length field is 32 bits wide, so a larger
/// `max_body_len` could never be reached.
const MAX_PROTOCOL_BODY_LEN: u64 = u32::MAX as u64;

/// Runtime limits for the networking-first server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// Maximum request body accepted from one command.
    ///
    /// Connection buffers grow to the frames and replies they actually carry,
    /// not to this limit. It must not exceed what the protocol's 32-bit body
    /// length field can express; [`ServerConfig::validate`] rejects larger
    /// values.
    pub max_body_len: usize,
    /// Maximum number of concurrently active connection tasks.
    pub max_connections: usize,
    /// Socket receive strategy.
    pub recv_mode: RecvMode,
    /// Maximum commands executed before a response batch is sent.
    pub max_batch_commands: usize,
    /// Maximum encoded response bytes accumulated before a batch is sent.
    ///
    /// A single response may exceed this limit by at most one command because
    /// its final size is only known after command execution.
    pub max_batch_response_bytes: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_body_len: 64 * 1024,
            max_connections: 4_096,
            recv_mode: RecvMode::Multishot,
            max_batch_commands: 2_048,
            max_batch_response_bytes: 1024 * 1024,
        }
    }
}

impl ServerConfig {
    /// Validate server limits before allocating connection resources.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::InvalidInput`] when a numeric limit is zero or
    /// the maximum frame length cannot be represented by `usize` or exceeds the
    /// protocol's 32-bit body length field.
    pub fn validate(&self) -> io::Result<()> {
        if self.max_body_len == 0
            || self.max_connections == 0
            || self.max_batch_commands == 0
            || self.max_batch_response_bytes == 0
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "server limits must be greater than zero",
            ));
        }
        self.max_body_len.checked_add(HEADER_LEN).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum frame length overflows usize",
            )
        })?;
        if self.max_body_len as u64 > MAX_PROTOCOL_BODY_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum body length exceeds the protocol's 32-bit limit",
            ));
        }
        Ok(())
    }
}

/// Build the provided-buffer ring that every multishot connection on a worker
/// shares.
///
/// A connection whose task stops polling its receive stream, for example
/// while a send blocks, pauses after holding [`MAX_UNCONSUMED_PER_RECEIVE`]
/// buffers instead of taking the whole ring.
pub(crate) fn recv_ring(buffers: u16) -> io::Result<RecvBufRing> {
    RecvBufRing::builder(1)
        .buf_cnt(buffers)
        .buf_len(RECV_BUFFER_LEN)
        .max_unconsumed_per_receive(MAX_UNCONSUMED_PER_RECEIVE)
        .build()
}

/// Accept connections and serve requests until the listener fails or closes.
///
/// # Errors
///
/// Returns an error when `config` is invalid or the multishot accept stream
/// fails.
pub async fn serve(
    listener: TcpListener,
    handler: MemoryHandler,
    config: ServerConfig,
) -> io::Result<()> {
    config.validate()?;
    let active = Rc::new(Cell::new(0_usize));
    let recv_ring = match config.recv_mode {
        RecvMode::Exact => None,
        RecvMode::Multishot => Some(recv_ring(RECV_RING_BUFFERS)?),
    };
    let mut incoming = pin!(listener.incoming());
    let mut accept_errors = AcceptErrors::default();
    while let Some(next) = incoming.next().await {
        let socket = match next {
            Ok(socket) => {
                accept_errors.accepted();
                socket
            }
            Err(error) => {
                accept_errors.recover(error).await?;
                continue;
            }
        };
        if active.get() >= config.max_connections {
            close_rejected(socket).await;
            continue;
        }

        active.set(active.get() + 1);
        let guard = ConnectionGuard(Rc::clone(&active));
        let handler = handler.clone();
        let recv_ring = recv_ring.clone();
        spawn(async move {
            let result = match recv_ring {
                Some(recv_ring) => {
                    serve_connection_multishot(socket, handler, config, recv_ring).await
                }
                None => serve_connection(socket, handler, config).await,
            };
            drop(guard);
            if let Err(error) = result {
                if error.kind() != io::ErrorKind::ConnectionReset {
                    eprintln!("connection failed: {error}");
                }
            }
        })
        .detach();
    }
    Ok(())
}

async fn serve_connection_multishot(
    socket: TcpSocket,
    handler: MemoryHandler,
    config: ServerConfig,
    recv_ring: RecvBufRing,
) -> io::Result<()> {
    socket.set_nodelay(true).await?;

    let mut incoming = Box::pin(socket.recv_ring_multi(&recv_ring));
    let mut pending = Vec::with_capacity(RECV_BUFFER_LEN);
    let mut response = Vec::with_capacity(RECV_BUFFER_LEN);

    loop {
        response.clear();
        let outcome =
            fill_batch(&mut incoming, &handler, config, &mut pending, &mut response).await;

        // Replies already in the batch belong to requests that were executed.
        // Send them before acting on a close or a fatal protocol error, so a
        // rejected or truncated frame does not discard earlier replies.
        if !response.is_empty() {
            response = send_all(&socket, response).await?;
        }
        match outcome {
            Ok(BatchEnd::Open) => {}
            Ok(BatchEnd::PeerClosed) => {
                drop(incoming);
                return Ok(());
            }
            Ok(BatchEnd::Quit) => {
                // Requests pipelined after QUIT are left unread.
                lingering_close(&socket, discard_multishot(&mut incoming)).await;
                return Ok(());
            }
            Err(error) => {
                lingering_close(&socket, discard_multishot(&mut incoming)).await;
                return Err(error);
            }
        }
    }
}

/// How a batch left the connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchEnd {
    /// Keep serving.
    Open,
    /// The peer closed its write half after complete frames.
    PeerClosed,
    /// A QUIT was executed; the peer may still be sending.
    Quit,
}

/// Execute one batch of buffered and newly received frames.
///
/// Replies are appended to `response`. Returns whether and why the connection
/// should close after the batch. On error, `response` still holds the replies of the
/// frames executed before the failure, and the caller sends them before
/// failing the connection.
async fn fill_batch<S>(
    incoming: &mut S,
    handler: &MemoryHandler,
    config: ServerConfig,
    pending: &mut Vec<u8>,
    response: &mut Vec<u8>,
) -> io::Result<BatchEnd>
where
    S: Stream<Item = io::Result<BufRingBuf>> + Unpin,
{
    let mut commands = 0;

    if !pending.is_empty() && process_pending(handler, config, pending, response, &mut commands)? {
        return Ok(BatchEnd::Quit);
    }

    for selected_count in 0..MAX_READY_RECEIVES_PER_BATCH {
        if batch_full(config, commands, response.len()) {
            break;
        }

        let next = if selected_count == 0 && commands == 0 {
            incoming.next().await
        } else {
            let Some(next) = incoming.next().now_or_never() else {
                break;
            };
            next
        };
        let selected = match next {
            Some(Ok(selected)) if !selected.is_empty() => selected,
            Some(Err(error)) => return Err(error),
            Some(Ok(_)) | None => {
                if pending.is_empty() {
                    return Ok(BatchEnd::PeerClosed);
                }
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        };
        if process_selected_buffer(selected, handler, config, pending, response, &mut commands)? {
            return Ok(BatchEnd::Quit);
        }
    }
    Ok(BatchEnd::Open)
}

fn process_selected_buffer(
    selected: BufRingBuf,
    handler: &MemoryHandler,
    config: ServerConfig,
    pending: &mut Vec<u8>,
    response: &mut Vec<u8>,
    commands: &mut usize,
) -> io::Result<bool> {
    debug_assert!(!selected.is_empty(), "the caller handles EOF");
    let close = if pending.is_empty() {
        let outcome =
            process_available_frames(handler, config, selected.as_slice(), response, commands)?;
        if outcome.consumed != selected.len() {
            grow_pending(pending, selected.len() - outcome.consumed)?;
            pending.extend_from_slice(&selected[outcome.consumed..]);
        }
        outcome.close
    } else {
        grow_pending(pending, selected.len())?;
        pending.extend_from_slice(selected.as_slice());
        process_pending(handler, config, pending, response, commands)?
    };

    Ok(close)
}

/// Reserve room to buffer `additional` more received bytes.
///
/// A client can stream up to `max_body_len` of one frame's body before the
/// frame completes, so growth must fail only this connection rather than
/// abort the process.
///
/// # Errors
///
/// Returns [`io::ErrorKind::OutOfMemory`] if the buffer cannot grow.
pub(crate) fn grow_pending(pending: &mut Vec<u8>, additional: usize) -> io::Result<()> {
    pending.try_reserve(additional).map_err(|_| {
        io::Error::new(
            io::ErrorKind::OutOfMemory,
            "cannot grow the partial request buffer",
        )
    })
}

fn process_pending(
    handler: &MemoryHandler,
    config: ServerConfig,
    pending: &mut Vec<u8>,
    response: &mut Vec<u8>,
    commands: &mut usize,
) -> io::Result<bool> {
    let outcome = process_available_frames(handler, config, pending, response, commands)?;
    if outcome.consumed != 0 {
        let remaining = pending.len() - outcome.consumed;
        pending.copy_within(outcome.consumed.., 0);
        pending.truncate(remaining);
    }
    // FrameDecoder enforces the leading frame's declared body limit. A batch
    // budget can leave many complete frames here; their aggregate length is
    // not a frame size.
    Ok(outcome.close)
}

#[derive(Debug, Clone, Copy)]
struct ProcessOutcome {
    consumed: usize,
    close: bool,
}

fn process_available_frames(
    handler: &MemoryHandler,
    config: ServerConfig,
    bytes: &[u8],
    response: &mut Vec<u8>,
    commands: &mut usize,
) -> io::Result<ProcessOutcome> {
    let decoder = FrameDecoder::new(config.max_body_len);
    let mut consumed = 0;
    while !batch_full(config, *commands, response.len()) {
        let DecodeStatus::Frame {
            frame,
            consumed: frame_len,
        } = decoder.decode(&bytes[consumed..]).map_err(invalid_data)?
        else {
            break;
        };

        let action = handler
            .handle(frame, &mut ResponseEncoder::new(response))
            .map_err(encode_error)?;
        consumed += frame_len;
        *commands += 1;

        if action == ConnectionAction::Close {
            return Ok(ProcessOutcome {
                consumed,
                close: true,
            });
        }
    }
    Ok(ProcessOutcome {
        consumed,
        close: false,
    })
}

fn batch_full(config: ServerConfig, commands: usize, response_bytes: usize) -> bool {
    commands >= config.max_batch_commands || response_bytes >= config.max_batch_response_bytes
}

struct ConnectionGuard(Rc<Cell<usize>>);

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

async fn serve_connection(
    socket: TcpSocket,
    handler: MemoryHandler,
    config: ServerConfig,
) -> io::Result<()> {
    socket.set_nodelay(true).await?;

    match serve_exact_frames(&socket, handler, config).await {
        Ok(BatchEnd::Quit) => {
            // Requests pipelined after QUIT are left unread.
            lingering_close(&socket, discard_exact(&socket)).await;
            Ok(())
        }
        Ok(BatchEnd::PeerClosed | BatchEnd::Open) => socket.close().await,
        Err(error) => {
            lingering_close(&socket, discard_exact(&socket)).await;
            Err(error)
        }
    }
}

/// Serve frames until the peer closes or sends QUIT.
async fn serve_exact_frames(
    socket: &TcpSocket,
    handler: MemoryHandler,
    config: ServerConfig,
) -> io::Result<BatchEnd> {
    let mut header_buf = RecvBuffer::new();
    // Grows to the largest body this connection receives, so a large
    // `max_body_len` costs memory only when a client sends such a frame.
    let mut body_buf = RecvBuffer::new();
    let mut response = Vec::with_capacity(RECV_BUFFER_LEN);

    loop {
        header_buf.reset(HEADER_LEN)?;
        let Some(returned) = recv_exact(socket, header_buf).await? else {
            return Ok(BatchEnd::PeerClosed);
        };
        header_buf = returned;

        let header = RequestHeader::decode(header_buf.filled_slice()).map_err(invalid_data)?;
        if header.body_len() > config.max_body_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "request body length {} exceeds configured maximum {}",
                    header.body_len(),
                    config.max_body_len
                ),
            ));
        }

        body_buf.reset(header.body_len())?;
        if header.body_len() != 0 {
            body_buf = recv_exact(socket, body_buf)
                .await?
                .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;
        }

        response.clear();
        let action = handler
            .handle(
                decode_body(header, body_buf.filled_slice()),
                &mut ResponseEncoder::new(&mut response),
            )
            .map_err(encode_error)?;

        if !response.is_empty() {
            response = send_all(socket, response).await?;
        }
        if action == ConnectionAction::Close {
            return Ok(BatchEnd::Quit);
        }
    }
}

async fn recv_exact(socket: &TcpSocket, mut buffer: RecvBuffer) -> io::Result<Option<RecvBuffer>> {
    while !buffer.is_full() {
        let (result, returned) = socket.recv(buffer).await;
        buffer = returned;
        let received = result?;
        if received == 0 {
            if buffer.filled() == 0 {
                return Ok(None);
            }
            return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
        }
    }
    Ok(Some(buffer))
}

async fn send_all(socket: &TcpSocket, buffer: Vec<u8>) -> io::Result<Vec<u8>> {
    let mut cursor = BufCursor::new(buffer);
    while cursor.bytes_init() != 0 {
        let (result, returned) = socket.send(cursor).await;
        cursor = returned;
        let sent = result?;
        if sent == 0 {
            return Err(io::Error::from(io::ErrorKind::WriteZero));
        }
        cursor.consume(sent);
    }
    Ok(cursor.into_inner())
}

fn invalid_data(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn encode_error(error: EncodeError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

/// Receive buffer filled up to a per-receive limit.
///
/// The received bytes are the vector's initialized length; receives write
/// into its spare capacity.
#[derive(Debug, Default)]
struct RecvBuffer {
    storage: Vec<u8>,
    limit: usize,
}

impl RecvBuffer {
    fn new() -> Self {
        Self::default()
    }

    /// Empty the buffer and accept up to `limit` bytes, growing the storage
    /// if it is smaller.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::OutOfMemory`] if the storage cannot grow, so
    /// only the connection that declared the frame fails.
    fn reset(&mut self, limit: usize) -> io::Result<()> {
        self.storage.clear();
        self.storage.try_reserve_exact(limit).map_err(|_| {
            io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("cannot allocate a {limit}-byte receive buffer"),
            )
        })?;
        self.limit = limit;
        Ok(())
    }

    fn filled(&self) -> usize {
        self.storage.len()
    }

    fn is_full(&self) -> bool {
        self.storage.len() == self.limit
    }

    fn filled_slice(&self) -> &[u8] {
        &self.storage
    }
}

// Safety: the vector's heap allocation stays put while the wrapper moves, and
// it is not resized while a receive operation owns the wrapper. The exposed
// tail is spare capacity: `len <= limit <= capacity`.
unsafe impl StableBufMut for RecvBuffer {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.storage.spare_capacity_mut().as_mut_ptr().cast()
    }

    fn bytes_remaining(&self) -> usize {
        self.limit - self.storage.len()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(init_len <= self.bytes_remaining());
        // Safety: the caller initialized `init_len` bytes of spare capacity,
        // which fits because `len + init_len <= limit <= capacity`.
        unsafe { self.storage.set_len(self.storage.len() + init_len) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::MemoryStore;
    use crate::protocol::{
        Status, OP_GET, OP_NOOP, OP_QUIT, OP_SET, REQUEST_MAGIC, RESPONSE_MAGIC,
    };

    #[test]
    fn accept_errors_are_classified() {
        for errno in [libc::EMFILE, libc::ENFILE, libc::ENOBUFS, libc::ENOMEM] {
            assert_eq!(
                classify_accept_error(Some(errno)),
                AcceptRecovery::BackOff,
                "errno {errno}"
            );
        }
        // Pending network errors of the new connection (accept(2) says to
        // retry them like EAGAIN) and the other connection-local errors.
        for errno in [
            libc::ENETDOWN,
            libc::EPROTO,
            libc::ENOPROTOOPT,
            libc::EHOSTDOWN,
            libc::ENONET,
            libc::EHOSTUNREACH,
            libc::ENETUNREACH,
            libc::ECONNABORTED,
            libc::EPERM,
            libc::EINTR,
        ] {
            assert_eq!(
                classify_accept_error(Some(errno)),
                AcceptRecovery::Skip,
                "errno {errno}"
            );
        }
        // EOPNOTSUPP and EACCES depend on the listener, so they repeat on
        // every accept and must stop it instead of spinning.
        for errno in [
            libc::EOPNOTSUPP,
            libc::EACCES,
            libc::EBADF,
            libc::EINVAL,
            libc::ENOTSOCK,
            libc::EFAULT,
        ] {
            assert_eq!(
                classify_accept_error(Some(errno)),
                AcceptRecovery::Stop,
                "errno {errno}"
            );
        }
        assert_eq!(classify_accept_error(None), AcceptRecovery::Stop);
    }

    #[test]
    fn accept_errors_back_off_skip_or_stop_the_listener() {
        use norn_executor::LocalExecutor;

        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async {
            let mut accept_errors = AcceptErrors::default();
            for errno in [
                libc::EMFILE,
                libc::ENFILE,
                libc::ECONNABORTED,
                libc::EPROTO,
                libc::ENETDOWN,
            ] {
                accept_errors
                    .recover(io::Error::from_raw_os_error(errno))
                    .await
                    .unwrap_or_else(|error| panic!("errno {errno} stopped the listener: {error}"));
            }
            for errno in [libc::EBADF, libc::EACCES, libc::EOPNOTSUPP] {
                let fatal = accept_errors
                    .recover(io::Error::from_raw_os_error(errno))
                    .await;
                assert_eq!(fatal.unwrap_err().raw_os_error(), Some(errno));
            }
        });
    }

    #[test]
    fn sticky_skipped_accept_errors_stop_the_listener() {
        let mut accept_errors = AcceptErrors::default();
        let sticky = io::Error::from_raw_os_error(libc::EPERM);
        for attempt in 1..=MAX_CONSECUTIVE_SKIPPED_ACCEPTS {
            assert_eq!(
                accept_errors.classify(&sticky),
                AcceptRecovery::Skip,
                "skip {attempt}"
            );
        }
        assert_eq!(accept_errors.classify(&sticky), AcceptRecovery::Stop);
        assert_eq!(accept_errors.classify(&sticky), AcceptRecovery::Stop);

        // A successful accept in between ends the run, so isolated errors
        // never accumulate toward the limit.
        accept_errors.accepted();
        for _ in 0..MAX_CONSECUTIVE_SKIPPED_ACCEPTS {
            assert_eq!(accept_errors.classify(&sticky), AcceptRecovery::Skip);
        }
        accept_errors.accepted();
        assert_eq!(accept_errors.classify(&sticky), AcceptRecovery::Skip);

        // Back-off errors are paced by their timeout and are not counted.
        let mut accept_errors = AcceptErrors::default();
        let load = io::Error::from_raw_os_error(libc::EMFILE);
        for _ in 0..=MAX_CONSECUTIVE_SKIPPED_ACCEPTS {
            assert_eq!(accept_errors.classify(&load), AcceptRecovery::BackOff);
        }
    }

    #[test]
    fn listener_level_eopnotsupp_stops_serve_instead_of_spinning() {
        use norn_executor::LocalExecutor;
        use norn_uring::net::UdpSocket;

        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
        let mut executor = LocalExecutor::new(driver);
        executor.block_on(async {
            // A "listener" whose every accept fails with EOPNOTSUPP: the
            // socket is not SOCK_STREAM (accept(2) ERRORS).
            let udp = UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap();
            let listener = TcpListener::from_uring_fd(udp.into_uring_fd());
            let handler = MemoryHandler::new(MemoryStore::new());
            let serve = pin!(serve(listener, handler, ServerConfig::default()));
            let timeout =
                pin!(norn_uring::Handle::current().timeout(std::time::Duration::from_secs(5)));
            match futures_util::future::select(serve, timeout).await {
                futures_util::future::Either::Left((result, _)) => {
                    let error = result.expect_err("serve on a non-stream socket must fail");
                    assert_eq!(error.raw_os_error(), Some(libc::EOPNOTSUPP));
                }
                futures_util::future::Either::Right(_) => {
                    panic!("serve kept running on a listener whose accept always fails")
                }
            }
        });
    }

    #[test]
    fn small_frame_limit_allows_budgeted_pipeline_and_incomplete_tail() {
        for response_limit in [false, true] {
            let handler = MemoryHandler::new(MemoryStore::new());
            let config = ServerConfig {
                max_body_len: 64,
                max_batch_commands: if response_limit { 256 } else { 1 },
                max_batch_response_bytes: if response_limit { 1 } else { 4096 },
                ..ServerConfig::default()
            };
            let mut pending = Vec::new();
            for opaque in 0..10_u32 {
                pending.extend_from_slice(&encode_request(OP_NOOP, &[], &[], &[], opaque));
            }
            let set = encode_request(
                OP_SET,
                b"key",
                &3_u32.to_be_bytes(),
                b"fragmented value",
                10,
            );
            pending.extend_from_slice(&set[..HEADER_LEN + 1]);
            let mut response = Vec::new();
            for opaque in 0..10_u32 {
                let mut commands = 0;
                response.clear();
                assert!(!process_pending(
                    &handler,
                    config,
                    &mut pending,
                    &mut response,
                    &mut commands
                )
                .unwrap());
                assert_eq!(commands, 1);
                assert_eq!(&response[12..16], &opaque.to_be_bytes());
            }
            assert_eq!(pending, set[..HEADER_LEN + 1]);
            response.clear();
            let mut commands = 0;
            assert!(
                !process_pending(&handler, config, &mut pending, &mut response, &mut commands)
                    .unwrap()
            );
            assert_eq!(commands, 0);
            pending.extend_from_slice(&set[HEADER_LEN + 1..]);
            assert!(
                !process_pending(&handler, config, &mut pending, &mut response, &mut commands)
                    .unwrap()
            );
            assert_eq!(commands, 1);
            assert_eq!(&response[12..16], &10_u32.to_be_bytes());
            assert!(pending.is_empty());

            // A declared oversized leading frame must still fail immediately.
            let mut oversized = encode_request(OP_SET, b"key", &[], &[0; 65], 11);
            oversized.truncate(HEADER_LEN);
            let error = process_pending(&handler, config, &mut oversized, &mut Vec::new(), &mut 0)
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn server_config_rejects_zero_limits() {
        let mut configs = [ServerConfig::default(); 4];
        configs[0].max_body_len = 0;
        configs[1].max_connections = 0;
        configs[2].max_batch_commands = 0;
        configs[3].max_batch_response_bytes = 0;

        for config in configs {
            assert_eq!(
                config.validate().unwrap_err().kind(),
                io::ErrorKind::InvalidInput
            );
        }
    }

    #[test]
    fn server_config_rejects_frame_length_overflow() {
        let config = ServerConfig {
            max_body_len: usize::MAX,
            ..ServerConfig::default()
        };
        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "maximum frame length overflows usize");
    }

    #[test]
    fn server_config_rejects_body_beyond_protocol_limit() {
        let accepted = ServerConfig {
            max_body_len: u32::MAX as usize,
            ..ServerConfig::default()
        };
        accepted.validate().unwrap();

        let config = ServerConfig {
            max_body_len: u32::MAX as usize + 1,
            ..ServerConfig::default()
        };
        let error = config.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            "maximum body length exceeds the protocol's 32-bit limit"
        );
    }

    #[test]
    fn command_limit_leaves_complete_pipeline_for_next_batch() {
        let handler = MemoryHandler::new(MemoryStore::new());
        let first = encode_request(OP_NOOP, &[], &[], &[], 1);
        let second = encode_request(OP_NOOP, &[], &[], &[], 2);
        let mut pending = first.clone();
        pending.extend_from_slice(&second);
        let config = ServerConfig {
            max_batch_commands: 1,
            ..ServerConfig::default()
        };

        let mut response = Vec::new();
        let mut commands = 0;
        assert!(
            !process_pending(&handler, config, &mut pending, &mut response, &mut commands,)
                .unwrap()
        );
        assert_eq!(commands, 1);
        assert_eq!(pending, second);
        assert_eq!(&response[12..16], &1_u32.to_be_bytes());

        response.clear();
        commands = 0;
        assert!(
            !process_pending(&handler, config, &mut pending, &mut response, &mut commands,)
                .unwrap()
        );
        assert_eq!(commands, 1);
        assert!(pending.is_empty());
        assert_eq!(&response[12..16], &2_u32.to_be_bytes());
    }

    #[test]
    fn response_limit_stops_after_the_command_that_crosses_it() {
        let handler = MemoryHandler::new(MemoryStore::new());
        let first = encode_request(OP_GET, b"missing-1", &[], &[], 1);
        let second = encode_request(OP_GET, b"missing-2", &[], &[], 2);
        let mut pipeline = first.clone();
        pipeline.extend_from_slice(&second);
        let config = ServerConfig {
            max_batch_response_bytes: 1,
            ..ServerConfig::default()
        };

        let mut response = Vec::new();
        let mut commands = 0;
        let outcome =
            process_available_frames(&handler, config, &pipeline, &mut response, &mut commands)
                .unwrap();
        assert_eq!(commands, 1);
        assert_eq!(outcome.consumed, first.len());
        assert_eq!(response.len(), HEADER_LEN);
        assert_eq!(&response[12..16], &1_u32.to_be_bytes());
    }

    #[test]
    fn receive_buffer_advances_without_overwriting() {
        let mut buffer = RecvBuffer::new();
        buffer.reset(5).unwrap();
        let first = buffer.stable_ptr_mut();
        unsafe {
            first.write(1);
            first.add(1).write(2);
            buffer.set_init(2);
        }
        let second = buffer.stable_ptr_mut();
        unsafe {
            second.write(3);
            second.add(1).write(4);
            second.add(2).write(5);
            buffer.set_init(3);
        }
        assert!(buffer.is_full());
        assert_eq!(buffer.filled_slice(), &[1, 2, 3, 4, 5]);
    }

    #[test]
    fn loopback_handles_fragmented_and_pipelined_requests() {
        for mode in [RecvMode::Exact, RecvMode::Multishot] {
            loopback_handles_fragmented_and_pipelined_requests_with(
                mode,
                ServerConfig::default(),
                64,
            );
        }
    }

    #[test]
    fn loopback_small_limits_preserve_coalesced_pipeline() {
        for response_limit in [false, true] {
            let config = ServerConfig {
                max_body_len: 64,
                max_batch_commands: if response_limit { 256 } else { 1 },
                max_batch_response_bytes: if response_limit { 1 } else { 4096 },
                ..ServerConfig::default()
            };
            loopback_handles_fragmented_and_pipelined_requests_with(
                RecvMode::Multishot,
                config,
                1024,
            );
        }
    }

    fn loopback_handles_fragmented_and_pipelined_requests_with(
        mode: RecvMode,
        config: ServerConfig,
        buf_len: usize,
    ) {
        use std::io::Write;
        use std::net::{SocketAddr, TcpStream};
        use std::sync::mpsc;
        use std::thread;

        use norn_executor::LocalExecutor;

        let (address_tx, address_rx) = mpsc::sync_channel::<SocketAddr>(1);
        let server = thread::spawn(move || -> io::Result<()> {
            let builder = io_uring::IoUring::builder();
            let driver = norn_uring::Driver::new(builder, 64)?;
            let mut executor = LocalExecutor::new(driver);
            executor.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8).await?;
                address_tx.send(listener.local_addr()?).unwrap();
                let (socket, _) = listener.accept().await?;
                let handler = MemoryHandler::new(MemoryStore::new());
                match mode {
                    RecvMode::Exact => serve_connection(socket, handler, config).await,
                    RecvMode::Multishot => {
                        let ring = RecvBufRing::builder(7)
                            .buf_cnt(64)
                            .buf_len(buf_len)
                            .build()?;
                        serve_connection_multishot(socket, handler, config, ring).await
                    }
                }
            })
        });

        let mut client = TcpStream::connect(address_rx.recv().unwrap()).unwrap();
        client.set_nodelay(true).unwrap();
        client
            .set_read_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();
        client
            .set_write_timeout(Some(std::time::Duration::from_secs(10)))
            .unwrap();

        let mut noops = Vec::new();
        for opaque in 100..110 {
            noops.extend_from_slice(&encode_request(OP_NOOP, &[], &[], &[], opaque));
        }
        // More than one frame cap, followed by a fragmented valid SET.
        client.write_all(&noops).unwrap();
        for opaque in 100..110_u32 {
            let (header, body) = read_response(&mut client);
            assert_eq!(&header[12..16], &opaque.to_be_bytes());
            assert!(body.is_empty());
        }

        let set = encode_request(OP_SET, b"key", &3_u32.to_be_bytes(), b"value", 1);
        for byte in set {
            client.write_all(&[byte]).unwrap();
        }
        let (header, body) = read_response(&mut client);
        assert_eq!(header[0], RESPONSE_MAGIC);
        assert_eq!(header[1], OP_SET);
        assert_eq!(&header[6..8], &(Status::Success as u16).to_be_bytes());
        assert!(body.is_empty());

        let mut pipeline = encode_request(OP_GET, b"key", &[], &[], 2);
        pipeline.extend_from_slice(&encode_request(OP_NOOP, &[], &[], &[], 3));
        pipeline.extend_from_slice(&encode_request(OP_QUIT, &[], &[], &[], 4));
        client.write_all(&pipeline).unwrap();

        let (get_header, get_body) = read_response(&mut client);
        assert_eq!(get_header[1], OP_GET);
        assert_eq!(&get_header[12..16], &2_u32.to_be_bytes());
        assert_eq!(&get_body[..4], &3_u32.to_be_bytes());
        assert_eq!(&get_body[4..], b"value");

        let (noop_header, noop_body) = read_response(&mut client);
        assert_eq!(noop_header[1], OP_NOOP);
        assert_eq!(&noop_header[12..16], &3_u32.to_be_bytes());
        assert!(noop_body.is_empty());

        let (quit_header, quit_body) = read_response(&mut client);
        assert_eq!(quit_header[1], OP_QUIT);
        assert_eq!(&quit_header[12..16], &4_u32.to_be_bytes());
        assert!(quit_body.is_empty());

        drop(client);
        server.join().unwrap().unwrap();
    }

    fn encode_request(
        opcode: u8,
        key: &[u8],
        extras_prefix: &[u8],
        value: &[u8],
        opaque: u32,
    ) -> Vec<u8> {
        let extras_len = if opcode == OP_SET { 8 } else { 0 };
        let mut extras = Vec::with_capacity(extras_len);
        extras.extend_from_slice(extras_prefix);
        extras.resize(extras_len, 0);
        let body_len = extras.len() + key.len() + value.len();
        let mut request = Vec::with_capacity(HEADER_LEN + body_len);
        request.push(REQUEST_MAGIC);
        request.push(opcode);
        request.extend_from_slice(&(key.len() as u16).to_be_bytes());
        request.push(extras_len as u8);
        request.push(0);
        request.extend_from_slice(&0_u16.to_be_bytes());
        request.extend_from_slice(&(body_len as u32).to_be_bytes());
        request.extend_from_slice(&opaque.to_be_bytes());
        request.extend_from_slice(&0_u64.to_be_bytes());
        request.extend_from_slice(&extras);
        request.extend_from_slice(key);
        request.extend_from_slice(value);
        request
    }

    fn read_response(stream: &mut std::net::TcpStream) -> ([u8; HEADER_LEN], Vec<u8>) {
        use std::io::Read;

        let mut header = [0_u8; HEADER_LEN];
        stream.read_exact(&mut header).unwrap();
        let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut body = vec![0_u8; body_len];
        stream.read_exact(&mut body).unwrap();
        (header, body)
    }

    #[test]
    fn non_reading_peer_does_not_stall_other_connections() {
        crate::slow_peer::hog_does_not_stall_other_connections(|_, socket, ring| {
            serve_connection_multishot(
                socket,
                MemoryHandler::fixed_response(crate::slow_peer::HOG_RESPONSE_BYTES),
                ServerConfig::default(),
                ring,
            )
        });
    }

    #[test]
    fn small_ring_completes_all_requests() {
        small_ring_connections(1);
    }

    #[test]
    fn shared_small_ring_preserves_two_connections() {
        small_ring_connections(2);
    }

    fn small_ring_connections(connections: u32) {
        use norn_executor::LocalExecutor;
        use std::io::Write;
        use std::net::TcpStream;
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let (address_tx, address_rx) = mpsc::sync_channel(1);
        let (ready_tx, ready_rx) = mpsc::sync_channel(1);
        let server = thread::spawn(move || -> io::Result<()> {
            let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 64)?;
            let mut executor = LocalExecutor::new(driver);
            executor.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8).await?;
                address_tx.send(listener.local_addr()?).unwrap();
                // Queue all 256 requests before arming either receive, as in
                // the review repro. Both tasks share this one-buffer ring.
                ready_rx.recv_timeout(Duration::from_secs(10)).unwrap();
                let ring = RecvBufRing::builder(7)
                    .ring_entries(1)
                    .buf_cnt(1)
                    .buf_len(64)
                    .build()?;
                let mut tasks = Vec::new();
                for _connection in 0..connections {
                    let (socket, _) = listener.accept().await?;
                    tasks.push(norn_executor::spawn(serve_connection_multishot(
                        socket,
                        MemoryHandler::fixed_response(4096),
                        ServerConfig::default(),
                        ring.clone(),
                    )));
                }
                for task in tasks {
                    task.await.unwrap()?;
                }
                Ok(())
            })
        });
        let address = address_rx.recv_timeout(Duration::from_secs(10)).unwrap();
        let mut clients = Vec::new();
        for connection in 0..connections {
            let mut client = TcpStream::connect(address).unwrap();
            client
                .set_read_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            client
                .set_write_timeout(Some(Duration::from_secs(10)))
                .unwrap();
            let mut pipeline = Vec::new();
            for sequence in 0..256 {
                pipeline.extend_from_slice(&encode_request(
                    OP_GET,
                    b"key",
                    &[],
                    &[],
                    connection * 1000 + sequence,
                ));
            }
            client.write_all(&pipeline).unwrap();
            clients.push(client);
        }
        ready_tx.send(()).unwrap();
        // Leave the second client's responses unread while draining the first.
        for (connection, client) in clients.iter_mut().enumerate() {
            for sequence in 0..256 {
                let (header, body) = read_response(client);
                assert_eq!(header[0], RESPONSE_MAGIC);
                assert_eq!(header[1], OP_GET);
                assert_eq!(&header[6..8], &(Status::Success as u16).to_be_bytes());
                assert_eq!(
                    &header[12..16],
                    &(connection as u32 * 1000 + sequence).to_be_bytes()
                );
                assert_eq!(&body[4..], vec![0x5a; 4096]);
            }
            // New traffic after recovering proves the connection is still usable.
            client
                .write_all(&encode_request(OP_NOOP, &[], &[], &[], 9000))
                .unwrap();
            let (header, body) = read_response(client);
            assert_eq!(header[1], OP_NOOP);
            assert_eq!(&header[12..16], &9000_u32.to_be_bytes());
            assert!(body.is_empty());
            client
                .write_all(&encode_request(OP_QUIT, &[], &[], &[], 9001))
                .unwrap();
            assert_eq!(read_response(client).0[1], OP_QUIT);
        }
        drop(clients);
        server.join().unwrap().unwrap();
    }
}
