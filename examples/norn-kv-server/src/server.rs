//! Norn TCP server for the in-memory protocol implementation.

use std::cell::Cell;
use std::io;
use std::pin::pin;
use std::rc::Rc;

use futures_util::{FutureExt, StreamExt};
use norn_executor::spawn;
use norn_uring::buf::{BufCursor, StableBuf, StableBufMut};
use norn_uring::bufring::{BufRingBuf, RecvBufRing};
use norn_uring::net::{TcpListener, TcpSocket};

use crate::codec::{decode_body, DecodeStatus, FrameDecoder, ResponseEncoder};
use crate::handler::{ConnectionAction, MemoryHandler};
use crate::protocol::{EncodeError, RequestHeader, HEADER_LEN};

const RECV_RING_BUFFERS: u16 = 8_192;
const RECV_BUFFER_LEN: usize = 8 * 1024;
const MAX_READY_RECEIVES_PER_BATCH: usize = 16;

/// Socket receive strategy used by the networking experiment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RecvMode {
    /// Submit exact-sized owned-buffer receives for each frame component.
    Exact,
    /// Keep a provided-buffer multishot receive armed on every connection.
    #[default]
    Multishot,
}

/// Runtime limits for the networking-first server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ServerConfig {
    /// Maximum request body accepted from one command.
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

/// Accept connections and serve requests until the listener fails or closes.
///
/// # Errors
///
/// Returns an error when the multishot accept stream fails.
pub async fn serve(
    listener: TcpListener,
    handler: MemoryHandler,
    config: ServerConfig,
) -> io::Result<()> {
    let active = Rc::new(Cell::new(0_usize));
    let recv_ring = match config.recv_mode {
        RecvMode::Exact => None,
        RecvMode::Multishot => Some(
            RecvBufRing::builder(1)
                .buf_cnt(RECV_RING_BUFFERS)
                .buf_len(RECV_BUFFER_LEN)
                .build()?,
        ),
    };
    let mut incoming = pin!(listener.incoming());
    while let Some(next) = incoming.next().await {
        let socket = next?;
        if active.get() >= config.max_connections {
            socket.close().await?;
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
    let mut response = Vec::with_capacity(config.max_body_len + HEADER_LEN);

    loop {
        response.clear();
        let mut commands = 0;
        let mut close = false;

        if !pending.is_empty() {
            close = process_pending(&handler, config, &mut pending, &mut response, &mut commands)?;
        }

        for selected_count in 0..MAX_READY_RECEIVES_PER_BATCH {
            if close || batch_full(config, commands, response.len()) {
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
            let Some(next) = next else {
                if pending.is_empty() {
                    close = true;
                } else {
                    return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
                }
                break;
            };
            close = process_selected_buffer(
                next?,
                &handler,
                config,
                &mut pending,
                &mut response,
                &mut commands,
            )?;
        }

        if !response.is_empty() {
            response = send_all(&socket, response).await?;
        }
        if close {
            drop(incoming);
            return Ok(());
        }
    }
}

fn process_selected_buffer(
    selected: BufRingBuf,
    handler: &MemoryHandler,
    config: ServerConfig,
    pending: &mut Vec<u8>,
    response: &mut Vec<u8>,
    commands: &mut usize,
) -> io::Result<bool> {
    if selected.is_empty() {
        if pending.is_empty() {
            return Ok(true);
        }
        return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
    }

    let close = if pending.is_empty() {
        let outcome =
            process_available_frames(handler, config, selected.as_slice(), response, commands)?;
        if outcome.consumed != selected.len() {
            pending.extend_from_slice(&selected[outcome.consumed..]);
        }
        outcome.close
    } else {
        pending.extend_from_slice(selected.as_slice());
        process_pending(handler, config, pending, response, commands)?
    };

    Ok(close)
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
    if pending.len() > config.max_body_len + HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "request frame exceeds configured maximum",
        ));
    }
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

    let mut header_buf = RecvBuffer::new(HEADER_LEN);
    let mut body_buf = RecvBuffer::new(config.max_body_len);
    let mut response = Vec::with_capacity(config.max_body_len + HEADER_LEN);

    loop {
        header_buf.reset(HEADER_LEN);
        let Some(returned) = recv_exact(&socket, header_buf).await? else {
            return socket.close().await;
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

        body_buf.reset(header.body_len());
        if header.body_len() != 0 {
            body_buf = recv_exact(&socket, body_buf)
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
            response = send_all(&socket, response).await?;
        }
        if action == ConnectionAction::Close {
            return socket.close().await;
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

#[derive(Debug)]
struct RecvBuffer {
    storage: Box<[u8]>,
    filled: usize,
    limit: usize,
}

impl RecvBuffer {
    fn new(capacity: usize) -> Self {
        Self {
            storage: vec![0_u8; capacity].into_boxed_slice(),
            filled: 0,
            limit: capacity,
        }
    }

    fn reset(&mut self, limit: usize) {
        assert!(limit <= self.storage.len());
        self.filled = 0;
        self.limit = limit;
    }

    fn filled(&self) -> usize {
        self.filled
    }

    fn is_full(&self) -> bool {
        self.filled == self.limit
    }

    fn filled_slice(&self) -> &[u8] {
        &self.storage[..self.filled]
    }
}

// Safety: the box keeps its allocation stable while the wrapper moves. The
// wrapper is exclusively owned by each receive operation, and the exposed tail
// remains within the initialized boxed allocation.
unsafe impl StableBufMut for RecvBuffer {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        // Safety: `filled <= limit <= storage.len()` and one-past is permitted
        // when the remaining length is zero.
        unsafe { self.storage.as_mut_ptr().add(self.filled) }
    }

    fn bytes_remaining(&self) -> usize {
        self.limit - self.filled
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(init_len <= self.bytes_remaining());
        self.filled += init_len;
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
        let mut buffer = RecvBuffer::new(8);
        buffer.reset(5);
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
            loopback_handles_fragmented_and_pipelined_requests_with(mode);
        }
    }

    fn loopback_handles_fragmented_and_pipelined_requests_with(mode: RecvMode) {
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
                    RecvMode::Exact => {
                        serve_connection(socket, handler, ServerConfig::default()).await
                    }
                    RecvMode::Multishot => {
                        let ring = RecvBufRing::builder(7).buf_cnt(64).buf_len(64).build()?;
                        serve_connection_multishot(socket, handler, ServerConfig::default(), ring)
                            .await
                    }
                }
            })
        });

        let mut client = TcpStream::connect(address_rx.recv().unwrap()).unwrap();
        client.set_nodelay(true).unwrap();

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
}
