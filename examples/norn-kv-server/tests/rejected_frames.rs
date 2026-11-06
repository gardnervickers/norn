//! A rejected or truncated frame must not discard replies to earlier
//! pipelined requests that the server already executed.
#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpStream};
use std::time::Duration;

use norn_kv_server::handler::HandlerConfig;
use norn_kv_server::server::{RecvMode, ServerConfig};
use norn_kv_server::sharded::{ShardedServer, ShardedServerConfig};

const HEADER_LEN: usize = 24;
const OP_GET: u8 = 0x00;
const OP_SET: u8 = 0x01;
const OP_NOOP: u8 = 0x0a;
const STATUS_SUCCESS: u16 = 0x0000;
const STATUS_INVALID_ARGUMENTS: u16 = 0x0004;

fn request(opcode: u8, key: &[u8], extras: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
    let body_len = extras.len() + key.len() + value.len();
    let mut bytes = Vec::with_capacity(HEADER_LEN + body_len);
    bytes.push(0x80);
    bytes.push(opcode);
    bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
    bytes.push(extras.len() as u8);
    bytes.push(0);
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&(body_len as u32).to_be_bytes());
    bytes.extend_from_slice(&opaque.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes.extend_from_slice(extras);
    bytes.extend_from_slice(key);
    bytes.extend_from_slice(value);
    bytes
}

fn set(key: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
    request(OP_SET, key, &[0; 8], value, opaque)
}

/// `(opcode, status, opaque)` of every reply until the server closes.
fn replies_until_close(stream: &mut TcpStream) -> Vec<(u8, u16, u32)> {
    let mut seen = Vec::new();
    loop {
        let mut header = [0_u8; HEADER_LEN];
        if stream.read_exact(&mut header).is_err() {
            return seen;
        }
        let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
        let mut body = vec![0; body_len];
        if stream.read_exact(&mut body).is_err() {
            return seen;
        }
        seen.push((
            header[1],
            u16::from_be_bytes([header[6], header[7]]),
            u32::from_be_bytes(header[12..16].try_into().unwrap()),
        ));
    }
}

/// Write `wire`, half-close, and collect replies until the server closes.
fn exchange(address: SocketAddr, wire: Vec<u8>) -> Vec<(u8, u16, u32)> {
    let mut stream = TcpStream::connect(address).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut writer = stream.try_clone().unwrap();
    // Write from another thread: the server may close before reading all of
    // a rejected frame, and replies must be drained concurrently.
    let send = std::thread::spawn(move || {
        if writer.write_all(&wire).is_ok() {
            let _ = writer.shutdown(Shutdown::Write);
        }
    });
    let replies = replies_until_close(&mut stream);
    let _ = send.join();
    replies
}

fn single_worker() -> SocketAddr {
    single_worker_with(RecvMode::Multishot)
}

fn single_worker_with(recv_mode: RecvMode) -> SocketAddr {
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let config = ServerConfig {
            recv_mode,
            ..ServerConfig::default()
        };
        let driver = norn_kv_server::server::driver(256, &config).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        executor
            .block_on(async move {
                let listener =
                    norn_uring::net::TcpListener::bind("127.0.0.1:0".parse().unwrap(), 64).await?;
                tx.send(listener.local_addr()?).unwrap();
                norn_kv_server::server::serve(listener, HandlerConfig::Memory.build(), config).await
            })
            .unwrap();
    });
    rx.recv().unwrap()
}

fn two_workers() -> ShardedServer {
    ShardedServer::start(ShardedServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        ..ShardedServerConfig::default()
    })
    .unwrap()
}

/// Pipeline `[SET, <rejected>, GET]` and return the replies.
fn rejected_frame_pipeline(address: SocketAddr, rejected: &[u8]) -> Vec<(u8, u16, u32)> {
    let mut wire = set(b"small", b"v", 1);
    wire.extend_from_slice(rejected);
    wire.extend(request(OP_GET, b"small", &[], &[], 3));
    exchange(address, wire)
}

/// A SET whose declared body exceeds `max_body_len` (64 KiB).
fn oversized_set() -> Vec<u8> {
    set(b"big", &vec![7; 70 * 1024], 2)
}

#[test]
fn oversized_frame_keeps_earlier_replies_single_worker() {
    let replies = rejected_frame_pipeline(single_worker(), &oversized_set());
    assert_eq!(replies, [(OP_SET, STATUS_SUCCESS, 1)]);
}

#[test]
fn oversized_frame_keeps_earlier_replies_sharded() {
    let server = two_workers();
    let replies = rejected_frame_pipeline(server.local_addr(), &oversized_set());
    server.shutdown().unwrap();
    assert_eq!(replies, [(OP_SET, STATUS_SUCCESS, 1)]);
}

/// A key longer than 250 bytes is a command error, not a framing error: the
/// body length still delimits the frame, so the server answers it with
/// invalid arguments and keeps serving the connection.
#[test]
fn long_key_is_answered_with_invalid_arguments() {
    let long_key = request(OP_GET, &[b'k'; 251], &[], &[], 2);
    let expected = [
        (OP_SET, STATUS_SUCCESS, 1),
        (OP_GET, STATUS_INVALID_ARGUMENTS, 2),
        (OP_GET, STATUS_SUCCESS, 3),
    ];
    for mode in [RecvMode::Multishot, RecvMode::Exact] {
        assert_eq!(
            rejected_frame_pipeline(single_worker_with(mode), &long_key),
            expected,
            "{mode:?}"
        );
    }
    let server = two_workers();
    let replies = rejected_frame_pipeline(server.local_addr(), &long_key);
    server.shutdown().unwrap();
    assert_eq!(replies, expected);
}

/// A nonzero data type is likewise answered without closing.
#[test]
fn unsupported_data_type_is_answered_with_invalid_arguments() {
    let mut bad_type = request(OP_GET, b"small", &[], &[], 2);
    bad_type[5] = 1;
    let expected = [
        (OP_SET, STATUS_SUCCESS, 1),
        (OP_GET, STATUS_INVALID_ARGUMENTS, 2),
        (OP_GET, STATUS_SUCCESS, 3),
    ];
    for mode in [RecvMode::Multishot, RecvMode::Exact] {
        assert_eq!(
            rejected_frame_pipeline(single_worker_with(mode), &bad_type),
            expected,
            "{mode:?}"
        );
    }
    let server = two_workers();
    let replies = rejected_frame_pipeline(server.local_addr(), &bad_type);
    server.shutdown().unwrap();
    assert_eq!(replies, expected);
}

/// Complete NOOPs followed by a truncated frame and FIN. The multishot
/// server often receives all of them together with the EOF.
fn truncated_tail_keeps_complete_replies(address: SocketAddr) {
    for _ in 0..10 {
        let mut wire = Vec::new();
        for opaque in 0..8_u32 {
            wire.extend(request(OP_NOOP, &[], &[], &[], opaque));
        }
        wire.extend(&request(OP_GET, b"partial", &[], &[], 99)[..30]);
        let replies = exchange(address, wire);
        let expected: Vec<_> = (0..8_u32)
            .map(|opaque| (OP_NOOP, STATUS_SUCCESS, opaque))
            .collect();
        assert_eq!(replies, expected);
    }
}

#[test]
fn truncated_tail_keeps_complete_replies_single_worker() {
    truncated_tail_keeps_complete_replies(single_worker());
}

#[test]
fn truncated_tail_keeps_complete_replies_sharded() {
    let server = two_workers();
    truncated_tail_keeps_complete_replies(server.local_addr());
    server.shutdown().unwrap();
}
