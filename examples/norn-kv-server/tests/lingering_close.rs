//! A connection that fails or executes QUIT while the peer is still sending
//! must not reset the connection and discard replies the server already
//! flushed.
#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::os::fd::FromRawFd;
use std::time::Duration;

use norn_kv_server::handler::HandlerConfig;
use norn_kv_server::server::{RecvMode, ServerConfig};
use norn_kv_server::sharded::{ShardedServer, ShardedServerConfig};

const HEADER_LEN: usize = 24;
const OP_GET: u8 = 0x00;
const OP_SET: u8 = 0x01;
const OP_QUIT: u8 = 0x07;
const GETS: u32 = 4;

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

/// Connect with a small receive buffer, so most of the replies wait in the
/// server's send queue while the client is not reading.
fn connect_small_rcvbuf(address: SocketAddr) -> TcpStream {
    let SocketAddr::V4(address) = address else {
        panic!("test servers listen on IPv4");
    };
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_CLOEXEC, 0);
        assert!(fd >= 0, "socket: {}", std::io::Error::last_os_error());
        let stream = TcpStream::from_raw_fd(fd);
        let rcvbuf: libc::c_int = 4096;
        let res = libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_RCVBUF,
            (&rcvbuf as *const libc::c_int).cast(),
            std::mem::size_of::<libc::c_int>() as libc::socklen_t,
        );
        assert_eq!(res, 0, "setsockopt: {}", std::io::Error::last_os_error());
        let sockaddr = libc::sockaddr_in {
            sin_family: libc::AF_INET as libc::sa_family_t,
            sin_port: address.port().to_be(),
            sin_addr: libc::in_addr {
                s_addr: u32::from(*address.ip()).to_be(),
            },
            sin_zero: [0; 8],
        };
        let res = libc::connect(
            fd,
            (&sockaddr as *const libc::sockaddr_in).cast(),
            std::mem::size_of::<libc::sockaddr_in>() as libc::socklen_t,
        );
        assert_eq!(res, 0, "connect: {}", std::io::Error::last_os_error());
        stream
    }
}

/// What ends the connection after the pipelined GETs.
#[derive(Clone, Copy)]
enum Ending {
    /// A SET header declaring a body far above `max_body_len`.
    Oversized,
    /// A QUIT, answered with opaque 2.
    Quit,
}

/// Pipeline a large SET, GETs of it, and `ending` followed by megabytes of
/// unread input, reading nothing until the server has ended the connection.
/// Returns the opaque of every complete reply.
fn replies_after(address: SocketAddr, ending: Ending) -> Vec<u32> {
    let mut stream = connect_small_rcvbuf(address);
    let mut wire = request(OP_SET, b"big", &[0; 8], &vec![b'v'; 60_000], 1);
    for opaque in 0..GETS {
        wire.extend(request(OP_GET, b"big", &[], &[], 100 + opaque));
    }
    match ending {
        Ending::Oversized => {
            let mut oversized = request(OP_SET, b"big", &[0; 8], &[], 2);
            oversized[8..12].copy_from_slice(&50_000_000_u32.to_be_bytes());
            wire.extend(&oversized[..HEADER_LEN]);
        }
        Ending::Quit => wire.extend(request(OP_QUIT, &[], &[], &[], 2)),
    }

    let mut writer = stream.try_clone().unwrap();
    std::thread::spawn(move || {
        if writer.write_all(&wire).is_err() {
            return;
        }
        let chunk = vec![b'j'; 64 * 1024];
        for _ in 0..128 {
            if writer.write_all(&chunk).is_err() {
                return;
            }
        }
    });

    // Let the server fail the connection while replies are still queued.
    std::thread::sleep(Duration::from_millis(500));
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    let mut received = Vec::new();
    let _ = stream.read_to_end(&mut received);

    let mut opaques = Vec::new();
    let mut rest = received.as_slice();
    while rest.len() >= HEADER_LEN {
        let body_len = u32::from_be_bytes(rest[8..12].try_into().unwrap()) as usize;
        if rest.len() < HEADER_LEN + body_len {
            break;
        }
        opaques.push(u32::from_be_bytes(rest[12..16].try_into().unwrap()));
        rest = &rest[HEADER_LEN + body_len..];
    }
    opaques
}

fn expected(ending: Ending) -> Vec<u32> {
    let mut opaques: Vec<_> = std::iter::once(1).chain(100..100 + GETS).collect();
    if let Ending::Quit = ending {
        opaques.push(2);
    }
    opaques
}

fn check_multishot(ending: Ending) {
    let replies = replies_after(single_worker(RecvMode::Multishot), ending);
    assert_eq!(replies, expected(ending));
}

fn check_exact(ending: Ending) {
    let replies = replies_after(single_worker(RecvMode::Exact), ending);
    assert_eq!(replies, expected(ending));
}

fn check_sharded(ending: Ending) {
    let server = ShardedServer::start(ShardedServerConfig {
        listen: "127.0.0.1:0".parse().unwrap(),
        ..ShardedServerConfig::default()
    })
    .unwrap();
    let replies = replies_after(server.local_addr(), ending);
    server.shutdown().unwrap();
    assert_eq!(replies, expected(ending));
}

fn single_worker(recv_mode: RecvMode) -> SocketAddr {
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

#[test]
fn failure_keeps_flushed_replies_multishot() {
    check_multishot(Ending::Oversized);
}

#[test]
fn failure_keeps_flushed_replies_exact() {
    check_exact(Ending::Oversized);
}

#[test]
fn failure_keeps_flushed_replies_sharded() {
    check_sharded(Ending::Oversized);
}

#[test]
fn quit_keeps_flushed_replies_multishot() {
    check_multishot(Ending::Quit);
}

#[test]
fn quit_keeps_flushed_replies_exact() {
    check_exact(Ending::Quit);
}

#[test]
fn quit_keeps_flushed_replies_sharded() {
    check_sharded(Ending::Quit);
}
