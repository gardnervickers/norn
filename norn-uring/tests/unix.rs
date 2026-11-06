#![cfg(target_os = "linux")]

use std::io;
use std::os::linux::net::SocketAddrExt;
use std::os::unix::net::SocketAddr;
use std::path::Path;

use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::net::{RecvOptions, UnixDatagram, UnixListener, UnixSocket};
use norn_uring::WithOptions;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod util;

fn abstract_name(tag: &str) -> SocketAddr {
    let name = format!("norn-test-{}-{tag}", std::process::id());
    SocketAddr::from_abstract_name(name.as_bytes()).unwrap()
}

#[test]
fn stream_connects_over_a_path() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    let path = dir.join("sock");
    util::with_test_env(|| async move {
        let listener = UnixListener::bind(&path, 8).await?;
        assert_eq!(listener.local_addr()?.as_pathname(), Some(path.as_path()));

        let client_path = path.clone();
        let client = spawn(async move { UnixSocket::connect(client_path).await });
        let (server, addr) = listener.accept().await?;
        let client = client.await??;

        // The client never bound its socket, so the server sees it unnamed.
        assert!(addr.is_unnamed());
        assert!(server.peer_addr()?.is_unnamed());
        assert!(client.local_addr()?.is_unnamed());
        assert_eq!(client.peer_addr()?.as_pathname(), Some(path.as_path()));

        let (res, _) = client.send(&b"ping"[..]).await;
        assert_eq!(res?, 4);
        let (res, buf) = server.recv(vec![0u8; 4]).await;
        assert_eq!(res?, 4);
        assert_eq!(buf, b"ping");

        client.shutdown(std::net::Shutdown::Write).await?;
        let (res, _) = server.recv(vec![0u8; 4]).await;
        assert_eq!(res?, 0, "a shut-down peer reads as end of stream");
        Ok(())
    })
}

#[test]
fn stream_connects_over_an_abstract_name() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let name = abstract_name("stream");
        let listener = UnixListener::bind_addr(&name, 8).await?;
        assert_eq!(
            listener.local_addr()?.as_abstract_name(),
            name.as_abstract_name()
        );

        let client_name = name.clone();
        let client = spawn(async move { UnixSocket::connect_addr(&client_name).await });
        let (server, _) = listener.accept().await?;
        let client = client.await??;
        assert_eq!(
            client.peer_addr()?.as_abstract_name(),
            name.as_abstract_name()
        );

        let (res, _) = server.send(&b"pong"[..]).await;
        assert_eq!(res?, 4);
        let (res, buf) = client.recv(vec![0u8; 4]).await;
        assert_eq!(res?, 4);
        assert_eq!(buf, b"pong");
        Ok(())
    })
}

#[test]
fn unnamed_addresses_cannot_be_bound_or_connected_to() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, _b) = UnixSocket::pair()?;
        let unnamed = a.local_addr()?;
        assert!(unnamed.is_unnamed());

        let err = UnixListener::bind_addr(&unnamed, 8).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let err = UnixSocket::connect_addr(&unnamed).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let err = UnixDatagram::bind_addr(&unnamed).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let socket = UnixDatagram::unbound().await?;
        let (res, buf) = socket.send_to_addr(vec![1u8, 2, 3], &unnamed).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buf, [1, 2, 3], "a failed send returns its buffer");

        let long = Path::new("/").join("x".repeat(200));
        let (res, buf) = socket.send_to(vec![4u8], &long).await;
        assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buf, [4]);
        Ok(())
    })
}

#[test]
fn incoming_yields_each_connection() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let name = abstract_name("incoming");
        let listener = UnixListener::bind_addr(&name, 8).await?;

        let mut clients = Vec::new();
        for i in 0..3u8 {
            let name = name.clone();
            clients.push(spawn(async move {
                let client = UnixSocket::connect_addr(&name).await?;
                let (res, _) = client.send(vec![i]).await;
                res?;
                io::Result::Ok(client)
            }));
        }

        let mut incoming = std::pin::pin!(listener.incoming());
        let mut seen = Vec::new();
        for _ in 0..3 {
            let server = incoming.next().await.expect("stream ended")?;
            let (res, buf) = server.recv(vec![0u8; 1]).await;
            assert_eq!(res?, 1);
            seen.push(buf[0]);
        }
        seen.sort_unstable();
        assert_eq!(seen, [0, 1, 2]);
        for client in clients {
            client.await??;
        }
        Ok(())
    })
}

#[test]
fn stream_receive_options_apply_to_one_request() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, b) = UnixSocket::pair()?;
        a.send(&b"hel"[..]).await.0?;
        a.send(&b"lo"[..]).await.0?;

        let peek = RecvOptions::new().peek(true);
        let (res, buf) = b.recv(vec![0u8; 5]).options(peek).await;
        assert_eq!(res?, 5);
        assert_eq!(buf, b"hello");

        // A stream receive rejects `trunc`, and `peek` with `wait_all`, and
        // returns the buffer untouched.
        let rejected = [
            RecvOptions::new().trunc(true),
            RecvOptions::new().peek(true).wait_all(true),
        ];
        for opts in rejected {
            let (res, buf) = b.recv(vec![7u8; 5]).options(opts).await;
            assert_eq!(res.unwrap_err().kind(), io::ErrorKind::InvalidInput);
            assert_eq!(buf, [7; 5]);
        }

        // `wait_all` alone waits for the whole buffer.
        let waiter = spawn(async move {
            let (res, buf) = b
                .recv(vec![0u8; 8])
                .options(RecvOptions::new().wait_all(true))
                .await;
            res.map(|n| buf[..n].to_vec())
        });
        norn_uring::noop().await?;
        a.send(&b"!!!"[..]).await.0?;
        assert_eq!(waiter.await??, b"hello!!!");
        Ok(())
    })
}

#[test]
fn unix_stream_reads_and_writes() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, b) = UnixSocket::pair()?;
        let mut a = a.into_stream();
        let (mut reader, mut writer) = b.into_stream().owned_split();

        // Larger than the default socket buffer, so writes wait for reads.
        let payload: Vec<u8> = (0..1 << 20).map(|i| i as u8).collect();
        let expected = payload.clone();
        let echo = spawn(async move {
            let mut buf = vec![0u8; 1 << 20];
            reader.read_exact(&mut buf).await?;
            writer.write_all(&buf).await?;
            writer.shutdown().await?;
            io::Result::Ok(())
        });

        let write = spawn(async move {
            a.write_all(&payload).await?;
            let mut echoed = Vec::new();
            a.read_to_end(&mut echoed).await?;
            io::Result::Ok(echoed)
        });
        let echoed = write.await??;
        echo.await??;
        assert_eq!(echoed, expected);
        Ok(())
    })
}

#[test]
fn unix_stream_writes_vectored() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, b) = UnixSocket::pair()?;
        let mut a = a.into_stream();
        let mut b = b.into_stream();
        let bufs = [
            io::IoSlice::new(b""),
            io::IoSlice::new(b"ab"),
            io::IoSlice::new(b"cd"),
        ];
        let n = a.write_vectored(&bufs).await?;
        assert_eq!(n, 4);
        let mut buf = [0u8; 4];
        b.read_exact(&mut buf).await?;
        assert_eq!(&buf, b"abcd");
        Ok(())
    })
}

#[test]
fn stream_write_to_a_closed_peer_fails() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, b) = UnixSocket::pair()?;
        b.close().await?;
        let mut a = a.into_stream();
        let err = a.write_all(b"payload").await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        Ok(())
    })
}

#[test]
fn datagrams_carry_their_source_address() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    let server_path = dir.join("server");
    let client_path = dir.join("client");
    util::with_test_env(|| async move {
        let server = UnixDatagram::bind(&server_path).await?;
        let client = UnixDatagram::bind(&client_path).await?;
        assert_eq!(
            server.local_addr()?.as_pathname(),
            Some(server_path.as_path())
        );

        // Queued before the receive: the nonblocking fast path.
        let (res, _) = client.send_to(&b"one"[..], &server_path).await;
        assert_eq!(res?, 3);
        let (res, buf) = server.recv_from(vec![0u8; 8]).await;
        let (n, from) = res?;
        assert_eq!(&buf[..n], b"one");
        assert_eq!(from.as_pathname(), Some(client_path.as_path()));

        // A reply to the reported address.
        let (res, _) = server.send_to_addr(&b"two"[..], &from).await;
        assert_eq!(res?, 3);
        let (res, buf) = client.recv_from(vec![0u8; 8]).await;
        let (n, from) = res?;
        assert_eq!(&buf[..n], b"two");
        assert_eq!(from.as_pathname(), Some(server_path.as_path()));

        // An unbound sender, received through io_uring because nothing is
        // queued when the receive starts.
        let unbound = UnixDatagram::unbound().await?;
        let recv = spawn(async move {
            let (res, buf) = server.recv_from(vec![0u8; 8]).await;
            res.map(|(n, from)| (buf[..n].to_vec(), from))
        });
        norn_uring::noop().await?;
        let (res, _) = unbound.send_to(&b"three"[..], &server_path).await;
        assert_eq!(res?, 5);
        let (data, from) = recv.await??;
        assert_eq!(data, b"three");
        assert!(from.is_unnamed());
        Ok(())
    })
}

#[test]
fn datagram_options_apply_to_one_request() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (a, b) = UnixDatagram::pair()?;
        assert!(a.peer_addr()?.is_unnamed());

        a.send(&b"datagram"[..]).await.0?;

        // A truncated receive reports the full length and leaves it queued
        // when peeking.
        let opts = RecvOptions::new().peek(true).trunc(true);
        let (res, buf) = b.recv(vec![0u8; 4]).options(opts).await;
        assert_eq!(res?, 8);
        assert_eq!(buf, b"data");

        let (res, buf) = b.recv_from(vec![0u8; 16]).options(RecvOptions::new()).await;
        let (n, from) = res?;
        assert_eq!(&buf[..n], b"datagram");
        assert!(from.is_unnamed());

        // Each datagram keeps its boundary.
        a.send(&b"x"[..]).await.0?;
        a.send(&b"yz"[..]).await.0?;
        let (res, _) = b.recv(vec![0u8; 16]).await;
        assert_eq!(res?, 1);
        let (res, _) = b.recv(vec![0u8; 16]).await;
        assert_eq!(res?, 2);
        Ok(())
    })
}

#[test]
fn connected_datagrams_over_an_abstract_name() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let name = abstract_name("dgram");
        let server = UnixDatagram::bind_addr(&name).await?;
        let client = UnixDatagram::unbound().await?;
        client.connect_addr(&name).await?;
        assert_eq!(
            client.peer_addr()?.as_abstract_name(),
            name.as_abstract_name()
        );

        let (res, _) = client.send(&b"hi"[..]).await;
        assert_eq!(res?, 2);
        let (res, buf) = server.recv_from(vec![0u8; 4]).await;
        let (n, from) = res?;
        assert_eq!(&buf[..n], b"hi");
        assert!(from.is_unnamed());
        Ok(())
    })
}

/// Bind a raw socket to a path that fills all of `sun_path`, with no NUL,
/// which `std` cannot represent.
fn bind_full_length_path(
    dir: &Path,
    ty: libc::c_int,
) -> io::Result<(std::os::fd::OwnedFd, std::path::PathBuf)> {
    use std::os::fd::FromRawFd;
    use std::os::unix::ffi::OsStrExt;

    let mut addr: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let prefix = dir.as_os_str().as_bytes();
    let path_len = addr.sun_path.len();
    assert!(
        prefix.len() + 2 < path_len,
        "test directory path is too long"
    );
    let mut bytes = prefix.to_vec();
    bytes.push(b'/');
    bytes.resize(path_len, b'p');
    addr.sun_family = libc::AF_UNIX as libc::sa_family_t;
    for (dst, src) in addr.sun_path.iter_mut().zip(&bytes) {
        *dst = *src as libc::c_char;
    }
    let fd = unsafe { libc::socket(libc::AF_UNIX, ty | libc::SOCK_CLOEXEC, 0) };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let fd = unsafe { std::os::fd::OwnedFd::from_raw_fd(fd) };
    let rc = unsafe {
        libc::bind(
            std::os::fd::AsRawFd::as_raw_fd(&fd),
            (&addr as *const libc::sockaddr_un).cast(),
            std::mem::size_of::<libc::sockaddr_un>() as libc::socklen_t,
        )
    };
    if rc < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok((
        fd,
        std::path::PathBuf::from(std::ffi::OsStr::from_bytes(&bytes)),
    ))
}

#[test]
fn peers_bound_to_a_full_length_path_are_reported_unnamed() -> Result<(), Box<dyn std::error::Error>>
{
    use std::os::fd::AsRawFd;

    let dir = util::ThreadNameTestDir::new();
    let dir_path = dir.to_path_buf();
    util::with_test_env(|| async move {
        // A datagram from such a sender is still received.
        let server_path = dir_path.join("server");
        let server = UnixDatagram::bind(&server_path).await?;
        let (sender, _) = bind_full_length_path(&dir_path, libc::SOCK_DGRAM)?;
        let std_sender = std::os::unix::net::UnixDatagram::from(sender);
        std_sender.send_to(b"long", &server_path)?;
        let (res, buf) = server.recv_from(vec![0u8; 8]).await;
        let (n, from) = res?;
        assert_eq!(&buf[..n], b"long");
        assert!(from.is_unnamed());

        // A connection from such a client is still accepted.
        let listener_path = dir_path.join("listener");
        let listener = UnixListener::bind(&listener_path, 8).await?;
        let client_dir = dir_path.join("c");
        std::fs::create_dir_all(&client_dir)?;
        let (client, _) = bind_full_length_path(&client_dir, libc::SOCK_STREAM)?;
        let client = std::os::unix::net::UnixStream::from(client);
        let addr = socket2::SockAddr::unix(&listener_path)?;
        let rc = unsafe { libc::connect(client.as_raw_fd(), addr.as_ptr().cast(), addr.len()) };
        assert_eq!(rc, 0, "{}", io::Error::last_os_error());
        let (server, peer) = listener.accept().await?;
        assert!(peer.is_unnamed());
        let (res, _) = server.send(&b"ok"[..]).await;
        assert_eq!(res?, 2);
        Ok(())
    })
}
