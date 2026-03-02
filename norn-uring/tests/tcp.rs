#![cfg(target_os = "linux")]

use std::io;
use std::net::SocketAddr;
use std::pin::pin;

use bytes::BytesMut;
use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::bufring::BufRing;
use norn_uring::net::{TcpListener, TcpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod util;

#[test]
fn incoming_connections() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        // Bind
        let listener = TcpListener::bind("0.0.0.0:9090".parse()?, 32).await?;

        // Connect
        let handle = spawn(async {
            let _ = TcpSocket::connect("0.0.0.0:9090".parse().unwrap()).await?;
            io::Result::Ok(())
        });

        let mut incoming = pin!(listener.incoming());
        let next = incoming.next().await.unwrap()?;
        next.close().await?;
        handle.await??;

        Ok(())
    })
}

#[test]
fn single_accept_connection() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;

        let handle = spawn(async move { TcpSocket::connect(addr).await });

        let (socket, peer_addr) = listener.accept().await?;
        let client = handle.await??;
        assert_eq!(peer_addr, client.local_addr()?);

        socket.close().await?;
        client.close().await?;

        Ok(())
    })
}

#[test]
fn echo() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let server = EchoServer::new().await?;

        let addr = server.local_addr()?;
        spawn(server.run()).detach();
        let conn = TcpSocket::connect(addr).await?;

        // Create a 128KB buffer containing the string "hello" repeated.
        let mut buf = Vec::with_capacity(128 * 1024);
        for _ in 0..128 {
            buf.extend_from_slice(b"hello");
        }
        let (reader, writer) = conn.into_stream().owned_split();
        let mut writer = pin!(writer);
        let mut reader = pin!(reader);

        for _ in 0..10 {
            writer.write_all(&buf[..]).await?;
            writer.flush().await?;
            let mut buf2 = vec![0; buf.len()];
            reader.read_exact(&mut buf2[..]).await?;
            assert_eq!(buf, buf2);
        }

        Ok(())
    })
}

#[test]
fn recv_ring_stream_socket_reports_peer() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;

        let client_task = spawn(async move { TcpSocket::connect(addr).await });
        let (server, peer_addr) = listener.accept().await?;
        let client = client_task.await??;

        let ring = BufRing::builder(7).buf_cnt(16).buf_len(1024).build()?;
        let payload = b"hello".to_vec();
        let (send_res, payload) = client.send(payload).await;
        assert_eq!(send_res?, payload.len());

        let (buf, recv_peer) = server.recv_ring(&ring).await?;
        assert_eq!(&buf[..payload.len()], payload.as_slice());
        assert_eq!(recv_peer, peer_addr);

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn send_zc_smoke() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        if let Err(err) = client.set_zerocopy(true).await {
            let _ = client.close().await;
            if util::zerocopy_unsupported(&err) {
                return Ok(());
            }
            return Err(err.into());
        }

        let payload = b"hello-zc".to_vec();
        let expected_len = payload.len();
        let recv_task =
            spawn(async move { server.recv(BytesMut::with_capacity(expected_len)).await });
        let (send_res, sent_buf) = client.send_zc(payload).await;
        let sent = match send_res {
            Ok(sent) => sent,
            Err(err) => {
                let _ = client.close().await;
                if util::zerocopy_unsupported(&err) {
                    return Ok(());
                }
                return Err(err.into());
            }
        };
        assert_eq!(sent, sent_buf.len());

        let (recv_res, recv_buf) = recv_task.await?;
        let recv_n = recv_res?;
        assert_eq!(&recv_buf[..recv_n], sent_buf.as_slice());

        client.close().await?;
        Ok(())
    })
}

#[test]
fn send_msg_zc_smoke() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        if let Err(err) = client.set_zerocopy(true).await {
            let _ = client.close().await;
            if util::zerocopy_unsupported(&err) {
                return Ok(());
            }
            return Err(err.into());
        }

        let payload = b"hello-zc-msg".to_vec();
        let expected_len = payload.len();
        let recv_task =
            spawn(async move { server.recv(BytesMut::with_capacity(expected_len)).await });
        let (send_res, sent_buf) = client.send_msg_zc(payload, 0).await;
        let sent = match send_res {
            Ok(sent) => sent,
            Err(err) => {
                let _ = client.close().await;
                if util::zerocopy_unsupported(&err) {
                    return Ok(());
                }
                return Err(err.into());
            }
        };
        assert_eq!(sent, sent_buf.len());

        let (recv_res, recv_buf) = recv_task.await?;
        let recv_n = recv_res?;
        assert_eq!(&recv_buf[..recv_n], sent_buf.as_slice());

        client.close().await?;
        Ok(())
    })
}

async fn connected_pair() -> Result<(TcpSocket, TcpSocket), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 32).await?;
    let addr = listener.local_addr()?;

    let client_task = spawn(async move { TcpSocket::connect(addr).await });
    let (server, _) = listener.accept().await?;
    let client = client_task.await??;
    Ok((server, client))
}

struct EchoServer {
    listener: TcpListener,
}

impl EchoServer {
    async fn new() -> io::Result<Self> {
        let listener = TcpListener::bind("0.0.0.0:0".parse().unwrap(), 32).await?;
        Ok(Self { listener })
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    async fn run(self) -> io::Result<()> {
        let mut incoming = pin!(self.listener.incoming());
        while let Some(socket) = incoming.next().await {
            let socket = socket?;
            // Set small buffer size to test the blocking behavior.
            socket.set_recv_buffer_size(64).await?;
            socket.set_send_buffer_size(64).await?;
            spawn(async move {
                let (mut reader, mut writer) = socket.into_stream().owned_split();
                let mut reader = pin!(reader);
                let mut writer = pin!(writer);
                if let Err(err) = tokio::io::copy(&mut reader, &mut writer).await {
                    log::error!("error copying: {:?}", err)
                }
            })
            .detach();
        }
        Ok(())
    }
}
