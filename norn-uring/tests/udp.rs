#![cfg(target_os = "linux")]

use std::pin::pin;

use bytes::{Bytes, BytesMut};
use futures_util::StreamExt;
use norn_uring::bufring::{BufRing, BufRingBufBundle};
use norn_uring::net::UdpSocket;

mod util;

fn flatten_bundle(bundle: &BufRingBufBundle) -> Vec<u8> {
    bundle
        .iter()
        .flat_map(|chunk| chunk.iter().copied())
        .collect()
}

#[test]
fn send_recv() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        // Send hello to s2
        let buf = Bytes::from_static(b"hello");
        let (res, buf) = s1.send_to(buf, s2.local_addr()?).await;
        // Assert that we sent 5 bytes
        assert_eq!(buf.len(), res?);

        // Receive hello on s2
        let buf = BytesMut::with_capacity(5);
        let (res, buf) = s2.recv_from(buf).await;
        let (n, addr) = res?;
        // Assert that we received 5 bytes
        assert_eq!(5, n);
        // Assert that we received from the correct address
        assert_eq!(s1.local_addr()?, addr);
        // Assert that the message is correct
        assert_eq!(b"hello", &buf[..n]);

        Ok(())
    })
}

#[test]
fn connected_send_recv() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        assert_eq!(s1.peer_addr()?, s2.local_addr()?);
        assert_eq!(s2.peer_addr()?, s1.local_addr()?);

        let req = Bytes::from_static(b"ping");
        let (res, req) = s1.send(req).await;
        assert_eq!(req.len(), res?);

        let (res, buf) = s2.recv(BytesMut::with_capacity(16)).await;
        let n = res?;
        assert_eq!(&buf[..n], b"ping");

        let rsp = Bytes::from_static(b"pong");
        let (res, rsp) = s2.send(rsp).await;
        assert_eq!(rsp.len(), res?);

        let (res, buf) = s1.recv(BytesMut::with_capacity(16)).await;
        let n = res?;
        assert_eq!(&buf[..n], b"pong");

        Ok(())
    })
}

#[test]
fn connected_send_recv_zc() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        if let Err(err) = s1.set_zerocopy(true).await {
            s1.close().await?;
            s2.close().await?;
            if util::zerocopy_unsupported(&err) {
                return Ok(());
            }
            return Err(err.into());
        }

        let recv_task =
            norn_executor::spawn(async move { s2.recv(BytesMut::with_capacity(64)).await });
        let payload = Bytes::from_static(b"udp-zc");
        let (res, sent) = s1.send_zc(payload).await;
        let sent_n = match res {
            Ok(n) => n,
            Err(err) => {
                s1.close().await?;
                if util::zerocopy_unsupported(&err) {
                    return Ok(());
                }
                return Err(err.into());
            }
        };
        assert_eq!(sent_n, sent.len());

        let (res, buf) = recv_task.await?;
        let n = res?;
        assert_eq!(&buf[..n], b"udp-zc");

        s1.close().await?;
        Ok(())
    })
}

#[test]
fn connected_send_recv_msg_zc() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        if let Err(err) = s1.set_zerocopy(true).await {
            s1.close().await?;
            s2.close().await?;
            if util::zerocopy_unsupported(&err) {
                return Ok(());
            }
            return Err(err.into());
        }

        let recv_task =
            norn_executor::spawn(async move { s2.recv(BytesMut::with_capacity(64)).await });
        let payload = Bytes::from_static(b"udp-zc-msg");
        let (res, sent) = s1.send_msg_zc(payload, 0).await;
        let sent_n = match res {
            Ok(n) => n,
            Err(err) => {
                s1.close().await?;
                if util::zerocopy_unsupported(&err) {
                    return Ok(());
                }
                return Err(err.into());
            }
        };
        assert_eq!(sent_n, sent.len());

        let (res, buf) = recv_task.await?;
        let n = res?;
        assert_eq!(&buf[..n], b"udp-zc-msg");

        s1.close().await?;
        Ok(())
    })
}

#[test]
fn send_recv_msg() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        let (res, sent) = s1
            .send_msg(Bytes::from_static(b"hello"), Some(s2.local_addr()?), 0)
            .await;
        assert_eq!(res?, sent.len());

        let (res, buf) = s2.recv_msg(BytesMut::with_capacity(16), 0).await;
        let (n, addr) = res?;
        assert_eq!(addr, s1.local_addr()?);
        assert_eq!(&buf[..n], b"hello");

        Ok(())
    })
}

#[test]
fn send_recv_ring() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(1).buf_cnt(32).buf_len(1024 * 16).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        // Send hello to s2
        let buf = Bytes::from_static(b"hello");
        s1.send_to(buf, s2.local_addr()?).await.0?;

        // Receive hello on s2 using the ring
        let (buf, addr) = s2.recv_from_ring(&ring).await?;
        // Assert that we received from the correct address
        assert_eq!(s1.local_addr()?, addr);
        // Assert that the message is correct
        assert_eq!(b"hello", &buf[..5]);

        Ok(())
    })
}

#[test]
fn send_recv_ring_multi_msg() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(4).buf_cnt(32).buf_len(2048).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = s1.local_addr()?;

        let mut recv = pin!(s2.recv_from_ring_multi(&ring));
        let messages: [&[u8]; 3] = [b"alpha", b"beta", b"gamma"];
        for message in messages {
            let payload = Bytes::copy_from_slice(message);
            s1.send_to(payload, s2.local_addr()?).await.0?;
            let (buf, addr) = recv.next().await.expect("multishot stream ended")?;
            assert_eq!(addr, sender);
            assert_eq!(&buf[..], message);
        }

        Ok(())
    })
}

#[test]
fn connected_send_recv_ring_multi() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(5).buf_cnt(32).buf_len(2048).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        let mut recv = pin!(s2.recv_ring_multi(&ring));
        let messages: [&[u8]; 3] = [b"one", b"two", b"three"];
        for message in messages {
            s1.send(Bytes::copy_from_slice(message)).await.0?;
            let buf = recv.next().await.expect("multishot stream ended")?;
            assert_eq!(&buf[..], message);
        }

        Ok(())
    })
}

#[test]
fn connected_send_recv_from_ring_multi() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(8).buf_cnt(32).buf_len(2048).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        let mut recv = pin!(s2.recv_from_ring_multi(&ring));
        let payload = Bytes::from_static(b"peer-fallback");
        s1.send(payload.clone()).await.0?;
        let (buf, addr) = recv.next().await.expect("multishot stream ended")?;
        assert_eq!(addr, s1.local_addr()?);
        assert_eq!(&buf[..], payload.as_ref());

        Ok(())
    })
}

#[test]
fn connected_send_recv_bundle() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(6).buf_cnt(64).buf_len(2048).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        let payload = Bytes::from_static(b"bundle-single");
        s1.send(payload.clone()).await.0?;

        let bundle = match s2.recv_bundle(&ring).await {
            Ok(bundle) => bundle,
            Err(err) if util::recv_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert_eq!(bundle.len(), payload.len());
        assert_eq!(flatten_bundle(&bundle), payload.as_ref());

        Ok(())
    })
}

#[test]
fn connected_send_recv_bundle_multi() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = BufRing::builder(7).buf_cnt(64).buf_len(2048).build()?;
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        s1.connect(s2.local_addr()?).await?;
        s2.connect(s1.local_addr()?).await?;

        let mut recv = pin!(s2.recv_bundle_multi(&ring));
        let messages: [&[u8]; 3] = [b"bundle-a", b"bundle-b", b"bundle-c"];
        for message in messages {
            s1.send(Bytes::copy_from_slice(message)).await.0?;
            let bundle = match recv.next().await.expect("multishot stream ended") {
                Ok(bundle) => bundle,
                Err(err) if util::recv_bundle_unsupported(&err) => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            assert_eq!(flatten_bundle(&bundle), message);
        }

        Ok(())
    })
}

struct UdpEchoServer {
    socket: UdpSocket,
}

impl UdpEchoServer {
    async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let socket = UdpSocket::bind("0.0.0.0:0".parse()?).await?;
        Ok(Self { socket })
    }

    fn local_addr(&self) -> Result<std::net::SocketAddr, Box<dyn std::error::Error>> {
        Ok(self.socket.local_addr()?)
    }

    async fn run(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let bufring = BufRing::builder(1)
            .buf_cnt(32)
            .buf_len(u16::MAX as usize)
            .build()?;
        loop {
            let (buf, origin) = self.socket.recv_from_ring(&bufring).await?;
            let buf = Bytes::copy_from_slice(&buf[..]);
            let (res, _) = self.socket.send_to(buf, origin).await;
            res?;
        }
    }
}

#[test]
fn echo_server_1k_requests() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let mut server = UdpEchoServer::new().await?;
        let addr = server.local_addr()?;

        norn_executor::spawn(async move {
            server.run().await.unwrap();
        })
        .detach();

        let s1 = UdpSocket::bind("0.0.0.0:0".parse()?).await?;
        for i in 0..1024 {
            let payload = format!("hello-{}", i);
            let payload = payload.as_bytes().to_vec();
            s1.send_to(payload.clone(), addr).await.0?;
            let (res, buf) = s1.recv_from(BytesMut::with_capacity(1024)).await;
            res?;
            assert_eq!(payload, buf)
        }

        Ok(())
    })
}

#[test]
fn buffer_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let mut server = UdpEchoServer::new().await?;
        let addr = server.local_addr()?;
        norn_executor::spawn(async move {
            server.run().await.unwrap();
        })
        .detach();

        // Create a bufring of 2 buffers of 64k each.
        let bufring = BufRing::builder(2)
            .buf_cnt(2)
            .buf_len(u16::MAX as usize)
            .build()?;

        let s1 = UdpSocket::bind("0.0.0.0:0".parse()?).await?;
        s1.send_to(Bytes::from_static(b"hello"), addr).await.0?;
        let b1 = s1.recv_from_ring(&bufring).await?;
        s1.send_to(Bytes::from_static(b"hello"), addr).await.0?;
        let _b2 = s1.recv_from_ring(&bufring).await?;

        // The third recv should fail with an error
        let res = s1.recv_from_ring(&bufring).await;
        assert!(res.is_err());

        s1.send_to(Bytes::from_static(b"hello"), addr).await.0?;
        // Dropping the buffers should allow the third recv to succeed
        drop(b1);
        s1.recv_from_ring(&bufring).await?;

        s1.close().await?;

        Ok(())
    })
}

#[test]
fn close_socket() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("0.0.0.0:0".parse()?).await?;
        s1.close().await?;
        Ok(())
    })
}

#[test]
fn poll_readiness_smoke() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let s1 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let s2 = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        let writable = s1.poll_readiness::<false>(libc::POLLOUT as u32).await?;
        assert!(writable.is_writeable() || writable.is_error());

        s1.send_to(Bytes::from_static(b"x"), s2.local_addr()?)
            .await
            .0?;
        let readable = s2.poll_readiness::<false>(libc::POLLIN as u32).await?;
        assert!(readable.is_readable());

        let (res, _) = s2.recv_from(BytesMut::with_capacity(16)).await;
        res?;
        Ok(())
    })
}

#[test]
fn drop_bufring_outside_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let ring = util::with_test_env(|| async {
        let ring = BufRing::builder(3).buf_cnt(8).buf_len(1024).build()?;
        Ok(ring)
    })?;

    drop(ring);
    Ok(())
}
