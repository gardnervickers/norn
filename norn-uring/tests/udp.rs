#![cfg(target_os = "linux")]

use bytes::{Bytes, BytesMut};
use norn_uring::bufring::BufRing;
use norn_uring::net::UdpSocket;

mod util;

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
fn drop_bufring_outside_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let ring = util::with_test_env(|| async {
        let ring = BufRing::builder(3).buf_cnt(8).buf_len(1024).build()?;
        Ok(ring)
    })?;

    drop(ring);
    Ok(())
}
