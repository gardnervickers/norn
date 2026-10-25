#![cfg(target_os = "linux")]

use futures_core::Stream;
use futures_util::StreamExt;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket, UdpSocket};
use std::io;
use std::pin::Pin;
use std::task::Context;
mod util;

type HeldReceiveStream = Pin<Box<dyn Stream<Item = io::Result<Box<dyn std::any::Any>>>>>;

// Drive the kernel to terminal ENOBUFS while the selected buffer is held.
// A waiting stream must neither yield an error/EOF nor schedule itself again.
async fn assert_waiting<S: Stream + ?Sized>(
    mut stream: Pin<&mut S>,
) -> futures_test::task::AwokenCount {
    let (waker, count) = futures_test::task::new_count_waker();
    let mut cx = Context::from_waker(&waker);
    for _ in 0..16 {
        assert!(stream.as_mut().poll_next(&mut cx).is_pending());
        norn_uring::noop().await;
    }
    let wakes = count.get();
    for _ in 0..64 {
        assert!(stream.as_mut().poll_next(&mut cx).is_pending());
    }
    assert_eq!(count.get(), wakes, "empty ring must not self-wake");
    count
}

async fn tcp_pair() -> io::Result<(TcpSocket, TcpSocket)> {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8).await?;
    let addr = listener.local_addr()?;
    let connect = norn_executor::spawn(async move { TcpSocket::connect(addr).await });
    let (server, _) = listener.accept().await?;
    let client = connect.await.unwrap()?;
    Ok((server, client))
}

#[test]
fn tcp_multishot_recovers_after_buffer_return() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        let ring = RecvBufRing::builder(1).buf_cnt(1).buf_len(1024).build()?;
        let mut stream = Box::pin(server.recv_ring_multi(&ring));
        client.send(b"first".to_vec()).await.0?;
        let held = stream.next().await.unwrap()?;
        assert_eq!(&held[..], b"first");
        client.send(b"second".to_vec()).await.0?;
        let count = assert_waiting(stream.as_mut()).await;
        let wakes = count.get();
        assert_eq!(&held[..], b"first");
        drop(held);
        assert!(count.get() > wakes, "buffer return must wake the stream");
        let next = stream.next().await.expect("stream ended on exhaustion")?;
        assert_eq!(&next[..], b"second");
        drop(next);
        client.send(b"third".to_vec()).await.0?;
        assert_eq!(&stream.next().await.unwrap()?[..], b"third");
        Ok(())
    })
}

#[test]
fn tcp_bundle_recovers_after_buffer_return() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        let ring = RecvBufRing::builder(1).buf_cnt(1).buf_len(1024).build()?;
        let mut stream = Box::pin(server.recv_bundle_multi(&ring));
        client.send(b"first".to_vec()).await.0?;
        let held = stream.next().await.unwrap()?;
        assert_eq!(held.len(), 5);
        client.send(b"second".to_vec()).await.0?;
        let count = assert_waiting(stream.as_mut()).await;
        let wakes = count.get();
        drop(held);
        assert!(count.get() > wakes, "buffer return must wake the stream");
        let next = stream
            .next()
            .await
            .expect("bundle stream ended on exhaustion")?;
        let bytes: Vec<_> = next.iter().flat_map(|buf| buf.iter().copied()).collect();
        assert_eq!(bytes, b"second");
        Ok(())
    })
}

#[test]
fn udp_multishot_recovers_all_variants() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        for (variant, early_return) in
            (0..3).flat_map(|variant| [false, true].map(|early| (variant, early)))
        {
            let server = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
            let client = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
            server.connect(client.local_addr()?).await?;
            client.connect(server.local_addr()?).await?;
            let ring = RecvBufRing::builder(variant + 1)
                .buf_cnt(1)
                .buf_len(1024)
                .build()?;
            // Erase buffer type while retaining the selected buffer until explicitly dropped.
            let mut stream: HeldReceiveStream = match variant {
                0 => Box::pin(
                    server
                        .recv_ring_multi(&ring)
                        .map(|r| r.map(|b| Box::new(b) as Box<dyn std::any::Any>)),
                ),
                1 => Box::pin(
                    server
                        .recv_bundle_multi(&ring)
                        .map(|r| r.map(|b| Box::new(b) as Box<dyn std::any::Any>)),
                ),
                _ => Box::pin(
                    server
                        .recv_from_ring_multi(&ring)
                        .map(|r| r.map(|b| Box::new(b) as Box<dyn std::any::Any>)),
                ),
            };
            client.send(b"first".to_vec()).await.0?;
            let held = stream.next().await.unwrap()?;
            client.send(b"second".to_vec()).await.0?;
            if early_return {
                // Terminal ENOBUFS has been reaped, but the stream has not yet
                // consumed it or registered its buffer-return waiter.
                for _ in 0..16 {
                    norn_uring::noop().await;
                }
                drop(held);
            } else {
                let count = assert_waiting(stream.as_mut()).await;
                let wakes = count.get();
                drop(held);
                assert!(count.get() > wakes, "buffer return must wake the stream");
            }
            let next = stream
                .next()
                .await
                .expect("UDP stream ended on exhaustion")?;
            match variant {
                0 => assert_eq!(
                    &next.downcast::<norn_uring::bufring::BufRingBuf>().unwrap()[..],
                    b"second"
                ),
                1 => {
                    let bundle = next
                        .downcast::<norn_uring::bufring::BufRingBufBundle>()
                        .unwrap();
                    let bytes: Vec<_> = bundle.iter().flat_map(|buf| buf.iter().copied()).collect();
                    assert_eq!(bytes, b"second");
                }
                _ => {
                    let (buffer, peer) = *next
                        .downcast::<(norn_uring::net::RecvMsgRingBuf, std::net::SocketAddr)>()
                        .unwrap();
                    assert_eq!(buffer.payload(), b"second");
                    assert_eq!(peer, client.local_addr()?);
                }
            }
        }
        Ok(())
    })
}

#[test]
fn udp_recvmsg_rearms_after_successful_terminal_cqe() -> Result<(), Box<dyn std::error::Error>> {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 2)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let server = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let ring = RecvBufRing::builder(1).buf_cnt(64).buf_len(1024).build()?;
        let mut stream = Box::pin(server.recv_from_ring_multi(&ring));
        sender.send_to(b"first", server.local_addr()?)?;
        drop(stream.next().await.unwrap()?);
        for index in 0..32u8 {
            let payload = if index % 3 == 0 {
                &[][..]
            } else {
                std::slice::from_ref(&index)
            };
            sender.send_to(payload, server.local_addr()?)?;
        }
        for index in 0..32u8 {
            let (buffer, peer) = stream.next().await.expect("healthy UDP stream ended")?;
            let expected = if index % 3 == 0 {
                &[][..]
            } else {
                std::slice::from_ref(&index)
            };
            assert_eq!(buffer.payload(), expected);
            assert_eq!(peer, sender.local_addr()?);
        }
        sender.send_to(b"still usable", server.local_addr()?)?;
        assert_eq!(stream.next().await.unwrap()?.0.payload(), b"still usable");
        Ok(())
    })
}

#[test]
fn tcp_reset_remains_terminal_for_both_receive_streams() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        for bundle in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
            let client = std::net::TcpStream::connect(listener.local_addr()?)?;
            let (server, _) = listener.accept().await?;
            let ring = RecvBufRing::builder(1).buf_cnt(2).buf_len(1024).build()?;
            let mut stream: Pin<Box<dyn Stream<Item = io::Result<()>>>> = if bundle {
                Box::pin(server.recv_bundle_multi(&ring).map(|item| item.map(drop)))
            } else {
                Box::pin(server.recv_ring_multi(&ring).map(|item| item.map(drop)))
            };
            socket2::SockRef::from(&client).set_linger(Some(std::time::Duration::ZERO))?;
            drop(client);
            let error = stream
                .next()
                .await
                .expect("reset must yield an error")
                .unwrap_err();
            assert_eq!(error.raw_os_error(), Some(libc::ECONNRESET));
            assert!(stream.next().await.is_none());
            server.close().await?;
        }
        Ok(())
    })
}

#[test]
fn udp_recvmsg_drop_recycles_unconsumed_completions() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let server = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = std::net::UdpSocket::bind("127.0.0.1:0")?;
        let ring = RecvBufRing::builder(1).buf_cnt(2).buf_len(1024).build()?;
        let mut stream = Box::pin(server.recv_from_ring_multi(&ring));
        sender.send_to(b"first", server.local_addr()?)?;
        drop(stream.next().await.unwrap()?);
        for payload in [b"queued-1", b"queued-2", b"queued-3"] {
            sender.send_to(payload, server.local_addr()?)?;
        }
        // Reap two owned completions followed by terminal ENOBUFS without
        // consuming any stream item. Dropping must recycle both selected buffers.
        for _ in 0..16 {
            norn_uring::noop().await;
        }
        drop(stream);
        let mut replacement = Box::pin(server.recv_from_ring_multi(&ring));
        let (held, _) = replacement.next().await.unwrap()?;
        assert_eq!(held.payload(), b"queued-3");
        sender.send_to(b"after cancellation", server.local_addr()?)?;
        let (second, _) = replacement.next().await.unwrap()?;
        assert_eq!(second.payload(), b"after cancellation");
        assert_eq!(held.payload(), b"queued-3");
        Ok(())
    })
}
