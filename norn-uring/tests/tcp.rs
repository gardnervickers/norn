#![cfg(target_os = "linux")]

use std::future::poll_fn;
use std::io;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::task::Poll;

use bytes::BytesMut;
use futures_core::Stream;
use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::buf::StableBuf;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{SendZcUsage, TcpListener, TcpListenerOptions, TcpSocket};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

mod util;

#[test]
fn msg_trunc_is_rejected_without_modifying_buffers_or_consuming_input(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept().await?;
        use std::io::Write;
        let mut peer = peer;

        for flags in [
            libc::MSG_TRUNC,
            libc::MSG_TRUNC | libc::MSG_PEEK,
            libc::MSG_TRUNC | libc::MSG_WAITALL,
        ] {
            for initialized in [false, true] {
                peer.write_all(b"hello")?;
                let buf = if initialized {
                    vec![0xa5; 5]
                } else {
                    Vec::with_capacity(5)
                };
                let ptr = buf.as_ptr();
                let capacity = buf.capacity();
                let (result, buf) = server.recv_with_flags(buf, flags).await;
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                assert_eq!(buf.as_ptr(), ptr);
                assert_eq!(buf.capacity(), capacity);
                assert_eq!(
                    buf,
                    if initialized {
                        vec![0xa5; 5]
                    } else {
                        Vec::new()
                    }
                );
                let (result, buf) = server.recv_with_flags(buf, libc::MSG_WAITALL).await;
                assert_eq!(result?, 5);
                assert_eq!(buf, b"hello");

                peer.write_all(b"hello")?;
                let mut buf = BytesMut::with_capacity(5);
                if initialized {
                    buf.extend_from_slice(&[0xa5; 5]);
                }
                let ptr = buf.as_ptr();
                let capacity = buf.capacity();
                let (result, buf) = server.recv_with_flags(buf, flags).await;
                assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
                assert_eq!(buf.as_ptr(), ptr);
                assert_eq!(buf.capacity(), capacity);
                assert_eq!(&buf[..], if initialized { &[0xa5; 5][..] } else { &[][..] });
                let (result, buf) = server.recv_with_flags(buf, libc::MSG_WAITALL).await;
                assert_eq!(result?, 5);
                assert_eq!(&buf[..], b"hello");
            }
        }
        server.close().await?;
        Ok(())
    })
}

#[cfg(target_pointer_width = "64")]
struct OversizedBuf(usize);

#[cfg(target_pointer_width = "64")]
// Safety: tests only use this buffer to exercise configuration rejection
// before the kernel can observe its nonzero logical length.
unsafe impl StableBuf for OversizedBuf {
    fn stable_ptr(&self) -> *const u8 {
        std::ptr::NonNull::<u8>::dangling().as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0
    }
}

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
fn incoming_rearms_multishot_accept_terminated_by_full_completion_queue(
) -> Result<(), Box<dyn std::error::Error>> {
    // Two SQ entries give a four-entry completion queue. The kernel ends a
    // multishot accept, without an error, when it cannot post another
    // completion; the stream must re-arm rather than report end of stream.
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 2)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 128).await?;
        let addr = listener.local_addr()?;
        let mut incoming = pin!(listener.incoming());

        let mut clients = vec![std::net::TcpStream::connect(addr)?];
        let mut accepted = vec![incoming.next().await.unwrap()?];
        // Completions for these are posted while the stream is not polled,
        // overflowing the completion queue.
        for _ in 0..32 {
            clients.push(std::net::TcpStream::connect(addr)?);
        }
        while accepted.len() < clients.len() {
            match incoming.next().await {
                Some(socket) => accepted.push(socket?),
                None => panic!("accept stream ended after {} connections", accepted.len()),
            }
        }
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
fn listener_bind_does_not_reuse_port_by_default() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let first = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = first.local_addr()?;

        let error = TcpListener::bind(addr, 32).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AddrInUse);

        first.close().await?;
        Ok(())
    })
}

#[test]
fn listener_options_allow_reusing_bound_port() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let options = TcpListenerOptions::new().reuse_port(true);
        let first = TcpListener::bind_with_options("127.0.0.1:0".parse()?, 32, options).await?;
        let addr = first.local_addr()?;
        let second = TcpListener::bind_with_options(addr, 32, options).await?;

        assert_eq!(second.local_addr()?, addr);

        second.close().await?;
        first.close().await?;
        Ok(())
    })
}

#[test]
fn tcp_socket_round_trips_through_uring_fd() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let server = TcpSocket::from_uring_fd(server.into_uring_fd());

        let payload = b"round-trip".to_vec();
        client.send(payload).await.0?;
        let (result, buffer) = server.recv(BytesMut::with_capacity(16)).await;
        let length = result?;
        assert_eq!(&buffer[..length], b"round-trip");

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[cfg(target_pointer_width = "64")]
#[test]
fn send_zc_rejects_buffers_larger_than_u32_max() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let (result, buffer) = server.send_zc(OversizedBuf(u32::MAX as usize + 1)).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
        assert_eq!(buffer.0, u32::MAX as usize + 1);

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn close_rejects_unpolled_operation_without_invalidating_it(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;

        let recv = server.recv(BytesMut::with_capacity(16));
        let err = server.close().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        let payload = b"still-open".to_vec();
        client.send(payload).await.0?;
        let (res, buf) = recv.await;
        let len = res?;
        assert_eq!(&buf[..len], b"still-open");

        client.close().await?;
        Ok(())
    })
}

#[test]
fn close_rejects_queued_operation_without_invalidating_it() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;

        let mut recv = pin!(server.recv(BytesMut::with_capacity(16)));
        assert!(futures_util::poll!(&mut recv).is_pending());

        let err = server.close().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        let payload = b"still-open".to_vec();
        client.send(payload).await.0?;
        let (res, buf) = recv.await;
        let len = res?;
        assert_eq!(&buf[..len], b"still-open");

        client.close().await?;
        Ok(())
    })
}

#[test]
fn close_rejects_submitted_operation_without_invalidating_it(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;

        let recv = server.recv(BytesMut::with_capacity(16));
        let recv_task = spawn(recv);

        // Completing a separate ring round trip ensures the receive was submitted
        // to the kernel before explicit close checks descriptor ownership.
        norn_uring::noop().await;
        let err = server.close().await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::WouldBlock);

        let payload = b"still-open".to_vec();
        client.send(payload).await.0?;
        let (res, buf) = recv_task.await?;
        let len = res?;
        assert_eq!(&buf[..len], b"still-open");

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
fn recv_ring_receives_payload_from_connected_peer() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;

        let client_task = spawn(async move { TcpSocket::connect(addr).await });
        let (server, peer_addr) = listener.accept().await?;
        let client = client_task.await??;

        let ring = RecvBufRing::builder(7).buf_cnt(16).buf_len(1024).build()?;
        let payload = b"hello".to_vec();
        let (send_res, payload) = client.send(payload).await;
        assert_eq!(send_res?, payload.len());

        let buf = server.recv_ring(&ring).await?;
        assert_eq!(&buf[..payload.len()], payload.as_slice());
        assert_eq!(server.peer_addr()?, peer_addr);

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn recv_ring_buf_can_be_echoed_directly() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(8).buf_cnt(1).buf_len(1024).build()?;
        let payload = b"direct-tcp-echo".to_vec();

        client.send(payload.clone()).await.0?;
        let buf = server.recv_ring(&ring).await?;
        assert_eq!(buf.as_slice(), payload.as_slice());

        let (send_result, buf) = server.send(buf).await;
        assert_eq!(send_result?, payload.len());
        drop(buf);

        let (recv_result, reply) = client.recv(BytesMut::with_capacity(payload.len())).await;
        let reply_len = recv_result?;
        assert_eq!(&reply[..reply_len], payload.as_slice());

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn recv_ring_reports_orderly_peer_close_as_empty_buffer() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(14).buf_cnt(1).buf_len(1024).build()?;
        client.close().await?;

        let buf = server.recv_ring(&ring).await?;
        assert!(buf.is_empty(), "EOF should yield an empty buffer");

        drop(buf);
        server.close().await?;
        Ok(())
    })
}

#[test]
fn recv_ring_returns_data_queued_before_peer_reset() -> Result<(), Box<dyn std::error::Error>> {
    use std::io::Write;

    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let mut peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept().await?;
        let ring = RecvBufRing::builder(15).buf_cnt(2).buf_len(1024).build()?;

        peer.write_all(b"final words")?;
        socket2::SockRef::from(&peer).set_linger(Some(std::time::Duration::ZERO))?;
        drop(peer);
        // Let loopback deliver both the payload and the RST.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let buf = server.recv_ring(&ring).await?;
        assert_eq!(&buf[..], b"final words");
        Ok(())
    })
}

#[test]
fn recv_ring_reports_eof_after_both_sides_shut_down() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept().await?;
        let ring = RecvBufRing::builder(16).buf_cnt(2).buf_len(1024).build()?;

        peer.shutdown(std::net::Shutdown::Write)?;
        server.shutdown(std::net::Shutdown::Write).await?;
        // Let the final ACK move the server socket from LAST_ACK to CLOSE.
        std::thread::sleep(std::time::Duration::from_millis(50));

        let buf = server.recv_ring(&ring).await?;
        assert!(buf.is_empty(), "EOF should yield an empty buffer");
        drop(peer);
        Ok(())
    })
}

#[test]
fn recv_ring_multi_ends_on_orderly_peer_close() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(9).buf_cnt(1).buf_len(1024).build()?;
        let mut incoming = Box::pin(server.recv_ring_multi(&ring));
        let close = spawn(async move { client.close().await });

        assert!(incoming.next().await.is_none());
        close.await??;

        server.close().await?;
        drop(incoming);
        Ok(())
    })
}

#[test]
fn recv_bundle_multi_ends_on_orderly_peer_close() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(13).buf_cnt(1).buf_len(1024).build()?;
        let mut incoming = Box::pin(server.recv_bundle_multi(&ring));
        let close = spawn(async move { client.close().await });

        let terminal = incoming.next().await;
        close.await??;
        match terminal {
            None => {}
            Some(Err(err)) if util::recv_bundle_unsupported(&err) => {}
            Some(result) => panic!("TCP EOF produced a bundle item: {result:?}"),
        }

        server.close().await?;
        drop(incoming);
        Ok(())
    })
}

#[test]
fn recv_ring_buf_can_be_echoed_with_send_zc() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(9).buf_cnt(1).buf_len(1024).build()?;

        if let Err(err) = server.set_zerocopy(true).await {
            server.close().await?;
            client.close().await?;
            if util::zerocopy_unsupported(&err) {
                return Ok(());
            }
            return Err(err.into());
        }

        let payload = b"direct-tcp-zc-echo".to_vec();
        client.send(payload.clone()).await.0?;
        let buf = server.recv_ring(&ring).await?;

        let (send_result, buf) = server.send_zc(buf).await;
        let sent = match send_result {
            Ok(sent) => sent,
            Err(err) => {
                drop(buf);
                server.close().await?;
                client.close().await?;
                if util::zerocopy_unsupported(&err) {
                    return Ok(());
                }
                return Err(err.into());
            }
        };
        assert_eq!(sent.bytes_sent(), payload.len());
        assert_ne!(sent.usage(), SendZcUsage::Unknown);
        drop(buf);

        let (recv_result, reply) = client.recv(BytesMut::with_capacity(payload.len())).await;
        let reply_len = recv_result?;
        assert_eq!(&reply[..reply_len], payload.as_slice());

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn recv_bundle_recycles_compact_and_materialized_buffers() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        server.set_nodelay(true).await?;
        client.set_nodelay(true).await?;
        let ring = RecvBufRing::builder(10).buf_cnt(32).buf_len(256).build()?;

        // The first round materializes and returns BIDs in reverse order. The
        // second consumes the untouched half of the ring. The third therefore
        // observes the reversed, non-contiguous BID sequence and exercises the
        // compact representation's sparse fallback.
        for (round, materialize_in_reverse) in [true, false, false].into_iter().enumerate() {
            let payload = vec![0x5A + round as u8; 4_096];
            let (send_result, payload) = client.send(payload).await;
            assert_eq!(send_result?, payload.len());

            let mut received = Vec::with_capacity(payload.len());
            let mut saw_multi_buffer_bundle = false;
            while received.len() < payload.len() {
                let bundle = match server.recv_bundle(&ring).await {
                    Ok(bundle) => bundle,
                    Err(err) if util::recv_bundle_unsupported(&err) => {
                        server.close().await?;
                        client.close().await?;
                        return Ok(());
                    }
                    Err(err) => return Err(err.into()),
                };
                saw_multi_buffer_bundle |= bundle.buffer_count() > 1;
                if materialize_in_reverse {
                    let bufs = bundle.into_bufs();
                    for buf in &bufs {
                        received.extend_from_slice(buf);
                    }
                    for buf in bufs.into_iter().rev() {
                        drop(buf);
                    }
                } else {
                    for chunk in bundle.iter() {
                        received.extend_from_slice(chunk);
                    }
                }
            }

            assert!(saw_multi_buffer_bundle);
            assert_eq!(received, payload);
        }

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

/// A partial `MSG_WAITALL` bundle can leave the kernel's buffer commits out of
/// step with its completion, letting the kernel reselect a buffer the bundle
/// owns. Single-shot bundles reject the flag before consuming input.
#[test]
fn recv_bundle_rejects_waitall_without_consuming_input() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(15).buf_cnt(2).buf_len(256).build()?;

        // The payload fills the whole ring, so an accepted receive would
        // complete rather than wait for more data.
        let payload = vec![0x22; 512];
        let (send_result, payload) = client.send(payload).await;
        assert_eq!(send_result?, payload.len());

        let err = server
            .recv_bundle_with_flags(&ring, libc::MSG_WAITALL)
            .await
            .expect_err("MSG_WAITALL bundle receive must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let mut received = Vec::with_capacity(payload.len());
        while received.len() < payload.len() {
            let bundle = match server.recv_bundle(&ring).await {
                Ok(bundle) => bundle,
                Err(err) if util::recv_bundle_unsupported(&err) => break,
                Err(err) => return Err(err.into()),
            };
            received.extend(bundle.iter().flatten().copied());
        }
        if !received.is_empty() {
            assert_eq!(received, payload);
        }

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

/// A peeking bundle that fills its buffers while more data is queued retries
/// in the kernel and copies the start of the queue again. Bundles reject
/// `MSG_PEEK` before consuming input.
#[test]
fn recv_bundle_rejects_peek_without_consuming_input() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let ring = RecvBufRing::builder(16).buf_cnt(8).buf_len(256).build()?;

        let payload: Vec<u8> = (0..1024u32).map(|i| (i % 251) as u8).collect();
        let (send_result, payload) = client.send(payload).await;
        assert_eq!(send_result?, payload.len());

        let err = server
            .recv_bundle_with_flags(&ring, libc::MSG_PEEK)
            .await
            .expect_err("MSG_PEEK bundle receive must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let mut multi = pin!(server.recv_bundle_multi_with_flags(&ring, libc::MSG_PEEK));
        let err = multi
            .next()
            .await
            .expect("configuration failure must produce one stream item")
            .expect_err("MSG_PEEK multishot bundle receive must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(multi.next().await.is_none());

        let mut received: Vec<u8> = Vec::with_capacity(payload.len());
        while received.len() < payload.len() {
            let bundle = match server.recv_bundle(&ring).await {
                Ok(bundle) => bundle,
                Err(err) if util::recv_bundle_unsupported(&err) => break,
                Err(err) => return Err(err.into()),
            };
            received.extend(bundle.iter().flatten().copied());
        }
        if !received.is_empty() {
            assert_eq!(received, payload);
        }

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn recv_bundle_shared_ring_handles_reverse_poll_order() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (first_server, first_client) = connected_pair().await?;
        let (second_server, second_client) = connected_pair().await?;
        let ring = RecvBufRing::builder(11).buf_cnt(8).buf_len(256).build()?;

        let mut first_recv = pin!(first_server.recv_bundle(&ring));
        let mut second_recv = pin!(second_server.recv_bundle(&ring));
        assert!(futures_util::poll!(&mut first_recv).is_pending());
        assert!(futures_util::poll!(&mut second_recv).is_pending());

        first_client.send(b"first".to_vec()).await.0?;
        second_client.send(b"second".to_vec()).await.0?;

        // Poll the receive futures in the opposite order from submission.
        // Reaping must reconcile ownership before either future is polled.
        let second = match second_recv.await {
            Ok(bundle) => bundle,
            Err(err) if util::recv_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let first = first_recv.await?;
        assert_eq!(
            second.iter().flatten().copied().collect::<Vec<_>>(),
            b"second"
        );
        assert_eq!(
            first.iter().flatten().copied().collect::<Vec<_>>(),
            b"first"
        );

        drop((first, second));
        first_server.close().await?;
        first_client.close().await?;
        second_server.close().await?;
        second_client.close().await?;
        Ok(())
    })
}

#[test]
fn recv_bundle_multishot_shared_ring_handles_reverse_poll_order(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (first_server, first_client) = connected_pair().await?;
        let (second_server, second_client) = connected_pair().await?;
        let ring = RecvBufRing::builder(12).buf_cnt(8).buf_len(256).build()?;

        let mut first_recv = pin!(first_server.recv_bundle_multi(&ring));
        let mut second_recv = pin!(second_server.recv_bundle_multi(&ring));
        std::future::poll_fn(|cx| {
            assert!(Stream::poll_next(first_recv.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;
        std::future::poll_fn(|cx| {
            assert!(Stream::poll_next(second_recv.as_mut(), cx).is_pending());
            std::task::Poll::Ready(())
        })
        .await;

        first_client.send(b"first".to_vec()).await.0?;
        second_client.send(b"second".to_vec()).await.0?;

        let second = match second_recv.next().await.expect("multishot stream ended") {
            Ok(bundle) => bundle,
            Err(err) if util::recv_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let first = first_recv.next().await.expect("multishot stream ended")?;
        assert_eq!(
            second.iter().flatten().copied().collect::<Vec<_>>(),
            b"second"
        );
        assert_eq!(
            first.iter().flatten().copied().collect::<Vec<_>>(),
            b"first"
        );

        drop((first, second, first_recv, second_recv));
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
        assert_eq!(sent.bytes_sent(), sent_buf.len());
        assert_ne!(sent.usage(), SendZcUsage::Unknown);

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
        let (send_res, sent_buf) = client.send_msg_zc(payload).await;
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
        assert_eq!(sent.bytes_sent(), sent_buf.len());
        assert_ne!(sent.usage(), SendZcUsage::Unknown);

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
                let (reader, writer) = socket.into_stream().owned_split();
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

/// `poll_write_vectored` must accept more than `IOV_MAX` (1024) slices and
/// write a prefix of them, as `std` does, instead of failing with `EMSGSIZE`.
#[test]
fn write_vectored_accepts_more_than_iov_max_slices() -> Result<(), Box<dyn std::error::Error>> {
    use std::future::poll_fn;
    use tokio::io::AsyncWrite;

    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept().await?;
        let mut stream = pin!(server.into_stream());

        let bytes = vec![7_u8; 2048];
        let slices: Vec<io::IoSlice<'_>> = bytes.chunks(1).map(io::IoSlice::new).collect();
        let written = poll_fn(|cx| stream.as_mut().poll_write_vectored(cx, &slices)).await?;
        assert!(written > 0 && written <= bytes.len());
        drop(peer);
        Ok(())
    })
}

/// Fill the send path of a connection whose peer never reads, abandon the
/// pending write, and return the stream with its readiness gate closed.
async fn stream_with_abandoned_write(
) -> Result<(Pin<Box<norn_uring::net::TcpStream>>, std::net::TcpStream), Box<dyn std::error::Error>>
{
    let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
    let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
    let (server, _) = listener.accept().await?;
    let mut stream = Box::pin(server.into_stream());

    let chunk = vec![0_u8; 64 * 1024];
    for _ in 0..4096 {
        match poll_fn(|cx| Poll::Ready(stream.as_mut().poll_write(cx, &chunk))).await {
            Poll::Ready(Ok(_)) => {}
            Poll::Ready(Err(err)) => return Err(err.into()),
            // The application abandons the write here, for example after a
            // write timeout.
            Poll::Pending => return Ok((stream, peer)),
        }
    }
    panic!("the send path never filled");
}

/// `poll_shutdown` must not wait for `POLLOUT`: `shutdown(SHUT_WR)` is valid
/// even when the send buffer is full and the peer has stopped reading.
#[test]
fn shutdown_after_abandoned_write_does_not_wait_for_writability(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (mut stream, peer) = stream_with_abandoned_write().await?;
        let mut shutdown = pin!(poll_fn(|cx| stream.as_mut().poll_shutdown(cx)));
        for _ in 0..200 {
            if let Poll::Ready(result) = futures_util::poll!(shutdown.as_mut()) {
                result?;
                drop(peer);
                return Ok(());
            }
            norn_uring::noop().await;
        }
        panic!("shutdown(SHUT_WR) is still waiting for POLLOUT from a peer that does not read");
    })
}

/// A TCP stream has no userspace buffer, so `poll_flush` must complete without
/// waiting for `POLLOUT`.
#[test]
fn flush_after_abandoned_write_does_not_wait_for_writability(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (mut stream, peer) = stream_with_abandoned_write().await?;
        let mut flush = pin!(poll_fn(|cx| stream.as_mut().poll_flush(cx)));
        for _ in 0..200 {
            if let Poll::Ready(result) = futures_util::poll!(flush.as_mut()) {
                result?;
                drop(peer);
                return Ok(());
            }
            norn_uring::noop().await;
        }
        panic!("flush is still waiting for POLLOUT from a peer that does not read");
    })
}

/// Leading empty slices must not eat the `IOV_MAX` cap: a vectored write with
/// data after more than `IOV_MAX` empty slices writes that data instead of
/// returning `Ok(0)`, which writers treat as a failure.
#[test]
fn write_vectored_skips_leading_empty_slices_before_capping(
) -> Result<(), Box<dyn std::error::Error>> {
    use std::future::poll_fn;
    use tokio::io::AsyncWrite;

    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        let (server, _) = listener.accept().await?;
        let mut stream = pin!(server.into_stream());

        let empty: &[u8] = &[];
        let mut slices: Vec<io::IoSlice<'_>> = vec![io::IoSlice::new(empty); 1500];
        slices.push(io::IoSlice::new(b"data"));
        let written = poll_fn(|cx| stream.as_mut().poll_write_vectored(cx, &slices)).await?;
        assert_eq!(written, 4);
        drop(peer);
        Ok(())
    })
}
