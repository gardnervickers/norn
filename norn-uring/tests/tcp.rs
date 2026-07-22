#![cfg(target_os = "linux")]

use std::io;
use std::net::SocketAddr;
use std::pin::pin;

use bytes::BytesMut;
use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::buf::StableBufMut;
use norn_uring::bufring::{RecvBufRing, SendBufRing};
use norn_uring::fs;
use norn_uring::net::{
    AttachRecvRingErrorKind, AttachSendRingErrorKind, FinishSendRingOutcome, TcpListener, TcpSocket,
};
use norn_uring::Driver;
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
fn recv_ring_stream_socket_reports_peer() -> Result<(), Box<dyn std::error::Error>> {
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

        let (buf, recv_peer) = server.recv_ring(&ring).await?;
        assert_eq!(&buf[..payload.len()], payload.as_slice());
        assert_eq!(recv_peer, peer_addr);

        server.close().await?;
        client.close().await?;
        Ok(())
    })
}

#[test]
fn bundled_receive_stream_rearms_after_ring_exhaustion() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let (reader, _server_writer) = server.into_stream().owned_split();
        let (_client_reader, client_writer) = client.into_stream().owned_split();
        let mut client_writer = pin!(client_writer);
        let ring = RecvBufRing::builder(17).buf_cnt(2).buf_len(32).build()?;

        let mut incoming = match reader.recv_bundles(ring) {
            Ok(incoming) => incoming,
            Err(err) if err.kind() == AttachRecvRingErrorKind::Unsupported => return Ok(()),
            Err(err) => return Err(err.into()),
        };

        let payload: Vec<u8> = (0..=255).cycle().take(512).collect();
        client_writer.write_all(&payload).await?;
        client_writer.shutdown().await?;

        let first = incoming.next().await.expect("missing first bundle")?;
        assert!(!first.is_empty());
        let mut held = vec![first];

        // Hold completed bundles until every BID is owned by userspace. The
        // adapter must then turn the terminal ENOBUFS into a capacity wait.
        loop {
            let mut next = pin!(incoming.next());
            match futures_util::poll!(next.as_mut()) {
                std::task::Poll::Ready(Some(Ok(bundle))) => held.push(bundle),
                std::task::Poll::Ready(Some(Err(err))) => return Err(err.into()),
                std::task::Poll::Ready(None) => panic!("receive stream ended before exhaustion"),
                std::task::Poll::Pending => break,
            }
        }
        let mut received: Vec<u8> = held
            .iter()
            .flat_map(|bundle| bundle.iter().flatten())
            .copied()
            .collect();
        drop(held);

        while let Some(bundle) = incoming.next().await {
            let bundle = bundle?;
            received.extend(bundle.iter().flatten().copied());
        }
        assert_eq!(received, payload);

        let _reader = incoming.finish().await?;
        Ok(())
    })
}

#[test]
fn bundled_receive_ring_is_shared_across_contending_adapters(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server_a, client_a) = connected_pair().await?;
        let (server_b, client_b) = connected_pair().await?;
        let (reader_a, _server_writer_a) = server_a.into_stream().owned_split();
        let (reader_b, _server_writer_b) = server_b.into_stream().owned_split();
        let (_client_reader_a, mut writer_a) = client_a.into_stream().owned_split();
        let (_client_reader_b, mut writer_b) = client_b.into_stream().owned_split();

        let ring = RecvBufRing::builder(27).buf_cnt(2).buf_len(64).build()?;
        let incoming_a = match reader_a.recv_bundles(ring.clone()) {
            Ok(incoming) => incoming,
            Err(error) if error.kind() == AttachRecvRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };
        let incoming_b = reader_b.recv_bundles(ring.clone())?;

        let receive_a = spawn(async move {
            let mut incoming = incoming_a;
            let mut bytes = Vec::new();
            while let Some(bundle) = incoming.next().await {
                bytes.extend(bundle?.iter().flatten().copied());
            }
            drop(incoming.finish().await?);
            io::Result::Ok(bytes)
        });
        let receive_b = spawn(async move {
            let mut incoming = incoming_b;
            let mut bytes = Vec::new();
            while let Some(bundle) = incoming.next().await {
                bytes.extend(bundle?.iter().flatten().copied());
            }
            drop(incoming.finish().await?);
            io::Result::Ok(bytes)
        });

        let payload_a: Vec<u8> = (0..=255).cycle().take(4096).collect();
        let payload_b: Vec<u8> = (0..=127).rev().cycle().take(4096).collect();
        futures_util::try_join!(
            async {
                writer_a.write_all(&payload_a).await?;
                writer_a.shutdown().await
            },
            async {
                writer_b.write_all(&payload_b).await?;
                writer_b.shutdown().await
            },
        )?;

        assert_eq!(receive_a.await??, payload_a);
        assert_eq!(receive_b.await??, payload_b);
        drop(ring);
        Ok(())
    })
}

#[test]
fn bundled_send_writer_streams_and_returns_clean_ring() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        server.set_recv_buffer_size(1024).await?;
        client.set_send_buffer_size(1024).await?;

        let (server_reader, _server_writer) = server.into_stream().owned_split();
        let receive = spawn(async move {
            let mut server_reader = pin!(server_reader);
            let mut bytes = Vec::new();
            server_reader.as_mut().read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        });

        let (_client_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(18).buf_count(8).buf_len(128).build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        let payload: Vec<u8> = (0..=255).cycle().take(64 * 1024).collect();
        writer.write_all(&payload).await?;
        writer.flush().await?;

        let FinishSendRingOutcome::Drained { writer, ring } = writer.finish_send_ring().await
        else {
            panic!("bundled send did not drain cleanly");
        };
        assert_eq!(ring.buf_count(), 8);
        let mut writer = pin!(writer);
        writer.as_mut().shutdown().await?;
        assert_eq!(receive.await??, payload);
        Ok(())
    })
}

#[test]
fn bundled_send_accepts_direct_file_reads_into_send_buffers(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("bundle-source");
        let payload: Vec<u8> = (0..=255).cycle().take(32 * 1024 + 17).collect();
        std::fs::write(&path, &payload)?;
        let file = fs::File::open(&path).await?;

        let (server, client) = connected_pair().await?;
        let (mut reader, _server_writer) = server.into_stream().owned_split();
        let receive = spawn(async move {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        });

        let (_client_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(22)
            .buf_count(4)
            .buf_len(1024)
            .build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        let mut offset = 0;
        loop {
            let buffer = writer.acquire().await?;
            let (result, buffer) = file.read_at(buffer, offset).await;
            let bytes = result?;
            if bytes == 0 {
                drop(buffer);
                break;
            }
            writer.enqueue(buffer, bytes)?;
            offset += bytes as u64;
        }

        let FinishSendRingOutcome::Drained { mut writer, ring } = writer.finish_send_ring().await
        else {
            panic!("bundled send did not drain cleanly");
        };
        writer.shutdown().await?;
        drop(ring);
        file.close().await?;
        assert_eq!(receive.await??, payload);
        Ok(())
    })
}

#[test]
fn cancelled_flush_does_not_own_committed_send_work_under_sq_pressure(
) -> Result<(), Box<dyn std::error::Error>> {
    let driver = Driver::new(io_uring::IoUring::builder(), 2)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let (server, client) = connected_pair().await?;
        let (mut reader, _server_writer) = server.into_stream().owned_split();
        let receive = spawn(async move {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        });
        let (_client_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(28).buf_count(4).buf_len(64).build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        // Fill the userspace SQ before the detached pump gets its first poll.
        // The final noop necessarily waits for capacity on this two-entry ring.
        let mut occupying = Vec::new();
        for _ in 0..3 {
            let mut op = Box::pin(norn_uring::noop());
            assert!(futures_util::poll!(op.as_mut()).is_pending());
            occupying.push(op);
        }

        let payload: Vec<u8> = (0..128).collect();
        writer.write_all(&payload).await?;
        let mut flush = Box::pin(writer.flush());
        assert!(futures_util::poll!(flush.as_mut()).is_pending());
        drop(flush);
        drop(occupying);

        let FinishSendRingOutcome::Drained { mut writer, ring } = writer.finish_send_ring().await
        else {
            panic!("bundled send did not drain cleanly");
        };
        writer.shutdown().await?;
        drop(ring);
        assert_eq!(receive.await??, payload);
        Ok(())
    })
}

#[test]
fn bundled_send_sqpoll_preserves_exact_fifo_when_available(
) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = io_uring::IoUring::builder();
    builder.setup_sqpoll(10);
    let driver = match Driver::new(builder, 32) {
        Ok(driver) => driver,
        Err(error)
            if error.raw_os_error() == Some(libc::EPERM)
                && std::env::var_os("NORN_REQUIRE_SQPOLL").is_none() =>
        {
            return Ok(())
        }
        Err(error) => return Err(error.into()),
    };
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let (server, client) = connected_pair().await?;
        let (mut reader, _server_writer) = server.into_stream().owned_split();
        let receive = spawn(async move {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        });
        let (_client_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(32).buf_count(8).buf_len(128).build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        let payload: Vec<u8> = (0..=255).cycle().take(128 * 1024).collect();
        writer.write_all(&payload).await?;
        let FinishSendRingOutcome::Drained { mut writer, ring } = writer.finish_send_ring().await
        else {
            panic!("SQPOLL bundled send did not drain cleanly");
        };
        writer.shutdown().await?;
        drop(ring);
        assert_eq!(receive.await??, payload);
        Ok(())
    })
}

#[test]
fn attach_send_ring_outside_executor_returns_inputs() -> Result<(), Box<dyn std::error::Error>> {
    let driver = Driver::new(io_uring::IoUring::builder(), 32)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    let (server, writer, ring) = executor.block_on(async {
        let (server, client) = connected_pair().await?;
        let (_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(20).buf_count(4).buf_len(64).build()?;
        Ok::<_, Box<dyn std::error::Error>>((server, writer, ring))
    })?;

    let error = writer
        .attach_send_ring(ring)
        .expect_err("attachment outside executor context should fail");
    assert_eq!(error.kind(), AttachSendRingErrorKind::NoExecutor);
    let (writer, ring) = error.into_parts();

    executor.block_on(async move {
        drop(writer);
        drop(ring);
        server.close().await
    })?;
    Ok(())
}

#[test]
fn attach_send_ring_rejects_a_different_active_executor() -> Result<(), Box<dyn std::error::Error>>
{
    let driver = Driver::new(io_uring::IoUring::builder(), 32)?;
    let mut socket_executor = norn_executor::LocalExecutor::new(driver);
    let (server, writer, ring) = socket_executor.block_on(async {
        let (server, client) = connected_pair().await?;
        let (_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(24).buf_count(4).buf_len(64).build()?;
        Ok::<_, Box<dyn std::error::Error>>((server, writer, ring))
    })?;

    let other_driver = Driver::new(io_uring::IoUring::builder(), 32)?;
    let mut other_executor = norn_executor::LocalExecutor::new(other_driver);
    let (writer, ring) = other_executor.block_on(async move {
        let error = writer
            .attach_send_ring(ring)
            .expect_err("attachment on a different executor should fail");
        assert_eq!(error.kind(), AttachSendRingErrorKind::WrongExecutor);
        error.into_parts()
    });

    socket_executor.block_on(async move {
        drop(writer);
        drop(ring);
        server.close().await
    })?;
    Ok(())
}

#[test]
fn cancelling_finish_abandons_and_destroys_the_ring() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let (_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(21).buf_count(2).buf_len(64).build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        // A checked-out buffer makes finish deterministically wait. Cancelling
        // that consuming future must switch the pump from Finishing to
        // Abandoned rather than leaving a detached task wedged forever.
        let held = writer.acquire().await?;
        let mut finish = Box::pin(writer.finish_send_ring());
        assert!(futures_util::poll!(finish.as_mut()).is_pending());
        drop(finish);
        drop(held);

        let mut replacement = None;
        for _ in 0..100 {
            match SendBufRing::builder(21).buf_count(2).buf_len(64).build() {
                Ok(ring) => {
                    replacement = Some(ring);
                    break;
                }
                Err(_) => norn_uring::noop().await,
            }
        }
        let replacement = replacement.expect("abandoned send ring was not destroyed");
        drop(replacement);
        server.close().await?;
        Ok(())
    })
}

#[test]
fn cancelling_finish_with_an_active_send_destroys_after_terminal_cqe(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        server.set_recv_buffer_size(1024).await?;
        client.set_send_buffer_size(1024).await?;
        let (_reader, writer) = client.into_stream().owned_split();
        let ring = SendBufRing::builder(29)
            .buf_count(16)
            .buf_len(64 * 1024)
            .build()?;
        let mut writer = match writer.attach_send_ring(ring) {
            Ok(writer) => writer,
            Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
            Err(error) => return Err(error.into()),
        };

        for _ in 0..16 {
            let mut buffer = writer.acquire().await?;
            let len = buffer.capacity();
            buffer.spare_capacity_mut().iter_mut().for_each(|slot| {
                slot.write(0x5a);
            });
            unsafe { buffer.set_init(len) };
            writer.enqueue(buffer, len)?;
        }

        // Yield once so the pump submits while the peer deliberately does not
        // read and the tiny socket buffer keeps the multishot send active.
        norn_uring::noop().await;
        let mut finish = Box::pin(writer.finish_send_ring());
        assert!(futures_util::poll!(finish.as_mut()).is_pending());
        drop(finish);
        server.close().await?;

        let mut replacement = None;
        for _ in 0..200 {
            match SendBufRing::builder(29)
                .buf_count(16)
                .buf_len(64 * 1024)
                .build()
            {
                Ok(ring) => {
                    replacement = Some(ring);
                    break;
                }
                Err(_) => norn_uring::noop().await,
            }
        }
        let replacement = replacement.expect("active abandoned send ring was not destroyed");
        drop(replacement);
        Ok(())
    })
}

#[test]
fn failed_send_ring_is_sanitized_before_cross_socket_reuse(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = connected_pair().await?;
        let (mut client_reader, mut ordinary_writer) = client.into_stream().owned_split();
        let mut ring = SendBufRing::builder(30)
            .buf_count(4)
            .buf_len(1024)
            .build()?;

        server.shutdown(std::net::Shutdown::Both).await?;
        server.close().await?;
        let mut eof = Vec::new();
        client_reader.read_to_end(&mut eof).await?;

        let mut attempts = 0;
        let sanitized = loop {
            attempts += 1;
            assert!(attempts <= 16, "closed peer never produced a send failure");
            let mut bundled = match ordinary_writer.attach_send_ring(ring) {
                Ok(writer) => writer,
                Err(error) if error.kind() == AttachSendRingErrorKind::Unsupported => return Ok(()),
                Err(error) => return Err(error.into()),
            };
            let _ = bundled.write_all(&vec![0xa5; 4096]).await;
            match bundled.finish_send_ring().await {
                FinishSendRingOutcome::SendFailed { ring, .. } => break ring,
                FinishSendRingOutcome::Drained {
                    writer,
                    ring: returned,
                } => {
                    ordinary_writer = writer;
                    ring = returned;
                }
                FinishSendRingOutcome::CleanupFailed { error } => {
                    panic!("failed ring could not be sanitized: {error}")
                }
            }
        };
        drop(client_reader);

        let (fresh_server, fresh_client) = connected_pair().await?;
        let (mut fresh_reader, _fresh_server_writer) = fresh_server.into_stream().owned_split();
        let receive = spawn(async move {
            let mut bytes = Vec::new();
            fresh_reader.read_to_end(&mut bytes).await?;
            io::Result::Ok(bytes)
        });
        let (_fresh_client_reader, fresh_writer) = fresh_client.into_stream().owned_split();
        let mut fresh_writer = fresh_writer.attach_send_ring(sanitized).unwrap();
        let payload = b"only fresh socket data".to_vec();
        fresh_writer.write_all(&payload).await?;
        let FinishSendRingOutcome::Drained {
            mut writer,
            ring: sanitized,
        } = fresh_writer.finish_send_ring().await
        else {
            panic!("sanitized ring failed on a fresh socket");
        };
        writer.shutdown().await?;
        drop(sanitized);
        assert_eq!(receive.await??, payload);
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
