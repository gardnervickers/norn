#![cfg(target_os = "linux")]

use std::io;
use std::net::SocketAddr;
use std::pin::pin;

use bytes::BytesMut;
use futures_util::StreamExt;
use norn_executor::spawn;
use norn_uring::bufring::{RecvBufRing, SendBufRing, SendStreamBatch};
use norn_uring::net::{TcpListener, TcpSocket};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

mod util;

fn stage_stream_segment(batch: &SendStreamBatch, payload: &[u8]) -> io::Result<()> {
    let mut buf = batch.checkout()?;
    buf.as_mut_slice()[..payload.len()].copy_from_slice(payload);
    buf.set_len(payload.len())?;
    buf.commit()
}

async fn drain_stream_bundle(
    sender: &TcpSocket,
    receiver: &TcpSocket,
    batch: &SendStreamBatch,
    flags: Option<i32>,
) -> io::Result<(Vec<u8>, usize)> {
    let mut received = Vec::with_capacity(batch.queued_len());
    let mut sends = 0;

    while !batch.is_empty() {
        let queued_before = batch.queued_len();
        let sent = match flags {
            Some(flags) => sender.send_bundle_with_flags(batch, flags).await?,
            None => sender.send_bundle(batch).await?,
        };
        if sent == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "TCP send bundle completed without draining queued bytes",
            ));
        }
        sends += 1;
        assert_eq!(batch.queued_len(), queued_before - sent);

        let mut remaining = sent;
        while remaining > 0 {
            let (result, buf) = receiver.recv(BytesMut::with_capacity(remaining)).await;
            let read = result?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "TCP peer closed before receiving the sent bundle bytes",
                ));
            }
            received.extend_from_slice(&buf[..read]);
            remaining -= read;
        }
    }

    Ok((received, sends))
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
fn send_bundle_stream_smoke() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (receiver, sender) = connected_pair().await?;
        let ring = SendBufRing::builder(14).buf_cnt(4).buf_len(256).build()?;
        let batch = ring.stream_batch()?;
        let segments: [&[u8]; 3] = [b"stream-", b"send-", b"bundle"];
        for segment in segments {
            stage_stream_segment(&batch, segment)?;
        }
        let expected: Vec<u8> = segments
            .iter()
            .flat_map(|segment| segment.iter().copied())
            .collect();

        let (received, sends) = match drain_stream_bundle(&sender, &receiver, &batch, None).await {
            Ok(result) => result,
            Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert_eq!(sends, 1);
        assert_eq!(received, expected);
        assert!(batch.is_empty());
        assert_eq!(batch.queued_buffers(), 0);
        assert_eq!(ring.available_buffers(), 4);

        drop(batch);
        sender.close().await?;
        receiver.close().await?;
        Ok(())
    })
}

#[test]
fn send_bundle_stream_repeated_partial_drains() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        const SEGMENTS: u16 = 4;
        const SEGMENT_LEN: usize = 256 * 1024;

        let (receiver, sender) = connected_pair().await?;
        sender.set_send_buffer_size(4 * 1024).await?;
        receiver.set_recv_buffer_size(4 * 1024).await?;

        let mut prefill_len = 0;
        loop {
            let payload = vec![0xa5; 4 * 1024];
            let (result, payload) = sender.send_with_flags(payload, libc::MSG_DONTWAIT).await;
            match result {
                Ok(0) => break,
                Ok(sent) => {
                    prefill_len += sent;
                    assert!(prefill_len <= 16 * 1024 * 1024);
                    assert!(sent <= payload.len());
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }
        assert!(prefill_len > 0);

        let open_window = prefill_len;
        let mut opened = 0;
        while opened < open_window {
            let (result, buf) = receiver
                .recv(BytesMut::with_capacity(open_window - opened))
                .await;
            let read = result?;
            assert!(read > 0);
            assert!(buf[..read].iter().all(|byte| *byte == 0xa5));
            opened += read;
        }
        let _ = sender.poll_readiness::<false>(libc::POLLOUT as u32).await?;

        let ring = SendBufRing::builder(15)
            .ring_entries(4)
            .buf_cnt(SEGMENTS)
            .buf_len(SEGMENT_LEN)
            .build()?;
        let batch = ring.stream_batch()?;
        let mut expected = Vec::with_capacity(SEGMENTS as usize * SEGMENT_LEN);
        for sequence in 0..SEGMENTS {
            let payload = vec![sequence as u8; SEGMENT_LEN];
            stage_stream_segment(&batch, &payload)?;
            expected.extend_from_slice(&payload);
        }

        let queued_before = batch.queued_len();
        let first_sent = match sender
            .send_bundle_with_flags(&batch, libc::MSG_DONTWAIT)
            .await
        {
            Ok(sent) => sent,
            Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert!(first_sent > 0);
        assert!(
            first_sent < SEGMENT_LEN,
            "prefilled socket should force a mid-segment short send"
        );
        assert_eq!(batch.queued_len(), queued_before - first_sent);

        sender.set_send_buffer_size(SEGMENT_LEN).await?;
        receiver.set_recv_buffer_size(SEGMENT_LEN).await?;

        let expected_len = expected.len();
        let remaining_prefill = prefill_len - opened;
        let receive_task = spawn(async move {
            let total_len = remaining_prefill + expected_len;
            let mut received = Vec::with_capacity(total_len);
            while received.len() < total_len {
                let (result, buf) = receiver
                    .recv(BytesMut::with_capacity(total_len - received.len()))
                    .await;
                let read = result?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TCP peer closed before receiving the send bundle",
                    ));
                }
                received.extend_from_slice(&buf[..read]);
            }
            io::Result::Ok(received)
        });

        let mut sends = 1;
        while !batch.is_empty() {
            let queued_before = batch.queued_len();
            let sent = match sender.send_bundle(&batch).await {
                Ok(sent) => sent,
                Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
                Err(err) => return Err(err.into()),
            };
            assert!(sent > 0);
            sends += 1;
            assert_eq!(batch.queued_len(), queued_before - sent);
        }
        let received = receive_task.await??;
        assert!(sends > 1, "short sends should require repeated submissions");
        assert!(received[..remaining_prefill]
            .iter()
            .all(|byte| *byte == 0xa5));
        assert_eq!(&received[remaining_prefill..], expected.as_slice());
        assert!(batch.is_empty());
        assert_eq!(ring.available_buffers(), SEGMENTS as usize);

        drop(batch);
        sender.close().await?;
        Ok(())
    })
}

#[test]
fn send_bundle_stream_retries_after_would_block_and_reuses_ring(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        const PAYLOAD_LEN: usize = 1024 * 1024;

        let (receiver, sender) = connected_pair().await?;
        sender.set_send_buffer_size(4 * 1024).await?;
        receiver.set_recv_buffer_size(4 * 1024).await?;

        let mut prefill_len = 0;
        loop {
            let payload = vec![0xa5; 4 * 1024];
            let (result, payload) = sender.send_with_flags(payload, libc::MSG_DONTWAIT).await;
            match result {
                Ok(0) => break,
                Ok(sent) => {
                    prefill_len += sent;
                    assert!(sent <= payload.len());
                    assert!(prefill_len <= 16 * 1024 * 1024);
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }
        assert!(prefill_len > 0);

        let ring = SendBufRing::builder(19)
            .buf_cnt(1)
            .buf_len(PAYLOAD_LEN)
            .build()?;
        let batch = ring.stream_batch()?;
        let payload: Vec<u8> = (0..PAYLOAD_LEN).map(|index| (index % 251) as u8).collect();
        stage_stream_segment(&batch, &payload)?;

        let mut sent_before_block = 0;
        loop {
            match sender
                .send_bundle_with_flags(&batch, libc::MSG_DONTWAIT)
                .await
            {
                Ok(sent) => {
                    assert!(sent > 0);
                    sent_before_block += sent;
                    assert!(
                        !batch.is_empty(),
                        "test failed to force EAGAIN before draining the batch"
                    );
                }
                Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err.into()),
            }
        }
        assert_eq!(batch.queued_len(), PAYLOAD_LEN - sent_before_block);
        assert_eq!(ring.available_buffers(), 0);

        sender.set_send_buffer_size(PAYLOAD_LEN).await?;
        receiver.set_recv_buffer_size(PAYLOAD_LEN).await?;

        let reuse_payload = b"ring-reuse-after-eagain".to_vec();
        let receive_len = prefill_len + payload.len() + reuse_payload.len();
        let receive_task = spawn(async move {
            let mut received = Vec::with_capacity(receive_len);
            while received.len() < receive_len {
                let (result, buf) = receiver
                    .recv(BytesMut::with_capacity(receive_len - received.len()))
                    .await;
                let read = result?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TCP peer closed before the EAGAIN retry drained",
                    ));
                }
                received.extend_from_slice(&buf[..read]);
            }
            receiver.close().await?;
            io::Result::Ok(received)
        });

        let _ = sender.poll_readiness::<false>(libc::POLLOUT as u32).await?;
        let retried = sender.send_bundle(&batch).await?;
        assert_eq!(retried, PAYLOAD_LEN - sent_before_block);
        assert!(batch.is_empty());
        assert_eq!(ring.available_buffers(), 1);
        drop(batch);

        let reuse = ring.stream_batch()?;
        stage_stream_segment(&reuse, &reuse_payload)?;
        assert_eq!(sender.send_bundle(&reuse).await?, reuse_payload.len());
        assert!(reuse.is_empty());
        drop(reuse);
        assert_eq!(ring.available_buffers(), 1);

        sender.close().await?;
        let received = receive_task.await??;
        assert!(received[..prefill_len].iter().all(|byte| *byte == 0xa5));
        assert_eq!(
            &received[prefill_len..prefill_len + payload.len()],
            payload.as_slice()
        );
        assert_eq!(&received[prefill_len + payload.len()..], reuse_payload);
        Ok(())
    })
}

#[test]
fn send_bundle_stream_accumulates_more_than_kernel_import_limit(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        const SEGMENTS: u16 = 257;
        const SEGMENT_LEN: usize = 4 * 1024;

        let (receiver, sender) = connected_pair().await?;
        let ring = SendBufRing::builder(18)
            .ring_entries(512)
            .buf_cnt(SEGMENTS)
            .buf_len(SEGMENT_LEN)
            .build()?;
        let batch = ring.stream_batch()?;
        let mut expected = Vec::with_capacity(SEGMENTS as usize * SEGMENT_LEN);
        for sequence in 0..SEGMENTS {
            let mut payload = vec![(sequence & 0xff) as u8; SEGMENT_LEN];
            payload[..2].copy_from_slice(&sequence.to_ne_bytes());
            stage_stream_segment(&batch, &payload)?;
            expected.extend_from_slice(&payload);
        }

        let expected_len = expected.len();
        let receive_task = spawn(async move {
            let mut received = Vec::with_capacity(expected_len);
            while received.len() < expected_len {
                let (result, buf) = receiver
                    .recv(BytesMut::with_capacity(expected_len - received.len()))
                    .await;
                let read = result?;
                if read == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "TCP peer closed before receiving the multi-CQE send bundle",
                    ));
                }
                received.extend_from_slice(&buf[..read]);
            }
            receiver.close().await?;
            io::Result::Ok(received)
        });

        let sent = match sender.send_bundle(&batch).await {
            Ok(sent) => sent,
            Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        let received = receive_task.await??;

        assert_eq!(sent, expected.len());
        assert_eq!(received, expected);
        assert!(batch.is_empty());
        assert_eq!(batch.queued_buffers(), 0);
        assert_eq!(ring.available_buffers(), SEGMENTS as usize);

        drop(batch);
        sender.close().await?;
        Ok(())
    })
}

#[test]
fn send_bundle_stream_reuses_ring_after_drain() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (receiver, sender) = connected_pair().await?;
        let ring = SendBufRing::builder(16).buf_cnt(4).buf_len(256).build()?;

        let first = ring.stream_batch()?;
        stage_stream_segment(&first, b"first-")?;
        stage_stream_segment(&first, b"batch")?;
        let (received, _) = match drain_stream_bundle(&sender, &receiver, &first, None).await {
            Ok(result) => result,
            Err(err) if util::send_bundle_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert_eq!(received, b"first-batch");
        drop(first);
        assert_eq!(ring.available_buffers(), 4);

        let second = ring.stream_batch()?;
        stage_stream_segment(&second, b"second-batch")?;
        let (received, _) =
            drain_stream_bundle(&sender, &receiver, &second, Some(libc::MSG_NOSIGNAL)).await?;
        assert_eq!(received, b"second-batch");
        drop(second);
        assert_eq!(ring.available_buffers(), 4);

        sender.close().await?;
        receiver.close().await?;
        Ok(())
    })
}

#[test]
fn send_bundle_stream_rejects_empty_batch() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (receiver, sender) = connected_pair().await?;
        let ring = SendBufRing::builder(17).buf_cnt(2).buf_len(64).build()?;
        let batch = ring.stream_batch()?;

        let err = sender.send_bundle(&batch).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert!(batch.is_empty());
        assert_eq!(ring.available_buffers(), 2);

        drop(batch);
        sender.close().await?;
        receiver.close().await?;
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
