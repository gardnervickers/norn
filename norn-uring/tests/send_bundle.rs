#![cfg(target_os = "linux")]

use std::future::Future;
use std::io;
use std::task::Poll;

use futures_util::future::poll_fn;
use norn_uring::bufring::{PublishErrorKind, SendBufRing, SendBundleResult};
use norn_uring::net::{TcpListener, TcpSocket};
mod util;

async fn tcp_pair() -> io::Result<(TcpSocket, TcpSocket)> {
    let listener = TcpListener::bind("127.0.0.1:0".parse().unwrap(), 8).await?;
    let addr = listener.local_addr()?;
    let connect = norn_executor::spawn(async move { TcpSocket::connect(addr).await });
    let (server, _) = listener.accept().await?;
    let client = connect.await.unwrap()?;
    Ok((server, client))
}

fn publish(ring: &SendBufRing, payload: &[u8]) {
    let mut buf = ring.try_acquire().expect("free send buffer");
    assert_eq!(buf.extend_from_slice(payload), payload.len());
    ring.publish(buf).expect("publish send buffer");
}

async fn recv_exact(socket: &TcpSocket, len: usize) -> io::Result<Vec<u8>> {
    let mut received = Vec::with_capacity(len);
    while received.len() < len {
        let (result, buf) = socket.recv(vec![0u8; 64 * 1024]).await;
        let n = result?;
        if n == 0 {
            return Err(io::ErrorKind::UnexpectedEof.into());
        }
        received.extend_from_slice(&buf[..n]);
    }
    Ok(received)
}

fn pattern(index: usize, len: usize) -> Vec<u8> {
    (0..len).map(|offset| (index * 31 + offset) as u8).collect()
}

#[test]
fn send_bundle_transmits_published_buffers_in_order() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        let ring = SendBufRing::builder(1).buf_cnt(4).buf_len(16).build()?;
        for payload in [&b"alpha "[..], b"beta ", b"gamma"] {
            publish(&ring, payload);
        }
        assert_eq!(
            (ring.queued(), ring.queued_bytes(), ring.available()),
            (3, 16, 1)
        );

        let sent = client.send_bundle(&ring).await?;
        assert_eq!(sent.bytes_sent(), 16);
        assert_eq!(sent.buffers_released(), 3);
        assert_eq!(sent.bytes_released(), 16);
        assert_eq!(
            (ring.queued(), ring.queued_bytes(), ring.available()),
            (0, 0, 4)
        );
        assert_eq!(recv_exact(&server, 16).await?, b"alpha beta gamma");

        // Released buffers are reusable and keep publication order.
        publish(&ring, b"again");
        let sent = client.send_bundle(&ring).await?;
        assert_eq!(sent.bytes_sent(), 5);
        assert_eq!(recv_exact(&server, 5).await?, b"again");
        Ok(())
    })
}

#[test]
fn send_bundle_requires_published_payload_and_one_sender() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        let ring = SendBufRing::builder(2).buf_cnt(2).buf_len(8).build()?;

        let error = client.send_bundle(&ring).await.unwrap_err();
        assert_eq!(error.error().raw_os_error(), Some(libc::ENOBUFS));
        assert_eq!(error.progress(), SendBundleResult::default());
        assert!(!error.is_quarantined());

        publish(&ring, b"owned");
        // A request claims the ring when it is first polled.
        let mut first = Box::pin(client.send_bundle(&ring));
        poll_fn(|cx| {
            assert!(first.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        let error = client.send_bundle(&ring).await.unwrap_err();
        assert_eq!(error.error().raw_os_error(), Some(libc::EBUSY));
        assert_eq!(error.progress(), SendBundleResult::default());
        assert!(!error.is_quarantined());

        let sent = first.await?;
        assert_eq!(sent.bytes_sent(), 5);
        assert_eq!(recv_exact(&server, 5).await?, b"owned");
        Ok(())
    })
}

#[test]
fn send_buffers_validate_publication_and_return_on_drop() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let ring = SendBufRing::builder(3).buf_cnt(2).buf_len(4).build()?;
        let other = SendBufRing::builder(4).buf_cnt(1).buf_len(4).build()?;

        let empty = ring.try_acquire().unwrap();
        let (kind, mut buf) = ring.publish(empty).unwrap_err().into_parts();
        assert_eq!(kind, PublishErrorKind::Empty);
        assert_eq!(buf.extend_from_slice(b"toolong"), 4);
        assert_eq!(&buf[..], b"tool");
        assert_eq!(buf.extend_from_slice(b"x"), 0);
        buf.set_len(2);
        assert_eq!(&buf[..], b"to");
        assert_eq!(&buf.as_full_slice()[2..], b"ol");

        let held = ring.try_acquire().unwrap();
        assert!(ring.try_acquire().is_none());
        drop(held);
        assert_eq!(ring.available(), 1);

        let error = other.publish(buf).unwrap_err();
        assert_eq!(error.kind(), PublishErrorKind::ForeignRing);
        drop(error.into_buf());
        assert_eq!(ring.available(), 2);
        assert_eq!(other.queued(), 0);

        let mut buf = ring.try_acquire().unwrap();
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| buf.set_len(5)));
        assert!(panic.is_err());

        assert!(SendBufRing::builder(5).buf_cnt(0).build().is_err());
        assert!(SendBufRing::builder(5).buf_len(0).build().is_err());
        assert!(SendBufRing::builder(5)
            .buf_len(i32::MAX as usize + 1)
            .build()
            .is_err());
        assert!(SendBufRing::builder(3).build().is_err(), "duplicate group");
        Ok(())
    })
}

#[test]
fn send_buffers_can_receive_and_forward_payload() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        let ring = SendBufRing::builder(6).buf_cnt(2).buf_len(32).build()?;
        client.send(b"echo me".to_vec()).await.0?;

        // A send buffer is an ordinary receive target, then publishable.
        let (result, buf) = server.recv(ring.try_acquire().unwrap()).await;
        assert_eq!(result?, 7);
        assert_eq!(&buf[..], b"echo me");
        ring.publish(buf).unwrap();
        assert_eq!(server.send_bundle(&ring).await?.bytes_sent(), 7);
        assert_eq!(recv_exact(&client, 7).await?, b"echo me");
        Ok(())
    })
}

#[test]
fn send_bundle_spans_bundles_backpressure_and_racing_publication(
) -> Result<(), Box<dyn std::error::Error>> {
    const BUF_CNT: usize = 600;
    const BUF_LEN: usize = 1024;
    const EXTRA: usize = 40;

    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        client.set_send_buffer_size(4096).await?;
        let ring = SendBufRing::builder(7)
            .buf_cnt(BUF_CNT as u16)
            .buf_len(BUF_LEN)
            .build()?;
        let initial = BUF_CNT - EXTRA;
        for index in 0..initial {
            publish(&ring, &pattern(index, BUF_LEN));
        }

        // More than the kernel's 256-buffer bundle limit, through a small
        // socket buffer: the request needs several bundles and blocking retries.
        let total = BUF_CNT * BUF_LEN;
        let reader = norn_executor::spawn(async move { recv_exact(&server, total).await });
        let mut send = Box::pin(client.send_bundle(&ring));
        poll_fn(|cx| {
            let _ = send.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        for index in initial..BUF_CNT {
            publish(&ring, &pattern(index, BUF_LEN));
        }

        let mut requests = vec![send.await?];
        while ring.queued() > 0 {
            // Racing publications may land after the kernel found the ring
            // empty; they remain queued for the next request.
            requests.push(client.send_bundle(&ring).await?);
        }
        for sent in &requests {
            assert!(sent.bytes_sent() > 0);
            assert_eq!(sent.bytes_released(), sent.bytes_sent());
        }
        let bytes_sent: usize = requests.iter().map(|sent| sent.bytes_sent()).sum();
        let buffers: usize = requests.iter().map(|sent| sent.buffers_released()).sum();
        assert_eq!(bytes_sent, total);
        assert_eq!(buffers, BUF_CNT);
        assert_eq!(ring.available(), BUF_CNT);

        let received = reader.await.unwrap()?;
        let expected: Vec<u8> = (0..BUF_CNT).flat_map(|i| pattern(i, BUF_LEN)).collect();
        assert!(
            received == expected,
            "bundle stream reordered or lost bytes"
        );
        Ok(())
    })
}

#[test]
fn failed_bundle_releases_selected_buffers_from_kernel_head(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let (_server, client) = tcp_pair().await?;
        let ring = SendBufRing::builder(8).buf_cnt(4).buf_len(8).build()?;
        for payload in [&b"one"[..], b"two", b"three"] {
            publish(&ring, payload);
        }
        client.shutdown(std::net::Shutdown::Write).await?;

        // The kernel selects and commits the buffers before sendmsg fails, but
        // the error completion does not say how many it selected.
        let error = client.send_bundle(&ring).await.unwrap_err();
        assert_eq!(error.error().raw_os_error(), Some(libc::EPIPE));
        assert!(!error.is_quarantined());
        // The error reports the consumed but untransmitted payload.
        let progress = error.progress();
        assert_eq!(progress.bytes_sent(), 0);
        assert_eq!(progress.buffers_released(), 3);
        assert_eq!(progress.bytes_released(), 11);
        assert_eq!(ring.queued(), 0, "selected buffers must be attributed");
        assert_eq!(ring.available(), 4);

        // Accounting stays usable: the ring is not quarantined.
        publish(&ring, b"four");
        let error = io::Error::from(client.send_bundle(&ring).await.unwrap_err());
        assert_eq!(error.raw_os_error(), Some(libc::EPIPE));
        assert_eq!(ring.available(), 4);
        Ok(())
    })
}

#[test]
fn cancelled_bundle_releases_its_claim_and_consumed_buffers(
) -> Result<(), Box<dyn std::error::Error>> {
    const BUF_CNT: usize = 32;
    const BUF_LEN: usize = 64 * 1024;

    util::with_test_env(|| async {
        let (server, client) = tcp_pair().await?;
        client.set_send_buffer_size(4096).await?;
        let ring = SendBufRing::builder(9)
            .buf_cnt(BUF_CNT as u16)
            .buf_len(BUF_LEN)
            .build()?;
        for index in 0..BUF_CNT {
            publish(&ring, &pattern(index, BUF_LEN));
        }

        // Nobody reads, so the bundle blocks until it is cancelled.
        let mut send = Box::pin(client.send_bundle(&ring));
        poll_fn(|cx| {
            assert!(send.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        for _ in 0..4 {
            norn_uring::noop().await?;
        }
        drop(send);

        let mut retried = None;
        for _ in 0..64 {
            norn_uring::noop().await?;
            if ring.available() > 0 {
                retried = Some(());
                break;
            }
        }
        retried.expect("cancelled bundle never released consumed buffers");
        // The cancelled request selected the whole ring; its buffers return
        // only after the kernel head proves consumption.
        assert_eq!(ring.available() + ring.queued(), BUF_CNT);

        // The claim is released with the terminal completion.
        let drain = norn_executor::spawn(async move {
            let mut total = 0;
            loop {
                let (result, _) = server.recv(vec![0u8; 64 * 1024]).await;
                match result {
                    Ok(0) | Err(_) => return total,
                    Ok(n) => total += n,
                }
            }
        });
        if ring.queued() > 0 {
            let sent = client.send_bundle(&ring).await?;
            assert_eq!(sent.bytes_sent(), sent.bytes_released());
        }
        assert_eq!(ring.available(), BUF_CNT);
        client.shutdown(std::net::Shutdown::Write).await?;
        assert!(drain.await.unwrap() > 0);
        Ok(())
    })
}

#[test]
fn send_bundle_rejects_another_driver() -> Result<(), Box<dyn std::error::Error>> {
    let left = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let mut executor = norn_executor::LocalExecutor::new(left);
    let (_server, client) = executor.block_on(tcp_pair())?;

    let right = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let mut right = norn_executor::LocalExecutor::new(right);
    let ring = right.block_on(async { SendBufRing::builder(1).buf_cnt(1).buf_len(8).build() })?;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(client.send_bundle(&ring));
    }));
    assert!(panic.is_err());
    drop(ring);
    Ok(())
}
