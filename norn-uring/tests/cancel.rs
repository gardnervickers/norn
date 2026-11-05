#![cfg(target_os = "linux")]

use std::io;
use std::pin::pin;

use futures_util::StreamExt;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::UdpSocket;

mod util;

fn assert_canceled<T: std::fmt::Debug>(result: io::Result<T>) {
    let err = result.unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::ECANCELED), "{err}");
}

#[test]
fn cancel_pending_cancels_only_requests_on_descriptor() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let receiver = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let other = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        assert_eq!(receiver.as_uring_fd().cancel_pending().await?, 0);

        let mut first = Box::pin(receiver.recv_from(Vec::<u8>::with_capacity(16)));
        let mut second = Box::pin(receiver.recv(Vec::<u8>::with_capacity(32)));
        let mut unrelated = Box::pin(other.recv_from(Vec::<u8>::with_capacity(16)));
        assert!(futures_util::poll!(first.as_mut()).is_pending());
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        assert!(futures_util::poll!(unrelated.as_mut()).is_pending());

        // The receives are still queued in the SQ ahead of the cancellation.
        assert_eq!(receiver.as_uring_fd().cancel_pending().await?, 2);

        let (result, buf) = first.await;
        assert_canceled(result);
        assert_eq!(buf.capacity(), 16);
        let (result, buf) = second.await;
        assert_canceled(result);
        assert_eq!(buf.capacity(), 32);

        assert!(futures_util::poll!(unrelated.as_mut()).is_pending());
        sender
            .send_to(b"still armed".to_vec(), other.local_addr()?)
            .await
            .0?;
        let (result, buf) = unrelated.await;
        assert_eq!(result?, (11, sender.local_addr()?));
        assert_eq!(&buf[..], b"still armed");

        // Canceled requests released their descriptor leases.
        receiver.close().await?;
        other.close().await?;
        sender.close().await?;
        Ok(())
    })
}

#[test]
fn cancel_pending_ends_multishot_receive_without_consuming_later_data(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let ring = RecvBufRing::builder(30).buf_cnt(4).buf_len(2048).build()?;
        let receiver = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let receiver_addr = receiver.local_addr()?;

        {
            let mut stream = pin!(receiver.recv_from_ring_multi(&ring));
            sender.send_to(b"first".to_vec(), receiver_addr).await.0?;
            let (buf, addr) = stream.next().await.expect("multishot stream ended")?;
            assert_eq!(addr, sender.local_addr()?);
            assert_eq!(&buf[..], b"first");
            drop(buf);

            assert_eq!(receiver.as_uring_fd().cancel_pending().await?, 1);
            assert_canceled(stream.next().await.expect("missing cancellation"));
            assert!(stream.next().await.is_none());
        }

        sender.send_to(b"second".to_vec(), receiver_addr).await.0?;
        let (result, buf) = receiver.recv_from(Vec::<u8>::with_capacity(16)).await;
        assert_eq!(result?.0, 6);
        assert_eq!(&buf[..], b"second");

        receiver.close().await?;
        sender.close().await?;
        drop(ring);
        Ok(())
    })
}

#[test]
fn cancel_pending_removes_multishot_poll() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let socket = UdpSocket::bind("127.0.0.1:0".parse()?).await?;

        {
            let mut events = pin!(socket.poll_readiness_multi(libc::POLLIN as u32));
            assert!(futures_util::poll!(events.next()).is_pending());

            assert_eq!(socket.as_uring_fd().cancel_pending().await?, 1);
            assert_canceled(events.next().await.expect("missing cancellation"));
            assert!(events.next().await.is_none());
        }

        socket.close().await?;
        Ok(())
    })
}
