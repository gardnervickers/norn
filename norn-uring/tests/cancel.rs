#![cfg(target_os = "linux")]

use norn_uring::BufResult;
use std::io;
use std::pin::pin;

use futures_util::StreamExt;
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::UdpSocket;
use norn_uring::Request;

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

        let BufResult(result, buf) = first.await;
        assert_canceled(result);
        assert_eq!(buf.capacity(), 16);
        let BufResult(result, buf) = second.await;
        assert_canceled(result);
        assert_eq!(buf.capacity(), 32);

        assert!(futures_util::poll!(unrelated.as_mut()).is_pending());
        sender
            .send_to(b"still armed".to_vec(), other.local_addr()?)
            .await
            .drop_buf()?;
        let BufResult(result, buf) = unrelated.await;
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
            sender
                .send_to(b"first".to_vec(), receiver_addr)
                .await
                .drop_buf()?;
            let (buf, addr) = stream.next().await.expect("multishot stream ended")?;
            assert_eq!(addr, sender.local_addr()?);
            assert_eq!(&buf[..], b"first");
            drop(buf);

            assert_eq!(receiver.as_uring_fd().cancel_pending().await?, 1);
            assert_canceled(stream.next().await.expect("missing cancellation"));
            assert!(stream.next().await.is_none());
        }

        sender
            .send_to(b"second".to_vec(), receiver_addr)
            .await
            .drop_buf()?;
        let BufResult(result, buf) = receiver.recv_from(Vec::<u8>::with_capacity(16)).await;
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

/// Requests still waiting for submission-queue space are submitted ahead of
/// the cancellation, so it matches them too.
#[test]
fn cancel_pending_matches_requests_waiting_for_queue_space(
) -> Result<(), Box<dyn std::error::Error>> {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 4)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let receiver = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        // Twice the ring's four entries, so half wait for queue space.
        let mut receives: Vec<_> = (0..8)
            .map(|_| Box::pin(receiver.recv(Vec::<u8>::with_capacity(8))))
            .collect();
        for receive in &mut receives {
            assert!(futures_util::poll!(receive.as_mut()).is_pending());
        }

        assert!(receiver.as_uring_fd().cancel_pending().await? > 0);
        for receive in receives {
            assert_canceled(receive.await.0);
        }
        Ok(())
    })
}

/// A request linked behind one on another descriptor is not issued until its
/// predecessor completes, so the cancellation does not see it.
#[test]
fn cancel_pending_skips_link_successors_not_yet_issued() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let head = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let tail = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let sender = UdpSocket::bind("127.0.0.1:0".parse()?).await?;
        let readable = libc::POLLIN as u32;
        let mut chain = Box::pin(
            head.poll_readiness(readable)
                .then(tail.poll_readiness(readable)),
        );
        assert!(futures_util::poll!(chain.as_mut()).is_pending());

        assert_eq!(tail.as_uring_fd().cancel_pending().await?, 0);

        sender
            .send_to(b"head".to_vec(), head.local_addr()?)
            .await
            .drop_buf()?;
        sender
            .send_to(b"tail".to_vec(), tail.local_addr()?)
            .await
            .drop_buf()?;
        let (first, second) = chain.await;
        assert!(first?.is_readable());
        assert!(second?.is_readable());
        Ok(())
    })
}

/// Canceling a request does not fail the request hard-linked after it: the
/// kernel issues that one instead, so the cancellation has to be repeated.
#[test]
fn repeated_cancel_pending_reaches_hard_linked_requests() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let socket = std::rc::Rc::new(UdpSocket::bind("127.0.0.1:0".parse()?).await?);
        let readable = libc::POLLIN as u32;
        let done = std::rc::Rc::new(std::cell::Cell::new(false));
        let chain = {
            let (socket, done) = (std::rc::Rc::clone(&socket), std::rc::Rc::clone(&done));
            norn_executor::spawn(async move {
                let results = socket
                    .poll_readiness(readable)
                    .then_always(socket.poll_readiness(readable))
                    .await;
                done.set(true);
                results
            })
        };
        let settle = || async {
            for _ in 0..4 {
                norn_uring::noop().await?;
            }
            Ok::<_, io::Error>(())
        };
        settle().await?;

        assert_eq!(socket.as_uring_fd().cancel_pending().await?, 1);
        settle().await?;
        assert!(
            !done.get(),
            "the hard-linked poll was issued and still waits"
        );

        assert_eq!(socket.as_uring_fd().cancel_pending().await?, 1);
        settle().await?;
        assert!(done.get());
        let (first, second) = chain.await.unwrap();
        assert_canceled(first);
        assert_canceled(second);
        assert_eq!(socket.as_uring_fd().cancel_pending().await?, 0);
        Ok(())
    })
}
