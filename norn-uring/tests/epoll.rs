#![cfg(target_os = "linux")]

use norn_uring::BufResult;
use std::io;
use std::net::SocketAddr;
use std::time::Duration;

use norn_uring::epoll::{Epoll, Events, Interest};
use norn_uring::net::UdpSocket;
use norn_uring::Request;

mod util;

fn epoll_supported() -> bool {
    let supported = util::opcode_supported(io_uring::opcode::EpollCtl::CODE)
        && util::opcode_supported(io_uring::opcode::EpollWait::CODE);
    if !supported {
        eprintln!("skipping: IORING_OP_EPOLL_WAIT requires Linux 6.15");
    }
    supported
}

fn localhost() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn send_datagram(from: &UdpSocket, to: &UdpSocket) -> io::Result<()> {
    from.send_to(b"ping".to_vec(), to.local_addr()?)
        .await
        .drop_buf()?;
    Ok(())
}

async fn assert_no_events(epoll: &Epoll) -> io::Result<Events> {
    let BufResult(result, events) = epoll
        .wait(Events::with_capacity(4))
        .timeout(Duration::from_millis(20))
        .await;
    assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
    assert!(events.is_empty());
    Ok(events)
}

#[test]
fn wait_reports_registered_readiness() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let receiver = UdpSocket::bind(localhost()).await?;
        let sender = UdpSocket::bind(localhost()).await?;
        epoll
            .add(receiver.as_uring_fd(), Interest::READABLE, 42)
            .await?;

        send_datagram(&sender, &receiver).await?;
        let BufResult(result, events) = epoll.wait(Events::with_capacity(4)).await;

        assert_eq!(result?, 1);
        assert_eq!(events.len(), 1);
        let event = events.iter().next().unwrap();
        assert_eq!(event.token(), 42);
        assert!(event.readiness().is_readable());
        assert!(!event.readiness().is_writable());

        epoll.close().await?;
        receiver.close().await?;
        sender.close().await?;
        Ok(())
    })
}

#[test]
fn wait_stays_pending_until_readiness() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let receiver = UdpSocket::bind(localhost()).await?;
        let sender = UdpSocket::bind(localhost()).await?;
        epoll
            .add(receiver.as_uring_fd(), Interest::READABLE, 1)
            .await?;

        let mut wait = Box::pin(epoll.wait(Events::with_capacity(4)));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        norn_uring::noop().await.unwrap();
        assert!(futures_util::poll!(wait.as_mut()).is_pending());

        send_datagram(&sender, &receiver).await?;
        let BufResult(result, events) = wait.await;

        assert_eq!(result?, 1);
        assert_eq!(events.iter().next().unwrap().token(), 1);
        Ok(())
    })
}

#[test]
fn modify_and_delete_update_interest_list() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let socket = UdpSocket::bind(localhost()).await?;
        let fd = socket.as_uring_fd();

        epoll.add(fd, Interest::READABLE, 1).await?;
        let events = assert_no_events(&epoll).await?;

        epoll.modify(fd, Interest::WRITABLE, 2).await?;
        let BufResult(result, events) = epoll.wait(events).await;
        assert_eq!(result?, 1);
        let event = events.iter().next().unwrap();
        assert_eq!(event.token(), 2);
        assert!(event.readiness().is_writable());

        epoll.delete(fd).await?;
        assert_no_events(&epoll).await?;

        let err = epoll.delete(fd).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        let err = epoll.modify(fd, Interest::READABLE, 3).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENOENT));
        Ok(())
    })
}

#[test]
fn duplicate_and_unsupported_registrations_fail() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let socket = UdpSocket::bind(localhost()).await?;
        epoll
            .add(socket.as_uring_fd(), Interest::READABLE, 1)
            .await?;

        let err = epoll
            .add(socket.as_uring_fd(), Interest::READABLE, 1)
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EEXIST));

        let dir = util::ThreadNameTestDir::new();
        let file = norn_uring::fs::File::with_options()
            .write(true)
            .create(true)
            .open(dir.join("regular"))
            .await?;
        let err = epoll
            .add(file.as_uring_fd(), Interest::READABLE, 2)
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EPERM));

        let err = epoll
            .add(epoll.as_uring_fd(), Interest::READABLE, 3)
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
        Ok(())
    })
}

#[test]
fn oneshot_registration_requires_rearm() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let socket = UdpSocket::bind(localhost()).await?;
        let fd = socket.as_uring_fd();
        epoll.add(fd, Interest::WRITABLE.oneshot(), 5).await?;

        let BufResult(result, events) = epoll.wait(Events::with_capacity(4)).await;
        assert_eq!(result?, 1);
        assert_no_events(&epoll).await?;

        epoll.modify(fd, Interest::WRITABLE.oneshot(), 6).await?;
        let BufResult(result, events) = epoll.wait(events).await;
        assert_eq!(result?, 1);
        assert_eq!(events.iter().next().unwrap().token(), 6);
        Ok(())
    })
}

#[test]
fn edge_triggered_registration_reports_transitions() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let receiver = UdpSocket::bind(localhost()).await?;
        let sender = UdpSocket::bind(localhost()).await?;
        epoll
            .add(
                receiver.as_uring_fd(),
                Interest::READABLE.edge_triggered(),
                8,
            )
            .await?;

        send_datagram(&sender, &receiver).await?;
        let BufResult(result, _events) = epoll.wait(Events::with_capacity(4)).await;
        assert_eq!(result?, 1);
        // The datagram is still queued, but no new transition occurred.
        assert_no_events(&epoll).await?;

        send_datagram(&sender, &receiver).await?;
        let BufResult(result, events) = epoll.wait(Events::with_capacity(4)).await;
        assert_eq!(result?, 1);
        assert_eq!(events.iter().next().unwrap().token(), 8);
        Ok(())
    })
}

#[test]
fn wait_truncates_to_capacity() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let first = UdpSocket::bind(localhost()).await?;
        let second = UdpSocket::bind(localhost()).await?;
        epoll
            .add(first.as_uring_fd(), Interest::WRITABLE, 1)
            .await?;
        epoll
            .add(second.as_uring_fd(), Interest::WRITABLE, 2)
            .await?;

        let BufResult(result, events) = epoll.wait(Events::with_capacity(1)).await;
        assert_eq!(result?, 1);
        assert_eq!(events.len(), 1);

        let BufResult(result, events) = epoll.wait(Events::with_capacity(4)).await;
        assert_eq!(result?, 2);
        let mut tokens: Vec<_> = events.iter().map(|event| event.token()).collect();
        tokens.sort_unstable();
        assert_eq!(tokens, [1, 2]);

        let BufResult(result, events) = epoll.wait(Events::with_capacity(0)).await;
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EINVAL));
        assert!(events.is_empty());
        Ok(())
    })
}

#[test]
fn dropped_wait_is_cancelled() -> Result<(), Box<dyn std::error::Error>> {
    if !epoll_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let epoll = Epoll::new()?;
        let receiver = UdpSocket::bind(localhost()).await?;
        let sender = UdpSocket::bind(localhost()).await?;
        epoll
            .add(receiver.as_uring_fd(), Interest::READABLE, 3)
            .await?;

        let mut wait = Box::pin(epoll.wait(Events::with_capacity(4)));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        norn_uring::noop().await.unwrap();
        drop(wait);
        norn_uring::noop().await.unwrap();

        // Level-triggered readiness is not consumed by the cancelled wait.
        send_datagram(&sender, &receiver).await?;
        let BufResult(result, events) = epoll.wait(Events::with_capacity(4)).await;
        assert_eq!(result?, 1);
        assert_eq!(events.iter().next().unwrap().token(), 3);

        // Leave a submitted wait pending when the root future finishes; the
        // driver must cancel and drain it during shutdown.
        let mut wait = std::pin::pin!(epoll.wait(Events::with_capacity(4)));
        receiver.recv(vec![0; 4]).await.drop_buf()?;
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        Ok(())
    })
}
