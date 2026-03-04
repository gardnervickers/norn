#![cfg(target_os = "linux")]

use std::cell::Cell;
use std::rc::Rc;
use std::time::Duration;

use norn_uring::fs;
use norn_uring::net::{TcpListener, TcpSocket};
use norn_uring::Request;

mod util;

#[test]
fn linked_file_write_then_read_observes_prior_write() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("linked-file-write-read");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).read(true).write(true);

        let file = opts.open(path).await?;
        let ((write_res, _), (read_res, buf)) = file
            .write_at(&b"hello"[..], 0)
            .then(file.read_at(vec![0; 5], 0))
            .await;

        assert_eq!(write_res?, 5);
        let n = read_res?;
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");

        file.close().await?;
        Ok(())
    })
}

#[test]
fn then_aux_preserves_primary_result_for_real_file_ops() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("linked-file-sync");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).read(true).write(true);

        let file = opts.open(&path).await?;
        let payload = &b"persisted"[..];
        let aux_ran = Rc::new(Cell::new(false));
        let aux_seen = Rc::clone(&aux_ran);
        let (write_res, returned) = file
            .write_at(payload, 0)
            .then_aux(
                file.read_at(vec![0; payload.len()], 0)
                    .map(move |(read_res, buf)| {
                        let n = read_res.expect("auxiliary read should succeed");
                        assert_eq!(&buf[..n], b"persisted");
                        aux_seen.set(true);
                    }),
            )
            .await;

        assert_eq!(write_res?, returned.len());
        assert!(aux_ran.get(), "auxiliary request should complete");
        file.close().await?;
        Ok(())
    })
}

#[test]
fn map_transforms_real_file_op_output() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("mapped-file-read");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).read(true).write(true);

        let file = opts.open(path).await?;
        file.write_at(&b"mapped"[..], 0).await.0?;

        let payload = file
            .read_at(vec![0; 16], 0)
            .map(|(read_res, buf)| {
                let n = read_res.expect("read should succeed");
                buf[..n].to_vec()
            })
            .await;

        assert_eq!(payload, b"mapped");
        file.close().await?;
        Ok(())
    })
}

#[test]
fn linked_tcp_send_then_recv_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;

        let server_task = norn_executor::spawn(async move {
            let (server, peer) = listener.accept().await?;
            listener.close().await?;

            let ((recv_res, buf), (send_res, _)) = server
                .recv(vec![0; 4])
                .then(server.send(&b"pong"[..]))
                .await;

            let n = recv_res?;
            assert_eq!(n, 4);
            assert_eq!(&buf[..n], b"ping");
            assert_eq!(peer, server.peer_addr()?);
            assert_eq!(send_res?, 4);

            server.close().await?;
            Ok::<(), Box<dyn std::error::Error>>(())
        });

        let client = TcpSocket::connect(addr).await?;
        let ((send_res, _), (recv_res, buf)) = client
            .send(&b"ping"[..])
            .then(client.recv(vec![0; 4]))
            .await;

        assert_eq!(send_res?, 4);
        let n = recv_res?;
        assert_eq!(n, 4);
        assert_eq!(&buf[..n], b"pong");

        client.close().await?;
        server_task.await??;
        Ok(())
    })
}

#[test]
fn linked_timeout_cancels_blocked_recv() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 32).await?;
        let addr = listener.local_addr()?;

        let server_task = norn_executor::spawn(async move {
            let (server, _) = listener.accept().await?;
            Ok::<_, Box<dyn std::error::Error>>(server)
        });

        let client = TcpSocket::connect(addr).await?;
        let (recv_res, buf) = client
            .recv(vec![0; 1])
            .timeout(Duration::from_millis(10))
            .await;

        assert_eq!(recv_res.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        assert_eq!(buf.len(), 1);

        client.close().await?;
        let server = server_task.await??;
        server.close().await?;
        Ok(())
    })
}
