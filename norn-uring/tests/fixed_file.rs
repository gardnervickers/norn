#![cfg(target_os = "linux")]

use std::io::{Read, Write};
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;
use std::pin::pin;

use norn_uring::fs::{File, OpenOptions};
use norn_uring::Handle;

mod util;

fn read_write_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(true);
    options
}

#[test]
fn direct_open_supports_file_io_and_close() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(4)?;
        let file = table.open(dir.join("data"), &read_write_options()).await?;
        assert_eq!(table.available(), 3);

        let (written, _) = file.write_at(b"hello fixed".to_vec(), 0).await;
        assert_eq!(written?, 11);
        file.sync().await?;
        file.datasync().await?;
        let (read, buf) = file.read_at(Vec::with_capacity(32), 0).await;
        assert_eq!(read?, 11);
        assert_eq!(&buf[..], b"hello fixed");
        assert_eq!(std::fs::read(dir.join("data"))?, b"hello fixed");

        file.close().await?;
        assert_eq!(table.available(), 4);
        Ok(())
    })
}

#[test]
fn direct_open_errors_release_the_reserved_slot() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(1)?;
        let mut options = OpenOptions::new();
        options.read(true);
        let err = table.open(dir.join("missing"), &options).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
        assert_eq!(table.available(), 1);

        let first = table.open(dir.join("a"), &read_write_options()).await?;
        let err = table
            .open(dir.join("b"), &read_write_options())
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ENFILE));
        drop(first);
        let second = table.open(dir.join("b"), &read_write_options()).await?;
        assert_eq!(second.index(), 0);
        Ok(())
    })
}

#[test]
fn files_update_insert_outlives_the_source_descriptor() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    std::fs::write(dir.join("data"), b"inserted")?;
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(2)?;
        let source = File::open(dir.join("data")).await?;

        // A request reserves its slot when first polled. Dropping it then
        // returns the slot once the driver has reaped the update.
        let mut pending = Box::pin(table.insert(source.as_uring_fd()));
        assert_eq!(table.available(), 2);
        assert!(futures_util::poll!(pending.as_mut()).is_pending());
        assert_eq!(table.available(), 1);
        drop(pending);
        norn_uring::noop().await?;
        assert_eq!(table.available(), 2);

        let fixed = table.insert(source.as_uring_fd()).await?;
        source.close().await?;
        let (read, buf) = fixed.read_at(Vec::with_capacity(16), 0).await;
        assert_eq!(read?, 8);
        assert_eq!(&buf[..], b"inserted");
        Ok(())
    })
}

#[test]
fn install_creates_an_independent_process_descriptor() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    std::fs::write(dir.join("data"), b"installed")?;
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(1)?;
        let mut options = OpenOptions::new();
        options.read(true);
        let fixed = table.open(dir.join("data"), &options).await?;
        let installed = match fixed.install().await {
            Ok(fd) => fd,
            Err(err) if err.raw_os_error() == Some(libc::EINVAL) => {
                eprintln!("skipping: IORING_OP_FIXED_FD_INSTALL is unsupported");
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };

        fixed.close().await?;
        assert_eq!(table.available(), 1);
        let file = File::from_uring_fd(installed);
        let (read, buf) = file.read_at(Vec::with_capacity(16), 0).await;
        assert_eq!(read?, 9);
        assert_eq!(&buf[..], b"installed");
        file.close().await?;
        Ok(())
    })
}

#[test]
fn send_and_recv_on_an_inserted_socket() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(1)?;
        let (local, mut peer) = UnixStream::pair()?;
        let fixed = table.insert_fd(local.as_fd())?;
        // The table's reference keeps the socket open.
        drop(local);

        let (sent, _) = fixed.send(b"ping".to_vec()).await;
        assert_eq!(sent?, 4);
        let mut buf = [0_u8; 4];
        peer.read_exact(&mut buf)?;
        assert_eq!(&buf, b"ping");

        peer.write_all(b"pong")?;
        let (received, buf) = fixed.recv(Vec::with_capacity(8)).await;
        assert_eq!(received?, 4);
        assert_eq!(&buf[..], b"pong");

        // Dropping the last owner removes the table reference and closes it.
        drop(fixed);
        let mut rest = Vec::new();
        assert_eq!(peer.read_to_end(&mut rest)?, 0);
        Ok(())
    })
}

#[test]
fn in_flight_requests_retain_their_slot() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(2)?;
        let (local, mut peer) = UnixStream::pair()?;
        let fixed = table.insert_fd(local.as_fd())?;
        drop(local);
        let index = fixed.index();

        let mut recv = pin!(fixed.recv(Vec::with_capacity(8)));
        assert!(futures_util::poll!(&mut recv).is_pending());

        let err = fixed.close().await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(table.available(), 1, "the submitted recv retains its slot");

        // A new install cannot reuse the retained index.
        let (other, _other_peer) = UnixStream::pair()?;
        let replacement = table.insert_fd(other.as_fd())?;
        assert_ne!(replacement.index(), index);

        peer.write_all(b"late")?;
        let (received, buf) = recv.await;
        assert_eq!(received?, 4);
        assert_eq!(&buf[..], b"late");
        assert_eq!(table.available(), 1);

        let mut rest = Vec::new();
        assert_eq!(peer.read_to_end(&mut rest)?, 0);
        Ok(())
    })
}

#[test]
fn fixed_buffers_work_with_fixed_files() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    util::with_test_env(|| async move {
        let handle = Handle::current();
        let table = handle.register_fixed_files(1)?;
        let pool = handle
            .register_fixed_buffers(vec![vec![0_u8; 64]])
            .map_err(|err| err.kind().to_string())?;
        let file = table.open(dir.join("data"), &read_write_options()).await?;

        let mut buf = pool.try_acquire()?;
        buf.set_payload(b"both registered")?;
        let (written, mut buf) = file.write_fixed_at(buf, 0).await;
        assert_eq!(written?, 15);

        buf.reset_range();
        let (read, buf) = file.read_fixed_at(buf, 0).await;
        assert_eq!(read?, 15);
        assert_eq!(&buf[..], b"both registered");
        drop(buf);
        drop(file);
        pool.unregister().map_err(|err| err.kind().to_string())?;
        Ok(())
    })
}

#[test]
fn the_table_keeps_an_inserted_socket_open_until_its_slot_is_released(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(2)?;
        let (local, mut peer) = UnixStream::pair()?;
        let fixed = table.insert_fd(local.as_fd())?;
        assert_eq!(table.available(), 1);
        // The table's reference keeps the socket open.
        drop(local);
        peer.set_nonblocking(true)?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            peer.read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        // Dropping the last owner removes the table reference and closes it.
        drop(fixed);
        assert_eq!(table.available(), 2);
        assert_eq!(peer.read(&mut byte)?, 0);
        Ok(())
    })
}
