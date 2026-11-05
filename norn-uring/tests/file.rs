#![cfg(target_os = "linux")]

use bytes::{Bytes, BytesMut};
use futures_core::Future;
use futures_util::future::poll_fn;
use norn_uring::fixedbuf::{AcquireError, FixedBuffer, RegisterErrorKind, UnregisterErrorKind};
use norn_uring::{fs, Request, WithOptions};
use std::ffi::OsString;
use std::os::unix::ffi::OsStringExt;
use std::task::Poll;
use std::time::Duration;

mod util;

#[test]
fn default_open_options_preserve_creation_permissions() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let mut modes = Vec::new();
        for (name, mut options) in [
            ("default", fs::OpenOptions::default()),
            ("new", fs::OpenOptions::new()),
            ("zero", {
                let mut options = fs::OpenOptions::default();
                options.mode(0);
                options
            }),
        ] {
            let path = dir.join(name);
            options
                .write(true)
                .create_new(true)
                .open(&path)
                .await?
                .close()
                .await?;
            modes.push(fs::metadata(&path).await?.stx_mode as u32 & 0o777);
        }
        // Compare under the existing umask without changing process-global state.
        assert_eq!(modes[0], modes[1]);
        assert_eq!(modes[2], 0);
        Ok(())
    })
}

#[test]
fn open_close() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("testfile");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true);

        let file = opts.open(path).await?;
        file.close().await?;
        Ok(())
    })
}

#[test]
fn filesystem_paths_preserve_non_utf8_bytes() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let file_path = dir.join(OsString::from_vec(b"file-\xff".to_vec()));
        let renamed_path = dir.join(OsString::from_vec(b"renamed-\xfe".to_vec()));
        let directory_path = dir.join(OsString::from_vec(b"directory-\xfd".to_vec()));

        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);
        let file = opts.open(&file_path).await?;
        file.close().await?;

        fs::rename(&file_path, &renamed_path).await?;
        assert!(fs::metadata(&renamed_path).await.is_ok());
        fs::remove_file(&renamed_path).await?;

        fs::create_dir(&directory_path).await?;
        assert!(fs::statx(
            &directory_path,
            libc::AT_STATX_SYNC_AS_STAT,
            libc::STATX_BASIC_STATS,
        )
        .await
        .is_ok());
        fs::remove_dir(&directory_path).await?;
        Ok(())
    })
}

#[test]
fn filesystem_paths_reject_embedded_nul() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join(OsString::from_vec(b"embedded\0nul".to_vec()));

        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true);
        let error = opts.open(&path).await.expect_err("embedded NUL must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);

        let error = fs::create_dir(&path)
            .await
            .expect_err("embedded NUL must fail");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        Ok(())
    })
}

#[test]
fn file_round_trips_through_uring_fd() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("uring-fd-round-trip");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(path).await?;
        let fd = file.into_uring_fd();
        let file = fs::File::from_uring_fd(fd);
        let (result, buffer) = file.write_at(b"round-trip".to_vec(), 0).await;
        assert_eq!(result?, buffer.len());
        file.close().await?;
        Ok(())
    })
}

#[test]
fn create_uses_readable_default_mode() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("created-mode");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(&path).await?;
        file.close().await?;

        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true);
        let file = opts.open(&path).await?;
        file.close().await?;

        Ok(())
    })
}

#[test]
fn read_write() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("testfile");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(path).await?;
        let buf = b"hello world";
        let (res, _) = file.write_at(&buf[..], 0).await;
        let n = res?;
        assert_eq!(n, buf.len());
        let buf = vec![0; buf.len()];
        let (res, buf) = file.read_at(buf, 0).await;
        let n = res?;
        assert_eq!(n, buf.len());
        assert_eq!(buf, b"hello world");
        Ok(())
    })
}

/// io_uring treats an SQE offset of `-1` (`u64::MAX`) as "use the file
/// cursor". Positional operations must reject every offset above `i64::MAX`
/// instead of silently reading or writing at the cursor.
#[test]
fn positional_ops_reject_offsets_above_i64_max() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("offset");
        std::fs::write(&path, b"hello world")?;
        let mut opts = fs::OpenOptions::new();
        opts.write(true).read(true);
        let file = opts.open(&path).await?;

        for offset in [u64::MAX, i64::MAX as u64 + 1] {
            let (result, buf) = file.write_at(&b"XY"[..], offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf, b"XY");

            let (result, buf) = file.read_at(vec![0_u8; 4], offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(buf, [0_u8; 4]);

            let (result, bufs) = file.writev_at(vec![b"XY".to_vec()], offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(bufs, [b"XY".to_vec()]);

            let (result, bufs) = file.readv_at(vec![vec![0_u8; 4]], offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(bufs, [vec![0_u8; 4]]);

            let pool = norn_uring::Handle::current()
                .register_fixed_buffers(vec![b"fixed payload".to_vec()])?;
            let (result, buf) = file.write_fixed_at(pool.try_acquire()?, offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(&*buf, b"fixed payload");
            let (result, buf) = file.read_fixed_at(buf, offset).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            assert_eq!(&*buf, b"fixed payload");
            drop(buf);
            assert_eq!(pool.unregister()?.len(), 1);
        }

        assert_eq!(std::fs::read(&path)?, b"hello world");
        file.close().await?;
        Ok(())
    })
}

#[repr(C, align(4096))]
struct AlignedBlock([u8; 4096]);

// Safety: the array is inline in `AlignedBlock`, and fixed-buffer registration
// captures its address only after the wrapper reaches final pool storage.
unsafe impl FixedBuffer for AlignedBlock {
    fn fixed_region(&mut self) -> &mut [u8] {
        &mut self.0
    }
}

#[test]
fn fixed_buffers_support_custom_inline_storage() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-inline");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true).read(true);
        let file = opts.open(path).await?;

        let pool = norn_uring::Handle::current()
            .register_fixed_buffers(vec![AlignedBlock([0; 4096]), AlignedBlock([0; 4096])])?;
        let payload = b"caller-selected fixed buffer";

        let mut write_buf = pool.try_acquire()?;
        write_buf.set_range(0..payload.len())?;
        write_buf.as_full_slice_mut().copy_from_slice(payload);
        write_buf.set_len(payload.len())?;
        let (result, write_buf) = file.write_fixed_at(write_buf, 0).await;
        assert_eq!(result?, payload.len());
        drop(write_buf);

        let mut read_buf = pool.try_acquire()?;
        read_buf.set_range(0..payload.len())?;
        let (result, read_buf) = file.read_fixed_at(read_buf, 0).await;
        assert_eq!(result?, payload.len());
        assert_eq!(read_buf.len(), payload.len());
        assert_eq!(&*read_buf, payload);
        drop(read_buf);

        let buffers = pool.unregister()?;
        assert_eq!(buffers.len(), 2);
        Ok(())
    })
}

#[test]
fn fixed_buffer_range_and_logical_len_bound_the_io() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-range");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true).read(true);
        let file = opts.open(&path).await?;

        let pool = norn_uring::Handle::current().register_fixed_buffers(vec![[0u8; 32]])?;
        let mut buffer = pool.try_acquire()?;
        buffer.set_range(8..24)?;
        buffer.set_payload(b"hello")?;
        let (result, mut buffer) = file.write_fixed_at(buffer, 0).await;
        assert_eq!(result?, 5);
        assert_eq!(std::fs::read(&path)?, b"hello");

        buffer.set_range(12..20)?;
        let (result, buffer) = file.read_fixed_at(buffer, 0).await;
        assert_eq!(result?, 5);
        assert_eq!(buffer.range(), 12..20);
        assert_eq!(&*buffer, b"hello");
        drop(buffer);

        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
fn fixed_buffers_support_heterogeneous_and_projected_types(
) -> Result<(), Box<dyn std::error::Error>> {
    fn cursor_region(cursor: &mut std::io::Cursor<[u8; 64]>) -> &mut [u8] {
        cursor.get_mut().as_mut_slice()
    }

    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-erased-and-foreign");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true).read(true);
        let file = opts.open(&path).await?;
        let handle = norn_uring::Handle::current();
        let buffers: Vec<Box<dyn FixedBuffer>> = vec![
            Box::new(vec![0u8; 32]),
            Box::new(vec![0u8; 32].into_boxed_slice()),
            Box::new(BytesMut::from(&[0u8; 32][..])),
            Box::new([0u8; 32]),
        ];
        let pool = handle.register_fixed_buffers(buffers)?;
        let mut erased = pool.try_acquire_at(0)?;
        erased.set_payload(b"erased")?;
        let (result, erased) = file.write_fixed_at(erased, 0).await;
        assert_eq!(result?, 6);
        drop(erased);
        let acquired = (1..pool.len())
            .map(|index| pool.try_acquire_at(index))
            .collect::<Result<Vec<_>, _>>()?;
        for (expected, buffer) in acquired.iter().enumerate() {
            let expected = expected + 1;
            assert_eq!(buffer.index(), expected);
            assert_eq!(buffer.capacity(), 32);
        }
        drop(acquired);
        assert_eq!(pool.unregister()?.len(), 4);

        let mut cursor = std::io::Cursor::new([0u8; 64]);
        cursor.set_position(7);
        // Safety: the projection selects the cursor's initialized inline array.
        // The cursor is inaccessible and never moved while registered.
        let pool = unsafe { handle.register_fixed_buffers_with(vec![cursor], cursor_region)? };
        let mut projected = pool.try_acquire()?;
        projected.set_range(8..32)?;
        projected.set_payload(b"foreign")?;
        let (result, projected) = file.write_fixed_at(projected, 16).await;
        assert_eq!(result?, 7);
        drop(projected);
        let recovered = pool.unregister()?;
        assert_eq!(recovered.len(), 1);
        let recovered = recovered[0].as_ref().expect("the slot stays installed");
        assert_eq!(recovered.position(), 7);
        assert_eq!(&recovered.get_ref()[8..15], b"foreign");
        Ok(())
    })
}

#[test]
fn fixed_buffer_table_reports_exclusion_and_recovers() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let handle = norn_uring::Handle::current();
        let pool = handle.register_fixed_buffers(vec![vec![0u8; 32]])?;
        let held = pool.try_acquire()?;
        assert_eq!(
            pool.try_acquire_at(0).unwrap_err(),
            AcquireError::InUse { index: 0 }
        );

        let err = pool.unregister().unwrap_err();
        assert!(matches!(
            err.kind(),
            UnregisterErrorKind::Busy { acquired: 1 }
        ));
        let pool = err.into_table();
        drop(held);

        let second = handle
            .register_fixed_buffers(vec![vec![0u8; 8]])
            .unwrap_err();
        assert!(matches!(
            second.kind(),
            RegisterErrorKind::AlreadyRegistered
        ));
        assert_eq!(second.into_buffers().len(), 1);

        assert_eq!(pool.unregister()?.len(), 1);
        let replacement = handle.register_fixed_buffers(vec![vec![0u8; 8]])?;
        assert_eq!(replacement.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
fn fixed_read_error_preserves_payload_and_unpolled_op_holds_slot(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-read-error");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true);
        let file = opts.open(path).await?;

        let pool = norn_uring::Handle::current()
            .register_fixed_buffers(vec![b"existing payload".to_vec()])?;
        let buffer = pool.try_acquire()?;
        let op = file.read_fixed_at(buffer, 0);
        assert_eq!(
            pool.try_acquire_at(0).unwrap_err(),
            AcquireError::InUse { index: 0 }
        );

        drop(op);
        let buffer = pool.try_acquire_at(0)?;
        let (result, buffer) = file.read_fixed_at(buffer, 0).await;
        assert!(result.is_err());
        assert_eq!(&*buffer, b"existing payload");
        drop(buffer);
        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
fn submitted_then_cancelled_fixed_op_holds_slot_until_terminal_cqe(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-cancel");
        std::fs::write(&path, b"completion")?;
        let file = fs::File::open(path).await?;
        let pool = norn_uring::Handle::current().register_fixed_buffers(vec![vec![0u8; 32]])?;

        let buffer = pool.try_acquire()?;
        let mut op = Box::pin(file.read_fixed_at(buffer, 0));
        poll_fn(|cx| {
            assert!(op.as_mut().poll(cx).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(op);
        assert_eq!(pool.try_acquire().unwrap_err(), AcquireError::Exhausted);

        let mut recovered = None;
        for _ in 0..4 {
            norn_uring::noop().await.unwrap();
            if let Ok(buffer) = pool.try_acquire() {
                recovered = Some(buffer);
                break;
            }
        }
        drop(recovered.expect("cancelled fixed operation did not reach a terminal CQE"));
        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
fn fixed_operations_reject_another_driver_but_ordinary_io_composes(
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    let path = dir.join("fixed-driver-identity");
    std::fs::write(&path, b"cross-driver ordinary read")?;

    let left_driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let mut left_executor = norn_executor::LocalExecutor::new(left_driver);
    let file = left_executor.block_on(fs::File::open(&path))?;

    let right_driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let right_handle = right_driver.handle();
    let pool = right_handle.register_fixed_buffers(vec![vec![0u8; 64]])?;

    let wrong_driver = pool.try_acquire()?;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        drop(file.read_fixed_at(wrong_driver, 0));
    }));
    assert!(panic.is_err());

    let ordinary = pool.try_acquire_at(0)?;
    let (result, ordinary) = left_executor.block_on(file.read_at(ordinary, 0));
    let n = result?;
    assert_eq!(&ordinary[..n], b"cross-driver ordinary read");
    drop(ordinary);

    left_executor.block_on(file.close())?;
    assert_eq!(pool.unregister()?.len(), 1);
    drop(right_handle);
    drop(right_driver);
    Ok(())
}

#[test]
fn readv_writev() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("vectored");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(path).await?;
        let bufs = vec![Bytes::from_static(b"hello "), Bytes::from_static(b"world")];
        let (res, _) = file.writev_at(bufs, 0).await;
        assert_eq!(res?, 11);

        let bufs = vec![vec![0u8; 8], vec![0u8; 8]];
        let (res, bufs) = file.readv_at(bufs, 0).await;
        let n = res?;
        assert_eq!(n, 11);
        assert_eq!(bufs[0], b"hello wo");
        assert_eq!(bufs[1], b"rld");

        Ok(())
    })
}

#[test]
fn read_write_options_round_trip() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("rw-options");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true).read(true);
        let file = opts.open(&path).await?;

        let dsync = fs::WriteOptions::new().dsync(true);
        let (res, _) = file.write_at(&b"hello "[..], 0).options(dsync).await;
        assert_eq!(res?, 6);

        let sync = fs::WriteOptions::new().sync(true);
        let bufs = vec![
            Bytes::from_static(b"vectored "),
            Bytes::from_static(b"and "),
        ];
        let (res, _) = file.writev_at_with(bufs, 6, sync).await;
        assert_eq!(res?, 13);

        // Regular files ignore write streams, so a placement hint never
        // changes the bytes written.
        let table = norn_uring::Handle::current().register_fixed_buffers(vec![[0u8; 16]])?;
        let mut buffer = table.try_acquire()?;
        buffer.set_payload(b"fixed")?;
        let stream = fs::WriteOptions::new()
            .dsync(true)
            .stream(fs::WriteStream::new(1));
        let (res, mut buffer) = file.write_fixed_at_with(buffer, 19, stream).await;
        assert_eq!(res?, 5);
        assert_eq!(std::fs::read(&path)?, b"hello vectored and fixed");

        let read_opts = fs::ReadOptions::new();
        let (res, buf) = file.read_at(vec![0u8; 5], 0).options(read_opts).await;
        assert_eq!(res?, 5);
        assert_eq!(buf, b"hello");

        let bufs = vec![vec![0u8; 9], vec![0u8; 4]];
        let (res, bufs) = file.readv_at_with(bufs, 6, read_opts).await;
        assert_eq!(res?, 13);
        assert_eq!(bufs[0], b"vectored ");
        assert_eq!(bufs[1], b"and ");

        buffer.clear();
        let (res, buffer) = file.read_fixed_at_with(buffer, 19, read_opts).await;
        assert_eq!(res?, 5);
        assert_eq!(buffer.as_slice(), b"fixed");
        drop(buffer);
        assert_eq!(table.unregister()?.len(), 1);

        file.close().await?;
        Ok(())
    })
}

/// `RWF_NOWAIT` fails a read that would wait instead of waiting for data.
/// Without the flag, io_uring waits for the empty pipe to become readable.
#[test]
fn nowait_read_of_an_empty_pipe_fails_instead_of_waiting() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let (reader, writer) = fs::pipe()?;
        let reader = fs::File::from_uring_fd(reader.into_uring_fd());
        let writer = fs::File::from_uring_fd(writer.into_uring_fd());
        let nowait = fs::ReadOptions::new().nowait(true);

        // The timeout only keeps a read that ignored the flag from hanging the
        // test; it cancels such a read, which then fails with `ECANCELED`.
        let (res, buf) = reader
            .read_at(vec![7u8; 4], 0)
            .options(nowait)
            .timeout(Duration::from_secs(5))
            .await;
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(buf, [7u8; 4]);

        let mut read = std::pin::pin!(reader.read_at(vec![0u8; 4], 0));
        assert!(poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx).is_pending())).await);
        writer.write_at(&b"data"[..], 0).await.0?;
        let (res, buf) = read.await;
        assert_eq!(res?, 4);
        assert_eq!(buf, b"data");

        // With data waiting in the pipe, the flag does not fail the read.
        writer.write_at(&b"more"[..], 0).await.0?;
        let (res, buf) = reader.read_at(vec![0u8; 4], 0).options(nowait).await;
        assert_eq!(res?, 4);
        assert_eq!(buf, b"more");

        reader.close().await?;
        writer.close().await?;
        Ok(())
    })
}

#[test]
fn unsupported_rw_flags_fail_without_losing_buffers() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        // `/dev/null` does not support uncached buffered I/O. Kernels before
        // 6.14 reject the unknown flag with the same error, so this observes
        // that the flag reaches the kernel on every supported version.
        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true);
        let file = opts.open("/dev/null").await?;

        let read_opts = fs::ReadOptions::new().dontcache(true);
        let (res, buf) = file.read_at(vec![1u8; 4], 0).options(read_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(buf, [1u8; 4]);

        let bufs = vec![vec![1u8; 2]];
        let (res, bufs) = file.readv_at_with(bufs, 0, read_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(bufs, [vec![1u8; 2]]);

        let write_opts = fs::WriteOptions::new().dontcache(true);
        let (res, buf) = file.write_at(vec![2u8; 4], 0).options(write_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(buf, [2u8; 4]);

        let bufs = vec![vec![3u8; 2]];
        let (res, bufs) = file.writev_at_with(bufs, 0, write_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(bufs, [vec![3u8; 2]]);

        let table = norn_uring::Handle::current().register_fixed_buffers(vec![[4u8; 4]])?;
        let mut buffer = table.try_acquire()?;
        buffer.set_payload(&[4u8; 4])?;
        let (res, buffer) = file.write_fixed_at_with(buffer, 0, write_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        let (res, buffer) = file.read_fixed_at_with(buffer, 0, read_opts).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::EOPNOTSUPP));
        assert_eq!(buffer.as_slice(), [4u8; 4]);
        drop(buffer);
        assert_eq!(table.unregister()?.len(), 1);

        // Default options submit the plain request, which `/dev/null` accepts.
        let (res, _) = file
            .write_at(vec![5u8; 4], 0)
            .options(fs::WriteOptions::new())
            .await;
        assert_eq!(res?, 4);

        file.close().await?;
        Ok(())
    })
}

#[test]
fn set_len() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("set_len");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(&path).await?;

        let initial = b"hello world";
        file.write_at(&initial[..], 0).await.0?;

        file.set_len(5).await?;
        assert_eq!(std::fs::metadata(&path)?.len(), 5);

        let (res, buf) = file.read_at(vec![0; 16], 0).await;
        let n = res?;
        assert_eq!(n, 5);
        assert_eq!(&buf[..n], b"hello");

        file.set_len(32).await?;
        assert_eq!(std::fs::metadata(&path)?.len(), 32);

        file.close().await?;
        Ok(())
    })
}

#[test]
fn path_and_metadata_ops() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let original = dir.join("original");
        let renamed = dir.join("renamed");
        let hardlink = dir.join("hardlink");
        let symlink = dir.join("symlink");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true);
        let file = opts.open(&original).await?;
        file.write_at(&b"abc"[..], 0).await.0?;
        file.close().await?;

        match fs::rename(&original, &renamed).await {
            Ok(()) => {}
            Err(err)
                if err.kind() == std::io::ErrorKind::PermissionDenied
                    || err.raw_os_error() == Some(libc::EPERM)
                    || err.raw_os_error() == Some(libc::EACCES)
                    || err.raw_os_error() == Some(libc::EOPNOTSUPP) =>
            {
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        }
        assert!(!original.exists());
        assert!(renamed.exists());

        let hard_linked = match fs::hard_link(&renamed, &hardlink).await {
            Ok(()) => {
                match std::fs::read(&hardlink) {
                    Ok(bytes) => assert_eq!(bytes, b"abc"),
                    Err(err)
                        if err.kind() == std::io::ErrorKind::PermissionDenied
                            || err.raw_os_error() == Some(libc::EPERM)
                            || err.raw_os_error() == Some(libc::EACCES)
                            || err.raw_os_error() == Some(libc::EOPNOTSUPP) =>
                    {
                        return Ok(());
                    }
                    Err(err) => return Err(err.into()),
                }
                true
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::PermissionDenied
                    || err.raw_os_error() == Some(libc::EPERM)
                    || err.raw_os_error() == Some(libc::EOPNOTSUPP) =>
            {
                false
            }
            Err(err) => return Err(err.into()),
        };

        match fs::symlink(&renamed, &symlink).await {
            Ok(()) => match std::fs::read_link(&symlink) {
                Ok(target) => assert_eq!(target, renamed),
                Err(err)
                    if err.kind() == std::io::ErrorKind::PermissionDenied
                        || err.raw_os_error() == Some(libc::EPERM)
                        || err.raw_os_error() == Some(libc::EOPNOTSUPP) => {}
                Err(err) => return Err(err.into()),
            },
            Err(err)
                if err.kind() == std::io::ErrorKind::PermissionDenied
                    || err.raw_os_error() == Some(libc::EPERM)
                    || err.raw_os_error() == Some(libc::EOPNOTSUPP) => {}
            Err(err) => return Err(err.into()),
        }

        let stat = match fs::metadata(&renamed).await {
            Ok(stat) => stat,
            Err(err)
                if err.kind() == std::io::ErrorKind::PermissionDenied
                    || err.raw_os_error() == Some(libc::EPERM)
                    || err.raw_os_error() == Some(libc::ENOSYS)
                    || err.raw_os_error() == Some(libc::EOPNOTSUPP) =>
            {
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };
        assert_eq!(stat.stx_size, 3);
        if hard_linked {
            assert!(stat.stx_nlink >= 2);
        }

        let stat = match fs::statx(
            &renamed,
            libc::AT_STATX_SYNC_AS_STAT,
            libc::STATX_BASIC_STATS,
        )
        .await
        {
            Ok(stat) => stat,
            Err(err)
                if err.kind() == std::io::ErrorKind::PermissionDenied
                    || err.raw_os_error() == Some(libc::EPERM)
                    || err.raw_os_error() == Some(libc::ENOSYS)
                    || err.raw_os_error() == Some(libc::EOPNOTSUPP) =>
            {
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };
        assert_eq!(stat.stx_size, 3);

        Ok(())
    })
}

#[test]
fn create_remove_dir() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("subdir");

        fs::create_dir(&path).await?;
        assert!(path.is_dir());

        fs::remove_dir(&path).await?;
        assert!(!path.exists());

        Ok(())
    })
}

#[test]
fn advise_and_xattr_ops() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("xattr-file");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true).read(true);

        let file = opts.open(&path).await?;
        file.write_at(&b"payload"[..], 0).await.0?;
        file.advise(0, 0, libc::POSIX_FADV_SEQUENTIAL).await?;
        file.advise(0, u64::from(u32::MAX), libc::POSIX_FADV_WILLNEED)
            .await?;
        let err = file
            .advise(0, 1 << 32, libc::POSIX_FADV_DONTNEED)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);

        let fd_name = b"user.norn.fd";
        let fd_expected = b"fd-value";
        let fd_value = Bytes::from_static(fd_expected);
        let (res, _) = file.set_xattr(fd_name, fd_value, 0).await;
        match res {
            Ok(()) => {}
            Err(err) if util::xattr_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        }

        let (res, buf) = file.get_xattr(fd_name, vec![0u8; 32]).await;
        let n = match res {
            Ok(n) => n,
            Err(err) if util::xattr_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert_eq!(&buf[..n], b"fd-value");

        let (res, buf) = file.get_xattr(fd_name, Vec::<u8>::new()).await;
        assert_eq!(res?, fd_expected.len());
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 0);

        let exact = vec![0u8; fd_expected.len()].into_boxed_slice();
        let (res, exact) = file.get_xattr(fd_name, exact).await;
        assert_eq!(res?, fd_expected.len());
        assert_eq!(&*exact, fd_expected);

        let undersized = vec![0u8; fd_expected.len() - 1].into_boxed_slice();
        let (res, undersized) = file.get_xattr(fd_name, undersized).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ERANGE));
        assert_eq!(undersized.len(), fd_expected.len() - 1);

        let path_name = b"user.norn.path";
        let path_expected = b"path-value";
        let path_value = Bytes::from_static(path_expected);
        let (res, _) = fs::set_xattr(&path, path_name, path_value, 0).await;
        match res {
            Ok(()) => {}
            Err(err) if util::xattr_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        }

        let (res, buf) = fs::get_xattr(&path, path_name, vec![0u8; 32]).await;
        let n = match res {
            Ok(n) => n,
            Err(err) if util::xattr_unsupported(&err) => return Ok(()),
            Err(err) => return Err(err.into()),
        };
        assert_eq!(&buf[..n], b"path-value");

        let (res, buf) = fs::get_xattr(&path, path_name, Vec::<u8>::new()).await;
        assert_eq!(res?, path_expected.len());
        assert!(buf.is_empty());
        assert_eq!(buf.capacity(), 0);

        let exact = vec![0u8; path_expected.len()].into_boxed_slice();
        let (res, exact) = fs::get_xattr(&path, path_name, exact).await;
        assert_eq!(res?, path_expected.len());
        assert_eq!(&*exact, path_expected);

        let undersized = vec![0u8; path_expected.len() - 1].into_boxed_slice();
        let (res, undersized) = fs::get_xattr(&path, path_name, undersized).await;
        assert_eq!(res.unwrap_err().raw_os_error(), Some(libc::ERANGE));
        assert_eq!(undersized.len(), path_expected.len() - 1);

        file.close().await?;
        Ok(())
    })
}

#[test]
fn drop_file_outside_runtime() -> Result<(), Box<dyn std::error::Error>> {
    let file = util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("drop-outside-runtime");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).write(true);
        let file = opts.open(path).await?;
        Ok(file)
    })?;

    drop(file);
    Ok(())
}

#[test]
fn splice_and_tee() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let src_path = dir.join("splice-src");
        let dst_path = dir.join("splice-dst");
        let tee_left_path = dir.join("tee-left");
        let tee_right_path = dir.join("tee-right");
        let payload = b"splice and tee payload";
        let len = payload.len() as u32;

        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).read(true).write(true);

        let src = opts.open(&src_path).await?;
        src.write_at(&payload[..], 0).await.0?;

        let dst = opts.open(&dst_path).await?;
        let tee_left = opts.open(&tee_left_path).await?;
        let tee_right = opts.open(&tee_right_path).await?;

        let (pipe_reader, pipe_writer) = fs::pipe()?;
        assert_eq!(
            src.splice_to_pipe(&pipe_writer, Some(0), len, 0).await?,
            payload.len()
        );
        assert_eq!(
            dst.splice_from_pipe(&pipe_reader, Some(0), len, 0).await?,
            payload.len()
        );

        let (res, buf) = dst.read_at(vec![0; payload.len()], 0).await;
        assert_eq!(res?, payload.len());
        assert_eq!(&buf[..], payload);

        let (source_reader, source_writer) = fs::pipe()?;
        let (dup_reader, dup_writer) = fs::pipe()?;
        assert_eq!(
            source_writer.splice_from(&src, Some(0), len, 0).await?,
            payload.len()
        );
        assert_eq!(
            source_reader.tee_to(&dup_writer, len, 0).await?,
            payload.len()
        );
        assert_eq!(
            source_reader.splice_to(&tee_left, Some(0), len, 0).await?,
            payload.len()
        );
        assert_eq!(
            dup_reader.splice_to(&tee_right, Some(0), len, 0).await?,
            payload.len()
        );

        let (res, buf) = tee_left.read_at(vec![0; payload.len()], 0).await;
        assert_eq!(res?, payload.len());
        assert_eq!(&buf[..], payload);

        let (res, buf) = tee_right.read_at(vec![0; payload.len()], 0).await;
        assert_eq!(res?, payload.len());
        assert_eq!(&buf[..], payload);

        Ok(())
    })
}

/// `SPLICE_F_FD_IN_FIXED`, which `libc` does not export.
const SPLICE_F_FD_IN_FIXED: u32 = 1 << 31;

/// Run a pipe test on its own named thread and fail instead of hanging if a
/// splice or tee never completes.
fn pipe_watchdog(
    name: &str,
    f: impl FnOnce() -> Result<(), Box<dyn std::error::Error>> + Send + 'static,
) {
    let (tx, rx) = std::sync::mpsc::channel();
    let thread = std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let res = f().map_err(|err| err.to_string());
            let _ = tx.send(res);
        })
        .unwrap();
    match rx.recv_timeout(std::time::Duration::from_secs(10)) {
        Ok(res) => {
            thread.join().unwrap();
            res.unwrap();
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            if let Err(panic) = thread.join() {
                std::panic::resume_unwind(panic);
            }
            unreachable!("pipe test thread exited without a result");
        }
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            panic!("{name}: a pipe operation did not complete within 10s")
        }
    }
}

fn pipe_test_opts() -> fs::OpenOptions {
    let mut opts = fs::OpenOptions::new();
    opts.create(true).truncate(true).read(true).write(true);
    opts
}

/// Let spawned pipe operations reach the kernel, fail with `EAGAIN`, and arm
/// their readiness wait.
async fn settle_pipe_ops(probe: &fs::File) -> std::io::Result<()> {
    for _ in 0..3 {
        probe.read_at(vec![0u8; 1], 0).await.0?;
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

#[test]
fn pipe_splice_consumer_waits_for_producer() {
    pipe_watchdog("pipe_splice_consumer_waits_for_producer", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let src = pipe_test_opts().open(dir.join("src")).await?;
            src.write_at(vec![7u8; 4096], 0).await.0?;
            let dst = std::rc::Rc::new(pipe_test_opts().open(dir.join("dst")).await?);
            let (reader, writer) = fs::pipe()?;
            let reader = std::rc::Rc::new(reader);

            let consumer = {
                let (reader, dst) = (reader.clone(), dst.clone());
                norn_executor::spawn(async move {
                    dst.splice_from_pipe(&reader, Some(0), 4096, 0).await
                })
            };
            settle_pipe_ops(&src).await?;
            assert_eq!(writer.splice_from(&src, Some(0), 4096, 0).await?, 4096);
            assert_eq!(consumer.await.unwrap()?, 4096);

            let (res, buf) = dst.read_at(vec![0u8; 4096], 0).await;
            assert_eq!(res?, 4096);
            assert!(buf.iter().all(|&b| b == 7));
            Ok(())
        })
    });
}

#[test]
fn pipe_splice_producer_waits_for_drain() {
    pipe_watchdog("pipe_splice_producer_waits_for_drain", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let src = std::rc::Rc::new(pipe_test_opts().open(dir.join("src")).await?);
            src.write_at(vec![3u8; 4096], 0).await.0?;
            let sink = pipe_test_opts().open(dir.join("sink")).await?;
            let (reader, writer) = fs::pipe()?;
            let writer = std::rc::Rc::new(writer);

            // Fill the pipe; the nonblocking attempt that finds it full reports it.
            let mut filled = 0;
            loop {
                match writer
                    .splice_from(&src, Some(0), 4096, libc::SPLICE_F_NONBLOCK)
                    .await
                {
                    Ok(n) => filled += n,
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(err) => return Err(err.into()),
                }
            }
            assert!(filled > 0);

            let producer = {
                let (writer, src) = (writer.clone(), src.clone());
                norn_executor::spawn(
                    async move { writer.splice_from(&src, Some(0), 4096, 0).await },
                )
            };
            settle_pipe_ops(&sink).await?;
            let mut drained = 0;
            while drained < filled {
                drained += reader.splice_to(&sink, None, 65536, 0).await?;
            }
            assert_eq!(producer.await.unwrap()?, 4096);
            assert_eq!(reader.splice_to(&sink, None, 65536, 0).await?, 4096);
            Ok(())
        })
    });
}

#[test]
fn pipe_tee_waits_for_data() {
    pipe_watchdog("pipe_tee_waits_for_data", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let src = pipe_test_opts().open(dir.join("src")).await?;
            src.write_at(vec![5u8; 512], 0).await.0?;
            let sink = pipe_test_opts().open(dir.join("sink")).await?;
            let (reader, writer) = fs::pipe()?;
            let (dup_reader, dup_writer) = fs::pipe()?;
            let reader = std::rc::Rc::new(reader);

            let tee = {
                let reader = reader.clone();
                norn_executor::spawn(async move { reader.tee_to(&dup_writer, 512, 0).await })
            };
            settle_pipe_ops(&src).await?;
            assert_eq!(writer.splice_from(&src, Some(0), 512, 0).await?, 512);
            assert_eq!(tee.await.unwrap()?, 512);

            // tee leaves the source pipe's data in place.
            assert_eq!(reader.splice_to(&sink, Some(0), 512, 0).await?, 512);
            assert_eq!(dup_reader.splice_to(&sink, Some(512), 512, 0).await?, 512);
            Ok(())
        })
    });
}

#[test]
fn pipe_splice_nonblock_flag_returns_would_block() {
    pipe_watchdog("pipe_splice_nonblock_flag_returns_would_block", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let dst = pipe_test_opts().open(dir.join("dst")).await?;
            let (reader, _writer) = fs::pipe()?;
            let (_dup_reader, dup_writer) = fs::pipe()?;

            let err = dst
                .splice_from_pipe(&reader, Some(0), 4096, libc::SPLICE_F_NONBLOCK)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            let err = reader
                .splice_to(&dst, Some(0), 4096, libc::SPLICE_F_NONBLOCK)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            let err = reader
                .tee_to(&dup_writer, 4096, libc::SPLICE_F_NONBLOCK)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            Ok(())
        })
    });
}

#[test]
fn pipe_splice_writer_drop_yields_eof() {
    pipe_watchdog("pipe_splice_writer_drop_yields_eof", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let dst = std::rc::Rc::new(pipe_test_opts().open(dir.join("dst")).await?);
            let (reader, writer) = fs::pipe()?;
            let (_dup_reader, dup_writer) = fs::pipe()?;
            let reader = std::rc::Rc::new(reader);

            let splice = {
                let (reader, dst) = (reader.clone(), dst.clone());
                norn_executor::spawn(async move {
                    dst.splice_from_pipe(&reader, Some(0), 4096, 0).await
                })
            };
            let tee = {
                let reader = reader.clone();
                norn_executor::spawn(async move { reader.tee_to(&dup_writer, 4096, 0).await })
            };
            settle_pipe_ops(&dst).await?;
            drop(writer);
            assert_eq!(splice.await.unwrap()?, 0);
            assert_eq!(tee.await.unwrap()?, 0);
            assert_eq!(reader.splice_to(&dst, Some(0), 4096, 0).await?, 0);
            Ok(())
        })
    });
}

#[test]
fn pipe_dropped_waiting_splice_releases_descriptor() {
    pipe_watchdog("pipe_dropped_waiting_splice_releases_descriptor", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let dst = pipe_test_opts().open(dir.join("dst")).await?;
            let (reader, _writer) = fs::pipe()?;
            {
                let mut splice = std::pin::pin!(dst.splice_from_pipe(&reader, Some(0), 4096, 0));
                for _ in 0..3 {
                    poll_fn(|cx| {
                        assert!(splice.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    settle_pipe_ops(&dst).await?;
                }
            }
            settle_pipe_ops(&dst).await?;
            reader.close().await?;
            Ok(())
        })
    });
}

#[test]
fn pipe_shutdown_with_waiting_splice_completes() {
    pipe_watchdog("pipe_shutdown_with_waiting_splice_completes", || {
        let writer = util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let dst = pipe_test_opts().open(dir.join("dst")).await?;
            let probe = pipe_test_opts().open(dir.join("probe")).await?;
            let (reader, writer) = fs::pipe()?;
            let _waiting = norn_executor::spawn(async move {
                let _ = dst.splice_from_pipe(&reader, Some(0), 4096, 0).await;
            });
            settle_pipe_ops(&probe).await?;
            // Keep the write end open past shutdown so the splice never sees EOF.
            Ok(writer)
        })?;
        drop(writer);
        Ok(())
    });
}

#[test]
fn pipe_splice_rejects_unsupported_flags() {
    pipe_watchdog("pipe_splice_rejects_unsupported_flags", || {
        util::with_test_env(|| async {
            let dir = util::ThreadNameTestDir::new();
            let src = pipe_test_opts().open(dir.join("src")).await?;
            src.write_at(vec![1u8; 64], 0).await.0?;
            let dst = pipe_test_opts().open(dir.join("dst")).await?;
            let (reader, writer) = fs::pipe()?;
            let (_dup_reader, dup_writer) = fs::pipe()?;
            assert_eq!(writer.splice_from(&src, Some(0), 64, 0).await?, 64);

            for flags in [SPLICE_F_FD_IN_FIXED, 0x10] {
                let errs = [
                    dst.splice_from_pipe(&reader, Some(0), 64, flags).await,
                    reader.splice_to(&dst, Some(0), 64, flags).await,
                    reader.tee_to(&dup_writer, 64, flags).await,
                    src.splice_to_pipe(&writer, Some(0), 64, flags).await,
                    writer.splice_from(&src, Some(0), 64, flags).await,
                ];
                for err in errs {
                    assert_eq!(err.unwrap_err().raw_os_error(), Some(libc::EINVAL));
                }
            }
            // Nothing was submitted, so the pipe still holds its data.
            assert_eq!(reader.splice_to(&dst, Some(0), 64, 0).await?, 64);
            Ok(())
        })
    });
}

#[test]
fn pipe_splice_stops_on_eagain_from_the_file_side() {
    pipe_watchdog("pipe_splice_stops_on_eagain_from_the_file_side", || {
        util::with_test_env(|| async {
            // A `File` wrapping an empty nonblocking pipe returns EAGAIN while
            // the destination pipe is writable, which waiting cannot fix.
            let (empty_reader, _empty_writer) = fs::pipe()?;
            let not_ready = fs::File::from_uring_fd(empty_reader.into_uring_fd());
            let (_reader, writer) = fs::pipe()?;
            let err = writer
                .splice_from(&not_ready, None, 4096, 0)
                .await
                .unwrap_err();
            assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
            Ok(())
        })
    });
}
