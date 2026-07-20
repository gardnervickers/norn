#![cfg(target_os = "linux")]

use bytes::{Bytes, BytesMut};
use futures_core::Future;
use futures_util::future::poll_fn;
use norn_uring::fixedbuf::{AcquireError, FixedBuffer, RegisterErrorKind, UnregisterErrorKind};
use norn_uring::fs;
use std::task::Poll;

mod util;

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
        assert_eq!(recovered[0].position(), 7);
        assert_eq!(&recovered[0].get_ref()[8..15], b"foreign");
        Ok(())
    })
}

#[test]
fn fixed_buffer_pool_reports_exclusion_and_recovers() -> Result<(), Box<dyn std::error::Error>> {
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
        let pool = err.into_pool();
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
            norn_uring::noop().await;
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
