#![cfg(target_os = "linux")]

use bytes::Bytes;
use norn_uring::fs;

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
            Ok(()) => match fs::read_link(&symlink).await {
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
