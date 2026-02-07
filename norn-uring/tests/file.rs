#![cfg(target_os = "linux")]

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
