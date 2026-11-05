#![cfg(target_os = "linux")]

//! Scatter and gather I/O over segments of one registered fixed buffer.

use futures_core::Future;
use futures_util::future::poll_fn;
use norn_uring::fixedbuf::AcquireError;
use norn_uring::fs;
use std::task::Poll;

mod util;

#[test]
fn fixed_vectored_io_uses_segments_of_one_slot() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-vectored");
        let mut opts = fs::OpenOptions::new();
        opts.create(true).truncate(true).write(true).read(true);
        let file = opts.open(&path).await?;

        let pool = norn_uring::Handle::current().register_fixed_buffers(vec![vec![0u8; 64]])?;
        let mut buffer = pool.try_acquire()?;
        buffer.set_range(8..48)?;
        buffer.as_full_slice_mut()[..4].copy_from_slice(b"head");
        buffer.as_full_slice_mut()[20..26].copy_from_slice(b"-tail!");
        buffer.set_len(3)?;

        // Segments are relative to the selected view and may be discontiguous.
        let (result, mut buffer) = file.writev_fixed_at(buffer, &[0..4, 20..26], 2).await;
        assert_eq!(result?, 10);
        assert_eq!(buffer.len(), 3, "gather writes ignore the logical length");
        assert_eq!(std::fs::read(&path)?, b"\0\0head-tail!");

        buffer.as_full_slice_mut().fill(b'.');
        let (result, buffer) = file
            .readv_fixed_at(buffer, &[30..34, 0..2, 10..14], 2)
            .await;
        assert_eq!(result?, 10);
        assert_eq!(buffer.len(), 3, "scatter reads do not choose a prefix");
        let view = buffer.as_full_slice();
        assert_eq!(&view[30..34], b"head");
        assert_eq!(&view[0..2], b"-t");
        assert_eq!(&view[10..14], b"ail!");
        assert_eq!(&view[4..10], b"......");
        drop(buffer);

        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn fixed_vectored_io_rejects_invalid_segments_without_submission(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-vectored-invalid");
        std::fs::write(&path, b"unchanged")?;
        let mut opts = fs::OpenOptions::new();
        opts.read(true).write(true);
        let file = opts.open(&path).await?;

        let pool = norn_uring::Handle::current().register_fixed_buffers(vec![vec![7u8; 16]])?;
        let mut buffer = pool.try_acquire()?;
        buffer.set_range(4..12)?;
        let too_many = vec![0..1; 1025];
        #[allow(clippy::reversed_empty_ranges)]
        let invalid: [&[std::ops::Range<usize>]; 5] =
            [&[], &[0..0], &[2..1], &[4..9], too_many.as_slice()];
        for segments in invalid {
            let (result, returned) = file.readv_fixed_at(buffer, segments, 0).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            let (result, returned) = file.writev_fixed_at(returned, segments, 0).await;
            assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
            buffer = returned;
        }
        assert_eq!(buffer.as_full_slice(), &[7u8; 8]);
        assert_eq!(std::fs::read(&path)?, b"unchanged");

        // A segment ending exactly at the selected capacity is valid.
        let (result, buffer) = file.readv_fixed_at(buffer, &[0..8], 0).await;
        assert_eq!(result?, 8);
        assert_eq!(buffer.as_full_slice(), b"unchange");
        drop(buffer);
        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn fixed_vectored_ops_hold_the_slot_until_completion() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-vectored-slot");
        std::fs::write(&path, b"slot")?;
        let file = fs::File::open(&path).await?;
        let pool = norn_uring::Handle::current().register_fixed_buffers(vec![vec![0u8; 8]])?;

        let op = file.readv_fixed_at(pool.try_acquire()?, &[0..4], 0);
        assert_eq!(pool.try_acquire().unwrap_err(), AcquireError::Exhausted);
        drop(op);

        let mut op = Box::pin(file.readv_fixed_at(pool.try_acquire()?, &[4..8], 0));
        poll_fn(|cx| {
            let _ = op.as_mut().poll(cx);
            Poll::Ready(())
        })
        .await;
        assert_eq!(pool.try_acquire().unwrap_err(), AcquireError::Exhausted);
        let (result, buffer) = op.await;
        assert_eq!(result?, 4);
        assert_eq!(&buffer.as_full_slice()[4..8], b"slot");
        drop(buffer);
        assert_eq!(pool.unregister()?.len(), 1);
        Ok(())
    })
}

#[test]
#[allow(clippy::single_range_in_vec_init)]
fn fixed_vectored_ops_reject_another_driver() -> Result<(), Box<dyn std::error::Error>> {
    let dir = util::ThreadNameTestDir::new();
    let path = dir.join("fixed-vectored-driver");
    std::fs::write(&path, b"driver")?;

    let left_driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let mut left_executor = norn_executor::LocalExecutor::new(left_driver);
    let file = left_executor.block_on(fs::File::open(&path))?;

    let right_driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 16)?;
    let pool = right_driver
        .handle()
        .register_fixed_buffers(vec![vec![0u8; 8]])?;
    for write in [false, true] {
        let buffer = pool.try_acquire()?;
        let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            if write {
                drop(file.writev_fixed_at(buffer, &[0..1], 0));
            } else {
                drop(file.readv_fixed_at(buffer, &[0..1], 0));
            }
        }));
        assert!(panic.is_err());
    }
    left_executor.block_on(file.close())?;
    assert_eq!(pool.unregister()?.len(), 1);
    drop(right_driver);
    Ok(())
}
