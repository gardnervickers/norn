#![cfg(target_os = "linux")]

//! Multishot pipe reads into a provided buffer ring.

use norn_uring::fs;

mod util;

async fn write_to_pipe(
    dir: &std::path::Path,
    writer: &fs::PipeWriter,
    payload: &[u8],
) -> std::io::Result<()> {
    // PipeWriter has no direct write method; route bytes through a file.
    let path = dir.join("pipe-source");
    std::fs::write(&path, payload)?;
    let source = fs::File::open(&path).await?;
    let n = writer
        .splice_from(&source, Some(0), payload.len() as u32, 0)
        .await?;
    assert_eq!(n, payload.len());
    source.close().await
}

#[test]
fn pipe_read_ring_multi_reads_until_writer_closes() -> Result<(), Box<dyn std::error::Error>> {
    use futures_util::StreamExt;

    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let ring = norn_uring::bufring::RecvBufRing::builder(41)
            .buf_cnt(4)
            .buf_len(8)
            .build()?;
        let (reader, writer) = fs::pipe()?;
        let mut stream = Box::pin(reader.read_ring_multi(&ring));

        write_to_pipe(&dir, &writer, b"hello").await?;
        let first = stream.next().await.expect("stream ended early")?;
        assert_eq!(&first[..], b"hello");
        assert_eq!(first.capacity(), 8);

        // A payload larger than one buffer is split across several reads.
        write_to_pipe(&dir, &writer, b"0123456789abcdef!").await?;
        let mut received = Vec::new();
        while received.len() < 17 {
            let buffer = stream.next().await.expect("stream ended early")?;
            assert!(buffer.len() <= 8);
            received.extend_from_slice(&buffer);
        }
        assert_eq!(received, b"0123456789abcdef!");

        writer.close().await?;
        assert!(
            stream.next().await.is_none(),
            "writer close must end the stream"
        );
        assert!(stream.next().await.is_none());
        drop(first);
        Ok(())
    })
}

#[test]
fn pipe_read_ring_multi_waits_for_buffer_return() -> Result<(), Box<dyn std::error::Error>> {
    use futures_core::Stream;
    use futures_util::StreamExt;

    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let ring = norn_uring::bufring::RecvBufRing::builder(42)
            .buf_cnt(1)
            .buf_len(16)
            .build()?;
        let (reader, writer) = fs::pipe()?;
        let mut stream = Box::pin(reader.read_ring_multi(&ring));

        write_to_pipe(&dir, &writer, b"first").await?;
        let held = stream.next().await.unwrap()?;
        write_to_pipe(&dir, &writer, b"second").await?;

        let (waker, count) = futures_test::task::new_count_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        for _ in 0..16 {
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());
            norn_uring::noop().await?;
        }
        let wakes = count.get();
        for _ in 0..64 {
            assert!(stream.as_mut().poll_next(&mut cx).is_pending());
        }
        assert_eq!(count.get(), wakes, "an empty ring must not self-wake");

        assert_eq!(&held[..], b"first");
        drop(held);
        assert!(count.get() > wakes, "a buffer return must wake the stream");
        assert_eq!(&stream.next().await.unwrap()?[..], b"second");
        Ok(())
    })
}

#[test]
fn pipe_read_ring_multi_rejects_non_pollable_files() -> Result<(), Box<dyn std::error::Error>> {
    use futures_util::StreamExt;

    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("regular");
        std::fs::write(&path, b"regular file")?;
        let file = fs::File::open(&path).await?;
        let reader = fs::PipeReader::from_uring_fd(file.into_uring_fd());
        let ring = norn_uring::bufring::RecvBufRing::builder(43)
            .buf_cnt(2)
            .buf_len(16)
            .build()?;
        let mut stream = Box::pin(reader.read_ring_multi(&ring));
        let error = stream
            .next()
            .await
            .expect("missing terminal error")
            .unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADFD));
        assert!(stream.next().await.is_none());
        Ok(())
    })
}
