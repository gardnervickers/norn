#![cfg(target_os = "linux")]
//! Regression test: `TcpStream` readiness notifications must not accumulate
//! while a reader keeps up with its input.
//!
//! The test counts `ReadyStream` `EAGAIN` retries through the `log` facade by
//! matching the `poll_op.would_block` trace message, so it installs a global
//! logger and lives in its own test binary.

use std::future::poll_fn;
use std::io::Write;
use std::pin::{pin, Pin};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;

use norn_uring::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, ReadBuf};

const TCP_LOG_TARGET: &str = "norn_uring::net::tcp";
static WOULD_BLOCK: AtomicUsize = AtomicUsize::new(0);

struct WouldBlockCounter;

impl log::Log for WouldBlockCounter {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == TCP_LOG_TARGET
    }

    fn log(&self, record: &log::Record<'_>) {
        if record.target() == TCP_LOG_TARGET && record.args().to_string() == "poll_op.would_block" {
            WOULD_BLOCK.fetch_add(1, Ordering::SeqCst);
        }
    }

    fn flush(&self) {}
}

/// Poll a one-byte read exactly once.
async fn read_once(mut reader: Pin<&mut TcpStream>) -> Poll<std::io::Result<()>> {
    let mut storage = [0_u8; 1];
    let mut buf = ReadBuf::new(&mut storage);
    poll_fn(|cx| Poll::Ready(reader.as_mut().poll_read(cx, &mut buf))).await
}

/// A reader that always finds data never observes `EAGAIN`. Readiness
/// notifications produced while it keeps up must not be retained, or the next
/// `EAGAIN` replays them inside one `poll_read`, issuing one failing `recv(2)`
/// per stale notification.
#[test]
fn readiness_does_not_accumulate_while_reader_keeps_up() -> Result<(), Box<dyn std::error::Error>> {
    const ARRIVALS: usize = 1000;
    log::set_logger(&WouldBlockCounter).expect("logger already installed");
    log::set_max_level(log::LevelFilter::Trace);

    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
        let mut peer = std::net::TcpStream::connect(listener.local_addr()?)?;
        peer.set_nodelay(true)?;
        let (server, _) = listener.accept().await?;
        let mut reader = pin!(server.into_stream());

        // No data yet: the read observes EAGAIN and waits for readiness.
        assert!(read_once(reader.as_mut()).await.is_pending());
        assert!(
            WOULD_BLOCK.load(Ordering::SeqCst) > 0,
            "the EAGAIN trace message was never logged; trace logging may be compiled out, \
             which would make this test pass vacuously"
        );

        // Keep exactly one byte of backlog so every read succeeds without
        // EAGAIN while each new arrival wakes the socket's wait queue.
        peer.write_all(b"ab")?;
        norn_uring::noop().await.unwrap();
        assert!(matches!(
            read_once(reader.as_mut()).await,
            Poll::Ready(Ok(()))
        ));
        for _ in 0..ARRIVALS {
            peer.write_all(b"c")?;
            norn_uring::noop().await.unwrap();
            assert!(matches!(
                read_once(reader.as_mut()).await,
                Poll::Ready(Ok(()))
            ));
        }
        assert!(matches!(
            read_once(reader.as_mut()).await,
            Poll::Ready(Ok(()))
        ));

        // The socket is now empty. One poll should observe EAGAIN and park.
        let before = WOULD_BLOCK.load(Ordering::SeqCst);
        assert!(read_once(reader.as_mut()).await.is_pending());
        let retries = WOULD_BLOCK.load(Ordering::SeqCst) - before;
        assert!(
            retries >= 1,
            "the EAGAIN of the final poll was not counted; the counter is not observing retries"
        );
        assert!(
            retries <= 2,
            "one poll_read replayed {retries} stale readiness events, issuing a failing recv for each"
        );
        drop(peer);
        Ok::<(), Box<dyn std::error::Error>>(())
    })
}
