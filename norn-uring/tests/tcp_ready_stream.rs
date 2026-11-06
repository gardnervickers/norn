#![cfg(target_os = "linux")]
//! `TcpStream` readiness edge cases: polls the kernel ends, slow readers,
//! split halves waiting together, and drop, EOF, and errors mid-wait.
//!
//! `ReadyStream`'s trace messages are counted per thread through the `log`
//! facade, so this file installs a global logger and is its own test binary.

use std::cell::RefCell;
use std::collections::HashMap;
use std::future::poll_fn;
use std::io::{Read, Write};
use std::pin::{pin, Pin};
use std::sync::Once;
use std::task::Poll;

use norn_uring::net::{TcpListener, TcpStream};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

const TCP_LOG_TARGET: &str = "norn_uring::net::tcp";

thread_local! {
    static COUNTS: RefCell<HashMap<String, usize>> = RefCell::new(HashMap::new());
}

struct Counter;

impl log::Log for Counter {
    fn enabled(&self, metadata: &log::Metadata<'_>) -> bool {
        metadata.target() == TCP_LOG_TARGET
    }

    fn log(&self, record: &log::Record<'_>) {
        if record.target() == TCP_LOG_TARGET {
            let message = record.args().to_string();
            COUNTS.with(|counts| *counts.borrow_mut().entry(message).or_default() += 1);
        }
    }

    fn flush(&self) {}
}

fn install_logger() {
    static ONCE: Once = Once::new();
    ONCE.call_once(|| {
        log::set_logger(&Counter).expect("logger already installed");
        log::set_max_level(log::LevelFilter::Trace);
    });
}

fn count(message: &str) -> usize {
    COUNTS.with(|counts| counts.borrow().get(message).copied().unwrap_or(0))
}

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn run<F>(builder: io_uring::Builder, entries: u32, f: impl FnOnce() -> F) -> TestResult
where
    F: std::future::Future<Output = TestResult>,
{
    install_logger();
    let driver = norn_uring::Driver::new(builder, entries)?;
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(f())
}

async fn connected() -> Result<(TcpStream, std::net::TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
    let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
    peer.set_nodelay(true)?;
    let (server, _) = listener.accept().await?;
    Ok((server.into_stream(), peer))
}

/// Poll a read into `storage` exactly once, returning the bytes read.
async fn read_once<R: AsyncRead>(
    mut reader: Pin<&mut R>,
    storage: &mut [u8],
) -> Poll<std::io::Result<usize>> {
    let mut buf = ReadBuf::new(storage);
    poll_fn(|cx| {
        Poll::Ready(
            reader
                .as_mut()
                .poll_read(cx, &mut buf)
                .map_ok(|()| buf.filled().len()),
        )
    })
    .await
}

/// Let the driver run one turn.
#[allow(clippy::let_unit_value)]
async fn turn() {
    let _turn = norn_uring::noop().await;
}

/// Drive `fut` with driver turns until it completes, failing after `turns`.
async fn within<T>(turns: usize, what: &str, fut: impl std::future::Future<Output = T>) -> T {
    let mut fut = pin!(fut);
    for _ in 0..turns {
        if let Poll::Ready(value) = futures_util::poll!(fut.as_mut()) {
            return value;
        }
        turn().await;
    }
    panic!("{what} did not complete within {turns} driver turns");
}

/// A two-entry CQ fills while bytes keep arriving, so the kernel ends the
/// reader's multishot poll without an error. The socket is still healthy:
/// later arrivals must still wake the reader.
#[test]
fn reader_rearms_a_poll_the_kernel_ended() -> TestResult {
    let mut builder = io_uring::IoUring::builder();
    builder.setup_cqsize(2);
    run(builder, 2, || async {
        let (stream, mut peer) = connected().await?;
        let mut reader = pin!(stream);
        let mut storage = [0_u8; 1];
        assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
        let arms = count("poll_op.ready.stream_arm");
        for round in 0..16 {
            // Wait for readiness again, so each round depends on the poll.
            assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
            // Several wakeups before the driver reaps any of their CQEs.
            for _ in 0..4 {
                peer.write_all(b"x")?;
            }
            for _ in 0..4 {
                let n = within(1000, &format!("read in round {round}"), async {
                    let mut buf = ReadBuf::new(&mut storage);
                    poll_fn(|cx| reader.as_mut().poll_read(cx, &mut buf)).await?;
                    Ok::<_, std::io::Error>(buf.filled().len())
                })
                .await?;
                assert_eq!(n, 1);
            }
        }
        assert!(
            count("poll_op.ready.stream_arm") > arms + 1,
            "the kernel never ended the poll, so the test did not exercise re-arming"
        );
        Ok(())
    })
}

/// A reader slower than its writer rarely observes `EAGAIN`. The readiness
/// completions posted meanwhile must be discarded as they arrive, never
/// retained until the next `EAGAIN` and replayed as failing receives.
#[test]
fn slow_reader_discards_readiness_as_it_goes() -> TestResult {
    const TOTAL: usize = 4 << 20;
    run(io_uring::IoUring::builder(), 32, || async {
        let (stream, peer) = connected().await?;
        let mut reader = pin!(stream);
        let mut storage = vec![0_u8; 1024];
        // Arm the poll before the transfer starts.
        assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
        let (go, wait) = std::sync::mpsc::channel::<()>();
        let writer = std::thread::spawn(move || {
            let mut peer = peer;
            let chunk = [7_u8; 512];
            let mut sent = 0;
            while sent < TOTAL {
                peer.write_all(&chunk)?;
                sent += chunk.len();
            }
            wait.recv().unwrap();
            peer.shutdown(std::net::Shutdown::Write)?;
            Ok::<_, std::io::Error>(peer)
        });

        let mut received = 0;
        let mut max_stale = 0;
        let mut max_would_block = 0;
        let mut read = async |reader: Pin<&mut TcpStream>, storage: &mut [u8]| {
            let stale = count("poll_op.stale");
            let would_block = count("poll_op.would_block");
            let n = read_once_await(reader, storage).await?;
            max_stale = max_stale.max(count("poll_op.stale") - stale);
            max_would_block = max_would_block.max(count("poll_op.would_block") - would_block);
            Ok::<_, std::io::Error>(n)
        };
        while received < TOTAL {
            received += read(reader.as_mut(), &mut storage).await?;
            // Slower than the writer: let the driver reap between reads.
            turn().await;
        }
        assert_eq!(received, TOTAL);
        // The socket is empty: one poll observes `EAGAIN` and parks.
        let would_block = count("poll_op.would_block");
        assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
        let retries = count("poll_op.would_block") - would_block;
        assert!(
            retries <= 2,
            "one poll_read replayed {retries} stale readiness events, issuing a failing recv for each"
        );
        go.send(()).unwrap();
        assert_eq!(read(reader.as_mut(), &mut storage).await?, 0);
        drop(writer.join().unwrap()?);
        eprintln!(
            "slow reader: {} completions discarded in total, at most {max_stale} by one read",
            count("poll_op.stale")
        );
        // The discarded completions are those reaped since the previous
        // attempt: one turn and possibly one wait, each reaping at most a
        // 64-entry CQ.
        assert!(
            max_stale <= 128,
            "one read discarded {max_stale} readiness completions"
        );
        // One `EAGAIN` before a wait, and at most one after a wakeup whose
        // data a previous read already consumed.
        assert!(
            max_would_block <= 2,
            "one read issued {max_would_block} failing receives"
        );
        Ok(())
    })
}

/// The split halves arm `POLLIN` and `POLLOUT` polls on the same socket and
/// wait on them together while a full-duplex transfer fills both directions.
#[test]
fn split_halves_wait_together() -> TestResult {
    const TOTAL: usize = 8 << 20;
    run(io_uring::IoUring::builder(), 32, || async {
        let (stream, peer) = connected().await?;
        let (mut reader, mut writer) = stream.owned_split();
        // The peer echoes everything back, reading in small slices so both
        // directions back up.
        let echo = std::thread::spawn(move || {
            let mut peer = peer;
            let mut buf = [0_u8; 4096];
            loop {
                let n = peer.read(&mut buf)?;
                if n == 0 {
                    peer.shutdown(std::net::Shutdown::Write)?;
                    return Ok::<_, std::io::Error>(());
                }
                peer.write_all(&buf[..n])?;
            }
        });

        let write = async {
            let pattern: Vec<u8> = (0..65536 + 251).map(|i| (i % 251) as u8).collect();
            let mut sent = 0;
            while sent < TOTAL {
                let start = sent % 251;
                let len = (TOTAL - sent).min(65536);
                let slice = &pattern[start..start + len];
                sent += poll_fn(|cx| Pin::new(&mut writer).poll_write(cx, slice)).await?;
            }
            poll_fn(|cx| Pin::new(&mut writer).poll_flush(cx)).await?;
            poll_fn(|cx| Pin::new(&mut writer).poll_shutdown(cx)).await?;
            Ok::<_, std::io::Error>(())
        };
        let read = async {
            let mut storage = vec![0_u8; 32768];
            let mut received = 0;
            loop {
                let n = read_once_await(Pin::new(&mut reader), &mut storage).await?;
                if n == 0 {
                    return Ok::<_, std::io::Error>(received);
                }
                for (offset, byte) in storage[..n].iter().enumerate() {
                    assert_eq!(*byte, ((received + offset) % 251) as u8);
                }
                received += n;
            }
        };
        let (written, received) = futures_util::future::join(write, read).await;
        written?;
        assert_eq!(received?, TOTAL);
        assert!(count("poll_op.would_block") > 0);
        echo.join().unwrap()?;
        Ok(())
    })
}

async fn read_once_await<R: AsyncRead>(
    mut reader: Pin<&mut R>,
    storage: &mut [u8],
) -> std::io::Result<usize> {
    let mut buf = ReadBuf::new(storage);
    poll_fn(|cx| reader.as_mut().poll_read(cx, &mut buf)).await?;
    Ok(buf.filled().len())
}

/// Dropping a stream while its read and write both wait must release the
/// descriptor once the polls' cancellations complete, so the peer observes
/// end of stream after the data the stream had already queued.
#[test]
fn drop_while_waiting_closes_the_socket() -> TestResult {
    run(io_uring::IoUring::builder(), 32, || async {
        let (stream, mut peer) = connected().await?;
        let mut stream = Box::pin(stream);
        let mut storage = [0_u8; 16];
        assert!(read_once(stream.as_mut(), &mut storage).await.is_pending());
        peer.write_all(b"ping")?;
        assert_eq!(
            within(1000, "read", read_once_await(stream.as_mut(), &mut storage)).await?,
            4
        );
        assert!(read_once(stream.as_mut(), &mut storage).await.is_pending());

        // Fill the send buffer, since the peer does not read, until a write
        // waits for room. Now the writer's poll is armed as well.
        let arms = count("poll_op.ready.stream_arm");
        let chunk = [7_u8; 64 * 1024];
        let mut written = 0_usize;
        loop {
            let polled = poll_fn(|cx| Poll::Ready(stream.as_mut().poll_write(cx, &chunk))).await;
            match polled {
                Poll::Ready(n) => written += n?,
                Poll::Pending => break,
            }
            assert!(written < 1 << 30, "the send buffer never filled");
        }
        assert!(
            count("poll_op.ready.stream_arm") > arms,
            "the writer did not arm a readiness poll, so only the read waits at the drop"
        );

        // The write's poll is only queued so far. Let the driver submit it,
        // so that both polls are armed in the kernel at the drop.
        turn().await;
        drop(stream);
        peer.set_nonblocking(true)?;
        let mut buf = vec![0_u8; 1 << 20];
        let mut drained = 0_usize;
        for _ in 0..10_000 {
            match peer.read(&mut buf) {
                Ok(0) => {
                    assert_eq!(drained, written, "queued data was lost before the close");
                    return Ok(());
                }
                Ok(n) => {
                    assert!(buf[..n].iter().all(|b| *b == 7), "unexpected data");
                    drained += n;
                }
                Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => turn().await,
                Err(err) if err.kind() == std::io::ErrorKind::ConnectionReset => return Ok(()),
                Err(err) => return Err(err.into()),
            }
        }
        panic!("the socket stayed open after the stream was dropped");
    })
}

/// End of stream and connection errors that arrive while a read waits wake
/// it with the result of the next receive.
#[test]
fn eof_and_reset_wake_a_waiting_read() -> TestResult {
    run(io_uring::IoUring::builder(), 32, || async {
        let (stream, peer) = connected().await?;
        let mut reader = pin!(stream);
        let mut storage = [0_u8; 16];
        assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
        peer.shutdown(std::net::Shutdown::Write)?;
        let n = within(1000, "EOF", read_once_await(reader.as_mut(), &mut storage)).await?;
        assert_eq!(n, 0);
        // End of stream is sticky.
        let n = within(10, "EOF", read_once_await(reader.as_mut(), &mut storage)).await?;
        assert_eq!(n, 0);

        let (stream, peer) = connected().await?;
        let mut reader = pin!(stream);
        assert!(read_once(reader.as_mut(), &mut storage).await.is_pending());
        socket2::SockRef::from(&peer).set_linger(Some(std::time::Duration::ZERO))?;
        drop(peer);
        let err = within(
            1000,
            "reset",
            read_once_await(reader.as_mut(), &mut storage),
        )
        .await
        .expect_err("a reset connection must fail the read");
        assert_eq!(err.kind(), std::io::ErrorKind::ConnectionReset);
        Ok(())
    })
}

/// A write that waited for `POLLOUT` resumes once the peer drains, and the
/// writer's armed poll does not prevent a later flush or shutdown.
#[test]
fn write_resumes_after_waiting_then_shuts_down() -> TestResult {
    run(io_uring::IoUring::builder(), 32, || async {
        let (stream, peer) = connected().await?;
        let mut stream = pin!(stream);
        let chunk = vec![1_u8; 64 * 1024];
        let mut sent = 0;
        while let Poll::Ready(n) =
            poll_fn(|cx| Poll::Ready(stream.as_mut().poll_write(cx, &chunk))).await
        {
            sent += n?;
        }
        let drain = std::thread::spawn(move || {
            let mut peer = peer;
            let mut all = Vec::new();
            peer.read_to_end(&mut all)?;
            Ok::<_, std::io::Error>(all.len())
        });
        let n = poll_fn(|cx| stream.as_mut().poll_write(cx, &chunk)).await?;
        sent += n;
        // A second round of filling and waiting reuses the armed poll.
        for _ in 0..64 {
            sent += poll_fn(|cx| stream.as_mut().poll_write(cx, &chunk)).await?;
        }
        poll_fn(|cx| stream.as_mut().poll_flush(cx)).await?;
        poll_fn(|cx| stream.as_mut().poll_shutdown(cx)).await?;
        assert_eq!(drain.join().unwrap()?, sent);
        Ok(())
    })
}
