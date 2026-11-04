#![cfg(target_os = "linux")]

//! Regression test for provided buffers named by failed receive completions.

use std::io::Write;
use std::os::fd::AsRawFd;
use std::pin::pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use norn_executor::park::{Park, Unpark};
use norn_uring::bufring::RecvBufRing;
use norn_uring::net::{TcpListener, TcpSocket};

async fn connected() -> Result<(TcpSocket, std::net::TcpStream), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:0".parse()?, 8).await?;
    let peer = std::net::TcpStream::connect(listener.local_addr()?)?;
    peer.set_nodelay(true)?;
    let (server, _) = listener.accept().await?;
    Ok((server, peer))
}

fn settle() {
    // Give loopback delivery and the io-wq worker time to reach a steady
    // state before the next step.
    std::thread::sleep(Duration::from_millis(50));
}

struct DrainNop;

// Safety: the NOP entry references no memory or descriptors.
unsafe impl norn_uring::Operation for DrainNop {
    type Completion = norn_uring::CQEResult;

    fn configure(&mut self) -> std::io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new()
            .build()
            .flags(io_uring::squeue::Flags::IO_DRAIN))
    }

    unsafe fn reap(&mut self, result: norn_uring::CQEResult) -> Self::Completion {
        result
    }
}

impl norn_uring::Singleshot for DrainNop {
    type Output = std::io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

/// An error CQE can still carry `IORING_CQE_F_BUFFER` when the kernel
/// committed the selected buffer before the failure (io-wq issue). Ignoring
/// that flag loses the buffer from the ring permanently.
#[test]
fn failed_ring_receive_does_not_lose_a_committed_buffer() -> Result<(), Box<dyn std::error::Error>>
{
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32)?;
    // While a drain is active the kernel defers every new request behind the
    // driver's outstanding eventfd read. Keep completing that read so the
    // test can make progress.
    let unparker = driver.unparker();
    let stop = Arc::new(AtomicBool::new(false));
    let helper = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let deadline = Instant::now() + Duration::from_secs(20);
            while !stop.load(Ordering::Acquire) {
                if Instant::now() > deadline {
                    eprintln!("failed_ring_receive_does_not_lose_a_committed_buffer timed out");
                    std::process::exit(101);
                }
                unparker.unpark();
                std::thread::sleep(Duration::from_millis(2));
            }
        })
    };
    let mut executor = norn_executor::LocalExecutor::new(driver);
    let result: Result<(), Box<dyn std::error::Error>> = executor.block_on(async {
        let (server, peer) = connected().await?;
        let (server2, mut peer2) = connected().await?;
        let (idle, mut idle_peer) = connected().await?;
        let ring = RecvBufRing::builder(43).buf_cnt(1).buf_len(256).build()?;
        let handle = norn_uring::Handle::current();

        // An in-flight request ahead of a drain barrier keeps the barrier
        // pending; the ring receive queued behind it is issued from io-wq,
        // where the kernel commits the selected buffer immediately.
        let mut blocker = pin!(idle.poll_readiness(libc::POLLIN as u32));
        assert!(futures_util::poll!(&mut blocker).is_pending());
        let mut drain = pin!(handle.submit(DrainNop));
        assert!(futures_util::poll!(&mut drain).is_pending());
        let mut recv = pin!(server.recv_ring(&ring));
        assert!(futures_util::poll!(&mut recv).is_pending());

        idle_peer.write_all(b"x")?;
        drain.await?;
        let _readiness = blocker.await?;
        // Let the io-wq worker select (and commit) the only buffer, find no
        // data, and arm its poll.
        settle();

        // Reset the connection so the waiting receive fails.
        let linger = libc::linger {
            l_onoff: 1,
            l_linger: 0,
        };
        let rc = unsafe {
            libc::setsockopt(
                peer.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_LINGER,
                (&linger as *const libc::linger).cast(),
                std::mem::size_of::<libc::linger>() as libc::socklen_t,
            )
        };
        assert_eq!(rc, 0);
        drop(peer);
        let failed = recv.await;
        assert!(failed.is_err(), "reset receive unexpectedly succeeded");

        // The ring has one buffer and nobody owns it, so this must succeed.
        peer2.write_all(b"hello")?;
        settle();
        let result = server2.recv_ring(&ring).await;
        assert!(
            result.is_ok(),
            "receive after a failed receive returned {:?}; the failed CQE's buffer was lost",
            result.as_ref().err()
        );
        let (buf, _) = result?;
        assert_eq!(&buf[..], b"hello");
        Ok(())
    });
    stop.store(true, Ordering::Release);
    helper.join().expect("unpark helper panicked");
    result
}
