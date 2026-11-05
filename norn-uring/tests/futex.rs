#![cfg(target_os = "linux")]

use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{mpsc, Arc};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use norn_executor::spawn;
use norn_uring::futex::{self, Futex, WaitAnyOutcome, WaitOutcome};
use norn_uring::Request;

mod util;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const DEADLINE: Duration = Duration::from_secs(5);

/// Return whether the running kernel lacks `io_uring` futex opcodes (Linux 6.7+).
async fn futex_unsupported() -> io::Result<bool> {
    match Futex::new(0).wake(1).await {
        Ok(_) => Ok(false),
        Err(err) if err.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipping: io_uring futex opcodes are unsupported");
            Ok(true)
        }
        Err(err) => Err(err),
    }
}

/// Let previously spawned tasks submit their requests.
///
/// A spawned task that is ready runs before this NOP completes, so its SQEs
/// are submitted in the same or an earlier batch and issued in SQ order.
async fn settle() {
    norn_uring::noop().await.unwrap();
    norn_uring::noop().await.unwrap();
}

/// Poll a future exactly once.
async fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    std::future::poll_fn(|cx: &mut Context<'_>| Poll::Ready(Pin::new(&mut *future).poll(cx))).await
}

fn thread_futex_wake(futex: &Futex, count: i32) -> io::Result<i32> {
    // Safety: the futex word is a live, aligned `u32` owned by `futex`.
    let woken = unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex.as_ptr(),
            libc::FUTEX_WAKE | libc::FUTEX_PRIVATE_FLAG,
            count,
        )
    };
    if woken < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(woken as i32)
}

fn thread_futex_wait(futex: &Futex, expected: u32, timeout: Duration) -> io::Result<()> {
    let timeout = libc::timespec {
        tv_sec: timeout.as_secs() as libc::time_t,
        tv_nsec: timeout.subsec_nanos() as libc::c_long,
    };
    // Safety: the futex word is a live, aligned `u32` owned by `futex`, and
    // `timeout` outlives the call.
    let result = unsafe {
        libc::syscall(
            libc::SYS_futex,
            futex.as_ptr(),
            libc::FUTEX_WAIT | libc::FUTEX_PRIVATE_FLAG,
            expected,
            &timeout as *const libc::timespec,
        )
    };
    if result < 0 {
        return Err(io::Error::last_os_error());
    }
    Ok(())
}

#[test]
fn wait_reports_mismatch_when_word_differs() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(1);
        assert_eq!(futex.wait(0).await?, WaitOutcome::Mismatch);
        Ok(())
    })
}

#[test]
fn wait_mismatch_cancels_linked_requests() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(1);
        let (wait, wake) = futex.wait(0).then(futex.wake(1)).await;
        assert_eq!(wait?, WaitOutcome::Mismatch);
        assert_eq!(wake.unwrap_err().raw_os_error(), Some(libc::ECANCELED));
        Ok(())
    })
}

#[test]
fn wake_without_waiters_wakes_nobody() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        assert_eq!(futex.wake(1).await?, 0);
        assert_eq!(futex.wake(u32::MAX).await?, 0);
        Ok(())
    })
}

#[test]
fn ring_wake_releases_ring_wait() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let waiter = spawn(futex.wait(0));
        settle().await;

        futex.store(1, Ordering::Release);
        assert_eq!(futex.wake(u32::MAX).await?, 1);
        assert_eq!(waiter.await??, WaitOutcome::Woken);
        Ok(())
    })
}

#[test]
fn wake_count_limits_released_waiters() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let waiters: Vec<_> = (0..3).map(|_| spawn(futex.wait(0))).collect();
        settle().await;

        assert_eq!(futex.wake(0).await?, 0);
        assert_eq!(futex.wake(2).await?, 2);
        assert_eq!(futex.wake(2).await?, 1);
        for waiter in waiters {
            assert_eq!(waiter.await??, WaitOutcome::Woken);
        }
        Ok(())
    })
}

#[test]
fn wake_bitset_selects_matching_waiters() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let readers = spawn(futex.wait_bitset(0, 0b01));
        settle().await;

        assert_eq!(futex.wake_bitset(u32::MAX, 0b10).await?, 0);
        assert_eq!(futex.wake_bitset(u32::MAX, 0b11).await?, 1);
        assert_eq!(readers.await??, WaitOutcome::Woken);
        Ok(())
    })
}

#[test]
fn zero_bitset_fails_without_reaching_kernel() -> TestResult {
    util::with_test_env(|| async {
        let futex = Futex::new(0);
        let err = futex.wait_bitset(0, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        let err = futex.wake_bitset(1, 0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    })
}

#[test]
fn wait_any_reports_woken_index() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let first = Futex::new(0);
        let second = Futex::new(3);
        let waiter = spawn(futex::wait_any(&[(&first, 0), (&second, 3)]));
        settle().await;

        assert_eq!(second.wake(1).await?, 1);
        assert_eq!(waiter.await??, WaitAnyOutcome::Woken { index: 1 });
        assert_eq!(first.wake(1).await?, 0, "the vectored wait is complete");
        Ok(())
    })
}

#[test]
fn wait_any_reports_mismatch() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let first = Futex::new(0);
        let second = Futex::new(3);
        let outcome = futex::wait_any(&[(&first, 0), (&second, 4)]).await?;
        assert_eq!(outcome, WaitAnyOutcome::Mismatch);
        assert_eq!(first.wake(1).await?, 0, "no waiter remains armed");
        Ok(())
    })
}

#[test]
fn wait_any_rejects_empty_and_oversized_lists() -> TestResult {
    util::with_test_env(|| async {
        let err = futex::wait_any(&[]).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);

        let futexes: Vec<_> = (0..129).map(|_| Futex::new(0)).collect();
        let waits: Vec<_> = futexes.iter().map(|futex| (futex, 0)).collect();
        let err = futex::wait_any(&waits).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    })
}

#[test]
fn linked_timeout_cancels_wait() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let err = futex
            .wait(0)
            .timeout(Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
        assert_eq!(futex.wake(1).await?, 0, "the timed-out waiter is gone");
        Ok(())
    })
}

#[test]
fn dropping_submitted_wait_cancels_it() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let mut wait = Box::pin(futex.wait(0));
        assert!(poll_once(&mut wait).await.is_pending());
        settle().await;
        drop(wait);
        settle().await;

        assert_eq!(futex.wake(1).await?, 0, "the dropped waiter is gone");
        Ok(())
    })
}

#[test]
fn thread_futex_wake_releases_ring_wait() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let remote = futex.clone();
        // The word never changes, so the ring wait can complete only through
        // this thread's private `futex(2)` wake.
        let thread = std::thread::spawn(move || -> io::Result<()> {
            let start = Instant::now();
            while thread_futex_wake(&remote, 1)? == 0 {
                if start.elapsed() > DEADLINE {
                    return Err(io::Error::from(io::ErrorKind::TimedOut));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });

        assert_eq!(futex.wait(0).await?, WaitOutcome::Woken);
        thread.join().expect("waker thread panicked")?;
        Ok(())
    })
}

#[test]
fn ring_wake_releases_thread_futex_wait() -> TestResult {
    util::with_test_env(|| async {
        if futex_unsupported().await? {
            return Ok(());
        }
        let futex = Futex::new(0);
        let remote = futex.clone();
        let thread = std::thread::spawn(move || thread_futex_wait(&remote, 0, DEADLINE));

        let start = Instant::now();
        while futex.wake(1).await? == 0 {
            assert!(start.elapsed() < DEADLINE, "thread never waited");
            norn_uring::noop().await.unwrap();
        }
        thread.join().expect("waiter thread panicked")?;
        Ok(())
    })
}

#[test]
fn driver_shutdown_terminates_pending_waits() {
    let (done_tx, done_rx) = mpsc::channel();
    std::thread::spawn(move || {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 32).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(driver);
        let task_word = Arc::new(AtomicU32::new(0));
        let supported = executor.block_on({
            let probe = Arc::clone(&task_word);
            let task_word = Arc::clone(&task_word);
            async move {
                if futex_unsupported().await.unwrap() {
                    return false;
                }
                // Executor shutdown drops this task, so its wait is canceled
                // through the driver's deferred cancellation path.
                spawn(Futex::from(task_word).wait(0)).detach();

                // The leaked request never cancels itself; only the driver's
                // cancel-all can complete it before the drain barrier.
                let futex = Futex::new(0);
                let mut leaked = Box::pin(futex.wait(0));
                assert!(poll_once(&mut leaked).await.is_pending());
                settle().await;
                std::mem::forget(leaked);
                // `probe`, the caller's word, and the submitted request.
                assert_eq!(
                    Arc::strong_count(&probe),
                    3,
                    "the task wait was not submitted"
                );
                true
            }
        });
        // Driver shutdown completes only after every submitted request has a
        // terminal CQE.
        drop(executor);
        done_tx
            .send((supported, Arc::strong_count(&task_word)))
            .unwrap();
    });

    let (supported, task_word_refs) = done_rx
        .recv_timeout(DEADLINE)
        .expect("driver shutdown did not finish");
    if supported {
        assert_eq!(task_word_refs, 1, "the canceled wait must release its word");
    }
}

#[test]
fn sqpoll_issued_wait_shares_private_futex_key() -> TestResult {
    let mut builder = io_uring::IoUring::builder();
    builder.setup_sqpoll(1000);
    let driver = match norn_uring::Driver::new(builder, 32) {
        Ok(driver) => driver,
        Err(err) if err.raw_os_error() == Some(libc::EPERM) => {
            eprintln!("skipping: SQPOLL is not permitted");
            return Ok(());
        }
        Err(err) => return Err(err.into()),
    };
    let mut executor = norn_executor::LocalExecutor::new(driver);
    executor.block_on(async {
        if futex_unsupported().await? {
            return Ok(());
        }
        // The SQPOLL thread issues the wait; a private wake from an ordinary
        // thread must still find it.
        let futex = Futex::new(0);
        let remote = futex.clone();
        let thread = std::thread::spawn(move || -> io::Result<()> {
            let start = Instant::now();
            while thread_futex_wake(&remote, 1)? == 0 {
                if start.elapsed() > DEADLINE {
                    return Err(io::Error::from(io::ErrorKind::TimedOut));
                }
                std::thread::sleep(Duration::from_millis(1));
            }
            Ok(())
        });

        assert_eq!(futex.wait(0).await?, WaitOutcome::Woken);
        thread.join().expect("waker thread panicked")?;
        Ok(())
    })
}
