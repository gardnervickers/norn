#![cfg(target_os = "linux")]

//! Durations whose seconds exceed `i64::MAX`, such as `Duration::MAX` used as
//! "no deadline", must not become negative kernel timespecs.

use std::io;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use norn_executor::park::{Park, ParkMode, Unpark};
use norn_uring::{CQEResult, Operation, Request, Singleshot};

struct Nop;

// Safety: a NOP references no memory and posts one terminal CQE.
unsafe impl Operation for Nop {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new().build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Nop {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion.into_result()
    }
}

/// A timed park with `Duration::MAX` waits for a wake. A wrapped timeout has
/// already expired, so a park layer that uses it as "no deadline" spins.
#[test]
fn huge_timed_park_waits_for_a_wake() {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let unparker = driver.unparker();
        let wake = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            unparker.unpark();
        });
        let start = Instant::now();
        driver.park(ParkMode::Timeout(Duration::MAX)).unwrap();
        let waited = start.elapsed();
        wake.join().unwrap();
        tx.send(waited).unwrap();
    });
    let waited = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("the park did not return after its wake");
    assert!(
        waited >= Duration::from_millis(50),
        "a Timeout(Duration::MAX) park returned after {waited:?} instead of waiting"
    );
}

/// `request.timeout(Duration::MAX)` must not fail the request. A wrapped
/// timespec makes the kernel reject the link timeout and cancel the chain.
#[test]
fn huge_link_timeout_does_not_fail_the_request() {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let mut ex = norn_executor::LocalExecutor::new(driver);
    let result = ex.block_on(async {
        norn_uring::Handle::current()
            .submit(Nop)
            .timeout(Duration::MAX)
            .await
    });
    assert_eq!(
        result.unwrap(),
        0,
        "a NOP with an unbounded deadline must succeed"
    );
}

/// A standalone `Handle::timeout(Duration::MAX)` stays pending; with a wrapped
/// timespec it fails with `EINVAL` at once. Resetting one to `Duration::MAX`
/// must work the same way.
#[test]
fn huge_standalone_timeout_and_reset_stay_pending() {
    let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let mut ex = norn_executor::LocalExecutor::new(driver);
    let outcome = ex.block_on(async {
        let handle = norn_uring::Handle::current();
        let huge = handle.timeout(Duration::MAX);
        let reset = handle.timeout(Duration::from_secs(1));
        let control = reset.control();
        let mut reset = Box::pin(reset);
        // Submit the second timeout, then push its deadline out to the maximum.
        assert!(futures_util::poll!(reset.as_mut()).is_pending());
        assert!(control.reset(Duration::MAX).await.unwrap());
        let short = handle.timeout(Duration::from_millis(50));
        let either = futures_util::future::select(Box::pin(huge), reset);
        match futures_util::future::select(either, Box::pin(short)).await {
            futures_util::future::Either::Left((done, _)) => {
                format!("a huge timeout finished first: {done:?}")
            }
            futures_util::future::Either::Right(_) => "pending".to_string(),
        }
    });
    assert_eq!(outcome, "pending");
}
