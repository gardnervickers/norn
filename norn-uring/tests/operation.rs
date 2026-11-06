#![cfg(target_os = "linux")]

use std::io;

use norn_uring::{CQEResult, Handle, Operation, Singleshot};

mod util;

#[derive(Debug)]
struct PublicNop;

// Safety: NOP references no resources and produces one terminal CQE. Returning
// `CQEResult` from `reap` preserves the result and flags without claiming
// ownership.
unsafe impl Operation for PublicNop {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new().build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for PublicNop {
    type Output = io::Result<u32>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        assert_eq!(completion.flags(), 0);
        assert!(!completion.more());
        assert!(!completion.is_notification());
        completion.into_result()
    }
}

#[test]
fn public_operation_extension_point_is_implementable() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        assert_eq!(Handle::current().submit(PublicNop).await?, 0);
        Ok(())
    })
}

/// A one-CQE operation whose entry carries extra SQE flags.
#[derive(Debug)]
struct Flagged {
    build: fn() -> io_uring::squeue::Entry,
    flags: io_uring::squeue::Flags,
}

/// Closing an invalid descriptor fails in the kernel.
fn failing_close() -> io_uring::squeue::Entry {
    io_uring::opcode::Close::new(io_uring::types::Fd(-1)).build()
}

fn nop() -> io_uring::squeue::Entry {
    io_uring::opcode::Nop::new().build()
}

// Safety: NOP and closing an invalid descriptor reference no resources and
// produce one terminal CQE.
unsafe impl Operation for Flagged {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok((self.build)().flags(self.flags))
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for Flagged {
    type Output = (bool, io::Result<u32>);

    fn complete(self, completion: Self::Completion) -> Self::Output {
        (completion.is_synthetic(), completion.into_result())
    }
}

fn assert_rejected_configuration((synthetic, result): (bool, io::Result<u32>)) {
    assert!(synthetic, "a reserved flag must fail before submission");
    assert_eq!(
        result.expect_err("a reserved flag must fail").kind(),
        io::ErrorKind::InvalidInput
    );
}

#[test]
fn operation_links_cannot_capture_the_next_unrelated_operation(
) -> Result<(), Box<dyn std::error::Error>> {
    use io_uring::squeue::Flags;

    for link in [Flags::IO_LINK, Flags::IO_HARDLINK] {
        util::with_test_env(|| async move {
            let handle = Handle::current();
            let mut linked = Box::pin(handle.submit(Flagged {
                build: failing_close,
                flags: link,
            }));
            let mut unrelated = Box::pin(handle.submit(PublicNop));
            // Queue both entries back to back, as two tasks polled in one
            // executor pass would.
            let waker = futures_test::task::noop_waker();
            let mut cx = std::task::Context::from_waker(&waker);
            let first = std::future::Future::poll(linked.as_mut(), &mut cx);
            let _ = std::future::Future::poll(unrelated.as_mut(), &mut cx);

            let linked = match first {
                std::task::Poll::Ready(output) => output,
                std::task::Poll::Pending => linked.await,
            };
            assert_eq!(
                unrelated.await.map_err(|err| err.raw_os_error()),
                Ok(0),
                "another operation's link must not cancel this one"
            );
            assert_rejected_configuration(linked);
            Ok(())
        })?;
    }
    Ok(())
}

#[test]
fn skipped_success_completions_are_rejected_and_shutdown_completes() {
    let (done, finished) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let output = ex.block_on(async {
            Handle::current()
                .submit(Flagged {
                    build: nop,
                    flags: io_uring::squeue::Flags::SKIP_SUCCESS,
                })
                .await
        });
        // A skipped success CQE would leave the operation in flight forever,
        // so shutdown, which waits for every terminal CQE, would never finish.
        drop(ex);
        done.send(output).unwrap();
    });

    let output = finished
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("operation or driver shutdown did not finish");
    assert_rejected_configuration(output);
}

/// `IO_DRAIN` acts on the shared ring: every SQE submitted after it, including
/// the driver's own cancellations and eventfd read, waits until every earlier
/// SQE has completed. One drain entry behind an idle read would stall the whole
/// runtime, so the runtime rejects it like the other ring-wide flags.
#[test]
fn io_drain_is_rejected_before_submission() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let drained = Handle::current()
            .submit(Flagged {
                build: nop,
                flags: io_uring::squeue::Flags::IO_DRAIN,
            })
            .await;
        assert_rejected_configuration(drained);
        // The ring is unaffected.
        assert_eq!(Handle::current().submit(PublicNop).await?, 0);
        Ok(())
    })
}
