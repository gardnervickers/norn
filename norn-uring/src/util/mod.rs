use std::io;
use std::time::Duration;

pub(crate) mod notify;

/// Convert `duration` to a kernel timespec, saturating at the longest time the
/// kernel can represent.
///
/// The kernel reads the seconds as an `i64`, so a larger value, such as
/// `Duration::MAX` used as "no deadline", would wrap to a negative timeout
/// that has already expired, or that the kernel rejects with `EINVAL`.
pub(crate) fn timespec(duration: Duration) -> io_uring::types::Timespec {
    io_uring::types::Timespec::new()
        .sec(duration.as_secs().min(i64::MAX as u64))
        .nsec(duration.subsec_nanos())
}

/// A no-op operation, useful for testing.
///
/// This will submit a no-op operation to the kernel,
/// and wait for it to complete. This checks the round-trip
/// functionality of the driver.
///
/// # Errors
///
/// Returns an error if the driver cannot submit the operation or the kernel
/// completes it with an error, for example because the driver has shut down.
///
/// # Panics
///
/// Panics if called outside a running driver context.
pub async fn noop() -> io::Result<()> {
    let handle = crate::Handle::current();

    struct Nop;
    // Safety: a NOP SQE references no external resources.
    unsafe impl crate::operation::Operation for Nop {
        type Completion = crate::operation::CQEResult;

        fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
            Ok(io_uring::opcode::Nop::new().build())
        }

        unsafe fn reap(&mut self, result: crate::operation::CQEResult) -> Self::Completion {
            result
        }
    }

    impl crate::operation::Singleshot for Nop {
        type Output = io::Result<()>;

        fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
            result.result?;
            Ok(())
        }
    }
    handle.submit(Nop).await
}
