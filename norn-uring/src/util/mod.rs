use std::io;
use std::pin::Pin;

pub(crate) mod notify;

/// A no-op operation, useful for testing.
///
/// This will submit a no-op operation to the kernel,
/// and wait for it to complete. This checks the round-trip
/// functionality of the driver.
///
/// ### Panics
/// Panics if called outside a running driver context, or if submission fails.
pub async fn noop() {
    let handle = crate::Handle::current();

    struct Nop;
    impl crate::operation::Operation for Nop {
        fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
            io_uring::opcode::Nop::new().build()
        }

        fn cleanup(&mut self, _: crate::operation::CQEResult) {}
    }

    impl crate::operation::Singleshot for Nop {
        type Output = io::Result<()>;

        fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
            result.result?;
            Ok(())
        }
    }
    handle.submit(Nop).await.unwrap();
}
