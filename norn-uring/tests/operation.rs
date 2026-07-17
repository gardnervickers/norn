#![cfg(target_os = "linux")]

use std::io;

use norn_uring::{CQEResult, Handle, Operation, Singleshot};

mod util;

#[derive(Debug)]
struct PublicNop;

// Safety: NOP has no referenced resources and produces exactly one terminal
// completion. `cleanup` consumes every unobserved completion result.
unsafe impl Operation for PublicNop {
    fn configure(&mut self) -> io_uring::squeue::Entry {
        io_uring::opcode::Nop::new().build()
    }

    fn cleanup(&mut self, result: CQEResult) {
        let _ = result.into_parts();
    }
}

impl Singleshot for PublicNop {
    type Output = io::Result<u32>;

    fn complete(self, result: CQEResult) -> Self::Output {
        assert_eq!(result.flags(), 0);
        assert!(!result.more());
        assert!(!result.is_notification());
        result.into_result()
    }
}

#[test]
fn public_operation_extension_point_is_implementable() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        assert_eq!(Handle::current().submit(PublicNop).await?, 0);
        Ok(())
    })
}
