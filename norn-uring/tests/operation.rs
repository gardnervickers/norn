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
