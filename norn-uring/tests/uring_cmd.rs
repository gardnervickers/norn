#![cfg(target_os = "linux")]

use std::io;

use norn_uring::fs::File;
use norn_uring::uring_cmd::{Command16, UringCommand16};
use norn_uring::CQEResult;

mod util;

#[derive(Debug)]
struct TestCommand;

// Safety: this command owns no referenced resources and requests one terminal
// completion. The unassigned command operation exercises the public extension
// path without introducing command-specific resources.
unsafe impl UringCommand16 for TestCommand {
    type Output = io::Result<u32>;

    fn encode(&mut self) -> io::Result<Command16> {
        Ok(Command16::new(u32::MAX, [0; 16]))
    }

    fn complete(self, result: CQEResult) -> Self::Output {
        result.into_result()
    }
}

#[test]
fn public_uring_command_extension_is_submitted() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let file = File::open("/dev/null").await?;

        let result = file.as_uring_fd().submit_command(TestCommand).await;

        match result {
            Ok(value) => assert_eq!(value, 0),
            Err(error) => assert!(error.raw_os_error().is_some()),
        }
        file.close().await?;
        Ok(())
    })
}
