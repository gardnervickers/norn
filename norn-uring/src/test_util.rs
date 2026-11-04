use std::ffi::OsStr;
use std::os::fd::RawFd;
use std::process::Command;
use std::time::{Duration, Instant};

const ISOLATED_TEST_ENV: &str = "NORN_URING_ISOLATED_TEST";

pub(crate) fn run_isolated(test_name: &str, test: impl FnOnce()) {
    match std::env::var_os(ISOLATED_TEST_ENV) {
        Some(active_test) => {
            assert_eq!(
                active_test,
                OsStr::new(test_name),
                "isolated test process ran an unexpected test"
            );
            test();
        }
        None => {
            let status = Command::new(std::env::current_exe().unwrap())
                .arg(test_name)
                .arg("--exact")
                .arg("--test-threads=1")
                .env(ISOLATED_TEST_ENV, test_name)
                .status()
                .expect("failed to spawn isolated regression test");
            assert!(status.success(), "isolated regression test failed");
        }
    }
}

/// Asserts that every read end of the pipe whose write end is `write_end` is
/// closed.
///
/// Unlike probing the closed descriptor number itself, this cannot be fooled
/// by another test thread reusing that number, because the pipe is private to
/// the caller. A concurrent `fork` in another test can briefly hold an inherited
/// copy of the read end until its `exec`, so closure is awaited for a bounded
/// time rather than sampled once.
pub(crate) fn assert_pipe_reader_closed(write_end: RawFd) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let mut pollfd = libc::pollfd {
            fd: write_end,
            events: libc::POLLOUT,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut pollfd, 1, 0) }, 1);
        if pollfd.revents & libc::POLLERR != 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "pipe reader was not closed within 5s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}
