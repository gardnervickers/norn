use std::ffi::OsStr;
use std::process::Command;

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
