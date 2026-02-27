#![cfg(target_os = "linux")]

use std::ops;
use std::path::{Path, PathBuf};
use std::{io, os::raw::c_int};

use futures_core::Future;

pub fn with_test_env<U, F>(f: impl FnOnce() -> F) -> Result<U, Box<dyn std::error::Error>>
where
    F: Future<Output = Result<U, Box<dyn std::error::Error>>>,
{
    let _ = env_logger::builder()
        .filter_level(log::LevelFilter::Trace)
        .is_test(true)
        .try_init();

    let builder = io_uring::IoUring::builder();
    let driver = norn_uring::Driver::new(builder, 32)?;
    let mut ex = norn_executor::LocalExecutor::new(driver);
    ex.block_on((f)())
}

/// [`ThreadNameTestDir`] creates a test directory under /tmp
/// using the current thread name. This is nice for tests
/// because cargo test will name the thread with the name of
/// the test.
///
/// The directory is automatically cleaned up on drop.
#[derive(Debug, Clone)]
pub struct ThreadNameTestDir {
    path: PathBuf,
}

impl ThreadNameTestDir {
    pub fn new() -> Self {
        Self::new0(std::env::temp_dir())
    }

    #[allow(dead_code)]
    pub fn with_parent<P: AsRef<Path>>(parent: P) -> Self {
        let parent = parent.as_ref().to_owned();
        Self::new0(parent)
    }

    fn new0(parent: PathBuf) -> Self {
        let thread = std::thread::current();
        let thread_name = thread.name().expect("no thread name");
        let sanitized = thread_name.replace("::", "-");
        let path = parent.join("thread-name-test-dir").join(sanitized);
        let _ = std::fs::remove_dir_all(&path);
        std::fs::create_dir_all(&path).expect("could not create directory");
        Self { path }
    }
}

impl Default for ThreadNameTestDir {
    fn default() -> Self {
        Self::new()
    }
}

impl ops::Deref for ThreadNameTestDir {
    type Target = std::path::Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}
impl AsRef<std::path::Path> for ThreadNameTestDir {
    fn as_ref(&self) -> &std::path::Path {
        self.path.as_path()
    }
}

impl Drop for ThreadNameTestDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[allow(dead_code)]
pub fn zerocopy_unsupported(err: &io::Error) -> bool {
    const ZC_UNSUPPORTED_ERRNOS: [c_int; 5] = [
        libc::ENOSYS,
        libc::EOPNOTSUPP,
        libc::ENOTSUP,
        libc::EINVAL,
        libc::ENOPROTOOPT,
    ];
    err.kind() == io::ErrorKind::Unsupported
        || ZC_UNSUPPORTED_ERRNOS.contains(&err.raw_os_error().unwrap_or_default())
}
