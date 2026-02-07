use std::io;
use std::path::Path;
use std::pin::Pin;

use io_uring::{opcode, types};

use crate::operation::{Operation, Singleshot};
use crate::Handle;

/// Remove a file from the filesystem.
///
/// This is equivalent to unlinkat.
pub async fn remove_file<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let remove = UnlinkAt::remove_file(&path)?;
    handle.submit(remove).await?;
    Ok(())
}

/// Create a directory at the specified path.
pub async fn create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let create = MkDirAt::new(&path, 0o777)?;
    handle.submit(create).await?;
    Ok(())
}

/// Remove a directory from the filesystem.
pub async fn remove_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let remove = UnlinkAt::remove_dir(&path)?;
    handle.submit(remove).await?;
    Ok(())
}

struct UnlinkAt {
    path: std::ffi::CString,
    flags: i32,
}

impl UnlinkAt {
    fn remove_file(path: &Path) -> io::Result<Self> {
        Self::new(path, 0)
    }

    fn remove_dir(path: &Path) -> io::Result<Self> {
        Self::new(path, libc::AT_REMOVEDIR)
    }

    fn new(path: &Path, flags: i32) -> io::Result<Self> {
        let path = path
            .to_str()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
        let path = std::ffi::CString::new(path)?;
        Ok(Self { path, flags })
    }
}

impl Operation for UnlinkAt {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        let ptr = this.path.as_ptr();
        opcode::UnlinkAt::new(types::Fd(libc::AT_FDCWD), ptr)
            .flags(this.flags)
            .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for UnlinkAt {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}

struct MkDirAt {
    path: std::ffi::CString,
    mode: u32,
}

impl MkDirAt {
    fn new(path: &Path, mode: u32) -> io::Result<Self> {
        let path = path
            .to_str()
            .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
        let path = std::ffi::CString::new(path)?;
        Ok(Self { path, mode })
    }
}

impl Operation for MkDirAt {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        let ptr = this.path.as_ptr();
        opcode::MkDirAt::new(types::Fd(libc::AT_FDCWD), ptr)
            .mode(this.mode)
            .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for MkDirAt {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}
