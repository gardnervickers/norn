use std::io;
use std::path::{Path, PathBuf};
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

/// Rename a file or directory.
///
/// This is equivalent to `renameat2` with no special flags.
pub async fn rename<P: AsRef<Path>, Q: AsRef<Path>>(from: P, to: Q) -> io::Result<()> {
    let from = from.as_ref().to_owned();
    let to = to.as_ref().to_owned();
    let handle = Handle::current();
    let rename = RenameAt::new(&from, &to, 0)?;
    handle.submit(rename).await?;
    Ok(())
}

/// Create a symbolic link at `linkpath` pointing to `target`.
pub async fn symlink<P: AsRef<Path>, Q: AsRef<Path>>(target: P, linkpath: Q) -> io::Result<()> {
    let target = target.as_ref().to_owned();
    let linkpath = linkpath.as_ref().to_owned();
    let handle = Handle::current();
    let link = SymlinkAt::new(&target, &linkpath)?;
    handle.submit(link).await?;
    Ok(())
}

/// Create a hard link at `newpath` for `oldpath`.
pub async fn hard_link<P: AsRef<Path>, Q: AsRef<Path>>(oldpath: P, newpath: Q) -> io::Result<()> {
    let oldpath = oldpath.as_ref().to_owned();
    let newpath = newpath.as_ref().to_owned();
    let handle = Handle::current();
    let link = LinkAt::new(&oldpath, &newpath, 0)?;
    handle.submit(link).await?;
    Ok(())
}

/// Read metadata using `statx(2)`.
///
/// `flags` and `mask` map directly to the `statx(2)` system call arguments.
pub async fn statx<P: AsRef<Path>>(path: P, flags: i32, mask: u32) -> io::Result<libc::statx> {
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let statx = Statx::new(&path, flags, mask)?;
    handle.submit(statx).await
}

/// Read file metadata with `STATX_BASIC_STATS`.
pub async fn metadata<P: AsRef<Path>>(path: P) -> io::Result<libc::statx> {
    statx(path, libc::AT_STATX_SYNC_AS_STAT, libc::STATX_BASIC_STATS).await
}

/// Read a symbolic link target.
///
/// Note: `io_uring` does not currently expose a `readlinkat` opcode in this version of the
/// dependency, so this uses `std::fs::read_link` directly.
pub async fn read_link<P: AsRef<Path>>(path: P) -> io::Result<PathBuf> {
    std::fs::read_link(path)
}

/// Read an extended attribute from a path into the provided buffer.
///
/// On success, returns the number of bytes written into `buf`.
pub async fn get_xattr<P, N, B>(path: P, name: N, buf: B) -> (io::Result<usize>, B)
where
    P: AsRef<Path>,
    N: AsRef<[u8]>,
    B: crate::buf::StableBufMut + 'static,
{
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let op = match PathGetXattr::new(&path, name.as_ref(), buf) {
        Ok(op) => op,
        Err((err, buf)) => return (Err(err), buf),
    };
    handle.submit(op).await
}

/// Set an extended attribute on a path from the provided buffer.
pub async fn set_xattr<P, N, B>(path: P, name: N, value: B, flags: i32) -> (io::Result<()>, B)
where
    P: AsRef<Path>,
    N: AsRef<[u8]>,
    B: crate::buf::StableBuf + 'static,
{
    let path = path.as_ref().to_owned();
    let handle = Handle::current();
    let op = match PathSetXattr::new(&path, name.as_ref(), value, flags) {
        Ok(op) => op,
        Err((err, value)) => return (Err(err), value),
    };
    handle.submit(op).await
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

struct RenameAt {
    oldpath: std::ffi::CString,
    newpath: std::ffi::CString,
    flags: u32,
}

impl RenameAt {
    fn new(oldpath: &Path, newpath: &Path, flags: u32) -> io::Result<Self> {
        Ok(Self {
            oldpath: path_to_cstring(oldpath)?,
            newpath: path_to_cstring(newpath)?,
            flags,
        })
    }
}

impl Operation for RenameAt {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        opcode::RenameAt::new(
            types::Fd(libc::AT_FDCWD),
            this.oldpath.as_ptr(),
            types::Fd(libc::AT_FDCWD),
            this.newpath.as_ptr(),
        )
        .flags(this.flags)
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for RenameAt {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}

struct SymlinkAt {
    target: std::ffi::CString,
    linkpath: std::ffi::CString,
}

impl SymlinkAt {
    fn new(target: &Path, linkpath: &Path) -> io::Result<Self> {
        Ok(Self {
            target: path_to_cstring(target)?,
            linkpath: path_to_cstring(linkpath)?,
        })
    }
}

impl Operation for SymlinkAt {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        opcode::SymlinkAt::new(
            types::Fd(libc::AT_FDCWD),
            this.target.as_ptr(),
            this.linkpath.as_ptr(),
        )
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for SymlinkAt {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}

struct LinkAt {
    oldpath: std::ffi::CString,
    newpath: std::ffi::CString,
    flags: i32,
}

impl LinkAt {
    fn new(oldpath: &Path, newpath: &Path, flags: i32) -> io::Result<Self> {
        Ok(Self {
            oldpath: path_to_cstring(oldpath)?,
            newpath: path_to_cstring(newpath)?,
            flags,
        })
    }
}

impl Operation for LinkAt {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        opcode::LinkAt::new(
            types::Fd(libc::AT_FDCWD),
            this.oldpath.as_ptr(),
            types::Fd(libc::AT_FDCWD),
            this.newpath.as_ptr(),
        )
        .flags(this.flags)
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for LinkAt {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result.map(drop)
    }
}

struct Statx {
    path: std::ffi::CString,
    flags: i32,
    mask: u32,
    statx: std::mem::MaybeUninit<libc::statx>,
}

impl Statx {
    fn new(path: &Path, flags: i32, mask: u32) -> io::Result<Self> {
        Ok(Self {
            path: path_to_cstring(path)?,
            flags,
            mask,
            statx: std::mem::MaybeUninit::zeroed(),
        })
    }
}

impl Operation for Statx {
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let this = self.get_mut();
        opcode::Statx::new(
            types::Fd(libc::AT_FDCWD),
            this.path.as_ptr(),
            this.statx.as_mut_ptr().cast(),
        )
        .flags(this.flags)
        .mask(this.mask)
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for Statx {
    type Output = io::Result<libc::statx>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result?;
        // Safety: the kernel initialized the statx struct on successful completion.
        Ok(unsafe { self.statx.assume_init() })
    }
}

struct PathGetXattr<B> {
    path: std::ffi::CString,
    name: std::ffi::CString,
    buf: B,
    len: u32,
}

impl<B> PathGetXattr<B>
where
    B: crate::buf::StableBufMut,
{
    fn new(path: &Path, name: &[u8], buf: B) -> Result<Self, (io::Error, B)> {
        let path = match path_to_cstring(path) {
            Ok(path) => path,
            Err(err) => return Err((err, buf)),
        };
        let name = match bytes_to_cstring(name, "xattr name") {
            Ok(name) => name,
            Err(err) => return Err((err, buf)),
        };
        let len = match usize_to_u32(buf.bytes_remaining(), "xattr buffer length") {
            Ok(len) => len,
            Err(err) => return Err((err, buf)),
        };
        Ok(Self {
            path,
            name,
            buf,
            len,
        })
    }
}

impl<B> Operation for PathGetXattr<B>
where
    B: crate::buf::StableBufMut,
{
    fn configure(mut self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let value = self.buf.stable_ptr_mut() as *mut libc::c_void;
        opcode::GetXattr::new(self.name.as_ptr(), value, self.path.as_ptr(), self.len).build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl<B> Singleshot for PathGetXattr<B>
where
    B: crate::buf::StableBufMut,
{
    type Output = (io::Result<usize>, B);

    fn complete(mut self, result: crate::operation::CQEResult) -> Self::Output {
        match result.result {
            Ok(n) => {
                let n = n as usize;
                unsafe { self.buf.set_init(n) };
                (Ok(n), self.buf)
            }
            Err(err) => (Err(err), self.buf),
        }
    }
}

struct PathSetXattr<B> {
    path: std::ffi::CString,
    name: std::ffi::CString,
    value: B,
    flags: i32,
    len: u32,
}

impl<B> PathSetXattr<B>
where
    B: crate::buf::StableBuf,
{
    fn new(path: &Path, name: &[u8], value: B, flags: i32) -> Result<Self, (io::Error, B)> {
        let path = match path_to_cstring(path) {
            Ok(path) => path,
            Err(err) => return Err((err, value)),
        };
        let name = match bytes_to_cstring(name, "xattr name") {
            Ok(name) => name,
            Err(err) => return Err((err, value)),
        };
        let len = match usize_to_u32(value.bytes_init(), "xattr value length") {
            Ok(len) => len,
            Err(err) => return Err((err, value)),
        };
        Ok(Self {
            path,
            name,
            value,
            flags,
            len,
        })
    }
}

impl<B> Operation for PathSetXattr<B>
where
    B: crate::buf::StableBuf,
{
    fn configure(self: Pin<&mut Self>) -> io_uring::squeue::Entry {
        let value = self.value.stable_ptr() as *const libc::c_void;
        opcode::SetXattr::new(self.name.as_ptr(), value, self.path.as_ptr(), self.len)
            .flags(self.flags)
            .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl<B> Singleshot for PathSetXattr<B>
where
    B: crate::buf::StableBuf,
{
    type Output = (io::Result<()>, B);

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        (result.result.map(|_| ()), self.value)
    }
}

fn path_to_cstring(path: &Path) -> io::Result<std::ffi::CString> {
    let path = path
        .to_str()
        .ok_or_else(|| io::Error::from_raw_os_error(libc::EINVAL))?;
    Ok(std::ffi::CString::new(path)?)
}

fn bytes_to_cstring(bytes: &[u8], what: &'static str) -> io::Result<std::ffi::CString> {
    std::ffi::CString::new(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} contains an interior NUL byte"),
        )
    })
}

fn usize_to_u32(len: usize, what: &'static str) -> io::Result<u32> {
    u32::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{what} exceeds u32::MAX"),
        )
    })
}
