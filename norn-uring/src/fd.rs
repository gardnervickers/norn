//! # File Descriptors
//!
//! We need a way to make sure that a file descriptor does not get
//! closed while we are using it. This can be when the app has a
//! reference to the file descriptor, but it can also be when
//! the kernel is using the file descriptor.
//!
//! Essentially we need a reference counted file descriptor.
//!
//! Additionally, io-uring supports two types of file descriptors,
//! regular file descriptors and fixed file descriptors.
use std::cell::Cell;
use std::io;
use std::os::fd::{AsRawFd, RawFd};
use std::rc::Rc;

use io_uring::{opcode, types};
use log::warn;

use crate::operation::{Operation, Singleshot};
use crate::util::notify::Notify;
use crate::Handle;

/// [`NornFd`] is a reference counted file descriptor.
#[derive(Clone, Debug)]
pub(crate) struct NornFd {
    inner: Rc<Inner>,
}

#[derive(Debug)]
struct Inner {
    kind: FdKind,
    handle: Option<Handle>,
    notify: Notify,
    closed: Cell<bool>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FdKind {
    Fd(types::Fd),
    #[allow(dead_code)]
    Fixed(types::Fixed),
}

impl NornFd {
    /// Create a new [`NornFd`] from a regular file descriptor.
    pub(crate) fn from_fd(fd: RawFd) -> Self {
        let raw = fd.as_raw_fd();
        Self::new(FdKind::Fd(types::Fd(raw)))
    }

    /// Create a new [`NornFd`] from a fixed file descriptor.
    #[allow(dead_code)]
    pub(crate) fn from_fixed(fixed: types::Fixed) -> Self {
        Self::new(FdKind::Fixed(fixed))
    }

    fn new(kind: FdKind) -> Self {
        let handle = Handle::try_current();
        let inner = Inner {
            kind,
            handle,
            notify: Notify::default(),
            closed: Cell::new(false),
        };
        let inner = Rc::new(inner);
        Self { inner }
    }

    pub(crate) fn kind(&self) -> &'_ FdKind {
        &self.inner.kind
    }

    pub(crate) async fn close(&self) -> io::Result<()> {
        loop {
            if self.inner.closed.get() {
                return Ok(());
            }
            if Rc::strong_count(&self.inner) == 1 {
                if let Some(handle) = &self.inner.handle {
                    if let Err(_err) = handle
                        .submit(CloseFd {
                            fd: self.inner.kind,
                        })
                        .await
                    {
                        // Fall back to best-effort direct close if the reactor is shutting down.
                        self.inner.close_direct()?;
                        self.inner.closed.set(true);
                        return Ok(());
                    }
                } else {
                    self.inner.close_direct()?;
                }
                self.inner.closed.set(true);
            } else {
                self.inner.notify.wait().await;
            }
        }
    }
}

impl Drop for NornFd {
    fn drop(&mut self) {
        self.inner.notify.notify(usize::MAX);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !self.closed.get() {
            // Best-effort close on drop. Errors are logged because drop cannot report them.
            let reactor_res = self
                .handle
                .as_ref()
                .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "no driver handle"))
                .and_then(|handle| handle.close_fd(&self.kind));
            if let Err(err) = reactor_res {
                if let Err(direct_err) = self.close_direct() {
                    warn!(target: "norn_uring::fd", "close_fd.failed: {}; direct_close.failed: {}", err, direct_err);
                }
            }
        }
    }
}

impl Inner {
    fn close_direct(&self) -> io::Result<()> {
        match self.kind {
            FdKind::Fd(fd) => {
                let res = unsafe { libc::close(fd.0) };
                if res == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            }
            FdKind::Fixed(_) => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "cannot directly close fixed descriptor",
            )),
        }
    }
}

struct CloseFd {
    fd: FdKind,
}

impl Operation for CloseFd {
    fn configure(self: std::pin::Pin<&mut Self>) -> io_uring::squeue::Entry {
        match self.fd {
            FdKind::Fd(fd) => opcode::Close::new(types::Fd(fd.0)),
            FdKind::Fixed(fd) => opcode::Close::new(types::Fixed(fd.0)),
        }
        .build()
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for CloseFd {
    type Output = io::Result<()>;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        result.result?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drop_fd_without_runtime_context_closes_descriptor() {
        let mut fds = [0; 2];
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);

        let read_end = fds[0];
        let write_end = fds[1];
        let fd = NornFd::from_fd(read_end);

        drop(fd);

        // Pipe read-end should be closed by NornFd drop fallback.
        let check = unsafe { libc::fcntl(read_end, libc::F_GETFD) };
        assert_eq!(check, -1);
        assert_eq!(io::Error::last_os_error().raw_os_error(), Some(libc::EBADF));

        unsafe {
            libc::close(write_end);
        }
    }
}
