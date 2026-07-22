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

mod mode;

pub(crate) use mode::{BundledPermit, Direction, ModeConflict, OrdinaryPermit};

use io_uring::{opcode, types};
use log::warn;

use crate::driver::CloseFdError;
use crate::operation::{Operation, Singleshot};
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
    closed: Cell<bool>,
    modes: Cell<mode::DirectionModes>,
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
            closed: Cell::new(false),
            modes: Cell::new(mode::DirectionModes::default()),
        };
        let inner = Rc::new(inner);
        Self { inner }
    }

    pub(crate) fn kind(&self) -> &'_ FdKind {
        &self.inner.kind
    }

    pub(crate) fn acquire_ordinary(
        &self,
        direction: Direction,
    ) -> Result<OrdinaryPermit, ModeConflict> {
        OrdinaryPermit::acquire(&self.inner, direction)
    }

    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "consumed by the next stacked bundle API PR")
    )]
    pub(crate) fn acquire_bundled(
        &self,
        direction: Direction,
    ) -> Result<BundledPermit, ModeConflict> {
        BundledPermit::acquire(&self.inner, direction)
    }

    pub(crate) async fn close(&self) -> io::Result<()> {
        if self.inner.closed.get() {
            return Ok(());
        }
        if Rc::strong_count(&self.inner) != 1 {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "explicit close requires sole descriptor ownership",
            ));
        }
        if let Some(handle) = &self.inner.handle {
            let result = handle
                .submit(CloseFd {
                    fd: self.inner.kind,
                })
                .await;
            self.inner.finish_tracked_close(result)
        } else {
            self.inner.close_direct_and_invalidate()
        }
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        if !self.closed.get() {
            // Best-effort close on drop. Errors are logged because drop cannot report them.
            if let Some(handle) = &self.handle {
                self.finish_drop_close(handle.close_fd(&self.kind));
            } else if let Err(err) = self.close_direct_and_invalidate() {
                warn!(target: "norn_uring::fd", "direct_close.failed: {err}");
            }
        }
    }
}

enum CloseResult {
    Closed,
    NeverSubmitted(io::Error),
    KernelError(io::Error),
}

impl Inner {
    fn finish_tracked_close(&self, result: CloseResult) -> io::Result<()> {
        match result {
            CloseResult::Closed => {
                self.closed.set(true);
                Ok(())
            }
            CloseResult::NeverSubmitted(_submit_err) => self.close_direct_and_invalidate(),
            CloseResult::KernelError(err) => {
                // Linux invalidates the descriptor when processing Close even if it reports a
                // later error. Retrying by integer fd could close an unrelated reused fd.
                self.closed.set(true);
                Err(err)
            }
        }
    }

    fn finish_drop_close(&self, result: Result<(), CloseFdError>) {
        match result {
            Ok(()) => {}
            Err(CloseFdError::NeverQueued(err)) => {
                if let Err(direct_err) = self.close_direct_and_invalidate() {
                    warn!(target: "norn_uring::fd", "close_fd.failed: {err}; direct_close.failed: {direct_err}");
                }
            }
            Err(CloseFdError::Queued(err)) => {
                // The SQE remains owned by the ring and may still be submitted. Direct close is
                // unsafe because the descriptor number can be reused before that happens.
                warn!(target: "norn_uring::fd", "close_fd.failed: {err}; direct_close.skipped: close SQE was already queued");
            }
        }
    }

    fn close_direct_and_invalidate(&self) -> io::Result<()> {
        let result = self.close_direct();
        if matches!(self.kind, FdKind::Fd(_)) {
            // On Linux a close error does not preserve ownership of the descriptor number.
            self.closed.set(true);
        }
        result
    }

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

// Safety: the SQE contains only the copied descriptor value; completion owns
// no borrowed memory, and cleanup closes a successfully returned descriptor.
unsafe impl Operation for CloseFd {
    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(match self.fd {
            FdKind::Fd(fd) => opcode::Close::new(types::Fd(fd.0)),
            FdKind::Fixed(fd) => opcode::Close::new(types::Fixed(fd.0)),
        }
        .build())
    }

    fn cleanup(&mut self, _: crate::operation::CQEResult) {}
}

impl Singleshot for CloseFd {
    type Output = CloseResult;

    fn complete(self, result: crate::operation::CQEResult) -> Self::Output {
        let synthetic = result.is_synthetic();
        match result.into_result() {
            Ok(_) => CloseResult::Closed,
            Err(err) if synthetic => CloseResult::NeverSubmitted(err),
            Err(err) => CloseResult::KernelError(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pipe() -> [RawFd; 2] {
        let mut fds = [0; 2];
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        fds
    }

    fn assert_open(fd: RawFd) {
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    }

    fn assert_pipe_reader_closed(write_end: RawFd) {
        let mut pollfd = libc::pollfd {
            fd: write_end,
            events: libc::POLLOUT,
            revents: 0,
        };
        assert_eq!(unsafe { libc::poll(&mut pollfd, 1, 0) }, 1);
        assert_ne!(pollfd.revents & libc::POLLERR, 0);
    }

    #[test]
    fn drop_fd_without_runtime_context_closes_descriptor() {
        let fds = pipe();
        let read_end = fds[0];
        let write_end = fds[1];
        let fd = NornFd::from_fd(read_end);

        drop(fd);

        // Pipe read-end should be closed by NornFd drop fallback.
        assert_pipe_reader_closed(write_end);

        unsafe {
            libc::close(write_end);
        }
    }

    #[test]
    fn close_completion_distinguishes_submission_from_kernel_errors() {
        let synthetic = CloseFd {
            fd: FdKind::Fd(types::Fd(-1)),
        }
        .complete(crate::operation::CQEResult::synthetic(Err(
            io::Error::from_raw_os_error(libc::EIO),
        )));
        assert!(matches!(
            synthetic,
            CloseResult::NeverSubmitted(err) if err.raw_os_error() == Some(libc::EIO)
        ));

        let kernel = CloseFd {
            fd: FdKind::Fd(types::Fd(-1)),
        }
        .complete(crate::operation::CQEResult::new(
            Err(io::Error::from_raw_os_error(libc::EIO)),
            0,
        ));
        assert!(matches!(
            kernel,
            CloseResult::KernelError(err) if err.raw_os_error() == Some(libc::EIO)
        ));
    }

    #[test]
    fn never_submitted_close_uses_direct_fallback() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        fd.inner
            .finish_tracked_close(CloseResult::NeverSubmitted(io::Error::from_raw_os_error(
                libc::EIO,
            )))
            .unwrap();

        assert_pipe_reader_closed(write_end);
        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn terminal_close_error_does_not_close_reused_descriptor() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        // Model the kernel invalidating the original descriptor before returning a late error,
        // then another thread reusing the same integer descriptor.
        assert_eq!(unsafe { libc::close(read_end) }, 0);
        assert_eq!(unsafe { libc::dup2(write_end, read_end) }, read_end);

        let err = fd
            .inner
            .finish_tracked_close(CloseResult::KernelError(io::Error::from_raw_os_error(
                libc::EIO,
            )))
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EIO));
        assert_open(read_end);

        drop(fd);
        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[test]
    fn queued_drop_close_failure_skips_direct_fallback() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        fd.inner
            .finish_drop_close(Err(CloseFdError::Queued(io::Error::from_raw_os_error(
                libc::EIO,
            ))));
        assert_open(read_end);

        // The test owns the descriptor because there is no real queued SQE to close it.
        fd.inner.closed.set(true);
        drop(fd);
        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[test]
    fn never_queued_drop_close_failure_uses_direct_fallback() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        fd.inner.finish_drop_close(Err(CloseFdError::NeverQueued(
            io::Error::from_raw_os_error(libc::EIO),
        )));
        assert_pipe_reader_closed(write_end);

        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn ordinary_and_bundled_modes_are_exclusive_per_direction() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        let read = fd.acquire_ordinary(Direction::Read).unwrap();
        assert_eq!(
            fd.acquire_bundled(Direction::Read).unwrap_err().direction(),
            Direction::Read
        );
        drop(read);

        let bundled = fd.acquire_bundled(Direction::Read).unwrap();
        assert_eq!(
            fd.acquire_ordinary(Direction::Read)
                .unwrap_err()
                .direction(),
            Direction::Read
        );
        drop(bundled);

        assert!(fd.acquire_ordinary(Direction::Read).is_ok());
        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn read_and_write_modes_are_independent() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        let _read = fd.acquire_bundled(Direction::Read).unwrap();
        let _write = fd.acquire_ordinary(Direction::Write).unwrap();

        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn acquiring_both_directions_is_atomic() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        let write = fd.acquire_bundled(Direction::Write).unwrap();
        assert_eq!(
            fd.acquire_ordinary(Direction::Both)
                .unwrap_err()
                .direction(),
            Direction::Write
        );
        // The failed two-direction acquisition must not retain the read side.
        let read = fd.acquire_bundled(Direction::Read).unwrap();
        drop(read);
        drop(write);

        let write = fd.acquire_ordinary(Direction::Write).unwrap();
        assert_eq!(
            fd.acquire_bundled(Direction::Both).unwrap_err().direction(),
            Direction::Write
        );
        // The failed two-direction acquisition must not mark the read side bundled.
        let read = fd.acquire_ordinary(Direction::Read).unwrap();
        drop(read);
        drop(write);

        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn bundled_mode_waits_for_every_ordinary_permit() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        let first = fd.acquire_ordinary(Direction::Read).unwrap();
        let second = fd.acquire_ordinary(Direction::Read).unwrap();
        assert!(fd.acquire_bundled(Direction::Read).is_err());

        drop(first);
        assert!(fd.acquire_bundled(Direction::Read).is_err());

        drop(second);
        assert!(fd.acquire_bundled(Direction::Read).is_ok());

        drop(fd);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn permits_release_on_their_last_owner() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        let mut ordinary = fd.acquire_ordinary(Direction::Write).unwrap();
        ordinary.release();
        ordinary.release();
        let bundled = fd.acquire_bundled(Direction::Write).unwrap();
        drop(ordinary);
        drop(bundled);

        let bundled = fd.acquire_bundled(Direction::Write).unwrap();
        let bundled_kernel_ref = bundled.clone();
        drop(bundled);
        assert!(fd.acquire_ordinary(Direction::Write).is_err());
        drop(bundled_kernel_ref);
        assert!(fd.acquire_ordinary(Direction::Write).is_ok());

        drop(fd);
        unsafe { libc::close(write_end) };
    }
}
