//! # File Descriptors
//!
//! We need a way to make sure that a file descriptor does not get
//! closed while we are using it. This can be when the app has a
//! reference to the file descriptor, but it can also be when
//! the kernel is using the file descriptor.
//!
//! Essentially we need a reference counted file descriptor.
//!
use std::cell::Cell;
use std::io;
use std::os::fd::RawFd;
use std::rc::Rc;

use io_uring::{opcode, types};
use log::warn;

use crate::driver::CloseFdError;
use crate::operation::{Op, Operation, Singleshot};
use crate::Handle;

/// A driver-bound file descriptor used by `io_uring` operations.
///
/// `UringFd` owns the descriptor and keeps it associated with the driver that
/// created it. Higher-level types such as filesystem files and sockets are
/// wrappers around this resource.
///
/// Submitted operations retain the descriptor until their terminal
/// completion. Consequently, [`UringFd::close`] returns
/// [`io::ErrorKind::WouldBlock`] while operations or internal shared socket
/// views still retain it.
pub struct UringFd {
    inner: NornFd,
}

impl std::fmt::Debug for UringFd {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UringFd").finish_non_exhaustive()
    }
}

impl UringFd {
    pub(crate) fn from_fd_on(fd: RawFd, handle: Handle) -> Self {
        Self {
            inner: NornFd::from_fd_on(fd, handle),
        }
    }

    pub(crate) fn from_inner(inner: NornFd) -> Self {
        Self { inner }
    }

    pub(crate) fn lease(&self) -> NornFd {
        self.inner.clone()
    }

    pub(crate) fn clone_internal(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }

    pub(crate) fn fd(&self) -> types::Fd {
        self.inner.fd()
    }

    pub(crate) fn handle(&self) -> &Handle {
        self.inner
            .inner
            .handle
            .as_ref()
            .expect("descriptor is not bound to an io_uring driver")
    }

    pub(crate) fn submit<T>(&self, op: T) -> Op<T>
    where
        T: Operation + 'static,
    {
        self.handle().submit(op)
    }

    /// Close the descriptor.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::WouldBlock`] if an operation or another
    /// internal socket view still retains the descriptor. Other errors are
    /// reported by the kernel close operation.
    pub async fn close(self) -> io::Result<()> {
        self.inner.close().await
    }
}

/// [`NornFd`] is a reference counted file descriptor.
#[derive(Clone, Debug)]
pub(crate) struct NornFd {
    inner: Rc<Inner>,
}

#[derive(Debug)]
struct Inner {
    fd: types::Fd,
    handle: Option<Handle>,
    closed: Cell<bool>,
}

impl NornFd {
    /// Create a new [`NornFd`] from a regular file descriptor.
    pub(crate) fn from_fd(fd: RawFd) -> Self {
        Self::new(types::Fd(fd))
    }

    pub(crate) fn from_fd_on(fd: RawFd, handle: Handle) -> Self {
        Self::new_with_handle(types::Fd(fd), Some(handle))
    }

    fn new(fd: types::Fd) -> Self {
        Self::new_with_handle(fd, Handle::try_current())
    }

    fn new_with_handle(fd: types::Fd, handle: Option<Handle>) -> Self {
        let inner = Inner {
            fd,
            handle,
            closed: Cell::new(false),
        };
        let inner = Rc::new(inner);
        Self { inner }
    }

    pub(crate) fn fd(&self) -> types::Fd {
        self.inner.fd
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
            handle
                .submit(CloseFd {
                    inner: Rc::clone(&self.inner),
                })
                .await
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
                self.finish_drop_close(handle.close_fd(self.fd));
            } else if let Err(err) = self.close_direct_and_invalidate() {
                warn!(target: "norn_uring::fd", "direct_close.failed: {err}");
            }
        }
    }
}

enum CloseResult {
    Closed,
    NeverSubmitted(io::Error),
    Canceled(io::Error),
    KernelError(io::Error),
}

impl Inner {
    fn finish_tracked_close(&self, result: CloseResult) -> io::Result<()> {
        match result {
            CloseResult::Closed => {
                self.closed.set(true);
                Ok(())
            }
            CloseResult::NeverSubmitted(_submit_err) | CloseResult::Canceled(_submit_err) => {
                self.close_direct_and_invalidate()
            }
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
        // On Linux a close error does not preserve ownership of the descriptor number.
        self.closed.set(true);
        result
    }

    fn close_direct(&self) -> io::Result<()> {
        let res = unsafe { libc::close(self.fd.0) };
        if res == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

struct CloseFd {
    // The close request, rather than the caller, owns the descriptor while the
    // copied integer is visible to the kernel. This prevents dropping a
    // canceled close future from queuing a second close for the same number.
    inner: Rc<Inner>,
}

fn classify_close_completion(result: crate::operation::CQEResult) -> CloseResult {
    let synthetic = result.is_synthetic();
    match result.into_result() {
        Ok(_) => CloseResult::Closed,
        Err(err) if synthetic => CloseResult::NeverSubmitted(err),
        // Cancellation proves that the close request did not execute. Once its
        // terminal CQE has arrived, a direct fallback can safely consume the
        // descriptor without racing the original SQE.
        Err(err) if err.raw_os_error() == Some(libc::ECANCELED) => CloseResult::Canceled(err),
        Err(err) => CloseResult::KernelError(err),
    }
}

// Safety: `inner` keeps the descriptor owned until the terminal CQE. Reaping
// immediately reconciles whether the kernel consumed that ownership; the
// queued completion contains no descriptor resource of its own.
unsafe impl Operation for CloseFd {
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(opcode::Close::new(self.inner.fd).build())
    }

    unsafe fn reap(&mut self, result: crate::operation::CQEResult) -> Self::Completion {
        self.inner
            .finish_tracked_close(classify_close_completion(result))
    }
}

impl Singleshot for CloseFd {
    type Output = io::Result<()>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
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
        let synthetic = classify_close_completion(crate::operation::CQEResult::synthetic(Err(
            io::Error::from_raw_os_error(libc::EIO),
        )));
        assert!(matches!(
            synthetic,
            CloseResult::NeverSubmitted(err) if err.raw_os_error() == Some(libc::EIO)
        ));

        let canceled = classify_close_completion(crate::operation::CQEResult::new(
            Err(io::Error::from_raw_os_error(libc::ECANCELED)),
            0,
        ));
        assert!(matches!(
            canceled,
            CloseResult::Canceled(err) if err.raw_os_error() == Some(libc::ECANCELED)
        ));

        let kernel = classify_close_completion(crate::operation::CQEResult::new(
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
    fn close_operation_owns_descriptor_until_reap() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);
        let mut close = CloseFd {
            inner: Rc::clone(&fd.inner),
        };

        drop(fd);
        assert_open(read_end);

        // Safety: this models the unique terminal CQE for this CloseFd.
        unsafe {
            close.reap(crate::operation::CQEResult::new(
                Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                0,
            ))
        }
        .unwrap();
        assert_pipe_reader_closed(write_end);

        drop(close);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn unconsumed_successful_close_cannot_close_reused_descriptor() {
        crate::test_util::run_isolated(
            "fd::tests::unconsumed_successful_close_cannot_close_reused_descriptor",
            unconsumed_successful_close_cannot_close_reused_descriptor_isolated,
        );
    }

    fn unconsumed_successful_close_cannot_close_reused_descriptor_isolated() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);
        let mut close = CloseFd {
            inner: Rc::clone(&fd.inner),
        };
        drop(fd);

        // Model the kernel consuming the close, followed by descriptor-number
        // reuse before the queued typed completion is observed or dropped.
        assert_eq!(unsafe { libc::close(read_end) }, 0);
        assert_eq!(unsafe { libc::dup2(write_end, read_end) }, read_end);
        // Safety: this models the unique terminal CQE for this CloseFd.
        let completion = unsafe { close.reap(crate::operation::CQEResult::new(Ok(0), 0)) };
        drop(completion);
        drop(close);

        assert_open(read_end);
        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[test]
    fn terminal_close_error_does_not_close_reused_descriptor() {
        crate::test_util::run_isolated(
            "fd::tests::terminal_close_error_does_not_close_reused_descriptor",
            terminal_close_error_does_not_close_reused_descriptor_isolated,
        );
    }

    fn terminal_close_error_does_not_close_reused_descriptor_isolated() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);

        // Model the kernel invalidating the original descriptor before returning a late error,
        // then reusing the same integer descriptor. Isolation prevents another parallel test
        // from claiming the number between close and dup2.
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
}
