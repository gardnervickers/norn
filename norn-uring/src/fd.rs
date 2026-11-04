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
    ///
    /// Dropping an in-flight operation (an unfinished write, or an `incoming()`
    /// stream) only requests its cancellation. The operation keeps its reference
    /// to the descriptor until the kernel reports its terminal completion, which
    /// the driver reaps on a later turn, so a `close` called right after such a
    /// drop can still return `WouldBlock`. Yield to the driver (for example with
    /// [`noop`](crate::noop)) and retry, or let the last owner's drop close the
    /// descriptor.
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
    /// Set when the runtime requests cancellation of an explicit close, and
    /// cleared when the next explicit close starts.
    close_canceled: Cell<bool>,
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
            close_canceled: Cell::new(false),
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
            // The flag describes only the close submitted below. A submitted
            // close that is dropped is still reaped, which sets `closed`. The
            // flag goes stale when the future is dropped before its entry
            // reaches the SQ (while the push waits for space): the guard below
            // set it, nothing was submitted, and the descriptor is still open.
            // A genuine -ECANCELED from a later close would then look
            // runtime-cancelled and trigger a close by fd number.
            self.inner.close_canceled.set(false);
            // Dropping this future before completion cancels a submitted close.
            // Record that before its terminal CQE can be reaped.
            let mut cancel = RecordCloseCancel(Some(&self.inner.close_canceled));
            let result = handle
                .submit(CloseFd {
                    inner: Rc::clone(&self.inner),
                })
                .await;
            cancel.0 = None;
            result
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

struct RecordCloseCancel<'a>(Option<&'a Cell<bool>>);

impl Drop for RecordCloseCancel<'_> {
    fn drop(&mut self) {
        if let Some(canceled) = self.0 {
            canceled.set(true);
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
    // The operation retains the descriptor while its copied integer is visible
    // to the kernel. If the caller drops both the close future and `NornFd`,
    // this prevents `Inner::drop` from queuing a second close for that number.
    inner: Rc<Inner>,
}

fn classify_close_completion(
    result: crate::operation::CQEResult,
    cancel_requested: bool,
) -> CloseResult {
    let synthetic = result.is_synthetic();
    match result.into_result() {
        Ok(_) => CloseResult::Closed,
        Err(err) if synthetic => CloseResult::NeverSubmitted(err),
        // After the runtime cancels a close, `-ECANCELED` means the request did
        // not execute. Once that terminal CQE arrives, the descriptor can be
        // closed directly without racing the original SQE. Without such a
        // cancellation it is the executed close's `->flush` result: the kernel
        // has already removed the descriptor, as for any other kernel error.
        Err(err) if cancel_requested && err.raw_os_error() == Some(libc::ECANCELED) => {
            CloseResult::Canceled(err)
        }
        Err(err) => CloseResult::KernelError(err),
    }
}

// Safety: `inner` retains the descriptor through the terminal CQE. `reap`
// records a successful kernel close or closes directly after a synthetic or
// runtime-canceled request, so the queued result owns no descriptor.
unsafe impl Operation for CloseFd {
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(opcode::Close::new(self.inner.fd).build())
    }

    unsafe fn reap(&mut self, result: crate::operation::CQEResult) -> Self::Completion {
        let cancel_requested = self.inner.close_canceled.get()
            || self
                .inner
                .handle
                .as_ref()
                .is_some_and(Handle::cancel_all_requested);
        self.inner
            .finish_tracked_close(classify_close_completion(result, cancel_requested))
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
    use crate::test_util::assert_pipe_reader_closed;

    fn pipe() -> [RawFd; 2] {
        let mut fds = [0; 2];
        let rc = unsafe { libc::pipe(fds.as_mut_ptr()) };
        assert_eq!(rc, 0);
        fds
    }

    fn assert_open(fd: RawFd) {
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
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
        let synthetic = classify_close_completion(
            crate::operation::CQEResult::synthetic(Err(io::Error::from_raw_os_error(libc::EIO))),
            false,
        );
        assert!(matches!(
            synthetic,
            CloseResult::NeverSubmitted(err) if err.raw_os_error() == Some(libc::EIO)
        ));

        let canceled = classify_close_completion(
            crate::operation::CQEResult::new(Err(io::Error::from_raw_os_error(libc::ECANCELED)), 0),
            true,
        );
        assert!(matches!(
            canceled,
            CloseResult::Canceled(err) if err.raw_os_error() == Some(libc::ECANCELED)
        ));

        // Without a runtime cancellation, `-ECANCELED` came from the executed close.
        let executed = classify_close_completion(
            crate::operation::CQEResult::new(Err(io::Error::from_raw_os_error(libc::ECANCELED)), 0),
            false,
        );
        assert!(matches!(
            executed,
            CloseResult::KernelError(err) if err.raw_os_error() == Some(libc::ECANCELED)
        ));

        let kernel = classify_close_completion(
            crate::operation::CQEResult::new(Err(io::Error::from_raw_os_error(libc::EIO)), 0),
            true,
        );
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

        // Model the runtime cancelling the close before the kernel executed it.
        close.inner.close_canceled.set(true);
        // Safety: the test supplies this `CloseFd`'s unreaped terminal CQE.
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
        // Safety: the test supplies this `CloseFd`'s unreaped terminal CQE.
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
    fn executed_close_reporting_ecanceled_does_not_close_reused_descriptor() {
        crate::test_util::run_isolated(
            "fd::tests::executed_close_reporting_ecanceled_does_not_close_reused_descriptor",
            executed_close_reporting_ecanceled_does_not_close_reused_descriptor_isolated,
        );
    }

    fn executed_close_reporting_ecanceled_does_not_close_reused_descriptor_isolated() {
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd(read_end);
        let mut close = CloseFd {
            inner: Rc::clone(&fd.inner),
        };
        drop(fd);

        // Model `io_close` removing the descriptor before `filp_close` returns
        // `-ECANCELED` from `->flush` (for example a FUSE FLUSH reply), followed by
        // reuse of the integer descriptor. The runtime never requested cancellation.
        assert_eq!(unsafe { libc::close(read_end) }, 0);
        assert_eq!(unsafe { libc::dup2(write_end, read_end) }, read_end);
        // Safety: the test supplies this `CloseFd`'s unreaped terminal CQE.
        let err = unsafe {
            close.reap(crate::operation::CQEResult::new(
                Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                0,
            ))
        }
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
        assert_open(read_end);

        drop(close);
        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[test]
    fn dropping_submitted_close_records_runtime_cancellation() {
        use norn_executor::park::{Park, ParkMode};
        use std::future::Future;
        use std::task::{Context, Waker};

        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd_on(read_end, driver.handle());
        let inner = Rc::downgrade(&fd.inner);

        let mut close = Box::pin(async move { fd.close().await });
        assert!(close
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(close);

        // The submitted close retains the descriptor until its terminal CQE, and
        // the cancellation is recorded before that CQE can be reaped.
        let retained = inner.upgrade().expect("submitted close retains descriptor");
        assert!(retained.close_canceled.get());
        assert!(!retained.closed.get());
        drop(retained);

        // A pipe has no `->flush`, so the kernel closes it inline before the
        // cancellation can match it.
        driver.park(ParkMode::NoPark).unwrap();
        assert_pipe_reader_closed(write_end);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn starting_a_close_clears_a_stale_cancellation_flag() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd_on(read_end, driver.handle());
        // Model an earlier close future dropped before its entry reached the SQ:
        // its guard set the flag, nothing was submitted, the descriptor is open.
        fd.inner.close_canceled.set(true);
        let inner = Rc::downgrade(&fd.inner);

        let mut close = Box::pin(async move { fd.close().await });
        assert!(close
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());

        let submitted = inner.upgrade().expect("submitted close retains descriptor");
        assert!(
            !submitted.close_canceled.get(),
            "a new close must not inherit the previous close's cancellation"
        );
        drop(submitted);
        drop(close);
        drop(driver);
        unsafe { libc::close(write_end) };
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
