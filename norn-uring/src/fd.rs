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

use io_uring::types::CancelBuilder;
use io_uring::{opcode, types};
use log::warn;

use crate::driver::CloseFdError;
use crate::operation::{CQEResult, Op, Operation, Singleshot};
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
    ///
    /// Once the close has been submitted, dropping the returned future does not
    /// cancel it: the kernel still closes the descriptor. The descriptor is
    /// released only through that close, never again by number.
    ///
    /// If the driver shuts down while the close waits in the kernel's worker
    /// queue, shutdown's cancellation can stop it before it runs. Such a close
    /// reports `ECANCELED`, which the runtime cannot tell apart from a file's
    /// own `->flush` error after the descriptor was removed. The runtime then
    /// leaves the descriptor alone and logs a warning, so the descriptor may
    /// stay open.
    pub async fn close(self) -> io::Result<()> {
        self.inner.close().await
    }

    /// Ask the kernel to cancel every in-flight request on this descriptor.
    ///
    /// The request resolves to the number of matches the kernel counted. Each
    /// canceled request then completes through its own future or stream,
    /// typically with `ECANCELED`, and releases its descriptor lease when
    /// consumed or dropped. This lets one task interrupt operations owned by
    /// other tasks, for example so that [`UringFd::close`] can succeed or a
    /// multishot stream ends after yielding the completions already posted.
    ///
    /// Treat the count as a hint. Zero means the kernel found nothing to
    /// cancel, but requests waiting for the same kernel worker can count once
    /// together, and requests that a stream keeps armed internally count too.
    ///
    /// Cancellation matches requests submitted before this one, including
    /// requests still waiting for submission-queue space, which are submitted
    /// ahead of it. Prepared requests that have not been polled, requests
    /// submitted afterwards, and linked requests the kernel has not issued yet
    /// (`b` in `a.then(b)` while `a` runs) are not matched. Canceling a
    /// request linked with [`Request::then`](crate::Request::then) also fails
    /// the requests linked after it, whatever their descriptor. A request
    /// hard-linked with [`Request::then_always`](crate::Request::then_always)
    /// is issued instead once its predecessor ends, so it can still be running
    /// on this descriptor afterwards. To cancel such chains too, repeat the
    /// cancellation until it reports zero.
    ///
    /// Streams that re-arm themselves, such as
    /// [`TcpListener::incoming`](crate::net::TcpListener::incoming) and
    /// receives into a buffer ring that pause at their unconsumed limit, may
    /// treat the cancellation as their own pause and submit a new request.
    ///
    /// The kernel matches requests by open file description, so in-flight
    /// requests on duplicates of this descriptor submitted to the same driver
    /// are also canceled. A request already executing in a kernel worker may
    /// be counted but finish normally. Requests on other drivers are not
    /// affected.
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be submitted, or `EINVAL` on
    /// kernels older than Linux 5.19, which lack descriptor-based
    /// cancellation.
    pub fn cancel_pending(&self) -> impl crate::Request<Output = io::Result<usize>> {
        self.submit(CancelFd { fd: self.lease() })
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
    /// Create a new [`NornFd`] from a regular file descriptor, bound to the
    /// driver current at the call. Production code binds with
    /// [`NornFd::from_fd_on`] or [`NornFd::sibling_from_fd`] instead, because
    /// the current driver is not necessarily the one that produced the
    /// descriptor.
    #[cfg(test)]
    pub(crate) fn from_fd(fd: RawFd) -> Self {
        Self::new(types::Fd(fd))
    }

    pub(crate) fn from_fd_on(fd: RawFd, handle: Handle) -> Self {
        Self::new_with_handle(types::Fd(fd), Some(handle))
    }

    /// Create a [`NornFd`] bound to the same driver as `self`, or unbound if
    /// `self` is unbound.
    pub(crate) fn sibling_from_fd(&self, fd: RawFd) -> Self {
        Self::new_with_handle(types::Fd(fd), self.inner.handle.clone())
    }

    #[cfg(test)]
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
            // The runtime never cancels a submitted close, so that an
            // `-ECANCELED` completion is the close's own result unless shutdown
            // cancelled every request; see `classify_close_completion`. A
            // cancelled close would have to be repeated by number, which is
            // safe only if the close is known not to have run.
            handle
                .submit(CloseFd {
                    inner: Rc::clone(&self.inner),
                })
                .uncancellable()
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
    /// The close never reached the kernel; the descriptor is still open.
    NeverSubmitted(io::Error),
    /// The kernel executed the close and reported an error. Linux removes the
    /// descriptor before running `->flush`, so the number is released.
    KernelError(io::Error),
    /// `-ECANCELED` after shutdown requested cancellation of every request:
    /// the close may have been cancelled before it ran, leaving the descriptor
    /// open, or may have run with `->flush` reporting `-ECANCELED`.
    Indeterminate(io::Error),
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
            CloseResult::Indeterminate(err) => {
                // Closing by number could close a descriptor that reused it.
                // Leaking the descriptor is the safe outcome.
                warn!(
                    target: "norn_uring::fd",
                    "close.indeterminate: fd={} {err}; shutdown cancellation may have stopped \
                     the close before it ran, and the descriptor may leak",
                    self.fd.0
                );
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

/// Classify an explicit close's terminal completion.
///
/// In Linux 6.18, `io_close` removes the descriptor from the table
/// (`file_close_fd_locked`) before `filp_close` runs `->flush`, so any error
/// from an executed close leaves the number released. A close of a file with
/// `->flush` is punted to io-wq, and a cancellation can stop it there without
/// it running: one that finds it pending removes it, and one that finds it
/// already taken by a worker sets `IO_WQ_WORK_CANCEL`, which
/// `io_wq_submit_work` checks before issuing. Either way it completes with
/// `-ECANCELED`, the same value a FUSE `->flush` can return after the close
/// ran.
///
/// The runtime never cancels a close individually (see `NornFd::close`), so
/// before shutdown's cancel-all an `-ECANCELED` completion is the executed
/// close's own result. After it, the completion cannot be attributed and is
/// [`CloseResult::Indeterminate`]. Neither case closes the number again.
fn classify_close_completion(
    result: crate::operation::CQEResult,
    cancel_all_requested: bool,
) -> CloseResult {
    let synthetic = result.is_synthetic();
    match result.into_result() {
        Ok(_) => CloseResult::Closed,
        Err(err) if synthetic => CloseResult::NeverSubmitted(err),
        Err(err) if cancel_all_requested && err.raw_os_error() == Some(libc::ECANCELED) => {
            CloseResult::Indeterminate(err)
        }
        Err(err) => CloseResult::KernelError(err),
    }
}

// Safety: `inner` retains the descriptor through the terminal CQE. `reap`
// records that the kernel consumed or may have consumed the descriptor, or
// closes it directly after a synthetic completion of a request that never
// reached the kernel, so the queued result owns no descriptor.
unsafe impl Operation for CloseFd {
    type Completion = io::Result<()>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(opcode::Close::new(self.inner.fd).build())
    }

    unsafe fn reap(&mut self, result: crate::operation::CQEResult) -> Self::Completion {
        let cancel_all_requested = self
            .inner
            .handle
            .as_ref()
            .is_some_and(Handle::cancel_all_requested);
        self.inner
            .finish_tracked_close(classify_close_completion(result, cancel_all_requested))
    }
}

impl Singleshot for CloseFd {
    type Output = io::Result<()>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

struct CancelFd {
    // The lease keeps the descriptor number bound to the same open file
    // description until the kernel has resolved it for this request.
    fd: NornFd,
}

// Safety: the SQE references no memory and names its target only by a
// descriptor that `fd` retains through the terminal CQE. Cancellation does not
// enable multishot completions.
unsafe impl Operation for CancelFd {
    type Completion = CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        let criteria = CancelBuilder::fd(self.fd.fd()).all();
        Ok(opcode::AsyncCancel2::new(criteria).build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result
    }
}

impl Singleshot for CancelFd {
    type Output = io::Result<usize>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        match completion.into_result() {
            Ok(count) => Ok(count as usize),
            // Linux reports a count with `IORING_ASYNC_CANCEL_ALL`; treat a
            // no-match result from any kernel the same way.
            Err(err) if err.raw_os_error() == Some(libc::ENOENT) => Ok(0),
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::assert_pipe_reader_closed;

    fn pipe() -> [RawFd; 2] {
        crate::test_util::pipe_cloexec()
    }

    fn assert_open(fd: RawFd) {
        assert_ne!(unsafe { libc::fcntl(fd, libc::F_GETFD) }, -1);
    }

    #[test]
    fn cancel_configures_descriptor_wide_async_cancel() {
        // `IORING_ASYNC_CANCEL_ALL | IORING_ASYNC_CANCEL_FD` from
        // `include/uapi/linux/io_uring.h`.
        const CANCEL_ALL_FD: u32 = 1 | 2;
        let [read_end, write_end] = pipe();
        let mut cancel = CancelFd {
            fd: NornFd::from_fd(read_end),
        };

        let entry = cancel.configure().unwrap();

        assert_eq!(entry.get_opcode(), opcode::AsyncCancel2::CODE.into());
        let bytes: [u8; 64] = unsafe { std::mem::transmute_copy(&entry) };
        assert_eq!(
            i32::from_ne_bytes(bytes[4..8].try_into().unwrap()),
            read_end
        );
        assert_eq!(u64::from_ne_bytes(bytes[16..24].try_into().unwrap()), 0);
        assert_eq!(
            u32::from_ne_bytes(bytes[28..32].try_into().unwrap()),
            CANCEL_ALL_FD
        );
        drop(cancel);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn cancel_completion_reports_matched_request_count() {
        let complete = |result| {
            let [read_end, write_end] = pipe();
            let cancel = CancelFd {
                fd: NornFd::from_fd(read_end),
            };
            let output = cancel.complete(CQEResult::new(result, 0));
            unsafe { libc::close(write_end) };
            output
        };

        assert_eq!(complete(Ok(3)).unwrap(), 3);
        assert_eq!(
            complete(Err(io::Error::from_raw_os_error(libc::ENOENT))).unwrap(),
            0
        );
        let err = complete(Err(io::Error::from_raw_os_error(libc::EINVAL))).unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::EINVAL));
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
        let kernel_err = |errno| CQEResult::new(Err(io::Error::from_raw_os_error(errno)), 0);

        for cancel_all_requested in [false, true] {
            assert!(matches!(
                classify_close_completion(CQEResult::new(Ok(0), 0), cancel_all_requested),
                CloseResult::Closed
            ));
            // A synthetic completion never reached the kernel, even if it
            // reports ECANCELED.
            for errno in [libc::EIO, libc::ECANCELED] {
                let synthetic = classify_close_completion(
                    CQEResult::synthetic(Err(io::Error::from_raw_os_error(errno))),
                    cancel_all_requested,
                );
                assert!(matches!(
                    synthetic,
                    CloseResult::NeverSubmitted(err) if err.raw_os_error() == Some(errno)
                ));
            }
            assert!(matches!(
                classify_close_completion(kernel_err(libc::EIO), cancel_all_requested),
                CloseResult::KernelError(err) if err.raw_os_error() == Some(libc::EIO)
            ));
        }

        // Nothing cancels a close individually, so before shutdown's cancel-all
        // `-ECANCELED` came from the executed close.
        assert!(matches!(
            classify_close_completion(kernel_err(libc::ECANCELED), false),
            CloseResult::KernelError(err) if err.raw_os_error() == Some(libc::ECANCELED)
        ));
        // After it, the close may or may not have run.
        assert!(matches!(
            classify_close_completion(kernel_err(libc::ECANCELED), true),
            CloseResult::Indeterminate(err) if err.raw_os_error() == Some(libc::ECANCELED)
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

        // Model shutdown failing the close before it reached the kernel.
        // Safety: the test supplies this `CloseFd`'s unreaped terminal completion.
        unsafe {
            close.reap(crate::operation::CQEResult::synthetic(Err(
                io::Error::from_raw_os_error(libc::ECANCELED),
            )))
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
    fn shutdown_canceled_close_does_not_close_reused_descriptor() {
        crate::test_util::run_isolated(
            "fd::tests::shutdown_canceled_close_does_not_close_reused_descriptor",
            shutdown_canceled_close_does_not_close_reused_descriptor_isolated,
        );
    }

    fn shutdown_canceled_close_does_not_close_reused_descriptor_isolated() {
        use norn_executor::park::Park;

        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd_on(read_end, driver.handle());
        let mut close = CloseFd {
            inner: Rc::clone(&fd.inner),
        };
        drop(fd);
        // Shutdown requests cancellation of every outstanding request.
        driver.shutdown();
        assert!(driver.handle().cancel_all_requested());

        // Model io-wq running the close before the cancel-all reached it: the
        // descriptor is removed, `->flush` reports `-ECANCELED`, and the
        // integer descriptor is reused.
        assert_eq!(unsafe { libc::close(read_end) }, 0);
        assert_eq!(unsafe { libc::dup2(write_end, read_end) }, read_end);
        // Safety: the test supplies this `CloseFd`'s unreaped terminal CQE.
        let err = unsafe {
            close.reap(CQEResult::new(
                Err(io::Error::from_raw_os_error(libc::ECANCELED)),
                0,
            ))
        }
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));
        assert_open(read_end);
        assert!(
            close.inner.closed.get(),
            "dropping must not close it either"
        );

        drop(close);
        assert_open(read_end);
        unsafe {
            libc::close(read_end);
            libc::close(write_end);
        }
    }

    #[test]
    fn dropping_submitted_close_does_not_cancel_it() {
        use norn_executor::park::{Park, ParkMode};
        use std::future::Future;
        use std::task::{Context, Waker};

        let mut driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let handle = driver.handle();
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd_on(read_end, driver.handle());
        let inner = Rc::downgrade(&fd.inner);

        let mut close = Box::pin(async move { fd.close().await });
        assert!(close
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(handle.in_flight(), 1);
        drop(close);

        // No cancellation is queued, so the close's own completion is the only
        // result, and the operation retains the descriptor until it arrives.
        assert_eq!(
            handle.in_flight(),
            1,
            "a dropped close must not be cancelled"
        );
        let retained = inner.upgrade().expect("submitted close retains descriptor");
        assert!(!retained.closed.get());
        drop(retained);

        for _ in 0..16 {
            if handle.in_flight() == 0 {
                break;
            }
            driver.park(ParkMode::NoPark).unwrap();
        }
        assert_eq!(handle.in_flight(), 0);
        assert!(
            inner.upgrade().is_none(),
            "the reaped close released the descriptor"
        );
        assert_pipe_reader_closed(write_end);
        unsafe { libc::close(write_end) };
    }

    #[test]
    fn close_dropped_before_shutdown_still_closes_descriptor() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let [read_end, write_end] = pipe();
        let fd = NornFd::from_fd_on(read_end, driver.handle());

        let mut close = Box::pin(async move { fd.close().await });
        assert!(close
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        drop(close);
        // Shutdown submits the queued close, cancels everything, and reaps it.
        // A pipe has no `->flush`, so the kernel closes it inline at submission,
        // before the cancel-all.
        drop(driver);

        assert_pipe_reader_closed(write_end);
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
