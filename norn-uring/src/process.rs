//! Child-process operations.
//!
//! These functions submit `IORING_OP_WAITID`, which requires Linux 6.7 or
//! newer. Older kernels reject the request with `EINVAL`.
//!
//! Only exit notification for a single child selected by process ID is
//! supported. Waiting for stopped or continued children, process groups,
//! `P_ALL`, pidfds, and `WNOHANG` polling are intentionally not exposed.

use std::io;
use std::os::unix::process::ExitStatusExt;
use std::process::ExitStatus;
use std::ptr;

use io_uring::opcode;

use crate::operation::{CQEResult, Operation, Singleshot};
use crate::Handle;

/// Wait for the child process `pid` to exit and reap it.
///
/// This is equivalent to `waitid(P_PID, pid, &info, WEXITED)`. `pid` is
/// typically [`std::process::Child::id`]. After this request succeeds the
/// child no longer exists, so [`std::process::Child::wait`] for the same child
/// fails with `ECHILD`. Use [`observe_exit`] to leave the child waitable.
///
/// Dropping the request before it resolves requests cancellation. If the child
/// exits concurrently, the kernel may still reap it; the status is then
/// discarded with the abandoned completion.
///
/// The returned request is lazy and can be linked or given a timeout.
///
/// # Example
///
/// ```no_run
/// use std::process::Command;
///
/// # async fn run() -> std::io::Result<()> {
/// let child = Command::new("true").spawn()?;
/// let status = norn_uring::process::wait_exit(child.id()).await?;
/// assert!(status.success());
/// # Ok(())
/// # }
/// ```
///
/// # Errors
///
/// Returns [`io::ErrorKind::InvalidInput`] if `pid` is zero or exceeds the
/// kernel's PID range, `ECHILD` if `pid` is not an unwaited child of the calling
/// process (including when `SIGCHLD` is ignored, which makes the kernel reap
/// children automatically), or `ECANCELED` if the request is cancelled.
///
/// # Panics
///
/// Panics if called outside an active [`Driver`](crate::Driver) context.
pub fn wait_exit(pid: u32) -> impl crate::Request<Output = io::Result<ExitStatus>> {
    Handle::current().submit(WaitExit::new(pid, Reap::Yes))
}

/// Wait for the child process `pid` to exit without reaping it.
///
/// This is equivalent to `waitid(P_PID, pid, &info, WEXITED | WNOWAIT)`. The
/// child remains a zombie and must still be reaped, for example with
/// [`std::process::Child::wait`] or [`wait_exit`], which then complete
/// immediately.
///
/// The returned request is lazy and can be linked or given a timeout.
///
/// # Errors
///
/// Returns the same errors as [`wait_exit`].
///
/// # Panics
///
/// Panics if called outside an active [`Driver`](crate::Driver) context.
pub fn observe_exit(pid: u32) -> impl crate::Request<Output = io::Result<ExitStatus>> {
    Handle::current().submit(WaitExit::new(pid, Reap::No))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Reap {
    Yes,
    No,
}

struct WaitExit {
    pid: u32,
    options: libc::c_int,
    // Written by the kernel when the wait completes.
    info: libc::siginfo_t,
}

impl WaitExit {
    fn new(pid: u32, reap: Reap) -> Self {
        let options = match reap {
            Reap::Yes => libc::WEXITED,
            Reap::No => libc::WEXITED | libc::WNOWAIT,
        };
        Self {
            pid,
            options,
            // Safety: `siginfo_t` is a plain C structure for which all-zero
            // bytes are a valid value.
            info: unsafe { std::mem::zeroed() },
        }
    }
}

// Safety: the SQE references only `info`, which is stored inline in the
// operation at a stable address through the terminal CQE. A wait owns no
// kernel resource that must be released after completion.
unsafe impl Operation for WaitExit {
    type Completion = io::Result<ExitStatus>;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        // The kernel reads the PID from a signed SQE field and rejects
        // non-positive values for `P_PID`.
        if self.pid == 0 || i32::try_from(self.pid).is_err() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "child process ID must be in 1..=i32::MAX",
            ));
        }
        let info = ptr::addr_of_mut!(self.info).cast_const();
        Ok(opcode::WaitId::new(libc::P_PID, self.pid, self.options)
            .infop(info)
            .build())
    }

    unsafe fn reap(&mut self, result: CQEResult) -> Self::Completion {
        result.into_result()?;
        exit_status(&self.info)
    }
}

impl Singleshot for WaitExit {
    type Output = io::Result<ExitStatus>;

    fn complete(self, completion: Self::Completion) -> Self::Output {
        completion
    }
}

/// Encode the `waitid` exit information as a `wait(2)` status word.
fn exit_status(info: &libc::siginfo_t) -> io::Result<ExitStatus> {
    // Safety: a successful `waitid` for `WEXITED` fills the `SIGCHLD` fields.
    let status = unsafe { info.si_status() };
    let raw = match info.si_code {
        libc::CLD_EXITED => (status & 0xff) << 8,
        libc::CLD_KILLED => status & 0x7f,
        libc::CLD_DUMPED => (status & 0x7f) | 0x80,
        code => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("waitid reported unexpected child state code {code}"),
            ))
        }
    };
    Ok(ExitStatus::from_raw(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mirror of the kernel's `SIGCHLD` siginfo layout, used to build test
    // completions because libc exposes these fields only through accessors.
    #[repr(C)]
    struct SigChld {
        signo: libc::c_int,
        errno: libc::c_int,
        code: libc::c_int,
        fields: SigChldFields,
    }

    // The `long` members give the field union its kernel alignment.
    #[repr(C)]
    struct SigChldFields {
        pid: libc::pid_t,
        uid: libc::uid_t,
        status: libc::c_int,
        utime: libc::c_long,
        stime: libc::c_long,
    }

    fn info(code: libc::c_int, status: libc::c_int) -> libc::siginfo_t {
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        let fields = SigChld {
            signo: libc::SIGCHLD,
            errno: 0,
            code,
            fields: SigChldFields {
                pid: 1234,
                uid: 0,
                status,
                utime: 0,
                stime: 0,
            },
        };
        assert!(std::mem::size_of::<SigChld>() <= std::mem::size_of::<libc::siginfo_t>());
        unsafe { ptr::write(ptr::addr_of_mut!(info).cast::<SigChld>(), fields) };
        info
    }

    #[test]
    fn decodes_exit_codes_and_signals() {
        let exited = exit_status(&info(libc::CLD_EXITED, 3)).unwrap();
        assert_eq!(exited.code(), Some(3));
        assert_eq!(exited.signal(), None);

        let killed = exit_status(&info(libc::CLD_KILLED, libc::SIGKILL)).unwrap();
        assert_eq!(killed.code(), None);
        assert_eq!(killed.signal(), Some(libc::SIGKILL));
        assert!(!killed.core_dumped());

        let dumped = exit_status(&info(libc::CLD_DUMPED, libc::SIGSEGV)).unwrap();
        assert_eq!(dumped.signal(), Some(libc::SIGSEGV));
        assert!(dumped.core_dumped());

        let err = exit_status(&info(libc::CLD_STOPPED, libc::SIGSTOP)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn configures_waitid_for_one_child() {
        for (reap, options) in [
            (Reap::Yes, libc::WEXITED),
            (Reap::No, libc::WEXITED | libc::WNOWAIT),
        ] {
            let mut op = WaitExit::new(4321, reap);
            let entry = op.configure().unwrap();
            assert_eq!(entry.get_opcode(), opcode::WaitId::CODE.into());
            let bytes: [u8; 64] = unsafe { std::mem::transmute_copy(&entry) };
            // `fd` holds the PID, `len` the ID type, `file_index` the options,
            // and `addr2` the siginfo destination.
            assert_eq!(i32::from_ne_bytes(bytes[4..8].try_into().unwrap()), 4321);
            assert_eq!(
                u64::from_ne_bytes(bytes[8..16].try_into().unwrap()),
                ptr::addr_of!(op.info) as u64
            );
            assert_eq!(
                u32::from_ne_bytes(bytes[24..28].try_into().unwrap()),
                libc::P_PID
            );
            assert_eq!(
                i32::from_ne_bytes(bytes[44..48].try_into().unwrap()),
                options
            );
        }
    }

    #[test]
    fn rejects_out_of_range_pids() {
        for pid in [0, i32::MAX as u32 + 1, u32::MAX] {
            let err = WaitExit::new(pid, Reap::Yes).configure().unwrap_err();
            assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        }
    }

    #[test]
    fn kernel_errors_are_reported() {
        let mut op = WaitExit::new(1, Reap::Yes);
        let err = unsafe {
            op.reap(CQEResult::new(
                Err(io::Error::from_raw_os_error(libc::ECHILD)),
                0,
            ))
        }
        .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECHILD));
    }
}
