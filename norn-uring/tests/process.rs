#![cfg(target_os = "linux")]

use std::io::{self, Write};
use std::os::unix::process::ExitStatusExt;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use norn_uring::process;
use norn_uring::Request;

mod util;

fn waitid_supported() -> bool {
    let supported = util::opcode_supported(io_uring::opcode::WaitId::CODE);
    if !supported {
        eprintln!("skipping: IORING_OP_WAITID requires Linux 6.7");
    }
    supported
}

fn shell(script: &str) -> io::Result<Child> {
    Command::new("sh").arg("-c").arg(script).spawn()
}

/// Spawn a child that blocks reading stdin and exits with `code` at EOF.
fn blocked_child(code: i32) -> io::Result<Child> {
    Command::new("sh")
        .arg("-c")
        .arg(format!("read _ignored; exit {code}"))
        .stdin(Stdio::piped())
        .spawn()
}

#[test]
fn wait_exit_reaps_child_exit_code() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mut child = shell("exit 3")?;

        let status = process::wait_exit(child.id()).await?;

        assert_eq!(status.code(), Some(3));
        let err = child.try_wait().unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECHILD));
        Ok(())
    })
}

#[test]
fn wait_exit_reports_terminating_signal() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let child = shell("kill -KILL $$")?;

        let status = process::wait_exit(child.id()).await?;

        assert_eq!(status.code(), None);
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        Ok(())
    })
}

#[test]
fn wait_exit_stays_pending_until_child_exits() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mut child = blocked_child(7)?;
        let mut wait = Box::pin(process::wait_exit(child.id()));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        norn_uring::noop().await.unwrap();
        assert!(futures_util::poll!(wait.as_mut()).is_pending());

        drop(child.stdin.take());

        assert_eq!(wait.await?.code(), Some(7));
        Ok(())
    })
}

#[test]
fn observe_exit_leaves_child_waitable() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mut child = shell("exit 5")?;

        let observed = process::observe_exit(child.id()).await?;
        assert_eq!(observed.code(), Some(5));
        let again = process::observe_exit(child.id()).await?;
        assert_eq!(again, observed);

        assert_eq!(child.wait()?, observed);
        Ok(())
    })
}

#[test]
fn timed_out_wait_does_not_reap_child() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mut child = blocked_child(9)?;

        let err = process::wait_exit(child.id())
            .timeout(Duration::from_millis(10))
            .await
            .unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECANCELED));

        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(b"\n")?;
        drop(stdin);
        assert_eq!(child.wait()?.code(), Some(9));
        Ok(())
    })
}

#[test]
fn dropped_wait_cancels_without_reaping() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let mut child = blocked_child(4)?;
        let mut wait = Box::pin(process::wait_exit(child.id()));
        assert!(futures_util::poll!(wait.as_mut()).is_pending());
        norn_uring::noop().await.unwrap();
        drop(wait);
        // Let the cancellation reach the kernel before the child exits.
        norn_uring::noop().await.unwrap();

        drop(child.stdin.take());
        assert_eq!(child.wait()?.code(), Some(4));
        Ok(())
    })
}

#[test]
fn pending_wait_is_cancelled_at_shutdown() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    let mut child = blocked_child(6)?;
    let pid = child.id();
    util::with_test_env(|| async move {
        // The task is still waiting when the executor and driver shut down.
        let task = norn_executor::spawn(process::wait_exit(pid));
        norn_uring::noop().await.unwrap();
        drop(task);
        Ok(())
    })?;

    drop(child.stdin.take());
    assert_eq!(child.wait()?.code(), Some(6));
    Ok(())
}

#[test]
fn wait_exit_rejects_non_children_and_invalid_pids() -> Result<(), Box<dyn std::error::Error>> {
    if !waitid_supported() {
        return Ok(());
    }
    util::with_test_env(|| async {
        let err = process::wait_exit(1).await.unwrap_err();
        assert_eq!(err.raw_os_error(), Some(libc::ECHILD));

        let err = process::wait_exit(0).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        Ok(())
    })
}
