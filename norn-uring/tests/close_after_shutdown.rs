#![cfg(target_os = "linux")]
//! A descriptor dropped after its driver shut down must still be closed.
//!
//! The test observes process-wide state (the descriptor table), so keep it the
//! only test in this binary.

use std::time::Duration;

use norn_uring::net::UdpSocket;

fn fd_count() -> usize {
    std::fs::read_dir("/proc/self/fd").unwrap().count()
}

/// Resources that outlive the executor must not leak their descriptors.
///
/// Driver shutdown used to submit `IOSQE_IO_DRAIN` sentinels, which leave the
/// ring in drain mode for the rest of its life, and a `NornFd` dropped after
/// shutdown still queued `IORING_OP_CLOSE` on that ring (`Inner::drop` ->
/// `Shared::close_fd`). Drain mode forced the close onto the deferred/io-wq
/// path, so it did not run inline; dropping the last `Handle` then closed the
/// ring, and ring exit cancelled the not-yet-run close. The descriptor stayed
/// open forever.
#[test]
fn descriptors_dropped_after_driver_shutdown_are_closed() {
    let before = fd_count();
    for _ in 0..50 {
        let driver = norn_uring::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let mut ex = norn_executor::LocalExecutor::new(driver);
        let socket = ex.block_on(async {
            UdpSocket::bind("127.0.0.1:0".parse().unwrap())
                .await
                .unwrap()
        });
        // Executor drop shuts the driver down; the socket keeps a Handle.
        drop(ex);
        // Give io-wq workers from earlier iterations time to go idle.
        std::thread::sleep(Duration::from_millis(20));
        drop(socket);
    }
    // Allow any legitimately asynchronous close to finish.
    std::thread::sleep(Duration::from_millis(500));
    let after = fd_count();
    assert_eq!(
        after,
        before,
        "{} descriptors leaked by drops after driver shutdown",
        after as isize - before as isize
    );
}
