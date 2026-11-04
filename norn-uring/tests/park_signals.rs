#![cfg(target_os = "linux")]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use norn_executor::park::{Park, ParkMode};
use norn_uring::Driver;

extern "C" fn ignore_signal(_: libc::c_int) {}

#[test]
fn timed_park_returns_by_its_deadline_under_repeated_signals() {
    // A handler (not SIG_IGN) makes the blocking io_uring_enter return EINTR.
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        action.sa_sigaction = ignore_signal as *const () as usize;
        action.sa_flags = 0;
        libc::sigemptyset(&mut action.sa_mask);
        assert_eq!(
            libc::sigaction(libc::SIGUSR1, &action, std::ptr::null_mut()),
            0
        );
    }

    let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    // Arm the eventfd read now, so the timed park below submits nothing and
    // waits for its whole duration.
    driver
        .park(ParkMode::Timeout(Duration::from_millis(1)))
        .unwrap();

    // Model a periodic process signal (for example a SIGPROF profiler or an
    // interval timer) that arrives more often than the park timeout.
    let target = unsafe { libc::pthread_self() };
    let stop = Arc::new(AtomicBool::new(false));
    let signaller = {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let give_up = Instant::now() + Duration::from_secs(2);
            while !stop.load(Ordering::Acquire) && Instant::now() < give_up {
                unsafe { libc::pthread_kill(target, libc::SIGUSR1) };
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };

    let start = Instant::now();
    driver
        .park(ParkMode::Timeout(Duration::from_millis(50)))
        .unwrap();
    let elapsed = start.elapsed();
    stop.store(true, Ordering::Release);
    signaller.join().unwrap();

    assert!(
        elapsed < Duration::from_millis(500),
        "a 50 ms timed park took {elapsed:?}"
    );
}
