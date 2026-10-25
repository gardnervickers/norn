#![cfg(target_os = "linux")]

use std::time::Duration;

use norn_executor::park::{Park, ParkMode};
use norn_executor::LocalExecutor;
use norn_uring::{fs::File, Driver};

async fn read_after_timeout() {
    let file = File::open("/dev/zero").await.unwrap();
    let (result, buffer) = file.read_at(vec![1; 8], 0).await;
    assert_eq!(result.unwrap(), 8);
    assert_eq!(buffer, [0; 8]);
    file.close().await.unwrap();
}

#[test]
fn repeated_idle_timeouts_keep_driver_healthy_and_accept_io() {
    let mut driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let handle = driver.handle();
    // The first park arms the eventfd read. Later parks submit nothing, so
    // io_uring_enter reports ETIME rather than a positive submission count.
    for _ in 0..5 {
        driver
            .park(ParkMode::Timeout(Duration::from_millis(1)))
            .unwrap();
        assert!(handle.health_error().is_none());
    }
    let mut executor = LocalExecutor::new(driver);
    executor.block_on(read_after_timeout());
    assert!(handle.health_error().is_none());
    drop(executor); // Also exercise clean shutdown with the armed eventfd read.
    assert!(handle.health_error().is_none());
}

#[test]
fn timer_over_uring_sleeps_across_multiple_deadlines_then_accepts_io() {
    let driver = Driver::new(io_uring::IoUring::builder(), 8).unwrap();
    let handle = driver.handle();
    let clock = norn_timer::Clock::system();
    let timer = norn_timer::Driver::new(driver, clock.clone());
    let mut executor = LocalExecutor::new(timer);
    executor.block_on(async {
        let timer = norn_timer::Handle::current();
        for delay in [2, 4, 6, 8] {
            // Compute each target after the previous wake, so even a loaded
            // host must perform several distinct timed waits.
            let deadline = clock.now() + Duration::from_millis(delay);
            timer.sleep_until(deadline).await.unwrap();
            assert!(clock.now() >= deadline);
        }
        read_after_timeout().await;
    });
    assert!(handle.health_error().is_none());
    drop(executor);
    assert!(handle.health_error().is_none());
}
