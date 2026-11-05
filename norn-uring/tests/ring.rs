#![cfg(target_os = "linux")]

use std::io;
use std::num::NonZeroU32;
use std::time::Duration;

use norn_executor::LocalExecutor;
use norn_uring::ring::{IoWqMaxWorkers, NapiConfig, NapiSettings, NapiTracking};
use norn_uring::Driver;

fn unsupported(err: &io::Error) -> bool {
    matches!(
        err.raw_os_error(),
        Some(libc::EINVAL | libc::EOPNOTSUPP | libc::ENOSYS)
    )
}

#[test]
fn io_wq_worker_limits_round_trip() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let handle = driver.handle();
    let original = match handle.io_wq_max_workers() {
        Err(err) if unsupported(&err) => return Ok(()),
        result => result?,
    };

    let limits = IoWqMaxWorkers {
        bounded: NonZeroU32::new(3),
        unbounded: NonZeroU32::new(5),
    };
    assert_eq!(handle.set_io_wq_max_workers(limits)?, original);
    assert_eq!(handle.io_wq_max_workers()?, limits);

    // `None` leaves a limit unchanged.
    let previous = handle.set_io_wq_max_workers(IoWqMaxWorkers {
        bounded: NonZeroU32::new(2),
        unbounded: None,
    })?;
    assert_eq!(previous, limits);
    assert_eq!(
        handle.io_wq_max_workers()?,
        IoWqMaxWorkers {
            bounded: NonZeroU32::new(2),
            unbounded: NonZeroU32::new(5),
        }
    );

    let err = handle
        .set_io_wq_max_workers(IoWqMaxWorkers {
            bounded: NonZeroU32::new(u32::MAX),
            unbounded: None,
        })
        .unwrap_err();
    assert_eq!(err.raw_os_error(), Some(libc::EINVAL));

    if original != IoWqMaxWorkers::default() {
        handle.set_io_wq_max_workers(original)?;
    }
    Ok(())
}

#[test]
fn napi_registration_reports_previous_settings() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let handle = driver.handle();
    let config = NapiConfig {
        busy_poll_timeout: Duration::from_micros(50),
        prefer_busy_poll: true,
        tracking: NapiTracking::Dynamic,
    };
    let previous = match handle.register_napi(config) {
        Err(err) if unsupported(&err) => return Ok(()),
        result => result?,
    };
    assert_eq!(
        previous,
        NapiSettings {
            busy_poll_timeout: Duration::ZERO,
            prefer_busy_poll: false,
        }
    );

    let previous = handle.register_napi(NapiConfig::new(Duration::from_micros(10)))?;
    assert_eq!(
        previous,
        NapiSettings {
            busy_poll_timeout: Duration::from_micros(50),
            prefer_busy_poll: true,
        }
    );

    let previous = handle.unregister_napi()?;
    assert_eq!(previous.busy_poll_timeout, Duration::from_micros(10));
    assert!(!previous.prefer_busy_poll);
    assert_eq!(handle.unregister_napi()?.busy_poll_timeout, Duration::ZERO);
    Ok(())
}

#[test]
fn static_napi_ids_require_static_tracking() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let handle = driver.handle();
    let config = NapiConfig {
        tracking: NapiTracking::Static,
        ..NapiConfig::new(Duration::from_micros(10))
    };
    match handle.register_napi(config) {
        // Static tracking requires Linux 6.13.
        Err(err) if unsupported(&err) => return Ok(()),
        result => drop(result?),
    }

    // NAPI ids at or below NR_CPUS are reserved for sender CPUs.
    let id = 1 << 20;
    handle.add_napi_id(id)?;
    handle.remove_napi_id(id)?;
    assert!(handle.remove_napi_id(id).is_err());

    handle.unregister_napi()?;
    assert_eq!(
        handle.add_napi_id(id).unwrap_err().raw_os_error(),
        Some(libc::EINVAL)
    );
    Ok(())
}

#[test]
fn napi_busy_polling_keeps_parks_and_shutdown_working() -> Result<(), Box<dyn std::error::Error>> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let handle = driver.handle();
    match handle.register_napi(NapiConfig::new(Duration::from_micros(100))) {
        Err(err) if unsupported(&err) => return Ok(()),
        result => drop(result?),
    }

    let mut executor = LocalExecutor::new(driver);
    executor.block_on(async {
        for _ in 0..4 {
            norn_uring::noop().await?;
        }
        io::Result::Ok(())
    })?;
    drop(executor);

    // The handle keeps the ring alive after shutdown; controls still apply.
    let previous = handle.unregister_napi()?;
    assert_eq!(previous.busy_poll_timeout, Duration::from_micros(100));
    assert!(handle
        .capabilities()?
        .supports_opcode(io_uring::opcode::Nop::CODE));
    Ok(())
}
