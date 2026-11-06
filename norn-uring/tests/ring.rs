#![cfg(target_os = "linux")]

use std::io;

use io_uring::opcode;
use norn_uring::Driver;

#[test]
fn capabilities_match_a_raw_probe_for_every_opcode() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let capabilities = driver.handle().capabilities().clone();

    let ring = io_uring::IoUring::new(2)?;
    let mut probe = io_uring::Probe::new();
    ring.submitter().register_probe(&mut probe)?;
    for code in 0..=u8::MAX {
        assert_eq!(
            capabilities.supports_opcode(code),
            probe.is_supported(code),
            "opcode {code}"
        );
    }
    let expected: Vec<u8> = (0..=u8::MAX)
        .filter(|&code| probe.is_supported(code))
        .collect();
    assert_eq!(
        capabilities.supported_opcodes().collect::<Vec<_>>(),
        expected
    );
    assert!(capabilities.supports_opcode(opcode::Nop::CODE));
    assert!(!capabilities.supports_opcode(u8::MAX));
    Ok(())
}

#[test]
fn capabilities_outlive_the_driver() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let handle = driver.handle();
    let before = handle.capabilities().clone();
    drop(driver);
    assert_eq!(handle.capabilities(), &before);
    Ok(())
}
