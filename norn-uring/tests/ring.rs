#![cfg(target_os = "linux")]

use std::io;

use io_uring::opcode;
use norn_uring::Driver;

#[test]
fn capabilities_report_supported_opcodes_and_setup() -> io::Result<()> {
    let driver = Driver::new(io_uring::IoUring::builder(), 8)?;
    let capabilities = driver.handle().capabilities()?;

    // Every kernel with a probe supports these, and none supports the last
    // opcode code.
    for code in [opcode::Nop::CODE, opcode::Read::CODE, opcode::Write::CODE] {
        assert!(capabilities.supports_opcode(code), "opcode {code}");
    }
    assert!(!capabilities.supports_opcode(u8::MAX));

    let supported: Vec<u8> = capabilities.supported_opcodes().collect();
    assert!(supported.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(supported.contains(&opcode::Nop::CODE));
    assert_eq!(capabilities.params().sq_entries(), 8);
    Ok(())
}
