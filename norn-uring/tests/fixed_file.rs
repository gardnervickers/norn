#![cfg(target_os = "linux")]

use std::io::Read;
use std::os::fd::AsFd;
use std::os::unix::net::UnixStream;

use norn_uring::Handle;

mod util;

#[test]
fn the_table_keeps_an_inserted_socket_open_until_its_slot_is_released(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async move {
        let table = Handle::current().register_fixed_files(2)?;
        let (local, mut peer) = UnixStream::pair()?;
        let fixed = table.insert_fd(local.as_fd())?;
        assert_eq!(table.available(), 1);
        // The table's reference keeps the socket open.
        drop(local);
        peer.set_nonblocking(true)?;
        let mut byte = [0_u8; 1];
        assert_eq!(
            peer.read(&mut byte).unwrap_err().kind(),
            std::io::ErrorKind::WouldBlock
        );

        // Dropping the last owner removes the table reference and closes it.
        drop(fixed);
        assert_eq!(table.available(), 2);
        assert_eq!(peer.read(&mut byte)?, 0);
        Ok(())
    })
}
