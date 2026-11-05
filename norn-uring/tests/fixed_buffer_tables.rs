#![cfg(target_os = "linux")]

//! Fixed-buffer tables whose slots are installed, replaced and removed while
//! registered, whether they start sparse or with every slot installed.

use norn_uring::fixedbuf::{AcquireError, SlotErrorKind};
use norn_uring::fs;

mod util;

#[test]
fn sparse_fixed_buffer_slots_are_replaced_in_the_kernel() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-sparse");
        std::fs::write(&path, b"kernel-visible replacement")?;
        let mut opts = fs::OpenOptions::new();
        opts.write(true).read(true);
        let file = opts.open(&path).await?;
        let table = norn_uring::Handle::current().register_sparse_fixed_buffers(2)?;

        table.install(0, vec![0u8; 32])?;
        let buffer = table.try_acquire_at(0)?;
        let op = file.read_fixed_at(buffer, 0);
        // An unpolled operation already owns the slot.
        let err = table.replace(0, vec![0u8; 8]).unwrap_err();
        assert!(matches!(err.kind(), SlotErrorKind::InUse { index: 0 }));
        let (result, buffer) = op.await;
        assert_eq!(result?, 26);
        assert_eq!(&*buffer, b"kernel-visible replacement");
        drop(buffer);

        // Fixed I/O through the new pointer succeeds only if the kernel table
        // now describes the replacement region; the old one would be EFAULT.
        let previous = table.replace(0, vec![0u8; 8])?;
        assert_eq!(&previous[..26], b"kernel-visible replacement");
        let (result, buffer) = file.read_fixed_at(table.try_acquire_at(0)?, 0).await;
        assert_eq!(result?, 8);
        assert_eq!(&*buffer, b"kernel-v");
        drop(buffer);

        table.install(1, b"SPARSE".to_vec())?;
        let mut buffer = table.try_acquire_at(1)?;
        buffer.set_len(6)?;
        let (result, buffer) = file.write_fixed_at(buffer, 0).await;
        assert_eq!(result?, 6);
        drop(buffer);
        assert_eq!(&std::fs::read(&path)?[..8], b"SPARSE-v");

        assert_eq!(table.remove(0)?, b"kernel-v".to_vec());
        assert_eq!(
            table.try_acquire_at(0).unwrap_err(),
            AcquireError::Vacant { index: 0 }
        );
        assert_eq!(table.unregister()?, vec![None, Some(b"SPARSE".to_vec())]);
        Ok(())
    })
}

/// A table registered with every slot installed supports the same slot
/// changes as a sparse one, and the kernel sees each new region.
#[test]
fn dense_table_slots_can_be_replaced_and_removed() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-dense-replace");
        std::fs::write(&path, b"dense table replacement")?;
        let mut opts = fs::OpenOptions::new();
        opts.read(true);
        let file = opts.open(&path).await?;
        let table = norn_uring::Handle::current()
            .register_fixed_buffers(vec![vec![0u8; 32], vec![0u8; 16]])?;
        assert!(table.is_installed(0) && table.is_installed(1));

        let previous = table.replace(0, vec![0u8; 5])?;
        assert_eq!(previous.len(), 32);
        let (result, buffer) = file.read_fixed_at(table.try_acquire_at(0)?, 0).await;
        assert_eq!(result?, 5);
        assert_eq!(&*buffer, b"dense");
        drop(buffer);

        assert_eq!(table.remove(1)?.len(), 16);
        assert_eq!(
            table.try_acquire_at(1).unwrap_err(),
            AcquireError::Vacant { index: 1 }
        );
        table.install(1, vec![0u8; 4])?;
        let (result, buffer) = file.read_fixed_at(table.try_acquire_at(1)?, 6).await;
        assert_eq!(result?, 4);
        assert_eq!(&*buffer, b"tabl");
        drop(buffer);

        let slots = table.unregister()?;
        assert_eq!(slots.iter().filter(|slot| slot.is_some()).count(), 2);
        Ok(())
    })
}
