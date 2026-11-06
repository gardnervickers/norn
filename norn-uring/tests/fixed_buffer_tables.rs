#![cfg(target_os = "linux")]

//! Fixed-buffer tables whose slots are installed, replaced and removed while
//! registered, whether they start sparse or with every slot installed.

use std::cell::RefCell;
use std::rc::Rc;

use norn_uring::fixedbuf::{AcquireError, FixedBufTable, FixedBuffer, SlotErrorKind};
use norn_uring::fs;
use norn_uring::BufResult;

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
        let BufResult(result, buffer) = op.await;
        assert_eq!(result?, 26);
        assert_eq!(&*buffer, b"kernel-visible replacement");
        drop(buffer);

        // Fixed I/O through the new pointer succeeds only if the kernel table
        // now describes the replacement region; the old one would be EFAULT.
        let previous = table.replace(0, vec![0u8; 8])?;
        assert_eq!(&previous[..26], b"kernel-visible replacement");
        let BufResult(result, buffer) = file.read_fixed_at(table.try_acquire_at(0)?, 0).await;
        assert_eq!(result?, 8);
        assert_eq!(&*buffer, b"kernel-v");
        drop(buffer);

        table.install(1, b"SPARSE".to_vec())?;
        let mut buffer = table.try_acquire_at(1)?;
        buffer.set_len(6)?;
        let BufResult(result, buffer) = file.write_fixed_at(buffer, 0).await;
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
        let BufResult(result, buffer) = file.read_fixed_at(table.try_acquire_at(0)?, 0).await;
        assert_eq!(result?, 5);
        assert_eq!(&*buffer, b"dense");
        drop(buffer);

        assert_eq!(table.remove(1)?.len(), 16);
        assert_eq!(
            table.try_acquire_at(1).unwrap_err(),
            AcquireError::Vacant { index: 1 }
        );
        table.install(1, vec![0u8; 4])?;
        let BufResult(result, buffer) = file.read_fixed_at(table.try_acquire_at(1)?, 6).await;
        assert_eq!(result?, 4);
        assert_eq!(&*buffer, b"tabl");
        drop(buffer);

        let slots = table.unregister()?;
        assert_eq!(slots.iter().filter(|slot| slot.is_some()).count(), 2);
        Ok(())
    })
}

type DynBuffer = Box<dyn FixedBuffer>;

/// A buffer whose `fixed_region` first runs a hook, so a test can change the
/// table from inside the projection of a value being installed or replaced.
struct Reentrant {
    region: Region,
    hook: Option<Box<dyn FnOnce()>>,
}

enum Region {
    Heap(Vec<u8>),
    /// An anonymous `MAP_NORESERVE` mapping just over the kernel's 1 GiB
    /// per-buffer limit: the kernel rejects it (EFAULT) without pinning pages.
    Oversized {
        ptr: *mut u8,
        len: usize,
    },
}

impl Reentrant {
    fn heap(len: usize, hook: impl FnOnce() + 'static) -> Self {
        Self {
            region: Region::Heap(vec![0u8; len]),
            hook: Some(Box::new(hook)),
        }
    }

    fn oversized(hook: impl FnOnce() + 'static) -> Self {
        let len = (1usize << 30) + 4096;
        // Safety: an anonymous private mapping with no address hint.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | libc::MAP_NORESERVE,
                -1,
                0,
            )
        };
        assert_ne!(ptr, libc::MAP_FAILED);
        Self {
            region: Region::Oversized {
                ptr: ptr.cast(),
                len,
            },
            hook: Some(Box::new(hook)),
        }
    }
}

impl Drop for Reentrant {
    fn drop(&mut self) {
        if let Region::Oversized { ptr, len } = self.region {
            // Safety: the mapping was created in `oversized` and is unmapped once.
            unsafe { libc::munmap(ptr.cast(), len) };
        }
    }
}

// Safety: the heap region or mapping is owned by this value, stays at a fixed
// address until drop, is zero-initialized and writable, and is exposed only
// here.
unsafe impl FixedBuffer for Reentrant {
    fn fixed_region(&mut self) -> &mut [u8] {
        if let Some(hook) = self.hook.take() {
            hook();
        }
        match &mut self.region {
            Region::Heap(vec) => vec,
            // Safety: see the impl's safety comment.
            Region::Oversized { ptr, len } => unsafe { std::slice::from_raw_parts_mut(*ptr, *len) },
        }
    }
}

/// Record the result of a slot change made from inside `fixed_region`.
fn record<T>(slot: &Rc<RefCell<Option<Result<(), String>>>>, result: Result<T, String>) {
    *slot.borrow_mut() = Some(result.map(drop));
}

/// An install into the slot an install is projecting is rejected, so a failed
/// outer kernel update cannot leave the slot describing a freed value.
#[test]
fn reentrant_install_into_a_projecting_slot_is_rejected() -> Result<(), Box<dyn std::error::Error>>
{
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("reentrant-install");
        std::fs::write(&path, [0xEEu8; 64])?;
        let file = fs::OpenOptions::new().read(true).open(&path).await?;
        let table: Rc<FixedBufTable<DynBuffer>> =
            Rc::new(norn_uring::Handle::current().register_sparse_fixed_buffers(1)?);

        let inner = Rc::new(RefCell::new(None));
        let hook = {
            let (table, inner) = (Rc::clone(&table), Rc::clone(&inner));
            move || {
                let result = table.install(0, Box::new(vec![0xAAu8; 64]) as DynBuffer);
                record(&inner, result.map_err(|err| err.to_string()));
            }
        };
        let err = table
            .install(0, Box::new(Reentrant::oversized(hook)))
            .expect_err("a region over 1 GiB is rejected by the kernel");
        drop(err);
        assert!(
            inner.borrow_mut().take().expect("hook ran").is_err(),
            "the reentrant install must be rejected"
        );
        assert!(!table.is_installed(0));
        assert_eq!(
            table.try_acquire_at(0).unwrap_err(),
            AcquireError::Vacant { index: 0 }
        );

        // The slot is still usable, and the kernel describes the new value.
        table.install(0, Box::new(vec![0u8; 64]) as DynBuffer)?;
        let BufResult(result, buffer) = file.read_fixed_at(table.try_acquire_at(0)?, 0).await;
        assert_eq!(result?, 64);
        assert_eq!(&*buffer, &[0xEEu8; 64][..]);
        drop(buffer);

        // A successful outer install puts its slot on the free list once.
        assert!(table.remove(0).is_ok());
        let hook = {
            let (table, inner) = (Rc::clone(&table), Rc::clone(&inner));
            move || {
                let result = table.install(0, Box::new(vec![0xAAu8; 64]) as DynBuffer);
                record(&inner, result.map_err(|err| err.to_string()));
            }
        };
        table.install(0, Box::new(Reentrant::heap(64, hook)))?;
        assert!(inner.borrow_mut().take().expect("hook ran").is_err());
        let first = table.try_acquire()?;
        assert_eq!(first.index(), 0);
        assert_eq!(table.try_acquire().unwrap_err(), AcquireError::Exhausted);
        drop(first);
        Ok(())
    })
}

/// A remove of the slot a replacement is projecting is rejected, so the
/// replacement's kernel update and free-list bookkeeping stay consistent.
#[test]
fn reentrant_remove_of_a_replacing_slot_is_rejected() -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("reentrant-replace");
        std::fs::write(&path, b"replacement region")?;
        let file = fs::OpenOptions::new().read(true).open(&path).await?;
        let table: Rc<FixedBufTable<DynBuffer>> = Rc::new(
            norn_uring::Handle::current()
                .register_fixed_buffers(vec![Box::new(vec![0u8; 32]) as DynBuffer])?,
        );

        let inner = Rc::new(RefCell::new(None));
        let hook = {
            let (table, inner) = (Rc::clone(&table), Rc::clone(&inner));
            move || {
                let result = table.remove(0);
                record(&inner, result.map_err(|err| err.to_string()));
            }
        };
        let previous = table
            .replace(0, Box::new(Reentrant::heap(11, hook)))
            .map_err(|err| err.to_string())?;
        drop(previous);
        assert!(
            inner.borrow_mut().take().expect("hook ran").is_err(),
            "the reentrant remove must be rejected"
        );
        assert!(table.is_installed(0));

        let BufResult(result, buffer) = file.read_fixed_at(table.try_acquire()?, 0).await;
        assert_eq!(result?, 11);
        assert_eq!(&*buffer, b"replacement");
        assert_eq!(table.try_acquire().unwrap_err(), AcquireError::Exhausted);
        drop(buffer);
        Ok(())
    })
}
