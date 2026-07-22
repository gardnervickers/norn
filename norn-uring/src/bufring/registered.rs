use std::cell::{Cell, UnsafeCell};
use std::io;
use std::mem::ManuallyDrop;
use std::ptr;
use std::sync::atomic::{AtomicU16, Ordering};

use io_uring::types::{self, BufRingEntry};
use io_uring::Submitter;
use log::warn;

use crate::Handle;

use super::{Bgid, Bid};

#[derive(Debug, Clone, Copy)]
pub(super) struct RegisteredEntry {
    pub(super) position: u64,
    pub(super) bid: Bid,
    pub(super) len: usize,
}

/// Physical storage and kernel registration shared by typed send/receive rings.
pub(super) struct RegisteredBufRing {
    handle: Handle,
    registered: Cell<bool>,
    bgid: Bgid,
    ring_entries_mask: u16,
    buf_cnt: u16,
    buf_len: usize,
    ring_start: ManuallyDrop<AnonymousMmap>,
    buffers: ManuallyDrop<Vec<UnsafeCell<Box<[u8]>>>>,
    shared_tail: *const AtomicU16,
}

impl RegisteredBufRing {
    pub(super) fn new(
        bgid: Bgid,
        ring_entries: u16,
        buf_cnt: u16,
        buf_len: usize,
        handle: Handle,
    ) -> io::Result<Self> {
        if buf_cnt == 0
            || buf_cnt > ring_entries
            || buf_len == 0
            || (ring_entries & (ring_entries - 1)) != 0
        {
            return Err(io::Error::from(io::ErrorKind::InvalidInput));
        }

        let entry_size = std::mem::size_of::<BufRingEntry>();
        assert_eq!(entry_size, 16);
        let ring_start = AnonymousMmap::new(entry_size * usize::from(ring_entries))?;
        let shared_tail =
            unsafe { types::BufRingEntry::tail(ring_start.as_ptr() as *const BufRingEntry) }
                as *const AtomicU16;
        let buffers = (0..buf_cnt)
            .map(|_| UnsafeCell::new(vec![0; buf_len].into_boxed_slice()))
            .collect();

        Ok(Self {
            handle,
            registered: Cell::new(false),
            bgid,
            ring_entries_mask: ring_entries - 1,
            buf_cnt,
            buf_len,
            ring_start: ManuallyDrop::new(ring_start),
            buffers: ManuallyDrop::new(buffers),
            shared_tail,
        })
    }

    pub(super) fn register(&self, submitter: &Submitter<'_>) -> io::Result<()> {
        if self.registered.get() {
            return Ok(());
        }
        let result = unsafe {
            submitter.register_buf_ring_with_flags(
                self.ring_start.as_ptr() as _,
                self.ring_entries(),
                self.bgid,
                0,
            )
        };
        if let Err(err) = result {
            let message = match err.raw_os_error() {
                Some(libc::EINVAL) => format!(
                    "buf_ring.register returned {err}, most likely indicating this kernel is not 5.19+"
                ),
                Some(libc::EEXIST) => format!(
                    "buf_ring.register returned `{err}`, indicating buffer group id {} is already registered",
                    self.bgid
                ),
                _ => format!(
                    "buf_ring.register returned `{err}` for group id {}",
                    self.bgid
                ),
            };
            return Err(io::Error::other(message));
        }
        self.registered.set(true);
        Ok(())
    }

    /// Remove every kernel-visible entry and register the empty ring again.
    pub(super) fn reset_registration(&self) -> io::Result<()> {
        if self.registered.get() {
            self.handle
                .with_submitter(|submitter| submitter.unregister_buf_ring(self.bgid))?;
            self.registered.set(false);
        }
        unsafe {
            (*self.shared_tail).store(0, Ordering::Release);
        }
        self.handle
            .with_submitter(|submitter| self.register(submitter))
    }

    pub(super) fn publish(&self, entries_to_publish: &[RegisteredEntry]) {
        let Some(last) = entries_to_publish.last() else {
            return;
        };
        let entries = self.ring_start.as_ptr_mut() as *mut BufRingEntry;
        for action in entries_to_publish {
            assert!(action.bid < self.buf_cnt);
            assert!(action.len <= self.buf_len);
            let ring_idx = action.position as u16 & self.ring_entries_mask;
            let entry = unsafe { &mut *entries.add(usize::from(ring_idx)) };
            entry.set_addr(self.stable_ptr(action.bid) as _);
            entry.set_len(action.len as _);
            entry.set_bid(action.bid);
        }
        unsafe {
            (*self.shared_tail).store(last.position.wrapping_add(1) as u16, Ordering::Release);
        }
    }

    pub(super) fn same_driver(&self, handle: &Handle) -> bool {
        self.handle.same_driver(handle)
    }

    pub(super) fn bgid(&self) -> Bgid {
        self.bgid
    }

    pub(super) fn buf_count(&self) -> u16 {
        self.buf_cnt
    }

    pub(super) fn buf_capacity(&self) -> usize {
        self.buf_len
    }

    pub(super) fn ring_entries(&self) -> u16 {
        self.ring_entries_mask + 1
    }

    pub(super) fn stable_ptr(&self, bid: Bid) -> *const u8 {
        assert!(bid < self.buf_cnt);
        unsafe { (&*self.buffers[usize::from(bid)].get()).as_ptr() }
    }

    pub(super) fn stable_mut_ptr(&self, bid: Bid) -> *mut u8 {
        assert!(bid < self.buf_cnt);
        unsafe { (&mut *self.buffers[usize::from(bid)].get()).as_mut_ptr() }
    }
}

impl Drop for RegisteredBufRing {
    fn drop(&mut self) {
        if self.registered.get() {
            if let Err(err) = self
                .handle
                .with_submitter(|submitter| submitter.unregister_buf_ring(self.bgid))
            {
                // The kernel may still dereference the registered ring and its
                // buffers. Intentionally retain both mappings rather than
                // freeing memory that may remain kernel-visible.
                warn!(target: "norn_uring::bufring", "unregister.failed_leaking_storage: {err}");
                return;
            }
            self.registered.set(false);
        }
        unsafe {
            ManuallyDrop::drop(&mut self.buffers);
            ManuallyDrop::drop(&mut self.ring_start);
        }
    }
}

/// An anonymous page-aligned, zero-filled memory mapping.
#[derive(Debug)]
struct AnonymousMmap {
    addr: ptr::NonNull<libc::c_void>,
    len: usize,
}

impl AnonymousMmap {
    fn new(len: usize) -> io::Result<Self> {
        Self::new_with_madvise(len, |addr, len| {
            match unsafe { libc::madvise(addr.as_ptr(), len, libc::MADV_DONTFORK) } {
                0 => Ok(()),
                _ => Err(io::Error::last_os_error()),
            }
        })
    }

    fn new_with_madvise(
        len: usize,
        madvise: impl FnOnce(ptr::NonNull<libc::c_void>, usize) -> io::Result<()>,
    ) -> io::Result<Self> {
        let addr = unsafe {
            match libc::mmap(
                ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE | libc::MAP_POPULATE,
                0,
                0,
            ) {
                libc::MAP_FAILED => return Err(io::Error::last_os_error()),
                addr => ptr::NonNull::new_unchecked(addr),
            }
        };
        let mmap = Self { addr, len };
        madvise(mmap.addr, mmap.len)?;
        Ok(mmap)
    }

    fn as_ptr(&self) -> *const libc::c_void {
        self.addr.as_ptr()
    }

    fn as_ptr_mut(&self) -> *mut libc::c_void {
        self.addr.as_ptr()
    }
}

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        unsafe {
            libc::munmap(self.addr.as_ptr(), self.len);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[test]
    fn anonymous_mmap_is_unmapped_when_madvise_fails() {
        let len = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let mapped_addr = Cell::new(ptr::null_mut());
        let result = AnonymousMmap::new_with_madvise(len, |addr, len| {
            mapped_addr.set(addr.as_ptr());
            let mut residency = 0;
            assert_eq!(
                unsafe { libc::mincore(addr.as_ptr(), len, &mut residency) },
                0
            );
            Err(io::Error::from_raw_os_error(libc::EINVAL))
        });
        assert_eq!(result.unwrap_err().raw_os_error(), Some(libc::EINVAL));

        let mut residency = 0;
        assert_eq!(
            unsafe { libc::mincore(mapped_addr.get(), len, &mut residency) },
            -1
        );
        assert_eq!(
            io::Error::last_os_error().raw_os_error(),
            Some(libc::ENOMEM)
        );
    }
}
