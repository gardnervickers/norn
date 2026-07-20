//! Zero-copy receive queue registration and buffers.

use std::cell::{Cell, UnsafeCell};
use std::fmt;
use std::io;
use std::ptr::NonNull;
use std::rc::Rc;
use std::sync::atomic::{AtomicU32, Ordering};

use io_uring::types;
use io_uring::Submitter;

use crate::operation::CQEResult;
use crate::Handle;

const DEFAULT_RQ_ENTRIES: u32 = 128;
const DEFAULT_AREA_LEN: usize = 128 * 4096;

/// Configuration for a zero-copy receive interface queue.
#[derive(Clone, Copy, Debug)]
pub struct ZcRxIfqConfig {
    /// Network interface index.
    pub if_index: u32,
    /// Hardware receive-queue index on the interface.
    pub rx_queue: u32,
    /// Number of entries in the refill queue.
    pub rq_entries: u32,
    /// Number of bytes in the registered receive area.
    pub area_len: usize,
}

impl Default for ZcRxIfqConfig {
    fn default() -> Self {
        Self {
            if_index: 0,
            rx_queue: 0,
            rq_entries: DEFAULT_RQ_ENTRIES,
            area_len: DEFAULT_AREA_LEN,
        }
    }
}

/// A handle to a driver-owned zero-copy receive interface queue.
#[derive(Clone)]
pub struct ZcRxIfq {
    inner: Rc<Inner>,
    handle: Handle,
}

/// A buffer received from a zero-copy receive interface queue.
///
/// Dropping the buffer returns its region to the kernel refill queue.
pub struct ZcRecvBuf {
    inner: Rc<Inner>,
    offset: usize,
    len: usize,
    refill_offset: u64,
}

pub(crate) struct Registration {
    inner: Rc<Inner>,
}

struct Inner {
    area: AnonymousMmap,
    _region: AnonymousMmap,
    head: NonNull<AtomicU32>,
    tail: NonNull<AtomicU32>,
    rqes: NonNull<types::io_uring_zcrx_rqe>,
    rq_entries: u32,
    area_len: usize,
    area_token: u64,
    zcrx_id: u32,
    local_tail: Cell<u32>,
    inflight: Cell<bool>,
}

struct AnonymousMmap {
    ptr: NonNull<u8>,
    len: usize,
}

impl ZcRxIfq {
    pub(crate) fn id(&self) -> u32 {
        self.inner.zcrx_id
    }

    pub(crate) fn acquire(&self, handle: &Handle) -> io::Result<()> {
        if !self.handle.same_driver(handle) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "zero-copy receive IFQ and socket must target the same CQE32 driver",
            ));
        }
        self.inner.acquire()
    }

    pub(crate) fn release(&self) {
        self.inner.inflight.set(false);
    }

    pub(crate) fn parse_completion(
        &self,
        submitted_len: u32,
        result: CQEResult,
    ) -> io::Result<ZcRecvBuf> {
        let extra = result.big_cqe().copied();
        let len = result.result?;
        let extra = extra.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion is missing its CQE32 payload",
            )
        })?;
        self.inner.recv_buf(submitted_len, len, extra[0])
    }
}

impl fmt::Debug for ZcRxIfq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZcRxIfq")
            .field("zcrx_id", &self.inner.zcrx_id)
            .field("rq_entries", &self.inner.rq_entries)
            .field("area_len", &self.inner.area_len)
            .finish()
    }
}

impl ZcRecvBuf {
    /// Return the number of initialized bytes in this buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Return whether the buffer contains no bytes.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Return the received bytes as a slice.
    pub fn as_slice(&self) -> &[u8] {
        // Safety: registration validates the range before constructing the
        // buffer. The mapping remains alive through `inner`, and the kernel
        // does not reuse this region until this buffer is dropped.
        unsafe {
            std::slice::from_raw_parts(self.inner.area.ptr.as_ptr().add(self.offset), self.len)
        }
    }
}

impl fmt::Debug for ZcRecvBuf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZcRecvBuf")
            .field("zcrx_id", &self.inner.zcrx_id)
            .field("offset", &self.offset)
            .field("len", &self.len)
            .finish()
    }
}

impl Drop for ZcRecvBuf {
    fn drop(&mut self) {
        self.inner
            .return_buffer(self.refill_offset, self.len as u32);
    }
}

impl Registration {
    pub(crate) fn new(submitter: &Submitter<'_>, config: ZcRxIfqConfig) -> io::Result<Self> {
        if config.rq_entries == 0 || !config.rq_entries.is_power_of_two() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero-copy receive rq_entries must be a non-zero power of two",
            ));
        }
        if config.area_len == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "zero-copy receive area_len must be non-zero",
            ));
        }

        let page_size = page_size()?;
        let area_len = align_up(config.area_len, page_size)?;
        let rqe_bytes = usize::try_from(config.rq_entries)
            .ok()
            .and_then(|entries| {
                entries.checked_mul(std::mem::size_of::<types::io_uring_zcrx_rqe>())
            })
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "refill queue is too large")
            })?;
        let region_len = page_size
            .checked_add(rqe_bytes)
            .and_then(|len| align_up(len, page_size).ok())
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "refill region is too large")
            })?;

        let area = AnonymousMmap::new(area_len)?;
        let region = AnonymousMmap::new(region_len)?;

        let area_reg = UnsafeCell::new(types::io_uring_zcrx_area_reg {
            addr: area.ptr.as_ptr() as u64,
            len: area_len as u64,
            ..Default::default()
        });
        let region_desc = UnsafeCell::new(types::io_uring_region_desc {
            user_addr: region.ptr.as_ptr() as u64,
            size: region_len as u64,
            flags: types::IORING_MEM_REGION_TYPE_USER,
            ..Default::default()
        });
        let ifq_reg = UnsafeCell::new(types::io_uring_zcrx_ifq_reg {
            if_idx: config.if_index,
            if_rxq: config.rx_queue,
            rq_entries: config.rq_entries,
            area_ptr: area_reg.get() as u64,
            region_ptr: region_desc.get() as u64,
            ..Default::default()
        });

        // Safety: all three registration structures and both mappings remain
        // allocated and fixed for the synchronous registration call. The
        // kernel may update the registration outputs through these pointers.
        submitter.register_ifq(unsafe { &*ifq_reg.get() })?;

        // Safety: the synchronous registration call has returned and no other
        // code can access these stack-local `UnsafeCell` values.
        let registered = unsafe { *ifq_reg.get() };
        let registered_area = unsafe { *area_reg.get() };
        let pointers = (|| {
            let head = pointer_at::<AtomicU32>(&region, registered.offsets.head as usize, 1)?;
            let tail = pointer_at::<AtomicU32>(&region, registered.offsets.tail as usize, 1)?;
            let rqes = pointer_at::<types::io_uring_zcrx_rqe>(
                &region,
                registered.offsets.rqes as usize,
                config.rq_entries as usize,
            )?;
            Ok::<_, io::Error>((head, tail, rqes))
        })();
        let (head, tail, rqes) = match pointers {
            Ok(pointers) => pointers,
            Err(err) => {
                // Registration succeeded, so the kernel owns references to
                // these mappings until ring destruction. Malformed offsets
                // are a kernel contract violation; quarantine the mappings
                // instead of releasing memory the kernel may still access.
                std::mem::forget(area);
                std::mem::forget(region);
                return Err(err);
            }
        };
        // Safety: registration initialized the shared tail location.
        let local_tail = unsafe { tail.as_ref().load(Ordering::Acquire) };

        Ok(Self {
            inner: Rc::new(Inner {
                area,
                _region: region,
                head,
                tail,
                rqes,
                rq_entries: config.rq_entries,
                area_len,
                area_token: registered_area.rq_area_token,
                zcrx_id: registered.zcrx_id,
                local_tail: Cell::new(local_tail),
                inflight: Cell::new(false),
            }),
        })
    }

    pub(crate) fn handle(&self, handle: Handle) -> ZcRxIfq {
        ZcRxIfq {
            inner: Rc::clone(&self.inner),
            handle,
        }
    }
}

impl Inner {
    fn acquire(&self) -> io::Result<()> {
        if self.inflight.replace(true) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "zero-copy receive IFQ already has an active consumer stream",
            ));
        }
        Ok(())
    }

    fn recv_buf(
        self: &Rc<Self>,
        submitted_len: u32,
        len: u32,
        cqe_offset: u64,
    ) -> io::Result<ZcRecvBuf> {
        if len > submitted_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion exceeds the submitted length",
            ));
        }

        let area_mask = area_mask();
        if cqe_offset & area_mask != self.area_token & area_mask {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion references a different registered area",
            ));
        }

        let offset = usize::try_from(cqe_offset & !area_mask).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion offset does not fit usize",
            )
        })?;
        let len = len as usize;
        let end = offset.checked_add(len).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion range overflowed",
            )
        })?;
        if end > self.area_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "zero-copy receive completion is outside the registered area",
            ));
        }

        Ok(ZcRecvBuf {
            inner: Rc::clone(self),
            offset,
            len,
            refill_offset: cqe_offset,
        })
    }

    fn return_buffer(&self, cqe_offset: u64, len: u32) {
        // The ZCRX RQ is a userspace-producer/kernel-consumer refill queue.
        // Publish the descriptor before the release-store to the shared tail.
        let tail = self.local_tail.get();
        // Safety: registration validated the shared head pointer.
        let head = unsafe { self.head.as_ref().load(Ordering::Acquire) };
        if tail.wrapping_sub(head) >= self.rq_entries {
            log::error!("zero-copy receive refill queue is full; buffer cannot be returned");
            return;
        }

        let index = tail & (self.rq_entries - 1);
        let rqe = types::io_uring_zcrx_rqe {
            off: (cqe_offset & !area_mask()) | (self.area_token & area_mask()),
            len,
            ..Default::default()
        };
        // Safety: `rqes` points to `rq_entries` registered entries and `index`
        // is masked into that range. This single-threaded driver is the sole
        // userspace producer.
        unsafe { self.rqes.as_ptr().add(index as usize).write(rqe) };
        let next = tail.wrapping_add(1);
        self.local_tail.set(next);
        // Safety: registration validated the shared tail pointer.
        unsafe { self.tail.as_ref().store(next, Ordering::Release) };
    }
}

impl AnonymousMmap {
    fn new(len: usize) -> io::Result<Self> {
        // Safety: the arguments request a private anonymous mapping with no fd.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
                -1,
                0,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).expect("mmap returned a null success pointer"),
            len,
        })
    }
}

impl Drop for AnonymousMmap {
    fn drop(&mut self) {
        // Safety: this is the exact mapping returned by `mmap` and it is
        // released only after the ring no longer owns the registration.
        let result = unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
        if result != 0 {
            log::error!(
                "failed to unmap zero-copy receive memory: {:?}",
                io::Error::last_os_error()
            );
        }
    }
}

fn page_size() -> io::Result<usize> {
    // Safety: `_SC_PAGESIZE` has no pointer arguments or side effects.
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page_size)
        .ok()
        .filter(|size| size.is_power_of_two())
        .ok_or_else(|| io::Error::other("failed to determine a power-of-two page size"))
}

fn align_up(value: usize, alignment: usize) -> io::Result<usize> {
    value
        .checked_add(alignment - 1)
        .map(|value| value & !(alignment - 1))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "mapping length overflowed"))
}

fn pointer_at<T>(mapping: &AnonymousMmap, offset: usize, count: usize) -> io::Result<NonNull<T>> {
    let bytes = count.checked_mul(std::mem::size_of::<T>()).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel returned an oversized IFQ range",
        )
    })?;
    let end = offset.checked_add(bytes).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel returned an overflowing IFQ offset",
        )
    })?;
    let align = std::mem::align_of::<T>();
    if offset & (align - 1) != 0 || end > mapping.len {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "kernel returned an invalid IFQ mapping offset",
        ));
    }
    // Safety: the range and alignment were checked against the live mapping.
    Ok(unsafe { NonNull::new_unchecked(mapping.ptr.as_ptr().add(offset).cast()) })
}

fn area_mask() -> u64 {
    !((1u64 << types::IORING_ZCRX_AREA_SHIFT) - 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_inner(area_len: usize, rq_entries: u32) -> Rc<Inner> {
        let page_size = page_size().unwrap();
        let area = AnonymousMmap::new(align_up(area_len, page_size).unwrap()).unwrap();
        let region = AnonymousMmap::new(page_size).unwrap();
        let head = pointer_at::<AtomicU32>(&region, 0, 1).unwrap();
        let tail = pointer_at::<AtomicU32>(&region, 4, 1).unwrap();
        let rqes = pointer_at::<types::io_uring_zcrx_rqe>(&region, 8, rq_entries as usize).unwrap();
        Rc::new(Inner {
            area_len: area.len,
            area,
            _region: region,
            head,
            tail,
            rqes,
            rq_entries,
            area_token: 3u64 << types::IORING_ZCRX_AREA_SHIFT,
            zcrx_id: 7,
            local_tail: Cell::new(0),
            inflight: Cell::new(false),
        })
    }

    #[test]
    fn buffer_drop_publishes_one_refill_entry() {
        let inner = test_inner(4096, 8);
        let cqe_offset = inner.area_token | 32;
        let buf = inner.recv_buf(128, 64, cqe_offset).unwrap();
        drop(buf);

        assert_eq!(inner.local_tail.get(), 1);
        // Safety: the test mapping owns these validated pointers.
        assert_eq!(unsafe { inner.tail.as_ref().load(Ordering::Acquire) }, 1);
        let rqe = unsafe { inner.rqes.as_ptr().read() };
        assert_eq!(rqe.off, cqe_offset);
        assert_eq!(rqe.len, 64);
    }

    #[test]
    fn inflight_guard_rejects_a_second_stream() {
        let inner = test_inner(4096, 8);
        inner.acquire().unwrap();
        assert_eq!(
            inner.acquire().unwrap_err().kind(),
            io::ErrorKind::WouldBlock
        );
        inner.inflight.set(false);
        inner.acquire().unwrap();
    }

    #[test]
    fn completion_parser_rejects_malformed_metadata() {
        let driver = crate::Driver::new(io_uring::IoUring::builder(), 8).unwrap();
        let inner = test_inner(4096, 8);
        let ifq = ZcRxIfq {
            inner: Rc::clone(&inner),
            handle: driver.handle(),
        };
        let more = (0..=u32::MAX)
            .find(|flags| io_uring::cqueue::more(*flags))
            .unwrap();

        let missing = ifq
            .parse_completion(64, CQEResult::new(Ok(32), more))
            .unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);

        let operation_error = ifq
            .parse_completion(
                64,
                CQEResult::new(Err(io::Error::from_raw_os_error(libc::ECANCELED)), more),
            )
            .unwrap_err();
        assert_eq!(operation_error.raw_os_error(), Some(libc::ECANCELED));

        let wrong_area =
            CQEResult::new_big(Ok(32), more, [4u64 << types::IORING_ZCRX_AREA_SHIFT, 0]);
        assert_eq!(
            ifq.parse_completion(64, wrong_area).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let out_of_bounds = CQEResult::new_big(
            Ok(32),
            more,
            [inner.area_token | (inner.area_len as u64 - 16), 0],
        );
        assert_eq!(
            ifq.parse_completion(64, out_of_bounds).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );

        let overlength = CQEResult::new_big(Ok(65), more, [inner.area_token, 0]);
        assert_eq!(
            ifq.parse_completion(64, overlength).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }
}
