#![cfg(target_os = "linux")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use norn_uring::buf::{FixedBuffer, StableBuf, StableBufMut};
use norn_uring::fs;

mod util;

struct CountingAllocator;

static COUNT_ALLOCATIONS: AtomicBool = AtomicBool::new(false);
static ALLOCATIONS: AtomicUsize = AtomicUsize::new(0);

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc_zeroed(layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if COUNT_ALLOCATIONS.load(Ordering::Relaxed) {
            ALLOCATIONS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

fn count_allocations<T>(f: impl FnOnce() -> T) -> (T, usize) {
    assert!(!COUNT_ALLOCATIONS.swap(true, Ordering::SeqCst));
    ALLOCATIONS.store(0, Ordering::SeqCst);
    let value = f();
    COUNT_ALLOCATIONS.store(false, Ordering::SeqCst);
    (value, ALLOCATIONS.load(Ordering::SeqCst))
}

#[repr(C, align(4096))]
struct AlignedBlock([u8; 4096]);

struct AlignedBuf(Box<AlignedBlock>);

impl AlignedBuf {
    fn zeroed() -> Self {
        Self(Box::new(AlignedBlock([0; 4096])))
    }
}

// Safety: the boxed allocation remains stable and fully initialized while an
// operation owns the wrapper.
unsafe impl StableBuf for AlignedBuf {
    fn stable_ptr(&self) -> *const u8 {
        self.0 .0.as_ptr()
    }

    fn bytes_init(&self) -> usize {
        self.0 .0.len()
    }
}

// Safety: the boxed allocation remains stable, initialized, and exclusively
// owned while an operation may write it.
unsafe impl StableBufMut for AlignedBuf {
    fn stable_ptr_mut(&mut self) -> *mut u8 {
        self.0 .0.as_mut_ptr()
    }

    fn bytes_remaining(&self) -> usize {
        self.0 .0.len()
    }

    unsafe fn set_init(&mut self, init_len: usize) {
        assert!(init_len <= self.0 .0.len());
    }
}

// Safety: registration owns the wrapper, and its box keeps the fully
// initialized writable region stable for the complete registration lifetime.
unsafe impl FixedBuffer for AlignedBuf {
    fn fixed_region(&mut self) -> &mut [u8] {
        &mut self.0 .0
    }
}

#[test]
fn fixed_request_construction_adds_no_allocation_beyond_raw_op(
) -> Result<(), Box<dyn std::error::Error>> {
    util::with_test_env(|| async {
        let dir = util::ThreadNameTestDir::new();
        let path = dir.join("fixed-allocation-count");
        let mut options = fs::OpenOptions::new();
        options.create(true).truncate(true).read(true).write(true);
        let file = options.open(path).await?;

        let buffer = AlignedBuf::zeroed();
        let (request, ordinary_read) = count_allocations(|| file.read_at(buffer, 0));
        drop(request);
        let buffer = AlignedBuf::zeroed();
        let (request, ordinary_write) = count_allocations(|| file.write_at(buffer, 0));
        drop(request);

        let pool =
            norn_uring::Handle::current().register_fixed_buffers(vec![AlignedBuf::zeroed()])?;
        let buffer = pool.try_acquire()?;
        let (request, fixed_read) = count_allocations(|| file.read_fixed_at(buffer, 0));
        drop(request);
        let buffer = pool.try_acquire()?;
        let (request, fixed_write) = count_allocations(|| file.write_fixed_at(buffer, 0));
        drop(request);

        assert_eq!(ordinary_read, 1, "ordinary read should allocate one RawOp");
        assert_eq!(
            ordinary_write, 1,
            "ordinary write should allocate one RawOp"
        );
        assert_eq!(fixed_read, 1, "fixed read should allocate only one RawOp");
        assert_eq!(fixed_write, 1, "fixed write should allocate only one RawOp");

        pool.unregister()?;
        file.close().await?;
        Ok(())
    })
}
