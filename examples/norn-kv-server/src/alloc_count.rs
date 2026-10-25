//! Thread-local allocation accounting for allocation regression tests.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    static ENABLED: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
}

struct CountingAllocator;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

fn record(bytes: usize) {
    // `try_with` keeps allocation during thread-local teardown safe.
    if ENABLED.try_with(Cell::get).unwrap_or(false) {
        ALLOCS.with(|count| count.set(count.get() + 1));
        BYTES.with(|count| count.set(count.get() + bytes));
    }
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc(layout)
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        System.alloc_zeroed(layout)
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        record(size);
        System.realloc(ptr, layout, size)
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        System.dealloc(ptr, layout)
    }
}

/// Return the allocation count and requested bytes made by this thread in `f`.
pub(crate) fn count(f: impl FnOnce()) -> (usize, usize) {
    ALLOCS.with(|count| count.set(0));
    BYTES.with(|count| count.set(0));
    ENABLED.with(|enabled| assert!(!enabled.replace(true)));
    f();
    ENABLED.with(|enabled| enabled.set(false));
    (ALLOCS.with(Cell::get), BYTES.with(Cell::get))
}
