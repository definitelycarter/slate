use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static TOTAL_ALLOCS: AtomicUsize = AtomicUsize::new(0);

pub struct TrackingAllocator;

unsafe impl GlobalAlloc for TrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            let size = layout.size();
            let current = ALLOCATED.fetch_add(size, Relaxed) + size;
            PEAK.fetch_max(current, Relaxed);
            TOTAL_ALLOCS.fetch_add(1, Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        ALLOCATED.fetch_sub(layout.size(), Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }
}

pub struct AllocStats {
    pub peak_bytes: usize,
    pub total_allocs: usize,
}

pub fn reset() {
    ALLOCATED.store(0, Relaxed);
    PEAK.store(0, Relaxed);
    TOTAL_ALLOCS.store(0, Relaxed);
}

pub fn snapshot() -> AllocStats {
    AllocStats {
        peak_bytes: PEAK.load(Relaxed),
        total_allocs: TOTAL_ALLOCS.load(Relaxed),
    }
}
