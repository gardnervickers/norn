//! What another thread reading the metrics costs the worker:
//! `noop_observed <reader_cpu> <interval_us> [tasks] [noops_per_task] [rounds]`.
//!
//! Runs the `noop_count` workload on the calling thread and prints that
//! thread's own user instructions and cycles per noop, counted with
//! `perf_event_open` so the reader thread is excluded. Meanwhile a reader
//! pinned to `reader_cpu` snapshots the executor and ring counters every
//! `interval_us` microseconds: 0 reads in a tight loop, a negative interval
//! runs no reader.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// The first 64 bytes of `struct perf_event_attr` (`PERF_ATTR_SIZE_VER0`).
#[repr(C)]
#[derive(Default)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
}

/// A user-mode hardware counter for the calling thread.
struct ThreadCounter(i32);

impl ThreadCounter {
    fn new(config: u64) -> Self {
        let attr = PerfEventAttr {
            type_: 0, // PERF_TYPE_HARDWARE
            size: std::mem::size_of::<PerfEventAttr>() as u32,
            config,
            // disabled | exclude_kernel | exclude_hv
            flags: 1 | (1 << 5) | (1 << 6),
            ..Default::default()
        };
        // Safety: `attr` is a valid, fully initialised version-0 attribute.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr as *const PerfEventAttr,
                0,
                -1,
                -1,
                0,
            )
        };
        assert!(
            fd >= 0,
            "perf_event_open: {}",
            std::io::Error::last_os_error()
        );
        Self(fd as i32)
    }

    fn enable(&self) {
        // Safety: PERF_EVENT_IOC_ENABLE on a perf event descriptor we own.
        unsafe { libc::ioctl(self.0, 0x2400, 0) };
    }

    fn disable(&self) {
        // Safety: PERF_EVENT_IOC_DISABLE on a perf event descriptor we own.
        unsafe { libc::ioctl(self.0, 0x2401, 0) };
    }

    fn read(&self) -> u64 {
        let mut value = 0u64;
        // Safety: reads one u64 count from a perf event descriptor we own.
        let n = unsafe { libc::read(self.0, (&mut value as *mut u64).cast(), 8) };
        assert_eq!(n, 8);
        value
    }
}

fn pin(cpu: usize) {
    // Safety: a zeroed cpu_set_t is a valid empty set.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let reader_cpu: usize = args.next().map_or(14, |a| a.parse().unwrap());
    let interval_us: i64 = args.next().map_or(-1, |a| a.parse().unwrap());
    let tasks: usize = args.next().map_or(64, |a| a.parse().unwrap());
    let per_task: usize = args.next().map_or(1563, |a| a.parse().unwrap());
    let rounds: usize = args.next().map_or(10, |a| a.parse().unwrap());
    let mut builder = io_uring::IoUring::builder();
    builder
        .dontfork()
        .setup_coop_taskrun()
        .setup_defer_taskrun()
        .setup_single_issuer()
        .setup_submit_all();
    let ring = norn_uring::Driver::new(builder, 32).unwrap();
    let ring_metrics = ring.metrics();
    let mut executor = norn_executor::LocalExecutor::new(ring);
    let executor_metrics = executor.metrics();

    let done = Arc::new(AtomicBool::new(false));
    let reader = (interval_us >= 0).then(|| {
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            pin(reader_cpu);
            let mut reads = 0u64;
            let mut checksum = 0u64;
            while !done.load(Ordering::Relaxed) {
                let e = executor_metrics.snapshot();
                let r = ring_metrics.snapshot();
                checksum = checksum.wrapping_add(e.polls ^ r.cqes);
                reads += 1;
                if interval_us > 0 {
                    std::thread::sleep(Duration::from_micros(interval_us as u64));
                }
            }
            (reads, checksum)
        })
    });

    let instructions = ThreadCounter::new(1); // PERF_COUNT_HW_INSTRUCTIONS
    let cycles = ThreadCounter::new(0); // PERF_COUNT_HW_CPU_CYCLES
    instructions.enable();
    cycles.enable();
    for _ in 0..rounds {
        executor.block_on(async {
            let mut handles = Vec::with_capacity(tasks);
            for _ in 0..tasks {
                handles.push(norn_executor::spawn(async move {
                    for _ in 0..per_task {
                        let _ = norn_uring::noop().await;
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
    cycles.disable();
    instructions.disable();
    done.store(true, Ordering::Relaxed);
    let reads = reader.map(|r| r.join().unwrap().0).unwrap_or(0);
    let ops = (tasks * per_task * rounds) as f64;
    println!(
        "interval_us={interval_us} reads={reads} inst/op={:.2} cyc/op={:.2}",
        instructions.read() as f64 / ops,
        cycles.read() as f64 / ops
    );
}
