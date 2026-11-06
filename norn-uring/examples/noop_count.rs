//! A fixed-work probe for counting instructions on the op path:
//! `noop_count [tasks] [noops_per_task] [rounds]`.
//!
//! Each task awaits noops one after another, so every noop is one submission,
//! one completion, one wake and one poll. Run it under
//! `perf stat -e instructions:u,cycles:u taskset -c <cpu>` and divide by the
//! printed op count.
#[cfg(target_os = "linux")]
fn main() {
    let mut args = std::env::args().skip(1);
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
    let mut executor = norn_executor::LocalExecutor::new(ring);
    let start = std::time::Instant::now();
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
    let elapsed = start.elapsed();
    let ops = tasks * per_task * rounds;
    eprintln!(
        "ops={ops} ns/op={:.1}",
        elapsed.as_nanos() as f64 / ops as f64
    );
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("noop_count needs io_uring, which is Linux-only");
}
