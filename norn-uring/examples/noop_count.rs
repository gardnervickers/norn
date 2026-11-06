//! Fixed-work probe for instruction counting: `noop_count <tasks> <noops_per_task> [rounds]`.
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
