//! Fixed-work probe for instruction counting: `yield_count <tasks> <yields_per_task> [rounds]`.
//!
//! Each task wakes itself and returns `Pending` once per yield, so every yield
//! is one wake (schedule) and one poll with no io_uring operation.
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

struct YieldNow(bool);

impl Future for YieldNow {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            return Poll::Ready(());
        }
        self.0 = true;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

fn main() {
    let mut args = std::env::args().skip(1);
    let tasks: usize = args.next().map_or(64, |a| a.parse().unwrap());
    let per_task: usize = args.next().map_or(15630, |a| a.parse().unwrap());
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
                        YieldNow(false).await;
                    }
                }));
            }
            for handle in handles {
                handle.await.unwrap();
            }
        });
    }
    let elapsed = start.elapsed();
    let yields = tasks * per_task * rounds;
    eprintln!(
        "yields={yields} ns/yield={:.1}",
        elapsed.as_nanos() as f64 / yields as f64
    );
}
