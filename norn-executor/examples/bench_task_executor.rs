use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use norn_executor::park::SpinPark;
use norn_executor::{spawn, LocalExecutor};
use norn_task::TaskQueue;

const ITERS: usize = 20_000;
const SAMPLES: usize = 7;

fn main() {
    println!("norn task/executor microbench (iterations={ITERS}, samples={SAMPLES})");
    bench_task_queue_spawn();
    bench_task_queue_join();
    bench_executor_spawn();
}

fn bench_task_queue_spawn() {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let tq = TaskQueue::new();
        let start = Instant::now();
        for _ in 0..ITERS {
            for _ in 0..128 {
                tq.spawn(async {
                    yield_now().await;
                })
                .detach();
            }
            while let Some(task) = tq.next() {
                task.run();
            }
        }
        let elapsed = start.elapsed();
        samples.push(elapsed.as_secs_f64() * 1e9 / ITERS as f64);
    }
    println!(
        "task_queue_spawn/128: median {:.1} ns/iter",
        median(&mut samples)
    );
}

fn bench_task_queue_join() {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let tq = TaskQueue::new();
        let start = Instant::now();
        for _ in 0..ITERS {
            let t1 = tq.spawn(async {
                yield_now().await;
                7usize
            });
            tq.spawn(async move {
                black_box(t1.await.unwrap());
            })
            .detach();
            while let Some(task) = tq.next() {
                task.run();
            }
        }
        let elapsed = start.elapsed();
        samples.push(elapsed.as_secs_f64() * 1e9 / ITERS as f64);
    }
    println!(
        "task_queue_join: median {:.1} ns/iter",
        median(&mut samples)
    );
}

fn bench_executor_spawn() {
    let mut samples = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let mut ex = LocalExecutor::new(SpinPark);
        let start = Instant::now();
        for _ in 0..ITERS {
            ex.block_on(async {
                let mut handles = Vec::with_capacity(64);
                for _ in 0..64 {
                    handles.push(spawn(async {
                        yield_now().await;
                    }));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            });
        }
        let elapsed = start.elapsed();
        samples.push(elapsed.as_secs_f64() * 1e9 / ITERS as f64);
    }
    println!(
        "executor_spawn/64: median {:.1} ns/iter",
        median(&mut samples)
    );
}

async fn yield_now() {
    struct YieldNow(bool);
    impl std::future::Future for YieldNow {
        type Output = ();

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.get_mut().0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }
    YieldNow(false).await;
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(f64::total_cmp);
    values[values.len() / 2]
}
