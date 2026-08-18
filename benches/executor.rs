use std::borrow::Cow;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_executor::park::SpinPark;
use norn_executor::{spawn, LocalExecutor};

mod support;

struct BlockOnReadyBench;

impl bencher::TDynBenchFn for BlockOnReadyBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(SpinPark);
        b.iter(|| {
            let value = std::future::ready(black_box(1_usize));
            black_box(executor.block_on(value));
        });
    }
}

struct BlockOnYieldBench;

impl bencher::TDynBenchFn for BlockOnYieldBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(SpinPark);
        b.iter(|| {
            executor.block_on(YieldOnce(false));
            black_box(())
        });
    }
}

struct YieldOnce(bool);

impl std::future::Future for YieldOnce {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct TaskYieldBench;

impl bencher::TDynBenchFn for TaskYieldBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(SpinPark);
        b.iter(|| {
            executor.block_on(async {
                let mut handles = Vec::with_capacity(128);
                for _ in 0..128 {
                    handles.push(spawn(run_yields(32)));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            });
            black_box(())
        });
    }
}

async fn run_yields(yields: usize) {
    for _ in 0..yields {
        YieldOnce(false).await;
    }
}

struct ExecutorSpawnBench(usize);

impl bencher::TDynBenchFn for ExecutorSpawnBench {
    fn run(&self, b: &mut Bencher) {
        let mut executor = LocalExecutor::new(SpinPark);
        let tasks = self.0;
        b.iter(|| {
            executor.block_on(async {
                let mut handles = Vec::with_capacity(tasks);
                for _ in 0..tasks {
                    handles.push(spawn(std::future::ready(())));
                }
                for handle in handles {
                    handle.await.unwrap();
                }
            });
            black_box(())
        });
    }
}

fn benches() -> Vec<TestDescAndFn> {
    vec![
        TestDescAndFn {
            desc: TestDesc {
                name: Cow::from("bench_block_on_ready"),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(BlockOnReadyBench)),
        },
        TestDescAndFn {
            desc: TestDesc {
                name: Cow::from("bench_block_on_yield"),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(BlockOnYieldBench)),
        },
        TestDescAndFn {
            desc: TestDesc {
                name: Cow::from("bench_task_yield/tasks=128/yields=32"),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(TaskYieldBench)),
        },
        TestDescAndFn {
            desc: TestDesc {
                name: Cow::from("bench_executor_spawn/tasks=1024"),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(ExecutorSpawnBench(1024))),
        },
    ]
}

fn main() {
    support::run(benches());
}
