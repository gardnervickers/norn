use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_task::TaskQueue;

mod support;

struct JoinBench;

impl JoinBench {
    fn new() -> Self {
        Self
    }
}

impl bencher::TDynBenchFn for JoinBench {
    fn run(&self, b: &mut Bencher) {
        let tq = TaskQueue::default();
        b.iter(|| {
            let t1 = tq.spawn(async {
                yield_now().await;
            });
            tq.spawn(async {
                t1.await.unwrap();
            })
            .detach();
            while let Some(task) = tq.next() {
                task.run();
            }
        })
    }
}

struct SpawnBench {
    num_tasks: usize,
}

impl SpawnBench {
    fn new(num_tasks: usize) -> Self {
        Self { num_tasks }
    }
}

impl bencher::TDynBenchFn for SpawnBench {
    fn run(&self, b: &mut Bencher) {
        let num_tasks = self.num_tasks;
        let tq = TaskQueue::default();

        b.iter(|| {
            for _ in 0..num_tasks {
                tq.spawn(async {
                    yield_now().await;
                })
                .detach();
            }
            while let Some(task) = tq.next() {
                task.run();
            }
        })
    }
}

async fn yield_now() {
    struct YieldNow(bool);
    impl Future for YieldNow {
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
    YieldNow(false).await
}

pub fn benches() -> ::std::vec::Vec<TestDescAndFn> {
    let mut benches = Vec::new();
    for num_tasks in [1, 128, 1024] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!("bench_spawn/num_tasks={}", num_tasks)),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(SpawnBench::new(num_tasks))),
        });
    }

    benches.push(TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("bench_join"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(JoinBench::new())),
    });

    benches
}

fn main() {
    support::run(benches());
}
