use std::borrow::Cow;
use std::pin::Pin;
use std::task::{Context, Poll};

use bencher::{run_tests_console, Bencher, TestDesc, TestDescAndFn, TestFn, TestOpts};
use norn_task::TaskQueue;

struct YieldBench {
    tasks: usize,
    yields: usize,
}

impl YieldBench {
    fn new(tasks: usize, yields: usize) -> Self {
        Self { tasks, yields }
    }
}

impl bencher::TDynBenchFn for YieldBench {
    fn run(&self, b: &mut Bencher) {
        let tq = TaskQueue::new();
        let tasks = self.tasks;
        let yields = self.yields;
        b.iter(|| {
            for _ in 0..tasks {
                tq.spawn(run_yields(yields)).detach();
            }
            while let Some(task) = tq.next() {
                task.run();
            }
        })
    }
}

async fn run_yields(yields: usize) {
    for _ in 0..yields {
        yield_now().await;
    }
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

pub fn benches() -> Vec<TestDescAndFn> {
    let mut benches = vec![];
    for tasks in [1, 32, 128] {
        for yields in [1, 8, 32] {
            benches.push(TestDescAndFn {
                desc: TestDesc {
                    name: Cow::from(format!(
                        "bench_task_yield/tasks={}/yields={}",
                        tasks, yields
                    )),
                    ignore: false,
                },
                testfn: TestFn::DynBenchFn(Box::new(YieldBench::new(tasks, yields))),
            });
        }
    }
    benches
}

fn main() {
    let mut test_opts = TestOpts::default();
    if let Some(arg) = std::env::args().skip(1).find(|arg| *arg != "--bench") {
        test_opts.filter = Some(arg);
    }

    run_tests_console(&test_opts, benches()).unwrap();
}
