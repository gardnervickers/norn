//! Benchmark noop submissions.

use std::borrow::Cow;
use std::cmp;

use bencher::{run_tests_console, Bencher, TestDesc, TestDescAndFn, TestFn, TestOpts};
use norn_executor::spawn;
use norn_uring::noop;

struct NoopBench(usize, usize);

impl NoopBench {
    fn new(tasks: usize, n: usize) -> Self {
        Self(tasks, n)
    }
}

impl bencher::TDynBenchFn for NoopBench {
    fn run(&self, b: &mut Bencher) {
        let mut builder = io_uring::IoUring::builder();
        builder
            .dontfork()
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let ring = norn_uring::Driver::new(builder, 32).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(ring);
        b.iter(|| {
            let tasks = self.0;
            let n = self.1;

            executor
                .block_on(async {
                    let mut handles = vec![];
                    for _ in 0..tasks {
                        let handle = spawn(async move {
                            for _ in 0..n {
                                noop().await;
                            }
                        });
                        handles.push(handle);
                    }
                    for handle in handles {
                        handle.await.unwrap();
                    }
                })
                .unwrap()
        });
    }
}

pub fn benches() -> ::std::vec::Vec<TestDescAndFn> {
    let mut benches = vec![];
    for num_tasks in [1, 32, 64] {
        for n in [1, 100_000] {
            let per_task = cmp::max(n / num_tasks, 1);
            benches.push(TestDescAndFn {
                desc: TestDesc {
                    name: Cow::from(format!("bench_noop/num_tasks={}/n={}", num_tasks, n)),
                    ignore: false,
                },
                testfn: TestFn::DynBenchFn(Box::new(NoopBench::new(num_tasks, per_task))),
            })
        }
    }
    benches
}

fn main() {
    let mut test_opts = TestOpts::default();
    if let Some(arg) = ::std::env::args().skip(1).find(|arg| *arg != "--bench") {
        test_opts.filter = Some(arg);
    }
    let mut all = Vec::new();
    all.extend(benches());
    run_tests_console(&test_opts, all).unwrap();
}
