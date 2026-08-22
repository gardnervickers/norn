//! Benchmark noop submissions.

use std::borrow::Cow;
use std::cmp;
use std::future::Future;
use std::io;
use std::task::{Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use futures::future;
use futures::task::noop_waker;
use norn_executor::spawn;
use norn_uring::noop;

mod support;

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

            executor.block_on(async {
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
        });
    }
}

/// Benchmark intentionally shaped to stress SQ backpressure.
///
/// We submit a burst of `n` noop futures with `join_all` while using a very
/// small ring. Since `n` is orders of magnitude larger than ring entries, this
/// necessarily exercises the SQ-full wait path in `PushFuture`.
struct NoopBackpressureBench {
    ring_entries: u32,
    n: usize,
}

struct TerminalDropBench(usize);

impl bencher::TDynBenchFn for TerminalDropBench {
    fn run(&self, b: &mut Bencher) {
        let builder = io_uring::IoUring::builder();
        let ring = norn_uring::Driver::new(builder, 32).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(ring);
        let n = self.0;

        b.bench_n(1, |b| {
            b.iter(|| executor.block_on(drop_terminal_noops(n)));
        });
    }
}

async fn drop_terminal_noops(n: usize) {
    let mut noops = Vec::with_capacity(n);
    for _ in 0..n {
        noops.push(Box::pin(noop()));
    }

    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    for noop in &mut noops {
        assert!(matches!(noop.as_mut().poll(&mut cx), Poll::Pending));
    }

    norn_uring::Handle::current().submit(DrainNop).await;
    drop(noops);
}

#[derive(Debug)]
struct DrainNop;

unsafe impl norn_uring::Operation for DrainNop {
    type Completion = norn_uring::CQEResult;

    fn configure(&mut self) -> io::Result<io_uring::squeue::Entry> {
        Ok(io_uring::opcode::Nop::new()
            .build()
            .flags(io_uring::squeue::Flags::IO_DRAIN))
    }

    unsafe fn reap(&mut self, result: norn_uring::CQEResult) -> Self::Completion {
        result
    }
}

impl norn_uring::Singleshot for DrainNop {
    type Output = ();

    fn complete(self, result: norn_uring::CQEResult) -> Self::Output {
        result.into_result().unwrap();
    }
}

impl NoopBackpressureBench {
    fn new(ring_entries: u32, n: usize) -> Self {
        assert!(n > ring_entries as usize);
        Self { ring_entries, n }
    }
}

impl bencher::TDynBenchFn for NoopBackpressureBench {
    fn run(&self, b: &mut Bencher) {
        let mut builder = io_uring::IoUring::builder();
        builder
            .dontfork()
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let ring = norn_uring::Driver::new(builder, self.ring_entries).unwrap();
        let mut executor = norn_executor::LocalExecutor::new(ring);
        b.iter(|| {
            let n = self.n;
            executor.block_on(async move {
                let mut futs = Vec::with_capacity(n);
                for _ in 0..n {
                    futs.push(noop());
                }
                future::join_all(futs).await;
            });
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
    for (ring_entries, n) in [(2u32, 4_096usize), (4u32, 16_384usize)] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!(
                    "bench_noop_backpressure/ring_entries={}/n={}",
                    ring_entries, n
                )),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(NoopBackpressureBench::new(ring_entries, n))),
        })
    }
    benches.push(TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("bench_drop_terminal/ops=16"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(TerminalDropBench(16))),
    });
    benches
}

fn main() {
    support::run(benches());
}
