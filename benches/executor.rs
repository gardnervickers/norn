use std::borrow::Cow;
use std::hint::black_box;
use std::pin::Pin;
use std::task::{Context, Poll};

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_executor::park::SpinPark;
use norn_executor::LocalExecutor;

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
    ]
}

fn main() {
    support::run(benches());
}
