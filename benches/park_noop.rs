//! Benchmark the fixed cost of a park that has nothing to do.
//!
//! Every executor tick parks the driver, so per-park bookkeeping (such as CQ
//! health checks) is paid even when no I/O is in flight. This measures
//! `park(ParkMode::NoPark)` on an empty ring, which never enters the kernel.
//!
//! One iteration is [`PARKS`] parks, so divide the reported ns/iter by it
//! for ns/park; a single park is too short for the harness to time stably.

use std::borrow::Cow;

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_executor::park::{Park, ParkMode};

mod support;

/// Parks per benchmark iteration.
const PARKS: usize = 1000;

struct ParkNoopBench;

impl bencher::TDynBenchFn for ParkNoopBench {
    fn run(&self, b: &mut Bencher) {
        let mut builder = io_uring::IoUring::builder();
        builder
            .dontfork()
            .setup_coop_taskrun()
            .setup_defer_taskrun()
            .setup_single_issuer()
            .setup_submit_all();
        let mut driver = norn_uring::Driver::new(builder, 32).unwrap();
        b.iter(|| {
            for _ in 0..PARKS {
                driver.park(ParkMode::NoPark).unwrap();
            }
        });
    }
}

pub fn benches() -> ::std::vec::Vec<TestDescAndFn> {
    vec![TestDescAndFn {
        desc: TestDesc {
            name: Cow::from("bench_park/no_park/empty_ring/parks=1000"),
            ignore: false,
        },
        testfn: TestFn::DynBenchFn(Box::new(ParkNoopBench)),
    }]
}

fn main() {
    support::run(benches());
}
