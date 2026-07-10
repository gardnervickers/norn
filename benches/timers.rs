use std::borrow::Cow;
use std::cmp;
use std::future::Future;
use std::task::{Context, Poll};
use std::time::Duration;

use bencher::{Bencher, TestDesc, TestDescAndFn, TestFn};
use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::{spawn, LocalExecutor};
use norn_timer::{Clock, Driver, Handle};

mod support;

struct TimerBench {
    tasks: usize,
    timers: usize,
}

struct CancelBench {
    timers: usize,
}

struct TokioCancelBench {
    timers: usize,
}

impl bencher::TDynBenchFn for CancelBench {
    fn run(&self, b: &mut Bencher) {
        let clock = Clock::simulated();
        let driver = Driver::new((), clock);
        let handle = driver.handle();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let timers = self.timers;

        b.iter(|| {
            let mut sleeps = (0..timers)
                .map(|offset| Box::pin(handle.sleep(Duration::from_millis(4096 + offset as u64))))
                .collect::<Vec<_>>();

            for sleep in &mut sleeps {
                assert!(matches!(sleep.as_mut().poll(&mut cx), Poll::Pending));
            }

            // All deadlines occupy the same higher-level wheel slot. Cancelling
            // in ascending deadline order stresses minimum-deadline maintenance.
            for sleep in sleeps {
                drop(sleep);
            }
        });
    }
}

impl bencher::TDynBenchFn for TokioCancelBench {
    fn run(&self, b: &mut Bencher) {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .unwrap();
        let _guard = runtime.enter();
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let timers = self.timers;

        b.iter(|| {
            let start = tokio::time::Instant::now();
            let mut sleeps = (0..timers)
                .map(|offset| {
                    Box::pin(tokio::time::sleep_until(
                        start + Duration::from_millis(4096 + offset as u64),
                    ))
                })
                .collect::<Vec<_>>();

            for sleep in &mut sleeps {
                assert!(matches!(sleep.as_mut().poll(&mut cx), Poll::Pending));
            }

            for sleep in sleeps {
                drop(sleep);
            }
        });
    }
}

impl bencher::TDynBenchFn for TimerBench {
    fn run(&self, b: &mut Bencher) {
        let clock = Clock::simulated();
        let park = FastPark(clock.clone());
        let driver = Driver::new(park, clock);
        let mut executor = LocalExecutor::new(driver);
        b.iter(|| {
            let tasks = self.tasks;
            let timers = self.timers;
            executor.block_on(async {
                let mut handles = vec![];
                for _ in 0..tasks {
                    let handle = spawn(async move {
                        for _ in 0..timers {
                            Handle::current()
                                .sleep(Duration::from_secs(1))
                                .await
                                .unwrap();
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

struct FastPark(Clock);

#[derive(Debug, Clone, Copy)]
struct Unparker;

impl Unpark for Unparker {
    fn unpark(&self) {}
}

impl Park for FastPark {
    type Unparker = Unparker;

    type Guard = ();

    fn park(&mut self, mode: ParkMode) -> Result<(), std::io::Error> {
        match mode {
            ParkMode::NoPark => Ok(()),
            ParkMode::NextCompletion => unimplemented!(),
            ParkMode::Timeout(duration) => {
                self.0.advance(duration);
                Ok(())
            }
        }
    }

    fn enter(&self) -> Self::Guard {}

    fn unparker(&self) -> Self::Unparker {
        Unparker
    }

    fn needs_park(&self) -> bool {
        false
    }

    fn shutdown(&mut self) {}
}

pub fn benches() -> ::std::vec::Vec<TestDescAndFn> {
    let mut benches = vec![];

    for num_tasks in [1, 32, 64] {
        for n in [16, 64, 256] {
            let per_task = cmp::max(n / num_tasks, 1);
            benches.push(TestDescAndFn {
                desc: TestDesc {
                    name: Cow::from(format!("bench_timers/num_tasks={}/n={}", num_tasks, n)),
                    ignore: false,
                },
                testfn: TestFn::DynBenchFn(Box::new(TimerBench {
                    tasks: num_tasks,
                    timers: per_task,
                })),
            })
        }
    }
    for timers in [64, 512, 4096] {
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!(
                    "bench_cancel_same_slot/runtime=norn/timers={timers}"
                )),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(CancelBench { timers })),
        });
        benches.push(TestDescAndFn {
            desc: TestDesc {
                name: Cow::from(format!(
                    "bench_cancel_same_slot/runtime=tokio/timers={timers}"
                )),
                ignore: false,
            },
            testfn: TestFn::DynBenchFn(Box::new(TokioCancelBench { timers })),
        });
    }
    benches
}

fn main() {
    support::run(benches());
}
