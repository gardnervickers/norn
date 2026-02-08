use std::borrow::Cow;
use std::cmp;
use std::time::Duration;

use bencher::{run_tests_console, Bencher, TestDesc, TestDescAndFn, TestFn, TestOpts};
use norn_executor::park::{Park, ParkMode, Unpark};
use norn_executor::{spawn, LocalExecutor};
use norn_timer::{Clock, Driver, Handle};

struct TimerBench {
    tasks: usize,
    timers: usize,
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
