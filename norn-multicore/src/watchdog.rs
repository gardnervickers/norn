//! A thread that watches every worker for runs without parking.

use std::panic::{self, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use norn_executor::{ExecutorMetrics, ExecutorSnapshot};

use crate::{GroupControl, WorkerId};

/// What the stall watchdog saw when it found a worker that had run for too
/// long without parking.
///
/// A worker that runs without parking is polling its root future or a task,
/// or running a waker or destructor they triggered, so a stall usually means
/// a blocking call or a long computation inside async code.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Stall {
    /// The stuck worker.
    pub worker: WorkerId,
    /// How long the worker had run without parking when it was reported. The
    /// run began between two samples, so this is a lower bound.
    pub running_for: Duration,
    /// The worker's executor counters when it was reported.
    pub executor: ExecutorSnapshot,
    /// The worker thread's stack when it was reported, innermost frame first,
    /// one symbolized frame per entry. Present only with the `stall-stacks`
    /// feature on Linux, when the capture succeeded.
    pub stack: Option<Vec<String>>,
}

pub(crate) struct WatchdogConfig {
    pub(crate) threshold: Duration,
    pub(crate) on_stall: Box<dyn Fn(&Stall) + Send + 'static>,
}

/// The running watchdog thread. Dropping it stops and joins the thread.
pub(crate) struct Watchdog {
    stop: Arc<AtomicBool>,
    thread: Option<JoinHandle<()>>,
}

impl Watchdog {
    pub(crate) fn spawn(
        config: WatchdogConfig,
        control: Arc<GroupControl>,
    ) -> std::io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread = {
            let stop = Arc::clone(&stop);
            thread::Builder::new()
                .name("norn-watchdog".to_owned())
                .spawn(move || watch(&config, &control, &stop))?
        };
        Ok(Self {
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for Watchdog {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take() {
            thread.thread().unpark();
            // A panicking callback ended the thread early; nothing to report.
            let _ = thread.join();
        }
    }
}

/// What the watchdog last saw from one worker.
struct Watch {
    activity: u64,
    since: Instant,
    reported: bool,
}

fn watch(config: &WatchdogConfig, control: &GroupControl, stop: &AtomicBool) {
    // Every started worker has set its sources before the watchdog starts.
    let workers: Vec<(WorkerId, ExecutorMetrics)> = control
        .metrics
        .iter()
        .enumerate()
        .filter_map(|(index, sources)| {
            let executor = sources.get()?.get::<ExecutorMetrics>()?.clone();
            Some((WorkerId::new(index), executor))
        })
        .collect();
    let interval = (config.threshold / 4).max(Duration::from_millis(1));
    let start = Instant::now();
    let mut watches: Vec<Watch> = workers
        .iter()
        .map(|_| Watch {
            activity: u64::MAX,
            since: start,
            reported: false,
        })
        .collect();
    while !stop.load(Ordering::Acquire) {
        thread::park_timeout(interval);
        if stop.load(Ordering::Acquire) {
            return;
        }
        let now = Instant::now();
        for ((worker, metrics), watch) in workers.iter().zip(&mut watches) {
            let executor = metrics.snapshot();
            // The activity word changes whenever the executor starts or stops
            // running, so the same value in two samples, while running, means
            // it ran without parking ever since the first of them.
            if executor.running && executor.activity == watch.activity {
                let running_for = now - watch.since;
                if running_for >= config.threshold && !watch.reported {
                    watch.reported = true;
                    let stall = Stall {
                        worker: *worker,
                        running_for,
                        executor,
                        stack: crate::stacks::capture(control, *worker),
                    };
                    // A panicking callback must not end the watchdog.
                    let _ = panic::catch_unwind(AssertUnwindSafe(|| (config.on_stall)(&stall)));
                }
            } else {
                *watch = Watch {
                    activity: executor.activity,
                    since: now,
                    reported: false,
                };
            }
        }
    }
}
