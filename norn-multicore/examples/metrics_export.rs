//! Read every worker's runtime metrics from another thread: flag a worker
//! whose tick runs too long, and print every layer's counters in the
//! Prometheus text format.
//!
//! `cargo run --release -p norn-multicore --example metrics_export`
//!
//! Two workers, each an `io_uring` driver under a timer wheel, run noops and
//! sleeps for three seconds. After one second, worker 1 runs a poll that
//! blocks its thread for 300 ms. A watchdog thread samples every 25 ms and
//! reports the stall without any help from the stuck worker. At the end the
//! example prints the final counts, which stay readable after the workers
//! have exited.
#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("metrics_export needs io_uring, which is Linux-only");
}

#[cfg(target_os = "linux")]
mod linux {
    use std::collections::BTreeMap;
    use std::fmt::Write as _;
    use std::io;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use norn_executor::ExecutorMetrics;
    use norn_metrics::{Kind, Metric, Sources, Visit};
    use norn_multicore::{RuntimeGroup, WorkerId};
    use norn_uring::RingMetrics;

    const STALL_THRESHOLD: Duration = Duration::from_millis(100);
    const SAMPLE_EVERY: Duration = Duration::from_millis(25);

    pub(super) fn main() {
        let mut builder = RuntimeGroup::builder(2);
        for index in 0..2 {
            builder
                .worker_with(
                    WorkerId::new(index),
                    || {
                        let ring = norn_uring::Driver::new(io_uring::IoUring::builder(), 64)?;
                        Ok::<_, io::Error>(norn_timer::Driver::new(
                            ring,
                            norn_timer::Clock::system(),
                        ))
                    },
                    move |_| workload(index),
                )
                .unwrap();
        }
        let group = builder.start().unwrap();
        // Executor, timer and ring views of every worker, collected at startup.
        let workers = group.metrics();

        let done = Arc::new(AtomicBool::new(false));
        let watchdog = {
            let workers = workers.clone();
            let done = Arc::clone(&done);
            std::thread::spawn(move || watch(&workers, &done))
        };
        group.join().unwrap();
        done.store(true, Ordering::Relaxed);
        watchdog.join().unwrap();

        // Typed views for a summary, then every value for Prometheus.
        for (id, sources) in &workers {
            let ring = sources.get::<RingMetrics>().unwrap().snapshot();
            let executor = sources.get::<ExecutorMetrics>().unwrap().snapshot();
            println!(
                "# worker {id}: {} polls, {} ticks, {} CQEs, {} enters, ring {:?}",
                executor.polls, executor.ticks, ring.cqes, ring.enters, ring.status
            );
        }
        print!("{}", prometheus_text(&workers));
    }

    async fn workload(index: usize) {
        let start = Instant::now();
        let mut blocked = false;
        while start.elapsed() < Duration::from_secs(3) {
            let noops: Vec<_> = (0..64)
                .map(|_| norn_executor::spawn(norn_uring::noop()))
                .collect();
            for noop in noops {
                noop.await.unwrap().unwrap();
            }
            norn_timer::Handle::current()
                .sleep(Duration::from_millis(5))
                .await
                .unwrap();
            if index == 1 && !blocked && start.elapsed() > Duration::from_secs(1) {
                blocked = true;
                // A poll that blocks the worker thread, as a synchronous call
                // hidden in async code would.
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    }

    /// What the watchdog last saw from one worker.
    struct Watch {
        activity: u64,
        since: Instant,
        reported: bool,
    }

    /// Report any worker that runs without parking for longer than the
    /// threshold.
    ///
    /// The executor's activity word changes whenever it starts or stops
    /// running, so if two samples show the same value and the executor is
    /// running, it has run without parking ever since the first of them.
    fn watch(workers: &[(WorkerId, Sources)], done: &AtomicBool) {
        let now = Instant::now();
        let mut watches: Vec<Watch> = workers
            .iter()
            .map(|_| Watch {
                activity: u64::MAX,
                since: now,
                reported: false,
            })
            .collect();
        while !done.load(Ordering::Relaxed) {
            std::thread::sleep(SAMPLE_EVERY);
            let now = Instant::now();
            for ((id, sources), watch) in workers.iter().zip(&mut watches) {
                let snapshot = sources.get::<ExecutorMetrics>().unwrap().snapshot();
                if snapshot.running && snapshot.activity == watch.activity {
                    let stuck_for = now - watch.since;
                    if stuck_for >= STALL_THRESHOLD && !watch.reported {
                        watch.reported = true;
                        println!(
                            "watchdog: worker {id} has run for at least {} ms without \
                             parking (after {} ticks, {} polls)",
                            stuck_for.as_millis(),
                            snapshot.ticks,
                            snapshot.polls
                        );
                    }
                } else {
                    *watch = Watch {
                        activity: snapshot.activity,
                        since: now,
                        reported: false,
                    };
                }
            }
        }
    }

    /// One metric's description and its value on each worker.
    type Family = (Metric, Vec<(WorkerId, u64)>);

    /// Render every worker's metrics in the Prometheus text exposition format,
    /// one family per metric name with a `worker` label per sample.
    fn prometheus_text(workers: &[(WorkerId, Sources)]) -> String {
        let mut families: BTreeMap<&'static str, Family> = BTreeMap::new();
        for (id, sources) in workers {
            sources.visit(&mut |metric| {
                families
                    .entry(metric.name)
                    .or_insert_with(|| (metric, Vec::new()))
                    .1
                    .push((*id, metric.value));
            });
        }
        let mut out = String::new();
        for (name, (metric, samples)) in families {
            let kind = match metric.kind {
                Kind::Counter => "counter",
                Kind::Gauge => "gauge",
            };
            writeln!(out, "# HELP {name} {}", metric.help).unwrap();
            writeln!(out, "# TYPE {name} {kind}").unwrap();
            for (id, value) in samples {
                writeln!(out, "{name}{{worker=\"{id}\"}} {value}").unwrap();
            }
        }
        out
    }
}
