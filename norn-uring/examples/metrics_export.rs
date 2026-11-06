//! Read every worker's runtime metrics from another thread: flag a worker
//! whose tick runs too long, and print all counters in the Prometheus text
//! format.
//!
//! `cargo run --release -p norn-uring --example metrics_export`
//!
//! Two workers run noops and timer sleeps for three seconds. After one
//! second, worker 1 runs a poll that blocks its thread for 300 ms. The
//! watchdog on the main thread samples every 25 ms and reports the stall
//! without any help from the stuck worker. At the end it prints the final
//! counts, which stay readable after the workers have exited.
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::mpsc;
use std::time::{Duration, Instant};

use norn_executor::{ExecutorMetrics, LocalExecutor};
use norn_metrics::{Kind, Metric, Visit};
use norn_timer::TimerMetrics;
use norn_uring::RingMetrics;

const STALL_THRESHOLD: Duration = Duration::from_millis(100);
const SAMPLE_EVERY: Duration = Duration::from_millis(25);

/// One worker's read-only views, collected once at startup.
struct WorkerMetrics {
    executor: ExecutorMetrics,
    ring: RingMetrics,
    timer: TimerMetrics,
}

fn worker(id: usize, tx: mpsc::Sender<(usize, WorkerMetrics)>) {
    let ring = norn_uring::Driver::new(io_uring::IoUring::builder(), 64).unwrap();
    let ring_metrics = ring.metrics();
    let timer = norn_timer::Driver::new(ring, norn_timer::Clock::system());
    let timer_metrics = timer.handle().metrics();
    let mut executor = LocalExecutor::new(timer);
    let metrics = WorkerMetrics {
        executor: executor.metrics(),
        ring: ring_metrics,
        timer: timer_metrics,
    };
    tx.send((id, metrics)).unwrap();
    drop(tx);
    executor.block_on(async move {
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
            if id == 1 && !blocked && start.elapsed() > Duration::from_secs(1) {
                blocked = true;
                // A poll that blocks the worker thread, as a synchronous call
                // hidden in async code would.
                std::thread::sleep(Duration::from_millis(300));
            }
        }
    });
}

/// What the watchdog last saw from one worker.
struct Watch {
    ticks: u64,
    parked: bool,
    since: Instant,
    reported: bool,
}

fn main() {
    let (tx, rx) = mpsc::channel();
    let threads: Vec<_> = (0..2)
        .map(|id| {
            let tx = tx.clone();
            std::thread::spawn(move || worker(id, tx))
        })
        .collect();
    drop(tx);
    let mut workers: Vec<(usize, WorkerMetrics)> = rx.iter().collect();
    workers.sort_by_key(|(id, _)| *id);

    // Stall detection: a tick counts itself before it parks, so if two
    // samples show the same tick count and neither shows the executor
    // parked, one tick has been running ever since the first of them.
    let now = Instant::now();
    let mut watches: Vec<Watch> = workers
        .iter()
        .map(|_| Watch {
            ticks: u64::MAX,
            parked: true,
            since: now,
            reported: false,
        })
        .collect();
    while threads.iter().any(|t| !t.is_finished()) {
        std::thread::sleep(SAMPLE_EVERY);
        let now = Instant::now();
        for ((id, metrics), watch) in workers.iter().zip(&mut watches) {
            let snapshot = metrics.executor.snapshot();
            if snapshot.ticks == watch.ticks && !snapshot.parked && !watch.parked {
                let stuck_for = now - watch.since;
                if stuck_for >= STALL_THRESHOLD && !watch.reported {
                    watch.reported = true;
                    println!(
                        "watchdog: worker {id} has run one tick for at least {} ms \
                         (tick {}, {} polls so far)",
                        stuck_for.as_millis(),
                        snapshot.ticks + 1,
                        snapshot.polls
                    );
                }
            } else {
                *watch = Watch {
                    ticks: snapshot.ticks,
                    parked: snapshot.parked,
                    since: now,
                    reported: false,
                };
            }
        }
    }
    for thread in threads {
        thread.join().unwrap();
    }

    // The workers and their drivers are gone; their counts are still here.
    let start = Instant::now();
    let text = prometheus_text(&workers);
    let render = start.elapsed();
    print!("{text}");
    println!(
        "# rendered {} workers in {} us",
        workers.len(),
        render.as_micros()
    );
}

/// One metric's description and its value on each worker.
type Family = (Metric, Vec<(usize, u64)>);

/// Render every worker's metrics in the Prometheus text exposition format,
/// one family per metric name with a `worker` label per sample.
fn prometheus_text(workers: &[(usize, WorkerMetrics)]) -> String {
    let mut families: BTreeMap<&'static str, Family> = BTreeMap::new();
    for (id, metrics) in workers {
        let mut add = |metric: Metric| {
            families
                .entry(metric.name)
                .or_insert_with(|| (metric, Vec::new()))
                .1
                .push((*id, metric.value));
        };
        metrics.executor.snapshot().visit(&mut add);
        metrics.ring.snapshot().visit(&mut add);
        metrics.timer.snapshot().visit(&mut add);
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
