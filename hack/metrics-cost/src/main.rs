//! Cost of one counter increment under each mechanism considered for norn's
//! runtime metrics.
//!
//! Usage: `metrics-cost <variant> <iterations> [threads]`
//!
//! Single-thread variants are meant to run under
//! `perf stat -e instructions:u,cycles:u taskset -c <cpu>` at two iteration
//! counts: the difference, divided by the extra iterations, is the cost of one
//! loop iteration with setup excluded. Subtract the `none` variant for the
//! loop itself. `mt-*` variants pin one thread per CPU listed in `CPUS`
//! (default `8,9,10,11,12,13,14,15`) and print nanoseconds per increment.
//!
//! Every loop passes the counter's owner through `black_box` each iteration,
//! so the compiler must materialise every increment in memory, as it would at
//! an increment site in the runtime between unrelated code.

use std::cell::Cell;
use std::hint::black_box;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Barrier};
use std::time::Instant;

use opentelemetry::metrics::MeterProvider as _;
use opentelemetry::KeyValue;

#[repr(align(128))]
struct Padded<T>(T);

/// A single-writer counter: only the owning thread stores, any thread loads.
#[repr(transparent)]
struct LocalCounter(AtomicU64);

impl LocalCounter {
    #[inline(always)]
    fn incr(&self) {
        // Not a read-modify-write: the owner is the only writer, so a relaxed
        // load and store cannot lose an update. On x86-64 this is one
        // `add qword ptr [mem], 1` with no lock prefix.
        self.0.store(self.0.load(Relaxed).wrapping_add(1), Relaxed);
    }
}

thread_local! {
    static TLS_POLLS: Cell<u64> = const { Cell::new(0) };
}

fn main() {
    let mut args = std::env::args().skip(1);
    let variant = args.next().expect("variant");
    let n: u64 = args.next().map_or(100_000_000, |a| a.parse().unwrap());
    let threads: usize = args.next().map_or(8, |a| a.parse().unwrap());
    let start = Instant::now();
    let check = match variant.as_str() {
        "none" => none(n),
        "cell" => cell(n),
        "sw-atomic" => sw_atomic(n),
        "rmw-atomic" => rmw_atomic(n),
        "tls-cell" => tls_cell(n),
        "facade-noop" => facade_noop(n),
        "facade-cached-noop" => facade_cached_noop(n),
        "facade-prom" => facade_prom(n),
        "facade-prom-label" => facade_prom_label(n),
        "facade-cached-prom" => facade_cached_prom(n),
        "otel-noop" => otel_noop(n),
        "otel-sdk" => otel_sdk(n),
        "otel-sdk-attr" => otel_sdk_attr(n),
        "otel-sdk-bound" => otel_sdk_bound(n),
        "instant-now" => instant_now(n),
        "rdtsc" => rdtsc(n),
        v if v.starts_with("mt-") => {
            multi_thread(v, n, threads);
            return;
        }
        other => panic!("unknown variant {other}"),
    };
    let elapsed = start.elapsed();
    eprintln!(
        "{variant} n={n} check={check} ns/iter={:.2}",
        elapsed.as_nanos() as f64 / n as f64
    );
}

#[inline(never)]
fn none(n: u64) -> u64 {
    let block = Box::new(Cell::new(0u64));
    for _ in 0..n {
        black_box(&*block);
    }
    block.get()
}

#[inline(never)]
fn cell(n: u64) -> u64 {
    let block = Box::new(Cell::new(0u64));
    for _ in 0..n {
        let c = black_box(&*block);
        c.set(c.get() + 1);
    }
    block.get()
}

#[inline(never)]
fn sw_atomic(n: u64) -> u64 {
    let block = Arc::new(Padded(LocalCounter(AtomicU64::new(0))));
    for _ in 0..n {
        black_box(&block.0).incr();
    }
    block.0 .0.load(Relaxed)
}

#[inline(never)]
fn rmw_atomic(n: u64) -> u64 {
    let block = Arc::new(Padded(AtomicU64::new(0)));
    for _ in 0..n {
        black_box(&block.0).fetch_add(1, Relaxed);
    }
    block.0.load(Relaxed)
}

#[inline(never)]
fn tls_cell(n: u64) -> u64 {
    for _ in 0..n {
        TLS_POLLS.with(|c| c.set(c.get() + 1));
        black_box(());
    }
    TLS_POLLS.with(Cell::get)
}

#[inline(never)]
fn facade_noop(n: u64) -> u64 {
    for _ in 0..n {
        metrics::counter!("norn_task_polls").increment(1);
        black_box(());
    }
    0
}

#[inline(never)]
fn facade_cached_noop(n: u64) -> u64 {
    let counter = metrics::counter!("norn_task_polls");
    for _ in 0..n {
        black_box(&counter).increment(1);
    }
    0
}

fn install_prometheus() -> metrics_exporter_prometheus::PrometheusHandle {
    let recorder = metrics_exporter_prometheus::PrometheusBuilder::new().build_recorder();
    let handle = recorder.handle();
    metrics::set_global_recorder(recorder).unwrap();
    handle
}

/// Sum of every sample of `name` in a Prometheus text rendering.
fn rendered_total(handle: &metrics_exporter_prometheus::PrometheusHandle, name: &str) -> u64 {
    handle
        .render()
        .lines()
        .filter(|line| line.starts_with(name))
        .filter_map(|line| line.rsplit(' ').next()?.parse::<f64>().ok())
        .sum::<f64>() as u64
}

#[inline(never)]
fn facade_prom(n: u64) -> u64 {
    let handle = install_prometheus();
    for _ in 0..n {
        metrics::counter!("norn_task_polls").increment(1);
        black_box(());
    }
    rendered_total(&handle, "norn_task_polls")
}

#[inline(never)]
fn facade_prom_label(n: u64) -> u64 {
    let handle = install_prometheus();
    for _ in 0..n {
        metrics::counter!("norn_task_polls", "worker" => "3").increment(1);
        black_box(());
    }
    rendered_total(&handle, "norn_task_polls")
}

#[inline(never)]
fn facade_cached_prom(n: u64) -> u64 {
    let handle = install_prometheus();
    let counter = metrics::counter!("norn_task_polls", "worker" => "3");
    for _ in 0..n {
        black_box(&counter).increment(1);
    }
    rendered_total(&handle, "norn_task_polls")
}

#[inline(never)]
fn otel_noop(n: u64) -> u64 {
    let meter = opentelemetry::global::meter("norn");
    let counter = meter.u64_counter("norn_task_polls").build();
    for _ in 0..n {
        black_box(&counter).add(1, &[]);
    }
    0
}

/// A [`ManualReader`] the benchmark can collect from after handing it to the
/// provider, which takes its reader by value.
///
/// [`ManualReader`]: opentelemetry_sdk::metrics::ManualReader
#[derive(Debug, Clone)]
struct SharedReader(Arc<opentelemetry_sdk::metrics::ManualReader>);

impl opentelemetry_sdk::metrics::reader::MetricReader for SharedReader {
    fn register_pipeline(&self, pipeline: std::sync::Weak<opentelemetry_sdk::metrics::Pipeline>) {
        self.0.register_pipeline(pipeline)
    }

    fn collect(
        &self,
        rm: &mut opentelemetry_sdk::metrics::data::ResourceMetrics,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.collect(rm)
    }

    fn force_flush(&self) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.force_flush()
    }

    fn shutdown_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> opentelemetry_sdk::error::OTelSdkResult {
        self.0.shutdown_with_timeout(timeout)
    }

    fn temporality(
        &self,
        kind: opentelemetry_sdk::metrics::InstrumentKind,
    ) -> opentelemetry_sdk::metrics::Temporality {
        self.0.temporality(kind)
    }
}

fn sdk_provider() -> (opentelemetry_sdk::metrics::SdkMeterProvider, SharedReader) {
    let reader = SharedReader(Arc::new(
        opentelemetry_sdk::metrics::ManualReader::builder().build(),
    ));
    let provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(reader.clone())
        .build();
    (provider, reader)
}

fn otel_total(reader: &SharedReader) -> u64 {
    use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData, ResourceMetrics};
    use opentelemetry_sdk::metrics::reader::MetricReader;
    let mut rm = ResourceMetrics::default();
    reader.collect(&mut rm).unwrap();
    let mut total = 0;
    for scope in rm.scope_metrics() {
        for metric in scope.metrics() {
            if let AggregatedMetrics::U64(MetricData::Sum(sum)) = metric.data() {
                total += sum.data_points().map(|p| p.value()).sum::<u64>();
            }
        }
    }
    total
}

#[inline(never)]
fn otel_sdk(n: u64) -> u64 {
    let (provider, reader) = sdk_provider();
    let counter = provider
        .meter("norn")
        .u64_counter("norn_task_polls")
        .build();
    for _ in 0..n {
        black_box(&counter).add(1, &[]);
    }
    otel_total(&reader)
}

#[inline(never)]
fn otel_sdk_attr(n: u64) -> u64 {
    let (provider, reader) = sdk_provider();
    let counter = provider
        .meter("norn")
        .u64_counter("norn_task_polls")
        .build();
    let attrs = [KeyValue::new("worker", 3i64)];
    for _ in 0..n {
        black_box(&counter).add(1, black_box(&attrs));
    }
    otel_total(&reader)
}

#[inline(never)]
fn otel_sdk_bound(n: u64) -> u64 {
    let (provider, reader) = sdk_provider();
    let counter = provider
        .meter("norn")
        .u64_counter("norn_task_polls")
        .build();
    let bound = counter.bind(&[KeyValue::new("worker", 3i64)]);
    for _ in 0..n {
        black_box(&bound).add(1);
    }
    drop(bound);
    otel_total(&reader)
}

#[inline(never)]
fn instant_now(n: u64) -> u64 {
    let mut acc = 0u64;
    for _ in 0..n {
        let t = black_box(Instant::now());
        acc = acc.wrapping_add(t.elapsed().subsec_nanos() as u64 & 1);
    }
    acc
}

#[inline(never)]
fn rdtsc(n: u64) -> u64 {
    let mut acc = 0u64;
    for _ in 0..n {
        // Safety: RDTSC has no preconditions on x86-64.
        acc = acc.wrapping_add(unsafe { core::arch::x86_64::_rdtsc() } & 1);
        black_box(());
    }
    acc
}

fn cpus() -> Vec<usize> {
    std::env::var("CPUS")
        .unwrap_or_else(|_| "8,9,10,11,12,13,14,15".into())
        .split(',')
        .map(|c| c.trim().parse().unwrap())
        .collect()
}

fn pin(cpu: usize) {
    // Safety: a zeroed cpu_set_t is a valid empty set.
    unsafe {
        let mut set: libc::cpu_set_t = std::mem::zeroed();
        libc::CPU_SET(cpu, &mut set);
        assert_eq!(
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set),
            0
        );
    }
}

const WORKER_LABELS: [&str; 16] = [
    "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14", "15",
];

/// Run `threads` pinned threads that each increment `n` times, then report
/// nanoseconds per increment (mean and slowest thread).
fn multi_thread(variant: &str, n: u64, threads: usize) {
    let cpus = cpus();
    assert!(
        threads <= cpus.len(),
        "not enough CPUS for {threads} threads"
    );
    let shared = Arc::new(Padded(AtomicU64::new(0)));
    let private: Arc<Vec<Padded<AtomicU64>>> =
        Arc::new((0..threads).map(|_| Padded(AtomicU64::new(0))).collect());
    let prom = variant.starts_with("mt-facade").then(install_prometheus);
    let otel = variant.starts_with("mt-otel").then(|| {
        let (provider, reader) = sdk_provider();
        let counter = provider
            .meter("norn")
            .u64_counter("norn_task_polls")
            .build();
        (provider, reader, counter)
    });
    let barrier = Arc::new(Barrier::new(threads));
    let handles: Vec<_> = (0..threads)
        .map(|i| {
            let cpu = cpus[i];
            let variant = variant.to_owned();
            let shared = Arc::clone(&shared);
            let private = Arc::clone(&private);
            let otel_counter = otel.as_ref().map(|(_, _, c)| c.clone());
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                pin(cpu);
                let label = WORKER_LABELS[i];
                let cached = metrics::counter!("norn_task_polls", "worker" => label);
                let attrs = [KeyValue::new("worker", i as i64)];
                barrier.wait();
                let start = Instant::now();
                match variant.as_str() {
                    "mt-rmw-shared" => {
                        for _ in 0..n {
                            black_box(&shared.0).fetch_add(1, Relaxed);
                        }
                    }
                    "mt-rmw-private" => {
                        for _ in 0..n {
                            black_box(&private[i].0).fetch_add(1, Relaxed);
                        }
                    }
                    "mt-sw-private" => {
                        let counter: &LocalCounter =
                            // Safety: LocalCounter is a transparent wrapper.
                            unsafe { &*(&private[i].0 as *const AtomicU64).cast() };
                        for _ in 0..n {
                            black_box(counter).incr();
                        }
                    }
                    "mt-facade-shared" => {
                        for _ in 0..n {
                            metrics::counter!("norn_task_polls").increment(1);
                            black_box(());
                        }
                    }
                    "mt-facade-labeled" => {
                        for _ in 0..n {
                            metrics::counter!("norn_task_polls", "worker" => label).increment(1);
                            black_box(());
                        }
                    }
                    "mt-facade-cached-labeled" => {
                        for _ in 0..n {
                            black_box(&cached).increment(1);
                        }
                    }
                    "mt-otel-shared" => {
                        let counter = otel_counter.unwrap();
                        for _ in 0..n {
                            black_box(&counter).add(1, &[]);
                        }
                    }
                    "mt-otel-bound" => {
                        let bound = otel_counter.unwrap().bind(&attrs);
                        for _ in 0..n {
                            black_box(&bound).add(1);
                        }
                    }
                    "mt-otel-attr" => {
                        let counter = otel_counter.unwrap();
                        for _ in 0..n {
                            black_box(&counter).add(1, black_box(&attrs));
                        }
                    }
                    other => panic!("unknown variant {other}"),
                }
                start.elapsed().as_nanos() as f64 / n as f64
            })
        })
        .collect();
    let per_thread: Vec<f64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
    let mean = per_thread.iter().sum::<f64>() / per_thread.len() as f64;
    let max = per_thread.iter().cloned().fold(0.0, f64::max);
    let total = shared.0.load(Relaxed)
        + private.iter().map(|p| p.0.load(Relaxed)).sum::<u64>()
        + prom.map_or(0, |h| rendered_total(&h, "norn_task_polls"))
        + otel.map_or(0, |(_, reader, _)| otel_total(&reader));
    eprintln!(
        "{variant} threads={threads} n={n} total={total} ns/incr mean={mean:.2} max={max:.2}"
    );
}
