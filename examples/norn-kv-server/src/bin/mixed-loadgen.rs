//! A bounded, paced TCP client for the mixed key/value workload.
//!
//! The client deliberately uses Tokio rather than Norn so that a server change
//! cannot also change the load generator's scheduling and socket behaviour.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Barrier, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use norn_kv_server::mixed_workload::{Class, Manifest, Phase, Profile};
use norn_kv_server::protocol::{HEADER_LEN, REQUEST_MAGIC, RESPONSE_MAGIC};
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{tcp::OwnedReadHalf, tcp::OwnedWriteHalf, TcpStream};
use tokio::sync::{mpsc, oneshot};

const DEFAULT_ADDRESS: &str = "127.0.0.1:11211";

#[derive(Debug)]
struct Config {
    manifest: PathBuf,
    output: PathBuf,
    address: SocketAddr,
}

#[derive(Debug, Default, Clone, Serialize)]
struct Counters {
    planned: u64,
    started: u64,
    outstanding: u64,
    valid_response: u64,
    invalid_response: u64,
    transport_failed: u64,
    unsent_backpressure: u64,
    unsent_generator: u64,
    unsent_transport: u64,
    deadline_misses: u64,
    schedule_lag_ns: u64,
    max_schedule_lag_ns: u64,
    request_bytes: u64,
    response_bytes: u64,
    max_connection_outstanding_requests: u64,
    max_connection_reserved_bytes: u64,
}

impl Counters {
    fn merge(&mut self, other: &Self) {
        self.planned += other.planned;
        self.started += other.started;
        self.outstanding += other.outstanding;
        self.valid_response += other.valid_response;
        self.invalid_response += other.invalid_response;
        self.transport_failed += other.transport_failed;
        self.unsent_backpressure += other.unsent_backpressure;
        self.unsent_generator += other.unsent_generator;
        self.unsent_transport += other.unsent_transport;
        self.deadline_misses += other.deadline_misses;
        self.schedule_lag_ns += other.schedule_lag_ns;
        self.max_schedule_lag_ns = self.max_schedule_lag_ns.max(other.max_schedule_lag_ns);
        self.request_bytes += other.request_bytes;
        self.response_bytes += other.response_bytes;
        self.max_connection_outstanding_requests = self
            .max_connection_outstanding_requests
            .max(other.max_connection_outstanding_requests);
        self.max_connection_reserved_bytes = self
            .max_connection_reserved_bytes
            .max(other.max_connection_reserved_bytes);
    }
}

#[derive(Debug)]
struct ClassStats {
    counters: Counters,
    offered_latency: Histogram<u64>,
    wire_latency: Histogram<u64>,
    histogram_overflow: u64,
    completion_windows: BTreeMap<u64, Histogram<u64>>,
}

impl ClassStats {
    fn new(max_us: u64) -> Self {
        let max = max_us.saturating_mul(1_000).max(1);
        Self {
            counters: Counters::default(),
            offered_latency: Histogram::new_with_max(max, 3).expect("valid histogram range"),
            wire_latency: Histogram::new_with_max(max, 3).expect("valid histogram range"),
            histogram_overflow: 0,
            completion_windows: BTreeMap::new(),
        }
    }

    fn record(&mut self, base: Instant, intended: Instant, started: Instant, completed: Instant) {
        let second = elapsed_ns(base, completed) / 1_000_000_000;
        let high = self.offered_latency.high();
        let window = self
            .completion_windows
            .entry(second)
            .or_insert_with(|| Histogram::new_with_max(high, 2).expect("valid histogram range"));
        if window.record(elapsed_ns(intended, completed)).is_err() {
            self.histogram_overflow += 1;
        }
        self.record_one(true, elapsed_ns(intended, completed));
        self.record_one(false, elapsed_ns(started, completed));
    }

    fn record_one(&mut self, offered: bool, value: u64) {
        let histogram = if offered {
            &mut self.offered_latency
        } else {
            &mut self.wire_latency
        };
        if histogram.record(value).is_err() {
            self.histogram_overflow += 1;
        }
    }

    fn merge(&mut self, other: &Self) {
        self.counters.merge(&other.counters);
        self.offered_latency
            .add(&other.offered_latency)
            .expect("same histogram range");
        self.wire_latency
            .add(&other.wire_latency)
            .expect("same histogram range");
        self.histogram_overflow += other.histogram_overflow;
        for (&second, histogram) in &other.completion_windows {
            self.completion_windows
                .entry(second)
                .or_insert_with(|| {
                    Histogram::new_with_max(histogram.high(), 2).expect("valid histogram range")
                })
                .add(histogram)
                .expect("same histogram range");
        }
    }
}

#[derive(Debug, Serialize)]
struct HistogramReport {
    significant_figures: u8,
    samples: u64,
    p50_us: f64,
    p95_us: f64,
    p99_us: Option<f64>,
    p999_us: Option<f64>,
    bins: Vec<HistogramBin>,
}

#[derive(Debug, Serialize)]
struct HistogramBin {
    upper_us: f64,
    count: u64,
}

impl From<&Histogram<u64>> for HistogramReport {
    fn from(histogram: &Histogram<u64>) -> Self {
        let us = |q| histogram.value_at_quantile(q) as f64 / 1_000.0;
        Self {
            significant_figures: histogram.sigfig(),
            samples: histogram.len(),
            p50_us: us(0.5),
            p95_us: us(0.95),
            p99_us: (histogram.len() >= 10_000).then(|| us(0.99)),
            p999_us: (histogram.len() >= 100_000).then(|| us(0.999)),
            bins: histogram
                .iter_recorded()
                .map(|value| HistogramBin {
                    upper_us: value.value_iterated_to() as f64 / 1_000.0,
                    count: value.count_since_last_iteration(),
                })
                .collect(),
        }
    }
}

#[derive(Debug, Serialize)]
struct ClassReport {
    class: String,
    counters: Counters,
    offered_latency: HistogramReport,
    wire_latency: HistogramReport,
    histogram_overflow: u64,
    completion_windows: BTreeMap<u64, HistogramReport>,
}

#[derive(Debug, Serialize)]
struct PhaseReport {
    phase: String,
    classes: Vec<ClassReport>,
}

#[derive(Debug, Serialize)]
struct Report {
    address: String,
    manifest: Manifest,
    process_elapsed_s: f64,
    setup_s: f64,
    measurement_duration_s: f64,
    drain_s: f64,
    valid: bool,
    source_valid: bool,
    performance_eligible: bool,
    error: Option<String>,
    phases: Vec<PhaseReport>,
}

#[derive(Debug)]
struct Expected {
    class: Class,
    phase_index: usize,
    base: Instant,
    intended: Instant,
    reserved_bytes: usize,
    opaque: u32,
    key: Vec<u8>,
    value: Vec<u8>,
    generation: u64,
    deadline: Duration,
    started: Option<oneshot::Receiver<Instant>>,
}

#[derive(Debug)]
struct Outgoing {
    bytes: Vec<u8>,
    started: oneshot::Sender<Instant>,
}

#[derive(Debug)]
struct Response {
    expected: Expected,
    started: Instant,
    completed: Instant,
    result: io::Result<usize>,
}

struct ConnectionResult {
    phases: Vec<[ClassStats; 4]>,
    error: Option<String>,
    drain_s: f64,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = parse_args()?;
    let text = fs::read_to_string(&config.manifest)?;
    let manifest: Manifest = serde_json::from_str(&text)?;
    manifest.validate().map_err(io::Error::other)?;

    let barrier = Arc::new(Barrier::new(manifest.threads + 1));
    let phase_start = Arc::new(OnceLock::new());
    let mut workers = Vec::with_capacity(manifest.threads);
    let started = Instant::now();
    for worker in 0..manifest.threads {
        let barrier = Arc::clone(&barrier);
        let phase_start = Arc::clone(&phase_start);
        let manifest = manifest.clone();
        let address = config.address;
        workers.push(thread::spawn(move || {
            worker_main(worker, manifest, address, barrier, phase_start)
        }));
    }
    barrier.wait();

    let phase_count = 1 + manifest.phases().len();
    let mut combined: Vec<[ClassStats; 4]> = (0..phase_count)
        .map(|_| std::array::from_fn(|_| ClassStats::new(manifest.histogram_max_us)))
        .collect();
    let mut error = None;
    let mut drain_s = 0.0_f64;
    for worker in workers {
        match worker.join() {
            Ok(Ok(results)) => {
                for result in results {
                    drain_s = drain_s.max(result.drain_s);
                    if error.is_none() {
                        error = result.error;
                    }
                    for (destination, source) in combined.iter_mut().zip(result.phases) {
                        for (destination, source) in destination.iter_mut().zip(source.iter()) {
                            destination.merge(source);
                        }
                    }
                }
            }
            Ok(Err(e)) => {
                error.get_or_insert_with(|| e.to_string());
            }
            Err(_) => {
                error.get_or_insert_with(|| "client worker panicked".to_owned());
            }
        }
    }
    let elapsed = started.elapsed();
    for stats in combined.iter().flatten() {
        let c = &stats.counters;
        if c.planned != c.started + c.unsent_backpressure + c.unsent_generator + c.unsent_transport
            || c.started
                != c.valid_response + c.invalid_response + c.transport_failed + c.outstanding
            || c.outstanding != 0
        {
            error.get_or_insert_with(|| "final request accounting is incomplete".to_owned());
        }
    }
    let source_valid = combined
        .iter()
        .skip(1)
        .flatten()
        .all(|stats| stats.histogram_overflow == 0 && stats.counters.unsent_generator == 0);
    let performance_eligible = source_valid
        && combined.iter().enumerate().skip(1).all(|(index, classes)| {
            classes.iter().all(|stats| {
                (manifest.profile == Profile::Overload && index == 2
                    || stats.counters.unsent_backpressure == 0)
                    && stats.counters.unsent_transport == 0
                    && stats.counters.transport_failed == 0
            })
        });
    let phase_names = std::iter::once("warmup".to_owned())
        .chain(manifest.phases().into_iter().map(|phase| phase.name))
        .collect::<Vec<_>>();
    let phases = combined
        .into_iter()
        .zip(phase_names)
        .map(|(stats, phase)| PhaseReport {
            phase,
            classes: Class::read_classes()
                .into_iter()
                .chain([Class::Overwrite])
                .map(|class| {
                    let stat = &stats[class.index()];
                    ClassReport {
                        class: format!("{class:?}"),
                        counters: stat.counters.clone(),
                        offered_latency: HistogramReport::from(&stat.offered_latency),
                        wire_latency: HistogramReport::from(&stat.wire_latency),
                        histogram_overflow: stat.histogram_overflow,
                        completion_windows: stat
                            .completion_windows
                            .iter()
                            .map(|(&second, histogram)| (second, HistogramReport::from(histogram)))
                            .collect(),
                    }
                })
                .collect(),
        })
        .collect();
    let report = Report {
        address: config.address.to_string(),
        manifest: manifest.clone(),
        process_elapsed_s: elapsed.as_secs_f64(),
        setup_s: phase_start.get().map_or(0.0, |base| {
            base.saturating_duration_since(started).as_secs_f64()
        }),
        measurement_duration_s: manifest
            .phases()
            .iter()
            .map(|phase| phase.duration_ms as f64 / 1000.0)
            .sum(),
        drain_s,
        valid: error.is_none(),
        source_valid,
        performance_eligible: error.is_none() && performance_eligible,
        error,
        phases,
    };
    fs::write(&config.output, serde_json::to_vec_pretty(&report)?)?;
    println!(
        "mixed-loadgen valid={} process_elapsed_s={:.3} output={}",
        report.valid,
        report.process_elapsed_s,
        config.output.display()
    );
    if !report.valid {
        return Err(
            io::Error::other(report.error.unwrap_or_else(|| "invalid trial".to_owned())).into(),
        );
    }
    Ok(())
}

fn worker_main(
    worker: usize,
    manifest: Manifest,
    address: SocketAddr,
    barrier: Arc<Barrier>,
    phase_start: Arc<OnceLock<Instant>>,
) -> io::Result<Vec<ConnectionResult>> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .enable_time()
        .build();
    let runtime = match runtime {
        Ok(runtime) => runtime,
        Err(error) => {
            barrier.wait();
            return Err(error);
        }
    };
    runtime.block_on(async move {
        let first = worker * manifest.connections / manifest.threads;
        let last = (worker + 1) * manifest.connections / manifest.threads;
        let mut preparation_error = None;
        let mut prepared = Vec::new();
        for connection in first..last {
            let preparation = async {
                let mut stream = TcpStream::connect(address).await?;
                stream.set_nodelay(true)?;
                populate(&mut stream, connection, &manifest).await?;
                Ok::<_, io::Error>(stream)
            };
            let preparation = tokio::time::timeout(Duration::from_secs(30), preparation)
                .await
                .unwrap_or_else(|_| {
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "population timed out",
                    ))
                });
            match preparation {
                Ok(stream) => prepared.push((connection, stream)),
                Err(error) => {
                    preparation_error = Some(error);
                    break;
                }
            }
        }
        barrier.wait();
        if let Some(error) = preparation_error {
            return Err(error);
        }
        let start = *phase_start.get_or_init(|| Instant::now() + Duration::from_millis(20));
        let mut joins = Vec::new();
        for (connection, stream) in prepared {
            let manifest = manifest.clone();
            joins.push(tokio::spawn(run_connection(
                connection, manifest, address, start, stream,
            )));
        }
        let mut results = Vec::with_capacity(joins.len());
        for join in joins {
            results.push(
                join.await
                    .map_err(|_| io::Error::other("connection task panicked"))?,
            );
        }
        Ok(results)
    })
}

async fn run_connection(
    connection: usize,
    manifest: Manifest,
    address: SocketAddr,
    start: Instant,
    stream: TcpStream,
) -> ConnectionResult {
    let phase_count = 1 + manifest.phases().len();
    let mut result = ConnectionResult {
        phases: (0..phase_count)
            .map(|_| std::array::from_fn(|_| ClassStats::new(manifest.histogram_max_us)))
            .collect(),
        error: None,
        drain_s: 0.0,
    };
    let run = async {
        let (read, write) = stream.into_split();
        let (out_tx, out_rx) = mpsc::channel(manifest.pipeline);
        let (expect_tx, expect_rx) = mpsc::unbounded_channel();
        let (response_tx, mut response_rx) = mpsc::unbounded_channel();
        let writer = tokio::spawn(writer_loop(write, out_rx));
        let reader = tokio::spawn(reader_loop(read, expect_rx, response_tx));

        let mut outstanding = 0usize;
        let mut outstanding_bytes = 0usize;
        let mut next_generations = BTreeMap::new();
        let mut next_opaque = 0_u32;
        let mut acknowledged = BTreeMap::new();
        let warmup = Phase {
            name: "warmup".to_owned(),
            duration_ms: manifest.warmup_ms,
            rate: manifest.rate,
        };
        let mut phases = vec![warmup];
        phases.extend(manifest.phases());
        for (phase_index, phase) in phases.iter().enumerate() {
            run_phase(
                connection,
                phase_index,
                phase,
                start,
                &manifest,
                &out_tx,
                &expect_tx,
                &mut response_rx,
                &mut result.phases,
                &mut outstanding,
                &mut outstanding_bytes,
                &mut next_generations,
                &mut next_opaque,
                &mut acknowledged,
            )
            .await?;
        }
        let drain_started = Instant::now();
        while outstanding != 0 {
            let remaining = Duration::from_millis(manifest.drain_ms)
                .checked_sub(drain_started.elapsed())
                .ok_or_else(|| io::Error::new(io::ErrorKind::TimedOut, "drain timeout"))?;
            let response = tokio::time::timeout(remaining, response_rx.recv())
                .await
                .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "drain timeout"))?
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "response task ended during drain",
                    )
                })?;
            process_response(
                response,
                &mut result.phases,
                &mut outstanding,
                &mut outstanding_bytes,
                &mut acknowledged,
            )?;
        }
        result.drain_s = drain_started.elapsed().as_secs_f64();
        drop(out_tx);
        drop(expect_tx);
        writer
            .await
            .map_err(|_| io::Error::other("writer task panicked"))??;
        reader
            .await
            .map_err(|_| io::Error::other("reader task panicked"))??;
        readback(address, connection, &manifest, acknowledged).await
    }
    .await;
    if let Err(error) = run {
        result.error = Some(error.to_string());
    }
    result
}

#[allow(clippy::too_many_arguments)]
async fn run_phase(
    connection: usize,
    phase_index: usize,
    phase: &Phase,
    base: Instant,
    manifest: &Manifest,
    out_tx: &mpsc::Sender<Outgoing>,
    expect_tx: &mpsc::UnboundedSender<Expected>,
    response_rx: &mut mpsc::UnboundedReceiver<Response>,
    stats: &mut [[ClassStats; 4]],
    outstanding: &mut usize,
    outstanding_bytes: &mut usize,
    next_generations: &mut BTreeMap<Vec<u8>, u64>,
    next_opaque: &mut u32,
    acknowledged: &mut BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
) -> io::Result<()> {
    let phase_start = base + phases_before(manifest, phase_index);
    let duration = Duration::from_millis(phase.duration_ms);
    let end = phase_start + duration;
    let mut sequence = connection as u64;
    let sequence_base = phase_sequence_base(manifest, phase_index);
    let mut turns = 0_u64;
    loop {
        turns = turns.wrapping_add(1);
        if turns.is_multiple_of(64) {
            tokio::task::yield_now().await;
        }
        // Reclaim completed credits even while the arrival schedule is behind.
        // Otherwise overload could prevent this connection from ever reading
        // already-completed responses until the next phase.
        while let Ok(response) = response_rx.try_recv() {
            process_response(
                response,
                stats,
                outstanding,
                outstanding_bytes,
                acknowledged,
            )?;
        }
        let offset = norn_kv_server::mixed_workload::scheduled_offset_ns(sequence, phase.rate);
        let intended = phase_start + Duration::from_nanos(offset);
        if intended >= end {
            break;
        }
        if *outstanding != 0 && Instant::now() < intended {
            match futures_util::future::select(
                Box::pin(response_rx.recv()),
                Box::pin(wait_until(intended)),
            )
            .await
            {
                futures_util::future::Either::Left((Some(response), _)) => {
                    process_response(
                        response,
                        stats,
                        outstanding,
                        outstanding_bytes,
                        acknowledged,
                    )?;
                    continue;
                }
                futures_util::future::Either::Left((None, _)) => {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "response task ended",
                    ))
                }
                futures_util::future::Either::Right(_) => {}
            }
        } else {
            wait_until(intended).await;
        }
        {
            let now = Instant::now();
            let request_sequence = sequence_base + sequence;
            let class = choose_class(manifest, connection, request_sequence);
            let entry = &mut stats[phase_index][class.index()].counters;
            entry.planned += 1;
            let lag = elapsed_ns(intended, now);
            entry.schedule_lag_ns += lag;
            entry.max_schedule_lag_ns = entry.max_schedule_lag_ns.max(lag);
            if lag > manifest.max_schedule_lag_us.saturating_mul(1_000) {
                entry.unsent_generator += 1;
                sequence += manifest.connections as u64;
                continue;
            }
            let (key, value, generation) = request_data(
                manifest,
                connection,
                request_sequence,
                class,
                next_generations,
            );
            // IDs advance only on admission. Skipped arrivals must not wrap the
            // wire sequence around an older request still held in a full pipeline.
            let opaque = *next_opaque;
            let bytes = build_request(class, &key, &value, opaque);
            let request_bytes = bytes.len();
            let reserved_bytes = request_bytes + expected_response_bytes(class, value.len());
            if *outstanding >= manifest.pipeline
                || outstanding_bytes.saturating_add(reserved_bytes)
                    > manifest.max_in_flight_bytes_per_connection
            {
                entry.unsent_backpressure += 1;
                sequence += manifest.connections as u64;
                continue;
            }
            let permit = match out_tx.try_reserve() {
                Ok(permit) => permit,
                Err(_) => {
                    entry.unsent_backpressure += 1;
                    sequence += manifest.connections as u64;
                    continue;
                }
            };
            let (started_tx, started_rx) = oneshot::channel();
            let deadline = Duration::from_micros(manifest.deadlines_us[class.index()]);
            expect_tx
                .send(Expected {
                    class,
                    phase_index,
                    base,
                    intended,
                    reserved_bytes,
                    opaque,
                    key,
                    value,
                    generation,
                    deadline,
                    started: Some(started_rx),
                })
                .map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "response task stopped"))?;
            if class == Class::Overwrite {
                let key = norn_kv_server::mixed_workload::write_key(
                    connection,
                    (norn_kv_server::mixed_workload::sample(manifest.seed, request_sequence)
                        as usize)
                        % manifest.overwrite_slots,
                );
                next_generations.insert(key, generation);
            }
            *next_opaque = next_opaque.wrapping_add(1);
            permit.send(Outgoing {
                bytes,
                started: started_tx,
            });
            *outstanding += 1;
            *outstanding_bytes += reserved_bytes;
            entry.started += 1;
            entry.outstanding += 1;
            entry.request_bytes += request_bytes as u64;
            entry.max_connection_outstanding_requests = entry
                .max_connection_outstanding_requests
                .max(*outstanding as u64);
            entry.max_connection_reserved_bytes = entry
                .max_connection_reserved_bytes
                .max(*outstanding_bytes as u64);
            sequence += manifest.connections as u64;
        }
    }
    Ok(())
}

fn phases_before(manifest: &Manifest, phase_index: usize) -> Duration {
    if phase_index == 0 {
        return Duration::ZERO;
    }
    let mut ms = manifest.warmup_ms;
    for phase in manifest
        .phases()
        .into_iter()
        .take(phase_index.saturating_sub(1))
    {
        ms += phase.duration_ms;
    }
    Duration::from_millis(ms)
}

fn phase_sequence_base(manifest: &Manifest, phase_index: usize) -> u64 {
    let mut count = manifest.rate.saturating_mul(manifest.warmup_ms) / 1_000 + 1;
    for phase in manifest
        .phases()
        .into_iter()
        .take(phase_index.saturating_sub(1))
    {
        count = count.saturating_add(phase.rate.saturating_mul(phase.duration_ms) / 1_000 + 1);
    }
    if phase_index == 0 {
        0
    } else {
        count
    }
}

async fn wait_until(deadline: Instant) {
    let now = Instant::now();
    if deadline > now + Duration::from_millis(2) {
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            deadline - Duration::from_millis(2),
        ))
        .await;
    }
    while Instant::now() < deadline {
        tokio::task::yield_now().await;
    }
}

fn choose_class(manifest: &Manifest, connection: usize, sequence: u64) -> Class {
    if manifest.profile == Profile::Interference {
        if connection < manifest.connections / 4 {
            Class::SmallRead
        } else {
            Class::LargeRead
        }
    } else {
        Class::for_sequence(manifest.seed, sequence)
    }
}

fn request_data(
    manifest: &Manifest,
    connection: usize,
    sequence: u64,
    class: Class,
    generations: &BTreeMap<Vec<u8>, u64>,
) -> (Vec<u8>, Vec<u8>, u64) {
    match class {
        Class::Overwrite => {
            let slot = (norn_kv_server::mixed_workload::sample(manifest.seed, sequence) as usize)
                % manifest.overwrite_slots;
            let key = norn_kv_server::mixed_workload::write_key(connection, slot);
            let generation = generations.get(&key).map_or(1, |generation| generation + 1);
            let value =
                norn_kv_server::mixed_workload::payload(&key, class.value_len(), generation);
            (key, value, generation)
        }
        _ => {
            let index =
                (norn_kv_server::mixed_workload::sample(manifest.seed ^ 0x72656164, sequence)
                    as usize)
                    % manifest.key_count(class);
            (manifest.read_key(class, index), Vec::new(), 0)
        }
    }
}

async fn writer_loop(
    mut write: OwnedWriteHalf,
    mut rx: mpsc::Receiver<Outgoing>,
) -> io::Result<()> {
    while let Some(outgoing) = rx.recv().await {
        let _ = outgoing.started.send(Instant::now());
        write.write_all(&outgoing.bytes).await?;
    }
    write.shutdown().await
}

async fn reader_loop(
    mut read: OwnedReadHalf,
    mut rx: mpsc::UnboundedReceiver<Expected>,
    tx: mpsc::UnboundedSender<Response>,
) -> io::Result<()> {
    while let Some(mut expected) = rx.recv().await {
        let started = expected
            .started
            .take()
            .expect("expected start receiver")
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "writer stopped before request start",
                )
            })?;
        let result = read_response(&mut read, &expected).await;
        let completed = Instant::now();
        let _ = tx.send(Response {
            expected,
            started,
            completed,
            result,
        });
    }
    Ok(())
}

async fn read_response<R: AsyncRead + Unpin>(
    read: &mut R,
    expected: &Expected,
) -> io::Result<usize> {
    let mut header = [0_u8; HEADER_LEN];
    read.read_exact(&mut header).await?;
    if header[0] != RESPONSE_MAGIC || header[1] != expected.class.opcode() || header[5] != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response magic, opcode, or datatype mismatch",
        ));
    }
    let key_len = u16::from_be_bytes(header[2..4].try_into().unwrap());
    let extras_len = header[4];
    let status = u16::from_be_bytes(header[6..8].try_into().unwrap());
    let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    let opaque = u32::from_be_bytes(header[12..16].try_into().unwrap());
    if key_len != 0 || status != 0 || opaque != expected.opaque {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response identifier, status, key, or CAS mismatch",
        ));
    }
    let expected_body = if expected.class == Class::Overwrite {
        0
    } else {
        expected.class.value_len() + 4
    };
    if extras_len as usize != usize::from(expected.class != Class::Overwrite) * 4
        || body_len != expected_body
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "response body layout mismatch",
        ));
    }
    let mut body = vec![0; body_len];
    read.read_exact(&mut body).await?;
    if expected.class != Class::Overwrite
        && (body[..4] != 0_u32.to_be_bytes()
            || body[4..]
                != norn_kv_server::mixed_workload::payload(
                    &expected.key,
                    expected.class.value_len(),
                    0,
                ))
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "GET payload verification failed",
        ));
    }
    Ok(HEADER_LEN + body_len)
}

fn process_response(
    response: Response,
    stats: &mut [[ClassStats; 4]],
    outstanding: &mut usize,
    outstanding_bytes: &mut usize,
    generations: &mut BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
) -> io::Result<()> {
    *outstanding = outstanding
        .checked_sub(1)
        .ok_or_else(|| io::Error::other("response without outstanding request"))?;
    *outstanding_bytes = outstanding_bytes
        .checked_sub(response.expected.reserved_bytes)
        .ok_or_else(|| io::Error::other("response byte accounting underflow"))?;
    let stat = &mut stats[response.expected.phase_index][response.expected.class.index()];
    stat.counters.outstanding = stat
        .counters
        .outstanding
        .checked_sub(1)
        .ok_or_else(|| io::Error::other("cohort outstanding accounting underflow"))?;
    match response.result {
        Ok(bytes) => {
            stat.counters.valid_response += 1;
            stat.counters.response_bytes += bytes as u64;
            stat.record(
                response.expected.base,
                response.expected.intended,
                response.started,
                response.completed,
            );
            if response
                .completed
                .duration_since(response.expected.intended)
                > response.expected.deadline
            {
                stat.counters.deadline_misses += 1;
            }
            if response.expected.class == Class::Overwrite {
                generations.insert(
                    response.expected.key,
                    (response.expected.generation, response.expected.value),
                );
            }
            Ok(())
        }
        Err(error) => {
            if error.kind() == io::ErrorKind::InvalidData {
                stat.counters.invalid_response += 1;
            } else {
                stat.counters.transport_failed += 1;
            }
            Err(error)
        }
    }
}

async fn populate(
    stream: &mut TcpStream,
    connection: usize,
    manifest: &Manifest,
) -> io::Result<()> {
    for class in Class::read_classes() {
        for index in (connection..manifest.key_count(class)).step_by(manifest.connections) {
            let key = manifest.read_key(class, index);
            let value = norn_kv_server::mixed_workload::payload(&key, class.value_len(), 0);
            round_trip_set(stream, &key, &value, index as u32).await?;
        }
    }
    for slot in 0..manifest.overwrite_slots {
        let key = norn_kv_server::mixed_workload::write_key(connection, slot);
        let value = norn_kv_server::mixed_workload::payload(&key, Class::Overwrite.value_len(), 0);
        round_trip_set(stream, &key, &value, slot as u32).await?;
    }
    Ok(())
}

async fn readback(
    address: SocketAddr,
    connection: usize,
    manifest: &Manifest,
    generations: BTreeMap<Vec<u8>, (u64, Vec<u8>)>,
) -> io::Result<()> {
    let mut stream = TcpStream::connect(address).await?;
    stream.set_nodelay(true)?;
    for (key, (_, value)) in generations {
        round_trip_get(&mut stream, &key, &value, connection as u32).await?;
    }
    let _ = manifest;
    Ok(())
}

async fn round_trip_set(
    stream: &mut TcpStream,
    key: &[u8],
    value: &[u8],
    opaque: u32,
) -> io::Result<()> {
    stream
        .write_all(&build_request(Class::Overwrite, key, value, opaque))
        .await?;
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    if header[0] != RESPONSE_MAGIC
        || header[1] != Class::Overwrite.opcode()
        || u16::from_be_bytes(header[6..8].try_into().unwrap()) != 0
        || u32::from_be_bytes(header[12..16].try_into().unwrap()) != opaque
        || header[2..6] != [0, 0, 0, 0]
        || header[8..12] != [0, 0, 0, 0]
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "SET preload response mismatch",
        ));
    }
    Ok(())
}
async fn round_trip_get(
    stream: &mut TcpStream,
    key: &[u8],
    value: &[u8],
    opaque: u32,
) -> io::Result<()> {
    stream
        .write_all(&build_request(Class::SmallRead, key, &[], opaque))
        .await?;
    let mut header = [0; HEADER_LEN];
    stream.read_exact(&mut header).await?;
    let len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
    if len != value.len() + 4 || header[2..6] != [0, 0, 4, 0] {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "readback body layout mismatch",
        ));
    }
    let mut body = vec![0; len];
    stream.read_exact(&mut body).await?;
    if header[0] != RESPONSE_MAGIC
        || header[1] != Class::SmallRead.opcode()
        || u16::from_be_bytes(header[6..8].try_into().unwrap()) != 0
        || u32::from_be_bytes(header[12..16].try_into().unwrap()) != opaque
        || body.len() != value.len() + 4
        || body[4..] != *value
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "write readback mismatch",
        ));
    }
    Ok(())
}

fn build_request(class: Class, key: &[u8], value: &[u8], opaque: u32) -> Vec<u8> {
    let set = class == Class::Overwrite;
    let extras = usize::from(set) * 8;
    let body = extras + key.len() + value.len();
    let mut bytes = Vec::with_capacity(HEADER_LEN + body);
    bytes.extend_from_slice(&[REQUEST_MAGIC, class.opcode()]);
    bytes.extend_from_slice(&(key.len() as u16).to_be_bytes());
    bytes.push(extras as u8);
    bytes.push(0);
    bytes.extend_from_slice(&0_u16.to_be_bytes());
    bytes.extend_from_slice(&(body as u32).to_be_bytes());
    bytes.extend_from_slice(&opaque.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    if set {
        bytes.extend_from_slice(&0_u64.to_be_bytes());
    }
    bytes.extend_from_slice(key);
    bytes.extend_from_slice(value);
    bytes
}
fn expected_response_bytes(class: Class, value_len: usize) -> usize {
    HEADER_LEN
        + if class == Class::Overwrite {
            0
        } else {
            value_len.max(class.value_len()) + 4
        }
}
fn elapsed_ns(start: Instant, end: Instant) -> u64 {
    end.saturating_duration_since(start)
        .as_nanos()
        .min(u64::MAX as u128) as u64
}

fn parse_args() -> io::Result<Config> {
    let mut manifest = None;
    let mut output = None;
    let mut address = DEFAULT_ADDRESS.parse().unwrap();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if matches!(arg.as_str(), "-h" | "--help") {
            eprintln!(
                "Usage: norn-mixed-loadgen --manifest PATH --output PATH [--address HOST:PORT]"
            );
            std::process::exit(0)
        }
        let (name, value) = if let Some((name, value)) = arg.split_once('=') {
            (name.to_owned(), value.to_owned())
        } else {
            let value = args.next().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "missing option value")
            })?;
            (arg, value)
        };
        match name.as_str() {
            "--manifest" => manifest = Some(value.into()),
            "--output" => output = Some(value.into()),
            "--address" => {
                address = value
                    .parse()
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("unknown argument {name}"),
                ))
            }
        }
    }
    Ok(Config {
        manifest: manifest
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--manifest is required"))?,
        output: output
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "--output is required"))?,
        address,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn expected(base: Instant) -> Expected {
        Expected {
            class: Class::SmallRead,
            phase_index: 0,
            base,
            intended: base,
            reserved_bytes: 512,
            opaque: 42,
            key: b"key".to_vec(),
            value: Vec::new(),
            generation: 0,
            deadline: Duration::from_millis(2),
            started: None,
        }
    }

    fn reply(expected: &Expected) -> Vec<u8> {
        let mut header = vec![0; HEADER_LEN];
        header[0] = RESPONSE_MAGIC;
        header[1] = expected.class.opcode();
        header[4] = 4;
        header[8..12].copy_from_slice(&260_u32.to_be_bytes());
        header[12..16].copy_from_slice(&expected.opaque.to_be_bytes());
        header[16..24].copy_from_slice(&1234_u64.to_be_bytes());
        header.extend_from_slice(&0_u32.to_be_bytes());
        header.extend(norn_kv_server::mixed_workload::payload(
            &expected.key,
            256,
            0,
        ));
        header
    }

    #[test]
    fn fragmented_response_accepts_opaque_cas_and_checks_payload() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let expected = expected(Instant::now());
                let bytes = reply(&expected);
                let (mut writer, mut reader) = tokio::io::duplex(1);
                let sender = tokio::spawn(async move {
                    for byte in bytes {
                        writer.write_all(&[byte]).await.unwrap();
                    }
                });
                assert_eq!(read_response(&mut reader, &expected).await.unwrap(), 284);
                sender.await.unwrap();
                let mut corrupted = reply(&expected);
                *corrupted.last_mut().unwrap() ^= 1;
                assert_eq!(
                    read_response(&mut corrupted.as_slice(), &expected)
                        .await
                        .unwrap_err()
                        .kind(),
                    io::ErrorKind::InvalidData
                );
            });
    }

    #[test]
    fn oversized_or_wrong_id_response_is_rejected_before_body_read() {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(async {
                let expected = expected(Instant::now());
                for bad_length in [false, true] {
                    let mut bytes = reply(&expected);
                    bytes.truncate(HEADER_LEN);
                    if bad_length {
                        bytes[8..12].copy_from_slice(&u32::MAX.to_be_bytes());
                    } else {
                        bytes[12] ^= 1;
                    }
                    assert_eq!(
                        read_response(&mut bytes.as_slice(), &expected)
                            .await
                            .unwrap_err()
                            .kind(),
                        io::ErrorKind::InvalidData
                    );
                }
            });
    }

    #[test]
    fn late_reply_keeps_arrival_cohort_and_releases_credit_once() {
        let base = Instant::now();
        let mut stats = vec![
            std::array::from_fn(|_| ClassStats::new(10_000_000)),
            std::array::from_fn(|_| ClassStats::new(10_000_000)),
        ];
        stats[0][0].counters.started = 1;
        stats[0][0].counters.outstanding = 1;
        let mut outstanding = 1;
        let mut bytes = 512;
        process_response(
            Response {
                expected: expected(base),
                started: base,
                completed: base + Duration::from_secs(2),
                result: Ok(284),
            },
            &mut stats,
            &mut outstanding,
            &mut bytes,
            &mut BTreeMap::new(),
        )
        .unwrap();
        assert_eq!((outstanding, bytes), (0, 0));
        assert_eq!(stats[0][0].counters.deadline_misses, 1);
        assert_eq!(stats[0][0].completion_windows[&2].len(), 1);
        assert_eq!(stats[1][0].counters.valid_response, 0);
    }

    #[test]
    fn set_frame_has_the_binary_protocol_layout() {
        let frame = build_request(Class::Overwrite, b"key", b"value", 0x1122_3344);
        assert_eq!(frame.len(), HEADER_LEN + 8 + 3 + 5);
        assert_eq!(frame[0], REQUEST_MAGIC);
        assert_eq!(frame[1], Class::Overwrite.opcode());
        assert_eq!(&frame[2..4], &3_u16.to_be_bytes());
        assert_eq!(frame[4], 8);
        assert_eq!(&frame[8..12], &16_u32.to_be_bytes());
        assert_eq!(&frame[12..16], &0x1122_3344_u32.to_be_bytes());
        assert_eq!(&frame[24..32], &0_u64.to_be_bytes());
        assert_eq!(&frame[32..], b"keyvalue");
    }

    #[test]
    fn phase_sequence_ranges_do_not_overlap() {
        let manifest = Manifest {
            warmup_ms: 10,
            steady_ms: 20,
            overload_ms: 30,
            recovery_ms: 40,
            rate: 1_000,
            overload_rate: 2_000,
            profile: Profile::Overload,
            ..Manifest::default()
        };
        assert_eq!(phase_sequence_base(&manifest, 0), 0);
        assert!(phase_sequence_base(&manifest, 1) > 10);
        assert!(phase_sequence_base(&manifest, 2) > phase_sequence_base(&manifest, 1));
        assert!(phase_sequence_base(&manifest, 3) > phase_sequence_base(&manifest, 2));
    }
}
