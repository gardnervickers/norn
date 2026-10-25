//! Result assembly, qualification, text summaries, and paired comparison.

use std::collections::BTreeMap;
use std::fmt::Write as _;

use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

use crate::client::{Mode, SegStats, Segment, COMPLETION_TICK_NS};
use crate::counters::Delta;
use crate::profile::Profile;

pub const RESULT_VERSION: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Quantiles {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: f64,
    pub p90_us: f64,
    pub p99_us: f64,
    pub p999_us: f64,
    pub max_us: f64,
}

impl Quantiles {
    pub fn of(h: &Histogram<u64>) -> Self {
        let us = |ns: u64| ns as f64 / 1000.0;
        if h.is_empty() {
            return Self::default();
        }
        Self {
            count: h.len(),
            mean_us: h.mean() / 1000.0,
            p50_us: us(h.value_at_quantile(0.5)),
            p90_us: us(h.value_at_quantile(0.9)),
            p99_us: us(h.value_at_quantile(0.99)),
            p999_us: us(h.value_at_quantile(0.999)),
            max_us: us(h.max()),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerSegment {
    pub cpu_ns: u64,
    /// Server CPU time / (window × server workers).
    pub utilization: f64,
    pub user_ns: u64,
    pub system_ns: u64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
    pub instructions: Option<u64>,
    pub instructions_user: Option<u64>,
    pub cycles: Option<u64>,
    pub cpu_ns_per_op: f64,
    pub instructions_per_op: Option<f64>,
    pub instructions_user_per_op: Option<f64>,
    pub cycles_per_op: Option<f64>,
    pub ipc: Option<f64>,
    pub switches_per_op: f64,
    /// Involuntary context switches per second across server threads.
    pub preemptions_per_s: f64,
    /// Run-queue wait across server threads.
    #[serde(default)]
    pub run_delay_ns: u64,
    /// Largest fraction of the window any server thread spent runnable but
    /// waiting for a CPU.
    #[serde(default)]
    pub run_delay_fraction: f64,
    pub cpu_ns_by_thread: BTreeMap<String, u64>,
    /// Server threads started or exited within the window; their counts
    /// are incomplete.
    #[serde(default)]
    pub thread_changes: u32,
    /// Smallest fraction of enabled time a perf counter was counting.
    #[serde(default)]
    pub perf_coverage: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClassResult {
    pub name: String,
    pub completed: u64,
    pub latency: Quantiles,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SegmentResult {
    pub name: String,
    pub mode: String,
    pub offered_rate: Option<u64>,
    pub closed_loop_depth: Option<usize>,
    pub window_s: f64,
    pub planned: u64,
    pub dropped: u64,
    /// Responses whose completion time fell in the nominal window.
    pub completed: u64,
    /// Responses completed within the coordinator's actual sampling
    /// interval; the denominator of every per-request server metric.
    #[serde(default)]
    pub sampled_completed: Option<u64>,
    /// Largest distance between a sampling instant and its nominal boundary.
    #[serde(default)]
    pub sample_skew_us: Option<f64>,
    /// Completions per second over the sampling interval (nominal window
    /// without server sampling).
    pub throughput: f64,
    pub churn_sessions: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub arrival_lag: Quantiles,
    /// All persistent-connection request classes merged.
    pub latency: Quantiles,
    pub classes: Vec<ClassResult>,
    pub server: Option<ServerSegment>,
    /// Involuntary context switches of client threads in the window.
    pub client_preemptions: Option<u64>,
    /// Largest fraction of the window any client thread spent runnable but
    /// waiting for a CPU.
    #[serde(default)]
    pub client_run_delay_fraction: Option<f64>,
    /// Largest busy fraction of an unused SMT sibling of a server CPU, from
    /// `/proc/stat`. Other work there shares the server core.
    #[serde(default)]
    pub server_sibling_busy: Option<f64>,
    /// Mean busy fraction of the server CPUs from `/proc/stat`. Unlike
    /// `server.utilization`, it includes interrupt time. On loopback the
    /// softirq work that delivers the server's sends runs on the server's
    /// CPU but is charged to no thread.
    #[serde(default)]
    pub server_cpu_busy: Option<f64>,
    /// Mean hard and soft interrupt fraction of the server CPUs.
    #[serde(default)]
    pub server_cpu_irq: Option<f64>,
    /// No qualification warning applies to this segment (or to the run).
    pub qualified: bool,
    /// Short reason codes, e.g. `server-preempted`.
    pub reasons: Vec<String>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Provenance {
    pub timestamp: String,
    pub host: String,
    pub kernel: String,
    pub cpu_model: String,
    pub governor: String,
    pub boost: String,
    pub git_revision: String,
    pub git_dirty: bool,
    pub server_command: Vec<String>,
    pub server_sha256: String,
    pub server_threads: Vec<String>,
    pub server_cpus: Vec<usize>,
    pub client_cpus: Vec<usize>,
    #[serde(default)]
    pub hold_cpus: Vec<usize>,
    pub client_sha256: String,
    pub perf_counters: bool,
    /// The coordinator's cgroup; the server and client threads inherit it.
    #[serde(default)]
    pub cgroup: String,
    /// `/proc/loadavg` when the run started.
    #[serde(default)]
    pub load_average: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunResult {
    pub result_version: u32,
    pub label: String,
    pub profile: Profile,
    pub provenance: Provenance,
    /// Every response validated, all work drained, and final readback passed.
    pub valid: bool,
    pub error: Option<String>,
    /// Valid and free of qualification warnings.
    pub qualified: bool,
    pub warnings: Vec<String>,
    pub populate_s: f64,
    pub drain_s: f64,
    pub verified_slots: u64,
    pub segments: Vec<SegmentResult>,
}

/// Coordinator measurements of one segment window.
pub struct Sample {
    /// Actual sampling instants relative to the shared start. Cost per
    /// request divides counters over this interval by completions in it.
    pub window_ns: (u64, u64),
    /// Server counter delta and the actual sampled wall interval.
    pub server: Option<(Delta, u64)>,
    pub client_preemptions: u64,
    /// Largest run-queue wait of one client thread, in nanoseconds.
    pub client_max_run_delay_ns: u64,
    /// Largest busy fraction of an unused SMT sibling of a server CPU.
    pub server_sibling_busy: Option<f64>,
    /// Mean busy fraction of the server CPUs, including interrupt time.
    pub server_cpu_busy: Option<f64>,
    /// Mean interrupt fraction of the server CPUs.
    pub server_cpu_irq: Option<f64>,
}

pub fn build_segments(
    profile: &Profile,
    segments: &[Segment],
    stats: &[SegStats],
    samples: &[Option<Sample>],
    completions: &[u32],
) -> Vec<SegmentResult> {
    let names = profile.class_names();
    let churn = names.len() - 1;
    segments
        .iter()
        .zip(stats)
        .zip(samples)
        .filter(|((s, _), _)| s.measured)
        .map(|((s, st), sample)| {
            let srv = sample.as_ref().and_then(|v| v.server.as_ref());
            let client_preemptions = sample.as_ref().map(|v| v.client_preemptions);
            let window_ns = s.end_ns - s.measure_start_ns;
            let client_run_delay_fraction = sample
                .as_ref()
                .map(|v| v.client_max_run_delay_ns as f64 / window_ns as f64);
            let window_s = window_ns as f64 / 1e9;
            let mut all = crate::client::new_histogram();
            for h in &st.latency[..churn] {
                all.add(h).unwrap();
            }
            let classes = names
                .iter()
                .enumerate()
                .map(|(i, name)| ClassResult {
                    name: name.clone(),
                    completed: st.completed_by_class[i],
                    latency: Quantiles::of(&st.latency[i]),
                })
                .collect();
            // Completions over the coordinator's actual sampling interval,
            // which can differ from the nominal window by its wakeup delay.
            let sampled = sample.as_ref().map(|v| {
                let (t0, t1) = v.window_ns;
                let ticks = |t: u64| ((t / COMPLETION_TICK_NS) as usize).min(completions.len());
                let count: u64 = completions[ticks(t0)..ticks(t1)]
                    .iter()
                    .map(|&c| u64::from(c))
                    .sum();
                let skew = t0.abs_diff(s.measure_start_ns).max(t1.abs_diff(s.end_ns));
                (count, t1 - t0, skew)
            });
            let ops = sampled.map_or(st.completed, |(count, _, _)| count).max(1) as f64;
            let server = srv.map(|(d, wall_ns)| {
                let per = |v: Option<u64>| v.map(|v| v as f64 / ops);
                ServerSegment {
                    cpu_ns: d.cpu_ns,
                    utilization: d.cpu_ns as f64
                        / (*wall_ns as f64 * profile.server_workers as f64),
                    user_ns: d.user_ns,
                    system_ns: d.system_ns,
                    voluntary_switches: d.voluntary_switches,
                    involuntary_switches: d.involuntary_switches,
                    instructions: d.instructions,
                    instructions_user: d.instructions_user,
                    cycles: d.cycles,
                    cpu_ns_per_op: d.cpu_ns as f64 / ops,
                    instructions_per_op: per(d.instructions),
                    instructions_user_per_op: per(d.instructions_user),
                    cycles_per_op: per(d.cycles),
                    ipc: match (d.instructions, d.cycles) {
                        (Some(i), Some(c)) if c > 0 => Some(i as f64 / c as f64),
                        _ => None,
                    },
                    switches_per_op: (d.voluntary_switches + d.involuntary_switches) as f64 / ops,
                    preemptions_per_s: d.involuntary_switches as f64 / (*wall_ns as f64 / 1e9),
                    run_delay_ns: d.run_delay_ns,
                    run_delay_fraction: d.run_delay_by_thread.values().copied().max().unwrap_or(0)
                        as f64
                        / *wall_ns as f64,
                    cpu_ns_by_thread: d.cpu_ns_by_thread.clone(),
                    thread_changes: d.thread_changes,
                    perf_coverage: d.perf_coverage,
                }
            });
            let (mode, offered_rate, closed_loop_depth) = match s.mode {
                Mode::Closed { depth } => ("closed".to_owned(), None, Some(depth)),
                Mode::Open { rate } => ("open".to_owned(), Some(rate), None),
            };
            SegmentResult {
                name: s.name.clone(),
                mode,
                offered_rate,
                closed_loop_depth,
                window_s,
                planned: st.planned,
                dropped: st.dropped,
                completed: st.completed,
                sampled_completed: sampled.map(|(count, _, _)| count),
                sample_skew_us: sampled.map(|(_, _, skew)| skew as f64 / 1e3),
                throughput: sampled.map_or(st.completed as f64 / window_s, |(count, wall, _)| {
                    count as f64 / (wall as f64 / 1e9)
                }),
                churn_sessions: st.churn_sessions,
                rx_bytes: st.rx_bytes,
                tx_bytes: st.tx_bytes,
                arrival_lag: Quantiles::of(&st.lag),
                latency: Quantiles::of(&all),
                classes,
                server,
                client_preemptions,
                client_run_delay_fraction,
                server_sibling_busy: sample.as_ref().and_then(|v| v.server_sibling_busy),
                server_cpu_busy: sample.as_ref().and_then(|v| v.server_cpu_busy),
                server_cpu_irq: sample.as_ref().and_then(|v| v.server_cpu_irq),
                qualified: true,
                reasons: Vec::new(),
                warnings: Vec::new(),
            }
        })
        .collect()
}

/// Attach qualification warnings to each segment. `run_warnings` (e.g.
/// placement) disqualify every segment. Returns all warnings.
pub fn qualify(
    profile: &Profile,
    segments: &mut [SegmentResult],
    run_warnings: &[String],
) -> Vec<String> {
    let mut all = run_warnings.to_vec();
    for s in segments.iter_mut() {
        let found = segment_warnings(profile, s);
        s.qualified = run_warnings.is_empty() && found.is_empty();
        s.reasons = found.iter().map(|(r, _)| (*r).to_owned()).collect();
        if !run_warnings.is_empty() {
            s.reasons.push("run-placement".into());
        }
        s.warnings = found.into_iter().map(|(_, w)| w).collect();
        all.extend(s.warnings.iter().cloned());
    }
    all
}

/// Largest sampling-boundary skew, as a fraction of the window. Completions
/// are counted over the actual interval, but a late boundary shifts the
/// window into the neighbouring segment's load.
const MAX_SAMPLE_SKEW: f64 = 0.005;
/// Smallest acceptable perf counter coverage before scaling estimates
/// dominate the counts.
const MIN_PERF_COVERAGE: f64 = 0.99;

/// Collects `(reason code, message)` pairs.
struct Warnings(Vec<(&'static str, String)>);

impl Warnings {
    fn push(&mut self, entry: (&'static str, String)) {
        self.0.push(entry);
    }
}

fn segment_warnings(profile: &Profile, s: &SegmentResult) -> Vec<(&'static str, String)> {
    let q = &profile.qualification;
    let mut warnings = Warnings(Vec::new());
    {
        if let Some(server) = &s.server {
            if server.preemptions_per_s > q.max_preemptions_per_s {
                warnings.push((
                    "server-preempted",
                    format!(
                        "{}: server threads preempted {:.0}/s; host is not quiet",
                        s.name, server.preemptions_per_s
                    ),
                ));
            }
        }
        if let Some(skew) = s
            .sample_skew_us
            .filter(|skew| skew * 1e3 > MAX_SAMPLE_SKEW * s.window_s * 1e9)
        {
            warnings.push((
                "sample-skew",
                format!(
                    "{}: the coordinator sampled {skew:.0}us away from a window boundary",
                    s.name
                ),
            ));
        }
        if let Some(server) = &s.server {
            if server.thread_changes > 0 {
                warnings.push((
                    "thread-churn",
                    format!(
                        "{}: {} server threads started or exited; their counts are incomplete",
                        s.name, server.thread_changes
                    ),
                ));
            }
            if let Some(coverage) = server.perf_coverage.filter(|c| *c < MIN_PERF_COVERAGE) {
                warnings.push((
                    "perf-multiplexed",
                    format!(
                        "{}: perf counters ran {:.1}% of the time; another perf user is multiplexing them",
                        s.name,
                        coverage * 100.0
                    ),
                ));
            }
        }
        let server_delayed = s
            .server
            .as_ref()
            .is_some_and(|v| v.run_delay_fraction > q.max_run_delay_fraction);
        if let Some(server) = s.server.as_ref().filter(|_| server_delayed) {
            warnings.push((
                "server-delayed",
                format!(
                    "{}: a server thread waited {:.1}% of the window for a CPU",
                    s.name,
                    server.run_delay_fraction * 100.0
                ),
            ));
        }
        if let Some(f) = s
            .client_run_delay_fraction
            .filter(|f| *f > q.max_run_delay_fraction)
        {
            warnings.push((
                "client-delayed",
                format!(
                    "{}: a client thread waited {:.1}% of the window for a CPU",
                    s.name,
                    f * 100.0
                ),
            ));
        }
        if let Some(f) = s
            .server_sibling_busy
            .filter(|f| *f > q.max_sibling_busy_fraction)
        {
            warnings.push((
                "smt-contention",
                format!(
                    "{}: an SMT sibling of a server CPU was {:.0}% busy",
                    s.name,
                    f * 100.0
                ),
            ));
        }
        if let Some(n) = s.client_preemptions {
            if n as f64 / s.window_s > q.max_preemptions_per_s {
                warnings.push((
                    "client-preempted",
                    format!(
                        "{}: client threads preempted {n} times; host is not quiet",
                        s.name
                    ),
                ));
            }
        }
        match s.offered_rate {
            None => {
                // Prefer the pinned server CPUs' busy time: thread CPU time
                // omits the interrupt work the server causes on its CPU. A
                // delayed server is already reported; its low utilization
                // does not imply a client bottleneck.
                let busy = s
                    .server_cpu_busy
                    .or_else(|| s.server.as_ref().map(|v| v.utilization));
                if let Some(busy) = busy.filter(|_| !server_delayed) {
                    if busy < q.min_capacity_utilization {
                        warnings.push((
                            "client-bound",
                            format!(
                                "{}: server CPUs {:.1}% busy < {:.0}%; the client may be the bottleneck",
                                s.name,
                                busy * 100.0,
                                q.min_capacity_utilization * 100.0
                            ),
                        ));
                    }
                }
            }
            Some(rate) => {
                if s.dropped > 0 {
                    warnings.push((
                        "over-capacity",
                        format!(
                            "{}: {} arrivals dropped at the per-connection cap; offered rate exceeds capacity",
                            s.name, s.dropped
                        ),
                    ));
                }
                if s.arrival_lag.p99_us > q.max_lag_p99_us {
                    warnings.push((
                        "arrival-lag",
                        format!(
                            "{}: client arrival lag p99 {:.1}us > {:.1}us",
                            s.name, s.arrival_lag.p99_us, q.max_lag_p99_us
                        ),
                    ));
                }
                // Compare with realized arrivals rather than the nominal
                // rate: bursty Poisson arrivals vary around the mean.
                let achieved = s.completed as f64 / s.planned.max(1) as f64;
                if !(0.98..=1.02).contains(&achieved) {
                    warnings.push((
                        "completion-shortfall",
                        format!(
                            "{}: completed {} of {} arrivals ({:.1}%) at offered {rate}/s",
                            s.name,
                            s.completed,
                            s.planned,
                            achieved * 100.0
                        ),
                    ));
                }
            }
        }
    }
    warnings.0
}

fn si(v: f64) -> String {
    if v >= 1e6 {
        format!("{:.2}M", v / 1e6)
    } else if v >= 1e4 {
        format!("{:.1}k", v / 1e3)
    } else if v >= 100.0 {
        format!("{v:.0}")
    } else {
        format!("{v:.2}")
    }
}

fn opt(v: Option<f64>, f: impl Fn(f64) -> String) -> String {
    v.map(f).unwrap_or_else(|| "-".into())
}

pub fn summary(result: &RunResult) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} [{}] valid={} qualified={}{}",
        result.profile.name,
        result.label,
        result.valid,
        result.qualified,
        result
            .error
            .as_ref()
            .map(|e| format!(" error={e}"))
            .unwrap_or_default()
    );
    let _ = writeln!(
        out,
        "{:<12} {:>9} {:>9} {:>6} {:>6} {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>5} {:>7} {:>8} {:>8} {:>8} {:>7}",
        "segment",
        "offered",
        "done/s",
        "util",
        "wait",
        "core",
        "irq",
        "cpu/op",
        "inst/op",
        "uinst/op",
        "cyc/op",
        "ipc",
        "csw/op",
        "p50",
        "p99",
        "p99.9",
        "lagmax"
    );
    for s in &result.segments {
        let offered = s
            .offered_rate
            .map(|r| si(r as f64))
            .unwrap_or_else(|| format!("depth{}", s.closed_loop_depth.unwrap_or(0)));
        let srv = s.server.as_ref();
        let _ = writeln!(
            out,
            "{:<12} {:>9} {:>9} {:>6} {:>6} {:>6} {:>6} {:>8} {:>8} {:>8} {:>8} {:>5} {:>7} {:>8} {:>8} {:>8} {:>7}",
            s.name,
            offered,
            si(s.throughput),
            opt(srv.map(|v| v.utilization), |u| format!("{:.1}%", u * 100.0)),
            opt(srv.map(|v| v.run_delay_fraction), |u| format!("{:.1}%", u * 100.0)),
            opt(s.server_cpu_busy, |u| format!("{:.1}%", u * 100.0)),
            opt(s.server_cpu_irq, |u| format!("{:.1}%", u * 100.0)),
            opt(srv.map(|v| v.cpu_ns_per_op), |v| format!("{v:.0}ns")),
            opt(srv.and_then(|v| v.instructions_per_op), si),
            opt(srv.and_then(|v| v.instructions_user_per_op), si),
            opt(srv.and_then(|v| v.cycles_per_op), si),
            opt(srv.and_then(|v| v.ipc), |v| format!("{v:.2}")),
            opt(srv.map(|v| v.switches_per_op), |v| format!("{v:.3}")),
            format!("{:.1}", s.latency.p50_us),
            format!("{:.1}", s.latency.p99_us),
            format!("{:.1}", s.latency.p999_us),
            format!("{:.1}", s.arrival_lag.max_us),
        );
    }
    for s in result.segments.iter().filter(|s| s.offered_rate.is_some()) {
        let _ = writeln!(
            out,
            "  {} latency by class (us, from intended arrival):",
            s.name
        );
        for c in &s.classes {
            if c.latency.count == 0 {
                continue;
            }
            let _ = writeln!(
                out,
                "    {:<12} n={:<8} p50={:<8.1} p99={:<8.1} p99.9={:<8.1} max={:.1}",
                c.name,
                c.latency.count,
                c.latency.p50_us,
                c.latency.p99_us,
                c.latency.p999_us,
                c.latency.max_us
            );
        }
    }
    for w in &result.warnings {
        let _ = writeln!(out, "warning: {w}");
    }
    out
}

type Extract = Box<dyn Fn(&RunResult) -> Option<f64>>;

/// One comparable scalar extracted from a run.
struct Metric {
    name: String,
    segment: usize,
    /// Whether larger values are better.
    higher_is_better: bool,
    extract: Extract,
}

fn metrics(template: &RunResult) -> Vec<Metric> {
    let mut list = Vec::new();
    for (i, s) in template.segments.iter().enumerate() {
        let closed = s.offered_rate.is_none();
        let mut push = |name: &str, higher_is_better: bool, extract: Extract| {
            list.push(Metric {
                name: format!("{} {name}", s.name),
                segment: i,
                higher_is_better,
                extract,
            });
        };
        let server = move |f: fn(&ServerSegment) -> Option<f64>| -> Extract {
            Box::new(move |r: &RunResult| {
                r.segments
                    .get(i)
                    .and_then(|s| s.server.as_ref())
                    .and_then(f)
            })
        };
        let segment = move |f: fn(&SegmentResult) -> f64| -> Extract {
            Box::new(move |r: &RunResult| r.segments.get(i).map(f))
        };
        if closed {
            push("throughput", true, segment(|s| s.throughput));
        }
        push(
            "user_instructions/op",
            false,
            server(|v| v.instructions_user_per_op),
        );
        push("instructions/op", false, server(|v| v.instructions_per_op));
        push("cpu_ns/op", false, server(|v| Some(v.cpu_ns_per_op)));
        push("cycles/op", false, server(|v| v.cycles_per_op));
        push("switches/op", false, server(|v| Some(v.switches_per_op)));
        if !closed {
            push("p50_us", false, segment(|s| s.latency.p50_us));
            push("p99_us", false, segment(|s| s.latency.p99_us));
        }
    }
    list
}

fn median(values: &mut [f64]) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = values.len();
    if n % 2 == 1 {
        values[n / 2]
    } else {
        (values[n / 2 - 1] + values[n / 2]) / 2.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricComparison {
    pub metric: String,
    pub baseline_median: f64,
    pub candidate_median: f64,
    /// Median of per-pair (candidate / baseline - 1).
    pub delta_median: f64,
    pub delta_min: f64,
    pub delta_max: f64,
    /// Larger of the two builds' (max - min) / median across runs.
    pub spread: f64,
    /// Pairs in which both runs reported the metric (with a nonzero
    /// baseline); every statistic above uses only these.
    pub pairs_used: usize,
    pub verdict: String,
}

/// Compare paired runs. `pairs[i] = (baseline, candidate)`.
pub fn compare(pairs: &[(RunResult, RunResult)]) -> Vec<MetricComparison> {
    let Some((template, _)) = pairs.first() else {
        return Vec::new();
    };
    let mut out = Vec::new();
    for metric in metrics(template) {
        let mut base = Vec::new();
        let mut cand = Vec::new();
        let mut deltas = Vec::new();
        for (b, c) in pairs {
            match ((metric.extract)(b), (metric.extract)(c)) {
                (Some(b), Some(c)) if b != 0.0 => {
                    base.push(b);
                    cand.push(c);
                    deltas.push(c / b - 1.0);
                }
                _ => {}
            }
        }
        if deltas.is_empty() {
            continue;
        }
        let spread = |v: &mut Vec<f64>| {
            let m = median(v);
            if m == 0.0 {
                return 0.0;
            }
            (v.last().unwrap() - v.first().unwrap()) / m
        };
        let base_median = median(&mut base);
        let cand_median = median(&mut cand);
        let within = spread(&mut base).max(spread(&mut cand));
        let delta_median = median(&mut deltas);
        let (lo, hi) = (deltas[0], *deltas.last().unwrap());
        let consistent = lo > 0.0 || hi < 0.0;
        let better = (delta_median > 0.0) == metric.higher_is_better;
        let qualified = pairs.iter().all(|(b, c)| {
            [b, c]
                .iter()
                .all(|r| r.segments.get(metric.segment).is_some_and(|s| s.qualified))
        });
        let indicated = if consistent && delta_median.abs() > within / 2.0 {
            if better {
                "better"
            } else {
                "worse"
            }
        } else {
            "noise"
        };
        // An unqualified verdict still shows what the data indicated, as a
        // hint for whether a qualified rerun is worthwhile.
        let verdict = if deltas.len() < 3 {
            "too few pairs".to_owned()
        } else if !qualified {
            format!("unqualified/{indicated}")
        } else {
            indicated.to_owned()
        };
        out.push(MetricComparison {
            metric: metric.name,
            baseline_median: base_median,
            candidate_median: cand_median,
            delta_median,
            delta_min: lo,
            delta_max: hi,
            spread: within,
            pairs_used: deltas.len(),
            verdict,
        });
    }
    out
}

pub fn comparison_table(rows: &[MetricComparison], pairs: usize) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{pairs} interleaved pairs; delta = median of per-pair candidate/baseline - 1; \
         `unqualified/X` = some run failed qualification for this segment; X is only a hint; n = pairs with the metric"
    );
    let _ = writeln!(
        out,
        "{:<34} {:>11} {:>11} {:>8} {:>18} {:>7} {:>2}  verdict",
        "metric", "baseline", "candidate", "delta", "pair range", "spread", "n"
    );
    for r in rows {
        let _ = writeln!(
            out,
            "{:<34} {:>11} {:>11} {:>+7.2}% {:>+8.2}%..{:>+6.2}% {:>6.2}% {:>2}  {}",
            r.metric,
            si(r.baseline_median),
            si(r.candidate_median),
            r.delta_median * 100.0,
            r.delta_min * 100.0,
            r.delta_max * 100.0,
            r.spread * 100.0,
            r.pairs_used,
            r.verdict
        );
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile() -> Profile {
        Profile::load(concat!(env!("CARGO_MANIFEST_DIR"), "/profiles/smoke.json")).unwrap()
    }

    /// A run whose every segment sampled `window` (relative to the shared
    /// start, per segment) and saw `instructions` user instructions, with
    /// one completion per tick over the whole timeline.
    fn run(
        instructions: Option<u64>,
        window: impl Fn(&Segment) -> (u64, u64),
    ) -> (RunResult, Vec<SegmentResult>) {
        let profile = profile();
        let timeline = crate::client::timeline(&profile);
        let mut stats: Vec<_> = timeline
            .iter()
            .map(|_| SegStats::empty(profile.class_names().len()))
            .collect();
        for (st, s) in stats.iter_mut().zip(&timeline) {
            st.completed = (s.end_ns - s.measure_start_ns) / COMPLETION_TICK_NS;
            st.planned = st.completed;
        }
        let end = timeline.last().unwrap().end_ns;
        let completions = vec![1_u32; (2 * end / COMPLETION_TICK_NS) as usize];
        let samples: Vec<_> = timeline
            .iter()
            .map(|s| {
                let (t0, t1) = window(s);
                Some(Sample {
                    window_ns: (t0, t1),
                    server: Some((
                        Delta {
                            instructions_user: instructions,
                            cpu_ns: t1 - t0,
                            ..Default::default()
                        },
                        t1 - t0,
                    )),
                    client_preemptions: 0,
                    client_max_run_delay_ns: 0,
                    server_sibling_busy: Some(0.0),
                    server_cpu_busy: Some(1.0),
                    server_cpu_irq: Some(0.0),
                })
            })
            .collect();
        let mut segments = build_segments(&profile, &timeline, &stats, &samples, &completions);
        qualify(&profile, &mut segments, &[]);
        let result = RunResult {
            result_version: RESULT_VERSION,
            label: "test".into(),
            profile,
            provenance: Provenance::default(),
            valid: true,
            error: None,
            qualified: true,
            warnings: vec![],
            populate_s: 0.0,
            drain_s: 0.0,
            verified_slots: 1,
            segments: segments.clone(),
        };
        (result, segments)
    }

    fn nominal(s: &Segment) -> (u64, u64) {
        (s.measure_start_ns, s.end_ns)
    }

    #[test]
    fn per_request_metrics_use_completions_in_the_sampled_interval() {
        // The coordinator woke late and sampled twice the nominal window.
        let (_, segments) = run(Some(1_000_000), |s| {
            (s.measure_start_ns, 2 * s.end_ns - s.measure_start_ns)
        });
        let s = &segments[0];
        let ticks = 2 * (s.window_s * 1e9) as u64 / COMPLETION_TICK_NS;
        assert_eq!(s.sampled_completed, Some(ticks));
        let per_op = s.server.as_ref().unwrap().instructions_user_per_op.unwrap();
        assert!((per_op - 1e6 / ticks as f64).abs() < 1e-9, "{per_op}");
        assert!(
            s.reasons.iter().any(|r| r == "sample-skew"),
            "{:?}",
            s.reasons
        );

        let (_, segments) = run(Some(1_000_000), nominal);
        assert!(segments[0].qualified, "{:?}", segments[0].warnings);
    }

    #[test]
    fn verdict_requires_three_pairs_with_the_metric() {
        let pairs = vec![
            (run(Some(100_000), nominal).0, run(Some(90_000), nominal).0),
            (run(None, nominal).0, run(None, nominal).0),
            (run(None, nominal).0, run(None, nominal).0),
        ];
        let rows = compare(&pairs);
        let row = rows
            .iter()
            .find(|r| r.metric == "capacity user_instructions/op")
            .unwrap();
        assert_eq!((row.pairs_used, row.verdict.as_str()), (1, "too few pairs"));

        let pairs: Vec<_> = (0..3)
            .map(|_| (run(Some(100_000), nominal).0, run(Some(90_000), nominal).0))
            .collect();
        let rows = compare(&pairs);
        let row = rows
            .iter()
            .find(|r| r.metric == "capacity user_instructions/op")
            .unwrap();
        assert_eq!((row.pairs_used, row.verdict.as_str()), (3, "better"));
    }
}
