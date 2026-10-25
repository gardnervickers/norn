//! Per-thread server CPU accounting.
//!
//! Each snapshot reads, for every thread of the server process:
//! - `/proc/PID/task/TID/schedstat`: nanosecond on-CPU time and run-queue
//!   wait (runnable but not running, e.g. behind another task),
//! - `/proc/PID/task/TID/stat`: user/system tick split,
//! - `/proc/PID/task/TID/status`: voluntary/involuntary context switches,
//! - optional hardware counters (instructions, user instructions, cycles)
//!   via `perf_event_open`, when the host permits it.
//!
//! Threads are rediscovered on every snapshot so io-wq workers created
//! during a run are attributed too.

use std::collections::BTreeMap;
use std::fs;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use serde::Serialize;

#[derive(Debug, Clone, Copy, Default, Serialize)]
pub struct ThreadSample {
    pub cpu_ns: u64,
    pub run_delay_ns: u64,
    pub utime_ticks: u64,
    pub stime_ticks: u64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
    pub instructions: Option<u64>,
    pub instructions_user: Option<u64>,
    pub cycles: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct Snapshot {
    pub threads: BTreeMap<i32, ThreadSample>,
}

/// Aggregated difference between two snapshots.
#[derive(Debug, Clone, Default, Serialize)]
pub struct Delta {
    pub cpu_ns: u64,
    /// Nanoseconds threads were runnable but waiting for a CPU.
    pub run_delay_ns: u64,
    pub user_ns: u64,
    pub system_ns: u64,
    pub voluntary_switches: u64,
    pub involuntary_switches: u64,
    pub instructions: Option<u64>,
    pub instructions_user: Option<u64>,
    pub cycles: Option<u64>,
    /// On-CPU nanoseconds per thread name (io-wq workers are merged).
    pub cpu_ns_by_thread: BTreeMap<String, u64>,
    /// Involuntary context switches (preemptions) per thread name.
    pub involuntary_by_thread: BTreeMap<String, u64>,
    /// Run-queue wait nanoseconds per thread name.
    pub run_delay_by_thread: BTreeMap<String, u64>,
}

struct PerfCounter {
    fd: OwnedFd,
}

#[repr(C)]
#[derive(Default)]
struct PerfEventAttr {
    type_: u32,
    size: u32,
    config: u64,
    sample_period: u64,
    sample_type: u64,
    read_format: u64,
    flags: u64,
    wakeup_events: u32,
    bp_type: u32,
    config1: u64,
    config2: u64,
}

const PERF_TYPE_HARDWARE: u32 = 0;
const PERF_COUNT_HW_CPU_CYCLES: u64 = 0;
const PERF_COUNT_HW_INSTRUCTIONS: u64 = 1;
const PERF_FORMAT_TOTAL_TIME_ENABLED: u64 = 1;
const PERF_FORMAT_TOTAL_TIME_RUNNING: u64 = 2;
const FLAG_EXCLUDE_KERNEL: u64 = 1 << 5;
const FLAG_EXCLUDE_HV: u64 = 1 << 6;
const PERF_FLAG_FD_CLOEXEC: libc::c_ulong = 8;

impl PerfCounter {
    fn open(tid: i32, config: u64, user_only: bool) -> Option<Self> {
        let attr = PerfEventAttr {
            type_: PERF_TYPE_HARDWARE,
            size: std::mem::size_of::<PerfEventAttr>() as u32,
            config,
            read_format: PERF_FORMAT_TOTAL_TIME_ENABLED | PERF_FORMAT_TOTAL_TIME_RUNNING,
            flags: FLAG_EXCLUDE_HV | if user_only { FLAG_EXCLUDE_KERNEL } else { 0 },
            ..Default::default()
        };
        // SAFETY: `attr` is a valid, fully initialized PERF_ATTR_SIZE_VER1
        // structure; the kernel copies it before returning.
        let fd = unsafe {
            libc::syscall(
                libc::SYS_perf_event_open,
                &attr as *const PerfEventAttr,
                tid,
                -1_i32,
                -1_i32,
                PERF_FLAG_FD_CLOEXEC,
            )
        };
        if fd < 0 {
            return None;
        }
        // SAFETY: perf_event_open returned a new descriptor we own.
        Some(Self {
            fd: unsafe { OwnedFd::from_raw_fd(fd as i32) },
        })
    }

    fn read(&self) -> Option<u64> {
        let mut buf = [0_u64; 3];
        // SAFETY: reading into a properly sized local buffer.
        let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), 24) };
        if n != 24 {
            return None;
        }
        let [value, enabled, running] = buf;
        if running == 0 {
            return Some(0);
        }
        // Scale if the kernel multiplexed the counter.
        Some(if running < enabled {
            (u128::from(value) * u128::from(enabled) / u128::from(running)) as u64
        } else {
            value
        })
    }
}

struct ThreadCounters {
    name: String,
    instructions: Option<PerfCounter>,
    instructions_user: Option<PerfCounter>,
    cycles: Option<PerfCounter>,
}

pub struct ProcessCounters {
    pid: i32,
    threads: BTreeMap<i32, ThreadCounters>,
    perf: bool,
    pub perf_available: bool,
    clock_ticks: u64,
}

fn read_u64s(path: &str) -> Option<Vec<u64>> {
    let raw = fs::read_to_string(path).ok()?;
    raw.split_whitespace().map(|v| v.parse().ok()).collect()
}

impl ProcessCounters {
    /// Track every thread of `pid`; `perf` enables hardware counters.
    pub fn new(pid: i32, perf: bool) -> Self {
        // SAFETY: sysconf has no preconditions.
        let ticks = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        let mut counters = Self {
            pid,
            threads: BTreeMap::new(),
            perf,
            perf_available: false,
            clock_ticks: if ticks > 0 { ticks as u64 } else { 100 },
        };
        counters.discover();
        // With perf_event_paranoid=2 only the user-space counter opens.
        counters.perf_available = counters.threads.values().any(|t| {
            t.instructions.is_some() || t.instructions_user.is_some() || t.cycles.is_some()
        });
        counters
    }

    pub fn thread_names(&self) -> Vec<String> {
        self.threads.values().map(|t| t.name.clone()).collect()
    }

    fn discover(&mut self) {
        let Ok(entries) = fs::read_dir(format!("/proc/{}/task", self.pid)) else {
            return;
        };
        for entry in entries.flatten() {
            let Some(tid) = entry
                .file_name()
                .to_str()
                .and_then(|s| s.parse::<i32>().ok())
            else {
                continue;
            };
            if self.threads.contains_key(&tid) {
                continue;
            }
            let name = fs::read_to_string(format!("/proc/{}/task/{tid}/comm", self.pid))
                .map(|s| s.trim().to_owned())
                .unwrap_or_default();
            let open = |config, user_only| {
                self.perf
                    .then(|| PerfCounter::open(tid, config, user_only))
                    .flatten()
            };
            let counters = ThreadCounters {
                name,
                instructions: open(PERF_COUNT_HW_INSTRUCTIONS, false),
                instructions_user: open(PERF_COUNT_HW_INSTRUCTIONS, true),
                cycles: open(PERF_COUNT_HW_CPU_CYCLES, false),
            };
            self.threads.insert(tid, counters);
        }
    }

    pub fn snapshot(&mut self) -> Snapshot {
        self.discover();
        let mut snapshot = Snapshot::default();
        for (&tid, counters) in &self.threads {
            let base = format!("/proc/{}/task/{tid}", self.pid);
            let Some(sched) = read_u64s(&format!("{base}/schedstat")) else {
                continue; // thread exited
            };
            let (utime, stime) = fs::read_to_string(format!("{base}/stat"))
                .ok()
                .and_then(|raw| {
                    // Fields after the parenthesized command name.
                    let rest = &raw[raw.rfind(')')? + 2..];
                    let fields: Vec<&str> = rest.split_whitespace().collect();
                    Some((fields.get(11)?.parse().ok()?, fields.get(12)?.parse().ok()?))
                })
                .unwrap_or((0, 0));
            let (mut voluntary, mut involuntary) = (0, 0);
            if let Ok(status) = fs::read_to_string(format!("{base}/status")) {
                for line in status.lines() {
                    if let Some(v) = line.strip_prefix("voluntary_ctxt_switches:") {
                        voluntary = v.trim().parse().unwrap_or(0);
                    } else if let Some(v) = line.strip_prefix("nonvoluntary_ctxt_switches:") {
                        involuntary = v.trim().parse().unwrap_or(0);
                    }
                }
            }
            snapshot.threads.insert(
                tid,
                ThreadSample {
                    cpu_ns: sched.first().copied().unwrap_or(0),
                    run_delay_ns: sched.get(1).copied().unwrap_or(0),
                    utime_ticks: utime,
                    stime_ticks: stime,
                    voluntary_switches: voluntary,
                    involuntary_switches: involuntary,
                    instructions: counters.instructions.as_ref().and_then(PerfCounter::read),
                    instructions_user: counters
                        .instructions_user
                        .as_ref()
                        .and_then(PerfCounter::read),
                    cycles: counters.cycles.as_ref().and_then(PerfCounter::read),
                },
            );
        }
        snapshot
    }

    pub fn delta(&self, start: &Snapshot, end: &Snapshot) -> Delta {
        let tick_ns = 1_000_000_000 / self.clock_ticks;
        let mut delta = Delta::default();
        let mut instructions = Some(0_u64);
        let mut instructions_user = Some(0_u64);
        let mut cycles = Some(0_u64);
        for (tid, e) in &end.threads {
            // A thread created during the interval started from zero.
            let s = start.threads.get(tid).copied().unwrap_or_default();
            let cpu = e.cpu_ns.saturating_sub(s.cpu_ns);
            delta.cpu_ns += cpu;
            let run_delay = e.run_delay_ns.saturating_sub(s.run_delay_ns);
            delta.run_delay_ns += run_delay;
            delta.user_ns += e.utime_ticks.saturating_sub(s.utime_ticks) * tick_ns;
            delta.system_ns += e.stime_ticks.saturating_sub(s.stime_ticks) * tick_ns;
            delta.voluntary_switches += e.voluntary_switches.saturating_sub(s.voluntary_switches);
            delta.involuntary_switches += e
                .involuntary_switches
                .saturating_sub(s.involuntary_switches);
            let add = |acc: &mut Option<u64>, end: Option<u64>, start: Option<u64>| {
                *acc = match (*acc, end) {
                    (Some(a), Some(e)) => Some(a + e.saturating_sub(start.unwrap_or(0))),
                    _ => None,
                };
            };
            // Threads with negligible CPU but no counter (e.g. exited before
            // discovery) should not void the aggregate.
            if e.instructions_user.is_some() || cpu > 1_000_000 {
                add(&mut instructions, e.instructions, s.instructions);
                add(
                    &mut instructions_user,
                    e.instructions_user,
                    s.instructions_user,
                );
                add(&mut cycles, e.cycles, s.cycles);
            }
            let name = self
                .threads
                .get(tid)
                .map(|t| {
                    if t.name.starts_with("iou-wrk") {
                        "iou-wrk".to_owned()
                    } else {
                        t.name.clone()
                    }
                })
                .unwrap_or_default();
            *delta.involuntary_by_thread.entry(name.clone()).or_default() += e
                .involuntary_switches
                .saturating_sub(s.involuntary_switches);
            *delta.run_delay_by_thread.entry(name.clone()).or_default() += run_delay;
            *delta.cpu_ns_by_thread.entry(name).or_default() += cpu;
        }
        delta.instructions = instructions.filter(|_| self.perf_available);
        delta.instructions_user = instructions_user.filter(|_| self.perf_available);
        delta.cycles = cycles.filter(|_| self.perf_available);
        delta
    }
}

/// Jiffies of one CPU from `/proc/stat`.
#[derive(Debug, Clone, Copy, Default)]
pub struct CpuJiffies {
    /// Everything except idle and iowait.
    pub busy: u64,
    /// Hard and soft interrupt time, which is not charged to any thread.
    pub irq: u64,
    pub total: u64,
}

/// Jiffies per CPU from `/proc/stat`.
pub fn cpu_jiffies() -> BTreeMap<usize, CpuJiffies> {
    let mut out = BTreeMap::new();
    let Ok(raw) = fs::read_to_string("/proc/stat") else {
        return out;
    };
    for line in raw.lines() {
        let mut fields = line.split_whitespace();
        let Some(cpu) = fields
            .next()
            .and_then(|n| n.strip_prefix("cpu"))
            .and_then(|n| n.parse::<usize>().ok())
        else {
            continue;
        };
        // user nice system idle iowait irq softirq steal; guest time is
        // already included in user.
        let v: Vec<u64> = fields.take(8).filter_map(|f| f.parse().ok()).collect();
        if v.len() == 8 {
            out.insert(
                cpu,
                CpuJiffies {
                    busy: v[0] + v[1] + v[2] + v[5] + v[6] + v[7],
                    irq: v[5] + v[6],
                    total: v.iter().sum(),
                },
            );
        }
    }
    out
}

/// Per-CPU fractions of `field` between two [`cpu_jiffies`] snapshots.
pub fn fractions(
    cpus: &[usize],
    start: &BTreeMap<usize, CpuJiffies>,
    end: &BTreeMap<usize, CpuJiffies>,
    field: fn(&CpuJiffies) -> u64,
) -> Vec<f64> {
    cpus.iter()
        .filter_map(|cpu| {
            let (s, e) = (start.get(cpu)?, end.get(cpu)?);
            let total = e.total.saturating_sub(s.total);
            (total > 0).then(|| field(e).saturating_sub(field(s)) as f64 / total as f64)
        })
        .collect()
}
