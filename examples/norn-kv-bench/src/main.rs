//! Mixed-workload benchmark driver for `norn-kv-server`.
//!
//! See `README.md` in this crate for methodology and usage.

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

#[cfg(target_os = "linux")]
mod client;
#[cfg(target_os = "linux")]
mod counters;
mod profile;
mod proto;
#[cfg(target_os = "linux")]
mod report;
#[cfg(target_os = "linux")]
mod server;
mod workload;

#[cfg(target_os = "linux")]
fn main() {
    if let Err(error) = linux::main() {
        eprintln!("norn-kv-bench: {error}");
        std::process::exit(1);
    }
}

#[cfg(not(target_os = "linux"))]
fn main() {
    eprintln!("norn-kv-bench requires Linux");
    std::process::exit(1);
}

#[cfg(target_os = "linux")]
mod linux {
    use std::fs;
    use std::net::SocketAddr;
    use std::path::{Path, PathBuf};
    use std::process::Command;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Barrier, Mutex};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    use crate::client::{self, SegStats, Shared};
    use crate::counters::{self, ProcessCounters};
    use crate::profile::Profile;
    use crate::report::{self, Provenance, RunResult, RESULT_VERSION};
    use crate::server::{self, Kind, Spec};
    use crate::workload::Workload;

    const USAGE: &str = "\
Usage:
  norn-kv-bench run --profile PATH [--server PATH | --address HOST:PORT]
                    [--server-kind norn|generic] [--server-arg ARG]...
                    [--server-cpus LIST] [--client-cpus LIST] [--hold-cpus LIST]
                    [--runs N] [--label NAME] [--out DIR]
  norn-kv-bench compare --profile PATH --baseline PATH --candidate PATH
                    [--pairs N] [--server-kind norn|generic] [--server-arg ARG]...
                    [--server-cpus LIST] [--client-cpus LIST] [--hold-cpus LIST]
                    [--out DIR]

CPU lists accept ranges, e.g. 0-3,8. `--server-cpus` must name one CPU per
server worker for norn-kv-server. `--hold-cpus` occupies CPUs, typically the
SMT siblings of the server CPUs on a shared host, with a `pause` loop so other
work cannot run there. Generic servers substitute {port} and
{workers} in --server-arg values, e.g. for memcached:
  --server-kind generic --server \"$(command -v memcached)\" \\
  --server-arg=-p --server-arg={port} --server-arg=-t --server-arg={workers} \\
  --server-arg=-m --server-arg=4096";

    #[derive(Debug, Default)]
    struct Args {
        command: String,
        profile: Option<String>,
        server: Option<PathBuf>,
        baseline: Option<PathBuf>,
        candidate: Option<PathBuf>,
        address: Option<SocketAddr>,
        kind: Option<Kind>,
        server_args: Vec<String>,
        server_cpus: Vec<usize>,
        client_cpus: Vec<usize>,
        hold_cpus: Vec<usize>,
        runs: usize,
        label: Option<String>,
        out: Option<PathBuf>,
    }

    fn parse_cpus(raw: &str) -> Result<Vec<usize>, String> {
        let mut cpus = Vec::new();
        for part in raw.split(',').filter(|p| !p.is_empty()) {
            let bad = || format!("invalid CPU list '{raw}'");
            match part.split_once('-') {
                Some((a, b)) => {
                    let (a, b): (usize, usize) =
                        (a.parse().map_err(|_| bad())?, b.parse().map_err(|_| bad())?);
                    if a > b {
                        return Err(bad());
                    }
                    cpus.extend(a..=b);
                }
                None => cpus.push(part.parse().map_err(|_| bad())?),
            }
        }
        let mut sorted = cpus.clone();
        sorted.sort_unstable();
        sorted.dedup();
        if sorted.len() != cpus.len() {
            return Err(format!("CPU list '{raw}' repeats a CPU"));
        }
        Ok(cpus)
    }

    fn parse_args() -> Result<Args, String> {
        let mut raw = std::env::args().skip(1);
        let mut args = Args {
            command: raw.next().unwrap_or_default(),
            runs: 1,
            ..Default::default()
        };
        if !matches!(args.command.as_str(), "run" | "compare") {
            return Err(USAGE.into());
        }
        while let Some(arg) = raw.next() {
            let (name, inline) = match arg.split_once('=') {
                Some((n, v)) => (n.to_owned(), Some(v.to_owned())),
                None => (arg.clone(), None),
            };
            if name == "--help" || name == "-h" {
                return Err(USAGE.into());
            }
            let mut value = || {
                inline
                    .clone()
                    .or_else(|| raw.next())
                    .ok_or_else(|| format!("missing value for {name}"))
            };
            match name.as_str() {
                "--profile" => args.profile = Some(value()?),
                "--server" => args.server = Some(value()?.into()),
                "--baseline" => args.baseline = Some(value()?.into()),
                "--candidate" => args.candidate = Some(value()?.into()),
                "--address" => {
                    args.address = Some(value()?.parse().map_err(|e| format!("--address: {e}"))?)
                }
                "--server-kind" => {
                    args.kind = Some(match value()?.as_str() {
                        "norn" => Kind::Norn,
                        "generic" => Kind::Generic,
                        other => return Err(format!("unknown server kind '{other}'")),
                    })
                }
                "--server-arg" => args.server_args.push(value()?),
                "--server-cpus" => args.server_cpus = parse_cpus(&value()?)?,
                "--client-cpus" => args.client_cpus = parse_cpus(&value()?)?,
                "--hold-cpus" => args.hold_cpus = parse_cpus(&value()?)?,
                "--runs" | "--pairs" => {
                    args.runs = value()?.parse().map_err(|_| format!("invalid {name}"))?;
                    if args.runs == 0 {
                        return Err(format!("{name} must be >= 1"));
                    }
                }
                "--label" => args.label = Some(value()?),
                "--out" => args.out = Some(value()?.into()),
                _ => return Err(format!("unknown argument {arg}\n{USAGE}")),
            }
        }
        if args.profile.is_none() {
            return Err(format!("--profile is required\n{USAGE}"));
        }
        Ok(args)
    }

    fn command_output(program: &str, args: &[&str]) -> String {
        Command::new(program)
            .args(args)
            .output()
            .ok()
            .filter(|o| o.status.success())
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_owned())
            .unwrap_or_default()
    }

    fn read_trim(path: &str) -> String {
        fs::read_to_string(path)
            .map(|s| s.trim().to_owned())
            .unwrap_or_default()
    }

    fn sha256(path: &Path) -> String {
        command_output("sha256sum", &[&path.display().to_string()])
            .split_whitespace()
            .next()
            .unwrap_or_default()
            .to_owned()
    }

    fn utc_timestamp() -> String {
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let (days, rem) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
        // Civil-from-days (Howard Hinnant).
        let z = days + 719_468;
        let era = z.div_euclid(146_097);
        let doe = z - era * 146_097;
        let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let d = doy - (153 * mp + 2) / 5 + 1;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = yoe + era * 400 + i64::from(m <= 2);
        format!(
            "{y:04}{m:02}{d:02}T{:02}{:02}{:02}Z",
            rem / 3600,
            rem % 3600 / 60,
            rem % 60
        )
    }

    fn base_provenance(server_cpus: &[usize], client_cpus: &[usize]) -> Provenance {
        let cpu_model = fs::read_to_string("/proc/cpuinfo")
            .ok()
            .and_then(|s| {
                s.lines()
                    .find(|l| l.starts_with("model name"))
                    .and_then(|l| l.split_once(':'))
                    .map(|(_, v)| v.trim().to_owned())
            })
            .unwrap_or_default();
        let governor_cpu = server_cpus.first().copied().unwrap_or(0);
        Provenance {
            timestamp: utc_timestamp(),
            host: read_trim("/proc/sys/kernel/hostname"),
            kernel: read_trim("/proc/sys/kernel/osrelease"),
            cpu_model,
            governor: read_trim(&format!(
                "/sys/devices/system/cpu/cpu{governor_cpu}/cpufreq/scaling_governor"
            )),
            boost: read_trim("/sys/devices/system/cpu/cpufreq/boost"),
            git_revision: command_output("git", &["rev-parse", "HEAD"]),
            git_dirty: !command_output("git", &["status", "--porcelain", "--untracked-files=no"])
                .is_empty(),
            server_cpus: server_cpus.to_vec(),
            client_cpus: client_cpus.to_vec(),
            client_sha256: std::env::current_exe()
                .map(|p| sha256(&p))
                .unwrap_or_default(),
            cgroup: read_trim("/proc/self/cgroup"),
            load_average: read_trim("/proc/loadavg"),
            ..Default::default()
        }
    }

    /// Threads that occupy CPUs with a `pause` loop for the duration of a run.
    struct Holders {
        stop: Arc<AtomicBool>,
        threads: Vec<std::thread::JoinHandle<()>>,
    }

    impl Holders {
        fn start(cpus: &[usize]) -> Self {
            let stop = Arc::new(AtomicBool::new(false));
            let threads = cpus
                .iter()
                .map(|&cpu| {
                    let stop = Arc::clone(&stop);
                    std::thread::Builder::new()
                        .name(format!("hold-{cpu}"))
                        .spawn(move || {
                            if client::pin_current_thread(cpu).is_err() {
                                return;
                            }
                            while !stop.load(Ordering::Relaxed) {
                                std::hint::spin_loop();
                            }
                        })
                        .unwrap()
                })
                .collect();
            Self { stop, threads }
        }
    }

    impl Drop for Holders {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Relaxed);
            for t in self.threads.drain(..) {
                let _ = t.join();
            }
        }
    }

    fn max(v: Vec<f64>) -> Option<f64> {
        v.into_iter().reduce(f64::max)
    }

    fn mean(v: Vec<f64>) -> Option<f64> {
        (!v.is_empty()).then(|| v.iter().sum::<f64>() / v.len() as f64)
    }

    fn siblings(cpu: usize) -> Vec<usize> {
        let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
        parse_cpus(&read_trim(&path)).unwrap_or_else(|_| vec![cpu])
    }

    /// Placement checks; returns warnings for conditions that add noise.
    fn check_placement(
        profile: &Profile,
        kind: Kind,
        external: bool,
        server_cpus: &[usize],
        client_cpus: &[usize],
    ) -> Result<Vec<String>, String> {
        let mut warnings = Vec::new();
        if !client_cpus.is_empty() && client_cpus.len() < profile.client.threads {
            return Err(format!(
                "--client-cpus names {} CPUs but the profile uses {} client threads",
                client_cpus.len(),
                profile.client.threads
            ));
        }
        if !external
            && kind == Kind::Norn
            && !server_cpus.is_empty()
            && server_cpus.len() != profile.server_workers
        {
            return Err(format!(
                "--server-cpus names {} CPUs but the profile uses {} server workers",
                server_cpus.len(),
                profile.server_workers
            ));
        }
        if client_cpus.is_empty() {
            warnings.push("client threads are not pinned (--client-cpus)".into());
        }
        if server_cpus.is_empty() && !external {
            warnings.push("server is not pinned (--server-cpus)".into());
        }
        for &s in server_cpus {
            for sib in siblings(s) {
                if client_cpus.contains(&sib) {
                    return Err(format!(
                        "server CPU {s} shares a core with client CPU {sib}"
                    ));
                }
                if sib != s && server_cpus.contains(&sib) {
                    warnings.push(format!("server CPUs {s} and {sib} are SMT siblings"));
                }
            }
        }
        Ok(warnings)
    }

    struct RunPlan<'a> {
        profile: &'a Profile,
        spec: Option<Spec>,
        address: Option<SocketAddr>,
        client_cpus: &'a [usize],
        hold_cpus: &'a [usize],
        placement_warnings: &'a [String],
        label: String,
        dir: PathBuf,
    }

    fn run_once(plan: &RunPlan<'_>) -> Result<RunResult, String> {
        let profile = plan.profile;
        fs::create_dir_all(&plan.dir).map_err(|e| format!("create {}: {e}", plan.dir.display()))?;
        let server_cpus = plan
            .spec
            .as_ref()
            .map(|s| s.cpus.clone())
            .unwrap_or_default();
        let mut provenance = base_provenance(&server_cpus, plan.client_cpus);
        provenance.hold_cpus = plan.hold_cpus.to_vec();
        let _holders = Holders::start(plan.hold_cpus);
        let process = match &plan.spec {
            Some(spec) => {
                provenance.server_sha256 = sha256(&spec.binary);
                Some(server::Process::start(spec, &plan.dir)?)
            }
            None => None,
        };
        let address = process
            .as_ref()
            .map(|p| p.address)
            .or(plan.address)
            .expect("address or server");
        if let Some(p) = &process {
            provenance.server_command = p.command.clone();
        }

        let segments = client::timeline(profile);
        let threads = profile.client.threads;
        let shared = Arc::new(Shared {
            profile: profile.clone(),
            workload: Workload::new(profile),
            segments: segments.clone(),
            address,
            epoch: Instant::now(),
            start_ns: Mutex::new(0),
            barrier: Barrier::new(threads + 1),
            sampled: AtomicBool::new(false),
            failed: AtomicBool::new(false),
            error: Mutex::new(None),
        });
        let handles: Vec<_> = (0..threads)
            .map(|t| {
                let shared = Arc::clone(&shared);
                let cpu = plan.client_cpus.get(t).copied();
                std::thread::Builder::new()
                    .name(format!("client-{t}"))
                    .spawn(move || client::run_thread(shared, t, cpu))
                    .unwrap()
            })
            .collect();

        // Population done; open counters on the settled server threads.
        shared.barrier.wait();
        let mut counters = process.as_ref().map(|p| ProcessCounters::new(p.pid, true));
        let mut own = ProcessCounters::new(std::process::id() as i32, false);
        let start = shared.now_ns() + 20_000_000;
        *shared.start_ns.lock().unwrap() = start;
        shared.barrier.wait();

        // SMT siblings of the server CPUs that the benchmark does not use.
        // Work on them shares the server core's execution resources. A held
        // sibling is busy with the holder; other work there shows up as the
        // holder's run-queue wait instead.
        let mut server_siblings: Vec<usize> = server_cpus
            .iter()
            .flat_map(|&cpu| siblings(cpu))
            .filter(|cpu| {
                !server_cpus.contains(cpu)
                    && !plan.client_cpus.contains(cpu)
                    && !plan.hold_cpus.contains(cpu)
            })
            .collect();
        server_siblings.sort_unstable();
        server_siblings.dedup();

        let mut samples = Vec::with_capacity(segments.len());
        for s in &segments {
            if !s.measured || shared.failed.load(Ordering::Acquire) {
                samples.push(None);
                continue;
            }
            sleep_until(&shared, start + s.measure_start_ns);
            let t0 = shared.now_ns();
            let a = counters.as_mut().map(ProcessCounters::snapshot);
            let own_a = own.snapshot();
            let host_a = counters::cpu_jiffies();
            sleep_until(&shared, start + s.end_ns);
            let t1 = shared.now_ns();
            let b = counters.as_mut().map(ProcessCounters::snapshot);
            let own_b = own.snapshot();
            let host_b = counters::cpu_jiffies();
            let own_delta = own.delta(&own_a, &own_b);
            let client = |by_thread: &'_ std::collections::BTreeMap<String, u64>| {
                by_thread
                    .iter()
                    .filter(|(name, _)| name.starts_with("client-"))
                    .map(|(_, n)| *n)
                    .collect::<Vec<u64>>()
            };
            let client_preemptions = client(&own_delta.involuntary_by_thread).iter().sum();
            let client_max_run_delay_ns = client(&own_delta.run_delay_by_thread)
                .into_iter()
                .max()
                .unwrap_or(0);
            let server = counters
                .as_ref()
                .zip(a.zip(b))
                .map(|(c, (a, b))| (c.delta(&a, &b), t1 - t0));
            samples.push(Some(report::Sample {
                window_ns: (t0 - start, t1 - start),
                server,
                client_preemptions,
                client_max_run_delay_ns,
                server_sibling_busy: max(counters::fractions(
                    &server_siblings,
                    &host_a,
                    &host_b,
                    |j| j.busy,
                )
                .into_iter()
                .chain(
                    own_delta
                        .run_delay_by_thread
                        .iter()
                        .filter(|(name, _)| name.starts_with("hold-"))
                        .map(|(_, ns)| *ns as f64 / (t1 - t0) as f64),
                )
                .collect()),
                server_cpu_busy: mean(counters::fractions(&server_cpus, &host_a, &host_b, |j| {
                    j.busy
                })),
                server_cpu_irq: mean(counters::fractions(&server_cpus, &host_a, &host_b, |j| {
                    j.irq
                })),
            }));
        }
        shared.sampled.store(true, Ordering::Release);
        if let Some(c) = &counters {
            provenance.perf_counters = c.perf_available;
            provenance.server_threads = c.thread_names();
        }

        let reports: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        let server_result = process.map(server::Process::stop).transpose();
        let mut error = shared.error.lock().unwrap().clone();
        if let Err(e) = server_result {
            // A server crash usually surfaces first as client resets; report both.
            error = Some(match error {
                Some(client) => format!("{e}; {client}"),
                None => e,
            });
        }

        let classes = profile.class_names().len();
        let mut merged: Vec<SegStats> = Vec::new();
        let mut completions: Vec<u32> = Vec::new();
        let (mut populate_ns, mut drain_ns, mut verified) = (0, 0, 0);
        for report in reports.iter().flatten() {
            populate_ns = populate_ns.max(report.populate_ns);
            drain_ns = drain_ns.max(report.drain_ns);
            verified += report.verified;
            if merged.is_empty() {
                merged = report
                    .segments
                    .iter()
                    .map(|_| client::SegStats::empty(classes))
                    .collect();
            }
            for (m, s) in merged.iter_mut().zip(&report.segments) {
                m.merge(s);
            }
            completions.resize(completions.len().max(report.completions.len()), 0);
            for (m, c) in completions.iter_mut().zip(&report.completions) {
                *m += c;
            }
        }
        let valid = error.is_none() && reports.iter().all(Option::is_some);
        let mut segment_results = if merged.is_empty() {
            Vec::new()
        } else {
            report::build_segments(profile, &segments, &merged, &samples, &completions)
        };
        let mut run_warnings = plan.placement_warnings.to_vec();
        if counters.is_some() && !provenance.perf_counters {
            run_warnings.push("hardware counters unavailable (perf_event_open failed)".into());
        }
        let warnings = report::qualify(profile, &mut segment_results, &run_warnings);
        let result = RunResult {
            result_version: RESULT_VERSION,
            label: plan.label.clone(),
            profile: profile.clone(),
            provenance,
            valid,
            error,
            qualified: valid && warnings.is_empty(),
            warnings,
            populate_s: populate_ns as f64 / 1e9,
            drain_s: drain_ns as f64 / 1e9,
            verified_slots: verified,
            segments: segment_results,
        };
        let json = serde_json::to_string_pretty(&result).unwrap();
        fs::write(plan.dir.join("result.json"), json).map_err(|e| format!("write result: {e}"))?;
        fs::write(plan.dir.join("summary.txt"), report::summary(&result))
            .map_err(|e| format!("write summary: {e}"))?;
        Ok(result)
    }

    fn sleep_until(shared: &Shared, deadline_ns: u64) {
        let now = shared.now_ns();
        if deadline_ns > now {
            std::thread::sleep(Duration::from_nanos(deadline_ns - now));
        }
    }

    fn default_out(profile: &Profile) -> PathBuf {
        PathBuf::from(format!(
            "benches/logs/norn-kv-bench/{}-{}",
            utc_timestamp(),
            profile.name
        ))
    }

    fn spec_for(args: &Args, binary: PathBuf, profile: &Profile) -> Spec {
        Spec {
            kind: args.kind.unwrap_or(Kind::Norn),
            binary,
            args: args.server_args.clone(),
            cpus: args.server_cpus.clone(),
            workers: profile.server_workers,
        }
    }

    pub fn main() -> Result<(), String> {
        let args = parse_args()?;
        let profile = Profile::load(args.profile.as_deref().unwrap())?;
        let out = args.out.clone().unwrap_or_else(|| default_out(&profile));
        let kind = args.kind.unwrap_or(Kind::Norn);
        let external = args.address.is_some();
        let placement = check_placement(
            &profile,
            kind,
            external,
            &args.server_cpus,
            &args.client_cpus,
        )?;
        if let Some(cpu) = args
            .hold_cpus
            .iter()
            .find(|c| args.server_cpus.contains(c) || args.client_cpus.contains(c))
        {
            return Err(format!(
                "--hold-cpus names CPU {cpu}, which is also a server or client CPU"
            ));
        }
        fs::create_dir_all(&out).map_err(|e| format!("create {}: {e}", out.display()))?;
        fs::write(
            out.join("profile.json"),
            serde_json::to_string_pretty(&profile).unwrap(),
        )
        .map_err(|e| format!("write profile copy: {e}"))?;

        match args.command.as_str() {
            "run" => {
                if args.server.is_some() == external {
                    return Err("run needs exactly one of --server or --address".into());
                }
                let label = args.label.clone().unwrap_or_else(|| "run".into());
                let mut failures = 0;
                for n in 1..=args.runs {
                    let plan = RunPlan {
                        profile: &profile,
                        spec: args.server.clone().map(|b| spec_for(&args, b, &profile)),
                        address: args.address,
                        client_cpus: &args.client_cpus,
                        hold_cpus: &args.hold_cpus,
                        placement_warnings: &placement,
                        label: label.clone(),
                        dir: out.join(format!("run-{n}-{label}")),
                    };
                    let result = run_once(&plan)?;
                    print!("{}", report::summary(&result));
                    if !result.valid {
                        failures += 1;
                    }
                }
                println!("results: {}", out.display());
                if failures > 0 {
                    return Err(format!("{failures} run(s) were invalid"));
                }
                Ok(())
            }
            "compare" => {
                let (Some(baseline), Some(candidate)) =
                    (args.baseline.clone(), args.candidate.clone())
                else {
                    return Err("compare needs --baseline and --candidate".into());
                };
                if external {
                    return Err("compare launches both servers; --address is not supported".into());
                }
                let mut pairs = Vec::new();
                for pair in 1..=args.runs {
                    // ABBA ordering cancels linear host drift.
                    let order = if pair % 2 == 1 {
                        [("baseline", &baseline), ("candidate", &candidate)]
                    } else {
                        [("candidate", &candidate), ("baseline", &baseline)]
                    };
                    let mut base = None;
                    let mut cand = None;
                    for (label, binary) in order {
                        let plan = RunPlan {
                            profile: &profile,
                            spec: Some(spec_for(&args, binary.clone(), &profile)),
                            address: None,
                            client_cpus: &args.client_cpus,
                            hold_cpus: &args.hold_cpus,
                            placement_warnings: &placement,
                            label: label.into(),
                            dir: out.join(format!("pair-{pair}-{label}")),
                        };
                        let result = run_once(&plan)?;
                        eprint!("{}", report::summary(&result));
                        if !result.valid {
                            return Err(format!(
                                "pair {pair} {label} run was invalid: {:?}",
                                result.error
                            ));
                        }
                        if label == "baseline" {
                            base = Some(result);
                        } else {
                            cand = Some(result);
                        }
                    }
                    pairs.push((base.unwrap(), cand.unwrap()));
                }
                let rows = report::compare(&pairs);
                let table = report::comparison_table(&rows, pairs.len());
                let mut text = table;
                let runs: Vec<&RunResult> = pairs.iter().flat_map(|(b, c)| [b, c]).collect();
                for (i, segment) in pairs[0].0.segments.iter().enumerate() {
                    let failed: Vec<&RunResult> = runs
                        .iter()
                        .copied()
                        .filter(|r| r.segments.get(i).is_some_and(|s| !s.qualified))
                        .collect();
                    if failed.is_empty() {
                        continue;
                    }
                    let mut reasons = std::collections::BTreeMap::<&str, usize>::new();
                    for r in &failed {
                        for reason in &r.segments[i].reasons {
                            *reasons.entry(reason.as_str()).or_default() += 1;
                        }
                    }
                    text.push_str(&format!(
                        "{}: {} of {} runs unqualified;",
                        segment.name,
                        failed.len(),
                        runs.len()
                    ));
                    let reasons: Vec<String> =
                        reasons.iter().map(|(r, n)| format!("{r} x{n}")).collect();
                    text.push_str(&format!(" {}\n", reasons.join(", ")));
                }
                print!("{text}");
                fs::write(out.join("compare.txt"), &text).map_err(|e| format!("write: {e}"))?;
                fs::write(
                    out.join("compare.json"),
                    serde_json::to_string_pretty(&rows).unwrap(),
                )
                .map_err(|e| format!("write: {e}"))?;
                println!("results: {}", out.display());
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    #[cfg(test)]
    mod tests {
        use std::collections::HashMap;
        use std::io::{Read, Write};
        use std::net::{TcpListener, TcpStream};
        use std::sync::{Arc, Mutex};

        use super::*;
        use crate::proto::HEADER_LEN;

        /// Minimal in-order memcached-binary GET/SET server. With `corrupt`,
        /// the first byte of every GET value is flipped.
        fn fake_server(corrupt: bool) -> SocketAddr {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let store: Arc<Mutex<HashMap<Vec<u8>, Vec<u8>>>> = Arc::default();
            std::thread::spawn(move || {
                for stream in listener.incoming() {
                    let Ok(stream) = stream else { break };
                    let store = Arc::clone(&store);
                    std::thread::spawn(move || serve(stream, &store, corrupt));
                }
            });
            address
        }

        fn serve(mut stream: TcpStream, store: &Mutex<HashMap<Vec<u8>, Vec<u8>>>, corrupt: bool) {
            stream.set_nodelay(true).unwrap();
            let mut header = [0_u8; HEADER_LEN];
            while stream.read_exact(&mut header).is_ok() {
                let key_len = u16::from_be_bytes([header[2], header[3]]) as usize;
                let extras = header[4] as usize;
                let body_len = u32::from_be_bytes(header[8..12].try_into().unwrap()) as usize;
                let mut body = vec![0; body_len];
                if stream.read_exact(&mut body).is_err() {
                    return;
                }
                let key = body[extras..extras + key_len].to_vec();
                let mut response = vec![0_u8; HEADER_LEN];
                response[0] = 0x81;
                response[1] = header[1];
                response[12..16].copy_from_slice(&header[12..16]);
                match header[1] {
                    0x01 => {
                        store
                            .lock()
                            .unwrap()
                            .insert(key, body[extras + key_len..].to_vec());
                    }
                    0x00 => match store.lock().unwrap().get(&key) {
                        Some(value) => {
                            response[4] = 4;
                            response.extend_from_slice(&[0; 4]);
                            response.extend_from_slice(value);
                            if corrupt {
                                response[HEADER_LEN + 4] ^= 1;
                            }
                        }
                        None => response[7] = 1,
                    },
                    _ => response[7] = 0x81,
                }
                let body_len = (response.len() - HEADER_LEN) as u32;
                response[8..12].copy_from_slice(&body_len.to_be_bytes());
                if stream.write_all(&response).is_err() {
                    return;
                }
            }
        }

        fn test_profile() -> Profile {
            let path = concat!(env!("CARGO_MANIFEST_DIR"), "/profiles/smoke.json");
            let mut profile = Profile::load(path).unwrap();
            profile.phases.warmup_ms = 100;
            profile.phases.capacity_ms = 300;
            profile.phases.settle_ms = 100;
            profile.phases.segment_ms = 300;
            profile.rates = vec![2_000];
            profile.validate().unwrap();
            profile
        }

        fn run_against(address: SocketAddr, name: &str) -> RunResult {
            let profile = test_profile();
            let dir = std::env::temp_dir()
                .join(format!("norn-kv-bench-test-{name}-{}", std::process::id()));
            let plan = RunPlan {
                profile: &profile,
                spec: None,
                address: Some(address),
                client_cpus: &[],
                hold_cpus: &[],
                placement_warnings: &[],
                label: name.into(),
                dir: dir.clone(),
            };
            let result = run_once(&plan).unwrap();
            let _ = fs::remove_dir_all(dir);
            result
        }

        #[test]
        fn validates_and_accounts_against_reference_server() {
            let result = run_against(fake_server(false), "ok");
            assert!(result.valid, "{:?}", result.error);
            let profile = test_profile();
            let slots = u64::from(profile.writable.slots_per_connection);
            assert_eq!(
                result.verified_slots,
                slots * profile.client.connections as u64
            );
            assert_eq!(result.segments.len(), 2);
            for segment in &result.segments {
                assert!(segment.completed > 0, "{} completed nothing", segment.name);
                assert_eq!(segment.dropped, 0);
                assert!(segment.server.is_none());
                let by_class: u64 = segment.classes.iter().map(|c| c.completed).sum();
                assert_eq!(by_class, segment.completed);
            }
            let open = &result.segments[1];
            assert_eq!(open.offered_rate, Some(2_000));
            assert!(open.latency.count > 0);
            // Every class of the mix is exercised.
            for class in &open.classes[..open.classes.len() - 1] {
                assert!(class.completed > 0, "class {} unused", class.name);
            }
        }

        #[test]
        fn detects_corrupted_values() {
            let result = run_against(fake_server(true), "corrupt");
            assert!(!result.valid);
            let error = result.error.unwrap();
            assert!(error.contains("value mismatch"), "{error}");
        }
    }
}
