use std::env;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use bencher::{bench, run_tests_console, TestDescAndFn, TestFn, TestOpts};
use pprof::protos::Message;

const PPROF_OUT_ENV: &str = "NORN_BENCH_PPROF";
const PPROF_FREQUENCY_HZ: i32 = 1_000;
const PPROF_BLOCKLIST: &[&str] = &["libc", "libgcc", "pthread", "vdso"];

pub fn run(benches: Vec<TestDescAndFn>) {
    let opts = test_opts_from_args();
    match ProfileOutput::from_env() {
        Ok(Some(output)) => profile_benches(&opts, benches, output),
        Ok(None) => {
            run_tests_console(&opts, benches).unwrap();
        }
        Err(err) => panic!("failed to configure {PPROF_OUT_ENV}: {err}"),
    }
}

fn profile_benches(opts: &TestOpts, benches: Vec<TestDescAndFn>, output: ProfileOutput) {
    let mut benches = benches
        .into_iter()
        .filter(|bench| !bench.desc.ignore)
        .collect::<Vec<_>>();

    if let Some(filter) = &opts.filter {
        benches.retain(|bench| bench.desc.name.contains(filter));
    }

    benches.sort_by(|left, right| left.desc.name.cmp(&right.desc.name));
    assert!(
        !benches.is_empty(),
        "no benchmarks matched{}",
        opts.filter
            .as_deref()
            .map(|filter| format!(" filter `{filter}`"))
            .unwrap_or_default()
    );

    for bench in benches {
        let bench_name = bench.desc.name.to_string();
        eprintln!("profiling {bench_name}");
        let guard = pprof::ProfilerGuardBuilder::default()
            .frequency(PPROF_FREQUENCY_HZ)
            .blocklist(PPROF_BLOCKLIST)
            .build()
            .unwrap_or_else(|err| panic!("failed to start pprof profiler: {err}"));

        match bench.testfn {
            TestFn::DynBenchFn(benchfn) => {
                let _ = bench::benchmark(|harness| benchfn.run(harness));
            }
            TestFn::StaticBenchFn(benchfn) => {
                let _ = bench::benchmark(benchfn);
            }
        }

        output
            .write_profile(&bench_name, guard)
            .unwrap_or_else(|err| panic!("failed to write pprof profile for {bench_name}: {err}"));
    }
}

fn test_opts_from_args() -> TestOpts {
    let mut test_opts = TestOpts::default();
    if let Some(arg) = env::args().skip(1).find(|arg| *arg != "--bench") {
        test_opts.filter = Some(arg);
    }
    test_opts
}

#[derive(Clone)]
struct ProfileOutput {
    root: PathBuf,
    bench_binary: String,
}

impl ProfileOutput {
    fn from_env() -> io::Result<Option<Self>> {
        let Some(path) = env::var_os(PPROF_OUT_ENV) else {
            return Ok(None);
        };

        let root = PathBuf::from(path);
        fs::create_dir_all(&root)?;
        Ok(Some(Self {
            root,
            bench_binary: current_bench_binary(),
        }))
    }

    fn write_profile(&self, bench_name: &str, guard: pprof::ProfilerGuard<'_>) -> io::Result<()> {
        let report = guard.report().build().map_err(to_io_error)?;
        let flamegraph = self.flamegraph_path(bench_name)?;
        let flamegraph_file = File::create(&flamegraph)?;
        report.flamegraph(flamegraph_file).map_err(to_io_error)?;
        eprintln!("wrote flamegraph to {}", flamegraph.display());

        let profile = report.pprof().map_err(to_io_error)?;

        let mut content = Vec::new();
        profile.encode(&mut content).map_err(to_io_error)?;

        let output = self.profile_path(bench_name)?;
        let mut file = File::create(&output)?;
        file.write_all(&content)?;
        eprintln!("wrote pprof profile to {}", output.display());
        Ok(())
    }

    fn profile_path(&self, bench_name: &str) -> io::Result<PathBuf> {
        let dir = self.root.join(&self.bench_binary);
        fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{}.pb", sanitize_name(bench_name))))
    }

    fn flamegraph_path(&self, bench_name: &str) -> io::Result<PathBuf> {
        let dir = self.root.join(&self.bench_binary);
        fs::create_dir_all(&dir)?;
        Ok(dir.join(format!("{}.svg", sanitize_name(bench_name))))
    }
}

fn current_bench_binary() -> String {
    let current_exe = env::current_exe().ok();
    let stem = current_exe
        .as_deref()
        .and_then(Path::file_stem)
        .and_then(|stem| stem.to_str())
        .unwrap_or("bench");
    sanitize_name(strip_cargo_hash(stem))
}

fn strip_cargo_hash(name: &str) -> &str {
    let Some((prefix, suffix)) = name.rsplit_once('-') else {
        return name;
    };

    if suffix.len() >= 8 && suffix.chars().all(|ch| ch.is_ascii_hexdigit()) {
        prefix
    } else {
        name
    }
}

fn sanitize_name(name: &str) -> String {
    let mut sanitized = String::with_capacity(name.len());
    for ch in name.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            sanitized.push(ch);
        } else {
            sanitized.push('_');
        }
    }

    let trimmed = sanitized.trim_matches('_');
    if trimmed.is_empty() {
        "benchmark".to_string()
    } else {
        trimmed.to_string()
    }
}

fn to_io_error(err: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::Other, err.to_string())
}
