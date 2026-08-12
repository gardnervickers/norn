# Rust Toolchain Performance Comparison

## Goal

- Target: compare representative Norn runtime hot paths when compiled with the
  repository-pinned nightly and the latest Rust stable release.
- Metric: bencher-reported nanoseconds per iteration.
- Direction: lower is better.
- Correctness checks: targeted tests for `norn-task`, `norn-executor`, and
  `norn-timer` under both toolchains.

## Environment

- Date: 2026-08-04 (America/New_York).
- Machine: local workstation.
- OS: NixOS, Linux 6.18.38 x86_64.
- CPU: AMD Ryzen 9 5950X, 16 cores / 32 threads; benchmark processes pinned to
  logical CPU 12; CPU 12 scaling governor `performance`; boost enabled.
- Memory: 62 GiB, no swap.
- Storage: Samsung SSD 970 EVO Plus NVMe.
- Baseline toolchain: `rustc 1.85.0-nightly (6d9f6ae36 2024-12-16)`, LLVM
  19.1.5, resolved by the repository's locked Fenix input.
- Comparison toolchain: `rustc 1.97.1 (8bab26f4f 2026-07-14)`, LLVM 22.1.6,
  installed with rustup stable.
- Relevant benchmark environment variables: none set.
- Commit: `6ce17d5c9a77c7658c87076d0f26ef4c1bf10095`.
- Initial worktree state: untracked `assets/` directory; preserved unchanged.

## Methodology

- Benchmark binaries are built in distinct target directories under `/tmp` so
  each toolchain's artifacts are isolated.
- Stable build command: `CARGO_TARGET_DIR=<stable-target> cargo +stable bench -p
  benches --bench schedule_task --bench task_state --bench executor --bench
  timers --no-run`.
- Pinned build command: the same Cargo arguments under `nix develop`, with a
  distinct `CARGO_TARGET_DIR`.
- Measurement command: `taskset -c 12 <benchmark-binary> <workload-filter>`.
- Every benchmark process is pinned with `taskset -c 12`.
- Workloads:
  - `schedule_task`: `bench_spawn/num_tasks=1024` and `bench_join`.
  - `task_state`: `bench_task_yield/tasks=128/yields=32`.
  - `executor`: `bench_block_on_ready` and `bench_block_on_yield`.
  - `timers`: `bench_timers/num_tasks=64/n=256` and
    `bench_cancel_same_slot/runtime=norn/timers=4096`.
- Warmup: one unrecorded invocation of every selected workload per toolchain.
- Repetitions: three recorded invocations per workload and toolchain, with the
  toolchain order alternated by trial.
- Summary statistic: median of the three bencher point estimates. Bencher itself
  reports a per-invocation median and spread.
- Noise threshold: deltas smaller than 5%, or results with overlapping observed
  trial ranges, are treated as inconclusive.
- Measurement overhead: process startup and compilation are outside bencher's
  timed loop.

## Correctness

- Pinned nightly: passed the targeted test command for `norn-task`,
  `norn-executor`, and `norn-timer` (42 unit tests, 1 doc test).
- Stable 1.97.1: passed the same command (42 unit tests, 1 doc test).
- Logs: `/tmp/norn-toolchain-comparison-2026-08-03/test-pinned.log` and
  `/tmp/norn-toolchain-comparison-2026-08-03/test-stable.log`.

## Raw Results

Raw command logs are stored under
`/tmp/norn-toolchain-comparison-2026-08-03/` for this run. Values below are
bencher point estimates in ns/iter.

### Trial 1 (pinned, then stable)

| Workload | Pinned | Stable |
|---|---:|---:|
| Spawn 1,024 tasks | 37,851 | 37,115 |
| Join | 89 | 86 |
| Yield 128 tasks x 32 | 36,535 | 39,011 |
| Block on ready | 5 | 4 |
| Block on yield | 6 | 4 |
| Timers 64 tasks / 256 total | 13,919 | 11,618 |
| Cancel 4,096 timers | 134,546 | 129,405 |

### Trial 2 (stable, then pinned)

| Workload | Pinned | Stable |
|---|---:|---:|
| Spawn 1,024 tasks | 38,000 | 36,648 |
| Join | 91 | 83 |
| Yield 128 tasks x 32 | 45,989 | 40,377 |
| Block on ready | 5 | 5 |
| Block on yield | 6 | 4 |
| Timers 64 tasks / 256 total | 14,184 | 10,938 |
| Cancel 4,096 timers | 137,318 | 132,079 |

The pinned timer trial reported a wide within-invocation spread for the timer
workload (`+/- 7,940 ns`), and the task-yield estimates varied substantially
between trials. These cases require the third trial before interpretation.

### Trial 3 (pinned, then stable)

| Workload | Pinned | Stable |
|---|---:|---:|
| Spawn 1,024 tasks | 37,417 | 37,363 |
| Join | 90 | 84 |
| Yield 128 tasks x 32 | 36,561 | 39,708 |
| Block on ready | 5 | 5 |
| Block on yield | 5 | 4 |
| Timers 64 tasks / 256 total | 13,985 | 11,235 |
| Cancel 4,096 timers | 133,976 | 128,525 |

## Summary

| Workload | Pinned median | Stable median | Stable delta | Interpretation |
|---|---:|---:|---:|---|
| Spawn 1,024 tasks | 37,851 | 37,115 | -1.9% | Inconclusive / equivalent |
| Join | 90 | 84 | -6.7% | Stable faster |
| Yield 128 tasks x 32 | 36,561 | 39,708 | +8.6% | Inconclusive; pinned trials were noisy |
| Block on ready | 5 | 5 | 0.0% | Equivalent at harness resolution |
| Block on yield | 6 | 4 | -33.3% | Stable faster, 2 ns absolute difference |
| Timers 64 tasks / 256 total | 13,985 | 11,235 | -19.7% | Stable materially faster |
| Cancel 4,096 timers | 134,546 | 129,405 | -3.8% | Consistent but below 5% threshold |

Negative deltas favor stable. Stable produced the clearest material improvement
in timer scheduling, with non-overlapping trial ranges (`10,938`-`11,618` ns
versus `13,919`-`14,184` ns). Join and block-on-yield also favored stable in
every trial, although the latter is only a 2 ns absolute difference and is near
the harness's integer-nanosecond resolution.

The task-yield workload cannot support a regression claim: the pinned results
spanned `36,535`-`45,989` ns and overlap the stable range. Spawn and block-ready
were effectively unchanged. Timer cancellation consistently favored stable,
but the median delta did not clear the predeclared 5% threshold.

Overall, the latest stable compiler is neutral-to-faster for the trustworthy
cases in this sample, passes the selected correctness suite, and shows no
confirmed regression. This comparison covers generated-code performance for
representative task, executor, and simulated-timer paths; it does not cover
Linux `io_uring` workloads, build time, Miri availability, or the full workspace
test suite.
