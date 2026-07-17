# gRPC Runtime Comparison

## Goal

- Target: compare gRPC over Norn with gRPC over Tokio while emphasizing async
  scheduling cost instead of shared gRPC, protobuf, or HTTP/2 implementation
  cost.
- Primary metric: incremental nanoseconds per injected wake/poll cycle; lower is
  better.
- Secondary metric: RPC throughput for the scheduler-amplified workload; higher
  is better.
- Correctness check: every response must echo the request id and the checksum of
  its payload. A mismatch fails the benchmark.

## Environment

- Machine: local AMD Ryzen 9 5950X workstation (16 cores, 32 hardware threads).
- Execution mode: local, pinned to CPU 2; its SMT sibling CPU 18 was idle at
  the start of measurement.
- Required OS: Linux (`norn-uring` is Linux-only).
- OS: NixOS, Linux 6.18.38 x86_64.
- CPU/power state: `performance` governor with boost enabled; initial load
  average 0.06/0.06/0.10 and CPU temperature 48.6 C.
- Runtime/toolchain: rustc 1.97.0 (2026-07-07), Cargo 1.97.0.
- Commit/worktree state: commit `86e81c6ddcc937c893299a0440cfa4b323b3f82d`
  with the uncommitted benchmark implementation under test.
- Raw results: `target/bench-results/grpc-runtime/<UTC timestamp>/` by default.

Do not compare numbers captured on different machines, CPU-frequency policies,
or materially different kernel/toolchain versions.

## Methodology

Both variants use the same:

- generated Tonic client, server, and unary service implementation;
- Tonic, Prost, Hyper, and HTTP/2 versions from one Cargo lockfile;
- 16-byte request payload and small fixed response;
- one persistent loopback TCP/HTTP2 connection;
- 64 concurrent RPC streams and 512 RPCs per timed sample;
- response validation and client-side request orchestration;
- single OS thread for the client, server, and transport driver.

Only the executor, timer adapter, and TCP I/O driver differ. Norn uses
`LocalExecutor` plus `norn-uring`; Tokio uses a current-thread runtime plus its
normal TCP driver. Connection establishment and 128 warmup RPCs happen before
the timed loop.

The benchmark contains a matched pair:

| Workload | Yields per RPC | Purpose |
| --- | ---: | --- |
| `protocol-control` | 0 | Measures the shared gRPC/HTTP2 floor plus transport/runtime I/O. |
| `runtime-wakes` | 128 | Adds identical self-wake/poll cycles inside every server RPC without adding protobuf or HTTP/2 work. |

`YieldMany` is the same future for both runtimes. Each injected yield returns
`Pending` and calls `wake_by_ref`, forcing another executor scheduling cycle.
For each complete run, calculate:

```text
incremental ns per wake/poll =
  (runtime-wakes - protocol-control) / (512 * 128)
```

The reported incremental result is the median of the paired per-run values.
Pairing before taking the median avoids subtracting samples collected under
different short-term host conditions. The final local result uses five paired
runs because the first three warranted confirmation of the derived metric.

This matched subtraction removes most of the common protocol cost. It should
not be interpreted as a general-purpose gRPC latency result.

## Commands

Enter the repository's development environment, then capture three complete
runs:

```bash
nix develop
./hack/bench-grpc-runtime.sh
```

On a stable benchmark host, pin the single-threaded process to one suitable CPU:

```bash
NORN_GRPC_BENCH_CPU=2 ./hack/bench-grpc-runtime.sh
```

To run only the scheduler-amplified pair:

```bash
NORN_GRPC_BENCH_FILTER=workload=runtime-wakes ./hack/bench-grpc-runtime.sh
```

The direct command is:

```bash
cargo bench -p benches --bench grpc_runtime -- bench_grpc_runtime
```

Run the untimed eight-RPC correctness smoke test with:

```bash
NORN_GRPC_BENCH_SMOKE=1 cargo bench -p benches --bench grpc_runtime
```

Use at least three complete runs. Compare medians for each matched case and
investigate run-to-run spread above 3% before making a performance claim. Add
confirmation runs when subtracting the control materially amplifies noise.

## Baseline

- Date/time: 2026-07-14 13:49 UTC.
- Command: `NORN_GRPC_BENCH_CPU=2 NORN_GRPC_BENCH_RUNS=3
  ./hack/bench-grpc-runtime.sh
  target/bench-results/grpc-runtime/20260714T134940Z-local`.
- Raw output: `target/bench-results/grpc-runtime/20260714T134940Z-local/`.
- Workload: initial 32-yield scheduler amplification.
- Median batch times: Norn control 4,839,264 ns, Norn runtime-wakes 5,467,962
  ns, Tokio control 5,299,978 ns, Tokio runtime-wakes 6,098,089 ns.
- Raw-case max-to-min spread: 0.97% to 2.12%, within the 3% threshold.
- Interpretation: Norn completed the control batch 8.69% faster and the
  runtime-wakes batch 10.33% faster. The paired incremental scheduler metric
  was not stable enough for a final claim, so the scheduler signal was
  increased without changing RPC traffic.

## Attempts

### Attempt 1: 32 injected wake/poll cycles per RPC

- Hypothesis: 16,384 injected cycles per 512-RPC batch would dominate enough of
  the shared protocol floor to isolate scheduler cost.
- Comparison baseline: matched zero-yield control on the same runtime and run.
- Correctness result: all four cases passed in all three runs.
- Median-of-case incremental estimates: Norn 38.37 ns/cycle; Tokio 48.71
  ns/cycle.
- Paired per-run incremental estimates: Norn 34.27/38.37/34.76 ns and Tokio
  46.71/48.71/46.86 ns.
- Paired incremental max-to-min spread: Norn 11.8%; Tokio 4.3%.
- Decision: revise. The four raw cases were individually stable, but subtracting
  the control amplified noise beyond the 3% threshold. Increase only the
  identical self-wake count from 32 to 128 and rerun.

### Attempt 2: 128 injected wake/poll cycles per RPC

- Hypothesis: quadrupling identical scheduler work while leaving the RPC count,
  payload, concurrency, connection, and protocol stack unchanged will make the
  derived scheduler cost robust against control-subtraction noise.
- Files changed: `benches/grpc_runtime.rs` and this methodology/results file.
- Comparison baseline: matched zero-yield control on the same runtime and run.
- Correctness result: all four cases passed in all five runs.
- Commands: three initial passes followed by two unchanged confirmation passes,
  all pinned to CPU 2.
- Raw output:
  - `target/bench-results/grpc-runtime/20260714T135228Z-local-yields128/`
  - `target/bench-results/grpc-runtime/20260714T154634Z-local-yields128-confirmation/`
- Raw-case max-to-min spread across five runs: Norn control 2.02%, Norn
  runtime-wakes 2.29%, Tokio control 1.02%, Tokio runtime-wakes 0.78%.
- Paired incremental relative median absolute deviation: Norn 0.71%; Tokio
  0.34%.
- Decision: keep. Every run independently shows lower incremental scheduler
  cost for Norn, with the size of that advantage ranging from 30.12% to 32.95%.

## Setup Validation

- `cargo check -p benches --bench grpc_runtime`: passed.
- `cargo check -p benches --all-targets`: passed.
- `cargo bench -p benches --bench grpc_runtime --no-run`: passed in the
  optimized benchmark profile.
- `NORN_GRPC_BENCH_SMOKE=1 cargo bench -p benches --bench grpc_runtime`:
  passed for both runtimes and both workload shapes when run outside the
  filesystem/process sandbox so `io_uring` was permitted.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy -p benches --bench grpc_runtime --no-deps -- -D warnings -A
  clippy::io-other-error`: passed. The narrow allow is for the existing
  `benches/support.rs` helper.
- The full dependency-aware `-D warnings` Clippy invocation is currently
  blocked by pre-existing `norn-uring` warnings under the installed newer
  Clippy, including `large_enum_variant`, `result_large_err`,
  `doc_overindented_list_items`, and `io_other_error`.

## Final Result

Five-run medians for each 512-RPC batch:

| Runtime | Control time | Runtime-wakes time | Control RPC/s | Runtime-wakes RPC/s |
| --- | ---: | ---: | ---: | ---: |
| Norn | 4.893 ms | 7.087 ms | 104,636 | 72,245 |
| Tokio current-thread | 5.334 ms | 8.538 ms | 95,986 | 59,967 |

Norn's batch time was 8.27% lower for the zero-yield control and 17.00% lower
for the scheduler-amplified workload.

The five paired incremental scheduler estimates were:

| Run | Norn ns/cycle | Tokio ns/cycle | Norn lower |
| ---: | ---: | ---: | ---: |
| 1 | 33.47 | 48.97 | 31.64% |
| 2 | 32.48 | 48.44 | 32.95% |
| 3 | 34.20 | 48.95 | 30.12% |
| 4 | 33.24 | 48.80 | 31.89% |
| 5 | 33.69 | 48.63 | 30.72% |

The paired medians are 33.47 ns/cycle for Norn and 48.80 ns/cycle for Tokio.
On this benchmark and host, Norn's incremental injected wake/poll cost is
31.41% lower. Relative median absolute deviation is 0.71% for Norn and 0.34%
for Tokio, and all four raw cases remain below the 3% max-to-min threshold.

## Cumulative Result

- Accepted benchmark change: increase the scheduler-amplified workload from 32
  to 128 identical yields per RPC; no runtime implementation was changed.
- Baseline summary: the 32-yield raw cases were stable, but the paired
  subtraction was too noisy for the primary metric.
- Final summary: five 128-yield paired runs show a consistent 30.12% to 32.95%
  lower incremental scheduling cost for Norn.
- Confidence: high for this synthetic single-threaded loopback workload on this
  host; not a claim about general gRPC latency or remote-network performance.
- Remaining ideas: repeat on a dedicated production-like Linux host, and add a
  split-endpoint variant if client-side and server-side runtime costs need to be
  separated.

## 2026-07-15 Inline Candidate Pass

- Goal: identify remaining task-runtime hot boundaries that are credible
  candidates for `#[inline]`, then keep only material repeatable improvements.
- Machine: the same local Ryzen 9 5950X, pinned to CPU 2 with the `performance`
  governor.
- Primary benchmark: `cargo bench -p benches --bench task_state --
  bench_task_yield/tasks=128/yields=32`.
- Secondary benchmark: the Norn `runtime-wakes` gRPC case in this document.
- Primary metric: median nanoseconds per 128-task/32-yield batch; lower is
  better.
- Repetitions: three baseline and three candidate runs, with a fresh profile
  used to choose candidates.
- Acceptance threshold: at least 5% faster, repeatable, correctness-preserving,
  and justified by the profile. Smaller changes are rejected unless they remove
  meaningful complexity.
- Raw evidence: `target/bench-results/inline-candidates/20260715T055214Z/`.
- Previously rejected ideas not to retry: inline only `Runnable::run`, force
  inline immediate SQ push, widen task flags, or replace the driver ring's
  `RefCell` with unchecked access.
- Baseline: 38,144 ns median (37,990 / 38,144 / 38,265 ns), with 0.72%
  full-range spread. An earlier set was discarded because a concurrent
  `canonical_ledger_sim` process saturated another core and thermally disturbed
  the host.
- Profile: 277 samples from the baseline. Self cost was concentrated in
  `State::prepare_poll` (56.68%, already inline and doing real state work),
  `VecDeque::wrap_index` (16.25%, standard-library queue mechanics),
  `TaskRef::run` (9.39%), the raw-waker callback (8.30%), and
  `Header::vtable` (7.58%). The remaining credible inline target is therefore
  the `TaskRef::run` -> vtable accessor dispatch chain; the two larger entries
  are not removable call boundaries in Norn.
- Rejected `#[inline]` on `TaskRef::run`: 42,725 ns median (49,833 /
  42,579 / 42,725 ns), 12.0% slower than baseline. This is distinct from the
  earlier rejected `Runnable::run` wrapper and shows that pulling the task
  dispatch body into its caller harms this hot loop.
- Rejected `#[inline]` on `Header::vtable`: 37,886 ns median (37,780 /
  38,487 / 37,886 ns), only 0.68% faster than baseline and within run noise.
- Conclusion: no additional inline annotation met the 5% threshold. The
  remaining hot profile entries are state-transition work, `VecDeque`
  mechanics, and a raw-waker callback boundary; improving them would require a
  data-structure or state-machine change, not another inline hint.

## Limitations

- Client and server share one runtime thread, so the result measures combined
  client, server, executor, and I/O-driver behavior.
- The self-waking future is intentionally synthetic. It isolates scheduling
  overhead but does not model application computation or external network
  latency.
- Loopback removes network variability and is not representative of remote RPC
  latency.
- Norn's `io_uring` path and Tokio's normal Linux network driver are part of the
  runtime comparison; this benchmark does not isolate executor code from I/O
  driver code.
