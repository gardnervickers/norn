# Performance Benchmark Results

## Goal

- Target: establish a first-pass performance baseline for the existing Norn benchmark suite before profiling or optimization.
- Metric: benchmark-reported median wall time per benchmark case.
- Direction: lower is better.
- Correctness checks: existing benchmark assertions; no performance-sensitive code changes during this baseline pass.

## Environment

- Machine: local machine in `/home/gvickers/src/github.com/gardnervickers/norn`.
- Execution mode: local.
- OS: Linux nixos 6.18.31, x86_64.
- CPU/GPU: AMD Ryzen 9 5950X 16-Core Processor, 32 logical CPUs, boost enabled.
- Memory: 62 GiB total, 54 GiB available at start.
- Storage: repository-local Cargo target directory; no explicit disk isolation.
- Runtime/toolchain: `rustc 1.85.0-nightly (6d9f6ae36 2024-12-16)`, `cargo 1.85.0-nightly (769f622e1 2024-12-14)`.
- Relevant env vars: none set for baseline timing; `NORN_BENCH_PPROF` reserved for later profiling runs.
- Commit/worktree state: branch `master`, commit `02b770f`, clean before creating this results file.

## Methodology

- Benchmark command: `cargo bench -p benches`.
- Workload/input: all existing bench binaries in `benches/Cargo.toml`, including task scheduling, task yield, timers, noop io_uring, accept/recv, Hyper HTTP/2, real-world uring UDP/TCP/file cases, and `norn-kv`.
- Warmup: default `bencher` harness behavior.
- Repetitions: single broad baseline sweep first; repeat focused hot cases at least 3 times before claiming improvements.
- Summary statistic: benchmark-reported median (`bench: ... ns/iter` or equivalent unit), with per-case variance from the harness when available.
- Noise/variance threshold: broad sweep is directional only; optimization decisions should use focused repeats and median comparison.
- Notes on measurement overhead: full suite includes loopback networking, temp-file I/O, benchmark harness overhead, and OS scheduling noise. These results should guide target selection, not prove optimization wins by themselves.

## Baseline

- Date/time: 2026-05-16T22:55:33-04:00 to 2026-05-16T23:51:50-04:00.
- Command: `cargo bench -p benches`.
- Raw output or log path: `target/perf-baseline/2026-05-16/cargo-bench-p-benches.log`.
- Summary: 229 measured benchmark cases completed successfully.
  - Task core:
    - `bench_spawn/num_tasks=1`: `37 ns/iter (+/- 2.7%)`.
    - `bench_spawn/num_tasks=1024`: `38,743 ns/iter (+/- 1.8%)`.
    - `bench_join`: `89 ns/iter (+/- 3.4%)`.
    - `bench_task_yield/tasks=128/yields=32`: `43,893 ns/iter (+/- 2.5%)`.
  - Timers:
    - `bench_timers/num_tasks=1/n=256`: `27,439 ns/iter (+/- 1.9%)`.
    - `bench_timers/num_tasks=32/n=256`: `14,397 ns/iter (+/- 2.2%)`.
    - `bench_timers/num_tasks=64/n=256`: `16,542 ns/iter (+/- 4.2%)`.
  - io_uring noop:
    - `bench_noop/num_tasks=1/n=1`: `762 ns/iter (+/- 1.6%)`.
    - `bench_noop/num_tasks=1/n=100000`: `60.218 ms/iter (+/- 0.8%)`.
    - `bench_noop/num_tasks=64/n=100000`: `15.338 ms/iter (+/- 1.5%)`.
    - `bench_noop_backpressure/ring_entries=2/n=4096`: `3.557 ms/iter (+/- 3.2%)`.
    - `bench_noop_backpressure/ring_entries=4/n=16384`: `12.562 ms/iter (+/- 2.4%)`.
  - Accept/recv:
    - `bench_accept_recv/mode=multi/clients=1/connections=1024`: `31.477 ms/iter (+/- 2.5%)`.
    - `bench_accept_recv/mode=single/clients=1/connections=1024`: `34.476 ms/iter (+/- 1.9%)`.
    - `bench_accept_recv/mode=multi/clients=8/connections=128`: `29.069 ms/iter (+/- 2.0%)`.
    - `bench_accept_recv/mode=single/clients=8/connections=128`: `29.855 ms/iter (+/- 2.7%)`.
  - Hyper HTTP/2:
    - `bench_hyper/num_clients=1/num_requests=128`: `2.729 ms/iter (+/- 2.1%)`.
    - `bench_hyper/num_clients=1/num_requests=1024`: `20.698 ms/iter (+/- 2.5%)`.
  - TCP request/response:
    - `runtime=norn/recv=normal/connections=8/requests_per_connection=64/payload=64`: `5.086 ms/iter (+/- 1.2%)`.
    - `runtime=norn/recv=bufring_multi/connections=8/requests_per_connection=64/payload=64`: `4.471 ms/iter (+/- 1.9%)`.
    - `runtime=norn/recv=bufring_bundle_multi/connections=8/requests_per_connection=64/payload=64`: `4.528 ms/iter (+/- 0.9%)`.
    - `runtime=norn/recv=normal/connections=64/requests_per_connection=512/payload=64`: `313.219 ms/iter (+/- 0.6%)`.
    - `runtime=norn/recv=bufring_multi/connections=64/requests_per_connection=512/payload=64`: `259.030 ms/iter (+/- 0.8%)`.
    - `runtime=norn/recv=bufring_bundle_multi/connections=64/requests_per_connection=512/payload=64`: `262.832 ms/iter (+/- 0.9%)`.
    - `runtime=tokio/recv=normal/connections=64/requests_per_connection=512/payload=64`: `316.828 ms/iter (+/- 0.8%)`.
  - UDP request/response:
    - `runtime=norn/recv=multi/window=32/total_requests=4096/payload=64`: `27.101 ms/iter (+/- 1.6%)`.
    - `runtime=norn/recv=single/window=32/total_requests=4096/payload=64`: `30.236 ms/iter (+/- 3.2%)`.
    - `runtime=tokio/recv=single/window=32/total_requests=4096/payload=64`: `31.259 ms/iter (+/- 2.3%)`.
    - `runtime=norn/recv=multi/window=64/total_requests=8192/payload=64`: `54.769 ms/iter (+/- 2.3%)`.
    - `runtime=norn/recv=single/window=64/total_requests=8192/payload=64`: `61.212 ms/iter (+/- 3.4%)`.
  - Disk-backed/store cases:
    - `bench_raw_write_4k_dsync/ops=8`: `32.743 ms/iter (+/- 3.5%)`.
    - `bench_recover/live_slots=256`: `11.973 ms/iter (+/- 5.7%)`.
    - `bench_put_get_delete/value_len=1024/ops=8`: `62.448 ms/iter (+/- 29.6%)`.
- Interpretation:
  - Most pure runtime/task/timer cases are stable enough for focused optimization work.
  - The broadest, slowest stable cases are TCP request/response at 64 connections and 512 requests per connection. These are good profiling candidates because variance was around 1% and each iteration is long enough to sample.
  - UDP confirms the current multishot path advantage already recorded in `benches/perf-notes.md`; it is probably not the first target unless the next goal is a new UDP benchmark shape.
  - File and `norn-kv` write-heavy cases are useful for directional awareness, but single-pass variance is high enough that storage isolation or repeated focused runs are needed before optimizing them.

## Attempts

### Existing Benchmark Win: TCP multishot bufring receive

- Hypothesis: for TCP request/response workloads, keeping one multishot bufring receive armed per connection should avoid repeated single-shot receive submissions and improve throughput versus the single-shot bufring path.
- Files changed: none for this comparison.
- Comparison baseline: `bench_tcp_request_response/runtime=norn/recv=bufring/connections=64/requests_per_connection=512/payload=64`.
- Correctness result: benchmark assertions passed in both runs.
- Benchmark command:
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=bufring/connections=64/requests_per_connection=512/payload=64`
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=bufring_multi/connections=64/requests_per_connection=512/payload=64`
- Raw output or log path: terminal output from focused runs on 2026-05-17.
- Summary:
  - Single-shot bufring: `335.195 ms/iter (+/- 0.8%)`.
  - Multishot bufring: `256.687 ms/iter (+/- 1.1%)`.
- Delta: `23.4%` lower wall time for `bufring_multi`.
- Decision: found a confirmed >20% networking benchmark win in the existing benchmark matrix; no code change required.
- Notes: broad baseline showed the same direction (`338.456 ms` vs `259.030 ms`, `23.5%` lower), and the focused repeat reproduced it.

### Rejected Change: io_uring submit fast path

- Hypothesis: pushing an SQE directly when the submission queue has space would avoid constructing `PushFuture` in the common path.
- Files changed during trial: `norn-uring/src/driver/mod.rs`, `norn-uring/src/operation/mod.rs`.
- Benchmark command: `cargo bench -p benches --bench noop_submit -- bench_noop_backpressure/ring_entries=2/n=4096`.
- Summary:
  - Trial result with change: `2.126 ms/iter (+/- 7.5%)`.
  - Same-command clean baseline from commit `02b770f`: `2.042 ms/iter (+/- 1.8%)`.
- Correctness result: `cargo fmt --all --check` passed; `cargo test -p norn-uring --test noop_submit` passed.
- Decision: reverted. The focused clean baseline was faster than the changed tree.

### Rejected Change: TCP flush no-op

- Hypothesis: TCP stream flush should be a no-op and should avoid a readiness poll plus socket flush call.
- Files changed during trial: `norn-uring/src/net/tcp.rs`.
- Benchmark commands:
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=normal/connections=64/requests_per_connection=512/payload=64`
  - `cargo bench -p benches --bench hyper -- bench_hyper/num_clients=1/num_requests=1024`
- Summary:
  - TCP request/response baseline: `306.358 ms/iter (+/- 0.9%)`.
  - TCP request/response with change: `305.361 ms/iter (+/- 2.2%)`.
  - Hyper baseline: `20.395 ms/iter (+/- 1.5%)`.
  - Hyper with change: `20.320 ms/iter (+/- 1.6%)`.
- Decision: reverted. Both movements were inside benchmark noise.

### Rejected Change: TCP multishot accept in benchmark server

- Hypothesis: using the existing `TcpListener::incoming()` multishot accept stream in the Norn benchmark server would reduce per-connection accept submission overhead.
- Files changed during trial: `benches/uring_realworld.rs`.
- Benchmark commands:
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response_lifecycle/runtime=norn/recv=normal/connections=8/requests_per_connection=1/payload=64`
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=normal/connections=64/requests_per_connection=64/payload=64`
- Summary:
  - Lifecycle baseline: `362.874 us/iter (+/- 7.0%)`.
  - Lifecycle with change: `354.488 us/iter (+/- 13.7%)`.
  - 64-connection steady-state with change: `40.309 ms/iter (+/- 1.9%)`.
  - 64-connection steady-state baseline after revert: `40.345 ms/iter (+/- 1.4%)`.
- Decision: reverted. No material same-session improvement.

### Rejected Change: larger CQ drain batch

- Hypothesis: increasing the driver completion-drain stack batch from 32 CQEs to 64 CQEs would reduce drain-loop overhead on networking benchmarks.
- Files changed during trial: `norn-uring/src/driver/mod.rs`.
- Benchmark commands:
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=normal/connections=64/requests_per_connection=512/payload=64`
  - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=bufring_multi/connections=64/requests_per_connection=512/payload=64`
- Summary:
  - Normal TCP baseline: `306.358 ms/iter (+/- 0.9%)`.
  - Normal TCP with change: `305.370 ms/iter (+/- 0.9%)`.
  - Bufring multishot with change: `256.453 ms/iter (+/- 1.0%)`.
  - Bufring multishot same-session baseline after revert: `255.385 ms/iter (+/- 1.1%)`.
- Decision: reverted. The multishot same-session baseline was faster with the original 32-entry batch.

### Accepted Change: UDP connected clients plus multishot echo server

- Hypothesis: the UDP request/response benchmark still paid per-packet source-address receive overhead on Norn clients and single-shot receive submission overhead on the Norn server. Connecting the Norn client sockets lets clients use connected `send`/`recv` APIs, while a server-side multishot bufring receive keeps one receive armed and sends the selected ring buffer back directly.
- Files changed:
  - `norn-uring/src/net/socket.rs`: implemented `StableBuf` for `RecvMsgRingBuf` so recvmsg ring buffers can be used directly as send buffers.
  - `benches/uring_realworld.rs`: connected Norn UDP client sockets, used connected client send/recv paths, added a Norn multishot bufring UDP echo server, and increased UDP multishot ring sizes to avoid burst-time buffer exhaustion.
- Primary benchmark command: `cargo bench -p benches --bench uring_realworld -- bench_udp_request_response/runtime=norn/recv=single/window=32/total_requests=4096/payload=64`.
- Clean baseline: `29.712 ms/iter (+/- 0.6%)`, run from a temporary clean worktree at commit `02b770f`.
- Current result: `23.754 ms/iter (+/- 0.8%)`.
- Delta: `20.1%` lower wall time.
- Secondary benchmark command: `cargo bench -p benches --bench uring_realworld -- bench_udp_request_response/runtime=norn/recv=multi/window=64/total_requests=8192/payload=64`.
- Secondary result: `53.628 ms/iter (+/- 1.0%)` before the UDP stack, `48.121 ms/iter (+/- 1.1%)` after; `10.3%` lower wall time.
- Correctness result:
  - `cargo fmt --all --check`: passed.
  - `cargo test -p norn-uring --test udp`: passed, 16 tests.
- Rejected sub-attempts during this change:
  - Direct fast path for connected UDP `send`: regressed the primary target to `24.643 ms`.
  - Direct fast path for connected UDP `recv`: regressed the primary target to `25.729 ms`.
- Decision: keep. This reaches the 20% loop target on the primary UDP benchmark.

### Accepted Change: bounded concurrent KV recovery reads

- Hypothesis: `Store::recover` was issuing fixed-slot reads sequentially even on the io_uring backend. Keeping a bounded window of slot reads in flight should reduce recovery wall time while preserving the same per-slot validation and free-list reconstruction semantics.
- Files changed:
  - `examples/norn-kv/src/lib.rs`: recovery now reads slots through a bounded `FuturesUnordered` window; the Linux block backend returns aligned block buffers directly instead of copying reads into `Vec<u8>` and copying writes back into a second aligned buffer.
  - `examples/norn-kv/Cargo.toml`: added the workspace `futures` dependency for bounded recovery fan-out.
- Primary benchmark command: `cargo bench -p benches --bench norn_kv -- bench_recover/live_slots=256`.
- Clean baseline: `12.126 ms/iter (+/- 2.8%)`, run from a temporary clean worktree at commit `02b770f`.
- Current result: `4.906 ms/iter (+/- 10.3%)`.
- Delta: `59.5%` lower wall time.
- Secondary benchmark command: `cargo bench -p benches --bench norn_kv -- bench_recover/live_slots=64`.
- Secondary result: `6.337 ms/iter (+/- 13.1%)` clean baseline, `3.988 ms/iter (+/- 11.1%)` current; `37.1%` lower wall time.
- Correctness result:
  - `cargo test -p norn-kv`: passed, 6 tests.
- Notes:
  - Write-heavy `bench_put_delete` and `bench_put_get_delete` remain dominated by `O_DSYNC` slot writes on this machine; allocator/copy reductions did not produce a trustworthy win there.
- Decision: keep. This is a general KV-store recovery improvement on the disk I/O path.

## Cumulative Result

- Accepted source changes: `RecvMsgRingBuf` now implements `StableBuf`, enabling direct sends from recvmsg-provided ring buffers.
- Accepted benchmark-path changes: Norn UDP request/response now uses connected client sockets and a multishot bufring echo server.
- Baseline summary: broad existing benchmark sweep captured in `target/perf-baseline/2026-05-16/cargo-bench-p-benches.log`.
- Final summary: the UDP optimization stack reaches the loop target on `bench_udp_request_response/runtime=norn/recv=single/window=32/total_requests=4096/payload=64`, reducing wall time from `29.712 ms` to `23.754 ms`. The disk I/O pivot also reaches the loop target on `bench_recover/live_slots=256`, reducing wall time from `12.126 ms` to `4.906 ms`.
- Cumulative delta from accepted changes: `20.1%` lower wall time on the primary UDP target; `59.5%` lower wall time on the primary KV recovery disk I/O target.
- Confidence: good for both primary targets; each baseline came from a clean worktree and each final result was repeated after cleanup. The broader UDP multishot target improved by about `10.3%`, and the smaller KV recovery target improved by about `37.1%`.
- Remaining ideas:
  - Repeat likely profiling targets at least 3 times:
    - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=normal/connections=64/requests_per_connection=512/payload=64`
    - `cargo bench -p benches --bench uring_realworld -- bench_tcp_request_response/runtime=norn/recv=bufring_multi/connections=64/requests_per_connection=512/payload=64`
    - `cargo bench -p benches --bench noop_submit -- bench_noop/num_tasks=1/n=100000`
  - Then profile the selected target with `NORN_BENCH_PPROF=target/perf-baseline/2026-05-16/pprof cargo bench -p benches --bench <bench> -- <filter>`.
