# norn-kv-bench

The end-to-end benchmark for driving runtime optimization. It runs a mixed
memcached-binary workload against `norn-kv-server`. Every run produces
low-noise **server cost per request** (CPU time, instructions, cycles, context
switches) at a saturating load and at fixed offered rates, plus coordinated
omission-corrected latency per request class.

The client deliberately depends on no Norn crate: it is a separate epoll-based
implementation, so a runtime change under test cannot change the load
generator. Every response is validated byte-for-byte.

## Why this benchmark

Earlier end-to-end harnesses could not resolve changes below about 5%:

- memtier wall time quantizes to 1-second buckets;
- the Norn-on-Norn `norn-kv-loadgen` changed with the runtime under test;
- the scheduled v1 workload ran at 20k requests/s, far below capacity, where
  p99 is dominated by loopback and wakeup latency (it saw no repeatable signal
  for PR #141).

This harness instead reads exact per-thread server counters at phase
boundaries and normalizes them by validated completions. On the development
workstation, server user instructions per request in the capacity segment
repeated within 0.3% across ten runs while the host was heavily loaded
(isolated as described under [Host preparation](#host-preparation)). A
synthetic +1% per-request cost measured +1.6–1.8% in every pair. See
[`benches/norn-kv-bench-results.md`](../../benches/norn-kv-bench-results.md).

## Quick start

```sh
nix develop -c cargo build --release -p norn-kv-server -p norn-kv-bench

# Conformance smoke (about 3 s).
target/release/norn-kv-bench run --profile examples/norn-kv-bench/profiles/smoke.json \
  --server target/release/norn-kv-server --server-cpus 0 --client-cpus 8,9

# One measured run of the primary profile (about 35 s).
target/release/norn-kv-bench run --profile examples/norn-kv-bench/profiles/mixed-1w.json \
  --server target/release/norn-kv-server --server-cpus 0 --client-cpus 8-11
```

To compare two builds, freeze both server binaries outside `target/`. Run
interleaved pairs; ordering alternates ABBA to cancel linear host drift:

```sh
target/release/norn-kv-bench compare --profile examples/norn-kv-bench/profiles/mixed-1w.json \
  --baseline /tmp/base/norn-kv-server --candidate /tmp/cand/norn-kv-server \
  --pairs 3 --server-cpus 0 --client-cpus 8-11
```

Results go to `benches/logs/norn-kv-bench/<timestamp>-<profile>/` (ignored by
git), or to `--out DIR`. Each run directory contains:

- `result.json` (everything below, plus provenance),
- `summary.txt`,
- the server's stdout and stderr.

`compare` also writes `compare.txt` and `compare.json`.

## What a run does

1. Start the server pinned to `--server-cpus` and wait for its announced
   listener. Norn servers are started with `--listen=127.0.0.1:0`,
   `--workers=N`, and `--worker-cpus`.
2. Connect the persistent connections and populate every key with pipelined
   SETs. Each client thread is pinned to one of `--client-cpus`.
3. Run the timeline. Each measured segment discards `settle_ms` before
   measuring:

   | Segment | Load | Purpose |
   | --- | --- | --- |
   | `warmup` | closed loop | Allocator, page, and cache warmup; not reported |
   | `capacity` | closed loop, `closed_loop_depth` outstanding per connection | Saturated cost per request and throughput |
   | `open-<rate>` | open loop, Poisson arrivals at an absolute rate | Cost per request and latency at a fixed load |

4. Drain all outstanding requests, then read back every connection-owned key
   and check that it holds the last version written.
5. Terminate the server and fail the run if it exited early.

The coordinator snapshots every server thread at each measured window's
boundaries. It records:

- `/proc/PID/task/*/schedstat` for nanosecond CPU time and run-queue wait
  (time runnable but waiting for a CPU);
- the `stat` user/system split;
- voluntary and involuntary context switches;
- per-thread `perf_event_open` counters for instructions, user-space
  instructions, and cycles.

Perf counts are scaled for multiplexing per interval, from raw count,
enabled, and running times. Threads are rediscovered at every snapshot, but a
thread that starts or exits within a window has incomplete counts, so the
window is disqualified (`thread-churn`). Per-request metrics divide by the
completions within the coordinator's actual sampling interval, taken from a
100 us completion-time histogram, and a boundary sampled more than 0.5% of
the window late is disqualified (`sample-skew`). Client threads wait for the
final snapshot before their readback traffic. The coordinator also reads `/proc/stat` for the server CPUs and
their SMT siblings.

The summary's `util` column is server thread CPU time per worker. `core` is
the server CPUs' busy time and `irq` its interrupt share. Over loopback,
the softirq work that delivers the server's sends runs on the server CPU,
but it is charged to no thread: a saturated single-worker server shows
`util` near 71% with `core` at 100% and `irq` near 29%. `wait` is the
largest share of the window a server thread spent runnable but waiting for
a CPU.

## Workload

The `mixed-1w` (one server worker) and `mixed-4w` (four workers, cross-worker
dispatch through `norn-channel`) profiles share one traffic shape:

| Feature | Setting |
| --- | --- |
| Keyspaces | 100k small (64–512 B), 10k medium (2–8 KiB), Zipf 0.99; 512 large (32–48 KiB), uniform |
| Request mix | 60% small GET, 14% medium GET, 2% large GET, 6% GET miss, 12% SET, 6% read-your-write GET |
| Bursts | Each arrival writes 1 (80%), 8 (15%) or 32 (5%) back-to-back requests on one connection |
| Connections | 128 (`1w`) or 256 (`4w`) persistent; plus short-lived churn connections (500/s or 1000/s, 4 GETs each) |
| SET targets | Slots owned by one connection, so every read of a written key has exactly one valid value |

The traffic exercises:

- multishot accept and connection-task spawn and teardown (churn);
- multishot receive with provided buffers and batched sends;
- frame reassembly across buffers (bursts and large values);
- the park and wake path at low load and batching at high load;
- in `mixed-4w`, credited cross-worker request and response channels.

A profile is versioned JSON validated on load. Keep a profile unchanged
across a comparison; rates are absolute requests/s, not fractions of
capacity. `done/s` includes churn-connection requests.

## Metrics: which one to use

| Metric | Use it for | Noise |
| --- | --- | --- |
| `uinst/op`: server user-space instructions per request | Runtime and server code-path changes | Lowest. Insensitive to frequency and cache state, but it varies with batching, so the server must be saturated (capacity) or the host quiet |
| `inst/op`: user + kernel instructions | Changes to syscall counts, batching, or io_uring usage | Low, with the same batching caveat |
| `cpu/op` (ns) and `cyc/op` | Real cost, including cache misses, IPC, and frequency | Needs a quiet, pinned host |
| `csw/op` | Parks per request; wakeup and batching behaviour | Moderate |
| capacity `done/s` | Throughput ceiling | Needs a quiet host, server `core` ≥ 90%, and idle SMT siblings |
| per-class `p50`/`p99`/`p99.9` | Latency at fixed offered load, measured from the intended arrival | Needs a quiet host; read the open segments, not `capacity` (closed-loop queueing) |

Cost per request depends strongly on load. At 20k/s nearly every request
pays a full park → `io_uring_enter` → wake cycle. At high rates the same work
is amortized over large batches. Compare like with like (same segment), and
expect an optimization to show up in some segments and not others.

## Validity and qualification

`valid` requires all of the following:

- every response matched its request's opaque value and expected
  status/opcode;
- every value was byte-identical to the expected payload;
- no connection failed;
- all work drained;
- the final readback passed;
- the server was still running at the end.

An invalid run is a correctness failure; investigate it. Invalid results are
never compared.

`qualified` additionally requires a measurement environment that did not
distort the result:

- server and client threads were preempted at most `max_preemptions_per_s`
  (default 20/s) in every window, and no thread waited for a CPU for more than
  `max_run_delay_fraction` (default 1%) of a window. Both detect a busy host;
  the wait check also catches rare preemptions that each cost tens of
  milliseconds;
- open-loop arrival lag p99 ≤ `max_lag_p99_us`;
- every open-loop segment completed 98–102% of its realized arrivals, with no
  arrivals dropped at `max_outstanding_per_connection`. This detects an
  offered rate above capacity;
- in the capacity segment, the pinned server CPUs were at least
  `min_capacity_utilization` busy, including interrupt time; unpinned servers
  use thread CPU time. This detects a client-bound capacity measurement;
- no unused SMT sibling of a server CPU was more than
  `max_sibling_busy_fraction` (default 5%) busy. On the development
  workstation, server IPC was about 0.3 with a busy sibling and about 0.5
  with a mostly idle one;
- no server thread started or exited in the window, perf counters ran at
  least 99% of their enabled time (no multiplexing by another perf user), and
  the coordinator sampled within 0.5% of the window of each boundary;
- client and server were pinned, with no shared physical cores.

On an unqualified run, capacity instruction counts are still a useful hint.
Time, throughput, latency, and open-loop instruction counts are not.
Preemption changes how much work each wakeup batches.

## Comparing builds

`compare` runs `--pairs` baseline/candidate pairs, each on a fresh server.
It reports, per metric:

- the medians of each build;
- the median per-pair delta and its range;
- the within-build spread.

The verdict is `better` or `worse` only when every pair agrees in sign and
the median delta exceeds half the within-build spread, and `noise` in all
other cases. If any run failed qualification for that segment, the verdict
is prefixed: `unqualified/worse` is a hint for whether a qualified rerun is
worthwhile, not a result. Fewer than three pairs in which both runs report
the metric (column `n`) reports `too few pairs`.

Recommended practice:

- Use three pairs for instruction metrics and five or more for time,
  throughput, and latency metrics on a quiet host.
- Freeze both binaries outside `target/`. Build each from a clean worktree;
  check the recorded SHA-256s.
- Keep the profile, CPU assignment, and client binary identical. The client
  binary hash is recorded too.
- Record immediate-parent comparisons separately from whole-stack comparisons.
- Keep the component benchmarks in `benches/` as diagnostics for *why* a
  number moved.

## Host preparation

- Pin both sides: `--server-cpus` (one CPU per server worker) and
  `--client-cpus` (at least `client.threads` CPUs). The tool rejects server
  and client CPUs on the same physical core. On the 5950X workstation, CPU
  *N* and *N*+16 are SMT siblings.
- Client threads busy-poll near arrivals and in closed loop, so give them
  dedicated cores.
- Use the `performance` governor. Frequency boost is recorded; disable it for
  `cpu/op` and `cyc/op` comparisons if possible.
- Hardware counters need `kernel.perf_event_paranoid` ≤ 1 for kernel-inclusive
  instructions. At 2 only `uinst/op` is available.
- Run on a quiet host. The preemption and run-queue wait counters show
  whether it was.
- If other work shares the host, run the benchmark in its own top-level
  cgroup so the scheduler shares CPU between cgroups rather than between
  threads, for example
  `systemd-run --user --scope -- target/release/norn-kv-bench ...`.
  It cannot match a quiet host, but where the benchmark shared a service
  cgroup with about 90 runnable nice-5 threads, it reduced
  server preemptions at capacity from 230/s to under 10/s. Each result
  records its cgroup and the starting load average in `provenance`.
- On a shared host, `--hold-cpus` occupies the server CPUs' SMT siblings
  with a `pause` loop, so other work cannot run there. A held sibling still
  shares the core, so this is more repeatable than a busy sibling but slower
  than an idle one. Work that displaces the holder counts toward the sibling
  check. The held CPUs are recorded in `provenance`.

## External servers

`--server-kind generic` starts any memcached-binary server. `{port}` and
`{workers}` are substituted in `--server-arg` values, and readiness is a TCP
connect probe. For example, memcached from the dev shell:

```sh
target/release/norn-kv-bench run --profile examples/norn-kv-bench/profiles/mixed-1w.json \
  --server-kind generic --server "$(nix develop -c sh -c 'command -v memcached')" \
  --server-arg=-p --server-arg={port} --server-arg=-t --server-arg={workers} \
  --server-arg=-m --server-arg=4096 --server-arg=-l --server-arg=127.0.0.1 \
  --server-cpus 0 --client-cpus 8-11
```

`--address HOST:PORT` drives an already running server, without server
counters.

## Limitations

- Loopback only. Part of the kernel network cost of the server's sends is
  charged to the receiving (client) CPUs, and NIC effects are absent.
- In `mixed-4w`, `SO_REUSEPORT` places connections on workers by hash, which
  varies from run to run. Check `cpu_ns_by_thread` in `result.json` for
  imbalance before trusting a 4-worker time metric.
- The offered rates are calibrated for the development workstation. On
  another host, measure `capacity` first and choose rates around 10%, 40%,
  and 80% of it, then freeze them in a copied profile.
- An open-loop rate above capacity drives the server into overload. The
  sharded server currently resets connections when multishot receive
  exhausts its provided-buffer ring (`ENOBUFS`), which makes the run invalid.

## Tests

```sh
nix develop -c cargo test -p norn-kv-bench
```

The tests include a full run against an in-process reference server, checking
accounting and readback, and a run against a server that corrupts values,
which must be detected.
