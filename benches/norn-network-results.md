# Norn network-only benchmark

This benchmark isolates the server's TCP, framing, batching, executor, and
cross-worker channel paths from key/value storage. In fixed-response mode each
worker owns one immutable payload and performs no `HashMap` lookup or mutation.

## Contract

- Server: release-mode `norn-kv-server`, multishot receive, loopback TCP.
- Workload: Memcached-binary GET requests with a fixed successful response.
- Payload sizes: 64, 256, 1024, and 4096 bytes. Each wire response also has a
  24-byte header and four-byte flags field.
- Topologies: 1, 2, and 4 workers pinned to CPUs `0`, `0,1`, and `0,1,2,3`.
- Client: 16 memtier threads pinned to CPUs `8-15,24-31`.
- Pipelines: 1 and 32.
- Repetitions: three full-matrix trials per configuration; repeat important
  cases before accepting an optimization.
- Work per trial: 4,000,000 completions at pipeline 1 and 32,000,000 at
  pipeline 32. The lower-throughput p1 pilot ran for 288 seconds at 32 million;
  four million still keeps the known approximately one-second memtier
  progress/termination cadence near 3% while avoiding an uninformative
  multi-hour matrix.
- Primary metric: exact completed operations divided by external memtier
  process wall time.
- Guardrails: exact expected completion count, zero misses, zero connection
  errors, and reported p99/p99.9 latency.

The default full-matrix command is:

```console
nix develop -c cargo build --release -p norn-kv-server --locked
nix develop -c ./hack/bench-norn-network-only.sh
```

Raw logs are written under `benches/logs/norn-network-only-<timestamp>/` and
are ignored by Git.

## Environment

- Host: AMD Ryzen 9 5950X (16 cores / 32 threads), 64 GiB RAM.
- Kernel: NixOS Linux 6.18.44, x86-64.
- Toolchain: `rustc 1.99.0-nightly (0e29c21d9 2026-07-21)`.
- Client: `memtier_benchmark 2.1.2`.
- Baseline revision: `4836757a55b5f36bf44fcf341e5ee21d593de3fd`.
- Full-matrix raw log:
  `benches/logs/norn-network-only-20260825T122805Z/matrix.log`.

The host was otherwise live, not isolated. Pipeline-32 wall times land close to
memtier's approximately one-second progress/termination cadence, so small
differences should be treated as noise. The broad scaling results and rejected
large regressions were repeated separately.

## Baseline

Each cell is the median of three valid trials. Throughput is exact completions
divided by external client wall time; latency columns are memtier percentiles.

| Payload | Pipeline | Workers | ops/s | p50 ms | p99 ms | p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 1 | 1 | 110,944 | 1.111 | 1.327 | 12.359 |
| 64 | 1 | 2 | 181,426 | 0.591 | 0.975 | 3.383 |
| 64 | 1 | 4 | 332,029 | 0.319 | 0.527 | 0.847 |
| 64 | 32 | 1 | 2,903,542 | 0.343 | 0.391 | 0.519 |
| 64 | 32 | 2 | 2,130,416 | 0.431 | 0.711 | 1.143 |
| 64 | 32 | 4 | 2,903,639 | 0.263 | 0.471 | 1.007 |
| 256 | 1 | 1 | 107,955 | 1.127 | 1.727 | 12.935 |
| 256 | 1 | 2 | 181,431 | 0.591 | 0.975 | 3.311 |
| 256 | 1 | 4 | 331,943 | 0.319 | 0.559 | 1.863 |
| 256 | 32 | 1 | 1,997,332 | 0.503 | 0.639 | 0.991 |
| 256 | 32 | 2 | 1,598,231 | 0.543 | 1.127 | 2.999 |
| 256 | 32 | 4 | 1,880,015 | 0.415 | 0.887 | 3.399 |
| 1024 | 1 | 1 | 105,115 | 1.159 | 1.287 | 12.487 |
| 1024 | 1 | 2 | 199,506 | 0.607 | 0.943 | 3.399 |
| 1024 | 1 | 4 | 332,054 | 0.327 | 0.535 | 0.727 |
| 1024 | 32 | 1 | 1,141,941 | 0.847 | 1.607 | 1.951 |
| 1024 | 32 | 2 | 799,466 | 0.887 | 3.095 | 5.223 |
| 1024 | 32 | 4 | 1,141,837 | 0.671 | 1.951 | 4.135 |
| 4096 | 1 | 1 | 97,436 | 1.279 | 1.463 | 12.903 |
| 4096 | 1 | 2 | 166,273 | 0.695 | 1.119 | 9.303 |
| 4096 | 1 | 4 | 284,762 | 0.375 | 0.631 | 1.383 |

At pipeline 1, two workers improve throughput by 64-90% and four workers by
192-216% over one worker. At pipeline 32, routing a storage-free response over
the shard channels costs 20-30% at two workers; four workers recover to roughly
the one-worker result. This is useful separation: reuse-port workers scale
latency-bound connections well, while the fixed-response saturated workload
does not benefit from key-owner routing.

The 4096-byte pipeline-32 case is excluded. The one-worker trial completed only
28,283,237 of 32,000,000 requests after six connection resets, while the server
remained alive. The same workload using exact receives completed all 32 million
requests at 100,610 ops/s with zero misses or errors. The multishot failure is
tracked in [issue #126](https://github.com/gardnervickers/norn/issues/126).

## Profiling

A per-process `perf` capture of the 256-byte, pipeline-32, four-worker routed
case collected 59,421 user-cycle samples. System-wide profiling was unavailable
because the host has `perf_event_paranoid=2`; running the server as the profiled
child with a one-page perf mmap buffer avoided changing host state and the
io_uring locked-memory limit.

| Top-frame group | Samples |
| --- | ---: |
| Other resolved symbols | 31.90% |
| Unresolved | 27.90% |
| `memmove` / `memcpy` | 12.39% |
| Allocator and `Vec` growth | 9.49% |
| Channel synchronization and drain | 5.73% |
| Shard response queue | 4.36% |
| Shard request queue | 3.29% |
| Frame decode | 3.25% |
| Owned command copy | 1.69% |

The profile supports copies, allocation, and cross-worker coordination as the
main removable costs in fixed-response mode. It does not identify a generally
safe runtime-level change: the two focused attempts below had payload-dependent
or negative results.

## Optimization log

No optimization was retained.

- Execute storage-free keyed requests on their accepting reuse-port worker.
  Pipeline-32 throughput improved from 2.90M to 6.38M ops/s at 64 bytes and
  from 1.88M to 2.90M ops/s at 256 bytes. Pipeline-1 at 256 bytes stayed flat
  at 332K ops/s. At 1024 bytes, however, throughput regressed from 1.14M to
  680K ops/s. The change was rejected because the benchmark contract spans
  payload sizes and a response-size cutoff would be benchmark-specific policy.
- Preallocate the exact fixed-response wire size for each response `Vec` while
  retaining key-owner routing. The 256-byte result was not reliably different,
  and the 1024-byte median fell to 820K ops/s. An unchanged-code refresh median
  was 999K ops/s, confirming an approximately 18% paired regression despite
  host variance. The change was rejected.

The next material opportunity requires a different response representation:
avoid constructing one owned `Vec` per command and copying it again into the
connection output batch. That would affect response ordering, cross-worker
ownership, and send lifetimes, so it should be designed and benchmarked as a
separate change rather than folded into this harness.

## 2026-08-25 deferred fixed-response follow-up

Goal: keep key-owner routing and ordered replies while replacing each fixed GET
response's temporary owned `Vec` with a compact deferred descriptor. The origin
worker can then encode directly into its reusable connection output batch,
removing one allocation and one payload copy per command.

The target remains this local Ryzen 9 5950X host. The primary metric, workload,
correctness guards, CPU placement, and toolchain are unchanged from the contract
above. Raw baseline logs are in `/tmp/norn-response-baseline-p32-w4` and
`/tmp/norn-response-baseline-p1-w4`.

### Fresh baseline at `d4e81d8`

| Payload | Pipeline | Workers | Median ops/s | p50 ms | p99 ms | p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 32 | 4 | 2,662,211 | 0.271 | 0.583 | 1.991 |
| 256 | 32 | 4 | 1,775,695 | 0.423 | 0.887 | 2.511 |
| 1024 | 32 | 4 | 999,244 | 0.679 | 2.191 | 4.095 |
| 256 | 1 | 4 | 332,005 | 0.319 | 0.535 | 0.967 |

Every trial completed its exact expected operation count with zero misses and
zero connection errors. The individual throughput trials still show the host's
one-second wall-time quantization and background variance, so candidates must
produce broad, repeated changes rather than rely on a close single comparison.

### Attempt 1: defer every fixed GET response

Hypothesis: pass only the request header back from the key owner and encode the
fixed response directly into the origin connection's reusable output batch.
This removes every fixed GET response allocation and its second payload copy.

| Payload | Pipeline | Baseline ops/s | Candidate ops/s | Delta | Candidate p99 ms | Candidate p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 32 | 2,662,211 | 3,192,653 | +19.9% | 0.463 | 0.591 |
| 256 | 32 | 1,775,695 | 1,997,133 | +12.5% | 0.799 | 2.159 |
| 1024 | 32 | 999,244 | 888,286 | -11.1% | 2.439 | 4.239 |
| 256 | 1 | 332,005 | 332,049 | +0.0% | 0.535 | 0.871 |

Raw logs are in `/tmp/norn-response-deferred-p32-w4` and
`/tmp/norn-response-deferred-p1-w4`. A second three-trial 1024-byte run in
`/tmp/norn-response-deferred-p32-w4-repeat` reproduced an 888,196 ops/s median,
so the large-payload regression is not accepted as noise. All trials passed the
exact-count, miss, and connection-error guards.

Decision: revise. Deferring remote responses moves response construction from
the hash owners onto connection-owning workers. That is valuable for small
responses but removes useful work distribution at 1024 bytes. The next attempt
defers only owner-local fixed GETs; remote owners retain the existing response
construction path.

### Attempt 2: defer only owner-local fixed GETs

Hypothesis: remove the temporary allocation and copy for the approximately 25%
of requests whose hash owner is also the connection worker, while preserving
remote response construction and work distribution.

| Payload | Pipeline | Baseline ops/s | Candidate ops/s | Delta | Candidate p99 ms | Candidate p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 32 | 2,662,211 | 2,456,967 | -7.7% | 0.567 | 2.407 |
| 256 | 32 | 1,775,695 | 1,598,211 | -10.0% | 0.951 | 2.527 |
| 1024 | 32 | 999,244 | 1,031,487 | +3.2% | 2.103 | 3.727 |

Raw logs are in `/tmp/norn-response-local-deferred-p32-w4`. Every trial passed
the correctness guards. Decision: reject. The gains are neither broad nor above
the host's variance, and the small-payload regressions are material.

The next attempt preserves owner-built response buffers and targets only the
second copy by sending the ordered buffers as a vectored batch from the origin
worker.

### Attempt 3: always send ordered response buffers as a vectored batch

Hypothesis: retain response construction on hash owners, keep the ordered owned
buffers alive at the origin, and send them with readiness-driven `writev`
instead of copying them into one contiguous buffer for an io_uring send.

| Payload | Pipeline | Baseline ops/s | Candidate ops/s | Delta | Candidate p99 ms | Candidate p99.9 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 32 | 2,662,211 | 2,662,021 | -0.0% | 0.535 | 0.991 |
| 256 | 32 | 1,775,695 | 1,598,201 | -10.0% | 0.863 | 3.063 |
| 1024 | 32 | 999,244 | 1,102,490 | +10.3% | 1.287 | 4.271 |

Raw logs are in `/tmp/norn-response-vectored-p32-w4`; every trial passed the
correctness guards. Decision: revise. Vectored readiness writes have a fixed
cost that is not repaid by small response copies, but avoiding the copy is
material at 1024 bytes. The next attempt retains contiguous io_uring sends for
responses below 1 KiB and selects vectored batches for larger responses.

### Attempt 4: specialize each connection by fixed response size

Hypothesis: select a monomorphized contiguous or vectored output implementation
once when the connection starts. Responses below 1 KiB use the original hot
loop and io_uring send; larger responses retain owner-built buffers and use
vectored readiness writes.

The comparison below uses the immediate paired baseline in
`/tmp/norn-response-paired-baseline-p32-w4`, not the noisier morning baseline.

| Payload | Pipeline | Baseline ops/s | Candidate ops/s | Delta | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 256 | 32 | 1,775,608 | 1,775,655 | +0.0% | 0.943 | 0.903 |
| 1024 | 32 | 888,261 | 1,184,091 | +33.3% | 2.447 | 1.191 |
| 256 | 1 | 306,575 | 306,609 | +0.0% | 0.583 | 0.575 |
| 1024 | 1 | 331,946 | 265,828 | -19.9% | 0.567 | 0.615 |

Candidate logs are in `/tmp/norn-response-specialized-p32-w4` and
`/tmp/norn-response-specialized-p1-w4`; the paired pipeline-1 baseline is in
`/tmp/norn-response-paired-baseline-p1-w4`. Every trial passed the correctness
guards. Decision: revise. The pipeline-32 gain is material and the small path is
isolated successfully, but a one-buffer 1 KiB batch does not amortize the
readiness/writev path. The next revision sends a single owned buffer through
the existing io_uring path and reserves writev for multi-buffer batches.

### Attempt 5: use io_uring for a one-buffer large-response batch

Hypothesis: retain vectored sends only when the ordered batch actually contains
multiple buffers. A single large response can be submitted directly as its
existing owned buffer, avoiding both the second copy and writev readiness cost.

| Payload | Pipeline | Paired baseline ops/s | Candidate ops/s | Delta | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1024 | 32 | 888,261 | 1,184,184 | +33.3% | 2.447 | 1.447 |
| 1024 | 1 | 331,946 | 331,997 | +0.0% | 0.567 | 0.551 |

Raw candidate logs are in `/tmp/norn-response-final-p32-w4` and
`/tmp/norn-response-final-p1-w4`. All trials passed the correctness guards.
Decision: accept, subject to one crossover calibration at a 512-byte payload.
The large pipelined gain repeated after the fallback, and the single-response
guardrail returned to baseline.

### Attempt 6: calibrate the threshold at 512 bytes

The 512-byte payload has a 540-byte wire response and therefore exercises the
lower 512-byte vectored threshold.

| Pipeline | Baseline ops/s | Candidate ops/s | Delta | Baseline p99 ms | Candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 1,453,006 | 1,682,274 | +15.8% | 1.255 | 0.975 |
| 1 | 332,034 | 332,010 | -0.0% | 0.543 | 0.551 |

Pipeline-32 logs are in `/tmp/norn-response-512-baseline-p32-w4` and
`/tmp/norn-response-512-vectored-p32-w4`. Pipeline-1 logs are in
`/tmp/norn-response-512-baseline-p1-w4` and
`/tmp/norn-response-512-final-p1-w4-repeat`. The first pipeline-1 candidate set
landed one coarse timing bucket lower; the immediate repeat recovered to the
baseline median. All trials passed the correctness guards. Decision: accept the
512-byte threshold.

### Final retained result

The retained design selects one output implementation per fixed-response
connection. Wire responses below 512 bytes retain the original contiguous
io_uring path. Larger responses preserve the owner-built buffers and avoid the
second copy with readiness-driven vectored writes when a batch has multiple
buffers; a one-buffer batch submits that owned buffer through io_uring.

| Payload | Pipeline | Paired/fresh baseline ops/s | Final ops/s | Delta | Baseline p99 ms | Final p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 64 | 32 | 2,662,211 | 2,902,925 | +9.0% | 0.583 | 0.487 |
| 256 | 32 | 1,775,608 | 1,775,655 | +0.0% | 0.943 | 0.903 |
| 512 | 32 | 1,453,006 | 1,682,274 | +15.8% | 1.255 | 0.975 |
| 1024 | 32 | 888,261 | 1,184,184 | +33.3% | 2.447 | 1.447 |
| 256 | 1 | 306,575 | 306,609 | +0.0% | 0.583 | 0.575 |
| 512 | 1 | 332,034 | 332,010 | -0.0% | 0.543 | 0.551 |
| 1024 | 1 | 331,946 | 331,997 | +0.0% | 0.567 | 0.551 |

The 64-byte comparison uses the fresh baseline at the start of this follow-up;
the other final comparisons use immediate paired baselines. The host remains
noisy and wall times are coarsely quantized, so the 64-byte number is treated as
a no-regression guardrail. The repeated 512-byte and 1024-byte gains are large
enough to clear that limitation.

Every retained candidate trial completed the exact expected operation count
with zero misses and zero connection errors. Final validation passed:

```text
cargo fmt --all -- --check
cargo test --workspace --all-features --locked
cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
/run/current-system/sw/bin/nix build
```

## 2026-08-27 current-master revalidation

The candidate was rebased onto `c4b46dd`, after the networking correctness PRs
and public-API flag cleanup. The earlier follow-up's readiness cancellation and
`MSG_NOSIGNAL` changes were deliberately omitted: the retained public API is the
dedicated ownership-bearing `TcpSocket::writer()`, and the KV connection drops
its receive, writer, and socket together so descriptor reclamation remains at
the terminal-cancellation boundary.

### Issue #126 correctness baseline

The exact one-worker, 4,096-byte, pipeline-32 issue #126 workload completed
three current-master trials of 32,000,000 operations each without misses,
connection errors, or server-side failures:

| trial | throughput (ops/s) | p50 ms | p99 ms | p99.9 ms |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 249,929.39 | 4.591 | 6.623 | 13.935 |
| 2 | 251,893.10 | 4.551 | 6.559 | 12.175 |
| 3 | 251,896.81 | 4.551 | 6.399 | 10.759 |

The throughput median is `251,893.10 ops/s` with 0.78% peak-to-peak spread.
The previously observed reset did not reproduce across 96,000,000 operations.
Raw output is under `/tmp/norn-issue-126-baseline-20260827/` and
`/tmp/norn-issue-126-baseline-repeat-20260827/`.

### Four-worker macro calibration

The historical 32-connection reuse-port shape remains the representative
latency workload, but absolute throughput varied by 45.8% across an initial
three-trial baseline. Increasing the population to 128 connections still
varied by 14.3-23.8%; 512 connections reduced placement variance while
distorting the workload to 111-124 ms p99 latency. Those calibration logs are
under `/tmp/norn-response-current-master-1024-p32-w4-20260827/` and the
`/tmp/norn-response-calibration*-20260827/` directories.

Decision: retain 32 connections, compare alternating full-workload pairs, and
report paired medians and dispersion instead of treating one aggregate median
as stable.

### Five paired 1,024-byte pipeline-32 runs

Every run completed exactly 32,000,000 operations with zero misses and zero
connection errors.

| pair | baseline ops/s | candidate ops/s | delta | baseline p99 ms | candidate p99 ms |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 999,228.93 | 1,065,817.03 | +6.66% | 2.135 | 1.639 |
| 2 | 999,197.03 | 1,102,478.01 | +10.34% | 2.191 | 1.535 |
| 3 | 913,652.10 | 940,479.54 | +2.94% | 2.423 | 1.959 |
| 4 | 999,210.41 | 1,278,797.03 | +27.98% | 2.151 | 1.311 |
| 5 | 913,627.72 | 1,102,523.51 | +20.68% | 2.431 | 1.615 |

The paired throughput median is `+10.34%` with 7.40 percentage-point MAD.
Median throughput is `999,197.03` versus `1,102,478.01 ops/s`. p99 improved in
all five pairs, with a paired median of `-29.94%` and 6.71-point MAD; the raw
medians are `2.191` versus `1.615 ms`. p99.9 did not produce a stable claim.
Raw output is under `/tmp/norn-response-paired-20260827/`.

### Guardrails

The 1,024-byte pipeline-1 path, which uses the existing single-buffer io_uring
send, measured `284,731.52` versus `306,546.35 ops/s` (+7.66%) and median p99
of `0.599` versus `0.559 ms`. Raw output is under
`/tmp/norn-response-guard-{baseline,candidate}-1024-p1-w4-20260827/`.

Five alternating 256-byte pipeline-32 pairs exercised the original contiguous
output path below the vectored threshold. Paired median throughput was
`-0.002%` with 9.99-point MAD; raw medians were `1,775,652.46` versus
`1,775,601.30 ops/s`. Median p99 was `0.935` versus `0.927 ms`. p99.9 was noisy
and moved against the candidate in four pairs, so no small-response tail claim
is made. Raw output is under `/tmp/norn-response-paired-256-20260827/`.

Decision: retain. The current paired result is smaller than the earlier 33.3%
headline but remains material: large pipelined responses improve by 10.34% in
throughput and 29.94% in p99, while low-pipeline and small-response guard
throughput do not regress.
