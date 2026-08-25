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
