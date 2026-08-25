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

Pending baseline capture.

## Baseline

Pending baseline capture.

## Profiling

Pending baseline capture.

## Optimization log

No optimization attempted before the baseline.
