# Norn KV networking performance

## Goal

- Target: steady-state single-thread Norn TCP and Memcached-binary protocol
  throughput with in-memory storage.
- Primary metric: median successful requests per second; higher is better.
- Guardrails: p99 request latency, exact completed-request count, zero GET
  misses after prefill, protocol and response-body correctness, bounded
  connection count, and no client or server errors.
- Stop condition: no remaining repeatable material networking improvement, or
  further ideas add disproportionate complexity. Changes below approximately
  5% are treated as inconclusive unless profiling shows they remove a specific
  bottleneck without meaningful downside.

## Environment

- Machine: local `nixos` workstation.
- Execution mode: local loopback; server and load generator are separate
  processes with disjoint CPU affinity.
- OS: NixOS, Linux 6.18.44 x86_64.
- CPU: AMD Ryzen 9 5950X, 16 cores / 32 threads, frequency boost enabled,
  `performance` governor on the benchmark CPUs.
- Memory: 62 GiB, no swap.
- Storage: not in the timed path; source and logs reside on an ext4 NVMe
  filesystem.
- Runtime/toolchain: repository Nix shell, rustc 1.99.0-nightly
  (`0e29c21d9`, 2026-07-21), LLVM 22.1.8.
- External load generator: flake-pinned `memtier_benchmark` 2.1.2.
- CPU placement: server CPU 0; load generator CPUs 8-15 and 24-31. CPU 16,
  the SMT sibling of server CPU 0, is excluded.
- Worktree: `/tmp/norn-kv-network`, branch `codex/kv-network`, based on
  `b52226e54c520d367e38a571885bc8687c40c105`.

## Methodology

- Build command:
  `nix develop -c cargo build --release -p norn-kv-server`.
- Primary benchmark command:
  `nix develop -c ./hack/bench-norn-kv-memtier.sh <pipeline-depth>`.
- Paired external-reference command:
  `nix develop -c ./hack/bench-norn-kv-paired.sh <pipeline-depth> <pairs>`.
- Server: release build, one Norn executor, io_uring depth 256, persistent
  connections, in-memory storage, loopback TCP, `TCP_NODELAY`.
- Load generator: external `memtier_benchmark` using Memcached binary, 16
  threads and two persistent connections per thread on 16 isolated client
  CPUs. The earlier pipeline screen used eight threads and four connections
  per thread while holding the same 32 total connections.
- Primary workload: a prefilled 100,000-key cache, Gaussian hot-key selection,
  90% GET / 10% SET, and weighted values of 64 B (40%), 256 B (40%), 1 KiB
  (15%), and 4 KiB (5%).
- Warmup: 2,000 mixed requests per connection before each trial.
- Measurement: 50,000 mixed requests per connection, or 1,600,000 operations
  per trial. Pipeline depths 1, 4, 8, 16, 32, and 64 establish the operating
  curve; p32 is the final retained knee.
- Repetitions: five trials; compare medians and retain raw per-trial output.
- Correctness gate: memtier must complete the exact request count with zero
  misses or process errors. The protocol/unit/fragmentation suite exercises
  both receive modes, while the Norn load generator separately validates every
  response field and GET value byte.
- Secondary control command:
  `./hack/bench-norn-kv-network.sh <noop|get|set>`. NOOP remains useful for
  isolating runtime overhead but no longer decides application-path changes.

The first iteration treated the non-pipelined NOOP control as the primary
workload. Review identified that this could reject an optimization whose value
comes from parsing and replying to requests already queued in a socket. The
NOOP measurements remain below as historical control evidence; all final
decisions use the external mixed workload.

## Realistic mixed-workload baseline

The exact owned-buffer server was first screened across client pipeline depths
with eight memtier threads, four connections per thread, and five trials per
depth. Every trial completed 1,600,000 operations with zero misses.

| Pipeline | Median ops/s | Median p50 | Median p99 | Throughput range |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 111,391.93 | 0.287 ms | 0.327 ms | 110,869.55–111,685.52 |
| 4 | 113,277.34 | 1.127 ms | 1.335 ms | 111,941.19–114,813.19 |
| 16 | 111,909.91 | 4.575 ms | 5.103 ms | 110,916.28–115,878.51 |
| 64 | 110,195.66 | 18.543 ms | 19.695 ms | 110,020.72–114,568.74 |

Raw summaries:

- `benches/logs/norn-kv-memtier-20260820T202041Z-p1/summary.log`
- `benches/logs/norn-kv-memtier-20260820T202232Z-p4/summary.log`
- `benches/logs/norn-kv-memtier-20260820T202407Z-p16/summary.log`
- `benches/logs/norn-kv-memtier-20260820T202539Z-p64/summary.log`

Pipeline 4 was the initial development comparison point: it had the highest
exact-read baseline throughput without the queueing latency introduced at
depths 16 and 64. The final multishot/batched implementation changes this
curve; its retained operating knee is pipeline 32, measured below.

## Pipelined receive experiments

### Shared multishot receive without response batching

The first realistic comparison changed only receive framing: one shared
8,192-buffer provided-buffer ring, an armed multishot receive per connection,
direct parsing of complete selected-buffer frames, and copying only split
frames. Exact and multishot modes ran from the same release binary.

| Pipeline | Mode | Median ops/s | Median p99 | Delta vs exact |
| ---: | --- | ---: | ---: | ---: |
| 4 | Exact | 113,648.44 | 1.263 ms | — |
| 4 | Multishot | 116,888.07 | 1.223 ms | +2.85% |
| 16 | Exact | 111,428.74 | 5.215 ms | — |
| 16 | Multishot | 115,897.13 | 5.023 ms | +4.01% |

Raw summaries:

- `benches/logs/norn-kv-memtier-20260820T203317Z-p4/summary.log`
- `benches/logs/norn-kv-memtier-20260820T203453Z-p4/summary.log`
- `benches/logs/norn-kv-memtier-20260820T203631Z-p16/summary.log`
- `benches/logs/norn-kv-memtier-20260820T203811Z-p16/summary.log`

This reverses the NOOP-only direction and validates multishot for queued real
requests, but receive framing alone remains below the approximately 5%
retention threshold.

### Shared multishot receive with response batching: accepted

The controlled follow-up retains the same multishot parser and accumulates the
ordered replies for all complete requests in one selected receive buffer into
one send. Memcached binary permits concatenated ordered responses, and memtier
waits for and parses every response in each pipeline. The selected receive
buffer is returned to the shared ring before the batched send is awaited.

The primary matched comparison uses all 16 isolated client CPUs, 16 memtier
threads, two connections per thread, pipeline 4, and 1,600,000 operations per
trial:

- Source: base `b52226e54c520d367e38a571885bc8687c40c105` plus functional
  diff SHA-256
  `feed4f883f5b311ab9bce6b166e2979ade39b6963c6f36d7ab19939bb20d9e63`
  (excluding this results file).

| Mode | Trial ops/s | Median ops/s | Median p99 |
| --- | --- | ---: | ---: |
| Exact | 108,350.72; 108,951.28; 109,065.37; 110,616.46; 109,877.45 | 109,065.37 | 1.295 ms |
| Multishot + batched replies | 363,544.90; 369,677.40; 301,858.77; 309,784.84; 336,048.96 | 336,048.96 | 1.311 ms |

- Median throughput improvement: **+208.12%**.
- Median p99 change: **+1.24%**, within the latency guardrail.
- Correctness: each mode completed 8,000,000 measured operations with zero GET
  misses and no client or unexpected server errors.
- Raw summaries:
  `benches/logs/norn-kv-memtier-20260820T204953Z-p4/summary.log` and
  `benches/logs/norn-kv-memtier-20260820T204859Z-p4/summary.log`.

Throughput in the batched mode ranges from 301,858.77 to 369,677.40 ops/s,
reflecting sensitivity to how the loopback TCP stack coalesces each pipeline
into provided buffers. The direction is high confidence because the slowest
candidate trial is still 176% above the exact median; the exact gain magnitude
has moderate confidence.

Pipeline 16 is a secondary confirmation using eight memtier threads and four
connections per thread: multishot plus batching reaches 213,325.28 median
ops/s versus 111,428.74 exact (**+91.45%**) and improves median p99 from 5.215
ms to 4.527 ms. The pipeline-1 guardrail is neutral at 111,598.08 versus
111,391.93 median ops/s (**+0.19%**), with median p99 changing from 0.327 ms
to 0.335 ms.

Raw summaries:

- `benches/logs/norn-kv-memtier-20260820T204223Z-p16/summary.log`
- `benches/logs/norn-kv-memtier-20260820T204426Z-p1/summary.log`

Decision: retain both exact and multishot modes for controlled comparisons and
make multishot with ordered response batching the application default. The
single-request path does not materially regress, while real pipelines cross
the retention threshold by a wide margin.

## Single-thread memcached reference

The accepted Norn path was compared with memcached 1.6.42 using the identical
prefill, mixed request distribution, client placement, 32 connections,
pipeline depth 4, and five-trial procedure. Memcached ran one worker thread on
CPU 0 with the binary protocol forced on.

| Server | Trial ops/s | Median ops/s | Median p99 |
| --- | --- | ---: | ---: |
| memcached 1.6.42 | 335,894.67; 335,786.96; 338,457.87; 327,741.25; 340,385.34 | 335,894.67 | 0.759 ms |
| Norn retained path | 363,544.90; 369,677.40; 301,858.77; 309,784.84; 336,048.96 | 336,048.96 | 1.311 ms |

Norn is only **0.05%** faster at the median, which is noise, and its median
p99 is **72.7%** higher. All reference trials completed exactly 1,600,000
operations with zero GET misses and no connection errors. This reference
invalidates throughput parity as a stopping condition: the networking path
must establish a material lead and improve tail latency before it can be
called extremely efficient.

## Retained-path mixed-workload profile

A release build with debug symbols was sampled under the primary p4 workload.
The instrumented run completed at 328,123.38 ops/s with zero misses. The
profile captured about 6,000 samples with none lost and about 5.5 billion CPU
cycles. Largest self-costs were request handling/inlined key lookup (23.94%),
libc `memcmp` (22.39%), libc `memmove` (11.10%), the connection future (6.72%),
`send_all` (6.09%), `MemoryStore::set` (3.06%), request decoding (1.88%),
response encoding (1.77%), frame scanning (1.66%), and hashing (1.58%).

Interpretation: io_uring submission and wakeup machinery are no longer the
dominant costs on the realistic pipelined path. Key lookup/equality and copying
stored values into the contiguous response buffer account for the clearest
remaining application CPU. Receive coalescing also remains unstable enough to
justify testing a bounded drain of already-completed multishot CQEs before
sending a response batch.

The accepted drain was profiled again under 3,200,000 measured p4 operations.
The instrumented run reached 363,371.19 ops/s at 0.439 ms p99 with zero misses;
4,000 samples covered about 5.53 billion user cycles with none lost. The top
self-costs remained request handling (29.01%), libc `memcmp` (24.69%), libc
`memmove` (13.28%), `send_all` (3.52%), SET storage work (2.71%), multishot
stream polling (2.22%), decode (2.05%), response encoding (1.90%), frame
scanning (1.82%), hashing (3.10% combined), and selected-buffer processing
(1.33%). The drain removed scheduling and send overhead from the leading group
without changing the remaining application-byte-work diagnosis. Raw profile:
`/tmp/norn-retained-profile.NeO6lq` on the benchmark host.

### Drain ready multishot completions before sending: accepted

- Hypothesis: the retained path sends immediately after one selected receive
  buffer even when the driver has already delivered more completions for that
  connection. A bounded nonblocking drain should form fuller response batches,
  remove receive-coalescing sensitivity, and reduce send operations without
  introducing another wait or a cross-connection head-of-line barrier.
- Change: after the first awaited selected buffer, poll at most 15 additional
  already-ready buffers from that connection's existing multishot stream.
  Return each selected buffer immediately after parsing, preserve request and
  response order, issue no additional wait, and send once for the accumulated
  responses. The 16-buffer cap bounds per-task work and response memory.
- Screen: 369,869.76; 364,069.69; and 369,599.00 ops/s, median
  369,599.00 ops/s with 0.535 ms median p99.
- Full confirmation: 370,174.57; 369,670.82; 371,882.61; 366,985.19; and
  367,392.37 ops/s, median **369,670.82 ops/s** with **0.543 ms** median p99.
- Delta from the prior retained Norn median: **+10.01%** throughput and
  **-58.6%** p99. Delta from single-thread memcached: **+10.06%** throughput
  and **-28.5%** p99.
- Correctness: all 8,000,000 confirmation operations completed with zero GET
  misses, connection errors, or unexpected server errors; the targeted suite
  also passed for both exact and multishot modes.
- Non-pipelined guardrail: 110,503.04; 109,974.22; and 110,286.62 ops/s,
  median 110,286.62 ops/s and 0.327 ms median p99. This is -1.18% versus the
  prior multishot p1 median and below the materiality threshold.
- Raw summaries:
  `benches/logs/norn-kv-memtier-20260820T211953Z-p4/summary.log` and
  `benches/logs/norn-kv-memtier-20260820T212030Z-p4/summary.log`; p1 guardrail:
  `benches/logs/norn-kv-memtier-20260820T212346Z-p1/summary.log`.
- Decision: retain. This establishes the first material throughput and tail
  latency lead over the external reference while keeping scheduling bounded.

### Inline key equality: rejected

- Hypothesis: the profile attributed 22.39% self-time to libc `memcmp` during
  borrowed `HashMap<Vec<u8>>` key lookup. A raw hash-table lookup using the same
  randomized hash and an inlined 8-byte-at-a-time equality check might avoid
  call and dispatch overhead for short Memcached keys.
- Change screened: use `hashbrown` raw entries while retaining `RandomState`,
  with an exhaustively tested inline comparison for key lengths 0 through 250.
- Result: 369,445.82; 370,276.17; and 368,526.97 ops/s, median
  369,445.82 ops/s and 0.543 ms median p99.
- Delta from the receive-drain baseline: **-0.06%**, indistinguishable from
  noise. Correctness passed and all 4,800,000 measured operations completed
  without misses or errors.
- Raw summary:
  `benches/logs/norn-kv-memtier-20260820T212724Z-p4/summary.log`.
- Decision: reject and remove the dependency and custom lookup code. The
  symbol was where unavoidable matching work accumulated; replacing libc's
  comparison moved rather than removed that work.

A stricter follow-up kept `std::HashMap` but used a transparent borrowed key
type with exact overlapping integer loads for every key up to 32 bytes. It
screened at 366,470.85; 365,671.24; and 371,265.02 ops/s, median
366,470.85 ops/s and 0.519 ms median p99, still within noise and slightly below
the five-trial retained median. A 2,000-sample follow-up profile showed no libc
`memcmp`; raw-table find/probe/equality simply absorbed 48.53% of self samples.
This confirms that removing the library symbol does not remove the required
lookup work. The wrapper and its unsafe transparent-slice conversion were
rejected. Raw benchmark:
`benches/logs/norn-kv-memtier-20260820T220224Z-p4/summary.log`; profile:
`/tmp/norn-short-key-profile.v9FbxG` on the benchmark host.

### Shared-value vectored responses: rejected

- Hypothesis: storing values in reference-counted stable buffers and sending
  response headers plus values as one owned scatter/gather operation would
  remove the libc `memmove` that held 11.10% of profile samples.
- Change screened: add an owned `TcpSocket::send_vectored` operation, pool
  response-header buffers per connection, retain values as shared `Bytes`, and
  retry correct partial writes across the returned buffer vector. The request
  parser, bounded receive drain, response ordering, and workload were unchanged.
- `IORING_OP_SENDMSG`: 343,765.03; 339,213.39; and 343,636.63 ops/s, median
  343,636.63 ops/s and 0.471 ms median p99. Throughput delta: **-7.04%**.
- `IORING_OP_WRITEV`: 339,035.06; 337,096.60; and 338,355.02 ops/s, median
  338,355.02 ops/s and 0.511 ms median p99. Throughput delta: **-8.47%**.
- Correctness: the new TCP operation test, fragmented/pipelined server suite,
  and all 9,600,000 measured operations passed without misses or errors.
- Raw summaries:
  `benches/logs/norn-kv-memtier-20260820T214215Z-p4/summary.log` and
  `benches/logs/norn-kv-memtier-20260820T214338Z-p4/summary.log`.
- Decision: reject and remove the API and server changes. For the weighted
  64 B to 4 KiB workload, iovec construction, shared-buffer bookkeeping, and
  scatter/gather socket processing cost more than one contiguous response copy.

### Ready-receive drain bound: retain 16

The accepted drain was screened with caps of 4, 8, and 32, followed by a
time-matched rerun of 16. Three-trial medians were 367,624.59, 367,896.52,
369,839.58, and 365,308.40 ops/s respectively. Median p99 values were 0.439,
0.439, 0.415, and 0.511 ms. The ranges overlap and no cap produced a repeatable
throughput change beyond noise; the time-matched 16 rerun itself ranged from
364,510.94 to 371,942.95 ops/s. Retain 16 as the previously confirmed bound:
it is small enough to limit one task's turn while allowing several completed
buffers to join a response batch.

Raw summaries:

- `benches/logs/norn-kv-memtier-20260820T215321Z-p4/summary.log` (4)
- `benches/logs/norn-kv-memtier-20260820T215402Z-p4/summary.log` (8)
- `benches/logs/norn-kv-memtier-20260820T215442Z-p4/summary.log` (32)
- `benches/logs/norn-kv-memtier-20260820T215527Z-p4/summary.log` (16 rerun)

### Box stored keys: rejected

- Hypothesis: keys never resize after insertion, so replacing each three-word
  `Vec<u8>` key with a two-word `Box<[u8]>` would shrink hash-table buckets and
  improve locality in the dominant lookup path.
- Result: 366,958.51; 364,357.71; and 369,294.29 ops/s, median
  366,958.51 ops/s and 0.535 ms median p99. This is within current run noise and
  -0.73% versus the confirmed receive-drain median.
- Correctness: the full targeted suite and all 4,800,000 operations passed with
  zero misses or errors.
- Decision: reject. The smaller metadata footprint does not produce a measured
  throughput benefit at the 100,000-key cardinality.
- Raw summary:
  `benches/logs/norn-kv-memtier-20260820T220730Z-p4/summary.log`.

### Faster randomized hashing: rejected

- Hypothesis: replace the standard randomized hasher, which accounts for about
  3.1% of profile samples, with per-process-randomized `ahash` while preserving
  exact key equality and collision-attack resistance.
- Result: 368,593.78; 364,491.26; and 367,170.55 ops/s, median
  367,170.55 ops/s and 0.439 ms median p99. This is -0.68% versus the confirmed
  retained median and within run noise.
- Correctness: the targeted suite and all 4,800,000 measured operations passed
  with zero misses or errors.
- Decision: reject and remove the dependency. Hash computation is too small a
  share of this workload to change end-to-end throughput materially.
- Raw summary:
  `benches/logs/norn-kv-memtier-20260820T220913Z-p4/summary.log`.

### Contiguous `SEND_ZC`: rejected

- Hypothesis: retain the one contiguous response batch but replace ordinary
  `SEND` with Norn's terminal-notification-aware `SEND_ZC`, enabling
  `SO_ZEROCOPY` once per connection.
- Result: 306,465.80; 307,142.64; and 306,006.45 ops/s, median
  306,465.80 ops/s, 0.415 ms median p50, and 0.527 ms median p99. Throughput is
  **-17.10%** versus the confirmed retained median, and p50 regresses 21.0%.
- Correctness: the targeted suite and all 4,800,000 measured operations passed
  with zero misses or errors.
- Decision: reject. Notification completion and zero-copy setup dominate the
  roughly 2 KiB response batches on loopback; ordinary `SEND` is the measured
  efficient path for this distribution.
- Raw summary:
  `benches/logs/norn-kv-memtier-20260820T221053Z-p4/summary.log`.

### Shared receive-ring working set: retain 8,192 buffers

- Hypothesis: the default 8,192 × 8 KiB shared ring is sized for the server's
  4,096-connection limit but is much larger than the 32 active benchmark
  connections. Smaller rings might improve cache/TLB locality.
- 512 buffers: 372,683.94; 369,549.66; and 368,948.21 ops/s, median
  369,549.66 ops/s and 0.463 ms median p99.
- 128 buffers: 368,668.35; 371,428.08; and 371,701.09 ops/s, median
  371,428.08 ops/s and 0.431 ms median p99.
- Both throughput deltas are below 0.5% of the confirmed retained median and
  within run noise; neither showed ring exhaustion in this load.
- Decision: retain 8,192. A smaller fixed ring would weaken the advertised
  high-connection capacity without a material measured throughput gain. A
  future configurable ring size can be justified by deployment memory limits,
  not by this hot-path result.
- Raw summaries:
  `benches/logs/norn-kv-memtier-20260820T221240Z-p4/summary.log` and
  `benches/logs/norn-kv-memtier-20260820T221324Z-p4/summary.log`.

### Multishot receive bundles: rejected

- Shared-ring attempt: failed the correctness gate before measurement. With
  multiple sockets consuming bundles from one provided-buffer ring, completion
  order does not identify each bundle's position in the ring; Norn's one cached
  ring head reported `selected bid ... but bundle head expected ...` and closed
  the measured connections. The kernel contract requires a cached head per
  ring because the CQE carries the first BID but not its ring position.
- Correct ownership screen: one private 64-buffer ring and one bundle operation
  per connection. All targeted tests and 4,800,000 measured operations passed.
  Results were 363,457.69; 366,141.19; and 367,282.48 ops/s, median
  366,141.19 ops/s and 0.487 ms median p99, **-0.95%** versus retained.
- Decision: reject and restore the ordinary shared multishot receive. At 8 KiB
  per provided buffer, the realistic request stream does not bundle enough
  buffers to repay per-connection registration and bundle accounting. The
  existing Norn bundle API must not be used concurrently with one shared ring
  until it enforces or documents exclusive bundle-head ownership.
- Raw failed shared-ring logs:
  `benches/logs/norn-kv-memtier-20260820T221522Z-p4/`; private-ring summary:
  `benches/logs/norn-kv-memtier-20260820T221919Z-p4/summary.log`.

## Retained pipeline curve and final external comparison

After the bounded receive drain was retained, the pipeline curve was screened
again with three trials per depth. The earlier p4 development point did not
supply enough queued work to expose the final server's batching capacity.

| Pipeline | Norn median ops/s | Norn median p99 | memcached median ops/s | memcached median p99 |
| ---: | ---: | ---: | ---: | ---: |
| 8 | 622,220.91 | 0.511 ms | 532,053.21 | 0.967 ms |
| 16 | 906,509.42 | 0.895 ms | 756,782.43 | 1.359 ms |
| 32 | 1,323,840.30 | 1.391 ms | 695,007.93 | 1.727 ms |
| 64 | 1,395,131.86 | 2.631 ms | 771,731.48 | 3.623 ms |

Pipeline 32 is the retained operating knee. Pipeline 64 adds only 5.4%
throughput while increasing Norn's p99 by 89%; p32 therefore gives the better
throughput/latency tradeoff. Norn continues to lead memcached at p64, but that
depth is a saturation guardrail rather than the recommended setting.

The primary p32 comparison was then repeated for five complete trials:

- Final source: base `b52226e54c520d367e38a571885bc8687c40c105` plus
  functional staged-patch SHA-256
  `5e77d8c0cc066a09588dd4d87ec28974c473772631c60e6669d96c477e0b6f2a`
  (excluding this results file).

| Server | Trial ops/s | Median ops/s | Median p50 | Median p99 | Median p99.9 |
| --- | --- | ---: | ---: | ---: | ---: |
| Norn retained | 1,262,270.65; 1,317,728.14; 1,286,938.14; 1,324,635.99; 1,247,724.85 | **1,286,938.14** | **0.759 ms** | **1.423 ms** | **1.735 ms** |
| memcached 1.6.42, one worker | 702,038.59; 681,836.97; 688,225.92; 692,869.55; 695,809.79 | **692,869.55** | **1.463 ms** | **1.783 ms** | **2.631 ms** |

- Norn throughput lead: **+85.74%**.
- Norn median p50: **48.12% lower**.
- Norn median p99: **20.19% lower**.
- Correctness: each server completed exactly 8,000,000 measured operations
  with zero GET misses and no connection errors. Norn also reported no
  unexpected server errors.
- Norn raw summaries:
  `benches/logs/norn-kv-memtier-20260820T222142Z-p8/summary.log`,
  `benches/logs/norn-kv-memtier-20260820T222210Z-p16/summary.log`,
  `benches/logs/norn-kv-memtier-20260820T222234Z-p32/summary.log`,
  `benches/logs/norn-kv-memtier-20260820T222301Z-p64/summary.log`, and
  `benches/logs/norn-kv-memtier-20260820T222544Z-p32/summary.log`.
- memcached screen logs: `/tmp/norn-memcached-curve.OWWIOz`; five-trial p32
  logs: `/tmp/norn-memcached-p32-final.8WneqW` on the benchmark host. Raw
  values needed to reproduce the summary are included in the table above.

The p32 result is the supported answer to the external-reference question:
the retained Norn networking path is materially faster than single-thread
memcached on realistic mixed requests and client pipelining, with lower median
and tail latency at the same connection count and CPU placement.

### Codec and handler separation with bounded batches: retained

The networking slice was then separated into three concrete layers: a
zero-copy incremental frame decoder/response encoder, an in-memory command
handler, and Norn-specific socket scheduling. The socket layer now stops a
batch after 2,048 commands or 1 MiB of encoded responses. A complete response
may cross the byte limit once; the next command remains buffered and is
processed before another receive is awaited. These bounds prevent an already
queued pipeline from monopolizing one executor turn or growing one response
batch without limit.

The same p32 mixed workload was repeated for five complete trials after the
refactor:

- Candidate source: base `b52226e54c520d367e38a571885bc8687c40c105` plus
  functional staged-patch SHA-256
  `1d55fffe462e33f1132d8331cdd352a07708a799e6fe6ddc95d0c289176ee8d2`
  (excluding this results file).

| Trial | Ops/s | p50 | p99 | p99.9 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 1,264,763.15 | 0.783 ms | 1.463 ms | 1.687 ms |
| 2 | 1,259,294.18 | 0.767 ms | 1.447 ms | 9.863 ms |
| 3 | 1,297,769.05 | 0.767 ms | 1.447 ms | 1.599 ms |
| 4 | 1,269,848.32 | 0.767 ms | 1.439 ms | 9.855 ms |
| 5 | 1,297,548.04 | 0.767 ms | 1.447 ms | 1.615 ms |

- Median: **1,269,848.32 ops/s**, 0.767 ms p50, 1.447 ms p99, and
  1.687 ms p99.9.
- Delta from the pre-refactor retained median: **-1.33% throughput**,
  **+1.05% p50**, and **+1.69% p99**. All are below the 5% materiality
  threshold.
- Correctness: 8,000,000 measured operations completed with zero misses,
  connection errors, or unexpected server errors.
- Decision: retain. The module boundaries and explicit memory/fairness limits
  have no material cost on the primary workload.
- Raw summary:
  `benches/logs/norn-kv-memtier-20260821T012735Z-p32/summary.log`.

This latest retained median remains **83.27% faster** than the matched
one-worker memcached reference, with **47.57% lower p50** and **18.84% lower
p99**.

## Shallow-pipeline parity investigation

Goal: determine why the realistic pipeline-1 path is far below the batched
pipeline-32 ceiling, then bring saturated Norn io_uring throughput within 5%
of an equivalent Norn readiness control. The machine, CPU placement, dataset,
90/10 GET/SET mix, value distribution, correctness gates, and release build are
the same as the primary benchmark. Pipeline depth remains one while connection
count varies. Each screen performs 1,600,000 measured operations; per-client
request count is reduced as connections increase.

### Pipeline-1 connection saturation screen

| Connections | Client requests | Ops/s | p50 | p99 | p99.9 |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 32 | 50,000 | 108,899.02 | 0.295 ms | 0.335 ms | 0.479 ms |
| 64 | 25,000 | 108,291.60 | 0.591 ms | 0.743 ms | 2.087 ms |
| 128 | 12,500 | 115,582.81 | 1.175 ms | 1.495 ms | 6.959 ms |
| 256 | 6,250 | 106,009.18 | 2.343 ms | 2.727 ms | 21.455 ms |

- Correctness: all 6,400,000 operations completed with zero misses,
  connection errors, or unexpected server errors.
- Interpretation: the 32-connection p1 workload already reaches essentially
  the same throughput ceiling as 64 and 256 connections. The 128-connection
  point is only 6.1% above 32 while carrying 4x the outstanding work and 4x
  p50 latency. Insufficient offered concurrency is therefore not the primary
  cause; the shallow path has a fixed per-response server cost.
- Decision: use 128 connections for the saturated p1 baseline and readiness
  comparison, while retaining 32 connections as the latency guardrail.
- Raw summaries: `/tmp/norn-kv-p1-screen-c32/summary.log`,
  `/tmp/norn-kv-p1-screen-c64/summary.log`,
  `/tmp/norn-kv-p1-screen-c128/summary.log`, and
  `/tmp/norn-kv-p1-screen-c256/summary.log`.

Five complete 128-connection baseline trials followed:

| Trial | Ops/s | p50 | p99 | p99.9 |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 114,789.63 | 1.175 ms | 1.647 ms | 7.103 ms |
| 2 | 115,046.87 | 1.167 ms | 1.783 ms | 7.127 ms |
| 3 | 107,925.28 | 1.175 ms | 1.367 ms | 7.183 ms |
| 4 | 108,164.90 | 1.175 ms | 1.327 ms | 6.575 ms |
| 5 | 108,792.84 | 1.167 ms | 1.327 ms | 6.655 ms |

- Baseline median: **108,792.84 ops/s**, 1.175 ms p50, 1.367 ms p99,
  and 7.103 ms p99.9.
- Throughput range: 107,925.28–115,046.87 ops/s (6.5% of the median).
  Candidate screens must therefore show a direction larger than this range or
  use alternating paired trials before a small delta is trusted.
- Raw summary: `/tmp/norn-kv-p1-baseline-c128/summary.log`.

### Pipeline-1 CPU profile

One additional 128-connection trial ran under a 999 Hz user-cycle profile
using the release binary with debug symbols. It completed all 1,600,000
measured operations at 107,120.36 ops/s with zero misses, connection errors,
or unexpected server errors. The profile captured 14,639 samples with none
lost and approximately 5.071 billion user cycles.

| Symbol or activity | Self cycles |
| --- | ---: |
| `MemoryHandler::handle` | 16.21% |
| libc `memcmp` | 13.90% |
| `server::send_all` future | 9.88% |
| libc `memmove` | 7.75% |
| multishot receive `Next::poll` | 5.91% |
| `FrameDecoder::decode` | 5.05% |
| connection task poll | 3.67% |
| task wake/run | 4.37% combined |
| SQ `try_push` | 1.69% |
| completion drain | 1.04% |

- Interpretation: handler lookup/comparison, parsing, and payload movement
  still account for most sampled CPU. The per-response `send_all` future is
  the largest I/O-specific cost and rises from 3.52% in the accepted
  pipeline-4 profile to 9.88% at pipeline one, where every response requires
  a separate send submission, completion, wake, and buffer return. This is a
  hypothesis, not yet a socket-path attribution; the next comparison keeps
  the codec and handler fixed and changes only the I/O mechanism.
- Raw profile: `/tmp/norn-kv-p1-perf.data`.

### Readiness and memcached controls

The same server was given a diagnostic readiness mode which retained the
codec, handler, 8 KiB receive chunks, response buffer, batch limits,
connection tasks, and CPU placement. Only socket I/O changed: the control used
direct nonblocking `recv`/`send` calls after Norn io_uring readiness
notifications instead of owned-buffer io_uring receive/send operations.

| Server or data path | Trial ops/s | Median ops/s | Median p50 | Median p99 | Median p99.9 |
| --- | --- | ---: | ---: | ---: | ---: |
| Norn io_uring | 114,789.63; 115,046.87; 107,925.28; 108,164.90; 108,792.84 | **108,792.84** | **1.175 ms** | **1.367 ms** | **7.103 ms** |
| Norn readiness control | 96,354.88 | 96,354.88 | 1.311 ms | 1.431 ms | 7.175 ms |
| memcached 1.6.42, one worker | 111,701.83; 110,888.03; 97,847.38; 97,622.03; 104,184.51 | **104,184.51** | **1.215 ms** | **1.415 ms** | **7.079 ms** |

- Norn io_uring is 12.9% faster than the otherwise equivalent readiness
  control in the diagnostic screen. This repeats the direction of the earlier
  NOOP comparison, where io_uring was 13.8% faster. The pipeline-1 ceiling is
  therefore not an io_uring parity failure.
- Norn io_uring is 4.4% faster than the matched five-trial memcached median,
  with 3.3% lower p50, 3.4% lower p99, and a 0.3% higher p99.9. All are within
  the investigation's 5% parity band except for the positive throughput lead.
- Every control completed its expected measured operations with zero misses
  or connection errors. The Norn readiness result is a directional diagnostic
  screen rather than a retained performance result; it is sufficiently below
  both the current and historical io_uring comparisons to reject the mode.
- Raw summaries: `/tmp/norn-kv-readiness-screen/summary.log` and
  `/tmp/memcached-p1-five/summary.log`.

At pipeline one, each connection can offer only one request per round trip and
each Norn reply requires its own send submission, completion, task wake, and
buffer return. At pipeline 32, 32 connections can keep 1,024 requests
outstanding, multiple requests commonly arrive in one receive completion, and
their ordered responses share one send completion. The approximately 12x
throughput increase is therefore batching amortization, not evidence that the
individual io_uring socket operations are 12x slower than readiness I/O.

### Paired alternating confirmation: flake-pinned memcached 1.6.31

The independent trial groups above showed enough workstation drift that their
4.4% median difference was not a reliable magnitude. The final control starts
and prefills a fresh server for each measurement, alternates Norn-first and
memcached-first order, uses a fresh port, and performs one complete warmup and
1,600,000-operation measurement per server in each pair. Norn uses the retained
io_uring path. The external reference is the flake-pinned memcached 1.6.31,
one worker pinned to the same CPU as Norn.

| Pair | First | Norn ops/s | memcached ops/s | Paired delta | Norn p99 | memcached p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Norn | 101,068.16 | 105,200.76 | -3.93% | 1.623 ms | 1.975 ms |
| 2 | memcached | 115,835.78 | 97,586.51 | +18.70% | 1.367 ms | 1.951 ms |
| 3 | Norn | 116,667.82 | 104,184.41 | +11.98% | 1.351 ms | 1.671 ms |
| 4 | memcached | 108,691.17 | 113,201.97 | -3.98% | 1.359 ms | 1.375 ms |
| 5 | Norn | 108,848.03 | 98,387.05 | +10.63% | 1.359 ms | 1.407 ms |
| 6 | memcached | 108,053.77 | 111,810.24 | -3.36% | 1.335 ms | 1.399 ms |
| 7 | Norn | 108,393.17 | 113,151.37 | -4.21% | 1.327 ms | 1.831 ms |

- Median paired throughput delta: **-3.36% for Norn**, inside the 5% parity
  band. Norn wins three pairs and memcached wins four. The three large Norn
  wins and four narrow losses show why the robust paired median is used
  instead of the mean.
- Median absolute throughput: 108,691.17 ops/s for Norn and 105,200.76 ops/s
  for memcached. Their independently computed median difference is +3.32% for
  Norn. The reversal between this aggregate and the paired median quantifies
  host drift; neither supports a material throughput difference.
- Median latency: Norn 1.167 ms p50, 1.359 ms p99, and 6.671 ms p99.9;
  memcached 1.199 ms p50, 1.671 ms p99, and 7.135 ms p99.9. Norn is 2.7%
  lower at p50, 18.7% lower at p99, and 6.5% lower at p99.9.
- Correctness: both servers completed exactly 11,200,000 measured operations
  across the seven pairs with zero GET misses or connection errors. Norn
  reported no unexpected server errors.
- Decision: the pipeline-1 io_uring path is within spitting distance of the
  readiness-based external reference and is slightly ahead by the robust
  throughput and latency summaries. No server optimization is retained from
  this investigation; the diagnostic Norn readiness mode was slower and was
  removed.
- Raw results:
  `benches/logs/norn-kv-paired-20260821T031748Z-p1/summary.log`.

### Same-version confirmation: memcached 1.6.42

The earlier external comparisons used memcached 1.6.42. To exclude a version
effect, the exact retained Norn release binary was compared in another seven
alternating pairs against that same binary. The benchmark contract and all
correctness gates were unchanged.

| Pair | First | Norn ops/s | memcached ops/s | Paired delta | Norn p99 | memcached p99 |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | Norn | 107,472.53 | 111,294.96 | -3.43% | 1.711 ms | 1.815 ms |
| 2 | memcached | 100,971.92 | 103,892.80 | -2.81% | 1.303 ms | 1.423 ms |
| 3 | Norn | 101,982.94 | 103,694.12 | -1.65% | 1.319 ms | 1.455 ms |
| 4 | memcached | 100,631.81 | 103,502.30 | -2.77% | 1.767 ms | 1.815 ms |
| 5 | Norn | 100,052.78 | 98,233.39 | +1.85% | 1.831 ms | 2.559 ms |
| 6 | memcached | 100,158.83 | 103,209.59 | -2.96% | 1.775 ms | 1.439 ms |
| 7 | Norn | 100,054.45 | 103,491.87 | -3.32% | 1.855 ms | 1.959 ms |

- Median paired throughput delta: **-2.81% for Norn**. Every pair lies
  between -3.43% and +1.85%, providing a substantially tighter parity result
  than either independent trial groups or the flake-pinned comparison.
- Median absolute throughput: 100,631.81 ops/s for Norn and 103,502.30 ops/s
  for memcached, a consistent -2.77% difference for Norn.
- Median latency: Norn 1.175 ms p50, 1.767 ms p99, and 6.687 ms p99.9;
  memcached 1.215 ms p50, 1.815 ms p99, and 7.087 ms p99.9. Norn is 3.3%
  lower at p50, 2.6% lower at p99, and 5.6% lower at p99.9. Norn has lower
  p99 in six of seven pairs.
- Correctness: both servers completed exactly 11,200,000 measured operations
  with zero GET misses or connection errors. Norn reported no unexpected
  server errors.
- Final decision: pipeline-1 Norn io_uring is within the predefined 5%
  throughput parity band of memcached 1.6.42 and has slightly better median
  latency. The remaining throughput difference is below run-to-run variance
  and does not justify a runtime or socket-path change.
- Raw results:
  `benches/logs/norn-kv-paired-20260821T032414Z-p1/summary.log`.

## Historical NOOP control baseline

- Source: `b52226e54c520d367e38a571885bc8687c40c105` plus functional staged-patch
  SHA-256 `6a8f1f315f5f994d635f0cc346c8df200a60da3927a90eea23e287ef213b1f35`
  (excluding this results file).
- Date: 2026-08-20.
- Workload: NOOP, 128 persistent connections, 12,800,000 measured requests
  per trial.
- Raw log: `benches/logs/norn-kv-network-20260820T162729Z/trials.log`.

| Trial | Requests/s | Sampled p50 | Sampled p99 | Elapsed |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 123,916.695 | 1,039.393 us | 1,081.653 us | 103.295 s |
| 2 | 126,838.269 | 1,000.510 us | 1,090.359 us | 100.916 s |
| 3 | 127,765.624 | 989.169 us | 1,082.455 us | 100.183 s |
| 4 | 128,301.355 | 992.716 us | 1,071.595 us | 99.765 s |
| 5 | 127,168.131 | 992.987 us | 1,093.336 us | 100.654 s |

- Median throughput: **127,168.131 requests/s**.
- Median sampled p99: **1,082.455 us**.
- Throughput range: 123,916.695 to 128,301.355 requests/s (3.45% of
  the median).
- Correctness: all 64,000,000 measured responses passed magic, opcode,
  key/extras/data-type, status, opaque, CAS, body-length, and body-content
  validation.

## Historical NOOP-guided attempts

### Syscall profile

A 128-connection release run under `strace -f -c` completed 128,000 measured
NOOPs at 98,231.867 requests/s under instrumentation. It recorded 8,894
`io_uring_enter` calls, accounting for 99.81% of traced syscall time. This is
consistent with repeated receive/send submission and parking being the first
networking boundary to test.

A follow-up 99 Hz `perf` profile captured 342 samples during 384,000 measured
NOOPs (five samples lost because the perf ring was deliberately limited to
leave locked memory for io_uring). The largest self-costs were the connection
future (22.14%), exact receive future (12.76%), `memmove` (8.65%), SQ
`try_push` (6.28%), and task wake (4.43%). Protocol response encoding was only
1.48%. The profile does not identify a remaining isolated application-code
hotspot above the materiality threshold.

### Shared multishot receive ring: rejected for the NOOP control

- Change: keep one provided-buffer multishot receive armed per connection,
  with all 128 connections sharing one 8,192-buffer group. The parser consumes
  complete frames directly from selected buffers and copies only split frames.
- Screen: one paired trial per mode, 1,000 warmup and 20,000 measured requests
  per connection.
- Exact: 121,804.272 requests/s, sampled p99 1,150.465 us.
- Shared multishot: 117,027.432 requests/s, sampled p99 1,199.828 us.
- Delta: -3.92% throughput and +4.29% sampled p99.
- Raw logs:
  `benches/logs/norn-kv-network-20260820T164710Z/trials.log` and
  `benches/logs/norn-kv-network-20260820T164742Z/trials.log`.
- Historical decision: reject the shared group for the non-pipelined NOOP
  control. The realistic pipelined study above supersedes this as application
  evidence.

### Per-connection multishot receive rings: rejected for the NOOP control

- Change from the shared-ring candidate: give each connection an independent
  64-buffer group, matching the receive shape that had helped earlier Norn TCP
  microbenchmarks.
- Screen: one trial with the same 1,000 warmup and 20,000 measured requests per
  connection.
- Result: 117,194.458 requests/s, sampled p99 1,167.768 us.
- Delta from the paired exact screen: -3.78% throughput and +1.50% sampled p99.
- Raw log: `benches/logs/norn-kv-network-20260820T165148Z/trials.log`.
- Historical decision: independent rings did not recover the NOOP regression,
  so shared-ring contention was not its cause. This does not contradict the
  later benefit from queued request parsing and response batching.

### Overlap response send with the next header receive: rejected

- Change: submit the response send and the next exact header receive in the
  same executor turn. Both operations retain short-send, fragmented-receive,
  EOF, and pipelining handling. Serial and overlap modes run in the same binary.
- Screen: one paired trial per mode, 1,000 warmup and 20,000 measured requests
  per connection.
- Serial: 120,396.712 requests/s, sampled p99 1,088.258 us.
- Overlap: 109,147.063 requests/s, sampled p99 1,267.486 us.
- Delta: -9.34% throughput and +16.47% sampled p99.
- Raw logs:
  `benches/logs/norn-kv-network-20260820T170107Z/trials.log` and
  `benches/logs/norn-kv-network-20260820T170142Z/trials.log`.
- Decision: reject. Coordinating two outstanding operation futures costs more
  than the saved submission turn in this runtime and workload.

### Executor and completion fairness budgets: retain defaults

- Change: expose the server executor task-poll budget and io_uring completion
  drain budget, varying one while holding the other at the default 32.
- Screen: one 1,000-warmup / 20,000-measured trial per setting.

| Task polls | CQEs drained | Requests/s | Sampled p99 |
| ---: | ---: | ---: | ---: |
| 32 | 32 | 123,454.008 | 1,062.248 us |
| 16 | 32 | 119,441.709 | 1,137.310 us |
| 128 | 32 | 119,478.256 | 1,089.730 us |
| 32 | 16 | 122,580.529 | 1,065.595 us |
| 32 | 128 | 123,075.720 | 1,273.759 us |

- Decision: retain 32/32. Neither direction improved throughput; the larger
  completion drain also caused a material tail-latency regression.
- Raw logs: `benches/logs/norn-kv-network-20260820T170740Z/trials.log`,
  `benches/logs/norn-kv-network-20260820T170939Z/trials.log`,
  `benches/logs/norn-kv-network-20260820T170851Z/trials.log`,
  `benches/logs/norn-kv-network-20260820T171017Z/trials.log`, and
  `benches/logs/norn-kv-network-20260820T170814Z/trials.log`.

### Readiness-backed TCP data path: rejected

- Change: convert accepted sockets to Norn's readiness-backed `TcpStream`,
  retaining multishot readiness operations while performing small
  nonblocking reads and writes directly. The owned-buffer io_uring path and
  readiness path run in the same binary.
- Screen: one paired trial per path, 1,000 warmup and 20,000 measured requests
  per connection.
- Owned-buffer io_uring: 120,018.030 requests/s, sampled p99 1,075.363 us.
- Readiness: 103,499.561 requests/s, sampled p99 1,248.451 us.
- Delta: -13.76% throughput and +16.10% sampled p99.
- Raw logs:
  `benches/logs/norn-kv-network-20260820T171722Z/trials.log` and
  `benches/logs/norn-kv-network-20260820T171758Z/trials.log`.
- Decision: reject. Direct nonblocking data syscalls plus readiness tracking
  are materially slower than owned-buffer io_uring operations here.

### io_uring task-run builder flags: retain defaults

- Change: compare the default builder against the common Norn benchmark flags
  `COOP_TASKRUN`, `SINGLE_ISSUER`, and `SUBMIT_ALL`, then add
  `DEFER_TASKRUN` separately.
- Screen: one paired 1,000-warmup / 20,000-measured trial per configuration.
- Default: 122,400.056 requests/s, sampled p99 1,092.803 us.
- Cooperative subset: 121,648.210 requests/s, sampled p99 1,113.531 us.
- With deferred task running: 123,171.388 requests/s, sampled p50 308.475 us,
  but sampled p99 10,440,508.529 us.
- Decision: retain the default builder. The cooperative subset is slightly
  slower. Deferred task running can strand completions while runnable tasks
  keep the executor in no-park cycles, creating unacceptable multi-second
  outliers even though aggregate throughput appears flat.
- Raw logs:
  `benches/logs/norn-kv-network-20260820T172314Z/trials.log`,
  `benches/logs/norn-kv-network-20260820T172545Z/trials.log`, and
  `benches/logs/norn-kv-network-20260820T172350Z/trials.log`.

## Retained-path guardrails

At this historical stage, the owned-buffer path was rebuilt after every
rejected NOOP candidate was removed. GET and SET each ran three 1,000-warmup /
20,000-measured trials per connection at the same 128-connection load.

| Workload | Trial requests/s | Median requests/s | Median sampled p99 |
| --- | --- | ---: | ---: |
| GET, 64-byte value | 113,211.198; 114,216.594; 113,684.817 | 113,684.817 | 1,166.279 us |
| SET, 64-byte value | 111,750.783; 113,287.811; 113,689.674 | 113,287.811 | 1,207.537 us |

- GET raw log: `benches/logs/norn-kv-network-20260820T173034Z/trials.log`.
- SET raw log: `benches/logs/norn-kv-network-20260820T173201Z/trials.log`.
- Correctness: GET validated flags and every returned value byte; SET
  validated every response header and empty success body. No server or load
  generator errors occurred.

## Cumulative result

- Accepted change: shared provided-buffer multishot receive, direct complete
  frame parsing, split-frame carry storage, ordered response batching, and a
  bounded nonblocking drain of up to 16 already-ready receive completions
  before each send.
- Primary exact baseline: 109,065.37 median ops/s and 1.295 ms median p99 for
  the external 90/10 mixed workload at the initial pipeline-4 comparison.
- At p4, the retained drain path reaches 369,670.82 median ops/s and 0.543 ms
  median p99: **+238.94%** throughput and **58.1% lower** p99 versus that exact
  baseline.
- At the final p32 operating knee, the bounded, modular retained Norn path
  reaches **1,269,848.32 median ops/s**, 0.767 ms median p50, and 1.447 ms
  median p99. The matched one-worker
  memcached reference reaches 692,869.55 ops/s, 1.463 ms p50, and 1.783 ms p99.
  Norn is **83.27% faster**, with **47.57% lower p50** and **18.84% lower p99**.
- Non-pipelined guardrail: the receive drain changes the prior multishot p1
  median by -1.18%, below the materiality threshold, with the same 0.327 ms
  median p99.
- Confidence: high. Both the original external comparison and the subsequent
  bounded-module guardrail use five pinned trials, identical requests and
  client placement, 8,000,000 verified operations, zero misses, and no
  client/server errors. Results apply to this local loopback host and
  toolchain.
- The NOOP screens remain valid control measurements, but their earlier
  application-level multishot rejection is superseded.
- Stop reason: the external workload exercises realistic request bodies, hot
  keys, writes, reads, varied value sizes, and client pipelines. Profiling and
  isolated screens exhausted the material networking candidates on this host:
  custom key comparison, boxed keys, alternate hashing, scatter/gather sends,
  `SEND_ZC`, ring-size changes, receive bundles, and drain-cap tuning were all
  neutral, slower, or invalid under the kernel ownership contract. Further
  work should preserve this single-core control while evaluating Norn runtime
  optimizations and one-runtime-per-core sharding.
