# Norn Channel Benchmark Results

## Goal

- Target: bounded cross-thread message delivery through a Norn `Park` wrapper.
- Primary metrics: steady-state nanoseconds per message and messages per second.
- Secondary metrics: single-message round-trip latency and idle driver overhead.
- Direction: lower latency and higher throughput are better.
- Correctness checks: exact message counts and payload checks, bounded queue behavior,
  bounded bulk drain, close/shutdown behavior, and cross-thread lost-wakeup tests.

## Environment

- Machine: local workstation.
- Execution mode: local.
- OS: NixOS Linux 6.18.38, x86_64.
- CPU: AMD Ryzen 9 5950X, 16 physical cores / 32 threads, one NUMA node.
- Benchmark CPUs: consumer CPU 2; producer CPUs 4, 6, 8, and 10. These are
  distinct physical cores and use the `performance` frequency governor.
- Memory: 62 GiB, no swap.
- Storage: not exercised by the focused benchmark.
- Runtime/toolchain: repository `nix develop` shell, `rustc
  1.85.0-nightly (6d9f6ae36 2024-12-16)`.
- Relevant environment variables: benchmark thread affinity is configured by
  `NORN_CHANNEL_CONSUMER_CPU` and `NORN_CHANNEL_PRODUCER_CPUS`.
- Initial repository state: clean `agent/norn-channel` at
  `6ce17d5c9a77c7658c87076d0f26ef4c1bf10095`.

## Methodology

- Benchmark command:

  ```console
  ./hack/bench-channel.sh <label> 7
  ```

  The helper pins the consumer to CPU 2 and producers to CPUs 4, 6, 8, and
  10, then invokes `nix develop -c cargo bench -p benches --bench channel`.

- Workloads:
  - one producer / one consumer throughput with receive limits 1, 16, and 32;
  - four producers / one consumer throughput with receive limit 32;
  - one producer / one consumer single-message round trip;
  - executor yield-once control with and without the channel driver installed.
- Queue capacity: fixed for each case and excluded from timed setup.
- Timed setup: threads, executors, channels, and buffers are constructed before
  the benchmark harness begins timing iterations.
- Warmup: automatic `bencher` harness warmup in each process.
- Repetitions: seven complete process-level runs for retained comparisons.
- Summary statistic: median of the seven process-level results.
- Noise threshold: investigate relative spread above 5%; do not claim changes
  below the observed noise or timer resolution.
- Materiality threshold: a complex optimization should improve a primary case
  by at least 5% without materially regressing another primary case.
- Raw logs: `/tmp/norn-channel-benchmark/`.
- Measurement limitation: the synthetic payload is intentionally small and
  measures the channel/executor critical path rather than application work.

## Functional Baseline

This is a new crate, so the first correctness-complete bounded MPSC
implementation is the baseline for subsequent optimization attempts. No
performance claim will be made until that implementation exists and seven
stable runs have been captured.

## Attempts

### A0: correctness-complete baseline, 16,384 messages per round

Raw logs: `/tmp/norn-channel-benchmark/baseline/`.

| Workload | Median | Derived rate | Seven-run relative spread |
| --- | ---: | ---: | ---: |
| executor yield, plain | 5 ns/iteration | - | 0.0% |
| executor yield, channel driver | 8 ns/iteration | - | 0.0% |
| 1P/1C round trip | 6,200 ns/round trip | - | 2.8% |
| 1P/1C, receive limit 1 | 250,229 ns/16,384 | 15.273 ns/message, 65.476 Mmsg/s | 16.0% |
| 1P/1C, receive limit 16 | 89,574 ns/16,384 | 5.467 ns/message, 182.910 Mmsg/s | 20.4% |
| 1P/1C, receive limit 32 | 91,472 ns/16,384 | 5.583 ns/message, 179.115 Mmsg/s | 19.6% |
| 4P/1C, receive limit 32 | 2,701,701 ns/16,384 | 164.899 ns/message, 6.064 Mmsg/s | 1.3% |

Correctness and the latency and multi-producer measurements were stable. The
single-producer throughput results were not: they exceeded the 5% noise
threshold and had large within-process variance as well. These results are a
diagnostic baseline, not accepted performance evidence.

Decision: retain the implementation, increase timed work to 65,536 messages
per round, and rerun before attempting channel optimizations. This reduces the
relative influence of round-control synchronization and gives the scheduler
more work over which to amortize transient interference.

### A0b: larger samples, 65,536 messages per round

Raw logs: `/tmp/norn-channel-benchmark/baseline-65536/`.

| Workload | Median | Derived rate | Seven-run relative spread |
| --- | ---: | ---: | ---: |
| executor yield, plain | 5 ns/iteration | - | 20.0% (1 ns resolution) |
| executor yield, channel driver | 8 ns/iteration | - | 0.0% |
| 1P/1C round trip | 6,189 ns/round trip | - | 4.9% |
| 1P/1C, receive limit 1 | 1,489,900 ns/65,536 | 22.734 ns/message, 43.987 Mmsg/s | 14.9% |
| 1P/1C, receive limit 16 | 415,009 ns/65,536 | 6.333 ns/message, 157.915 Mmsg/s | 36.5% |
| 1P/1C, receive limit 32 | 436,183 ns/65,536 | 6.656 ns/message, 150.249 Mmsg/s | 122.4% |
| 4P/1C, receive limit 32 | 10,985,024 ns/65,536 | 167.618 ns/message, 5.966 Mmsg/s | 1.1% |

Increasing the round size did not stabilize 1P/1C. It ruled out the
round-control channels as the main source of variance: the longer 4P/1C case
remained stable while the 1P/1C cases still switched between distinct modes.

The generic MPMC `ArrayQueue` baseline also performs a contended atomic remote
notification on every message, even though the driver only needs a notification
when the FIFO head becomes ready. The next attempt replaces it with a bounded
MPSC ring whose producer that publishes the FIFO head performs the remote
notification. That preserves the lost-wakeup handshake while coalescing
notifications for a continuously non-empty queue.

### A1: specialized MPSC ring and head-publication notification

Raw log: `/tmp/norn-channel-benchmark/candidate-mpsc-ring/run-1.log`
(incomplete trial).

The candidate used a bounded multi-producer reservation ring and notified only
when a producer published the current FIFO head. It passed the channel and
queue stress tests, including 400,000 messages from four producers.

Rejected. The first pinned process measured the 1P/1C receive-limit-32 case at
1,397,974 ns/65,536 versus the A0b median of 436,183 ns, a 220% regression.
The 4P/1C calibration also became slow enough that the trial was terminated.
The added unsafe queue implementation was not justified by the result.

### Benchmark correction: isolate channel transfer from caller backoff

The A0 and A0b queue capacity was 4,096 messages, so each 65,536-message round
spent substantial time in the benchmark's `Full` retry spin loop. That loop is
caller policy, not channel work, and its producer/consumer phase changes caused
the unstable 1P/1C modes. The primary throughput matrix now uses a bounded
capacity equal to each timed round (65,536 messages for A0c and 262,144 for
A0d). This remains bounded while avoiding deliberate saturation;
capacity/backoff behavior should be measured separately once a producer-side
waiting policy is in scope.

### A0c: bounded transfer baseline without forced saturation

Raw logs: `/tmp/norn-channel-benchmark/baseline-transfer/`.

| Workload | Median | Derived rate | Seven-run relative spread |
| --- | ---: | ---: | ---: |
| executor yield, plain | 5 ns/iteration | - | 20.0% (1 ns resolution) |
| executor yield, channel driver | 8 ns/iteration | - | 0.0% |
| 1P/1C round trip | 6,243 ns/round trip | - | 6.0% |
| 1P/1C, receive limit 1 | 424,653 ns/65,536 | 6.480 ns/message, 154.328 Mmsg/s | 0.5% |
| 1P/1C, receive limit 16 | 433,503 ns/65,536 | 6.615 ns/message, 151.178 Mmsg/s | 6.4% |
| 1P/1C, receive limit 32 | 430,560 ns/65,536 | 6.570 ns/message, 152.211 Mmsg/s | 8.5% |
| 4P/1C, receive limit 32 | 10,991,752 ns/65,536 | 167.721 ns/message, 5.962 Mmsg/s | 0.9% |

Removing forced saturation stabilized the limit-1 and 4P/1C primary cases.
The bulk cases improved substantially but remained just above the process-level
noise threshold. Increase the timed transfer to 262,144 messages for the final
throughput capture; keep the shorter round-trip benchmark unchanged.

### A0d: final transfer baseline, 262,144 messages per round

Raw logs: `/tmp/norn-channel-benchmark/final-transfer/`.

| Workload | Median | Derived rate | Seven-run relative spread |
| --- | ---: | ---: | ---: |
| executor yield, plain | 5 ns/iteration | - | 20.0% (1 ns resolution) |
| executor yield, channel driver | 8 ns/iteration | - | one 15 ns outlier |
| 1P/1C round trip | 6,273 ns/round trip | - | 6.1% |
| 1P/1C, receive limit 1 | 1,665,707 ns/262,144 | 6.354 ns/message, 157.377 Mmsg/s | 0.7% |
| 1P/1C, receive limit 16 | 1,823,842 ns/262,144 | 6.957 ns/message, 143.732 Mmsg/s | 2.6% |
| 1P/1C, receive limit 32 | 1,803,752 ns/262,144 | 6.881 ns/message, 145.333 Mmsg/s | 44.9% |
| 4P/1C, receive limit 32 | 44,383,233 ns/262,144 | 169.309 ns/message, 5.906 Mmsg/s | 5.4% |

The primary 1P limit-1 and limit-16 cases are below the 5% process-level noise
threshold. The 4P case is just above it because of one slow process; the other
six results span 1.5% and support the median.

The limit-32 case had one fast and one slow outlier around a five-result cluster
whose spread was 3.3%. A separate seven-process focused capture in
`/tmp/norn-channel-benchmark/final-limit32/` produced a 1,844,869 ns median
(7.038 ns/message, 142.094 Mmsg/s), with six results in a 2.5% cluster and one
fast outlier. Report this case as bimodal; do not use it to support small
performance comparisons.

## Cumulative Result

- Accepted changes: correctness-complete bounded MPSC channel, bounded bulk
  drain API, local-waker driver integration, and corrected transfer benchmark.
- Baseline summary: the forced-saturation measurements exposed unstable caller
  backoff behavior and were retained as diagnostics rather than headline data.
- Final summary: 157.377 Mmsg/s for 1P/1C limit 1, 143.732 Mmsg/s for limit 16,
  145.333 Mmsg/s for limit 32, 5.906 Mmsg/s for 4P/1C limit 32, 6.273
  microsecond round trip, and a 3 ns median incremental idle-driver cost.
- Cumulative delta: not applicable; this is a new crate with no pre-change
  channel implementation. The unsafe specialized queue candidate was rejected
  after a measured 220% regression in a primary case.
- Confidence: high for the limit-1 and limit-16 throughput medians; moderate for
  round-trip and 4P/1C; limit-32 is explicitly bimodal and should not support
  small comparative claims.
- Remaining ideas: bounded bulk submit is intentionally deferred; evaluate a
  bulk enqueue API only after the receive-side design and traffic profiles are
  established.
