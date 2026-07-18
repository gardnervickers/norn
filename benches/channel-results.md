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
  - four producers / one consumer sharing one MPSC queue, receive limit 32;
  - four producers / one consumer using four fixed ingress lanes, receive limit
    32;
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

### B0: benchmark-driven development baseline on PR commit `c72884e`

Raw logs: `/tmp/norn-channel-benchmark/bdd-baseline-c728/`.

This recapture followed two upstream rebases and is the comparison point for
the optimization loop. One process experienced a host-wide disturbance (13 ns
driver yield and 9,015 ns round trip versus the usual 8 ns and roughly 6,200
ns); the robust process median is retained, but that run is not evidence for a
candidate-level delta.

| Workload | Median | Derived rate |
| --- | ---: | ---: |
| 1P/1C, receive limit 1 | 1,646,526 ns/262,144 | 6.281 ns/message, 159.210 Mmsg/s |
| 1P/1C, receive limit 16 | 1,853,897 ns/262,144 | 7.072 ns/message, 141.402 Mmsg/s |
| 1P/1C, receive limit 32 | 1,780,140 ns/262,144 | 6.791 ns/message, 147.260 Mmsg/s |
| 4P/1C, receive limit 32 | 45,451,780 ns/262,144 | 173.385 ns/message, 5.768 Mmsg/s |
| 1P/1C round trip | 6,218 ns/round trip | - |
| executor yield, channel vs plain | 8 ns vs 5 ns | - |

Decision: use B0 as the current-tree baseline. The roughly 27.6x gap between
1P and 4P aggregate throughput is reproducible and is the first optimization
target.

### B1: profile the multi-producer collapse

Profiles: `/tmp/norn-channel-profile/b0/` and
`/tmp/norn-channel-profile/b0-debug/`.

The symbolized 1 kHz process-wide profile collected 7,574 samples from the
four-producer workload. Producer-side `Sender::try_send` accounted for 6,629
samples (87.5%). Within that path, `ArrayQueue::push` accounted for 5,148
samples (68.0% of the total profile): 2,558 samples were atomic tail loads,
1,342 were contention backoff, and 1,248 were failed or successful weak CAS
operations. `Remote::notify` and its atomic swap accounted for another 1,446
samples (19.1%). Consumer-side `ArrayQueue::pop` accounted for 890 samples
(11.8%); driver parking and actual OS unparks were below 1%.

Decision: optimize producer contention first. Bulk-drain tuning cannot address
the dominant cost, and the driver is not the source of the 4P collapse. The
first candidate will give independently cloned producers separate ingress
lanes while retaining one exact channel-wide bound.

### B2: dynamically sharded segmented ingress

Raw log: `/tmp/norn-channel-benchmark/b2-sharded-segqueue-smoke/run-1.log`.

Each cloned sender received a private `SegQueue`; one channel-wide atomic
occupancy reservation retained the exact configured bound. The receiver
refreshed its lane list only when senders were cloned and drained ready lanes
round-robin. The existing notification handshake was unchanged. All channel
tests passed.

| Workload | B0 median | B2 screening run | Delta |
| --- | ---: | ---: | ---: |
| 1P/1C, receive limit 1 | 1,646,526 ns | 12,757,503 ns | +674.8% |
| 1P/1C, receive limit 16 | 1,853,897 ns | 12,346,730 ns | +566.0% |
| 1P/1C, receive limit 32 | 1,780,140 ns | 11,914,430 ns | +569.3% |
| 4P/1C, receive limit 32 | 45,451,780 ns | 21,860,076 ns | -51.9% |

Rejected. Sharding removed enough shared-tail contention to more than double
4P throughput, but segmented queue traversal and the channel-wide occupancy
accounting caused an unacceptable single-producer regression. Retain the
sharded topology as a lead, but screen preallocated per-producer rings before
considering its memory/API tradeoffs.

### B3: dynamically sharded preallocated rings

Raw log: `/tmp/norn-channel-benchmark/b3-sharded-arrayqueue-smoke/run-1.log`.

This candidate replaced each segmented lane with a preallocated `ArrayQueue`
while keeping the exact channel-wide occupancy reservation.

| Workload | B0 median | B3 screening run | Delta |
| --- | ---: | ---: | ---: |
| 1P/1C, receive limit 1 | 1,646,526 ns | 10,114,532 ns | +514.3% |
| 1P/1C, receive limit 16 | 1,853,897 ns | 9,350,800 ns | +404.4% |
| 1P/1C, receive limit 32 | 1,780,140 ns | 8,933,170 ns | +401.8% |
| 4P/1C, receive limit 32 | 45,451,780 ns | 21,840,990 ns | -52.0% |

Rejected. Preallocation recovered part of the single-producer loss but left it
more than 5x slower, while 4P was essentially unchanged from B2. The global
occupancy cache line, which is modified by every producer and the consumer, is
the remaining architectural bottleneck. Do not retain hidden dynamic lanes;
test an explicit fixed-lane fan-in API whose lane capacities sum to the exact
channel bound and require no shared per-message occupancy counter.

### B4: explicit fixed-lane fan-in

Raw logs: `/tmp/norn-channel-benchmark/b4-fixed-lanes-smoke/` and
`/tmp/norn-channel-benchmark/b4-fixed-lanes-full-smoke/`.

The new `bounded_sharded` constructor returns one sender per requested
producer. Its preallocated lane capacities sum exactly to the requested total;
the local receiver drains them round-robin. Cloning a sender deliberately
shares only that sender's lane, while ordinary `bounded` remains a one-lane
MPSC. No per-message global occupancy counter is required. Channel tests pass,
including 100,000 messages from each of four lanes with per-producer FIFO
validation.

The first focused 4P run measured 6,696,996 ns and the full-matrix process
measured 7,059,695 ns, versus the 45,451,780 ns B0 median: a 6.4x to 6.8x
speedup. The same full process kept unsharded 4P at 44,582,111 ns and round-trip
latency at 6,178 ns, confirming that the new result comes from removing the
shared producer tail rather than a host-wide fast mode.

The initial receiver used the general round-robin loop even for one lane. Its
single-receive case regressed to 2,614,154 ns (+58.8%), although the bulk cases
were faster in this screening process. Retain the architecture provisionally,
specialize the one-lane pop path, and require a full seven-process matrix
before accepting it.

Splitting the ordinary and sharded APIs into distinct concrete sender/receiver
types preserved the original channel's generated hot path. After removing
benchmark-only enum dispatch, the focused ordinary 1P limit-1 screen measured
1,728,251 ns, within 5.0% of B0. The sharded path remained at 7,201,475 ns in
the preceding full screen. Retain the explicit fixed-lane design.

### B5: per-lane notification coalescing

Raw log: `/tmp/norn-channel-benchmark/b5-lane-notify-smoke/run-1.log`.

The B4 profile contained 6,601 samples. The global `Remote::notify` swap was
3,065 samples (46.4%), producer queue push was 1,026 (15.5%), and receiver pop
was 2,142 (32.4%). Each lane now owns a notification bit. A successful send
swaps that non-contended bit and only the first send in an active burst touches
the driver-wide remote state. When a lane appears empty, the receiver clears
the bit and rechecks the queue: publication before the clear is observed by
the recheck, while publication after the clear performs a remote notification.

The first pinned process measured 1,128,809 ns/262,144 messages, or 4.306
ns/message and 232.232 Mmsg/s aggregate. That is 83.1% faster than the first B4
screen and 97.5% faster (40.3x throughput) than the B0 shared-MPSC median.
Retain provisionally; validate forced empty-transition races, recapture the
seven-process matrix, and profile the new ceiling.

### B6: drain one lane at a time within the bounded batch

Raw log: `/tmp/norn-channel-benchmark/b6-lane-bulk-drain-smoke/run-1.log`.

The candidate replaced per-message round-robin selection with a bounded drain
from one lane before advancing to the next lane. It retained the caller's exact
limit and passed all channel tests. The pinned screen measured 1,137,272 ns,
0.75% slower than B5's 1,128,809 ns and far below the 5% materiality threshold.

Rejected. Lane selection is not a material cost at four lanes and a batch of
32; retain message-level round-robin fairness.

### B5 final validation: seven-process matrix

Raw logs: `/tmp/norn-channel-benchmark/b5-lane-notify-final/`.

| Workload | B0 median | B5 median | Delta / derived rate |
| --- | ---: | ---: | ---: |
| executor yield, channel vs plain | 8 vs 5 ns | 8 vs 5 ns | unchanged |
| 1P/1C round trip | 6,218 ns | 6,126 ns | -1.5% |
| 1P/1C, receive limit 1 | 1,646,526 ns | 1,710,085 ns | +3.9% |
| 1P/1C, receive limit 16 | 1,853,897 ns | 1,843,289 ns | -0.6% |
| 1P/1C, receive limit 32 | 1,780,140 ns | 1,822,051 ns | +2.4% |
| 4P/1C, shared queue | 45,451,780 ns | 45,430,386 ns | -0.05% |
| 4P/1C, four lanes | - | 1,135,596 ns | 4.332 ns/message, 230.843 Mmsg/s |

The sharded result's full seven-process range was 1,130,320 to 1,140,785 ns
(0.9%). Relative to the unchanged shared-MPSC baseline, it reduces transfer
time by 97.5% and increases aggregate throughput by 40.0x. Every pre-existing
workload remained within the 5% process-level noise/materiality threshold.

Accepted. Fixed producer lanes and per-lane notification coalescing are both
material, repeatable wins. The forced empty-transition test completed 25,000
one-message handoffs alternating across two capacity-one lanes without a lost
wakeup.

### B7: specialized SPSC lane rings

Raw log: `/tmp/norn-channel-benchmark/b7-spsc-lanes-smoke/run-1.log`.

Because `bounded_sharded` already returns one handle per producer, this
candidate made those handles non-cloneable and replaced each generic MPMC lane
with a cache-padded SPSC ring. Producer and consumer indices were cached
locally; a slot was published only after its value was initialized. The
candidate passed all channel tests, including the empty-transition race.

Rejected. The pinned screen measured 2,191,609 ns, 93.0% slower than the B5
median. The simpler unsafe ring did not outperform `ArrayQueue`'s mature cache
layout and algorithms. Revert the unsafe implementation and retain cloneable
lane senders backed by safe library queues.

### C0: detached receiver with a per-send attachment lookup

Raw logs: `/tmp/norn-channel-benchmark/detached-api-head-final/` and
`/tmp/norn-channel-benchmark/detached-api-base-final/`.

The first detached-receiver design stored the destination `Remote` in a
`OnceLock` owned by each channel. It made topology construction clean, and all
new pre-attachment and cross-thread tests passed, but ordinary senders queried
the cell after every successful queue push. The seven-process candidate was
stable at 1,940,741 ns for receive limit 16 and 1,917,199 ns for receive limit
32. Those results were 5.3% and 5.2% slower than the retained B5 medians.

The exact-tree baseline recapture entered faster but highly multimodal bulk
modes: its limit-16 spread was 33.8% and its limit-32 spread was 14.9%. Stable
paired cases showed no regression, but the direct lookup was still above the
5% threshold relative to the trustworthy retained matrix.

Rejected. Keep detached construction, but move attachment discovery off the
ordinary per-message path.

### C1: embedded ordinary-channel notification bit

Raw log: `/tmp/norn-channel-benchmark/detached-api-coalesced-screen/run-1.log`
(incomplete trial).

This candidate placed an ordinary-channel notification bit next to the queue
and consulted the attachment cell only when that bit became active. The 1P
screen recovered bulk throughput, but the shared 4P calibration failed to
complete. Placing another producer-contended atomic in the queue allocation was
not acceptable for the existing shared-MPSC path.

Rejected. Preserve the old separately allocated notification cache line.

### C2: separate attachment-aware notification proxy

Raw logs: `/tmp/norn-channel-benchmark/detached-api-proxy-screen/` and
`/tmp/norn-channel-benchmark/detached-api-proxy-final/` (incomplete trial).

A separately allocated proxy restored the old send-path shape: every send does
one pending-bit RMW, while only the first notification in a driver cycle reads
the attached target. The first complete screen was fast, but the repeated
round-trip capture stalled. Its driver-side clear used a release-only store, so
a coalesced producer could observe the old active state without the driver
acquiring that producer's preceding queue publication before checking
readiness.

Rejected. An attachment-aware pending bit needs an acquire-release clear.

### C3: acquire-release attachment proxy

Raw logs: `/tmp/norn-channel-benchmark/detached-api-final/`.

This candidate cleared the separate channel pending bit with an
acquire-release swap before checking receiver readiness. If a producer
coalesces its notification against the old active state, that swap acquires the
producer's queue publication. Attachment is still one-time, and the send hot
path retains one separately allocated notification RMW per message.

| Workload | B5 retained median | Detached median | Delta | Detached spread |
| --- | ---: | ---: | ---: | ---: |
| executor yield, channel vs plain | 8 vs 5 ns | 8 vs 5 ns | unchanged | 1 ns timer resolution |
| 1P/1C round trip | 6,126 ns | 6,128 ns | +0.03% | 2.1% |
| 1P/1C, receive limit 1 | 1,710,085 ns | 1,541,610 ns | -9.9% | 0.4% |
| 1P/1C, receive limit 16 | 1,843,289 ns | 1,901,378 ns | +3.2% | 1.3% |
| 1P/1C, receive limit 32 | 1,822,051 ns | 1,871,251 ns | +2.7% | 0.9% |
| 4P/1C, shared queue | 45,430,386 ns | 44,672,487 ns | -1.7% | 1.9% |
| 4P/1C, four lanes | 1,135,596 ns | 1,119,466 ns | -1.4% | 0.5% |

Rejected despite the native benchmark. The full Miri suite deterministically
deadlocked in the forced ordinary empty-transition test. A second pending-bit
protocol between each channel and the driver duplicated the driver's existing
notification state and remained too difficult to make obviously correct.

### C4: preconstructed driver endpoint

Raw logs: `/tmp/norn-channel-benchmark/detached-api-endpoint-final/`.

The final API moves attachment up one level. A sendable `DriverBuilder` owns
the destination's real `Remote` before runtime threads start, and channels are
constructed against its `Endpoint`. The builder moves to the destination
thread and installs the inner park layer's unparker exactly once. Senders and
the destination driver therefore share the original single pending bit; there
is no per-channel proxy or per-message attachment lookup. A detached receiver
also verifies that it is attached to the driver for its endpoint.

| Workload | B5 retained median | Endpoint median | Delta | Endpoint spread |
| --- | ---: | ---: | ---: | ---: |
| executor yield, channel vs plain | 8 vs 5 ns | 8 vs 6 ns | 1 ns timer resolution | 1 ns timer resolution |
| 1P/1C round trip | 6,126 ns | 6,165 ns | +0.6% | 4.9% |
| 1P/1C, receive limit 1 | 1,710,085 ns | 1,491,801 ns | -12.8% | 0.9% |
| 1P/1C, receive limit 16 | 1,843,289 ns | 1,905,894 ns | +3.4% | 1.1% |
| 1P/1C, receive limit 32 | 1,822,051 ns | 1,885,382 ns | +3.5% | 0.8% |
| 4P/1C, shared queue | 45,430,386 ns | 44,449,039 ns | -2.2% | 1.7% |
| 4P/1C, four lanes | 1,135,596 ns | 1,117,053 ns | -1.6% | 0.4% |

Accepted. Every retained workload remains within the 5% regression threshold,
and the final sharded result is 4.261 ns/message and 234.675 Mmsg/s. The full
Miri suite passes, including messages and last-sender closure before runtime
startup, 25,000 forced ordinary and sharded empty transitions, portable setup
types, endpoint mismatch rejection, and two Norn runtimes exchanging messages
without a bootstrap transport.

## Cumulative Result

- Accepted changes: correctness-complete bounded MPSC channel, bounded bulk
  drain API, local-waker driver integration, pre-runtime detached receiver
  construction, explicit fixed-lane fan-in, and lost-wakeup-safe notification
  coalescing.
- Final ordinary-channel summary: 175.723 Mmsg/s for 1P/1C limit 1, 137.544
  Mmsg/s for limit 16, 139.040 Mmsg/s for limit 32, 5.898 Mmsg/s for shared
  4P/1C, 6.165 microsecond round trip, and a 2 ns median incremental idle-driver
  cost. All retained workloads are within the 5% regression threshold.
- Final sharded summary: 234.675 Mmsg/s aggregate for four producer lanes,
  4.261 ns/message, with a 0.4% seven-process range.
- Cumulative delta: the new sharded API is 39.8x faster than four producers
  cloning one shared sender (97.5% lower transfer time) while retaining an
  exact total capacity and bounded receive work.
- Confidence: high. The retained sharded result is exceptionally stable, the
  unchanged stable paths were recaptured against the exact old tree, and both
  ordinary and sharded notification races have forced empty-transition stress
  tests plus Miri-scaled variants.
- Exhausted candidates: generic dynamic sharding, preallocated dynamic lanes,
  lane-local bulk drain, a specialized unsafe SPSC ring, per-send attachment
  lookup, embedded notification state, and per-channel attachment proxies were
  all rejected on measured regressions, correctness failures, or sub-threshold
  results. The remaining per-message notification RMW is required by the safe
  lost-wakeup handshake.
- Deliberately deferred: bounded bulk submit still needs partial-enqueue and
  ownership-return semantics; it is not part of this PR.
