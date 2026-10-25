# norn-kv-bench validation, 2026-10-24

Validation evidence for [`norn-kv-bench`](../examples/norn-kv-bench/README.md),
gathered while developing it. These runs qualify the harness, not a server
optimization.

## Environment

- AMD Ryzen 9 5950X (16 cores, 32 threads), Linux 6.18.55, `performance`
  governor, boost enabled, `perf_event_paranoid = 1`.
- Server on CPU 0; client threads on CPUs 8–11. No server CPU shares a
  physical core with a client CPU.
- **The host was heavily loaded** throughout (load average 80–118 from
  unrelated JVM test runs on all CPUs). The tool's qualification gate flagged
  every run: 30–280 server preemptions/s and thousands of client preemptions
  per segment. These results therefore show which metrics survive a hostile
  host. They are not quiet-host baselines, and the profile rates have not
  yet been calibrated against a quiet-host capacity measurement.

## Functional checks

- `smoke` and `mixed-1w` complete with every response validated
  byte-for-byte, zero outstanding requests after drain, and every
  connection-owned slot read back at its last written version.
- memcached 1.6.31 driven with `--server-kind generic`: valid. This is an
  independent protocol cross-check of the client.
- `cargo test -p norn-kv-bench`: covers protocol, workload, and profile units.
  Two full runs go against an in-process reference server: one checks
  accounting and readback; the other checks that a server corrupting values is
  detected.

## Defects found in the server stack

1. **Multishot accept was not re-armed after the kernel ended it.**
   `TcpListener::incoming()` treated a multishot accept that the kernel ended
   without an error as end of stream. The kernel ends it that way, for
   example, when a completion cannot be posted to a full CQ ring. The
   single-worker `serve()` then returned `Ok(())`, so `norn-kv-server` exited
   with status 0 mid-run, and every client connection was reset or refused.
   - Connection churn plus a preempted server made this happen in 4 of 5
     early `mixed-1w` runs.
   - Fixed separately in `norn-uring`, with a regression test
     (`incoming_rearms_multishot_accept_terminated_by_full_completion_queue`).
     The test reproduces it deterministically with a 2-entry ring.
   - This may also explain the unexplained resets in issue #126.
2. **Sharded server resets connections when multishot receive hits ENOBUFS.**
   At an offered rate above capacity (`mixed-4w`, 700k/s on the loaded
   host), multishot receive exhausted the provided-buffer ring. The connection
   task failed with `No buffer space available (os error 105)` and closed the
   connection. Under overload the server should apply backpressure, for
   example by re-arming once buffers return, rather than reset clients. This
   is not yet fixed.

## A/A noise (identical binaries)

`compare --pairs 3` of two copies of the same `norn-kv-server` build
(`fa9b957e…`), `mixed-1w`, on the loaded host:

| Run | capacity `uinst/op` | capacity `inst/op` | capacity `cpu/op` (ns) | capacity throughput | server util |
| --- | ---: | ---: | ---: | ---: | ---: |
| pair-1 baseline | 1351 | 4123 | 2195 | 189.5k/s | 42% |
| pair-1 candidate | 1355 | 4101 | 2269 | 180.5k/s | 41% |
| pair-2 baseline | 1341 | 4009 | 2185 | 258.2k/s | 56% |
| pair-2 candidate | 1347 | 4063 | 2278 | 217.4k/s | 50% |
| pair-3 baseline | 1345 | 4026 | 2281 | 232.3k/s | 53% |
| pair-3 candidate | 1340 | 4006 | 2263 | 248.6k/s | 56% |

- **Capacity `uinst/op`** stays within 1.1% (1340–1355) across six runs. The
  per-pair deltas run from −0.38% to +0.51%.
- **Throughput and time metrics** vary by up to 35% because the client and
  server were preempted. The tool marks every one of these runs unqualified.
- **Open-loop instruction counts** vary by 7–50% on this host: when the
  server is preempted, its backlog is processed in larger batches.
- **One false positive led to the qualification gate.** Before it, A/A
  reported a spurious `worse` verdict for open-250k p99 from unqualified runs.
  `compare` now reports `unqualified` for any metric of a segment that some
  run failed to qualify.

## A/B sensitivity (synthetic +1% per-request cost)

The candidate adds a four-iteration `black_box` multiply-add loop to every
command in `MemoryHandler::execute_command`. That is roughly 15 user
instructions per request, about 1% of the capacity `uinst/op`. Same
conditions as above:

| Pair | Baseline `uinst/op` | Candidate `uinst/op` | Delta | Candidate throughput |
| --- | ---: | ---: | ---: | ---: |
| 1 | 1348 | 1363 | +1.1% | 308.0k/s |
| 2 | 1352 | 1365 | +1.0% | 234.7k/s |
| 3 | 1347 | 1326 | −1.6% | 525.4k/s |

- Pairs 1 and 2 resolve the 1% change.
- In pair 3 the host briefly quieted during the candidate run. It reached
  525k/s and processed larger batches, so its instructions per request fell
  below the baseline's.
- So instruction counts are much less noisy than time, but they still depend
  on batching. A verdict needs qualified runs, in which the server is
  saturated and the host quiet. That run also shows that the workstation's
  `mixed-1w` capacity is at least about 525k requests/s when the client is not
  preempted.

## Isolation on a shared host

Later the same day, with the same load (load average 77–118), I traced
the remaining noise to three causes and adjusted the harness:

1. **Shared cgroup.** The JVM threads (nice 5) and the benchmark both ran in
   one service cgroup, so the server competed with about 90 runnable threads
   for CPU time. Running the benchmark under `systemd-run --user --scope`
   moves it to its own top-level cgroup: capacity-segment server preemptions
   fell from about 230/s to under 10/s, and client preemptions from about
   2300 to about 100 per window.
2. **Uncharged interrupt time.** Server `util` stayed at 71% with almost no
   voluntary switches or run-queue wait. `/proc/stat` showed the server CPU
   100% busy, 29% of it in interrupts: the loopback softirq work for the
   server's sends is charged to no thread. The capacity check now uses the
   server CPUs' busy time, so it no longer misreports a saturated server as
   client-bound.
3. **SMT sibling contention.** Capacity throughput was bimodal (about 320k
   or 585k requests/s at the same utilization), and the 585k run had IPC 0.51
   instead of about 0.31. In single runs, the server CPU's sibling (CPU 16)
   was 94–100% busy, with IPC about 0.30. In one open-250k window where the
   sibling happened to be 7% busy, IPC was 0.46 and p50 latency 143 us,
   compared with about 9 ms. The harness now records sibling busy time
   (`smt-contention`) and run-queue wait (`server-delayed`,
   `client-delayed`). With `--hold-cpus 16`, a `pause` loop occupies the
   sibling so no other work runs there.

`compare --pairs 5`, `mixed-1w`, server on CPU 0, client on CPUs 8–11,
binaries built from `f7b9bfc` (`ad24d02b…`, perturbed `56f25726…`, the same
+1% perturbation as above):

| Setup | Comparison | capacity `uinst/op` pair deltas | Within-build spread | Capacity runs qualified |
| --- | --- | ---: | ---: | ---: |
| user scope | A/A | −0.91% .. +0.08% | 0.91% | 0 of 10 (all misreported as client-bound before the fix) |
| user scope | A/B | +0.94% .. +1.93% | 0.94% | 0 of 10 |
| user scope + `--hold-cpus 16` | A/A | −0.04% .. +0.12% | 0.26% | 9 of 10 |
| user scope + `--hold-cpus 16` | A/B | +1.57% .. +1.77% | 0.19% | 5 of 10 |

- With the sibling held, the synthetic 1% change is about ten times the
  A/A noise in capacity `uinst/op`. Every A/B pair resolved it, and
  `compare` printed `unqualified/worse` for it and `unqualified/noise` for the
  A/A run.
- Capacity throughput still varies by about 7–17% (376k–426k requests/s).
  A held sibling is more repeatable than a busy one but slower than an idle
  one.
- The open-loop segments remain unqualified on this host. When the server
  wakes from idle it waits behind other work on its own CPU
  (`server-delayed`, 4–13% of the window), and at 20k/s the client threads
  are delayed too.

```sh
XDG_RUNTIME_DIR=/run/user/1000 systemd-run --user --scope -- \
  target/release/norn-kv-bench compare --profile examples/norn-kv-bench/profiles/mixed-1w.json \
  --baseline /tmp/kvb/bin2/base --candidate /tmp/kvb/bin2/perturbed \
  --pairs 5 --server-cpus 0 --client-cpus 8-11 --hold-cpus 16
```

## Quiet-host calibration

The unrelated jobs stopped for about ten minutes (21:51–22:01, load average
about 4). Plain runs, no scope or held CPUs, binaries as above.

`mixed-1w` (server CPU 0, client CPUs 8–11). In the first run every segment
qualified:

| Segment | done/s | core | irq | `uinst/op` | `cpu/op` | IPC | p50 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| capacity | 589.0k | 100% | 27% | 1301 | 1237 ns | 0.51 | 1.7 ms | 2.0 ms |
| open-20k | 21.7k | 17% | 4% | 2161 | 4375 ns | 0.47 | 41 us | 112 us |
| open-100k | 102.1k | 48% | 11% | 1960 | 3174 ns | 0.48 | 42 us | 103 us |
| open-250k | 251.8k | 89% | 24% | 1814 | 2437 ns | 0.50 | 65 us | 1.4 ms |

- Capacity was 586k–610k in four further runs, with `uinst/op` 1301–1304.
- **Open-loop knee.** At 320k the CPU was 99% busy with p50 258 us; at 380k,
  p50 was 2.3 ms. The knee (about 300k) is about half of closed-loop capacity,
  because closed-loop queues keep 1024 requests in flight. Cost per request
  falls from about 2.4 us in the open-loop segments to 1.2 us at capacity.
  The rates are now 30k/120k/240k, about 10%, 40%, and 80% of the knee.

`mixed-4w` (server CPUs 0–3, client CPUs 8–15):

| Segment | done/s | core | `uinst/op` | `cpu/op` | p50 | p99 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| capacity | 1.06M | 99.9% | 3760 | 2847 ns | 1.9 ms | 3.1 ms |
| open-500k | 503.3k | 85% | 5115 | 4831 ns | 82 us | 233 us |
| open-680k | 682.8k | 94.5% | 4792 | 4027 ns | 117 us | 400 us |

- The workers were balanced, with 3750–3784 ms of CPU each in the capacity
  window. No ENOBUFS resets occurred up to 680k on the quiet host.
- **The sharded path costs 2.9 times the single-worker server's user
  instructions per request** (3760 vs 1301 at capacity). That is the largest
  optimization target the benchmark has shown; see the review's sharded
  response-copy findings.
- The rates are now 70k/280k/560k. A slower shared host reached the ENOBUFS
  reset at 640k, so the top rate keeps a margin until that is fixed.

**A/A, `mixed-1w`, five pairs.** The jobs restarted during the last pairs.
Capacity `uinst/op` deltas were −0.22% to +0.51% (spread 0.65%), and 8 of 10
capacity runs qualified.

**A/B, `mixed-1w`, five pairs**, shared host again (load average about 50), user
scope plus `--hold-cpus 16`: capacity `uinst/op` +1.40% to +1.66% in every
pair (spread 0.38%), 9 of 10 capacity runs qualified. This agrees with the
earlier isolated A/B (+1.57% to +1.77%). With the sibling held, 240k is past
the knee: p50 was 3 ms, so the harness now also disqualifies an open-loop
segment whose server CPUs are at least 98% busy (`saturated`).

**A/A and A/B, `mixed-4w`, five pairs**, shared host (load average 50–62),
user scope plus `--hold-cpus 16-19`. The four-worker layout gives other work
far more CPUs to interfere on: only 1 of 10 A/A capacity runs qualified, and
capacity `uinst/op` spread 2.1% (3761–3840). The A/B stopped after three
pairs, because pair 4's baseline hit the ENOBUFS reset at 560k. In the
completed pairs the candidate measured 3804–3829 against 3768–3778, about +1%,
which is not resolvable against that A/A spread. Until the ENOBUFS reset is
fixed, `mixed-4w` comparisons need a quiet host.

## Still to do

- Repeat the A/A and A/B comparisons on a host that stays quiet for their
  whole duration. Time, throughput, and latency metrics have not yet been
  qualified across a full comparison.
- Fix the sharded ENOBUFS overload reset. The 4-worker profile can then
  include a deliberate overload segment that checks recovery.
