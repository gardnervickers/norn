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

## Still to do

- Repeat A/A and A/B on a quiet host. Then calibrate the `mixed-1w` and
  `mixed-4w` open-loop rates to about 10%, 40%, and 80% of the measured
  capacity, and check that the capacity segment reaches ≥ 90% server
  utilization with the profile's client threads.
- Fix the sharded ENOBUFS overload reset. The 4-worker profile can then
  include a deliberate overload segment that checks recovery.
