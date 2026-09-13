# Mixed workload performance screening, 2026-09-13

## Contract

Run on the authorized local Ryzen 9 5950X workstation, Linux 6.18.48.
Use the existing release client and server from workload commit `1051145`;
the server source is unchanged from current master `9b5e75b`. GitHub master
and the successful PR #151 checks were verified before this screen.

The initial goal is three eligible repetitions before a server comparison.
Preserve the 2 ms generator-lateness limit, full payload validation, exact
accounting, and zero final outstanding requests. Record every rejected run.
For comparisons, freeze the client, manifest, CPU assignment, and server build
settings. Compare class-specific offered-latency tails at the same offered
rate; increase offered rates in a separate capacity experiment. A fixed-rate
screen does not establish maximum throughput.

## Unchanged eight-thread generator

```sh
python3 hack/bench-mixed-workload.py \
  --manifest examples/norn-kv-server/workloads/mixed-v1-balanced.json \
  --server-cpus 0 --client-cpus 8,9,10,11,12,13,14,15 \
  --runs 3 --require-performance
```

Raw artifacts: `benches/logs/mixed-workload/20260913T062304Z-1763441/`.
All trials passed response validity and final accounting.

| Trial | Planned | Valid responses | Generator misses | Maximum schedule lag (us) | Eligible |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 400,000 | 400,000 | 0 | 267.836 | yes |
| 2 | 400,000 | 400,000 | 0 | 170.076 | yes |
| 3 | 400,000 | 399,986 | 14 | 2316.847 | no |

This configuration did not meet the repetition gate. No server optimization
claim follows from these trials.

Frozen executable identities:

```text
client sha256 0025115626cbbe08021819cdf8ca305f4258da54704c6c269a036d14551ed692
server sha256 5f1014a5b0c62a3679c21f8c9d75d74d6296ce8b686732f738de76106243ee0c
```

These are retained release artifacts, not binaries rebuilt in this screen.
The runner records the currently available `rustc`; that field alone does not
prove the compiler used for a pre-existing executable.

## First server comparison

PR #141 changes sharded channel lane selection. The CLI routes `--workers 1`
directly into the single-worker server, bypassing the affected channel path.
Use four workers and a matching manifest for its application-level comparison.
Reuse-port connection placement varies between runs, so interpret this as an
application regression screen alongside the existing channel microbenchmark.

Compare workload commit `1051145` with that same commit plus only #141's
`norn-channel/src/mpsc.rs` patch. Comparing old performance branch heads
directly would also change intervening correctness fixes. An isolated candidate
checkout is prepared at `/tmp/norn-mixed-perf-141`; it has not been accepted as
a measured improvement.

PR #145 needs a separate call-path check: adding a bulk-send API does not make
the mixed server use it. Zero-copy performance needs physical-network evidence.

## Single-thread generator qualification

Change only the balanced manifest's `threads` from 8 to 1, and use
`--client-cpus 8`. Keep all 128 connections, 20,000 requests/s, durations,
payloads, and the 2 ms lateness limit. Run the same command with that manifest
and `--runs 3 --require-performance`.

Raw artifacts: `benches/logs/mixed-workload/20260913T062512Z-1764398/`.
All three trials validated 400,000 of 400,000 measured arrivals, with zero
generator misses and zero outstanding requests. Maximum scheduling lag was
1035.747, 899.512, and 1288.712 us respectively. This qualifies this client
configuration at this rate; it does not establish higher-rate capacity.

A source review found contiguous connection assignment gives each of eight
client runtimes clustered arrival windows. Striped assignment is a plausible
future generator improvement, but is not part of this server comparison.

## Four-worker PR #141 screen

The manifest is the single-client balanced qualification manifest with
`server_workers = 4`; all request rates and payloads remain unchanged.
Server CPUs are `0,1,2,3`, client CPU is `8`. Intended sequence is three
pairs: baseline/candidate, candidate/baseline, baseline/candidate. Stop on
any ineligible run and retain it instead of rerunning until it passes.

Both servers were built with:

```sh
nix develop -c env CARGO_INCREMENTAL=0 cargo build --release \
  -p norn-kv-server --bin norn-kv-server
```

The candidate used the baseline target directory as a compilation cache.
Cargo initially left the candidate at the common executable output path when
checking the baseline. Hash verification caught this before measurement;
cleaning the task-local KV release package and rebuilding produced a distinct
baseline. Frozen copies are outside the Cargo output directory.

```text
baseline server sha256 3e4440691595479fbf41ea22680786effaafd9b16bb910a875eeb9fcb88e2208
candidate server sha256 062aab776085826dadc21dc3e079ee697077e78ac7a6d110a0bdf0a50b24bb84
client sha256 0025115626cbbe08021819cdf8ca305f4258da54704c6c269a036d14551ed692
```

Candidate source differs only by `git show 268e1bf --format= --
norn-channel/src/mpsc.rs`. Its added round-robin contract test passed with
`cargo test -p norn-channel sharded_receiver_round_robins_ready_lanes`
inside the Nix shell, with debug information and incremental builds disabled.

Each invocation uses:

```sh
python3 hack/bench-mixed-workload.py \
  --manifest /tmp/norn-perf-screen-20260913/balanced-four-workers.json \
  --server /tmp/norn-perf-screen-20260913/LABEL-server \
  --client /tmp/norn-perf-screen-20260913/client \
  --workers 4 --server-cpus 0,1,2,3 --client-cpus 8 \
  --runs 1 --require-performance --output-dir OUTPUT
```

Raw artifacts and the actual command sequence are under
`benches/logs/mixed-workload/screen-141-20260913/`. The copied manifests and
executable hashes in those artifacts identify each run; the runner's checkout
revision describes the harness, not the external candidate executable.

### Result

All six trials were valid and performance-eligible. Each completed and validated
all 400,000 measured requests with no generator misses or final outstanding work.

| Pair | Binary | Small read p99 (us) | Medium read p99 (us) | Large read p99 (us) | Overwrite p99 (us) |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | baseline | 51.455 | 54.431 | 85.503 | 62.687 |
| 1 | candidate | 49.631 | 52.511 | 83.711 | 53.183 |
| 2 | baseline | 51.679 | 54.335 | 85.055 | 62.111 |
| 2 | candidate | 52.927 | 63.327 | 86.015 | 63.263 |
| 3 | baseline | 50.303 | 52.927 | 84.799 | 61.887 |
| 3 | candidate | 51.007 | 53.599 | 84.863 | 61.599 |

There is no repeatable application latency improvement at this offered rate.
The second pair includes a 16.5% medium-read p99 increase; three pairs cannot
establish whether this is a persistent regression. Keep that observation rather
than treating favorable medians as evidence of a win. This screen does not
invalidate the separately measured channel microbenchmark improvement, and does
not justify a new application-level performance claim or merge decision.

The exact four-worker manifest is retained as
`examples/norn-kv-server/workloads/mixed-v1-balanced-four-workers.json`.
Use this as a low-rate regression screen. Before capacity claims, qualify the
frozen generator at each higher offered rate, then locate the baseline's latency
knee and run matched comparisons there. The interference and recovery profiles
remain additional gates; they were not measured in this screen.

After measurement, all 20 candidate channel tests and its doctest passed.
