# Mixed workload qualification, 2026-09-09

These are harness qualification results on the local Ryzen 9 5950X/Linux
workstation. They establish protocol/accounting coverage and expose generator
noise. They do not establish an optimization win or maximum sustainable rate.
The server implementation is unchanged from master `9b5e75b`.

## Checks

- One-worker mixed smoke: every planned request completed and validated;
  no outstanding work remained after drain.
- Four-worker interference smoke: small probes and large hot-shard reads
  completed with valid payloads and balanced final accounting.
- Local memcached conformance: population, mixed GET/SET, opaque CAS values,
  full payload validation, and final acknowledged-write readback passed.
- Deliberate pressure with a one-request window and 1,000,000 scheduled
  requests/s during the elevated-rate phase: 300,000 planned arrivals,
  33,667 valid responses and 266,333 explicit unsent-backpressure outcomes;
  no outstanding requests after drain. The overall trial was correctly marked
  performance-ineligible because ordinary phases also rejected work.

## Full balanced qualification

The full manifest uses a 60 MiB value population, 128 connections, eight client
threads, pipeline limit 32, and 20,000 total intended requests/s. Each trial
has 10 seconds of warmup and 20 measured seconds. Server CPU: 0. Client CPU
mask: 8,9,10,11,12,13,14,15. Client and server do not share physical cores.
The generator-lateness tolerance remained 2 ms for all three trials.

```sh
nix develop -c python3 hack/bench-mixed-workload.py \
  --manifest examples/norn-kv-server/workloads/mixed-v1-balanced.json \
  --server-cpus 0 --client-cpus 8,9,10,11,12,13,14,15 \
  --runs 3 --require-performance
```

The command stopped after the second run failed performance eligibility. One
additional run with `--runs 1` collected the third observation.

| Trial | Planned | Valid responses | Unsent generator | Outstanding after drain | Performance eligible |
| --- | ---: | ---: | ---: | ---: | --- |
| 1 | 400,000 | 400,000 | 0 | 0 | yes |
| 2 | 400,000 | 399,992 | 8 | 0 | no |
| 3 | 400,000 | 399,897 | 103 | 0 | no |

All three passed response validity and accounting. Trial 2 included client
schedule stalls up to 2.44 ms. Rejected arrivals are retained in the report;
neither a reduced denominator nor a relaxed threshold is used to turn those
trials into successful performance measurements. The next optimization
experiment needs a qualified generator/environment and repeated eligible runs.

For context only, eligible trial 1 reported offered-load p99 values of 35.295 us
(small read), 37.791 us (medium read), 71.999 us (large read), and 37.631 us
(overwrite). These are one trial at a fixed offered rate. No comparative or
repeatability claim is made from them.

The three full trials used identical binaries:

```text
client sha256 300b228831bdb5523f7f0fa159e108029a9e7398277682044c9696049037d0e2
server sha256 5f1014a5b0c62a3679c21f8c9d75d74d6296ce8b686732f738de76106243ee0c
```

Raw manifests, results, histograms, process samples, and provenance were
retained locally under `benches/logs/mixed-workload/`:

- `20260909T221605Z-1548085`: initial one-worker smoke.
- `20260909T221954Z-1550962`: four-worker interference smoke.
- `memcached-conformance`: external protocol validation.
- `20260909T222252Z-1551972`: intentional pressure check.
- `20260909T222331Z-1552086`: full trials 1 and 2.
- `20260909T222548Z-1552319`: full trial 3.

These runs occurred during harness development. A subsequent correctness
review changed wire IDs to advance only for admitted requests, preventing a
long sequence of skipped arrivals from reusing a still-outstanding wire ID.
The final source is validated separately; the hashes above identify the exact
qualification binaries and should not be mistaken for final-head timings.

## Final-source validation

The release smoke after the admission-ID correction passed all three validity
flags, with 1,700 valid responses including warmup and zero outstanding work.
Its artifact is `20260909T223429Z-1565212` in the same local log directory.

Validation commands:

```sh
nix develop -c cargo test -p norn-kv-server --all-targets
nix develop -c cargo clippy --workspace --all-targets --all-features -- -D warnings
nix develop -c env CARGO_PROFILE_TEST_DEBUG=0 CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 cargo test --workspace --all-features
python3 -m unittest hack/test-bench-mixed-workload.py
```

The full workspace suite passed 411 tests, including doctests; the Python
runner passed four tests. An initial full workspace build exhausted available
disk space while linking with debug information. Removing this worktree's
generated debug artifacts and rerunning with the settings above succeeded.
Source files, release binaries, and benchmark evidence were retained.
