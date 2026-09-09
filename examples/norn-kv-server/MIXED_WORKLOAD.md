# Mixed network workload

The approved service workload is a network first benchmark for the optimized
`norn-kv-server`. It uses the server's normal in memory handler and binary
GET/SET framing. The fixed response handler and the older memtier and network
scripts remain useful transport diagnostics; they are not the primary mixed
workload.

The workload manifest is versioned JSON. It fixes the seed, key populations,
request mix, connection and pipeline bounds, byte budget, phase durations,
offered rates, deadlines, and histogram range. Keep one manifest unchanged
when comparing server revisions. The client schedules arrivals independently
of response completion, records unsent work and late responses, validates every
opaque ID and payload, drains outstanding requests, and writes a JSON result
to the path supplied by `--output`.

Build both release binaries once, then run a smoke trial:

```sh
nix develop -c cargo build --release -p norn-kv-server --bins
python3 hack/bench-mixed-workload.py \
  --manifest examples/norn-kv-server/workloads/mixed-v1-smoke.json \
  --server target/release/norn-kv-server \
  --client target/release/norn-mixed-loadgen \
  --smoke
```

The runner starts a fresh server for every run, waits for its announced
listener and verifies it with a TCP connect probe. It records the copied
manifest, SHA-256 hashes of both binaries, host and topology information,
affinity, server output, client output, and the client's machine readable
result under `benches/logs/mixed-workload/`. It terminates only the process it
started. A forced kill or an incomplete client result makes the trial
unsuitable for a performance claim.

For a measured comparison, provide disjoint CPU lists. The runner checks that
they do not overlap and, where Linux topology files are available, that they
do not share a physical core:

```sh
python3 hack/bench-mixed-workload.py \
  --manifest examples/norn-kv-server/workloads/mixed-v1-balanced.json \
  --workers 1 --server-cpus 0 --client-cpus 8,9,10,11,12,13,14,15 \
  --require-performance
```

Use the same manifest, client binary, CPU assignment, and runner options for
the baseline and candidate. The runner itself does not publish results or
claim that process exit proves kernel resource reclamation. Inspect the
per-class accounting, deadline misses, unresolved requests, drain duration,
and client scheduling lag before accepting a result.

## Profiles and inputs

| Manifest | Traffic |
| --- | --- |
| `mixed-v1-balanced.json` | 70% 256 B GET, 20% 4 KiB GET, 5% 32 KiB GET, 5% 4 KiB SET |
| `mixed-v1-interference.json` | One quarter of connections issue small probes; the remainder issue large reads targeting shard zero on four server workers |
| `mixed-v1-overload.json` | Balanced mix with steady, elevated-rate, and recovery intervals |
| `mixed-v1-smoke.json` | Small populations and short intervals for conformance checks |

The default full dataset contains 56 MiB of immutable values and 4 MiB of
connection-owned overwrite slots, excluding key/table overhead. Population and
connections finish before the shared phase start. Every overwrite slot is
initialized before warmup; each logical connection is its only writer. Final
acknowledged values are read back after drain. Read payloads are checked in
full. GET and SET use acknowledgements and per-connection opaque IDs.

Rates are total intended requests per second, not rates per thread. The
default 20,000 requests/s is an initial characterization rate, not a measured
saturation point. Calibrate a baseline rate sweep before choosing an overload
rate for optimization. Freeze those absolute rates for both builds. Each
connection has both a request-slot limit and a byte reservation for outgoing
frames plus expected replies. Credits remain held through response validation.

The independent Tokio client uses cooperative polling close to arrival times
to avoid imposing millisecond timer granularity on the measurement. Allocate
dedicated client cores and retain the same client binary for all comparisons.
Client CPU and memory cost are part of generator qualification. Fixed per-class
histograms are currently allocated per connection; high connection counts can
make client memory significant. Inspect `resources.json` before interpreting
a source-limited run.

Interference keys use the server's current FNV-1a ownership rule. Reuse-port
connection placement can vary, so this profile does not promise an exact
local/remote dispatch ratio. The one-worker control and four-worker profile
are separate experiments. Kernel completion counters and per-worker dispatch
instrumentation are not supplied by this client.

## Reading a result

`valid` means response validation and final request accounting succeeded.
`source_valid` additionally requires no measured-phase generator misses or
histogram overflow. `performance_eligible` also rejects unsent requests outside
the deliberate overload interval. Warmup does not determine source quality.
`--require-performance` makes the runner fail when that flag is false, while
retaining the result for diagnosis. A deadline miss is observed and reported;
it never aborts a request. Late responses continue to consume credits and drain.

Each class/cohort obeys these equations:

```text
planned = started + unsent_generator + unsent_backpressure + unsent_transport
started = valid_response + invalid_response + transport_failed + outstanding
```

Here `started` means handed to the bounded writer queue. Wire latency begins
when the writer begins that frame; offered-load latency begins at its intended
arrival. `outstanding` must be zero for a valid final result. Per-connection
high-water marks are maxima, not estimates of aggregate simultaneous occupancy.

`process_elapsed_s` includes setup, warmup, verification, and report aggregation.
Use `measurement_duration_s` for the scheduled measured interval, and use the
individual manifest phase durations for phase rates. Never divide measured-only
completions by process elapsed time. Cohort completions can extend into drain;
`completion_windows` groups responses by completion second relative to the
shared start, including warmup. This keeps late overload replies attributable
to their original cohort while showing when they actually complete.

Phase/class HDR histograms use three significant figures; completion-window
histograms use two. Bins are exported with counts and upper bounds in
microseconds. Merge counts within one run, retaining repetitions separately.
The p99 and p99.9 fields are null below 10,000 and 100,000 observations,
respectively, so a rare class needs a longer run to populate its tail statistic.
Overflow is counted and prevents source qualification. `drain_s` is the maximum
connection drain interval; it is not proof of server-side kernel reclamation.

`resources.json` samples process CPU totals and RSS every 100 ms. These samples
include setup and verification, may miss short peaks, and are diagnostic rather
than an exact measurement-window CPU-per-operation result. Server termination
is runner cleanup; the result makes no graceful-shutdown guarantee.

The runner supports explicit `--client` and `--server` paths for comparisons.
Build each server separately, pass the same frozen client path to both, and
alternate process-isolated baseline/candidate runs. Record each immediate-parent
comparison separately from a full-stack comparison. Keep existing component
benchmarks as diagnostic controls. Loopback evidence does not establish physical
NIC throughput or zero-copy performance.

## Validation

```sh
nix develop -c cargo test -p norn-kv-server --all-targets
nix develop -c cargo clippy -p norn-kv-server --all-targets -- -D warnings
python3 -m unittest hack/test-bench-mixed-workload.py
```

The client can also target a separately started memcached binary with
`norn-mixed-loadgen --manifest PATH --address HOST:PORT --output RESULT.json`.
It accepts opaque CAS values, validates the same response payloads, and performs
the same final write readback. Record the external server's version and memory
budget separately; the runner currently starts Norn servers only.
