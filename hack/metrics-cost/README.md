# metrics-cost

Measurements behind the runtime metrics design: what one counter increment
costs under each mechanism considered, alone and on norn's hot paths.

- `src/main.rs`: one increment per loop iteration for each mechanism
  (`Cell`, single-writer and `fetch_add` atomics, `thread_local!`, the
  `metrics` facade with and without a Prometheus recorder, OpenTelemetry with
  no provider, no attributes, one attribute and a bound counter, `rdtsc`,
  `Instant::now()`), plus `mt-*` variants that run eight pinned threads.
  It is a standalone crate so its dependencies stay out of norn's lockfile.
- `run-single.sh [cpu] [reps] [variants...]`: per-iteration instructions and
  cycles from the difference of two run lengths, setup excluded.
- `run-insitu.sh <bin-dir> <probe> <units> <cpu> <reps> <args...>`: median
  instructions and median and minimum cycles per unit for `<probe>.<backend>`
  binaries, interleaved, against `BASE` (default `none`).

To compare backends in place, build `noop_count` and `yield_count` from
branch `bench/metrics-backends` once per `norn-stats-shim` feature, copy each
to `<bin-dir>/<probe>.<feature>`, and run `run-insitu.sh`. For example:

```sh
cargo build --release -p norn-uring --example noop_count --features norn-stats-shim/sw
cp target/release/examples/noop_count bin/noop_count.sw
BACKENDS="none sw" ./run-insitu.sh bin noop_count 1000320 12 7 64 1563 10
```

Cycle counts need a quiet core and sibling; instruction counts do not.
