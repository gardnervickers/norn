#!/usr/bin/env bash
set -euo pipefail

if [[ $(uname -s) != Linux ]]; then
  echo "grpc_runtime requires Linux because the Norn side uses norn-uring" >&2
  exit 1
fi

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

runs=${NORN_GRPC_BENCH_RUNS:-3}
filter=${NORN_GRPC_BENCH_FILTER:-bench_grpc_runtime}
timestamp=$(date -u +%Y%m%dT%H%M%SZ)
output_dir=${1:-target/bench-results/grpc-runtime/$timestamp}

if [[ ! $runs =~ ^[1-9][0-9]*$ ]]; then
  echo "NORN_GRPC_BENCH_RUNS must be a positive integer" >&2
  exit 1
fi

mkdir -p "$output_dir"

{
  echo "captured_at_utc=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "commit=$(git rev-parse HEAD)"
  echo "filter=$filter"
  echo "runs=$runs"
  echo "cpu_pin=${NORN_GRPC_BENCH_CPU:-unbound}"
  echo "RUSTFLAGS=${RUSTFLAGS:-}"
  echo "CARGO_PROFILE_BENCH_LTO=${CARGO_PROFILE_BENCH_LTO:-}"
  uname -a
  rustc -Vv
  cargo -V
  if command -v lscpu >/dev/null 2>&1; then
    lscpu
  fi
  git status --short
} >"$output_dir/environment.txt"

command=(cargo bench -p benches --bench grpc_runtime -- "$filter")
if [[ -n ${NORN_GRPC_BENCH_CPU:-} ]]; then
  if ! command -v taskset >/dev/null 2>&1; then
    echo "NORN_GRPC_BENCH_CPU requires taskset" >&2
    exit 1
  fi
  command=(taskset -c "$NORN_GRPC_BENCH_CPU" "${command[@]}")
fi

for ((run = 1; run <= runs; run++)); do
  echo "grpc runtime benchmark run $run/$runs"
  "${command[@]}" 2>&1 | tee "$output_dir/run-$run.log"
done

echo "wrote benchmark evidence to $output_dir"
