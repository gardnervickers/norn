#!/usr/bin/env bash

set -euo pipefail

label="${1:?usage: $0 LABEL [RUNS] [FILTER]}"
runs="${2:-7}"
filter="${3:-}"
output_root="${NORN_CHANNEL_BENCH_DIR:-/tmp/norn-channel-benchmark}"

if [[ ! "$label" =~ ^[a-zA-Z0-9._-]+$ ]]; then
    echo "invalid benchmark label: $label" >&2
    exit 2
fi
if [[ ! "$runs" =~ ^[1-9][0-9]*$ ]]; then
    echo "invalid run count: $runs" >&2
    exit 2
fi

output_dir="$output_root/$label"
mkdir -p "$output_dir"

cargo_args=(bench -p benches --bench channel)
if [[ -n "$filter" ]]; then
    cargo_args+=("$filter")
fi

for run in $(seq 1 "$runs"); do
    log="$output_dir/run-$run.log"
    taskset -c 2,4,6,8,10 \
        env \
        NORN_CHANNEL_CONSUMER_CPU=2 \
        NORN_CHANNEL_PRODUCER_CPUS=4,6,8,10 \
        nix develop -c \
        cargo "${cargo_args[@]}" \
        2>&1 | tee "$log"
done
