#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

pipeline="${1:-1}"
pairs="${2:-7}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="${NORN_KV_PAIRED_LOG_DIR:-benches/logs/norn-kv-paired-${timestamp}-p${pipeline}}"
memcached_bin="${NORN_KV_MEMCACHED_BIN:-memcached}"

summary_ops() {
    awk '/^trial=1 / {
        for (field = 1; field <= NF; field++) {
            if ($field ~ /^ops_per_sec=/) {
                split($field, parts, "=");
                print parts[2];
                exit;
            }
        }
    }' "$1"
}

if ! [[ "$pipeline" =~ ^[1-9][0-9]*$ ]]; then
    echo "pipeline must be a positive integer" >&2
    exit 1
fi
if ! [[ "$pairs" =~ ^[1-9][0-9]*$ ]]; then
    echo "pairs must be a positive integer" >&2
    exit 1
fi

threads="${NORN_KV_MEMTIER_THREADS:-16}"
if [[ "$pipeline" == 1 ]]; then
    clients_per_thread="${NORN_KV_MEMTIER_CLIENTS_PER_THREAD:-8}"
    requests="${NORN_KV_MEMTIER_REQUESTS:-12500}"
    warmup_requests="${NORN_KV_MEMTIER_WARMUP_REQUESTS:-1000}"
else
    clients_per_thread="${NORN_KV_MEMTIER_CLIENTS_PER_THREAD:-2}"
    requests="${NORN_KV_MEMTIER_REQUESTS:-50000}"
    warmup_requests="${NORN_KV_MEMTIER_WARMUP_REQUESTS:-2000}"
fi

mkdir -p "$log_dir"
summary="$log_dir/summary.log"
{
    echo "benchmark_timestamp=$timestamp"
    echo "comparison=paired-alternating servers=norn,memcached"
    echo "memcached_version=$("$memcached_bin" --version | head -n 1)"
    echo "pipeline=$pipeline pairs=$pairs threads=$threads clients_per_thread=$clients_per_thread"
    echo "warmup_requests=$warmup_requests requests=$requests"
    echo "server_cpu=${NORN_KV_SERVER_CPU:-0} client_cpus=${NORN_KV_CLIENT_CPUS:-8-15,24-31}"
} | tee "$summary"

for pair in $(seq 1 "$pairs"); do
    if (( pair % 2 == 1 )); then
        order=(norn memcached)
    else
        order=(memcached norn)
    fi

    for position in 0 1; do
        server="${order[$position]}"
        port=$((11210 + pair * 2 + position))
        run_dir="$log_dir/pair-$pair-$server"
        NORN_KV_SERVER_KIND="$server" \
        NORN_KV_ADDRESS="127.0.0.1:$port" \
        NORN_KV_MEMTIER_THREADS="$threads" \
        NORN_KV_MEMTIER_CLIENTS_PER_THREAD="$clients_per_thread" \
        NORN_KV_MEMTIER_WARMUP_REQUESTS="$warmup_requests" \
        NORN_KV_MEMTIER_REQUESTS="$requests" \
        NORN_KV_TRIALS=1 \
        NORN_KV_LOG_DIR="$run_dir" \
            ./hack/bench-norn-kv-memtier.sh "$pipeline"
    done

    norn_dir="$log_dir/pair-$pair-norn"
    memcached_dir="$log_dir/pair-$pair-memcached"
    norn_json="$norn_dir/trial-1.json"
    memcached_json="$memcached_dir/trial-1.json"
    norn_ops="$(summary_ops "$norn_dir/summary.log")"
    memcached_ops="$(summary_ops "$memcached_dir/summary.log")"
    if ! [[ "$norn_ops" =~ ^[1-9][0-9]*([.][0-9]+)?$ && \
            "$memcached_ops" =~ ^[1-9][0-9]*([.][0-9]+)?$ ]]; then
        echo "pair $pair did not report valid global throughput" >&2
        exit 1
    fi
    norn_p99="$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.00"' "$norn_json")"
    memcached_p99="$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.00"' "$memcached_json")"
    delta="$(awk -v norn="$norn_ops" -v memcached="$memcached_ops" 'BEGIN { printf "%.2f", (norn / memcached - 1) * 100 }')"
    echo "pair=$pair first=${order[0]} norn_ops=$norn_ops memcached_ops=$memcached_ops delta_percent=$delta norn_p99_ms=$norn_p99 memcached_p99_ms=$memcached_p99" | tee -a "$summary"
done

echo "raw_log=$summary"
