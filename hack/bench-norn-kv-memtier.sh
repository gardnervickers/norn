#!/usr/bin/env bash
set -euo pipefail

pipeline="${1:-32}"
server_cpu="${NORN_KV_SERVER_CPU:-0}"
client_cpus="${NORN_KV_CLIENT_CPUS:-8-15,24-31}"
address="${NORN_KV_ADDRESS:-127.0.0.1:11211}"
server_kind="${NORN_KV_SERVER_KIND:-norn}"
threads="${NORN_KV_MEMTIER_THREADS:-16}"
clients_per_thread="${NORN_KV_MEMTIER_CLIENTS_PER_THREAD:-2}"
warmup_requests="${NORN_KV_MEMTIER_WARMUP_REQUESTS:-2000}"
requests="${NORN_KV_MEMTIER_REQUESTS:-50000}"
trials="${NORN_KV_TRIALS:-5}"
key_minimum="${NORN_KV_KEY_MINIMUM:-1}"
key_maximum="${NORN_KV_KEY_MAXIMUM:-100000}"
ratio="${NORN_KV_RATIO:-1:9}"
key_pattern="${NORN_KV_KEY_PATTERN:-G:G}"
data_size_list="${NORN_KV_DATA_SIZE_LIST:-64:40,256:40,1024:15,4096:5}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="${NORN_KV_LOG_DIR:-benches/logs/norn-kv-memtier-${timestamp}-p${pipeline}}"

server_bin="target/release/norn-kv-server"
memcached_bin="${NORN_KV_MEMCACHED_BIN:-memcached}"
recv_mode="${NORN_KV_RECV_MODE:-multishot}"
if [[ "$server_kind" == norn && ! -x "$server_bin" ]]; then
    echo "missing release server; run: nix develop -c cargo build --release -p norn-kv-server" >&2
    exit 1
fi
for tool in jq memtier_benchmark taskset; do
    if ! command -v "$tool" >/dev/null; then
        echo "missing $tool; run this benchmark from nix develop" >&2
        exit 1
    fi
done
if [[ "$server_kind" == memcached ]] && ! command -v "$memcached_bin" >/dev/null; then
    echo "missing memcached; run this benchmark from nix develop" >&2
    exit 1
fi
if [[ "$server_kind" != norn && "$server_kind" != memcached ]]; then
    echo "NORN_KV_SERVER_KIND must be norn or memcached" >&2
    exit 1
fi
if ! [[ "$pipeline" =~ ^[1-9][0-9]*$ ]]; then
    echo "pipeline must be a positive integer" >&2
    exit 1
fi

mkdir -p "$log_dir"
server_log="$log_dir/server.log"
summary_log="$log_dir/summary.log"

cleanup() {
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

host="${address%:*}"
port="${address##*:}"
if [[ "$server_kind" == norn ]]; then
    server_args=(
        --listen "$address"
        --ring-entries 256
        --max-connections 4096
    )
    if [[ -n "$recv_mode" ]]; then
        server_args+=(--recv-mode "$recv_mode")
    fi
    taskset -c "$server_cpu" "$server_bin" \
        "${server_args[@]}" \
        >"$server_log" 2>&1 &
else
    taskset -c "$server_cpu" "$memcached_bin" \
        --listen "$host" \
        --port "$port" \
        --udp-port 0 \
        --threads 1 \
        --conn-limit 4096 \
        --memory-limit 512 \
        --protocol binary \
        >"$server_log" 2>&1 &
fi
server_pid=$!

ready=0
for _ in $(seq 1 100); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "server exited before benchmark" >&2
        sed -n '1,120p' "$server_log" >&2
        exit 1
    fi
    if [[ "$server_kind" == norn ]] && grep -q '^listening on ' "$server_log"; then
        ready=1
        break
    fi
    if [[ "$server_kind" == memcached ]] && bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.02
done
if [[ "$ready" != 1 ]]; then
    echo "server did not become ready at $address" >&2
    exit 1
fi

common=(
    --server="$host"
    --port="$port"
    --protocol=memcache_binary
    --key-minimum="$key_minimum"
    --data-size-list="$data_size_list"
    --hide-histogram
)
mixed=(
    "${common[@]}"
    --threads="$threads"
    --clients="$clients_per_thread"
    --ratio="$ratio"
    --pipeline="$pipeline"
    --key-maximum="$key_maximum"
    --key-pattern="$key_pattern"
    --distinct-client-seed
)

# memtier treats --key-maximum as exclusive for --requests=allkeys but may
# select it for random/Gaussian workloads. Prefill one extra key to cover the
# measured workload's inclusive upper endpoint.
prefill_maximum=$((key_maximum + 1))
taskset -c "$client_cpus" memtier_benchmark \
    "${common[@]}" \
    --threads=1 \
    --clients=1 \
    --ratio=1:0 \
    --pipeline=64 \
    --requests=allkeys \
    --key-maximum="$prefill_maximum" \
    --key-pattern=S:S \
    >"$log_dir/prefill.log" 2>&1

{
    if [[ "$server_kind" == norn ]]; then
        reported_recv_mode="${recv_mode:-exact}"
    else
        reported_recv_mode="not-applicable"
    fi
    echo "benchmark_timestamp=$timestamp"
    echo "server_kind=$server_kind server_cpu=$server_cpu client_cpus=$client_cpus address=$address recv_mode=$reported_recv_mode"
    echo "threads=$threads clients_per_thread=$clients_per_thread pipeline=$pipeline"
    echo "ratio=$ratio key_range=$key_minimum..=$key_maximum key_pattern=$key_pattern"
    echo "data_size_list=$data_size_list warmup_requests=$warmup_requests requests=$requests trials=$trials"
} | tee "$summary_log"

for trial in $(seq 1 "$trials"); do
    warmup_log="$log_dir/warmup-${trial}.log"
    trial_log="$log_dir/trial-${trial}.log"
    trial_json="$log_dir/trial-${trial}.json"

    taskset -c "$client_cpus" memtier_benchmark \
        "${mixed[@]}" \
        --requests="$warmup_requests" \
        >"$warmup_log" 2>&1

    taskset -c "$client_cpus" memtier_benchmark \
        "${mixed[@]}" \
        --requests="$requests" \
        --json-out-file="$trial_json" \
        >"$trial_log" 2>&1

    expected_requests=$((threads * clients_per_thread * requests))
    actual_requests="$(jq -r '."ALL STATS".Totals.Count' "$trial_json")"
    misses="$(jq -r '."ALL STATS".Gets."Misses/sec"' "$trial_json")"
    connection_errors="$(jq -r '."ALL STATS".Totals."Connection Errors" // 0' "$trial_json")"
    if [[ "$actual_requests" != "$expected_requests" || "$misses" != "0" && "$misses" != "0.00" || "$connection_errors" != "0" ]]; then
        echo "trial $trial failed correctness guardrails: expected=$expected_requests actual=$actual_requests misses=$misses connection_errors=$connection_errors" >&2
        exit 1
    fi

    ops="$(jq -r '."ALL STATS".Totals."Ops/sec"' "$trial_json")"
    p50="$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p50.00"' "$trial_json")"
    p99="$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.00"' "$trial_json")"
    p999="$(jq -r '."ALL STATS".Totals."Percentile Latencies"."p99.90"' "$trial_json")"
    echo "trial=$trial ops_per_sec=$ops p50_ms=$p50 p99_ms=$p99 p999_ms=$p999 requests=$actual_requests misses=$misses connection_errors=$connection_errors" | tee -a "$summary_log"
done

if [[ "$server_kind" == norn ]]; then
    if grep -vE '^connection failed: Connection reset by peer \(os error 104\)$' "$server_log" | grep -q '^connection failed:'; then
        echo "server reported an unexpected connection failure" >&2
        grep '^connection failed:' "$server_log" >&2
        exit 1
    fi
fi

echo "raw_log=$summary_log"
