#!/usr/bin/env bash
set -euo pipefail

workload="${1:-noop}"
server_cpu="${NORN_KV_SERVER_CPU:-0}"
client_cpus="${NORN_KV_CLIENT_CPUS:-8-15,24-31}"
address="${NORN_KV_ADDRESS:-127.0.0.1:11211}"
threads="${NORN_KV_LOAD_THREADS:-8}"
connections_per_thread="${NORN_KV_CONNECTIONS_PER_THREAD:-16}"
warmup_requests="${NORN_KV_WARMUP_REQUESTS:-5000}"
requests="${NORN_KV_REQUESTS:-100000}"
sample_every="${NORN_KV_SAMPLE_EVERY:-1024}"
value_size="${NORN_KV_VALUE_SIZE:-64}"
trials="${NORN_KV_TRIALS:-5}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="${NORN_KV_LOG_DIR:-benches/logs/norn-kv-network-${timestamp}}"

server_bin="target/release/norn-kv-server"
loadgen_bin="target/release/norn-kv-loadgen"
if [[ ! -x "$server_bin" || ! -x "$loadgen_bin" ]]; then
    echo "missing release binaries; run: nix develop -c cargo build --release -p norn-kv-server" >&2
    exit 1
fi

mkdir -p "$log_dir"
server_log="$log_dir/server.log"
trial_log="$log_dir/trials.log"

cleanup() {
    if [[ -n "${server_pid:-}" ]] && kill -0 "$server_pid" 2>/dev/null; then
        kill "$server_pid" 2>/dev/null || true
        wait "$server_pid" 2>/dev/null || true
    fi
}
trap cleanup EXIT INT TERM

taskset -c "$server_cpu" "$server_bin" \
    --listen "$address" \
    --ring-entries 256 \
    --max-connections 4096 \
    >"$server_log" 2>&1 &
server_pid=$!

host="${address%:*}"
port="${address##*:}"
ready=0
for _ in $(seq 1 100); do
    if ! kill -0 "$server_pid" 2>/dev/null; then
        echo "server exited before benchmark" >&2
        sed -n '1,120p' "$server_log" >&2
        exit 1
    fi
    if bash -c "exec 3<>/dev/tcp/${host}/${port}" 2>/dev/null; then
        ready=1
        break
    fi
    sleep 0.02
done
if [[ "$ready" != 1 ]]; then
    echo "server did not become ready at $address" >&2
    exit 1
fi

{
    echo "benchmark_timestamp=$timestamp"
    echo "workload=$workload server_cpu=$server_cpu client_cpus=$client_cpus address=$address"
    echo "threads=$threads connections_per_thread=$connections_per_thread warmup_requests=$warmup_requests requests=$requests sample_every=$sample_every value_size=$value_size trials=$trials"
} | tee "$trial_log"

for trial in $(seq 1 "$trials"); do
    echo "trial=$trial" | tee -a "$trial_log"
    taskset -c "$client_cpus" "$loadgen_bin" \
        --address "$address" \
        --workload "$workload" \
        --threads "$threads" \
        --connections-per-thread "$connections_per_thread" \
        --warmup-requests "$warmup_requests" \
        --requests "$requests" \
        --sample-every "$sample_every" \
        --value-size "$value_size" \
        --ring-entries 256 \
        | tee -a "$trial_log"
done

echo "raw_log=$trial_log"
