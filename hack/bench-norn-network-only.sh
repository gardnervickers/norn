#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

payloads="${NORN_NETWORK_PAYLOADS:-64 256 1024 4096}"
pipelines="${NORN_NETWORK_PIPELINES:-1 32}"
workers_list="${NORN_NETWORK_WORKERS:-1 2 4}"
trials="${NORN_NETWORK_TRIALS:-3}"
threads="${NORN_NETWORK_CLIENT_THREADS:-16}"
client_cpus="${NORN_NETWORK_CLIENT_CPUS:-8-15,24-31}"
port_base="${NORN_NETWORK_PORT_BASE:-11320}"
timestamp="$(date -u +%Y%m%dT%H%M%SZ)"
log_dir="${NORN_NETWORK_LOG_DIR:-benches/logs/norn-network-only-${timestamp}}"

worker_cpus() {
    case "$1" in
        1) echo "${NORN_NETWORK_SERVER_CPUS_1:-0}" ;;
        2) echo "${NORN_NETWORK_SERVER_CPUS_2:-0,1}" ;;
        4) echo "${NORN_NETWORK_SERVER_CPUS_4:-0,1,2,3}" ;;
        *)
            echo "unsupported worker count '$1'; set NORN_NETWORK_WORKERS to a subset of: 1 2 4" >&2
            return 1
            ;;
    esac
}

if ! [[ "$trials" =~ ^[1-9][0-9]*$ ]]; then
    echo "NORN_NETWORK_TRIALS must be a positive integer" >&2
    exit 1
fi
if ! [[ "$threads" =~ ^[1-9][0-9]*$ ]]; then
    echo "NORN_NETWORK_CLIENT_THREADS must be a positive integer" >&2
    exit 1
fi
if ! [[ "$port_base" =~ ^[1-9][0-9]*$ ]] || (( port_base > 65500 )); then
    echo "NORN_NETWORK_PORT_BASE must be an integer from 1 through 65500" >&2
    exit 1
fi

mkdir -p "$log_dir"
summary="$log_dir/matrix.log"
{
    echo "benchmark_timestamp=$timestamp"
    echo "benchmark=norn-network-only"
    echo "git_revision=$(git rev-parse HEAD)"
    echo "kernel=$(uname -srmo)"
    echo "memtier_version=$(memtier_benchmark --version | head -n 1)"
    echo "payload_bytes=$payloads pipelines=$pipelines workers=$workers_list trials=$trials"
    echo "client_threads=$threads client_cpus=$client_cpus"
    echo "throughput_source=exact_completed_operations_over_external_memtier_process_wall_elapsed"
    echo "response_wire_bytes=24_byte_header_plus_4_byte_flags_plus_payload"
} | tee "$summary"

combination=0
for payload in $payloads; do
    if ! [[ "$payload" =~ ^[1-9][0-9]*$ ]]; then
        echo "payload sizes must be positive integers" >&2
        exit 1
    fi
    for pipeline in $pipelines; do
        if ! [[ "$pipeline" =~ ^[1-9][0-9]*$ ]]; then
            echo "pipelines must be positive integers" >&2
            exit 1
        fi
        if [[ "$pipeline" == 1 ]]; then
            clients_per_thread="${NORN_NETWORK_CLIENTS_P1:-8}"
            requests="${NORN_NETWORK_REQUESTS_P1:-31250}"
            warmup_requests="${NORN_NETWORK_WARMUP_P1:-2000}"
        else
            clients_per_thread="${NORN_NETWORK_CLIENTS_PIPELINED:-2}"
            requests="${NORN_NETWORK_REQUESTS_PIPELINED:-1000000}"
            warmup_requests="${NORN_NETWORK_WARMUP_PIPELINED:-5000}"
        fi
        for workers in $workers_list; do
            cpus="$(worker_cpus "$workers")"
            port=$((port_base + combination))
            if (( port > 65535 )); then
                echo "benchmark port range exceeds 65535" >&2
                exit 1
            fi
            run_dir="$log_dir/payload-${payload}-p${pipeline}-w${workers}"
            echo "combination payload_bytes=$payload pipeline=$pipeline workers=$workers worker_cpus=$cpus port=$port" | tee -a "$summary"
            NORN_KV_WORKERS="$workers" \
            NORN_KV_WORKER_CPUS="$cpus" \
            NORN_KV_CLIENT_CPUS="$client_cpus" \
            NORN_KV_ADDRESS="127.0.0.1:$port" \
            NORN_KV_FIXED_RESPONSE_BYTES="$payload" \
            NORN_KV_MEMTIER_THREADS="$threads" \
            NORN_KV_MEMTIER_CLIENTS_PER_THREAD="$clients_per_thread" \
            NORN_KV_MEMTIER_WARMUP_REQUESTS="$warmup_requests" \
            NORN_KV_MEMTIER_REQUESTS="$requests" \
            NORN_KV_TRIALS="$trials" \
            NORN_KV_RATIO="0:1" \
            NORN_KV_KEY_PATTERN="R:R" \
            NORN_KV_LOG_DIR="$run_dir" \
                ./hack/bench-norn-kv-memtier.sh "$pipeline"
            awk -v payload="$payload" -v pipeline="$pipeline" -v workers="$workers" \
                '/^trial=[0-9]+ / { print "result payload_bytes=" payload " pipeline=" pipeline " workers=" workers " " $0 }' \
                "$run_dir/summary.log" | tee -a "$summary"
            combination=$((combination + 1))
        done
    done
done

echo "raw_log=$summary"
