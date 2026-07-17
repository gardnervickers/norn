#!/usr/bin/env bash
set -euo pipefail

: "${NORN_FIXEDBUF_RESULT_DIR:?set NORN_FIXEDBUF_RESULT_DIR to an empty or new result directory}"
: "${NORN_FIXEDBUF_BENCH_DIR:?set NORN_FIXEDBUF_BENCH_DIR to the benchmark data directory}"

cpu="${NORN_FIXEDBUF_CPU:-15}"
smt_sibling="${NORN_FIXEDBUF_SMT_SIBLING:-31}"
trials="${NORN_FIXEDBUF_TRIALS:-7}"
result_dir="${NORN_FIXEDBUF_RESULT_DIR}"
bench_dir="$(realpath -e "${NORN_FIXEDBUF_BENCH_DIR}")"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "fixed-buffer benchmarks require Linux" >&2
  exit 1
fi

if [[ "${trials}" -lt 7 ]]; then
  echo "NORN_FIXEDBUF_TRIALS must be at least 7" >&2
  exit 1
fi

if [[ ! -d "${bench_dir}" ]]; then
  echo "benchmark data path is not a directory: ${bench_dir}" >&2
  exit 1
fi

governor_path="/sys/devices/system/cpu/cpu${cpu}/cpufreq/scaling_governor"
if [[ ! -r "${governor_path}" ]] || [[ "$(<"${governor_path}")" != "performance" ]]; then
  echo "CPU ${cpu} must use the performance governor" >&2
  exit 1
fi

thread_siblings_path="/sys/devices/system/cpu/cpu${cpu}/topology/thread_siblings_list"
thread_siblings="$(<"${thread_siblings_path}")"
if ! grep -Eq "(^|,)${smt_sibling}(,|$)" <<<"${thread_siblings}"; then
  echo "CPU ${smt_sibling} is not an SMT sibling of CPU ${cpu}: ${thread_siblings}" >&2
  exit 1
fi

read -r mount_target mount_source filesystem mount_options device_maj_min \
  < <(findmnt -n -T "${bench_dir}" -o TARGET,SOURCE,FSTYPE,OPTIONS,MAJ:MIN)
expected_filesystem="${NORN_FIXEDBUF_EXPECT_FSTYPE:-ext4}"
if [[ "${filesystem}" != "${expected_filesystem}" ]]; then
  echo "benchmark data path must use ${expected_filesystem}, found ${filesystem}" >&2
  exit 1
fi
block_device="${mount_source%%\[*}"
device_model="$(lsblk -ndo MODEL "${block_device}" 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)"
if [[ -z "${device_model}" ]]; then
  parent_device="$(lsblk -ndo PKNAME "${block_device}" 2>/dev/null || true)"
  if [[ -n "${parent_device}" ]]; then
    device_model="$(lsblk -ndo MODEL "/dev/${parent_device}" 2>/dev/null | sed -e 's/^[[:space:]]*//' -e 's/[[:space:]]*$//' || true)"
  fi
fi

if [[ -e "${result_dir}" ]]; then
  if [[ ! -d "${result_dir}" ]]; then
    echo "result path exists and is not a directory: ${result_dir}" >&2
    exit 1
  fi
  if [[ -n "$(find "${result_dir}" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "result directory is not empty: ${result_dir}" >&2
    exit 1
  fi
fi
mkdir -p "${result_dir}"
result_dir="$(realpath -e "${result_dir}")"
manifest="${result_dir}/order.tsv"

ps -eLo psr,pid,tid,comm >"${result_dir}/processes-before.tsv"

{
  echo -e "field\tvalue"
  echo -e "started_utc\t$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo -e "git_revision\t$(git rev-parse HEAD)"
  echo -e "git_dirty\t$(test -n "$(git status --porcelain)" && echo true || echo false)"
  echo -e "kernel\t$(uname -srvo)"
  echo -e "cpu\t${cpu}"
  echo -e "governor\t$(<"${governor_path}")"
  echo -e "thread_siblings\t${thread_siblings}"
  echo -e "expected_idle_smt_sibling\t${smt_sibling}"
  echo -e "isolated_cpus\t$(</sys/devices/system/cpu/isolated)"
  echo -e "memlock_kib\t$(ulimit -l)"
  echo -e "trials\t${trials}"
  echo -e "bench_dir_realpath\t${bench_dir}"
  echo -e "mount_target\t${mount_target}"
  echo -e "mount_source\t${mount_source}"
  echo -e "block_device\t${block_device}"
  echo -e "filesystem\t${filesystem}"
  echo -e "mount_options\t${mount_options}"
  echo -e "device_maj_min\t${device_maj_min}"
  echo -e "device_model\t${device_model}"
} >"${result_dir}/environment.tsv"

echo -e "direction\tqd\ttrial\tposition\tmode\tlog" >"${manifest}"

run_one() {
  local direction="$1"
  local qd="$2"
  local trial="$3"
  local position="$4"
  local mode="$5"
  local filter
  local log

  filter="fixed_file_io/mode=${mode}/direction=${direction}/storage=aligned_heap/block=4096/qd=${qd}/ops=16384"
  log="direction-${direction}_qd-${qd}_trial-${trial}_position-${position}_mode-${mode}.log"
  echo -e "${direction}\t${qd}\t${trial}\t${position}\t${mode}\t${log}" >>"${manifest}"
  echo "running ${filter} (trial ${trial}, position ${position})"
  NORN_FIXEDBUF_BENCH_DIR="${bench_dir}" \
    taskset -c "${cpu}" cargo bench -p benches --bench fixed_buffers -- "${filter}" \
    2>&1 | tee "${result_dir}/${log}"
}

for direction in read write; do
  for qd in 1 32 128; do
    for trial in $(seq 1 "${trials}"); do
      if (( trial % 2 == 1 )); then
        first="ordinary"
        second="fixed"
      else
        first="fixed"
        second="ordinary"
      fi
      run_one "${direction}" "${qd}" "${trial}" 1 "${first}"
      run_one "${direction}" "${qd}" "${trial}" 2 "${second}"
    done
  done
done

ps -eLo psr,pid,tid,comm >"${result_dir}/processes-after.tsv"
"$(dirname "$0")/summarize-fixed-buffer-pairs.sh" "${result_dir}"
echo "raw paired results written to ${result_dir}"
