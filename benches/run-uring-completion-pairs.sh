#!/usr/bin/env bash
set -euo pipefail

baseline_dir=${NORN_COMPLETION_BASELINE_DIR:?set NORN_COMPLETION_BASELINE_DIR}
candidate_dir=${NORN_COMPLETION_CANDIDATE_DIR:?set NORN_COMPLETION_CANDIDATE_DIR}
result_dir=${NORN_COMPLETION_RESULT_DIR:?set NORN_COMPLETION_RESULT_DIR}
bench_cpu=${NORN_COMPLETION_CPU:-15}
pairs=${NORN_COMPLETION_PAIRS:-7}

if [[ ! -d "$baseline_dir/.git" && ! -f "$baseline_dir/.git" ]]; then
  echo "baseline is not a git worktree: $baseline_dir" >&2
  exit 2
fi
if [[ ! -d "$candidate_dir/.git" && ! -f "$candidate_dir/.git" ]]; then
  echo "candidate is not a git worktree: $candidate_dir" >&2
  exit 2
fi
if [[ -e "$result_dir" ]]; then
  echo "result directory already exists: $result_dir" >&2
  exit 2
fi
mkdir -p "$result_dir/logs"

baseline_harness=$(git -C "$baseline_dir" hash-object benches/uring_completion_backlog.rs)
candidate_harness=$(git -C "$candidate_dir" hash-object benches/uring_completion_backlog.rs)
if [[ "$baseline_harness" != "$candidate_harness" ]]; then
  echo "baseline and candidate benchmark harnesses differ" >&2
  exit 2
fi

{
  printf 'started_utc\t%s\n' "$(date --utc --iso-8601=seconds)"
  printf 'baseline_dir\t%s\n' "$baseline_dir"
  printf 'baseline_head\t%s\n' "$(git -C "$baseline_dir" rev-parse HEAD)"
  printf 'baseline_tree\t%s\n' "$(git -C "$baseline_dir" rev-parse HEAD^{tree})"
  printf 'candidate_dir\t%s\n' "$candidate_dir"
  printf 'candidate_head\t%s\n' "$(git -C "$candidate_dir" rev-parse HEAD)"
  printf 'candidate_tree\t%s\n' "$(git -C "$candidate_dir" rev-parse HEAD^{tree})"
  printf 'harness_blob\t%s\n' "$baseline_harness"
  printf 'cpu\t%s\n' "$bench_cpu"
  printf 'pairs\t%s\n' "$pairs"
  printf 'governor\t%s\n' "$(cat "/sys/devices/system/cpu/cpu${bench_cpu}/cpufreq/scaling_governor")"
  printf 'kernel\t%s\n' "$(uname -srvm)"
  printf 'load_before\t%s\n' "$(cat /proc/loadavg)"
} > "$result_dir/manifest.tsv"
lscpu > "$result_dir/lscpu.txt"
ps -eo pid,comm,psr,%cpu,%mem --sort=-%cpu > "$result_dir/processes-before.txt"

for tree in "$baseline_dir" "$candidate_dir"; do
  (
    cd "$tree"
    nix develop -c cargo bench -p benches --bench uring_completion_backlog --no-run
  )
done

if [[ -n ${NORN_COMPLETION_CASE:-} ]]; then
  cases=("$NORN_COMPLETION_CASE")
else
  cases=(
    'real_multishot/steady/messages=4096'
    'real_multishot/burst/messages=1024'
    'real_multishot/burst/messages=4096'
    'real_multishot/lagged/messages=16384/consume'
  )
fi

run_case() {
  local side=$1
  local tree=$2
  local pair=$3
  local case_name=$4
  local safe_name=${case_name//\//_}
  safe_name=${safe_name//\=/-}
  local log="$result_dir/logs/${safe_name}.pair${pair}.${side}.log"
  (
    cd "$tree"
    timeout 30s nix develop -c taskset -c "$bench_cpu" \
      cargo bench -p benches --bench uring_completion_backlog -- "$case_name"
  ) > "$log" 2>&1
}

for case_name in "${cases[@]}"; do
  for pair in $(seq 1 "$pairs"); do
    if (( pair % 2 == 1 )); then
      run_case baseline "$baseline_dir" "$pair" "$case_name"
      run_case candidate "$candidate_dir" "$pair" "$case_name"
    else
      run_case candidate "$candidate_dir" "$pair" "$case_name"
      run_case baseline "$baseline_dir" "$pair" "$case_name"
    fi
  done
done

{
  printf 'finished_utc\t%s\n' "$(date --utc --iso-8601=seconds)"
  printf 'load_after\t%s\n' "$(cat /proc/loadavg)"
} >> "$result_dir/manifest.tsv"
ps -eo pid,comm,psr,%cpu,%mem --sort=-%cpu > "$result_dir/processes-after.txt"

echo "wrote paired logs to $result_dir"
