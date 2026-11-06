#!/usr/bin/env bash
# In-place cost of each counter backend on norn's probes.
#
# Runs every `<probe>.<backend>` binary in BIN_DIR REPS times, interleaved,
# pinned to one CPU under `perf stat`, and prints median user instructions,
# median and minimum cycles per unit of work, with deltas against the `none`
# backend (`BASE` overrides it). Interference from other processes only adds
# cycles, so on a shared host the minimum is the steadier cycle estimate.
#
# Usage: run-insitu.sh <bin-dir> <probe> <units> <cpu> <reps> <args...>
set -euo pipefail
dir=$1 probe=$2 units=$3 cpu=$4 reps=$5; shift 5
backends=(${BACKENDS:-master none sw cell rmw facade-cached facade otel-bound otel otel-attr})
tmp=$(mktemp -d)
for rep in $(seq "$reps"); do
  for b in "${backends[@]}"; do
    perf stat -x, -e instructions:u,cycles:u taskset -c "$cpu" "$dir/$probe.$b" "$@" 2>&1 >/dev/null \
      | awk -F, -v b="$b" '$3 ~ /instructions/ {i=$1} $3 ~ /cycles/ {c=$1} END {print b, i, c}' >> "$tmp/raw"
  done
done
awk -v units="$units" -v base="${BASE:-none}" '
  {inst[$1] = inst[$1] " " $2; cyc[$1] = cyc[$1] " " $3; if (!($1 in seen)) {order[++n] = $1; seen[$1] = 1}}
  function median(s,   a, k, i, j, t) {
    k = split(s, a, " ");
    for (i = 1; i <= k; i++) for (j = i + 1; j <= k; j++) if (a[j] + 0 < a[i] + 0) {t = a[i]; a[i] = a[j]; a[j] = t}
    return (k % 2) ? a[(k + 1) / 2] : (a[k / 2] + a[k / 2 + 1]) / 2
  }
  function minimum(s,   a, k, i, m) {
    k = split(s, a, " "); m = a[1] + 0;
    for (i = 2; i <= k; i++) if (a[i] + 0 < m) m = a[i] + 0;
    return m
  }
  END {
    bi = median(inst[base]) / units; bc = median(cyc[base]) / units; bm = minimum(cyc[base]) / units;
    printf "%-14s %10s %10s %10s %9s %9s %9s\n", "backend", "inst/unit", "cyc/unit", "mincyc", "d_inst", "d_cyc", "d_mincyc";
    for (x = 1; x <= n; x++) {
      b = order[x]; i = median(inst[b]) / units; c = median(cyc[b]) / units; m = minimum(cyc[b]) / units;
      printf "%-14s %10.1f %10.1f %10.1f %+8.1f%% %+8.1f%% %+8.1f%%\n", b, i, c, m, (i / bi - 1) * 100, (c / bc - 1) * 100, (m / bm - 1) * 100
    }
  }' "$tmp/raw"
cp "$tmp/raw" "${RAW_OUT:-/dev/null}" 2>/dev/null || true
rm -rf "$tmp"
