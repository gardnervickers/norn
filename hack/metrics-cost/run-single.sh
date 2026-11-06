#!/usr/bin/env bash
# Per-iteration cost of each single-thread variant, setup excluded.
#
# Each variant runs at two iteration counts, REPS times each, pinned to one
# CPU under `perf stat`. The per-iteration cost is the difference of the
# median counts divided by the difference in iterations. `none` is the loop
# overhead to subtract.
#
# Usage: run-single.sh [cpu] [reps] [variants...]
set -euo pipefail
cpu=${1:-12}; shift || true
reps=${1:-3}; shift || true
bin=$(dirname "$0")/target/release/metrics-cost
variants=("$@")
if [ ${#variants[@]} -eq 0 ]; then
  variants=(none cell sw-atomic rmw-atomic tls-cell facade-noop facade-cached-noop
            facade-prom facade-prom-label facade-cached-prom otel-noop otel-sdk
            otel-sdk-attr otel-sdk-bound rdtsc instant-now)
fi
median() { sort -n | awk '{a[NR]=$1} END {print (NR%2) ? a[(NR+1)/2] : (a[NR/2]+a[NR/2+1])/2}'; }
count() { # variant n event
  for _ in $(seq "$reps"); do
    perf stat -x, -e instructions:u,cycles:u taskset -c "$cpu" "$bin" "$1" "$2" 2>&1 >/dev/null \
      | awk -F, -v ev="$3" '$3 ~ ev {print $1}'
  done | median
}
printf '%-20s %12s %12s\n' variant instr/iter cycles/iter
for v in "${variants[@]}"; do
  case $v in
    facade-prom*|otel-sdk|otel-sdk-attr|instant-now|facade-noop|facade-cached-prom|otel-sdk-bound|rdtsc) n1=10000000; n2=60000000 ;;
    *) n1=100000000; n2=600000000 ;;
  esac
  i1=$(count "$v" $n1 instructions); i2=$(count "$v" $n2 instructions)
  c1=$(count "$v" $n1 cycles); c2=$(count "$v" $n2 cycles)
  awk -v v="$v" -v i1="$i1" -v i2="$i2" -v c1="$c1" -v c2="$c2" -v d=$((n2-n1)) \
    'BEGIN {printf "%-20s %12.2f %12.2f\n", v, (i2-i1)/d, (c2-c1)/d}'
done
