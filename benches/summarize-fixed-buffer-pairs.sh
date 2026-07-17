#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 RESULT_DIR" >&2
  exit 2
fi

result_dir="$(realpath -e "$1")"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

if [[ ! -f "${result_dir}/order.tsv" ]]; then
  echo "missing result manifest: ${result_dir}/order.tsv" >&2
  exit 1
fi

gawk \
  -v result_dir="${result_dir}" \
  -v pairs_path="${result_dir}/pairs.tsv" \
  -v summary_path="${result_dir}/summary.tsv" \
  -f "${script_dir}/summarize-fixed-buffer-pairs.awk" \
  "${result_dir}/order.tsv"

echo "paired rows: ${result_dir}/pairs.tsv"
echo "summary: ${result_dir}/summary.tsv"
