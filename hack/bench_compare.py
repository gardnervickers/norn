#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from bench_run import load_bench_names, normalize_bench_selection, run_benchmarks  # noqa: E402


def repo_default() -> Path:
    return SCRIPT_DIR.parent


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare norn benchmark results between two refs.")
    parser.add_argument("--repo-root", type=Path, default=repo_default())
    parser.add_argument("--base-ref", required=True, help="git ref used for the baseline run")
    parser.add_argument("--head-ref", default="HEAD", help="git ref used for the comparison run")
    parser.add_argument("--cargo-command", default="cargo")
    parser.add_argument("--bench", dest="benches", action="append", default=[])
    parser.add_argument("--filter", default="")
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--cpu", default="")
    parser.add_argument(
        "--collect-flamegraphs",
        action="store_true",
        help="collect pprof/flamegraph artifacts for base and head runs",
    )
    return parser.parse_args()


def git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=repo_root,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise SystemExit(
            f"git {' '.join(args)} failed\n\nstdout:\n{completed.stdout}\n\nstderr:\n{completed.stderr}"
        )
    return completed.stdout.strip()


def build_selection(repo_root: Path, base_root: Path, requested: list[str]) -> list[str]:
    head_available = load_bench_names(repo_root / "benches" / "Cargo.toml")
    base_available = load_bench_names(base_root / "benches" / "Cargo.toml")

    if requested:
        normalized = normalize_bench_selection(requested, head_available, repo_root)
        missing_from_base = [name for name in normalized if name not in base_available]
        if missing_from_base:
            missing_fmt = ", ".join(missing_from_base)
            raise SystemExit(f"requested benches missing from base ref: {missing_fmt}")
        return normalized

    base_available_set = set(base_available)
    available_both = [name for name in head_available if name in base_available_set]
    return normalize_bench_selection([], available_both, repo_root)


def resolve_commit(repo_root: Path, ref: str) -> str:
    return git(repo_root, "rev-parse", ref)


def resolve_remote_or_local_commit(repo_root: Path, ref: str) -> str:
    candidates = []
    if ref != "HEAD":
        candidates.append(f"refs/remotes/origin/{ref}")
    candidates.append(ref)

    for candidate in candidates:
        completed = subprocess.run(
            ["git", "rev-parse", "--verify", candidate],
            cwd=repo_root,
            text=True,
            capture_output=True,
            check=False,
        )
        if completed.returncode == 0:
            return completed.stdout.strip()

    raise SystemExit(f"could not resolve git ref: {ref}")


def ensure_available_commit(repo_root: Path, ref: str) -> str:
    try:
        return resolve_remote_or_local_commit(repo_root, ref)
    except SystemExit:
        git(repo_root, "fetch", "--no-tags", "--prune", "origin", ref)
        return resolve_remote_or_local_commit(repo_root, ref)


def create_base_worktree(repo_root: Path, base_commit: str, temp_dir: Path) -> Path:
    base_root = temp_dir / "base"
    subprocess.run(
        ["git", "worktree", "add", "--detach", str(base_root), base_commit],
        cwd=repo_root,
        text=True,
        check=True,
    )
    return base_root


def relative_change(base_ns: float, head_ns: float) -> float:
    if base_ns == 0:
        return 0.0
    return ((head_ns - base_ns) / base_ns) * 100.0


def format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def format_duration_ns(ns_per_iter: float) -> str:
    magnitude = abs(ns_per_iter)
    if magnitude < 1_000:
        return f"{format_number(ns_per_iter)} ns"
    if magnitude < 1_000_000:
        return f"{format_number(ns_per_iter / 1_000)} us"
    if magnitude < 1_000_000_000:
        return f"{format_number(ns_per_iter / 1_000_000)} ms"
    return f"{format_number(ns_per_iter / 1_000_000_000)} s"


def summarize_change(base_ns: float, head_ns: float) -> tuple[str, str]:
    delta = head_ns - base_ns
    pct = relative_change(base_ns, head_ns)
    if delta > 0:
        delta_text = f"+{format_duration_ns(delta)}"
    elif delta < 0:
        delta_text = f"-{format_duration_ns(abs(delta))}"
    else:
        delta_text = "0 ns"
    if abs(pct) < 0.05:
        return delta_text, "flat"
    direction = "slower" if pct > 0 else "faster"
    return delta_text, f"{pct:+.2f}% {direction}"


def flatten_measurements(payload: dict[str, object]) -> dict[str, dict[str, object]]:
    flattened: dict[str, dict[str, object]] = {}
    for result in payload["results"]:
        bench = result["bench"]
        for measurement in result["measurements"]:
            flattened[str(measurement["name"])] = {
                "bench": bench,
                **measurement,
            }
    return flattened


def render_markdown(
    *,
    base_ref: str,
    base_commit: str,
    head_ref: str,
    head_commit: str,
    filter_text: str,
    base_payload: dict[str, object],
    head_payload: dict[str, object],
) -> str:
    base_measurements = flatten_measurements(base_payload)
    head_measurements = flatten_measurements(head_payload)
    names = sorted(set(base_measurements) | set(head_measurements))

    lines = [
        "# Benchmark Diff",
        "",
        f"- Baseline: `{base_ref}` at `{base_commit[:12]}`",
        f"- Head: `{head_ref}` at `{head_commit[:12]}`",
        f"- Filter: `{filter_text or '(none)'}`",
        "",
        "| Benchmark | Base | Head | Delta | Relative |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]

    for name in names:
        base = base_measurements.get(name)
        head = head_measurements.get(name)
        if base is None:
            lines.append(
                f"| {name} | missing | {head['display']} | n/a | added in head |"
            )
            continue
        if head is None:
            lines.append(
                f"| {name} | {base['display']} | missing | n/a | missing in head |"
            )
            continue

        delta_text, rel_text = summarize_change(
            float(base["ns_per_iter"]),
            float(head["ns_per_iter"]),
        )
        lines.append(
            f"| {name} | {base['display']} | {head['display']} | {delta_text} | {rel_text} |"
        )

    return "\n".join(lines)


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    output_dir = args.output_dir.resolve()
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    base_commit = ensure_available_commit(repo_root, args.base_ref)
    head_commit = ensure_available_commit(repo_root, args.head_ref)

    with tempfile.TemporaryDirectory(prefix="norn-bench-compare-") as temp_name:
        temp_dir = Path(temp_name)
        base_root = create_base_worktree(repo_root, base_commit, temp_dir)
        try:
            selected_benches = build_selection(repo_root, base_root, args.benches)
            if not selected_benches:
                raise SystemExit("no overlapping benchmark binaries were found for comparison")

            base_profiles = output_dir / "profiles" / "base" if args.collect_flamegraphs else None
            head_profiles = output_dir / "profiles" / "head" if args.collect_flamegraphs else None

            base_payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "repo_root": str(base_root),
                "selected_benches": selected_benches,
                "results": run_benchmarks(
                    repo_root=base_root,
                    benches=selected_benches,
                    filter_text=args.filter,
                    cargo_command=args.cargo_command,
                    log_dir=output_dir / "logs" / "base",
                    profile_output=base_profiles,
                    cpu=args.cpu,
                ),
            }

            head_payload = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "repo_root": str(repo_root),
                "selected_benches": selected_benches,
                "results": run_benchmarks(
                    repo_root=repo_root,
                    benches=selected_benches,
                    filter_text=args.filter,
                    cargo_command=args.cargo_command,
                    log_dir=output_dir / "logs" / "head",
                    profile_output=head_profiles,
                    cpu=args.cpu,
                ),
            }
        finally:
            subprocess.run(
                ["git", "worktree", "remove", "--force", str(base_root)],
                cwd=repo_root,
                text=True,
                check=False,
            )

    compare_payload = {
        "base_ref": args.base_ref,
        "base_commit": base_commit,
        "head_ref": args.head_ref,
        "head_commit": head_commit,
        "filter": args.filter,
        "selected_benches": base_payload["selected_benches"],
        "base": base_payload,
        "head": head_payload,
    }

    write_json(output_dir / "base.json", base_payload)
    write_json(output_dir / "head.json", head_payload)
    write_json(output_dir / "compare.json", compare_payload)

    report = render_markdown(
        base_ref=args.base_ref,
        base_commit=base_commit,
        head_ref=args.head_ref,
        head_commit=head_commit,
        filter_text=args.filter,
        base_payload=base_payload,
        head_payload=head_payload,
    )
    (output_dir / "report.md").write_text(report + "\n")
    print(report)

    return 0


if __name__ == "__main__":
    sys.exit(main())
