#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import shlex
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable


LINUX_ONLY_BENCHES = {
    "hyper",
    "io_uring_accept_recv",
    "noop_submit",
    "uring_realworld",
}

UNIT_TO_NS = {
    "ns": 1.0,
    "us": 1_000.0,
    "µs": 1_000.0,
    "ms": 1_000_000.0,
    "s": 1_000_000_000.0,
}

BENCH_RESULT_RE = re.compile(
    r"^test\s+(?P<name>.+?)\s+\.\.\.\s+bench:\s+"
    r"(?P<value>[0-9][0-9,]*(?:\.[0-9]+)?)\s+"
    r"(?P<unit>ns|us|µs|ms|s)/iter\s+\(\+/-\s+"
    r"(?P<spread>[0-9][0-9,]*(?:\.[0-9]+)?)\)$"
)
BENCH_BLOCK_RE = re.compile(r"(?ms)^\[\[bench\]\]\s*(?P<body>.*?)(?=^\[|\Z)")
BENCH_NAME_RE = re.compile(r'(?m)^name\s*=\s*"(?P<name>[^"]+)"\s*$')


@dataclass(frozen=True)
class BenchMeasurement:
    name: str
    value: float
    unit: str
    spread: float

    @property
    def ns_per_iter(self) -> float:
        return self.value * UNIT_TO_NS[self.unit]

    @property
    def spread_ns(self) -> float:
        return self.spread * UNIT_TO_NS[self.unit]

    def to_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "value": self.value,
            "unit": self.unit,
            "ns_per_iter": self.ns_per_iter,
            "spread": self.spread,
            "spread_ns": self.spread_ns,
            "display": f"{format_number(self.value)} {self.unit}/iter",
            "spread_display": f"+/- {format_number(self.spread)} {self.unit}",
        }


def repo_default() -> Path:
    return Path(__file__).resolve().parents[1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run norn benchmarks and capture structured output.")
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=repo_default(),
        help="repository checkout to benchmark",
    )
    parser.add_argument(
        "--bench-config",
        type=Path,
        help="path to benches/Cargo.toml; defaults to <repo-root>/benches/Cargo.toml",
    )
    parser.add_argument(
        "--bench",
        dest="benches",
        action="append",
        default=[],
        help="bench binary to run; can be repeated or comma-separated",
    )
    parser.add_argument(
        "--filter",
        default="",
        help="optional benchmark filter passed after --",
    )
    parser.add_argument(
        "--cargo-command",
        default="cargo",
        help="cargo command prefix, for example 'nix develop -c cargo'",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        required=True,
        help="path for structured benchmark output",
    )
    parser.add_argument(
        "--output-markdown",
        type=Path,
        help="optional Markdown summary path",
    )
    parser.add_argument(
        "--log-dir",
        type=Path,
        help="optional directory for raw stdout/stderr logs",
    )
    parser.add_argument(
        "--profile-output",
        type=Path,
        help="optional directory for pprof/flamegraph artifacts",
    )
    parser.add_argument(
        "--cpu",
        default="",
        help="optional CPU to pin benchmark processes to with taskset",
    )
    return parser.parse_args()


def load_bench_names(bench_config: Path) -> list[str]:
    names = []
    content = bench_config.read_text()
    for block in BENCH_BLOCK_RE.finditer(content):
        match = BENCH_NAME_RE.search(block.group("body"))
        if match:
            names.append(match.group("name"))
    if not names:
        raise SystemExit(f"no [[bench]] entries found in {bench_config}")
    return names


def normalize_bench_selection(
    requested: Iterable[str],
    available: list[str],
    repo_root: Path,
) -> list[str]:
    seen: set[str] = set()
    normalized: list[str] = []
    for item in requested:
        for value in item.split(","):
            name = value.strip()
            if not name or name in seen:
                continue
            seen.add(name)
            normalized.append(name)

    if normalized:
        missing = [name for name in normalized if name not in available]
        if missing:
            missing_fmt = ", ".join(missing)
            raise SystemExit(f"unknown bench binary for {repo_root}: {missing_fmt}")
        return normalized

    if platform.system().lower() == "linux":
        return available

    return [name for name in available if name not in LINUX_ONLY_BENCHES]


def parse_measurements(stdout: str) -> list[BenchMeasurement]:
    measurements: list[BenchMeasurement] = []
    for line in stdout.splitlines():
        match = BENCH_RESULT_RE.match(line.strip())
        if not match:
            continue
        value = float(match.group("value").replace(",", ""))
        spread = float(match.group("spread").replace(",", ""))
        measurements.append(
            BenchMeasurement(
                name=match.group("name"),
                value=value,
                unit=match.group("unit"),
                spread=spread,
            )
        )
    return measurements


def run_command(
    command: list[str],
    *,
    cwd: Path,
    env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    process = subprocess.Popen(
        command,
        cwd=cwd,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        bufsize=1,
    )
    assert process.stdout is not None

    stdout_parts: list[str] = []
    for line in process.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()
        stdout_parts.append(line)

    returncode = process.wait()
    return subprocess.CompletedProcess(
        command,
        returncode,
        "".join(stdout_parts),
        "",
    )


def bench_command(cargo_command: str, bench_name: str, filter_text: str, cpu: str) -> list[str]:
    command = shlex.split(cargo_command)
    if not command:
        raise SystemExit("empty --cargo-command is not valid")

    if cpu:
        taskset = shutil.which("taskset")
        if taskset:
            command = [taskset, "-c", cpu, *command]

    command.extend(["bench", "-p", "benches", "--bench", bench_name])
    if filter_text:
        command.extend(["--", filter_text])
    return command


def run_benchmarks(
    *,
    repo_root: Path,
    benches: list[str],
    filter_text: str,
    cargo_command: str,
    log_dir: Path | None,
    profile_output: Path | None,
    cpu: str,
) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    env = os.environ.copy()
    if profile_output is not None:
        env["NORN_BENCH_PPROF"] = str(profile_output.resolve())

    total = len(benches)
    for index, bench_name in enumerate(benches, start=1):
        print(
            f"[bench] ({index}/{total}) {repo_root.name}: running `{bench_name}`"
            + (f" with filter `{filter_text}`" if filter_text else ""),
            flush=True,
        )
        command = bench_command(cargo_command, bench_name, filter_text, cpu)
        completed = run_command(command, cwd=repo_root, env=env)

        measurements = parse_measurements(completed.stdout)
        entry = {
            "bench": bench_name,
            "command": command,
            "returncode": completed.returncode,
            "measurements": [measurement.to_json() for measurement in measurements],
            "stdout": completed.stdout,
            "stderr": completed.stderr,
        }
        results.append(entry)

        if log_dir is not None:
            log_dir.mkdir(parents=True, exist_ok=True)
            (log_dir / f"{bench_name}.stdout.log").write_text(completed.stdout)
            (log_dir / f"{bench_name}.stderr.log").write_text(completed.stderr)

        if completed.returncode != 0:
            raise SystemExit(render_failed_command(command, completed))

        if not measurements:
            raise SystemExit(
                f"no benchmark measurements were parsed for {bench_name}\n"
                f"{render_failed_command(command, completed)}"
            )

        print(
            f"[bench] ({index}/{total}) {repo_root.name}: completed `{bench_name}`"
            f" with {len(measurements)} measurements",
            flush=True,
        )

    return results


def render_failed_command(command: list[str], completed: subprocess.CompletedProcess[str]) -> str:
    stdout = completed.stdout.strip()
    stderr = completed.stderr.strip()
    parts = [f"command failed: {' '.join(shlex.quote(part) for part in command)}"]
    if stdout:
        parts.append(f"stdout:\n{stdout}")
    if stderr:
        parts.append(f"stderr:\n{stderr}")
    return "\n\n".join(parts)


def render_markdown(
    *,
    repo_root: Path,
    results: list[dict[str, object]],
    filter_text: str,
) -> str:
    lines = [
        "# Benchmark Summary",
        "",
        f"- Repository: `{repo_root}`",
        f"- Filter: `{filter_text or '(none)'}`",
        "",
    ]

    for result in results:
        bench_name = str(result["bench"])
        measurements = result["measurements"]
        lines.append(f"## {bench_name}")
        lines.append("")
        lines.append("| Benchmark | Time | Spread |")
        lines.append("| --- | ---: | ---: |")
        for measurement in measurements:
            lines.append(
                "| {name} | {display} | {spread_display} |".format(
                    name=measurement["name"],
                    display=measurement["display"],
                    spread_display=measurement["spread_display"],
                )
            )
        lines.append("")

    return "\n".join(lines)


def format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return f"{value:.2f}".rstrip("0").rstrip(".")


def main() -> int:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    bench_config = (args.bench_config or (repo_root / "benches" / "Cargo.toml")).resolve()
    available = load_bench_names(bench_config)
    selected = normalize_bench_selection(args.benches, available, repo_root)
    if not selected:
        raise SystemExit(
            "no benchmarks selected for this platform; pass --bench explicitly to override"
        )

    results = run_benchmarks(
        repo_root=repo_root,
        benches=selected,
        filter_text=args.filter,
        cargo_command=args.cargo_command,
        log_dir=args.log_dir.resolve() if args.log_dir else None,
        profile_output=args.profile_output.resolve() if args.profile_output else None,
        cpu=args.cpu,
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "repo_root": str(repo_root),
        "hostname": platform.node(),
        "platform": platform.platform(),
        "cargo_command": args.cargo_command,
        "filter": args.filter,
        "selected_benches": selected,
        "results": results,
    }

    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")

    if args.output_markdown:
        args.output_markdown.parent.mkdir(parents=True, exist_ok=True)
        args.output_markdown.write_text(
            render_markdown(repo_root=repo_root, results=results, filter_text=args.filter) + "\n"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
