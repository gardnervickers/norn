#!/usr/bin/env python3
"""Run one isolated Norn mixed workload trial and save reproducible artifacts.

The load generator owns workload accounting.  This wrapper owns process
lifecycle, provenance, and the server/client measurement boundary.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import socket
import subprocess
import time
import threading


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def cpu_set(value, name):
    if not value:
        return None
    try:
        cpus = sorted({int(item) for item in value.split(",")})
    except ValueError as error:
        raise SystemExit(f"{name} must be a comma-separated CPU list: {error}")
    if not cpus or any(cpu < 0 for cpu in cpus):
        raise SystemExit(f"{name} must contain non-negative CPU numbers")
    return cpus


def topology(cpus):
    result = {}
    for cpu in cpus:
        base = Path(f"/sys/devices/system/cpu/cpu{cpu}/topology")
        try:
            result[str(cpu)] = {
                "package": (base / "physical_package_id").read_text().strip(),
                "core": (base / "core_id").read_text().strip(),
            }
        except OSError:
            result[str(cpu)] = None
    return result


def check_disjoint(server, client):
    if server and client and set(server) & set(client):
        raise SystemExit("--server-cpus and --client-cpus must be disjoint")
    if not server or not client:
        return
    topo = topology(sorted(set(server + client)))
    server_cores = {(topo[str(c)]["package"], topo[str(c)]["core"]) for c in server if topo[str(c)]}
    client_cores = {(topo[str(c)]["package"], topo[str(c)]["core"]) for c in client if topo[str(c)]}
    if server_cores & client_cores:
        raise SystemExit("server and client CPU lists share a physical core (SMT sibling)")


def command_affinity(cpus):
    if not cpus or not hasattr(os, "sched_setaffinity"):
        return None
    return lambda: os.sched_setaffinity(0, cpus)


def format_address(host, port):
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def terminate(proc, log):
    if proc.poll() is not None:
        return False
    try:
        proc.send_signal(signal.SIGTERM)
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        log.write_text(log.read_text() + "\nserver did not exit after SIGTERM; sending SIGKILL\n")
        proc.kill()
        proc.wait()
        return True
    return False


def manifest_timeout(manifest):
    phases = ("warmup_ms", "steady_ms", "overload_ms", "recovery_ms", "drain_ms")
    return 30 + sum(int(manifest.get(key, 0)) for key in phases) / 1000


def process_sample(pid):
    try:
        fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
        status = Path(f"/proc/{pid}/status").read_text().splitlines()
        rss = next((int(line.split()[1]) for line in status if line.startswith("VmRSS:")), 0)
        return {"cpu_seconds": (int(fields[11]) + int(fields[12])) / os.sysconf("SC_CLK_TCK"),
                "rss_kib": rss}
    except (OSError, ValueError, IndexError):
        return None


def sample_processes(server, client, start, stopped, samples):
    while not stopped.is_set():
        samples.append({"elapsed_s": time.monotonic() - start,
                        "server": process_sample(server.pid), "client": process_sample(client.pid)})
        stopped.wait(0.1)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--server", type=Path, default=Path("target/release/norn-kv-server"))
    parser.add_argument("--client", type=Path, default=Path("target/release/norn-mixed-loadgen"))
    parser.add_argument("--output-dir", type=Path, default=Path("benches/logs/mixed-workload"))
    parser.add_argument("--server-cpus", help="comma-separated CPUs for server workers")
    parser.add_argument("--client-cpus", help="comma-separated CPUs for load generator")
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--address", default="127.0.0.1:0")
    parser.add_argument("--runs", type=int, default=1)
    parser.add_argument("--require-performance", action="store_true", help="reject source-limited or otherwise ineligible measurements")
    parser.add_argument("--smoke", action="store_true", help="label this as a smoke run")
    args = parser.parse_args()
    if args.runs < 1 or args.workers < 1:
        parser.error("--runs and --workers must be positive")
    server_cpus = cpu_set(args.server_cpus, "--server-cpus")
    client_cpus = cpu_set(args.client_cpus, "--client-cpus")
    if server_cpus and len(server_cpus) != args.workers:
        raise SystemExit("--server-cpus must contain exactly one CPU per worker")
    check_disjoint(server_cpus, client_cpus)
    manifest = json.loads(args.manifest.read_text())
    if not isinstance(manifest, dict):
        raise SystemExit("manifest must contain a JSON object")
    for key in ("seed", "connections", "pipeline", "rate"):
        if key not in manifest:
            raise SystemExit(f"manifest is missing required field: {key}")
    for binary in (args.server, args.client):
        if not binary.is_file() or not os.access(binary, os.X_OK):
            raise SystemExit(f"executable not found: {binary}")
    if manifest.get("server_workers", 1) != args.workers:
        raise SystemExit("--workers does not match manifest server_workers")
    args.server = args.server.resolve()
    args.client = args.client.resolve()
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    root = args.output_dir / f"{stamp}-{os.getpid()}"
    root.mkdir(parents=True, exist_ok=False)
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    copied_manifest = (root / "manifest.json").resolve()
    host = {
        "time_utc": stamp,
        "hostname": platform.node(),
        "platform": platform.platform(),
        "kernel": platform.release(),
        "python": platform.python_version(),
        "allowed_cpus": sorted(os.sched_getaffinity(0)) if hasattr(os, "sched_getaffinity") else None,
        "server_cpus": server_cpus,
        "client_cpus": client_cpus,
        "topology": topology(sorted(set((server_cpus or []) + (client_cpus or [])))),
        "governor": {
            str(cpu): (Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").read_text().strip()
                       if Path(f"/sys/devices/system/cpu/cpu{cpu}/cpufreq/scaling_governor").is_file() else None)
            for cpu in sorted(set((server_cpus or []) + (client_cpus or [])))
        },
    }
    (root / "host.json").write_text(json.dumps(host, indent=2, sort_keys=True) + "\n")
    git_head = subprocess.run(["git", "rev-parse", "HEAD"], capture_output=True, text=True, check=False)
    git_diff = subprocess.run(["git", "diff", "--binary", "HEAD"], capture_output=True, check=False)
    toolchain = subprocess.run(["rustc", "-Vv"], capture_output=True, text=True, check=False)
    provenance = {"server": str(args.server), "client": str(args.client),
                  "server_sha256": sha256(args.server), "client_sha256": sha256(args.client),
                  "manifest_sha256": sha256(root / "manifest.json"), "smoke": args.smoke,
                  "workers": args.workers, "address": args.address,
                  "git_revision": git_head.stdout.strip() or None,
                  "git_dirty": bool(git_diff.stdout),
                  "git_diff_sha256": hashlib.sha256(git_diff.stdout).hexdigest(),
                  "rustc": toolchain.stdout.strip() or None}
    (root / "provenance.json").write_text(json.dumps(provenance, indent=2, sort_keys=True) + "\n")
    for run in range(1, args.runs + 1):
        run_dir = root / f"run-{run}"
        run_dir.mkdir()
        server_log = run_dir / "server.log"
        client_log = run_dir / "client.stderr.log"
        result_path = run_dir / "result.json"
        address = args.address
        if address.endswith(":0"):
            # Select a concrete port so the readiness probe is independent of
            # stdout buffering and the client receives the same address.
            bind_host = address.rsplit(":", 1)[0].strip("[]")
            with socket.socket(socket.AF_INET6 if ":" in bind_host else socket.AF_INET) as probe:
                probe.bind((bind_host, 0))
                address = format_address(bind_host, probe.getsockname()[1])
        server_args = [str(args.server), "--listen", address, "--workers", str(args.workers)]
        if server_cpus:
            server_args += ["--worker-cpus", ",".join(map(str, server_cpus[:args.workers]))]
        started = time.monotonic()
        with server_log.open("w") as server_stream:
            server = subprocess.Popen(server_args, stdout=server_stream, stderr=subprocess.STDOUT,
                                      preexec_fn=command_affinity(server_cpus))
        try:
            deadline = time.monotonic() + 10
            advertised = None
            while time.monotonic() < deadline:
                text = server_log.read_text()
                for line in text.splitlines():
                    if line.startswith("listening on "):
                        advertised = line.removeprefix("listening on ").strip()
                        break
                if advertised:
                    break
                if server.poll() is not None:
                    raise RuntimeError(f"server exited with status {server.returncode}")
                # stdout can be block buffered when redirected to a file. The
                # concrete address is safe to probe even before the log line
                # is flushed.
                if address.rsplit(":", 1)[-1] != "0":
                    try:
                        probe_host, probe_port = address.rsplit(":", 1)
                        with socket.create_connection((probe_host.strip("[]"), int(probe_port)), timeout=0.05):
                            advertised = address
                            break
                    except (OSError, ValueError):
                        pass
                time.sleep(0.01)
            if not advertised:
                raise RuntimeError("server did not announce readiness within 10 seconds")
            # A TCP connect probe verifies that the announced listener accepts connections.
            host_name, port = advertised.rsplit(":", 1)
            with socket.create_connection((host_name.strip("[]"), int(port)), timeout=2):
                pass
            client_args = [str(args.client), "--manifest", str(copied_manifest), "--address", advertised,
                           "--output", str(result_path)]
            with client_log.open("w") as stream:
                client = subprocess.Popen(client_args, stdout=subprocess.PIPE, stderr=stream,
                                          text=True, preexec_fn=command_affinity(client_cpus))
                resource_samples = []
                stopped = threading.Event()
                sampler = threading.Thread(target=sample_processes, args=(server, client, started, stopped, resource_samples), daemon=True)
                sampler.start()
                try:
                    client_stdout, _ = client.communicate(timeout=manifest_timeout(manifest))
                except subprocess.TimeoutExpired as error:
                    terminate(client, client_log)
                    (run_dir / "timeout.json").write_text(json.dumps({"timeout_seconds": error.timeout}) + "\n")
                    raise RuntimeError("load generator exceeded manifest phase timeout") from error
                except BaseException:
                    terminate(client, client_log)
                    raise
                finally:
                    stopped.set()
                    sampler.join(timeout=1)
                    (run_dir / "resources.json").write_text(json.dumps(resource_samples) + "\n")
            (run_dir / "client.stdout.log").write_text(client_stdout or "")
            if client.returncode:
                (run_dir / "invalid.json").write_text(json.dumps({"client_returncode": client.returncode}) + "\n")
                raise RuntimeError(f"load generator exited with status {client.returncode}")
            if not result_path.is_file():
                raise RuntimeError("load generator did not write its JSON result")
            try:
                result = json.loads(result_path.read_text())
            except json.JSONDecodeError as error:
                (run_dir / "invalid.json").write_text(json.dumps({"error": str(error)}) + "\n")
                raise RuntimeError(f"load generator wrote invalid JSON: {error}") from error
            if result.get("valid") is not True:
                (run_dir / "invalid.json").write_text(json.dumps({"error": "client reported invalid trial"}) + "\n")
                raise RuntimeError("client reported invalid trial")
            if args.require_performance and result.get("performance_eligible") is not True:
                (run_dir / "invalid.json").write_text(json.dumps({"error": "performance eligibility failed"}) + "\n")
                raise RuntimeError("performance eligibility failed; inspect generator lag and rejected work")
            if server.poll() is not None:
                raise RuntimeError(f"server exited during the trial: {server.returncode}")
            metadata = {"server_started_monotonic": started, "client_returncode": client.returncode,
                        "server_address": advertised}
            (run_dir / "runner.json").write_text(json.dumps(metadata, indent=2) + "\n")
        finally:
            if terminate(server, server_log):
                (run_dir / "invalid.json").write_text(json.dumps({"error": "server required SIGKILL"}) + "\n")
                raise RuntimeError("server required SIGKILL")
    print(root)


if __name__ == "__main__":
    def interrupted(_signum, _frame):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupted)
    try:
        main()
    except KeyboardInterrupt:
        raise SystemExit(130)
