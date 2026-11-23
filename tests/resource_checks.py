"""Real Podman quotas, eight CDN containers, 32 in-flight chunks and failures.

Run with sudo/root in WSL after building the project image. Services have
separate quotas; Origin and impairment proxies remain trusted host fixtures.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import time
import uuid

from extended_tests import ROOT, chunk_requests, require
from framework.data import make_file
from tools.public_tests import Lab, events, peak


class ContainerLab(Lab):
    def __init__(self, root, image):
        super().__init__(root)
        self.image = image
        self.containers = []
        self.container_logs = {}

    def start(self, name, args):
        command = [sys.executable, "-u", *args]
        if args[:1] == ["-m"] and args[1] in ("student.client", "student.cdn"):
            container = "cs305-check-" + uuid.uuid4().hex[:12]
            self.containers.append(container)
            config_index = args.index("--config") + 1
            config = json.loads(Path(args[config_index]).read_text())
            if args[1] == "student.client":
                config["output_root"] = "/output"
            wire = self.config(name + "-wire", config)
            inner = list(args)
            inner[config_index] = "/run/project-config.json"
            command = ["podman", "run", "--rm", "--name", container, "--network", "host",
                       "--cpus=1", "--memory=256m", "--memory-swap=256m", "--pids-limit=64",
                       "--ulimit", "nofile=128:128", "--read-only", "--read-only-tmpfs=false",
                       "--tmpfs", "/tmp:rw,size=16m,mode=1777",
                       "--tmpfs", "/output:rw,size=16m,mode=1777",
                       "--user", "10001:10001", "--cap-drop", "ALL",
                       "--security-opt", "no-new-privileges", "--workdir", "/app",
                       "--mount", f"type=bind,src={wire},dst=/run/project-config.json,ro=true",
                       "--mount", f"type=bind,src={ROOT / 'tests/container_worker.py'},dst=/app/resource_worker.py,ro=true",
                       self.image, "python", "-u", "/app/resource_worker.py", *inner]
            self.container_logs[name] = self.root / (name + ".jsonl")
        stdout, stderr = self.root / (name + ".jsonl"), self.root / (name + ".stderr")
        out_stream, err_stream = stdout.open("w"), stderr.open("w")
        self.streams.extend((out_stream, err_stream))
        process = subprocess.Popen(command, stdout=out_stream, stderr=err_stream,
                                   stdin=subprocess.DEVNULL, cwd=ROOT)
        self.processes.append(process)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            ready = [row for row in events(stdout) if row.get("event") == "ready"]
            if ready:
                port = ready[0].get("port")
                return process, stdout, "http://127.0.0.1:" + str(port) if port else None
            if process.poll() is not None:
                raise AssertionError(name + ": " + stderr.read_text()[-2500:])
            time.sleep(0.02)
        raise AssertionError(name + " did not become ready")

    def stop(self):
        for container in self.containers:
            subprocess.run(["podman", "stop", "--time", "2", container],
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        super().stop()


def witness(log):
    rows = [row for row in events(log) if row.get("event") == "resource_witness"]
    require(len(rows) == 1, "missing independent container output/resource witness")
    row = rows[0]
    require(row["uid"] == 10001 and row["memory_limit"] == 256 * 1024**2
            and row["pids_limit"] == 64 and row["nofile_limit"] == [128, 128]
            and row["tmp_bytes"] == row["output_bytes"] == 16 * 1024**2,
            "requested quotas were not enforced")
    quota, period = map(int, row["cpu_max"].split())
    require(quota == period, "CPU quota is not one CPU")
    require(row["peak_child_fds"] <= 128 and row["pids_peak"] <= 64
            and row["memory_peak_bytes"] <= 256 * 1024**2, "container resource quota exceeded")
    return row


def client(lab, name, file_id, sources, **settings):
    config = lab.config(name, {"origin_url": lab.origin_url, "output_root": "/output",
                              "cdns": [{"id": node, "url": url} for node, url in sources],
                              "download_timeout_s": 35, **settings})
    process, log, _ = lab.start(name, ["-m", "student.client", "--config", str(config),
                                      "--download", file_id, "--output", "result.bin"])
    process.wait(timeout=45)
    return log, witness(log)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="localhost/cs305-v2")
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/resource-results.json")
    args = parser.parse_args()
    require(os.geteuid() == 0, "run via sudo/root for cgroup-enforced Podman")
    results = {}
    evidence = args.output.parent / "resource-evidence"
    evidence.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="cs305-resource-") as folder:
        lab = ContainerLab(Path(folder), args.image)
        try:
            for file_id, size, chunk in (("wide", 8 * 1024**2, 65536), ("dense", 256 * 1024, 256),
                                          ("small", 65536, 32768)):
                lab.manifests[file_id] = make_file(lab.root / "data", file_id, size, chunk)
            lab.token = secrets.token_hex(24)
            _, _, lab.origin_url = lab.service("framework.origin", "origin", {
                "host": "127.0.0.1", "port": 0, "data_root": str(lab.root / "data"),
                "chunk_access_token": lab.token, "audit_path": str(lab.root / "origin-audit.jsonl"),
            })
            sources, logs = [], []
            for index in range(8):
                node = "edge" + str(index)
                url, log = lab.proxy("shaped-" + node, lab.cdn(node), {"latency_ms": 15, "rate_mbps": 2})
                sources.append((node, url))
                logs.append(log)
            log, wide = client(lab, "wide-client", "wide", sources, max_inflight_chunks=32,
                                max_inflight_per_cdn=8, probe_interval_s=0.1, _test_seed_bytes=8 * 1024**2)
            require(wide["exit_code"] == 0 and wide["outputs"] == {
                "result.bin": {"size": 8 * 1024**2, "sha256": lab.manifests["wide"]["file_sha256"]}},
                "8 MiB atomic replacement/hash/16 MiB output quota")
            rows = [row for path in logs for row in chunk_requests(path)]
            require(peak(rows) == 32, "full 32-request concurrency was not exercised")
            require(all(peak(chunk_requests(path)) <= 8 for path in logs), "node concurrency cap")
            by_path = {}
            for row in rows:
                by_path.setdefault(row["path"], []).append(row)
            require(len(rows) == len(by_path) == 128 and all(peak(group) == 1 for group in by_path.values()),
                    "duplicate chunks under 32-way concurrency")
            all_measurements = []
            for path in logs:
                measurements = [row for row in events(path) if row.get("event") == "request"
                                and row["path"] in ("/probe", "/status")]
                require(peak(measurements) <= 1, "measurement overlap at one node")
                all_measurements.extend(measurements)
            require(peak(all_measurements) <= 8, "global measurement cap")
            wide["gateway_chunk_peak"] = peak(rows)
            wide["gateway_measurement_peak"] = peak(all_measurements)
            results["eight_nodes_32_inflight"] = wide

            log, dense = client(lab, "dense-client", "dense", [sources[0]],
                                 max_inflight_chunks=8, max_inflight_per_cdn=8)
            require(dense["exit_code"] == 0 and dense["outputs"]["result.bin"]["sha256"]
                    == lab.manifests["dense"]["file_sha256"], "dense chunk file")
            results["dense_chunks_log_budget"] = dense

            bad, _ = lab.proxy("all-corrupt", sources[0][1], {"rules": [{"action": "corrupt"}]})
            log, failed = client(lab, "failed-client", "small", [("bad", bad)], _test_seed_bytes=1024)
            expected_old = {"size": 1024, "sha256": hashlib.sha256(b"Z" * 1024).hexdigest()}
            require(failed["exit_code"] == 1 and failed["outputs"] == {"result.bin": expected_old},
                    "container failure cleanup/old output")
            results["corruption_protection"] = failed

            stalled, _ = lab.proxy("all-stalled", sources[0][1], {
                "rules": [{"action": "stall", "seconds": 5, "all_paths": True}],
            })
            log, deadline = client(lab, "deadline-client", "small", [("stalled", stalled)],
                                    _test_seed_bytes=1024, download_timeout_s=0.4,
                                    connect_timeout_s=5, read_idle_timeout_s=5)
            require(deadline["exit_code"] == 1 and deadline["runtime_s"] < 1.3
                    and deadline["outputs"] == {"result.bin": expected_old}, "container deadline/protection")
            results["deadline_protection"] = deadline
        finally:
            lab.stop()
            for name, log in lab.container_logs.items():
                if any(row.get("event") == "resource_witness" for row in events(log)):
                    row = witness(log)
                    stderr = log.with_suffix(".stderr")
                    total_logs = log.stat().st_size + stderr.stat().st_size
                    require(total_logs <= 256 * 1024, "container logs exceed 256 KiB")
                    row["log_bytes"] = total_logs
                    if name.startswith("cdn-"):
                        results[name] = row
                    else:
                        key = {"wide-client": "eight_nodes_32_inflight",
                               "dense-client": "dense_chunks_log_budget",
                               "failed-client": "corruption_protection",
                               "deadline-client": "deadline_protection"}.get(name)
                        if key in results:
                            results[key].update(row)
                    shutil.copyfile(log, evidence / log.name)
                    shutil.copyfile(stderr, evidence / stderr.name)
            for index, path in enumerate(locals().get("logs", [])):
                shutil.copyfile(path, evidence / f"edge{index}-proxy.jsonl")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"image": args.image, "containers": results}, indent=2) + "\n",
                           encoding="utf-8")
    print(json.dumps({"passed": True, "wide_client": results["eight_nodes_32_inflight"],
                      "checked_containers": len(results), "report": str(args.output)}), flush=True)
    print("PASS resource_checks: real cgroups, readonly/non-root, 8 nodes, 32 in flight, file/disk/log quotas, failures")


if __name__ == "__main__":
    main()
