"""Long-lived interactive client: 8 nodes, 32 slots, repeated tasks and leaks."""
import argparse
import hashlib
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tempfile
import time

from extended_tests import ROOT, chunk_requests, require
from framework.data import make_file
from tools.public_tests import Lab, events, peak


def process_sample(pid):
    base = Path("/proc") / str(pid)
    data = dict(line.split(":", 1) for line in (base / "status").read_text().splitlines() if ":" in line)
    return {"threads": int(data["Threads"]), "fds": len(list((base / "fd").iterdir())),
            "rss_bytes": int(data["VmRSS"].split()[0]) * 1024}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=30)
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/stress-results.json")
    args = parser.parse_args()
    require(args.tasks >= 2, "use at least two tasks")
    started, samples = time.monotonic(), []
    with tempfile.TemporaryDirectory(prefix="cs305-stress-") as folder:
        lab = Lab(Path(folder))
        try:
            for file_id in ("stress-a", "stress-b"):
                lab.manifests[file_id] = make_file(lab.root / "data", file_id, 2 * 1024**2, 32768)
            lab.token = secrets.token_hex(24)
            _, _, lab.origin_url = lab.service("framework.origin", "origin", {
                "data_root": str(lab.root / "data"), "chunk_access_token": lab.token,
                "audit_path": str(lab.root / "origin-audit.jsonl"),
            })
            sources, logs = [], []
            faults = {0: {"action": "status", "status": 503, "retry_after": "0.1", "count": 2},
                      1: {"action": "reset", "count": 1},
                      2: {"action": "status", "status": 429, "count": 1}}
            for index in range(8):
                node = "stress" + str(index)
                url, log = lab.proxy(node + "-proxy", lab.cdn(node), {
                    "latency_ms": 5, "rate_mbps": 2,
                    "rules": [faults[index]] if index in faults else [],
                })
                sources.append({"id": node, "url": url})
                logs.append(log)
            output = lab.root / "output"
            config = lab.config("stress-client", {
                "origin_url": lab.origin_url, "output_root": str(output), "cdns": sources,
                "max_inflight_chunks": 32, "max_inflight_per_cdn": 4,
                "download_timeout_s": 15, "probe_interval_s": 0.2,
                "node_cooldown_s": 0.1, "retry_backoff_s": 0.08,
            })
            log = lab.root / "client.jsonl"
            out_stream, err_stream = log.open("w"), (lab.root / "client.stderr").open("w")
            lab.streams.extend((out_stream, err_stream))
            process = subprocess.Popen([sys.executable, "-u", "-m", "student.client", "--config", str(config)],
                                       cwd=ROOT, stdin=subprocess.PIPE, stdout=out_stream,
                                       stderr=err_stream, text=True)
            lab.processes.append(process)
            reader, partial = log.open(), ""
            try:
                for index in range(args.tasks):
                    request_id, file_id = "job" + str(index), "stress-a" if index % 2 == 0 else "stress-b"
                    process.stdin.write(json.dumps({"command": "DOWNLOAD", "request_id": request_id,
                                                    "file_id": file_id, "output_path": "result.bin"}) + "\n")
                    process.stdin.flush()
                    deadline, done = time.monotonic() + 18, False
                    while time.monotonic() < deadline and not done:
                        partial += reader.read()
                        lines = partial.split("\n")
                        partial = lines.pop()
                        for line in lines:
                            if not line.strip():
                                continue
                            row = json.loads(line)
                            require(row["event"] != "command_error", "interactive lifecycle rejected a task")
                            if row.get("request_id") == request_id and row["event"] in ("download_complete", "download_failed"):
                                require(row["event"] == "download_complete", "stress task failed: " + str(row))
                                done = True
                        if not done:
                            time.sleep(0.01)
                    require(done, "stress task did not terminate")
                    require(hashlib.sha256((output / "result.bin").read_bytes()).hexdigest()
                            == lab.manifests[file_id]["file_sha256"], "stress output hash")
                    require(sorted(path.name for path in output.iterdir()) == ["result.bin"], "stress temp-file leak")
                    sample = process_sample(process.pid)
                    require(sample["threads"] <= 3 and sample["fds"] <= 5, "threads/FDs leaked between tasks")
                    samples.append(sample)
                    if (index + 1) % 5 == 0:
                        print(json.dumps({"completed_tasks": index + 1, "idle_resources": sample}), flush=True)
                process.stdin.write('{"command":"EXIT","request_id":"exit"}\n')
                process.stdin.flush()
                process.stdin.close()
                require(process.wait(timeout=3) == 0, "stress interactive exit")
            finally:
                reader.close()
            time.sleep(0.15)
            rows = [row for path in logs for row in chunk_requests(path)]
            require(peak(rows) <= 32, "stress global in-flight cap")
            require(all(peak(chunk_requests(path)) <= 4 for path in logs), "stress per-node in-flight cap")
            require(len({row["request_id"] for row in rows}) == len(rows), "attempt IDs reused across tasks")
            total_logs = log.stat().st_size + (lab.root / "client.stderr").stat().st_size
            require(total_logs <= 256 * 1024, "long-lived client exceeded the functional log budget")
            require(max(sample["rss_bytes"] for sample in samples) - min(sample["rss_bytes"] for sample in samples)
                    < 16 * 1024**2, "unbounded memory growth between tasks")
            evidence = args.output.parent / "stress-evidence"
            evidence.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(log, evidence / "client.jsonl")
            for index, path in enumerate(logs):
                shutil.copyfile(path, evidence / ("edge" + str(index) + "-proxy.jsonl"))
            report = {"passed": True, "tasks": args.tasks, "elapsed_s": round(time.monotonic() - started, 3),
                      "file_bytes_per_task": 2 * 1024**2, "idle_samples": samples,
                      "client_log_bytes": total_logs, "gateway_peak": peak(rows),
                      "actual_chunk_requests": len(rows)}
        finally:
            lab.stop()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({key: value for key, value in report.items() if key != "idle_samples"}), flush=True)
    print("PASS stress_checks: repeated tasks; hashes; atomic output; bounded threads/FDs/memory/logs")


if __name__ == "__main__":
    main()
