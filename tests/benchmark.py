"""Controlled baseline comparison using real HTTP and real external load."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
from pathlib import Path
import shutil
import threading
import time

from extended_tests import ROOT, chunk_requests, download, laboratory, require
from framework.common import get_bytes
from framework.data import make_file
from tools.public_tests import events


def run_case(scenario, policy, repeat, evidence):
    with laboratory() as lab:
        size = 1024 * 1024
        lab.manifests["bench"] = make_file(lab.root / "data", "bench", size, 32768)
        ids = ("north", "south")
        cdns = {}
        if scenario == "external_load":
            make_file(lab.root / "data", "background", 6 * 16384, 16384)
            relay, _ = lab.proxy("background-origin", lab.origin_url, {
                "rules": [{"action": "stall", "seconds": 0.15, "file": "background"}],
            })
            _, _, cdns["north"] = lab.service("student.cdn", "loaded-north", {
                "cdn_id": "north", "origin_url": relay, "origin_chunk_token": lab.token,
                "cache_capacity_chunks": 2, "max_active_requests": 2,
            })
        else:
            cdns["north"] = lab.cdn("north")
        cdns["south"] = lab.cdn("south")
        rates = {"north": 6 if scenario == "external_load" else 1, "south": 3 if scenario == "external_load" else 6}
        if scenario == "shared_link":
            rates = {"north": 6, "south": 6}
        logs, entries = {}, []
        for node in ids:
            logs[node] = lab.root / (node + "-proxy.jsonl")
            faults = lab.config(node + "-shape", {"latency_ms": 15, "rate_mbps": rates[node]})
            entries.append({"upstream": cdns[node], "faults": str(faults), "log": str(logs[node])})
        proxy_config = lab.config("paired-proxies", {
            "entries": entries, "shared_rate_mbps": 3 if scenario == "shared_link" else None,
        })
        _, ready, _ = lab.start("paired-proxies", [str(ROOT / "tests/multi_proxy.py"),
                                                   "--config", str(proxy_config)])
        ports = next(row["ports"] for row in events(ready) if row.get("event") == "ready")
        urls = [(node, "http://127.0.0.1:" + str(port)) for node, port in zip(ids, ports)]
        stop = threading.Event()
        load_pool = None
        started_load = None

        def background(offset):
            index = offset
            while not stop.is_set() and time.monotonic() - started_load < 3:
                try:
                    get_bytes(cdns["north"], f"/chunk/background/{index % 6}",
                              connect_timeout_s=0.5, read_idle_timeout_s=1, body_deadline_s=1)
                except Exception:
                    pass
                index += 1

        try:
            if scenario == "external_load":
                started_load = time.monotonic()
                load_pool = ThreadPoolExecutor(max_workers=2)
                for offset in (0, 3):
                    load_pool.submit(background, offset)
                time.sleep(0.05)
            settings = {"strategy": policy if policy in ("adaptive", "round_robin") else "fixed",
                        "max_inflight_chunks": 2 if scenario == "shared_link" else 1,
                        "max_inflight_per_cdn": 2 if scenario == "shared_link" else 1,
                        "probe_interval_s": 0.2, "download_timeout_s": 25}
            if policy.startswith("fixed_"):
                settings["fixed_cdn_id"] = policy.removeprefix("fixed_")
            code, output, client_log, duration = download(lab, "benchmark", "bench", urls, **settings)
        finally:
            stop.set()
            if load_pool:
                load_pool.shutdown(wait=True)
        require(code == 0 and lab.file_ok(output, "bench"), f"{scenario}/{policy}: file/exit failure")
        evidence.mkdir(parents=True, exist_ok=True)
        counts = {node: len(chunk_requests(logs[node])) for node in ids}
        summary = next(row for row in events(client_log) if row["event"] == "recovery_summary")
        status = [row.get("load_ratio", 0) for row in events(client_log)
                  if row.get("event") == "measurement" and row.get("sample_kind") == "status"
                  and row.get("cdn_id") == "north"]
        result = {"scenario": scenario, "policy": policy, "repeat": repeat,
                  "elapsed_s": round(duration, 4), "goodput_mbps": round(size * 8 / duration / 1e6, 4),
                  "actual_chunk_requests": counts, "failed_attempts": summary["failed_attempts"],
                  "max_observed_north_load": round(max(status), 4) if status else None, "passed": True}
        for node in ids:
            shutil.copyfile(logs[node], evidence / (node + "-proxy.jsonl"))
        shutil.copyfile(client_log, evidence / "client.jsonl")
        print(json.dumps(result), flush=True)
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--scenarios", nargs="+", choices=["independent", "shared_link", "external_load"],
                        default=["independent", "shared_link", "external_load"])
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/benchmark-results.json")
    args = parser.parse_args()
    require(args.repeats >= 1, "repeats must be positive")
    results = []
    try:
        for scenario in args.scenarios:
            for repeat in range(args.repeats):
                for policy in ("fixed_north", "fixed_south", "round_robin", "adaptive"):
                    evidence = args.output.parent / "benchmark-evidence" / f"{scenario}-{repeat}-{policy}"
                    results.append(run_case(scenario, policy, repeat, evidence))
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps({"runs": results}, indent=2) + "\n", encoding="utf-8")
    print("PASS benchmark: controlled fixed/round-robin/adaptive comparison; all output hashes checked")


if __name__ == "__main__":
    main()
