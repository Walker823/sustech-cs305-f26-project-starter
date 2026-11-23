"""Reproducible real-process speed swaps; evidence comes from proxy requests."""
from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import shutil
import sys
import time

from extended_tests import ROOT, chunk_requests, laboratory, require
from framework.data import make_file
from tools.public_tests import events, peak


def replace_faults(path, rate):
    temporary = path.with_suffix(".new")
    temporary.write_text(json.dumps({"latency_ms": 20, "rate_mbps": rate}), encoding="utf-8")
    temporary.replace(path)


def experiment(first_fast, evidence):
    with laboratory() as lab:
        lab.manifests["adapt"] = make_file(lab.root / "data", "adapt", 4 * 1024 * 1024, 32768)
        ids = ("north", "south")
        other = next(node for node in ids if node != first_fast)
        proxies, logs = {}, {}
        for node in ids:
            proxies[node], logs[node] = lab.proxy("adapt-" + node, lab.cdn(node), {
                "latency_ms": 20, "rate_mbps": 4 if node == first_fast else 1,
            })
        output = lab.root / "adapt-output"
        config = lab.config("adaptive-client", {
            "origin_url": lab.origin_url, "output_root": str(output),
            "cdns": [{"id": node, "url": proxies[node]} for node in ids],
            "max_inflight_chunks": 2, "max_inflight_per_cdn": 2,
            "probe_interval_s": 0.2, "measurement_alpha": 0.3,
            "exploration_every": 6, "download_timeout_s": 45,
        })
        started, wall_start = time.monotonic(), time.time()
        process, client_log, _ = lab.start("adaptive-client", [
            "-m", "student.client", "--config", str(config),
            "--download", "adapt", "--output", "result.bin",
        ])
        swaps = []
        for at, fast in ((2.5, other), (5.0, first_fast)):
            while process.poll() is None and time.monotonic() - started < at:
                time.sleep(0.02)
            require(process.poll() is None, "download ended before both swaps were triggered")
            for node in ids:
                replace_faults(lab.root / ("adapt-" + node + "-faults.json"), 4 if node == fast else 1)
            swaps.append(time.time())
        code = process.wait(timeout=35)
        duration = time.monotonic() - started
        time.sleep(0.15)
        require(code == 0 and lab.file_ok(output, "adapt"), "adaptive file integrity/exit code")
        phases = [Counter({node: 0 for node in ids}) for _ in range(3)]
        all_chunks, all_measurements = [], []
        evidence.mkdir(parents=True, exist_ok=True)
        for node in ids:
            rows = chunk_requests(logs[node])
            require(peak(rows) <= 2, "adaptive per-CDN in-flight cap")
            for row in rows:
                phase = sum(row["received"] >= swap for swap in swaps)
                phases[phase][node] += 1
            all_chunks.extend(rows)
            measurements = [row for row in events(logs[node]) if row.get("event") == "request"
                            and row["path"] in ("/probe", "/status")]
            require(peak(measurements) <= 1, "per-CDN measurement concurrency")
            for path in ("/probe", "/status"):
                times = sorted(row["received"] for row in measurements if row["path"] == path)
                if times:
                    require(len(times) <= (times[-1] - times[0]) / 0.2 + 2.1,
                            "measurement request frequency")
            all_measurements.extend(measurements)
            shutil.copyfile(logs[node], evidence / (node + "-proxy.jsonl"))
        require(peak(all_chunks) <= 2 and peak(all_measurements) <= 8, "adaptive global limits")
        require(len(all_chunks) == len(lab.manifests["adapt"]["chunks"]), "duplicate/adaptive requests")
        fast_order = [first_fast, other, first_fast]
        shares = [phase[fast] / max(1, sum(phase.values())) for phase, fast in zip(phases, fast_order)]
        shutil.copyfile(client_log, evidence / "client.jsonl")
        require(all(sum(phase.values()) >= 8 for phase in phases), "insufficient chunks in a swap phase")
        for index in (1, 2):
            newly_fast = fast_order[index]
            previous_share = phases[index - 1][newly_fast] / sum(phases[index - 1].values())
            require(shares[index] > 0.55 and shares[index] > previous_share + 0.1,
                    "allocation did not adapt after speed swap: " + json.dumps(phases))
        result = {"first_fast": first_fast, "elapsed_s": round(duration, 3),
                  "file_bytes": lab.manifests["adapt"]["file_size"], "chunk_bytes": 32768,
                  "global_limit": 2, "per_cdn_limit": 2, "rates_mbps": [4, 1],
                  "swap_after_s": [round(value - wall_start, 3) for value in swaps],
                  "phase_assignments": [dict(phase) for phase in phases],
                  "phase_fast_share": [round(value, 3) for value in shares],
                  "measurement_peak": peak(all_measurements), "passed": True}
        print(json.dumps(result, ensure_ascii=False), flush=True)
        return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--repeats", type=int, default=1)
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/adaptive-results.json")
    args = parser.parse_args()
    require(args.repeats >= 1, "repeats must be positive")
    evidence = args.output.parent / "adaptive-evidence"
    results = []
    for first in ("north", "south"):
        for repeat in range(args.repeats):
            try:
                result = experiment(first, evidence / f"first-{first}-run{repeat}")
            except AssertionError as exc:
                result = {"first_fast": first, "passed": False, "reason": str(exc)}
                print(json.dumps(result), flush=True)
            result["repeat"] = repeat
            results.append(result)
    groups = {first: {"successes": sum(row["passed"] for row in results if row["first_fast"] == first),
                      "trials": args.repeats} for first in ("north", "south")}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps({"experiments": results, "groups": groups}, ensure_ascii=False, indent=2) + "\n",
                           encoding="utf-8")
    require(all(row["passed"] for row in results), "one or more speed-swap experiments failed; see report/evidence")
    print("PASS adaptive_swaps: " + json.dumps(groups) + "; evidence at " + str(evidence))


if __name__ == "__main__":
    main()
