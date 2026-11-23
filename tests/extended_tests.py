"""Student-owned integration checks. Run from the package root in Linux."""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from collections import defaultdict
import json
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from framework.common import get_bytes, sha256  # noqa: E402
from framework.data import make_file  # noqa: E402
from student.cache import LRUCache  # noqa: E402
from student.measurement import Measurements  # noqa: E402
from student.scheduler import select_chunk_source  # noqa: E402
from student.state import ChunkState, NodeState  # noqa: E402
from tools.public_tests import Lab, events, peak  # noqa: E402


def require(condition, message):
    if not condition:
        raise AssertionError(message)


@contextmanager
def laboratory():
    with tempfile.TemporaryDirectory(prefix="cs305-extra-") as folder:
        lab = Lab(Path(folder))
        try:
            for name, size in (("demo", 3 * 65536 + 17), ("other", 65536), ("empty", 0)):
                lab.manifests[name] = make_file(lab.root / "data", name, size, 65536)
            lab.token = secrets.token_hex(24)
            _, _, lab.origin_url = lab.service("framework.origin", "origin", {
                "host": "127.0.0.1", "port": 0, "data_root": str(lab.root / "data"),
                "chunk_access_token": lab.token, "audit_path": str(lab.root / "origin-audit.jsonl"),
            })
            lab.cdn_a = lab.cdn("A")
            yield lab
        finally:
            lab.stop()


def download(lab, name, file_id, cdns, *, existing=None, output_path="result.bin",
             wait_timeout=35, prepare_output=None, **settings):
    output = lab.root / ("out-" + name)
    output.mkdir()
    if existing is not None:
        (output / "result.bin").write_bytes(existing)
    if prepare_output is not None:
        prepare_output(output)
    config = lab.config("client-" + name, {
        "origin_url": lab.origin_url, "output_root": str(output),
        "cdns": [{"id": node_id, "url": url} for node_id, url in cdns],
        "max_inflight_chunks": 2, "max_inflight_per_cdn": 1,
        "download_timeout_s": 20, **settings,
    })
    started = time.monotonic()
    process, log, _ = lab.start("client-" + name, [
        "-m", "student.client", "--config", str(config),
        "--download", file_id, "--output", output_path,
    ])
    code = process.wait(timeout=wait_timeout)
    duration = time.monotonic() - started
    time.sleep(0.12)  # Allow proxies to flush their last request records.
    return code, output, log, duration


def terminal_events(log, success):
    rows = events(log)
    require(sum(row["event"] == "download_started" for row in rows) == 1, "one task-start event")
    terminal = [row for row in rows if row["event"] in ("download_complete", "download_failed")]
    require(len(terminal) == 1 and terminal[0]["event"] == (
        "download_complete" if success else "download_failed"), "one correct terminal event")
    return terminal[0]


def cdn_checks():
    cache = LRUCache(2)
    cache.put(("a", 0), b"a")
    cache.put(("b", 0), b"b")
    require(cache.get(("a", 0)) == b"a", "cache lookup")
    require(cache.put(("a", 1), b"c") == ("b", 0), "LRU promotion/eviction")
    require(cache.put(("a", 0), b"updated") is None, "overwrite must not evict")
    require(len(cache) == 2, "capacity counts chunks")

    shared = LRUCache(7)

    def work(worker):
        for index in range(150):
            key = ("file" + str(worker), index % 11)
            shared.put(key, repr(key).encode())
            data = shared.get(key)
            require(data is None or data == repr(key).encode(), "concurrent cache data")
            require(len(shared) <= 7, "concurrent cache overflow")

    with ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(work, range(12)))

    with laboratory() as lab:
        for path, code in (("/chunk/missing/0", "FILE_NOT_FOUND"),
                           ("/chunk/demo/999", "CHUNK_NOT_FOUND"),
                           ("/chunk/empty/0", "CHUNK_NOT_FOUND")):
            response = get_bytes(lab.cdn_a, path)
            require(response.status == 404 and json.loads(response.body)["error"]["code"] == code,
                    "wrong missing-file/chunk response: " + path)
        for file_id in ("demo", "other"):
            meta = lab.manifests[file_id]["chunks"][0]
            for expected in ("MISS", "HIT"):
                response = get_bytes(lab.cdn_a, f"/chunk/{file_id}/0")
                require(response.status == 200 and response.headers["x-cache"] == expected,
                        "cache status does not match access")
                require(response.headers["x-chunk-sha256"] == meta["sha256"]
                        and sha256(response.body) == meta["sha256"], "untrusted content/header hash")

        proxy, _ = lab.proxy("origin-fault", lab.origin_url, {"rules": [
            {"action": "status", "status": 503, "count": 1},
            {"action": "corrupt", "count": 1},
        ]})
        _, _, cdn = lab.service("student.cdn", "origin-fault-cdn", {
            "cdn_id": "fault", "origin_url": proxy, "origin_chunk_token": lab.token,
            "cache_capacity_chunks": 2,
        })
        for expected_status, expected_code in ((502, "ORIGIN_ERROR"),
                                               (502, "UPSTREAM_INTEGRITY_ERROR")):
            response = get_bytes(cdn, "/chunk/demo/0")
            require(response.status == expected_status
                    and json.loads(response.body)["error"]["code"] == expected_code,
                    "origin errors/corrupt data must not be successful or cached")
        for expected in ("MISS", "HIT"):
            response = get_bytes(cdn, "/chunk/demo/0")
            require(response.status == 200 and response.headers["x-cache"] == expected,
                    "origin recovery/cache after failure")
        fetched = [row for row in events(lab.root / "origin-audit.jsonl")
                   if row["event"] == "origin_chunk_request" and row.get("file_id") == "demo"]
        require(len(fetched) == 3, "unexpected actual origin fetch count")

        _, _, forbidden = lab.service("student.cdn", "wrong-token-cdn", {
            "cdn_id": "forbidden", "origin_url": lab.origin_url,
            "origin_chunk_token": "deliberately-wrong-token", "cache_capacity_chunks": 2,
        })
        require(get_bytes(forbidden, "/chunk/demo/0").status == 502, "wrong origin token handling")
    print("PASS extra_cdn: concurrent LRU; cross-file cache; 404/502; hashes; origin recovery/audit")


def download_checks():
    with laboratory() as lab:
        old = b"existing output must survive failure"
        code, output, log, _ = download(lab, "empty", "empty", [("A", lab.cdn_a)], existing=old)
        require(code == 0 and (output / "result.bin").read_bytes() == b"", "empty-file atomic replacement")
        terminal_events(log, True)
        require(sorted(p.name for p in output.iterdir()) == ["result.bin"], "empty-file cleanup")

        code, output, log, _ = download(lab, "replace", "demo", [("A", lab.cdn_a)], existing=old)
        require(code == 0 and lab.file_ok(output, "demo"), "replace existing file after verification")
        terminal_events(log, True)

        bad, _ = lab.proxy("bad-content", lab.cdn_a, {"rules": [{"action": "corrupt"}]})
        code, output, log, _ = download(lab, "protect", "demo", [("A", bad)], existing=old)
        require(code == 1 and (output / "result.bin").read_bytes() == old, "failure must preserve target")
        require(sorted(p.name for p in output.iterdir()) == ["result.bin"], "failure temp-file cleanup")
        terminal_events(log, False)

        outside = lab.root / "outside.bin"
        outside.write_bytes(old)
        code, output, log, _ = download(lab, "escape", "demo", [("A", lab.cdn_a)],
                                        output_path="../outside.bin")
        require(code == 1 and outside.read_bytes() == old and not list(output.iterdir()), "path traversal")
        require(terminal_events(log, False)["code"] == "INVALID_OUTPUT_PATH", "path error code")

        code, _, log, _ = download(lab, "absolute", "demo", [("A", lab.cdn_a)],
                                   output_path=str(outside))
        require(code == 1 and outside.read_bytes() == old, "absolute output path")
        require(terminal_events(log, False)["code"] == "INVALID_OUTPUT_PATH", "absolute path error")
        code, output, log, _ = download(
            lab, "symlink", "demo", [("A", lab.cdn_a)], output_path="elsewhere/outside.bin",
            prepare_output=lambda folder: (folder / "elsewhere").symlink_to(lab.root, target_is_directory=True))
        require(code == 1 and outside.read_bytes() == old, "symlink output escape")
        require(terminal_events(log, False)["code"] == "INVALID_OUTPUT_PATH", "symlink path error")

        code, output, log, _ = download(lab, "size-limit", "demo", [("A", lab.cdn_a)],
                                        existing=old, max_file_size_bytes=1)
        require(code == 1 and (output / "result.bin").read_bytes() == old, "file-size guard")
        require(terminal_events(log, False)["code"] == "FILE_TOO_LARGE", "file-size error code")

        manifest = make_file(lab.root / "data", "whole", 120000, 65536)
        manifest["file_sha256"] = "0" * 64
        (lab.root / "data/whole/manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        code, output, log, _ = download(lab, "whole-hash", "whole", [("A", lab.cdn_a)], existing=old)
        require(code == 1 and (output / "result.bin").read_bytes() == old, "whole-file hash/old output")
        require(terminal_events(log, False)["code"] == "FILE_INTEGRITY_ERROR", "whole-file error code")
    print("PASS extra_download: empty file; replacement; failure cleanup; path/size guard; whole-file hash")


def concurrency_checks():
    with laboratory() as lab:
        lab.manifests["parallel"] = make_file(lab.root / "data", "parallel", 12 * 32768, 32768)
        sources, logs = [], []
        for index, node_id in enumerate(("ridge", "lakeside", "east")):
            cdn = lab.cdn(node_id)
            url, log = lab.proxy("bounded-" + node_id, cdn, {
                "latency_ms": 10, "rate_mbps": 2,
                "rules": [{"action": "stall", "seconds": 0.12, "count": 1}],
            })
            sources.append((node_id, url))
            logs.append(log)
        for global_limit, per_node in ((1, 1), (3, 1), (4, 2)):
            before = [len(events(log)) for log in logs]
            name = f"bounded-{global_limit}-{per_node}"
            code, output, _, _ = download(lab, name, "parallel", sources,
                                          max_inflight_chunks=global_limit,
                                          max_inflight_per_cdn=per_node)
            require(code == 0 and lab.file_ok(output, "parallel"), "concurrent file reassembly")
            all_rows = []
            all_measurements = []
            for log, offset in zip(logs, before):
                current = events(log)[offset:]
                rows = [row for row in current
                        if row.get("event") == "request" and row["path"].startswith("/chunk/")]
                require(peak(rows) <= per_node, "per-CDN limit exceeded")
                all_rows.extend(rows)
                measurements = [row for row in current if row.get("event") == "request"
                                and row["path"] in ("/probe", "/status")]
                require(peak(measurements) <= 1, "overlapping measurements at one CDN")
                all_measurements.extend(measurements)
            require(peak(all_rows) <= global_limit, "global in-flight limit exceeded")
            require(peak(all_measurements) <= 8, "global measurement limit exceeded")
            by_chunk = defaultdict(list)
            for row in all_rows:
                by_chunk[row["path"]].append(row)
            require(len(all_rows) == 12 and len(by_chunk) == 12, "completed chunk requested again")
            require(all(peak(rows) <= 1 for rows in by_chunk.values()), "duplicate in-flight chunk")
    print("PASS extra_concurrency: 3 arbitrary IDs; limits 1/1, 3/1, 4/2; unique chunks; reassembly")


def chunk_requests(log):
    return [row for row in events(log)
            if row.get("event") == "request" and row["path"].startswith("/chunk/")]


def recovery_checks():
    with laboratory() as lab:
        common = {"max_inflight_chunks": 1, "max_inflight_per_cdn": 1,
                  "node_cooldown_s": 0.04, "retry_backoff_s": 0.02,
                  "connect_timeout_s": 0.2, "read_idle_timeout_s": 0.2}
        for action, extra in (("truncate", {"after_bytes": 1000}), ("reset", {}),
                              ("stall", {"seconds": 3})):
            proxy, requests = lab.proxy("retry-" + action, lab.cdn_a, {
                "rules": [{"action": action, "count": 1, **extra}],
            })
            code, output, log, duration = download(lab, "retry-" + action, "demo", [("A", proxy)],
                                                    **common)
            require(code == 0 and lab.file_ok(output, "demo"), "failed to recover from " + action)
            require(duration < 3, "stalled request was not bounded")
            terminal_events(log, True)
            rows = chunk_requests(requests)
            require(len(rows) == 5, "only the failed chunk should be retried: " + action)
            require(len({row["request_id"] for row in rows}) == len(rows), "attempt ID reused")
            require(peak(rows) <= 1, "failed request released its in-flight slot too early")

        proxy, requests = lab.proxy("retry-after", lab.cdn_a, {
            "rules": [{"action": "status", "status": 503, "retry_after": "0.35", "count": 1}],
        })
        code, output, _, _ = download(lab, "retry-after", "demo", [("A", proxy)], **common)
        require(code == 0 and lab.file_ok(output, "demo"), "503 recovery")
        rows = sorted(chunk_requests(requests), key=lambda row: row["received"])
        require(rows[1]["received"] - rows[0]["end"] >= 0.33, "Retry-After was ignored")

        lab.manifests["tiny"] = make_file(lab.root / "data", "tiny", 1, 1)
        proxy, requests = lab.proxy("tiny-http-error", lab.cdn_a, {
            "rules": [{"action": "status", "status": 503, "retry_after": "0.1", "count": 1}],
        })
        code, output, log, _ = download(lab, "tiny-http-error", "tiny", [("A", proxy)], **common)
        require(code == 0 and lab.file_ok(output, "tiny"), "HTTP error body longer than a tiny chunk")
        require(not any(row.get("state") == "quarantined" for row in events(log)),
                "503 JSON was incorrectly treated as chunk length corruption")

        bad, bad_log = lab.proxy("one-node-waits", lab.cdn_a, {
            "rules": [{"action": "status", "status": 503, "retry_after": 1, "count": 1}],
        })
        good, good_log = lab.proxy("other-node-continues", lab.cdn_b, {"rate_mbps": 2})
        code, output, _, _ = download(lab, "local-cooldown", "demo", [("A", bad), ("B", good)],
                                      node_cooldown_s=0.04, retry_backoff_s=0.02)
        require(code == 0 and lab.file_ok(output, "demo"), "reassignment to healthy CDN")
        failure = next(row for row in chunk_requests(bad_log) if row["status"] == 503)
        require(any(failure["end"] < row["received"] < failure["end"] + 0.8
                    for row in chunk_requests(good_log)), "Retry-After incorrectly blocked other CDNs")
        require(all(row["received"] >= failure["end"] + 0.98
                    for row in chunk_requests(bad_log) if row["received"] > failure["end"]),
                "failed CDN reused during Retry-After")

        proxy, requests = lab.proxy("always-429", lab.cdn_a, {
            "rules": [{"action": "status", "status": 429}],
        })
        old = b"preserve old output on recovery failure"
        code, output, log, _ = download(lab, "budget", "demo", [("A", proxy)], existing=old,
                                        max_attempts_per_chunk=2, **common)
        require(code == 1 and (output / "result.bin").read_bytes() == old, "429 failure protection")
        require(terminal_events(log, False)["code"] == "RETRY_EXHAUSTED", "429 retry budget")
        counts = defaultdict(int)
        for row in chunk_requests(requests):
            counts[row["path"]] += 1
        require(max(counts.values()) == 2 and all(count <= 2 for count in counts.values()),
                "max_attempts_per_chunk not enforced")

        proxy, _ = lab.proxy("slow-body", lab.cdn_a, {"rate_mbps": 0.05})
        code, output, log, duration = download(lab, "body-deadline", "demo", [("A", proxy)],
                                               existing=old, max_attempts_per_chunk=2,
                                               max_inflight_chunks=1, max_inflight_per_cdn=1,
                                               body_deadline_s=0.08, read_idle_timeout_s=0.5,
                                               node_cooldown_s=0.04, retry_backoff_s=0.02)
        require(code == 1 and duration < 2, "response-body deadline not enforced")
        require(any(row.get("code") == "BODY_DEADLINE" for row in events(log)), "body vs idle timeout")
        require((output / "result.bin").read_bytes() == old, "body-timeout output protection")

        proxy, _ = lab.proxy("all-stall", lab.cdn_a, {
            "rules": [{"action": "stall", "seconds": 5, "all_paths": True}],
        })
        code, output, log, duration = download(lab, "total-deadline", "demo", [("A", proxy)],
                                               existing=old, download_timeout_s=0.4,
                                               connect_timeout_s=5, read_idle_timeout_s=5,
                                               body_deadline_s=5)
        require(code == 1 and duration < 1.5, "overall deadline failed to interrupt slow headers")
        require(terminal_events(log, False)["code"] == "DOWNLOAD_DEADLINE", "deadline terminal error")
        require((output / "result.bin").read_bytes() == old
                and sorted(p.name for p in output.iterdir()) == ["result.bin"], "deadline cleanup")

        proxy, requests = lab.proxy("old-success", lab.cdn_a, {
            "latency_ms": 80, "rate_mbps": 2,
            "rules": [{"action": "status", "status": 503, "count": 1}],
        })
        code, output, _, _ = download(lab, "generation", "demo", [("A", proxy)],
                                      max_inflight_chunks=2, max_inflight_per_cdn=2,
                                      node_cooldown_s=0.6, retry_backoff_s=0.02)
        require(code == 0 and lab.file_ok(output, "demo"), "cooldown/trial recovery")
        rows = sorted(chunk_requests(requests), key=lambda row: row["received"])
        failed = next(row for row in rows if row["status"] == 503)
        require(any(row["received"] < failed["end"] and row.get("body_end", 0) > failed["end"]
                    for row in rows if row["status"] == 200), "old in-flight success scenario not triggered")
        later = [row for row in rows if row["received"] > failed["end"]]
        require(later[0]["received"] - failed["end"] >= 0.57, "old success cleared newer cooldown")
        require(all(row["received"] >= later[0]["body_end"] - 0.01 for row in later[1:]),
                "more than one trial request after cooldown")

    print("PASS extra_recovery: truncation/reset/idle timeout; Retry-After locality; 429 budget; "
          "body/overall deadlines; old-success generation; single trial")


def interactive_checks():
    with laboratory() as lab:
        bad, bad_log = lab.proxy("interactive-bad", lab.cdn_a, {
            "rules": [{"action": "corrupt", "count": 1}],
        })
        output = lab.root / "interactive-output"
        config = lab.config("interactive", {
            "origin_url": lab.origin_url, "output_root": str(output),
            "cdns": [{"id": "A", "url": bad}, {"id": "B", "url": lab.cdn_b}],
            "max_inflight_chunks": 2, "max_inflight_per_cdn": 1,
        })
        log = lab.root / "interactive-client.jsonl"
        stdout = log.open("w")
        stderr = (lab.root / "interactive-client.stderr").open("w")
        lab.streams.extend((stdout, stderr))
        process = subprocess.Popen([sys.executable, "-u", "-m", "student.client", "--config", str(config)],
                                   stdin=subprocess.PIPE, stdout=stdout, stderr=stderr, cwd=ROOT, text=True)
        lab.processes.append(process)
        first_count = 0
        for request_id in ("first", "second"):
            process.stdin.write(json.dumps({"command": "DOWNLOAD", "request_id": request_id,
                                            "file_id": "demo", "output_path": request_id + ".bin"}) + "\n")
            process.stdin.flush()
            until = time.monotonic() + 8
            while time.monotonic() < until:
                finished = [row for row in events(log) if row.get("request_id") == request_id
                            and row["event"] in ("download_complete", "download_failed")]
                if finished:
                    break
                time.sleep(0.01)
            require(finished and finished[0]["event"] == "download_complete", "interactive task failed")
            require(sha256((output / (request_id + ".bin")).read_bytes())
                    == lab.manifests["demo"]["file_sha256"], "interactive result hash")
            if request_id == "first":
                first_count = len(chunk_requests(bad_log))
        process.stdin.write('{"command":"EXIT","request_id":"quit"}\n')
        process.stdin.flush()
        process.stdin.close()
        require(process.wait(timeout=3) == 0, "interactive shutdown")
        require(len(chunk_requests(bad_log)) > first_count, "quarantine did not reset for next file task")
        require(not any(row["event"] == "command_error" for row in events(log)), "terminal/busy race")
    print("PASS extra_interactive: sequential tasks; quarantine reset; terminal/busy lifecycle; EXIT")


def measurement_checks():
    measurements = Measurements(0.25)
    measurements.register("unknown")
    require(measurements.snapshot()["unknown"]["goodput_mbps"] is None, "unknown values must be null")
    measurements.observe_probe("north", 100)
    measurements.observe_probe("north", 200)
    measurements.observe_chunk("north", 125000, 1000)
    require(measurements.snapshot()["north"]["goodput_mbps"] == 1, "Mbps unit conversion")
    measurements.observe_chunk("north", 125000, 500)
    measurements.observe_chunk("north", 125000, 2000)
    measurements.observe_status("north", {"active_requests": 3, "max_active_requests": 4})
    row = measurements.snapshot()["north"]
    require(row["probe_latency_ms"] == 125 and row["load_ratio"] == 0.75, "latency/load units and EWMA")
    require(abs(row["goodput_mbps"] - 0.6875) < 1e-12, "quick decrease/gradual increase")
    measurements.probe_failed("north")
    require(measurements.snapshot()["north"]["reachable"] is False, "Probe reachability")

    a, b = NodeState("north", "http://127.0.0.1:1", 2), NodeState("south", "http://127.0.0.1:2", 2)
    chunk = ChunkState(0, 65536, "0" * 64)
    now = time.monotonic()
    view = {node.node_id: {"goodput_mbps": 2, "probe_latency_ms": 20,
                          "chunk_updated_at": now, "status_updated_at": now,
                          "load_ratio": load} for node, load in ((a, 0.9), (b, 0.0))}
    view["__policy__"] = {"assignments": 1, "exploration_every": 8}
    require(select_chunk_source(chunk, [a, b], {}, view) is b, "server load does not affect selection")
    view[a.node_id]["load_ratio"], view[b.node_id]["load_ratio"] = 0.0, 0.9
    require(select_chunk_source(chunk, [b, a], {}, view) is a, "load change/list reversal not handled")
    unknown = {a.node_id: {"status_updated_at": now, "active_requests": 4,
                           "max_active_requests": 4, "goodput_mbps": None},
               b.node_id: {"goodput_mbps": 2, "probe_latency_ms": 20,
                           "chunk_updated_at": now, "status_updated_at": now,
                           "active_requests": 0, "max_active_requests": 4}}
    require(select_chunk_source(chunk, [a, b], {}, unknown) is b,
            "unknown/full source overrides a healthy measured alternative")
    a.quarantined = True
    require(select_chunk_source(chunk, [a, b], {}, view) is b, "quarantine overrides good measurements")
    print("PASS extra_measurement: units; EWMA; fast slowdown; unknown/null; load changes; quarantine")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage", choices=["cdn", "download", "concurrency", "recovery", "interactive", "measurement"], required=True)
    args = parser.parse_args()
    {"cdn": cdn_checks, "download": download_checks,
     "concurrency": concurrency_checks, "recovery": recovery_checks,
     "interactive": interactive_checks, "measurement": measurement_checks}[args.stage]()


if __name__ == "__main__":
    main()
