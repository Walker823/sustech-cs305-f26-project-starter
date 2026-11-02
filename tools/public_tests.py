"""Public self-tests: real local Origin, CDN, Client and fault_proxy.py processes.

Not a grading sandbox and not the hidden tests. Each sample reports PASS or FAIL on its own:

    python3 public_tests.py --skeleton          # lifecycle of the empty starter
    python3 public_tests.py                     # all samples
    python3 public_tests.py --only multi_source # one sample
"""
from __future__ import annotations
import argparse
import json
from pathlib import Path
import secrets
import subprocess
import sys
import tempfile
import time

# Package root (the folder that holds framework/ and student/); this script lives in tools/.
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from framework.common import get_bytes, sha256  # noqa: E402
from framework.data import make_file  # noqa: E402


def events(path):
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict):
                result.append(value)
        except ValueError:
            pass
    return result


def check(condition, message):
    if not condition:
        raise AssertionError(message)


class Lab:
    """Processes for one run. Everything stops and the temporary folder goes away at the end."""

    def __init__(self, root):
        self.root, self.processes, self.streams, self.manifests = root, [], [], {}
        self._cdn_b = None

    def start(self, name, args):
        stdout, err = self.root / (name + ".jsonl"), self.root / (name + ".stderr")
        out_stream, err_stream = stdout.open("w"), err.open("w")
        self.streams.extend((out_stream, err_stream))
        process = subprocess.Popen([sys.executable, "-u", *args], stdout=out_stream, stderr=err_stream,
                                   stdin=subprocess.DEVNULL, cwd=ROOT)
        self.processes.append(process)
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            ready = [e for e in events(stdout) if e.get("event") == "ready"]
            if ready:
                port = ready[0].get("port")  # the Client has no port
                return process, stdout, None if port is None else "http://127.0.0.1:" + str(port)
            if process.poll() is not None:
                raise AssertionError(name + " 启动失败: " + err.read_text()[-2000:])
            time.sleep(0.02)
        raise AssertionError(name + " 未报告 ready")

    def config(self, name, value):
        path = self.root / (name + ".json")
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def service(self, module, name, value):
        return self.start(name, ["-m", module, "--config", str(self.config(name, value))])

    def cdn(self, node_id):
        _, _, url = self.service("student.cdn", "cdn-" + node_id, {
            "cdn_id": node_id, "host": "127.0.0.1", "port": 0, "origin_url": self.origin_url,
            "origin_chunk_token": self.token, "cache_capacity_chunks": 2})
        return url

    @property
    def cdn_b(self):
        if self._cdn_b is None:  # started only for the samples that use two CDNs
            self._cdn_b = self.cdn("B")
        return self._cdn_b

    def proxy(self, name, upstream, faults):
        log = self.root / (name + "-requests.jsonl")
        _, _, url = self.start(name, [str(ROOT / "tools" / "fault_proxy.py"), "--upstream", upstream,
                                      "--faults", str(self.config(name + "-faults", faults)), "--log", str(log)])
        return url, log

    def download(self, name, file_id, cdns, existing=None, **settings):
        """Run the Client once; returns (exit code, output folder, Client event log)."""
        output = self.root / ("out-" + name)
        output.mkdir()
        if existing is not None:
            (output / "result.bin").write_bytes(existing)
        cfg = self.config("client-" + name, {"origin_url": self.origin_url, "output_root": str(output),
                                             "cdns": [{"id": i, "url": u} for i, u in cdns],
                                             "max_inflight_chunks": 2, "max_inflight_per_cdn": 1,
                                             "download_timeout_s": 20, **settings})
        process, log, _ = self.start("client-" + name, ["-m", "student.client", "--config", str(cfg),
                                                        "--download", file_id, "--output", "result.bin"])
        try:
            code = process.wait(timeout=40)
        except subprocess.TimeoutExpired:
            raise AssertionError("Client 40 秒内没有退出")
        time.sleep(0.2)  # let fault_proxy finish writing its last request lines
        return code, output, log

    def file_ok(self, output, file_id):
        path = output / "result.bin"
        return path.is_file() and sha256(path.read_bytes()) == self.manifests[file_id]["file_sha256"]

    def stop(self):
        for process in reversed(self.processes):
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(3)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(3)
        for stream in self.streams:
            stream.close()


def chunk_rows(log):
    """fault_proxy request lines for /chunk/ (Probe and Status are left out)."""
    return [r for r in events(log) if r.get("event") == "request" and r["path"].startswith("/chunk/")]


def held_until(row):
    return row.get("body_end", row["end"])  # last byte not yet written, so the Client still holds the slot


def peak(rows):
    points = sorted([(r["received"], 1) for r in rows] + [(held_until(r), -1) for r in rows],
                    key=lambda p: (p[0], p[1]))
    current = best = 0
    for _, step in points:
        current += step
        best = max(best, current)
    return best


def body_overlap_ms(rows_a, rows_b):
    """Longest overlap of two correct bodies of different chunks, one through each proxy."""
    good = lambda rows: [r for r in rows if r.get("status") == 200 and r["fault"] is None and "body_end" in r]
    best = 0.0
    for a in good(rows_a):
        for b in good(rows_b):
            if a["path"] != b["path"]:
                best = max(best, min(a["body_end"], b["body_end"]) - max(a["body_start"], b["body_start"]))
    return best * 1000


def sample_download(lab):
    """One CDN, a final chunk shorter than the others, independent file hash."""
    cdn = lab.cdn_a
    check(get_bytes(cdn, "/health").status == 200, "health")
    check(get_bytes(cdn, "/probe").body == b"OK", "probe")
    check(json.loads(get_bytes(cdn, "/status").body)["active_requests"] == 0, "status")
    check(get_bytes(lab.origin_url, "/chunk/demo/0").status == 403, "Origin chunk 访问边界")
    code, output, _ = lab.download("download", "demo", [("A", cdn)], max_inflight_per_cdn=2)
    check(code == 0, "下载失败，退出码 %s" % code)
    check(lab.file_ok(output, "demo"), "输出文件 hash 与 Manifest 不符")


def sample_serial_lru(lab):
    """Capacity 2, requests 0,1,0,2,1 one at a time: X-Cache and the Origin fetch order."""
    for chunk, expected in ((0, "MISS"), (1, "MISS"), (0, "HIT"), (2, "MISS"), (1, "MISS")):
        response = get_bytes(lab.cdn_a, "/chunk/lru/" + str(chunk))
        check(response.status == 200, "块 %d 返回 %d" % (chunk, response.status))
        check(response.headers.get("x-cache") == expected, "块 %d 应为 %s，实为 %s" % (
            chunk, expected, response.headers.get("x-cache")))
    fetched = [e["chunk_id"] for e in events(lab.root / "origin-audit.jsonl")
               if e.get("event") == "origin_chunk_request" and e.get("file_id") == "lru"]
    check(fetched == [0, 1, 2, 1], "Origin 回源顺序应为 [0, 1, 2, 1]，实为 %s" % fetched)


def sample_multi_source(lab):
    """Two CDNs behind fault_proxy (2 Mbps each); global 2, per CDN 1. Bodies must overlap."""
    shaping = {"latency_ms": 10, "rate_mbps": 2}
    url_a, log_a = lab.proxy("proxy-multi-a", lab.cdn_a, shaping)
    url_b, log_b = lab.proxy("proxy-multi-b", lab.cdn_b, shaping)
    code, output, _ = lab.download("multi", "multi", [("A", url_a), ("B", url_b)])
    check(code == 0, "下载失败，退出码 %s" % code)
    check(lab.file_ok(output, "multi"), "输出文件 hash 与 Manifest 不符")
    rows_a, rows_b = chunk_rows(log_a), chunk_rows(log_b)
    check(rows_a and rows_b, "只用了一个来源（A %d 次、B %d 次块请求）" % (len(rows_a), len(rows_b)))
    check(peak(rows_a) <= 1 and peak(rows_b) <= 1, "单 CDN 同时在途超过 1（A %d、B %d）" % (peak(rows_a), peak(rows_b)))
    check(peak(rows_a + rows_b) <= 2, "全局同时在途超过 2（%d）" % peak(rows_a + rows_b))
    overlap = body_overlap_ms(rows_a, rows_b)
    check(overlap >= 20, "两个来源的数据没有同时传输（最长重叠 %.1f ms，需要 20 ms）；只是请求同时发出不算" % overlap)


def sample_corrupt_recovery(lab):
    """CDN A corrupts the first chunk it serves; the file must still be right and A isolated."""
    url_a, log_a = lab.proxy("proxy-bad-a", lab.cdn_a, {"rate_mbps": 2, "rules": [{"action": "corrupt", "count": 1}]})
    url_b, _ = lab.proxy("proxy-bad-b", lab.cdn_b, {"rate_mbps": 2})
    code, output, _ = lab.download("bad", "bad", [("A", url_a), ("B", url_b)])
    check(code == 0, "下载失败，退出码 %s" % code)
    check(lab.file_ok(output, "bad"), "输出文件 hash 与 Manifest 不符：坏块被写进了文件")
    rows = chunk_rows(log_a)
    bad = [r for r in rows if r["fault"] == "corrupt"]
    check(bad, "A 没有收到块请求，坏块没有被触发；Client 应同时使用两个来源")
    later = [r for r in rows if r["received"] > bad[0]["end"] + 0.1]
    check(not later, "A 给出坏块后又收到 %d 次块请求；协议要求本次任务内隔离给出错误内容的来源" % len(later))


def sample_failure_protection(lab):
    """The only CDN corrupts every chunk: exit 1, keep the old result.bin, leave no temporary files."""
    old = b"previous result, must survive a failed download\n"
    url, log = lab.proxy("proxy-fail", lab.cdn_a, {"rules": [{"action": "corrupt"}]})
    code, output, _ = lab.download("fail", "fail", [("A", url)], existing=old, download_timeout_s=15)
    check(any(r["fault"] == "corrupt" for r in chunk_rows(log)), "Client 没有向 CDN 请求块，这个样例没有被触发")
    check(code == 1, "所有来源都给出坏数据时应退出 1，实为 %s" % code)
    target = output / "result.bin"
    check(target.is_file() and target.read_bytes() == old, "下载失败后原有 result.bin 被改动或删除")
    leftover = sorted(p.name for p in output.iterdir() if p.name != "result.bin")
    check(not leftover, "失败后输出目录留下了临时文件：%s" % leftover)


SAMPLES = {"download": sample_download, "serial_lru": sample_serial_lru, "multi_source": sample_multi_source,
           "corrupt_recovery": sample_corrupt_recovery, "failure_protection": sample_failure_protection}


def sample_skeleton(lab):
    """Empty starter: services start and answer, every TODO fails explicitly."""
    cdn = lab.cdn_a
    check(get_bytes(cdn, "/health").status == 200, "health")
    check(get_bytes(cdn, "/probe").body == b"OK", "probe")
    check(json.loads(get_bytes(cdn, "/status").body)["active_requests"] == 0, "status")
    check(get_bytes(lab.origin_url, "/chunk/demo/0").status == 403, "Origin chunk 访问边界")
    code, output, log = lab.download("skeleton", "demo", [("A", cdn)])
    check(code == 1, "空骨架必须明确失败，退出 1")
    check(any(e.get("code") == "NOT_IMPLEMENTED" for e in events(log)), "缺少未实现错误")
    check(not (output / "result.bin").exists(), "未实现不得生成假成功文件")
    check(get_bytes(cdn, "/chunk/demo/0").status == 501, "CDN TODO 必须显式失败")


def run(names, skeleton=False):
    """Start Origin and CDN A once, then run each sample; a failed sample does not stop the rest."""
    results = []
    with tempfile.TemporaryDirectory(prefix="cs305-public-") as folder:
        lab = Lab(Path(folder))
        try:
            data = lab.root / "data"
            for file_id, size, chunk in (("demo", 3 * 65536 + 137, 65536), ("lru", 3 * 65536, 65536),
                                         ("multi", 12 * 32768, 32768), ("bad", 12 * 32768, 32768),
                                         ("fail", 3 * 16384, 16384)):
                lab.manifests[file_id] = make_file(data, file_id, size, chunk)
            lab.token = secrets.token_hex(24)
            _, _, lab.origin_url = lab.service("framework.origin", "origin", {
                "host": "127.0.0.1", "port": 0, "data_root": str(data), "chunk_access_token": lab.token,
                "audit_path": str(lab.root / "origin-audit.jsonl")})
            lab.cdn_a = lab.cdn("A")
        except (AssertionError, OSError, ValueError) as exc:
            lab.stop()
            return [{"name": name, "passed": False, "reason": "环境启动失败: " + str(exc)} for name in names]
        try:
            for name in names:
                try:
                    (sample_skeleton if skeleton else SAMPLES[name])(lab)
                    results.append({"name": name, "passed": True})
                except (AssertionError, OSError, ValueError, KeyError, TypeError, subprocess.SubprocessError) as exc:
                    results.append({"name": name, "passed": False, "reason": str(exc) or type(exc).__name__})
        finally:
            lab.stop()
    return results


def main():
    parser = argparse.ArgumentParser(description="运行本地公开功能检查；不会给出原生或正式成绩")
    parser.add_argument("--skeleton", action="store_true", help="只检查空骨架的生命周期")
    parser.add_argument("--only", nargs="+", choices=sorted(SAMPLES), help="只运行这些样例")
    args = parser.parse_args()
    if args.skeleton and args.only:
        parser.error("--skeleton 不能和 --only 同时使用")
    names = ["skeleton"] if args.skeleton else (args.only or list(SAMPLES))
    results = run(names, skeleton=args.skeleton)
    for result in results:
        print(("PASS " if result["passed"] else "FAIL ") + result["name"]
              + ("" if result["passed"] else "：" + result["reason"]), file=sys.stderr)
    passed = bool(results) and all(r["passed"] for r in results)
    print(json.dumps({"passed": passed, "mode": "SKELETON_ONLY" if args.skeleton else "PUBLIC_FUNCTIONAL",
                      "samples": results, "native_docker_netem_validated": False, "course_score": None},
                     ensure_ascii=False))
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
