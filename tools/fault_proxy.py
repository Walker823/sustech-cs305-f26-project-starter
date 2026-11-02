"""Local fault-injection proxy for self-testing. Not used by grading.

Put it between your Client and a CDN (or between your CDN and the Origin):

    python3 fault_proxy.py --upstream http://127.0.0.1:9001 --faults faults.json --log proxy-a.jsonl

It prints one `ready` JSON line with its port; point the Client's `cdns[].url` at it.
Every request is forwarded unchanged unless a rule matches. faults.json (all keys optional):

    {"latency_ms": 20, "rate_mbps": 4,
     "rules": [{"action": "corrupt", "count": 1, "file": "demo", "chunk": 2},
               {"action": "truncate", "after_bytes": 1000},
               {"action": "stall", "seconds": 5},
               {"action": "status", "status": 503, "retry_after": 1},
               {"action": "reset"}]}

- latency_ms: delay before each response; rate_mbps: one shared bandwidth limit for all
  transfers through this proxy, so concurrent transfers split it like a real link.
- Rules apply to /chunk/ requests; "file" and "chunk" narrow them. Add "all_paths": true to
  a stall/status/reset rule to hit /probe, /status and /health too (e.g. a dead node).
- "count" is how many requests a rule affects; leave it out for every request. The first
  rule with uses left wins. corrupt/truncate only act on 200 responses.
- corrupt flips one body byte and keeps the upstream headers, including X-Chunk-SHA256.
  truncate sends the full Content-Length but closes after `after_bytes`. stall waits before
  forwarding. status answers locally. reset closes the connection without a response.
- The file is re-read on every request: edit it (write a new file, then rename it over the
  old one) to change faults during a download. Counters restart whenever its content changes.

Each request appends a JSON line to --log (or stderr): path, fault, status, bytes_sent and
wall-clock times `received`, `body_start`, `body_end` (just before the last write) and `end`.
Compare two proxies' logs to see whether bodies really overlapped. The defaults here are not
the grading parameters. Binds to 127.0.0.1 by default and has no authentication.
"""
from __future__ import annotations
import argparse
import json
import math
from pathlib import Path
import select
import socket
import struct
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # package root; this script lives in tools/
from framework.common import ProjectError, base_url, get_bytes  # noqa: E402

ACTIONS = {"corrupt": set(), "truncate": {"after_bytes"}, "stall": {"seconds", "all_paths"},
           "status": {"status", "retry_after", "all_paths"}, "reset": {"all_paths"}}
HOP = {"connection", "keep-alive", "proxy-authenticate", "proxy-authorization", "te", "trailer",
       "transfer-encoding", "upgrade", "host", "content-length", "date", "server"}
SLICE = 4096


def number(value, name, *, zero_ok=True):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{name} must be a finite number")
    if value < 0 or (value == 0 and not zero_ok):
        raise ValueError(f"{name} must be {'non-negative' if zero_ok else 'positive'}")
    return float(value)


def parse_faults(value):
    """Validate faults.json; returns (latency_s, rate_bytes_per_s or None, rules)."""
    if not isinstance(value, dict) or set(value) - {"latency_ms", "rate_mbps", "rules"}:
        raise ValueError("faults file must be an object with latency_ms, rate_mbps, rules")
    latency = number(value.get("latency_ms", 0), "latency_ms") / 1000
    rate = value.get("rate_mbps")
    rate = None if rate is None else number(rate, "rate_mbps", zero_ok=False) * 1e6 / 8
    rules = value.get("rules", [])
    if not isinstance(rules, list) or len(rules) > 64:
        raise ValueError("rules must be a list of at most 64 rules")
    result = []
    for index, rule in enumerate(rules):
        where = f"rules[{index}]"
        if not isinstance(rule, dict) or rule.get("action") not in ACTIONS:
            raise ValueError(f"{where}.action must be one of {sorted(ACTIONS)}")
        extra = set(rule) - {"action", "count", "file", "chunk"} - ACTIONS[rule["action"]]
        if extra:
            raise ValueError(f"{where} has unknown keys {sorted(extra)}")
        count = rule.get("count")
        if count is not None and (type(count) is not int or count < 0):
            raise ValueError(f"{where}.count must be a non-negative integer")
        if "file" in rule and not isinstance(rule["file"], str):
            raise ValueError(f"{where}.file must be a string")
        if "chunk" in rule and (type(rule["chunk"]) is not int or rule["chunk"] < 0):
            raise ValueError(f"{where}.chunk must be a non-negative integer")
        if rule["action"] == "truncate" and (type(rule.get("after_bytes")) is not int or rule["after_bytes"] < 0):
            raise ValueError(f"{where}.after_bytes must be a non-negative integer")
        if rule["action"] == "stall":
            number(rule.get("seconds"), f"{where}.seconds")
        if rule["action"] == "status":
            if type(rule.get("status")) is not int or not 400 <= rule["status"] <= 599:
                raise ValueError(f"{where}.status must be an integer in [400, 599]")
            retry = rule.get("retry_after")
            if retry is not None and not ((type(retry) is int and retry >= 0) or (
                    isinstance(retry, str) and len(retry) <= 32 and retry.isprintable())):
                raise ValueError(f"{where}.retry_after must be a non-negative integer or a short string")
        result.append(dict(rule))
    return latency, rate, result


class Link:
    """Token bucket shared by every transfer through one proxy (about 20 ms of burst)."""

    def __init__(self):
        self.lock = threading.Lock()
        self.tokens = 0.0
        self.last = time.monotonic()

    def take(self, size, rate, stop):
        with self.lock:
            while not stop.is_set():
                now = time.monotonic()
                burst = max(rate * 0.02, SLICE)
                self.tokens = min(burst, self.tokens + (now - self.last) * rate)
                self.last = now
                if self.tokens >= size:
                    self.tokens -= size
                    return
                stop.wait((size - self.tokens) / rate)


class Proxy(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address, upstream, faults_path=None, log_path=None):
        self.upstream = base_url(upstream)
        self.faults_path = Path(faults_path) if faults_path else None
        self.log_lock = threading.Lock()
        self.log_stream = open(log_path, "a", encoding="utf-8", buffering=1) if log_path else None
        self.lock = threading.Lock()
        self.stop = threading.Event()
        self.link = Link()
        self.raw, self.generation = None, 0
        self.latency, self.rate, self.rules, self.left = 0.0, None, [], []
        if self.faults_path is not None:
            self.reload(strict=True)
        super().__init__(address, ProxyHandler)

    def reload(self, strict=False):
        """Re-read the faults file; an invalid edit keeps the previous rules."""
        if self.faults_path is None:
            return
        try:
            raw = self.faults_path.read_bytes() if self.faults_path.exists() else b""
        except OSError:
            return
        with self.lock:
            if raw == self.raw:
                return
        try:
            if len(raw) > 65536:
                raise ValueError("faults file is larger than 64 KiB")
            parsed = parse_faults(json.loads(raw)) if raw.strip() else (0.0, None, [])
        except ValueError as exc:
            if strict:
                raise
            with self.lock:
                self.raw = raw
            self.write({"event": "faults_invalid", "reason": str(exc), "time": now()})
            return
        with self.lock:
            self.raw = raw
            self.latency, self.rate, self.rules = parsed
            self.left = [r.get("count", -1) for r in self.rules]
            self.generation += 1
        self.write({"event": "faults_loaded", "rules": len(parsed[2]), "time": now()})

    def pick(self, path):
        """First matching rule with uses left. stall/status/reset are used up here;
        corrupt/truncate only when they actually change a 200 response (see consume)."""
        parts = path.split("?", 1)[0].split("/")
        chunk = None
        if len(parts) == 4 and parts[1] == "chunk" and parts[3].isascii() and parts[3].isdigit():
            chunk = (parts[2], int(parts[3]))
        with self.lock:
            for index, rule in enumerate(self.rules):
                if self.left[index] == 0:
                    continue
                if chunk is None:
                    if not rule.get("all_paths") or "file" in rule or "chunk" in rule:
                        continue
                elif rule.get("file", chunk[0]) != chunk[0] or rule.get("chunk", chunk[1]) != chunk[1]:
                    continue
                if rule["action"] in ("stall", "status", "reset") and self.left[index] > 0:
                    self.left[index] -= 1
                return rule, self.generation, index
        return None, self.generation, None

    def consume(self, generation, index):
        with self.lock:
            if generation != self.generation or self.left[index] == 0:
                return False
            if self.left[index] > 0:
                self.left[index] -= 1
            return True

    def write(self, row):
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":"))
        with self.log_lock:
            if self.log_stream is not None:
                self.log_stream.write(line + "\n")
            else:
                print(line, file=sys.stderr, flush=True)

    def server_close(self):
        super().server_close()
        with self.log_lock:
            if self.log_stream is not None:
                self.log_stream.close()
                self.log_stream = None


def now():
    return round(time.time(), 6)


class ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "CS305-FaultProxy"
    sys_version = ""
    timeout = 30

    def log_message(self, fmt, *args):
        return  # every request is logged as one JSON line instead

    def do_GET(self):
        proxy = self.server
        proxy.reload()
        self.row = {"event": "request", "path": self.path[:512],
                    "request_id": (self.headers.get("X-Request-ID") or "")[:128] or None,
                    "fault": None, "received": now()}
        rule, generation, index = proxy.pick(self.path)
        try:
            self.row["outcome"] = self.serve(rule, generation, index)
        except (OSError, TimeoutError):
            self.row["outcome"] = "client_disconnected"
        finally:
            self.row["end"] = now()
            proxy.write(self.row)

    def serve(self, rule, generation, index):
        proxy, row = self.server, self.row
        action = rule["action"] if rule else None
        if action == "reset":
            row["fault"] = "reset"
            self.reset()
            return "reset"
        if action == "stall":
            row["fault"] = "stall"
            if not self.wait(rule["seconds"]):
                return "client_gave_up"
        if action == "status":
            row["fault"] = "status"
            body = json.dumps({"error": {"code": "INJECTED_FAULT", "message": "fault_proxy status rule"},
                               "request_id": row["request_id"] or ""}).encode()
            extra = {"Content-Type": "application/json; charset=utf-8"}
            if rule.get("retry_after") is not None:
                extra["Retry-After"] = str(rule["retry_after"])
            return self.send(rule["status"], extra, body)
        headers = {k: v for k, v in self.headers.items() if k.lower() not in HOP}
        try:
            result = get_bytes(proxy.upstream, self.path, headers=headers, connect_timeout_s=3,
                               read_idle_timeout_s=30, body_deadline_s=120, stop_event=proxy.stop)
        except ProjectError as exc:
            row["upstream_error"] = exc.code  # e.g. the CDN behind this proxy has exited
            self.reset()
            return "upstream_failed"
        body, cut = result.body, None
        if action in ("corrupt", "truncate") and result.status == 200 and body and proxy.consume(generation, index):
            row["fault"] = action
            if action == "corrupt":
                middle = len(body) // 2
                body = body[:middle] + bytes([body[middle] ^ 0xFF]) + body[middle + 1:]
            else:
                cut = min(rule["after_bytes"], len(body) - 1)
        extra = {k: v for k, v in result.headers.items() if k.lower() not in HOP}
        return self.send(result.status, extra, body, cut)

    def send(self, status, headers, body, cut=None):
        """Latency, then headers, then the body paced by the shared link."""
        proxy, row = self.server, self.row
        if proxy.latency and not self.wait(proxy.latency):
            return "client_gave_up"
        self.send_response(status)
        for name, value in headers.items():
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        row["status"], row["bytes_sent"] = status, 0
        payload = body if cut is None else body[:cut]
        step = SLICE if proxy.rate else max(len(payload), 1)
        row["body_start"] = now()
        for offset in range(0, len(payload), step):
            piece = payload[offset:offset + step]
            if proxy.rate:
                proxy.link.take(len(piece), proxy.rate, proxy.stop)
            if offset + step >= len(payload):
                row["body_end"] = now()
            self.wfile.write(piece)
            row["bytes_sent"] += len(piece)
        row.setdefault("body_end", row["body_start"])
        return "truncated" if cut is not None else "sent"

    def reset(self):
        """Close at once with SO_LINGER 0, so the peer sees a reset and no response."""
        try:
            fmt = "HH" if sys.platform == "win32" else "ii"
            self.connection.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack(fmt, 1, 0))
            self.connection.close()
        except OSError:
            pass
        self.close_connection = True

    def wait(self, seconds):
        """Sleep, but return False early if the client closes the connection."""
        deadline = time.monotonic() + seconds
        while not self.server.stop.is_set():
            left = deadline - time.monotonic()
            if left <= 0:
                return True
            readable, _, _ = select.select([self.connection], [], [], min(left, 0.05))
            if readable:
                try:
                    if self.connection.recv(1, socket.MSG_PEEK) == b"":
                        return False
                except OSError:
                    return False
                self.server.stop.wait(min(left, 0.05))
        return False

    def refuse(self):
        body = b'{"error":{"code":"METHOD_NOT_ALLOWED","message":"only GET is supported"}}'
        self.send_response(405)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            self.wfile.write(body)

    do_POST = do_PUT = do_DELETE = do_PATCH = do_HEAD = do_OPTIONS = refuse


def main():
    p = argparse.ArgumentParser(description="本地故障注入代理，用于自测；不参与评分 / local fault proxy for self-testing")
    p.add_argument("--upstream", required=True, help="CDN or Origin URL, e.g. http://127.0.0.1:9001")
    p.add_argument("--faults", help="faults.json, re-read on every request")
    p.add_argument("--log", help="append one JSON line per request (default: stderr)")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=0)
    a = p.parse_args()
    try:
        proxy = Proxy((a.host, a.port), a.upstream, a.faults, a.log)
    except (OSError, ValueError, ProjectError) as exc:
        print(json.dumps({"event": "error", "message": str(exc)}), flush=True)
        return 2
    host, port = proxy.server_address[:2]
    print(json.dumps({"event": "ready", "role": "fault_proxy", "host": host, "port": port,
                      "upstream": proxy.upstream}), flush=True)
    try:
        proxy.serve_forever(poll_interval=0.2)
    except KeyboardInterrupt:
        pass
    finally:
        proxy.stop.set()
        proxy.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
