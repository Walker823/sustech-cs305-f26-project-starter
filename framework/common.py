"""Shared teaching infrastructure: configuration, JSONL events and bounded HTTP I/O.

This is local laboratory infrastructure, not a public Internet web server.
"""
from __future__ import annotations

import hashlib
import http.client
import json
import math
import re
import signal
import socket
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

PROTOCOL_VERSION = "m3-v0.3.1"
MAX_CHUNK_BYTES = 8 * 1024 * 1024
MAX_MANIFEST_BYTES = 8 * 1024 * 1024
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,63}\Z")
_SHA = re.compile(r"[0-9a-f]{64}\Z")


class ProjectError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def valid_id(value: Any) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise ProjectError("INVALID_ID", "ID must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
    return value


def integer(value: Any, name: str, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise ProjectError("INVALID_CONFIG", f"{name} must be an integer in [{low}, {high}]")
    return value


def positive(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ProjectError("INVALID_CONFIG", f"{name} must be a positive finite number")
    result = float(value)
    if not math.isfinite(result) or result <= 0:
        raise ProjectError("INVALID_CONFIG", f"{name} must be a positive finite number")
    return result


def load_config(path: str | Path) -> tuple[dict, Path]:
    path = Path(path).resolve()
    with path.open(encoding="utf-8") as stream:
        config = json.load(stream)
    if not isinstance(config, dict):
        raise ProjectError("INVALID_CONFIG", "configuration must be a JSON object")
    return config, path.parent


def resolve_path(base: Path, path: str) -> Path:
    result = Path(path)
    return (base / result).resolve() if not result.is_absolute() else result.resolve()


def base_url(value: Any) -> str:
    if not isinstance(value, str):
        raise ProjectError("INVALID_CONFIG", "URL must be a string")
    try:
        parsed = urlsplit(value)
        _ = parsed.port
    except ValueError as exc:
        raise ProjectError("INVALID_CONFIG", "invalid URL") from exc
    if (parsed.scheme != "http" or not parsed.hostname or parsed.username or parsed.password
            or parsed.query or parsed.fragment or parsed.path not in ("", "/")):
        raise ProjectError("INVALID_CONFIG", "expected http://host:port without path or credentials")
    return value.rstrip("/")


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def validate_manifest(value: Any, expected_id: str) -> dict:
    if not isinstance(value, dict) or value.get("file_id") != expected_id:
        raise ProjectError("INVALID_MANIFEST", "manifest file_id mismatch")
    try:
        size = integer(value.get("file_size"), "file_size", 0, 1024**4)
        chunk_size = integer(value.get("chunk_size"), "chunk_size", 1, MAX_CHUNK_BYTES)
        chunks = value.get("chunks")
        expected_count = (size + chunk_size - 1) // chunk_size
        if not isinstance(chunks, list) or len(chunks) != expected_count:
            raise ValueError("chunk count mismatch")
        if not isinstance(value.get("file_sha256"), str) or not _SHA.fullmatch(value["file_sha256"]):
            raise ValueError("invalid whole-file SHA256")
        for index, chunk in enumerate(chunks):
            expected_size = min(chunk_size, size - index * chunk_size)
            if (not isinstance(chunk, dict) or type(chunk.get("id")) is not int
                    or chunk["id"] != index or type(chunk.get("size")) is not int
                    or chunk["size"] != expected_size
                    or not isinstance(chunk.get("sha256"), str)
                    or not _SHA.fullmatch(chunk["sha256"])):
                raise ValueError(f"invalid chunk descriptor {index}")
    except (ValueError, ProjectError) as exc:
        raise ProjectError("INVALID_MANIFEST", str(exc)) from exc
    return value


class Events:
    """One JSON object per line; optional teacher-owned audit sink."""
    def __init__(self, role: str, node_id: str, audit_path: Path | None = None):
        self.role, self.node_id = role, node_id
        self.started = time.monotonic()
        self.lock = threading.Lock()
        self.audit = None
        if audit_path is not None:
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            self.audit = audit_path.open("a", encoding="utf-8", buffering=1)

    def emit(self, event: str, **fields: Any) -> None:
        row = {"version": PROTOCOL_VERSION,
               "time_utc": datetime.now(timezone.utc).isoformat(),
               "elapsed_ms": round((time.monotonic() - self.started) * 1000, 3),
               "role": self.role, "node_id": self.node_id, "event": event, **fields}
        line = json.dumps(row, ensure_ascii=False, allow_nan=False, separators=(",", ":"))
        with self.lock:
            if self.audit is not None:
                self.audit.write(line + "\n")
                self.audit.flush()
            print(line, flush=True)

    def close(self) -> None:
        with self.lock:
            if self.audit is not None:
                self.audit.close()
                self.audit = None


@dataclass(frozen=True)
class HTTPResult:
    status: int
    headers: dict[str, str]
    body: bytes
    elapsed_ms: float


def get_bytes(url: str, path: str, *, headers: dict[str, str] | None = None,
              connect_timeout_s: float = 2.0, read_idle_timeout_s: float = 10.0,
              max_bytes: int = MAX_CHUNK_BYTES, body_deadline_s: float = 120.0,
              absolute_deadline: float | None = None,
              stop_event: threading.Event | None = None) -> HTTPResult:
    """Bounded HTTP GET with cancellable headers and a hard body budget.

    Each request owns a socket and (only when needed) one cancellation watcher.
    A socket idle timeout alone cannot bound http.client's header parser: a
    remote can keep supplying individual header bytes. Shutdown interrupts that
    parser and blocked reads without reusing a timed-out buffered stream.
    DNS endpoints in this lab are fixed local/container names; OS resolver
    latency is still separately bounded by the external grader process budget.
    """
    def remaining(default):
        if stop_event is not None and stop_event.is_set():
            raise ProjectError('CANCELLED', 'request cancelled')
        if absolute_deadline is None:
            return default
        left = absolute_deadline - time.monotonic()
        if left <= 0:
            raise ProjectError('DOWNLOAD_DEADLINE', 'download deadline exceeded')
        return min(default, left)

    parsed = urlsplit(base_url(url))
    started = time.monotonic()
    conn = http.client.HTTPConnection(parsed.hostname, parsed.port or 80,
                                      timeout=remaining(connect_timeout_s))
    response = None
    transport_socket = None
    finished = threading.Event()
    watcher = None
    body_deadline = None

    def watch():
        while not finished.is_set():
            cancelled = stop_event is not None and stop_event.is_set()
            expired = absolute_deadline is not None and time.monotonic() >= absolute_deadline
            if cancelled or expired:
                sock = transport_socket
                if sock is not None:
                    try:
                        sock.shutdown(socket.SHUT_RDWR)
                    except OSError:
                        pass
                return
            pause = 0.02
            if absolute_deadline is not None:
                pause = min(pause, max(0.001, absolute_deadline - time.monotonic()))
            finished.wait(pause)

    def body_remaining():
        budget = remaining(read_idle_timeout_s)
        left = body_deadline - time.monotonic()
        if left <= 0:
            raise ProjectError('BODY_DEADLINE', 'response body exceeded transfer deadline', 504)
        return min(budget, left)

    try:
        conn.connect()
        assert conn.sock is not None
        transport_socket = conn.sock
        transport_socket.settimeout(remaining(read_idle_timeout_s))
        if absolute_deadline is not None or stop_event is not None:
            watcher = threading.Thread(target=watch, name='http-cancel-watch', daemon=True)
            watcher.start()
        conn.request('GET', path, headers={'Connection': 'close', **(headers or {})})
        response = conn.getresponse()
        remaining(read_idle_timeout_s)
        items = response.getheaders()
        lengths = [value for key, value in items if key.lower() == 'content-length']
        response_headers = {key.lower(): value for key, value in items}
        # Teaching protocol deliberately permits exactly one fixed length.
        # Reject duplicate/ambiguous framing instead of allowing parser disagreement.
        if (response_headers.get('transfer-encoding') or len(lengths) != 1
                or not lengths[0].isascii() or not lengths[0].isdigit()):
            raise ProjectError('BAD_RESPONSE', 'exactly one Content-Length required; transfer encoding unsupported', 502)
        if len(lengths[0]) > 20:
            raise ProjectError('RESPONSE_TOO_LARGE', 'Content-Length exceeds configured bound', 502)
        length = int(lengths[0])
        if length > max_bytes:
            raise ProjectError('RESPONSE_TOO_LARGE', 'response exceeds configured bound', 502)
        body = bytearray()
        body_deadline = time.monotonic() + body_deadline_s
        while len(body) < length:
            # A blocking read must itself be capped by both budgets, not just
            # checked before the operation; the last read may otherwise overrun.
            transport_socket.settimeout(body_remaining())
            part = response.read1(min(65536, length - len(body)))
            body_remaining()
            if not part:
                raise ProjectError('TRUNCATED_RESPONSE', 'body shorter than Content-Length', 502)
            body.extend(part)
        remaining(read_idle_timeout_s)
        return HTTPResult(response.status, response_headers, bytes(body),
                          (time.monotonic() - started) * 1000)
    except ProjectError:
        remaining(read_idle_timeout_s)
        raise
    except (socket.timeout, TimeoutError) as exc:
        remaining(read_idle_timeout_s)
        if body_deadline is not None and time.monotonic() >= body_deadline:
            raise ProjectError('BODY_DEADLINE', 'response body exceeded transfer deadline', 504) from exc
        raise ProjectError('NETWORK_TIMEOUT', 'connection or blocking read timed out', 504) from exc
    except (OSError, http.client.HTTPException) as exc:
        remaining(read_idle_timeout_s)
        raise ProjectError('NETWORK_ERROR', f'HTTP transport failed: {type(exc).__name__}', 502) from exc
    finally:
        finished.set()
        if watcher is not None:
            watcher.join()
        if response is not None:
            response.close()
        conn.close()


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "CS305-Lab"
    sys_version = ""

    def setup(self) -> None:
        super().setup()
        self.connection.settimeout(10)

    def log_message(self, fmt: str, *args: Any) -> None:
        # stdout is exclusively JSONL; avoid mixing access text with events.
        return

    def request_id(self) -> str:
        value = self.headers.get("X-Request-ID", "")
        return value if _ID.fullmatch(value) else uuid.uuid4().hex

    def send_body(self, status: int, body: bytes, content_type: str = "application/octet-stream",
                  extra: dict[str, str] | None = None) -> int:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.send_header("Cache-Control", "no-store")
        for name, value in (extra or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.close_connection = True
        if self.command != "HEAD":
            self.wfile.write(body)
            self.wfile.flush()
            return len(body)
        return 0

    def send_json(self, status: int, value: dict, extra: dict[str, str] | None = None) -> None:
        self.send_body(status, json.dumps(value, ensure_ascii=False).encode("utf-8"),
                       "application/json; charset=utf-8", extra)

    def failure(self, exc: ProjectError, request_id: str) -> None:
        self.send_json(exc.status, {"error": {"code": exc.code, "message": exc.message},
                                    "request_id": request_id})

    def do_GET(self) -> None:
        request_id = self.request_id()
        try:
            parsed = urlsplit(self.path)
            if parsed.query or parsed.fragment or "%" in parsed.path:
                raise ProjectError("INVALID_PATH", "queries and encoded path components are unsupported")
            self.route(parsed.path, request_id)
        except ProjectError as exc:
            try:
                self.failure(exc, request_id)
            except (OSError, TimeoutError):
                pass
        except (OSError, TimeoutError):
            # A disconnected caller must not crash the service.
            return
        except Exception as exc:
            print(f"internal error: {type(exc).__name__}: {exc}", file=sys.stderr, flush=True)
            try:
                self.failure(ProjectError("INTERNAL_ERROR", "internal service error", 500), request_id)
            except OSError:
                pass

    def route(self, path: str, request_id: str) -> None:
        raise NotImplementedError

    def do_POST(self) -> None:
        self.failure(ProjectError("METHOD_NOT_ALLOWED", "only GET is supported", 405), self.request_id())

    do_PUT = do_POST
    do_DELETE = do_POST
    do_PATCH = do_POST
    do_HEAD = do_POST
    do_OPTIONS = do_POST


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 32


def serve(server: Server, events: Events) -> None:
    """shutdown() runs outside serve_forever's thread to avoid deadlock."""
    def stop(_signum: int, _frame: Any) -> None:
        threading.Thread(target=server.shutdown, daemon=True).start()
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    host, port = server.server_address[:2]
    events.emit("ready", host=host, port=port)
    try:
        server.serve_forever(poll_interval=0.05)
    finally:
        server.server_close()
        events.emit("stopped")
        events.close()
