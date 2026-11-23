"""Verify that a full recovery slot does not bypass the untried CDN."""
from contextlib import ExitStack
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time

from extended_tests import ROOT, require


CHUNKS = [bytes([letter]) * 32768 for letter in b"ABC"]
FILE = b"".join(CHUNKS)
MANIFEST = {
    "file_id": "retry", "file_size": len(FILE), "chunk_size": len(CHUNKS[0]),
    "file_sha256": hashlib.sha256(FILE).hexdigest(),
    "chunks": [{"id": index, "size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
               for index, data in enumerate(CHUNKS)],
}


class Gateway(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        node = self.server.node
        status = 200
        if node == "origin":
            require(self.path == "/manifest/retry", "unexpected Origin endpoint")
            body = json.dumps(MANIFEST).encode()
        elif self.path == "/probe":
            body = b"OK"
        elif self.path == "/status":
            body = json.dumps({"cdn_id": node, "active_requests": 0,
                               "max_active_requests": 8, "queue_length": 0}).encode()
        else:
            chunk = int(self.path.rsplit("/", 1)[-1])
            with self.server.traffic_lock:
                rows = self.server.traffic[chunk]
                first = not rows
                record = {"node": node, "start": time.monotonic()}
                rows.append(record)
            if first and chunk in (0, 1):
                if chunk == 0:
                    time.sleep(0.25)
                status, body = 503, b""
            else:
                if chunk == 2:
                    time.sleep(0.95)
                body = CHUNKS[chunk]
            record.update(end=time.monotonic(), status=status)
        self.send_response(status)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def launch(stack, node, traffic, lock):
    server = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), Gateway))
    server.node = node
    server.traffic = traffic
    server.traffic_lock = lock
    threading.Thread(target=server.serve_forever, daemon=True).start()
    stack.callback(server.shutdown)
    return f"http://127.0.0.1:{server.server_port}"


def main():
    traffic = [[] for _ in CHUNKS]
    lock = threading.Lock()
    with tempfile.TemporaryDirectory() as folder, ExitStack() as stack:
        root = Path(folder)
        urls = {node: launch(stack, node, traffic, lock) for node in ("origin", "north", "south")}
        config = {
            "origin_url": urls["origin"],
            "cdns": [{"id": node, "url": urls[node]} for node in ("north", "south")],
            "output_root": str(root / "output"),
            "max_inflight_chunks": 2, "max_inflight_per_cdn": 1, "max_attempts_per_chunk": 2,
            "node_cooldown_s": 0.12, "node_cooldown_max_s": 0.12,
            "retry_backoff_s": 0.02, "retry_backoff_max_s": 0.02,
            "download_timeout_s": 8,
        }
        config_path = root / "client.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        run = subprocess.run([sys.executable, "-m", "student.client", "--config", str(config_path),
                              "--download", "retry", "--output", "result.bin"],
                             cwd=ROOT, capture_output=True, text=True, timeout=10)
        require(run.returncode == 0, run.stdout + run.stderr)
        require((root / "output/result.bin").read_bytes() == FILE, "wrong output")

    require([len(rows) for rows in traffic] == [2, 2, 1], "unexpected attempt counts")
    first, retry = traffic[0]
    alternate = traffic[1][0]
    trial = traffic[2][0]
    require(first["status"] == alternate["status"] == 503 and trial["status"] == 200
            and trial["node"] == alternate["node"] != first["node"]
            and trial["start"] < first["end"] < trial["end"], "recovery-slot scenario did not trigger")
    require(retry["node"] == trial["node"] and retry["start"] >= trial["end"],
            "retried the failed CDN while the untried CDN's trial occupied its slot")
    print("PASS retry_slots: waited for the untried CDN's trial and reassigned with two attempts")


if __name__ == "__main__":
    main()
