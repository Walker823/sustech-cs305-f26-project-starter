"""Run the real CDN under Docker quotas with 8 MiB chunks and slow readers."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
import hashlib
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
from urllib.parse import urlsplit

from extended_tests import ROOT, require
from framework.common import ProjectError
from resource_checks import ContainerLab, witness
from student.cache import ChunkBody, ChunkMemory, LRUCache


MIB = 1024 * 1024
CHUNK = b"Z" * (8 * MIB)
DIGEST = hashlib.sha256(CHUNK).hexdigest()
TOKEN = "local-cache-pressure"


class Origin(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        parts = self.path.strip("/").split("/")
        if len(parts) == 2 and parts[0] == "manifest":
            body = json.dumps({
                "file_id": parts[1], "file_size": len(CHUNK), "chunk_size": len(CHUNK),
                "file_sha256": DIGEST,
                "chunks": [{"id": 0, "size": len(CHUNK), "sha256": DIGEST}],
            }).encode()
        elif (len(parts) == 3 and parts[0] == "chunk" and parts[2] == "0"
              and self.headers.get("X-Origin-Token") == TOKEN):
            body = CHUNK
        else:
            self.send_error(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass


@contextmanager
def origin():
    with ThreadingHTTPServer(("127.0.0.1", 0), Origin) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield f"http://127.0.0.1:{server.server_port}"
        finally:
            server.shutdown()
            thread.join()


@contextmanager
def get(port, path, *, slow=False):
    conn = HTTPConnection("127.0.0.1", port, timeout=8)
    try:
        if slow:
            conn.connect()
            conn.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
        conn.request("GET", path)
        with conn.getresponse() as response:
            require(response.status == 200, f"{path}: HTTP {response.status}")
            yield response
    finally:
        conn.close()


def check_body(response, cache_status):
    require(response.getheader("X-Cache") == cache_status, "wrong X-Cache")
    require(response.getheader("X-Chunk-SHA256") == DIGEST, "wrong chunk digest header")
    digest = hashlib.sha256()
    size = 0
    while part := response.read(65536):
        size += len(part)
        digest.update(part)
    require(size == len(CHUNK) and digest.hexdigest() == DIGEST, "wrong chunk body")


def check_reservation():
    # The first chunk is evicted from the LRU but is still in a caller's hands.
    memory = ChunkMemory(limit=3 * MIB // 2)
    cache = LRUCache(28, lock=memory.condition)
    data = b"x" * (MIB // 2)
    ticket = memory.reserve(len(data), cache, [])
    pinned = ChunkBody(data, ticket)
    ticket.retain_body(len(data))
    cache.put(("pinned", 0), pinned)
    evicted = []
    try:
        borrowed = memory.reserve(len(data), cache, evicted)
    except ProjectError as exc:
        require(exc.status == 503, "pinned overload must be retryable")
    else:
        borrowed.release()
        raise AssertionError("pinned bytes stopped counting too early")
    require(evicted == [("pinned", 0)] and cache.get(("pinned", 0)) is None
            and memory.bytes_used >= len(data), "evicted response is still charged")
    del pinned
    require(memory.bytes_used == 0, "finished response did not release its bytes")
    memory.reserve(len(data), cache, []).release()


def check_http(port):
    def request(file_id, cache_status="MISS"):
        with get(port, f"/chunk/{file_id}/0") as response:
            check_body(response, cache_status)

    for index in range(40):
        request(f"serial-{index}")
    request("serial-39", "HIT")

    with get(port, "/chunk/pinned/0", slow=True) as slow:
        with get(port, "/status") as response:
            require(json.load(response)["active_requests"] == 1, "slow response did not pin data")
        for index in range(22):
            request(f"pressure-{index}")
        request("pinned")  # Evicted while the original connection is still sending.
        check_body(slow, "MISS")

    barrier = threading.Barrier(8, timeout=5)

    def simultaneous(index):
        barrier.wait()
        request(f"parallel-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(simultaneous, range(8)))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--image", default="cs305-optimized")
    parser.add_argument("--output", type=Path, default=ROOT / "runtime/cache-pressure.json")
    args = parser.parse_args()
    check_reservation()
    with tempfile.TemporaryDirectory() as folder, origin() as upstream:
        lab = ContainerLab(Path(folder), args.image)
        try:
            process, log, url = lab.service("student.cdn", "cdn-pressure", {
                "cdn_id": "pressure", "origin_url": upstream, "origin_chunk_token": TOKEN,
                "cache_capacity_chunks": 28, "max_active_requests": 8,
            })
            check_http(urlsplit(url).port)
            require(process.poll() is None, "CDN did not survive memory pressure")
            subprocess.run(["docker", "stop", "--time", "3", lab.containers[-1]],
                           check=True, capture_output=True, text=True)
            process.wait(timeout=5)
            limits = witness(log)
            require(process.returncode == limits["exit_code"] == 0, "CDN did not shut down cleanly")
        finally:
            lab.stop()
    report = {"passed": True, "serial_requests": 40, "parallel_requests": 8,
              "chunk_bytes": len(CHUNK), "cache_capacity_chunks": 28, "container": limits}
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("PASS cache_pressure: serial, concurrent, pinned eviction, retryable overload")
    print(json.dumps(report))


if __name__ == "__main__":
    main()
