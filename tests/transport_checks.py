"""Adversarial HTTP framing, deadlines and 32-request cancellation checks."""
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, redirect_stdout
import io
import json
import socketserver
import threading
import time

from extended_tests import download, events, laboratory, require, terminal_events
from framework.common import ProjectError
from student.cdn import CDN
from student.transport import HTTPTransport


class Handler(socketserver.BaseRequestHandler):
    def handle(self):
        try:
            data = b""
            self.request.settimeout(3)
            while b"\r\n\r\n" not in data:
                data += self.request.recv(4096)
            path = data.split(b" ")[1].decode()
            if path.startswith("/manifest/") and hasattr(self.server, "manifest"):
                body = json.dumps(self.server.manifest).encode()
                self.request.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: "
                                     + str(len(body)).encode() + b"\r\n\r\n" + body)
                return
            if path.startswith("/chunk/"):
                path = "/wrong-length"
            cases = {
                "/good": b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata",
                "/duplicate": b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nContent-Length: 4\r\n\r\ndata",
                "/missing": b"HTTP/1.1 200 OK\r\n\r\ndata",
                "/te": b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nTransfer-Encoding: chunked\r\n\r\ndata",
                "/te-empty": b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\nTransfer-Encoding: \r\n\r\ndata",
                "/huge": b"HTTP/1.1 200 OK\r\nContent-Length: 999999999\r\n\r\n",
                "/short": b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\nabc",
                "/redirect": b"HTTP/1.1 302 Found\r\nContent-Length: 0\r\nLocation: http://127.0.0.1:1/\r\n\r\n",
            }
            if path in cases:
                self.request.sendall(cases[path])
            elif path == "/headers":
                for byte in b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\ndata":
                    self.request.sendall(bytes([byte]))
                    time.sleep(0.025)
            elif path == "/drip":
                self.request.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 100000\r\n\r\n")
                for _ in range(100):
                    self.request.sendall(b"a" * 1024)
                    time.sleep(0.02)
            elif path in ("/hold", "/wrong-length", "/busy"):
                if path == "/hold":
                    header = b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n"
                elif path == "/wrong-length":
                    header = b"HTTP/1.1 200 OK\r\nContent-Length: 8388608\r\n\r\n"
                else:
                    header = b"HTTP/1.1 503 Busy\r\nContent-Length: 8388608\r\nRetry-After: 0.35\r\n\r\n"
                self.request.sendall(header)
                with self.server.condition:
                    self.server.arrivals += 1
                    self.server.condition.notify_all()
                self.server.release.wait(4)
                self.request.sendall(b"data")
        except OSError:
            pass


class Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True
    request_queue_size = 128


@contextmanager
def endpoint():
    server = Server(("127.0.0.1", 0), Handler)
    server.release = threading.Event()
    server.condition = threading.Condition()
    server.arrivals = 0
    thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.02})
    thread.start()
    try:
        yield server, "http://127.0.0.1:" + str(server.server_address[1])
    finally:
        server.release.set()
        server.shutdown()
        server.server_close()
        thread.join()


def error(transport, url, path, expected, **settings):
    try:
        transport.get_bytes(url, path, **settings)
    except ProjectError as exc:
        require(exc.code == expected, f"{path}: expected {expected}, got {exc.code}")
    else:
        raise AssertionError(path + " unexpectedly succeeded")


def main():
    with endpoint() as (_, url):
        transport = HTTPTransport()
        try:
            require(transport.get_bytes(url, "/good").body == b"data", "ordinary response")
            require(transport.get_bytes(url, "/good", expected_length=4).body == b"data",
                    "known-length ordinary response")
            error(transport, url, "/good", "CHUNK_LENGTH_MISMATCH", expected_length=5)
            require(transport.get_bytes(url, "/redirect").status == 302, "redirect must not be followed")
            for path in ("/duplicate", "/missing", "/te", "/te-empty"):
                error(transport, url, path, "BAD_RESPONSE")
            error(transport, url, "/huge", "RESPONSE_TOO_LARGE")
            error(transport, url, "/short", "TRUNCATED_RESPONSE")
            start = time.monotonic()
            error(transport, url, "/headers", "DOWNLOAD_DEADLINE", read_idle_timeout_s=0.1,
                  absolute_deadline=start + 0.15)
            require(time.monotonic() - start < 0.4, "slow headers ignored absolute deadline")
            error(transport, url, "/drip", "BODY_DEADLINE", read_idle_timeout_s=0.1,
                  body_deadline_s=0.14, absolute_deadline=time.monotonic() + 2)
        finally:
            transport.close()

    with endpoint() as (server, url):
        transport = HTTPTransport()
        try:
            start = time.monotonic()
            with ThreadPoolExecutor(max_workers=32) as pool:
                futures = [pool.submit(error, transport, url, "/wrong-length", "CHUNK_LENGTH_MISMATCH",
                                       expected_length=32768, read_idle_timeout_s=3)
                           for _ in range(32)]
                for future in futures:
                    future.result(timeout=2)
            require(server.arrivals == 32 and time.monotonic() - start < 1.5,
                    "mismatched 8 MiB headers were buffered or read before rejection")
            start = time.monotonic()
            response = transport.get_bytes(url, "/busy", expected_length=1, read_error_body=False,
                                           read_idle_timeout_s=3)
            require(response.status == 503 and response.body == b""
                    and response.headers["retry-after"] == "0.35" and time.monotonic() - start < 0.5,
                    "error body stalled, or HTTP error was mistaken for a bad chunk")
        finally:
            transport.close()

    with laboratory() as lab, endpoint() as (server, url):
        old = b"keep old output"
        code, output, log, elapsed = download(lab, "wrong-length", "demo", [("bad", url)],
                                               existing=old, max_inflight_chunks=1,
                                               max_inflight_per_cdn=1, read_idle_timeout_s=3)
        require(code == 1 and elapsed < 1.5 and terminal_events(log, False)["code"] == "NO_VALID_SOURCE",
                "client failed to quarantine a wrong Content-Length before reading body")
        require(any(row.get("code") == "CHUNK_LENGTH_MISMATCH" for row in events(log))
                and (output / "result.bin").read_bytes() == old, "length guard or old output protection")
        server.manifest = lab.manifests["demo"]
        with redirect_stdout(io.StringIO()):
            node = CDN({"cdn_id": "length", "origin_url": url,
                        "origin_chunk_token": lab.token, "cache_capacity_chunks": 2})
            try:
                for _ in range(2):
                    try:
                        node.get_chunk("demo", 0, "bad-origin-length")
                    except ProjectError as exc:
                        require(exc.code == "UPSTREAM_INTEGRITY_ERROR", "origin length error mapping")
                    else:
                        raise AssertionError("CDN accepted an Origin chunk with wrong length")
                require(len(node.cache) == 0, "wrong-length Origin bytes were cached")
            finally:
                node.events.close()

    for cancel in (False, True):
        with endpoint() as (server, url):
            transport = HTTPTransport()
            with ThreadPoolExecutor(max_workers=32) as pool:
                deadline = time.monotonic() + 5
                futures = [pool.submit(transport.get_bytes, url, "/hold", connect_timeout_s=2,
                                       read_idle_timeout_s=3, absolute_deadline=deadline)
                           for _ in range(32)]
                with server.condition:
                    require(server.condition.wait_for(lambda: server.arrivals == 32, timeout=3),
                            "32 requests did not reach the fixture")
                require(transport.peak_active == 32, "32-request transport capacity")
                watchers = [thread for thread in threading.enumerate() if thread.name == "socket-supervisor"]
                require(len(watchers) == 1, "per-request cancellation threads were created")
                if cancel:
                    start = time.monotonic()
                    transport.close()
                    for future in futures:
                        try:
                            future.result(timeout=1)
                        except ProjectError as exc:
                            require(exc.code == "CANCELLED", "close cancellation error")
                        else:
                            raise AssertionError("cancelled request succeeded")
                    require(time.monotonic() - start < 0.5, "shared cancellation was not prompt")
                else:
                    server.release.set()
                    require(all(future.result().body == b"data" for future in futures), "32 response bodies")
                    transport.close()
    print("PASS transport: framing; early expected-length rejection; bounded error bodies; redirect; truncation; drip/header deadlines; 32 requests; one supervisor; cancellation")


if __name__ == "__main__":
    main()
