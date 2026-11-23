"""Teaching HTTP contract with one cancellation supervisor per task/service.

The framing/timeouts match framework.common.get_bytes. A shared supervisor
replaces its per-request watcher, allowing 32 downloads within the PID budget.
No connection pooling, redirects, transfer encoding or third-party libraries.
"""
import http.client
import errno
import os
import select
import socket
import threading
import time
from urllib.parse import urlsplit

from framework.common import HTTPResult, MAX_CHUNK_BYTES, ProjectError, base_url


class _Connection(http.client.HTTPConnection):
    def __init__(self, host, port, connector):
        super().__init__(host, port)
        self._connector = connector

    def connect(self):
        self.sock = self._connector(self.host, self.port)


class HTTPTransport:
    def __init__(self):
        self._condition = threading.Condition()
        self._requests = {}
        self._thread = None
        self._closed = False
        self.peak_active = 0

    def _connect(self, host, port, timeout, remaining):
        # Polling nonblocking connect also makes cancellation prompt while a
        # SYN is being dropped. DNS remains an OS resolver call, as documented
        # by the course; its elapsed time is checked before sending anything.
        deadline = time.monotonic() + remaining(timeout)
        addresses = socket.getaddrinfo(host, port, type=socket.SOCK_STREAM)
        last_error = None
        for family, kind, protocol, _, address in addresses:
            remaining(timeout)
            if time.monotonic() >= deadline:
                raise TimeoutError("connection setup timed out")
            sock = socket.socket(family, kind, protocol)
            try:
                sock.setblocking(False)
                result = sock.connect_ex(address)
                if result not in (0, errno.EISCONN):
                    if result not in (errno.EINPROGRESS, errno.EWOULDBLOCK, errno.EALREADY, errno.EINTR):
                        raise OSError(result, os.strerror(result))
                    while True:
                        left = min(remaining(timeout), deadline - time.monotonic())
                        if left <= 0:
                            raise TimeoutError("connection setup timed out")
                        _, ready, failed = select.select([], [sock], [sock], min(left, 0.02))
                        if ready or failed:
                            result = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
                            if result:
                                raise OSError(result, os.strerror(result))
                            break
                remaining(timeout)
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                return sock
            except ProjectError:
                sock.close()
                raise
            except OSError as exc:
                sock.close()
                last_error = exc
        if last_error is not None:
            raise last_error
        raise OSError("no address available")

    @staticmethod
    def _abort(sock):
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass

    def _register(self, sock, deadline, stop):
        handle = object()
        with self._condition:
            if self._closed:
                raise ProjectError("CANCELLED", "HTTP transport closed")
            self._requests[handle] = [sock, deadline, stop, False]
            self.peak_active = max(self.peak_active, len(self._requests))
            if self._thread is None:
                self._thread = threading.Thread(target=self._watch, name="socket-supervisor", daemon=True)
                self._thread.start()
            self._condition.notify()
        return handle

    def _unregister(self, handle):
        with self._condition:
            self._requests.pop(handle, None)
            self._condition.notify()

    def _watch(self):
        while True:
            with self._condition:
                while not self._closed and not self._requests:
                    self._condition.wait()
                if self._closed:
                    return
                entries = list(self._requests.values())
            now = time.monotonic()
            pause = 0.02
            for entry in entries:
                sock, deadline, stop, aborted = entry
                if aborted:
                    continue
                if ((stop is not None and stop.is_set())
                        or (deadline is not None and now >= deadline)):
                    entry[3] = True
                    self._abort(sock)
                elif deadline is not None:
                    pause = min(pause, max(0.001, deadline - now))
            with self._condition:
                if not self._closed:
                    self._condition.wait(pause)

    def close(self):
        with self._condition:
            self._closed = True
            sockets = [entry[0] for entry in self._requests.values()]
            self._condition.notify_all()
        for sock in sockets:
            self._abort(sock)
        if self._thread is not None and self._thread is not threading.current_thread():
            self._thread.join()

    def get_bytes(self, url, path, *, headers=None, connect_timeout_s=2.0,
                  read_idle_timeout_s=10.0, max_bytes=MAX_CHUNK_BYTES,
                  body_deadline_s=120.0, absolute_deadline=None, stop_event=None,
                  expected_length=None, read_error_body=True, destination=None):
        def remaining(default):
            if self._closed or (stop_event is not None and stop_event.is_set()):
                raise ProjectError("CANCELLED", "request cancelled")
            if absolute_deadline is not None:
                left = absolute_deadline - time.monotonic()
                if left <= 0:
                    raise ProjectError("DOWNLOAD_DEADLINE", "download deadline exceeded")
                return min(default, left)
            return default

        def body_remaining():
            budget = remaining(read_idle_timeout_s)
            left = body_deadline - time.monotonic()
            if left <= 0:
                raise ProjectError("BODY_DEADLINE", "response body exceeded transfer deadline", 504)
            return min(budget, left)

        parsed = urlsplit(base_url(url))
        started = time.monotonic()
        conn = _Connection(parsed.hostname, parsed.port or 80,
                           lambda host, port: self._connect(host, port, connect_timeout_s, remaining))
        response = None
        handle = None
        body_deadline = None
        transport_socket = None
        body = part = None
        try:
            # Connect has its own timeout, capped by the task's remaining time.
            # DNS uses the OS resolver, as in the course helper.
            conn.connect()
            transport_socket = conn.sock
            transport_socket.settimeout(remaining(read_idle_timeout_s))
            handle = self._register(transport_socket, absolute_deadline, stop_event)
            remaining(read_idle_timeout_s)
            conn.request("GET", path, headers={"Connection": "close", **(headers or {})})
            response = conn.getresponse()
            remaining(read_idle_timeout_s)
            items = response.getheaders()
            lengths = [value for key, value in items if key.lower() == "content-length"]
            response_headers = {key.lower(): value for key, value in items}
            if ("transfer-encoding" in response_headers or len(lengths) != 1
                    or not lengths[0].isascii() or not lengths[0].isdigit()):
                raise ProjectError("BAD_RESPONSE", "exactly one Content-Length required; transfer encoding unsupported", 502)
            if len(lengths[0]) > 20 or int(lengths[0]) > max_bytes:
                raise ProjectError("RESPONSE_TOO_LARGE", "response exceeds configured bound", 502)
            length = int(lengths[0])
            if response.status == 200 and expected_length is not None and length != expected_length:
                raise ProjectError("CHUNK_LENGTH_MISMATCH", "Content-Length does not match manifest", 502)
            if response.status != 200 and not read_error_body:
                # These callers use only status/Retry-After. Closing now avoids
                # buffering an untrusted error body at every concurrent slot.
                return HTTPResult(response.status, response_headers, b"",
                                  (time.monotonic() - started) * 1000)
            body = bytearray(length) if destination is None else destination
            body_deadline = time.monotonic() + body_deadline_s
            received = 0
            while received < length:
                transport_socket.settimeout(body_remaining())
                part = response.read1(min(65536, length - received))
                body_remaining()
                if not part:
                    raise ProjectError("TRUNCATED_RESPONSE", "body shorter than Content-Length", 502)
                end = received + len(part)
                body[received:end] = part
                received = end
            remaining(read_idle_timeout_s)
            data = bytes(body) if destination is None else memoryview(body).toreadonly()
            return HTTPResult(response.status, response_headers, data,
                              (time.monotonic() - started) * 1000)
        except ProjectError:
            remaining(read_idle_timeout_s)
            raise
        except (socket.timeout, TimeoutError) as exc:
            remaining(read_idle_timeout_s)
            if body_deadline is not None and time.monotonic() >= body_deadline:
                raise ProjectError("BODY_DEADLINE", "response body exceeded transfer deadline", 504) from exc
            raise ProjectError("NETWORK_TIMEOUT", "connection or blocking read timed out", 504) from exc
        except (OSError, http.client.HTTPException) as exc:
            remaining(read_idle_timeout_s)
            raise ProjectError("NETWORK_ERROR", f"HTTP transport failed: {type(exc).__name__}", 502) from exc
        finally:
            # Exception tracebacks can outlive the upstream memory reservation.
            body = part = destination = None
            if handle is not None:
                self._unregister(handle)
            if response is not None:
                response.close()
            conn.close()
