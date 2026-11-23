"""CDN policy; HTTP routing and service lifecycle belong to the framework."""
import json
import threading
import time

from framework.common import (MAX_MANIFEST_BYTES, ProjectError, base_url,
                              integer, positive, sha256, valid_id,
                              validate_manifest)
from framework.cdn_runtime import main
from student.cache import ByteLRUCache, CoalescedCache, LRUCache
from student.transport import HTTPTransport
from student.util import BoundedEvents, EventBudget


class CDN:
    def __init__(self, config):
        self.config = config
        self.node_id = valid_id(config["cdn_id"])
        self.origin_url = base_url(config["origin_url"])
        self.token = config["origin_chunk_token"]
        if (not isinstance(self.token, str) or not self.token.isascii()
                or len(self.token) < 16 or "\r" in self.token or "\n" in self.token):
            raise ProjectError("INVALID_CONFIG", "origin_chunk_token must contain at least 16 ASCII characters")
        self.cache = LRUCache(config["cache_capacity_chunks"])
        self.limit = integer(config.get("max_active_requests", 8), "max_active_requests", 1, 64)
        self.slots = threading.BoundedSemaphore(self.limit)
        self.lock = threading.Lock()
        self.active = 0
        self.transport = HTTPTransport()
        self.events = BoundedEvents("cdn", self.node_id, on_close=self.transport.close)
        self.log = EventBudget(self.events, 24 * 1024)
        self.connect_timeout = positive(config.get("connect_timeout_s", 2), "connect_timeout_s")
        self.read_timeout = positive(config.get("read_idle_timeout_s", 10), "read_idle_timeout_s")
        self.body_deadline = positive(config.get("body_deadline_s", 120), "body_deadline_s")
        budget = self.connect_timeout + self.read_timeout + self.body_deadline
        self._manifests = ByteLRUCache(config.get("manifest_cache_bytes", 4 * 1024 * 1024))
        self._manifest_fills = CoalescedCache(self._manifests, budget)
        self._chunk_fills = CoalescedCache(self.cache, budget, require_cached_hit=True)
        # Bound peak JSON parsing memory for different uncached files, too.
        self._manifest_slots = threading.BoundedSemaphore(1)

    def _get(self, path, request_id, *, chunk=False, expected_length=None):
        headers = {"X-Request-ID": request_id}
        if chunk:
            headers["X-Origin-Token"] = self.token
        try:
            return self.transport.get_bytes(
                self.origin_url, path, headers=headers,
                connect_timeout_s=self.connect_timeout,
                read_idle_timeout_s=self.read_timeout,
                body_deadline_s=self.body_deadline,
                max_bytes=MAX_MANIFEST_BYTES,
                absolute_deadline=time.monotonic() + self.connect_timeout
                + self.read_timeout + self.body_deadline,
                expected_length=expected_length, read_error_body=False,
            )
        except ProjectError as exc:
            if chunk and exc.code == "CHUNK_LENGTH_MISMATCH":
                raise ProjectError("UPSTREAM_INTEGRITY_ERROR", "origin chunk length does not match manifest", 502) from exc
            raise ProjectError("ORIGIN_ERROR", f"origin transport failed: {exc.code}", 502) from exc

    def _manifest(self, file_id, request_id):
        # Immutable file IDs permit cached trusted metadata. In-flight fills
        # share even a manifest too large to retain in the byte-budgeted LRU.
        value, _ = self._manifest_fills.get_or_fetch(
            file_id, lambda: self._fetch_manifest(file_id, request_id))
        return value

    def _fetch_manifest(self, file_id, request_id):
        budget = self.connect_timeout + self.read_timeout + self.body_deadline
        if not self._manifest_slots.acquire(timeout=budget):
            raise ProjectError("ORIGIN_ERROR", "manifest parsing slots timed out", 502)
        try:
            response = self._get(f"/manifest/{file_id}", request_id)
            if response.status == 404:
                raise ProjectError("FILE_NOT_FOUND", "unknown file_id", 404)
            if response.status != 200:
                raise ProjectError("ORIGIN_ERROR", f"manifest HTTP {response.status}", 502)
            try:
                return validate_manifest(json.loads(response.body), file_id)
            except (ValueError, RecursionError, ProjectError) as exc:
                raise ProjectError("ORIGIN_ERROR", "invalid origin manifest", 502) from exc
        finally:
            self._manifest_slots.release()

    def get_chunk(self, file_id, chunk_id, request_id):
        valid_id(file_id)
        if type(chunk_id) is not int or chunk_id < 0:
            raise ProjectError("INVALID_CHUNK_ID", "invalid nonnegative chunk_id")
        key = (file_id, chunk_id)
        cached = self.cache.get(key)
        if cached is not None:
            # Verified immutable bytes are sufficient to serve a real HIT,
            # even when its metadata was evicted or Origin is unavailable.
            self.log.emit("cache_hit", request_id=request_id, file_id=file_id, chunk_id=chunk_id)
            return cached, "HIT", sha256(cached)
        manifest = self._manifest(file_id, request_id)
        if chunk_id >= len(manifest["chunks"]):
            raise ProjectError("CHUNK_NOT_FOUND", "unknown chunk_id", 404)
        meta = manifest["chunks"][chunk_id]
        del manifest  # Do not retain an oversized uncached manifest during body I/O.

        def evicted(old_key):
            self.log.emit("cache_evict", request_id=request_id, file_id=old_key[0], chunk_id=old_key[1])

        data, status = self._chunk_fills.get_or_fetch(
            key, lambda: self._fetch_chunk(file_id, chunk_id, request_id, meta), on_evict=evicted)
        if status == "HIT":
            self.log.emit("cache_hit", request_id=request_id, file_id=file_id, chunk_id=chunk_id)
        return data, status, meta["sha256"]

    def _fetch_chunk(self, file_id, chunk_id, request_id, meta):
        self.log.emit("cache_miss", request_id=request_id, file_id=file_id, chunk_id=chunk_id)
        response = self._get(f"/chunk/{file_id}/{chunk_id}", request_id, chunk=True,
                             expected_length=meta["size"])
        if response.status != 200:
            raise ProjectError("ORIGIN_ERROR", f"origin fetch HTTP {response.status}", 502)
        data = response.body
        if len(data) != meta["size"] or sha256(data) != meta["sha256"]:
            raise ProjectError("UPSTREAM_INTEGRITY_ERROR", "origin chunk does not match manifest", 502)
        return data


if __name__ == "__main__":
    raise SystemExit(main(CDN))
