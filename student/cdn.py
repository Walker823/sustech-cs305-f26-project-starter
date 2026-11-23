"""CDN policy; HTTP routing and service lifecycle belong to the framework."""
import json
import threading
import time

from framework.common import (MAX_MANIFEST_BYTES, ProjectError, base_url,
                              integer, positive, sha256, valid_id,
                              validate_manifest)
from framework.cdn_runtime import main
from student.cache import (MAPPED_CHUNK_MIN, ByteLRUCache, CachedChunk, ChunkBody,
                           ChunkMemory, CoalescedCache, LRUCache, MappedChunk)
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
        self._chunk_memory = ChunkMemory()
        # Eviction can drop the last body reference and release its reservation
        # on the same thread. A shared reentrant lock keeps this atomic.
        self.cache = LRUCache(config["cache_capacity_chunks"], lock=self._chunk_memory.condition)
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
        manifest_limit = 4 * 1024 * 1024
        manifest_capacity = integer(config.get("manifest_cache_bytes", manifest_limit),
                                    "manifest_cache_bytes", 1, 64 * 1024 * 1024)
        self._manifests = ByteLRUCache(min(manifest_capacity, manifest_limit))
        self._manifest_fills = CoalescedCache(self._manifests, budget)
        self._chunk_fills = CoalescedCache(self.cache, budget, require_cached_hit=True)
        # Bound peak JSON parsing memory for different uncached files, too.
        self._manifest_slots = threading.BoundedSemaphore(1)

    def _get(self, path, request_id, *, chunk=False, expected_length=None, destination=None):
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
                destination=destination,
            )
        except ProjectError as exc:
            if chunk and exc.code == "CHUNK_LENGTH_MISMATCH":
                raise ProjectError("UPSTREAM_INTEGRITY_ERROR", "origin chunk length does not match manifest", 502) from exc
            raise ProjectError("ORIGIN_ERROR", f"origin transport failed: {exc.code}", 502) from exc
        finally:
            destination = None  # Do not pin the mapping through an error traceback.

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
                manifest = validate_manifest(json.loads(response.body), file_id)
            except (ValueError, RecursionError, ProjectError) as exc:
                raise ProjectError("ORIGIN_ERROR", "invalid origin manifest", 502) from exc
            finally:
                response = None
            digests = bytearray(len(manifest["chunks"]) * 32)
            for index, chunk in enumerate(manifest["chunks"]):
                offset = 32 * index
                digests[offset:offset + 32] = bytes.fromhex(chunk["sha256"])
            return manifest["file_size"], manifest["chunk_size"], bytes(digests)
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
            return cached.data, "HIT", cached.digest
        file_size, chunk_size, digests = self._manifest(file_id, request_id)
        offset = chunk_id * 32
        if offset >= len(digests):
            raise ProjectError("CHUNK_NOT_FOUND", "unknown chunk_id", 404)
        digest = digests[offset:offset + 32].hex()
        size = min(chunk_size, file_size - chunk_id * chunk_size)
        del digests  # An uncached manifest need not stay alive during body I/O.

        def evicted(old_key):
            self.log.emit("cache_evict", request_id=request_id, file_id=old_key[0], chunk_id=old_key[1])

        data, status = self._chunk_fills.get_or_fetch(
            key, lambda: self._fetch_chunk(file_id, chunk_id, request_id, size, digest, evicted),
            on_evict=evicted)
        if status == "HIT":
            self.log.emit("cache_hit", request_id=request_id, file_id=file_id, chunk_id=chunk_id)
        return data.data, status, data.digest

    def _fetch_chunk(self, file_id, chunk_id, request_id, size, digest, on_evict):
        evicted = []
        reservation = response = body = mapped = None
        try:
            reservation = self._chunk_memory.reserve(size, self.cache, evicted)
            for key in evicted:
                on_evict(key)
            if size >= MAPPED_CHUNK_MIN:
                try:
                    mapped = MappedChunk(size, reservation)
                except OSError as exc:
                    raise ProjectError("BUSY", "chunk memory unavailable", 503) from exc
            self.log.emit("cache_miss", request_id=request_id, file_id=file_id, chunk_id=chunk_id)
            response = self._get(f"/chunk/{file_id}/{chunk_id}", request_id, chunk=True,
                                 expected_length=size, destination=mapped)
            if response.status != 200:
                raise ProjectError("ORIGIN_ERROR", f"origin fetch HTTP {response.status}", 502)
            if len(response.body) != size or sha256(response.body) != digest:
                raise ProjectError("UPSTREAM_INTEGRITY_ERROR", "origin chunk does not match manifest", 502)
            body = response.body if mapped is not None else ChunkBody(response.body, reservation)
        finally:
            response = None
            if body is not None:
                reservation.retain_body(size)
            elif mapped is None and reservation is not None:
                reservation.release()
            elif reservation is None:
                for key in evicted:
                    on_evict(key)
            mapped = None
        return CachedChunk(body, digest)


if __name__ == "__main__":
    raise SystemExit(main(CDN))
