"""One capacity-bounded, thread-safe LRU shared by all files on a CDN."""
from collections import OrderedDict
from dataclasses import dataclass
import mmap
import threading
import sys
import time

from framework.common import ProjectError, integer


CHUNK_MEMORY_LIMIT = 160 * 1024 * 1024
MAPPED_CHUNK_MIN = 1024 * 1024


class ChunkMemory:
    """Account for chunk bytes from upstream allocation through downstream send."""

    def __init__(self, limit=CHUNK_MEMORY_LIMIT):
        self.limit = limit
        self.condition = threading.Condition()
        self._used = 0

    def reserve(self, length, cache, evicted):
        # Large chunks are read directly into their final mapping; small
        # chunks can have a raw buffer and an immutable copy during conversion.
        if length >= MAPPED_CHUNK_MIN:
            amount = self.resident_size(length)
        else:
            amount = 2 * length
        amount += 256 * 1024
        deadline = time.monotonic() + 0.25
        with self.condition:
            while self._used + amount > self.limit:
                old_key = cache.evict_oldest()
                if old_key is not None:
                    evicted.append(old_key)
                    continue
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise ProjectError("BUSY", "chunk memory in use", 503)
                self.condition.wait(remaining)
            self._used += amount
        return _Reservation(self, amount)

    @staticmethod
    def resident_size(length):
        if length >= MAPPED_CHUNK_MIN:
            length = (length + mmap.PAGESIZE - 1) // mmap.PAGESIZE * mmap.PAGESIZE
        return length + 1024

    def _release(self, amount):
        with self.condition:
            self._used -= amount
            self.condition.notify_all()

    @property
    def bytes_used(self):
        with self.condition:
            return self._used


class _Reservation:
    def __init__(self, memory, amount):
        self.memory = memory
        self.amount = amount

    def retain_body(self, length):
        retained = self.memory.resident_size(length)
        self.memory._release(self.amount - retained)
        self.amount = retained

    def release(self):
        if self.amount:
            amount = self.amount
            self.amount = 0
            self.memory._release(amount)


class ChunkBody(bytes):
    """The framework holds the reservation by retaining the response bytes."""

    def __new__(cls, data, reservation):
        body = super().__new__(cls, data)
        body._reservation = reservation
        return body

    def __del__(self):
        reservation = getattr(self, "_reservation", None)
        if reservation is not None:
            reservation.release()


class MappedChunk(mmap.mmap):
    """Anonymous storage released when the final read-only response view dies."""

    def __new__(cls, length, reservation):
        body = super().__new__(cls, -1, length)
        body._reservation = reservation
        return body

    def __del__(self):
        self.close()
        reservation = getattr(self, "_reservation", None)
        if reservation is not None:
            reservation.release()


@dataclass(frozen=True)
class CachedChunk:
    data: bytes | memoryview
    digest: str


class LRUCache:
    def __init__(self, capacity, *, lock=None):
        self.capacity = integer(capacity, "cache_capacity_chunks", 1, 100000)
        self._items = OrderedDict()
        self._lock = threading.Lock() if lock is None else lock

    def get(self, key):
        with self._lock:
            if key not in self._items:
                return None
            self._items.move_to_end(key)
            return self._items[key]

    def put(self, key, data):
        with self._lock:
            self._items[key] = data
            self._items.move_to_end(key)
            if len(self._items) > self.capacity:
                return self._items.popitem(last=False)[0]
            return None

    def __len__(self):
        with self._lock:
            return len(self._items)

    def evict_oldest(self):
        with self._lock:
            if self._items:
                return self._items.popitem(last=False)[0]
            return None


def resident_bytes(value):
    """Estimate the retained object graph, counting shared references once."""
    size, seen, pending = 0, set(), [value]
    while pending:
        item = pending.pop()
        identity = id(item)
        if identity in seen:
            continue
        seen.add(identity)
        size += sys.getsizeof(item)
        if isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, (tuple, list, set, frozenset)):
            pending.extend(item)
    return size


class ByteLRUCache:
    """Byte-budgeted metadata cache, separate from the required chunk LRU."""

    def __init__(self, capacity, max_items=64):
        self.capacity = integer(capacity, "manifest_cache_bytes", 1, 64 * 1024 * 1024)
        self.max_items = max_items
        self._items = OrderedDict()
        self._used = 0
        self._lock = threading.Lock()

    def get(self, key):
        with self._lock:
            if key not in self._items:
                return None
            self._items.move_to_end(key)
            return self._items[key][0]

    def put(self, key, value):
        weight = resident_bytes((key, value))
        evicted = []
        with self._lock:
            previous = self._items.pop(key, None)
            if previous is not None:
                self._used -= previous[1]
            if weight > self.capacity:
                return evicted
            self._items[key] = (value, weight)
            self._used += weight
            while self._used > self.capacity or len(self._items) > self.max_items:
                old_key, (_, old_weight) = self._items.popitem(last=False)
                self._used -= old_weight
                evicted.append(old_key)
        return evicted

    @property
    def bytes_used(self):
        with self._lock:
            return self._used

    def __len__(self):
        with self._lock:
            return len(self._items)


class _Flight:
    def __init__(self):
        self.ready = threading.Event()
        self.value = None
        self.error = None


class CoalescedCache:
    """One producer per key; errors are broadcast, never cached.

    Chunk waiters re-check the real LRU before reporting HIT. If another key
    evicted it meanwhile, they perform a fresh, auditable MISS instead.
    Oversized metadata may be shared for a flight without being retained.
    """

    def __init__(self, cache, wait_timeout, *, require_cached_hit=False):
        self.cache = cache
        self.wait_timeout = wait_timeout
        self.require_cached_hit = require_cached_hit
        self._flights = {}
        self._lock = threading.Lock()

    def get_or_fetch(self, key, fetch, on_evict=None):
        deadline = time.monotonic() + self.wait_timeout
        while True:
            if time.monotonic() >= deadline:
                raise ProjectError("ORIGIN_ERROR", "shared origin fetch timed out", 502)
            with self._lock:
                value = self.cache.get(key)
                if value is not None:
                    return value, "HIT"
                flight = self._flights.get(key)
                producer = flight is None
                if producer:
                    flight = self._flights[key] = _Flight()
            if not producer:
                if not flight.ready.wait(max(0.0, deadline - time.monotonic())):
                    raise ProjectError("ORIGIN_ERROR", "shared origin fetch timed out", 502)
                if flight.error is not None:
                    raise ProjectError(*flight.error)
                if self.require_cached_hit:
                    continue
                return flight.value, "HIT"
            try:
                value = fetch()
                if time.monotonic() >= deadline:
                    raise ProjectError("ORIGIN_ERROR", "shared origin fetch timed out", 502)
                evicted = self.cache.put(key, value)
                if on_evict is not None and evicted is not None:
                    on_evict(evicted)
                if not self.require_cached_hit:
                    flight.value = value
            except BaseException as exc:
                flight.error = ((exc.code, exc.message, exc.status) if isinstance(exc, ProjectError)
                                else ("ORIGIN_ERROR", "shared origin fetch failed", 502))
                raise
            finally:
                with self._lock:
                    self._flights.pop(key, None)
                    flight.ready.set()
            return value, "MISS"
