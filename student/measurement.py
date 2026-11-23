"""Per-task measurements. Units: milliseconds, Mbps, and server load ratio."""
import math
import threading
import time

from framework.common import ProjectError, positive


class Measurements:
    def __init__(self, alpha=0.25):
        self.alpha = positive(alpha, "measurement_alpha")
        if self.alpha > 1:
            raise ProjectError("INVALID_CONFIG", "measurement_alpha must not exceed 1")
        self._nodes = {}
        self._lock = threading.Lock()

    def _row(self, node_id):
        return self._nodes.setdefault(node_id, {
            "probe_latency_ms": None, "goodput_mbps": None, "load_ratio": None,
            "reachable": None, "probe_samples": 0, "chunk_samples": 0, "status_samples": 0,
            "active_requests": None, "max_active_requests": None,
            "probe_updated_at": None, "chunk_updated_at": None, "status_updated_at": None,
        })

    def register(self, node_id):
        with self._lock:
            self._row(node_id)

    @staticmethod
    def _number(value):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value < 0):
            raise ValueError("measurement must be a finite nonnegative number")
        return float(value)

    def _smooth(self, old, sample, *, fast_fall=False):
        if old is None:
            return sample
        weight = max(self.alpha, 0.75) if fast_fall and sample < old else self.alpha
        return (1 - weight) * old + weight * sample

    def observe_probe(self, node_id, elapsed_ms):
        sample = self._number(elapsed_ms)
        with self._lock:
            row = self._row(node_id)
            row["probe_latency_ms"] = self._smooth(row["probe_latency_ms"], sample)
            row["probe_samples"] += 1
            row["probe_updated_at"] = time.monotonic()
            row["reachable"] = True

    def probe_failed(self, node_id):
        with self._lock:
            self._row(node_id)["reachable"] = False

    def observe_chunk(self, node_id, byte_count, elapsed_ms):
        size, elapsed = self._number(byte_count), self._number(elapsed_ms)
        if elapsed <= 0:
            raise ValueError("chunk duration must be positive")
        sample = size * 8 / (elapsed / 1000) / 1_000_000
        with self._lock:
            row = self._row(node_id)
            row["goodput_mbps"] = self._smooth(row["goodput_mbps"], sample, fast_fall=True)
            row["chunk_samples"] += 1
            row["chunk_updated_at"] = time.monotonic()

    def observe_status(self, node_id, status):
        active, maximum = status["active_requests"], status["max_active_requests"]
        if (type(active) is not int or type(maximum) is not int
                or not 0 <= active <= maximum or not 1 <= maximum <= 64):
            raise ValueError("invalid server load")
        with self._lock:
            row = self._row(node_id)
            row["load_ratio"] = self._smooth(row["load_ratio"], active / maximum)
            row["active_requests"], row["max_active_requests"] = active, maximum
            row["status_samples"] += 1
            row["status_updated_at"] = time.monotonic()

    def snapshot(self):
        with self._lock:
            return {node_id: dict(row) for node_id, row in self._nodes.items()}
