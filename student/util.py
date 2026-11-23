"""Small student-owned helpers; no third-party dependencies."""
import json
import re
import threading

from framework.common import Events


class BoundedEvents(Events):
    """Bound CDN diagnostics, including request events emitted by its runtime."""

    def __init__(self, role, node_id, on_close=None):
        super().__init__(role, node_id)
        self._remaining = 96 * 1024
        self._budget_lock = threading.Lock()
        self._reported_limit = False
        self._on_close = on_close

    def close(self):
        if self._on_close is not None:
            self._on_close()
        super().close()

    def emit(self, event, **fields):
        if event not in ("ready", "stopped"):
            with self._budget_lock:
                cost = (len(json.dumps(fields, ensure_ascii=False).encode("utf-8")) + len(event) + 220
                        if self._remaining >= 220 else 221)
                if cost > self._remaining:
                    if not self._reported_limit:
                        self._reported_limit = True
                        super().emit("diagnostics_limited", reason="log budget reached")
                    return
                self._remaining -= cost
        super().emit(event, **fields)


def retry_after_seconds(value):
    """Accept bounded decimal delta-seconds; ignore dates, signs, NaN, etc."""
    if not isinstance(value, str):
        return 0.0
    value = value.strip()
    if len(value) > 32 or not re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", value):
        return 0.0
    seconds = float(value)
    return seconds if 0 <= seconds <= 60 else 0.0


class EventBudget:
    """Bound optional diagnostics, leaving room for framework/terminal events."""

    def __init__(self, events, limit=64 * 1024):
        self.events = events
        self.remaining = limit
        self.lock = threading.Lock()

    def emit(self, event, **fields):
        with self.lock:
            if self.remaining < 220:
                return
            cost = len(json.dumps(fields, ensure_ascii=False).encode("utf-8")) + len(event) + 220
            if cost > self.remaining:
                return
            self.remaining -= cost
        self.events.emit(event, **fields)
