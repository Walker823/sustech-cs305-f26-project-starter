"""Download state. Only the task coordinator mutates chunks and nodes."""
from dataclasses import dataclass, field
import threading
import time

from framework.common import ProjectError, integer, positive


@dataclass(frozen=True)
class DownloadOptions:
    max_inflight: int
    per_cdn: int
    connect_timeout: float
    read_timeout: float
    body_deadline: float
    download_timeout: float
    max_attempts: int
    backoff: float
    backoff_max: float
    cooldown: float
    cooldown_max: float
    probe_interval: float
    alpha: float
    exploration_every: int
    max_file_size: int

    @classmethod
    def from_config(cls, config):
        def number(name, default):
            return positive(config.get(name, default), name)

        value = cls(
            integer(config.get("max_inflight_chunks", 4), "max_inflight_chunks", 1, 32),
            integer(config.get("max_inflight_per_cdn", 2), "max_inflight_per_cdn", 1, 8),
            number("connect_timeout_s", 2), number("read_idle_timeout_s", 10),
            number("body_deadline_s", 120), number("download_timeout_s", 180),
            integer(config.get("max_attempts_per_chunk", 4), "max_attempts_per_chunk", 1, 16),
            number("retry_backoff_s", 0.15), number("retry_backoff_max_s", 2),
            number("node_cooldown_s", 0.5), number("node_cooldown_max_s", 4),
            number("probe_interval_s", 0.5), number("measurement_alpha", 0.25),
            integer(config.get("exploration_every", 8), "exploration_every", 2, 64),
            integer(config.get("max_file_size_bytes", 1024**3), "max_file_size_bytes", 0, 1024**4),
        )
        if (value.probe_interval < 0.1 or value.alpha > 1
                or value.backoff_max < value.backoff or value.cooldown_max < value.cooldown):
            raise ProjectError("INVALID_CONFIG", "invalid measurement period, alpha, backoff or cooldown cap")
        return value


@dataclass
class ChunkState:
    chunk_id: int
    size: int
    digest: str
    # PENDING -> IN_FLIGHT -> COMPLETED, or IN_FLIGHT -> RETRY_WAIT.
    phase: str = "PENDING"
    attempts: int = 0
    ready_at: float = 0.0
    last_cdn: str | None = None
    tried: set[str] = field(default_factory=set)


@dataclass
class NodeState:
    node_id: str
    url: str
    limit: int
    inflight: int = 0
    assignments: int = 0
    completed: int = 0
    recent_assignments: int = 0
    exploration_every: int = 8
    quarantined: bool = False
    recovering: bool = False
    trial_inflight: bool = False
    cooldown_until: float = 0.0
    retry_after_until: float = 0.0
    failures: int = 0
    generation: int = 0
    cancelled: threading.Event = field(default_factory=threading.Event, repr=False)


@dataclass(frozen=True)
class Attempt:
    attempt_id: str
    node_id: str
    number: int
    generation: int
    trial: bool


class AttemptFailure(ProjectError):
    def __init__(self, code, message, status=502, *, retry_after=0.0, quarantine=False):
        super().__init__(code, message, status)
        self.retry_after = retry_after
        self.quarantine = quarantine


class StopToken:
    """Combine task cleanup and user cancellation without another watcher thread."""

    def __init__(self, *events):
        self.events = events

    def is_set(self):
        return any(event.is_set() for event in self.events)

    def wait(self, timeout):
        until = time.monotonic() + timeout
        while not self.is_set():
            left = until - time.monotonic()
            if left <= 0:
                return False
            self.events[0].wait(min(left, 0.05))
        return True
