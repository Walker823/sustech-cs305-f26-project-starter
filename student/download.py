"""File-task coordinator. Workers fetch bytes; the coordinator owns all state."""
import hashlib
import heapq
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
import json
import os
from pathlib import Path
import tempfile
import threading
import time
import uuid

from framework.common import (MAX_CHUNK_BYTES, MAX_MANIFEST_BYTES, ProjectError,
                              sha256, valid_id, validate_manifest)
from student.state import Attempt, AttemptFailure, ChunkState, NodeState, StopToken
from student.scheduler import select_chunk_source
from student.measurement import Measurements
from student.transport import HTTPTransport
from student.util import retry_after_seconds


class DownloadTask:
    def __init__(self, client, command):
        self.client, self.command = client, command
        self.options = client.options
        self.request_id = command["request_id"]
        self.file_id = command.get("file_id")
        self.deadline = command.get("_accepted_at", time.monotonic()) + self.options.download_timeout
        self.request_stop = threading.Event()
        self.stop = StopToken(self.request_stop, client.cancelled)
        self.transport = HTTPTransport()
        self.nodes = {source["id"]: NodeState(
            source["id"], source["url"], self.options.per_cdn,
            exploration_every=self.options.exploration_every,
        ) for source in client.sources}
        self.chunks = []
        self.inflight = 0
        self.verified = 0
        self.attempts = 0
        self.failed_attempts = 0
        self.file = None
        self.temp_path = None
        self.executor = None
        self.futures = {}
        self.rotation = deque(self.nodes)
        self.measurements = Measurements(self.options.alpha)
        for node_id in self.nodes:
            self.measurements.register(node_id)
        self.assignment_history = deque(maxlen=max(16, 4 * self.options.exploration_every))
        self.measurement_stop = threading.Event()
        self.measurement_thread = None
        self.measurement_executor = None
        # One shared cancellation supervisor permits the full configured limit.
        self.worker_limit = min(self.options.max_inflight,
                                sum(node.limit for node in self.nodes.values()))
        self.log = client.log

    def emit(self, event, **fields):
        self.log.emit(event, request_id=self.request_id, file_id=self.file_id, **fields)

    def check(self):
        if self.client.cancelled.is_set():
            raise ProjectError("CANCELLED", "download cancelled")
        if time.monotonic() >= self.deadline:
            raise ProjectError("DOWNLOAD_DEADLINE", "download deadline exceeded")

    def get(self, url, path, attempt_id, max_bytes, stop=None, expected_length=None):
        self.check()
        return self.transport.get_bytes(
            url, path, headers={"X-Request-ID": attempt_id}, max_bytes=max_bytes,
            connect_timeout_s=self.options.connect_timeout,
            read_idle_timeout_s=self.options.read_timeout,
            body_deadline_s=self.options.body_deadline,
            absolute_deadline=self.deadline, stop_event=stop or self.stop,
            expected_length=expected_length, read_error_body=False,
        )

    def load_manifest(self):
        valid_id(self.file_id)
        response = self.get(self.client.origin_url, f"/manifest/{self.file_id}",
                            uuid.uuid4().hex, MAX_MANIFEST_BYTES)
        if response.status != 200:
            raise ProjectError("FILE_NOT_FOUND" if response.status == 404 else "MANIFEST_ERROR",
                               f"manifest HTTP {response.status}", response.status)
        try:
            self.manifest = validate_manifest(json.loads(response.body), self.file_id)
        except (ValueError, RecursionError) as exc:
            raise ProjectError("INVALID_MANIFEST", "manifest is not valid JSON") from exc
        if self.manifest["file_size"] > self.options.max_file_size:
            raise ProjectError("FILE_TOO_LARGE", "file exceeds max_file_size_bytes")
        self.chunks = [ChunkState(meta["id"], meta["size"], meta["sha256"])
                       for meta in self.manifest["chunks"]]

    def output_target(self):
        value = self.command.get("output_path")
        if not isinstance(value, str) or not value or Path(value).is_absolute():
            raise ProjectError("INVALID_OUTPUT_PATH", "output path must be relative to output_root")
        root = self.client.output_root.resolve()
        target = (root / value).resolve()
        if target == root or not target.is_relative_to(root) or target.is_dir():
            raise ProjectError("INVALID_OUTPUT_PATH", "output path must resolve to a file inside output_root")
        return target

    def create_temporary(self):
        self.target = self.output_target()
        self.target.parent.mkdir(parents=True, exist_ok=True)
        if self.output_target() != self.target:
            raise ProjectError("INVALID_OUTPUT_PATH", "output path changed")
        fd, name = tempfile.mkstemp(prefix="." + self.target.name + ".", suffix=".part",
                                    dir=self.target.parent)
        self.temp_path = Path(name)
        self.file = os.fdopen(fd, "w+b")
        self.file.truncate(self.manifest["file_size"])

    def start_attempt(self, chunk, node):
        self.check()
        if chunk.phase == "IN_FLIGHT" or chunk.phase == "COMPLETED":
            raise RuntimeError("chunk was scheduled twice")
        if self.inflight >= self.options.max_inflight or node.inflight >= node.limit:
            raise RuntimeError("in-flight limit exceeded")
        chunk.phase = "IN_FLIGHT"
        chunk.attempts += 1
        chunk.tried.add(node.node_id)
        previous = chunk.last_cdn
        chunk.last_cdn = node.node_id
        node.inflight += 1
        node.assignments += 1
        if len(self.assignment_history) == self.assignment_history.maxlen:
            self.nodes[self.assignment_history.popleft()].recent_assignments -= 1
        self.assignment_history.append(node.node_id)
        node.recent_assignments += 1
        if node.recovering:
            node.trial_inflight = True
        self.inflight += 1
        self.attempts += 1
        attempt = Attempt(uuid.uuid4().hex, node.node_id, chunk.attempts,
                          node.generation, node.recovering)
        self.emit("schedule", chunk_id=chunk.chunk_id, cdn_id=node.node_id,
                  attempt_id=attempt.attempt_id, attempt_number=attempt.number,
                  reason="initial" if previous is None else "retry")
        if previous is not None:
            self.emit("chunk_reassign", chunk_id=chunk.chunk_id, from_cdn=previous,
                      to_cdn=node.node_id, attempt_id=attempt.attempt_id,
                      attempt_number=attempt.number)
        return attempt

    def release_attempt(self, node):
        # Called only after get_bytes has finished and closed its connection.
        node.inflight -= 1
        self.inflight -= 1
        if node.inflight == 0:
            node.trial_inflight = False
        if node.inflight < 0 or self.inflight < 0:
            raise RuntimeError("negative in-flight count")

    def fetch_chunk(self, chunk, node, attempt):
        try:
            response = self.get(node.url, f"/chunk/{self.file_id}/{chunk.chunk_id}",
                                attempt.attempt_id, MAX_CHUNK_BYTES,
                                StopToken(self.request_stop, self.client.cancelled, node.cancelled),
                                expected_length=chunk.size)
        except ProjectError as exc:
            if exc.code == "CANCELLED" and node.quarantined and not self.stop.is_set():
                raise AttemptFailure("SOURCE_QUARANTINED", "source was quarantined",
                                     quarantine=True) from exc
            raise AttemptFailure(exc.code, str(exc), exc.status,
                                 quarantine=exc.code in ("RESPONSE_TOO_LARGE", "CHUNK_LENGTH_MISMATCH")) from exc
        if response.status != 200:
            raise AttemptFailure("CDN_HTTP_ERROR", f"CDN HTTP {response.status}", response.status,
                                 retry_after=retry_after_seconds(response.headers.get("retry-after")))
        if len(response.body) != chunk.size or sha256(response.body) != chunk.digest:
            raise AttemptFailure("CHUNK_INTEGRITY_ERROR", "chunk length or SHA-256 mismatch",
                                 quarantine=True)
        return response

    def attempt_failed(self, chunk, node, attempt, exc, retry_wait):
        self.check()
        if exc.code == "CANCELLED":
            raise exc
        self.failed_attempts += 1
        chunk.phase = "RETRY_WAIT"
        node.generation += 1
        node.failures += 1
        node.trial_inflight = False
        now = time.monotonic()
        if exc.quarantine:
            node.quarantined = True
            node.cancelled.set()
            state = "quarantined"
        else:
            node.recovering = True
            delay = min(self.options.cooldown_max,
                        self.options.cooldown * 2 ** min(node.failures - 1, 30))
            node.cooldown_until = max(node.cooldown_until, now + delay)
            node.retry_after_until = max(node.retry_after_until, now + exc.retry_after)
            state = "cooldown"
        self.emit("attempt_failed", chunk_id=chunk.chunk_id, cdn_id=node.node_id,
                  attempt_id=attempt.attempt_id, attempt_number=attempt.number, code=exc.code)
        self.emit("node_state", cdn_id=node.node_id, state=state, reason=exc.code)
        if all(source.quarantined for source in self.nodes.values()):
            raise ProjectError("NO_VALID_SOURCE", "every CDN returned invalid content")
        if chunk.attempts >= self.options.max_attempts:
            raise ProjectError("RETRY_EXHAUSTED", f"chunk {chunk.chunk_id} exhausted its attempt budget")
        backoff = min(self.options.backoff_max,
                      self.options.backoff * 2 ** min(chunk.attempts - 1, 30))
        chunk.ready_at = now + backoff
        heapq.heappush(retry_wait, (chunk.ready_at, chunk.chunk_id, chunk))
        self.emit("chunk_retry", chunk_id=chunk.chunk_id, previous_cdn=node.node_id,
                  retry_after_s=backoff, cause=exc.code)

    def attempt_succeeded(self, node, attempt):
        # An old success must never undo a more recent failure or quarantine.
        if attempt.trial and attempt.generation == node.generation and not node.quarantined:
            node.recovering = False
            node.failures = 0
            node.cooldown_until = node.retry_after_until = 0.0
            self.emit("node_state", cdn_id=node.node_id, state="healthy", reason="verified_trial")

    def choose_source(self, chunk):
        now = time.monotonic()
        candidates = [node for node in self.nodes.values() if not node.quarantined
                      and now >= max(node.cooldown_until, node.retry_after_until)
                      and not node.trial_inflight
                      and (not node.recovering or node.inflight == 0)]
        # Prefer an untried healthy source. Filter capacity afterwards: when
        # that source is merely full, wait instead of retrying the failed CDN.
        if chunk.attempts:
            untried = [node for node in candidates if node.node_id not in chunk.tried]
            if untried:
                candidates = untried
        if not any(node.inflight < node.limit for node in candidates):
            return None
        if self.client.strategy == "round_robin":
            allowed = {node.node_id for node in candidates if node.inflight < node.limit}
            for _ in range(len(self.rotation)):
                node_id = self.rotation[0]
                self.rotation.rotate(-1)
                if node_id in allowed:
                    return self.nodes[node_id]
            return None
        view = self.measurements.snapshot()
        # __policy__ cannot collide with a legal CDN ID (IDs start alphanumeric).
        view["__policy__"] = {"assignments": self.attempts,
                              "exploration_every": self.options.exploration_every,
                              "learning_window_open": len(self.chunks) - self.verified - self.inflight
                              > 2 * len(self.nodes),
                              "stale_after_s": max(2.0, 4 * self.options.probe_interval)}
        return select_chunk_source(chunk, candidates,
                                   {key: n.inflight for key, n in self.nodes.items()}, view)

    def write_chunk(self, chunk, node, attempt, response):
        self.check()
        self.file.seek(chunk.chunk_id * self.manifest["chunk_size"])
        if self.file.write(response.body) != chunk.size:
            raise ProjectError("LOCAL_ERROR", "short local file write")
        chunk.phase = "COMPLETED"
        node.completed += 1
        self.verified += 1
        self.measurements.observe_chunk(node.node_id, chunk.size, response.elapsed_ms)
        self.emit("chunk_verified", chunk_id=chunk.chunk_id, cdn_id=node.node_id,
                  attempt_id=attempt.attempt_id, byte_count=chunk.size,
                  request_elapsed_ms=round(response.elapsed_ms, 3))
        self.emit("measurement", cdn_id=node.node_id, sample_kind="chunk", sample_unit="Mbps",
                  sample_goodput_mbps=round(chunk.size * 8 / response.elapsed_ms / 1000, 6),
                  sample_count=self.measurements.snapshot()[node.node_id]["chunk_samples"])

    def measure_node(self, node):
        stop = StopToken(self.measurement_stop, self.request_stop, self.client.cancelled)
        for kind, path in (("probe", "/probe"), ("status", "/status")):
            if stop.is_set():
                return
            try:
                # Measurement has a short sampling budget as well as the task
                # deadline. A dead node cannot hold up all following rounds.
                budget = min(1.0, max(0.1, self.options.probe_interval))
                response = self.transport.get_bytes(
                    node.url, path, headers={"X-Request-ID": uuid.uuid4().hex}, max_bytes=65536,
                    connect_timeout_s=min(self.options.connect_timeout, budget),
                    read_idle_timeout_s=self.options.read_timeout,
                    body_deadline_s=self.options.body_deadline,
                    absolute_deadline=min(self.deadline, time.monotonic() + budget), stop_event=stop,
                    read_error_body=False,
                )
                if response.status != 200:
                    raise ProjectError("MEASUREMENT_ERROR", f"measurement HTTP {response.status}")
                if kind == "probe":
                    if response.body != b"OK":
                        raise ProjectError("MEASUREMENT_ERROR", "invalid Probe response")
                    self.measurements.observe_probe(node.node_id, response.elapsed_ms)
                    self.emit("measurement", cdn_id=node.node_id, sample_kind=kind, sample_unit="ms",
                              probe_latency_ms=round(response.elapsed_ms, 3),
                              sample_count=self.measurements.snapshot()[node.node_id]["probe_samples"])
                else:
                    status = json.loads(response.body)
                    self.measurements.observe_status(node.node_id, status)
                    row = self.measurements.snapshot()[node.node_id]
                    self.emit("measurement", cdn_id=node.node_id, sample_kind=kind, sample_unit="ratio",
                              load_ratio=row["load_ratio"], sample_count=row["status_samples"])
            except (ProjectError, OSError, ValueError, TypeError, KeyError, RecursionError) as exc:
                if stop.is_set():
                    return
                if kind == "probe":
                    self.measurements.probe_failed(node.node_id)
                self.emit("measurement_error", cdn_id=node.node_id, sample_kind=kind,
                          code=exc.code if isinstance(exc, ProjectError) else "INVALID_MEASUREMENT")

    def measurement_loop(self):
        try:
            while not self.measurement_stop.is_set() and not self.stop.is_set():
                started = time.monotonic()
                futures = [self.measurement_executor.submit(self.measure_node, node)
                           for node in self.nodes.values() if not node.quarantined]
                # Each node's Probe then Status are sequential; the next round
                # starts only after the entire previous round has finished.
                for future in futures:
                    future.result()
                self.measurement_stop.wait(max(0.0, started + self.options.probe_interval
                                                - time.monotonic()))
        finally:
            self.measurement_executor.shutdown(wait=True, cancel_futures=True)

    def start_measurements(self):
        if not self.chunks:
            return
        self.measurement_executor = ThreadPoolExecutor(max_workers=min(8, len(self.nodes)),
                                                       thread_name_prefix="measurement")
        self.measurement_thread = threading.Thread(target=self.measurement_loop,
                                                   name="measurement-rounds")
        self.measurement_thread.start()

    def download_chunks(self):
        pending = deque(self.chunks)
        retry_wait = []
        self.executor = ThreadPoolExecutor(max_workers=self.worker_limit,
                                           thread_name_prefix="chunk")
        while pending or self.futures or retry_wait:
            self.check()
            for future in [future for future in self.futures if future.done()]:
                chunk, node, attempt = self.futures.pop(future)
                self.release_attempt(node)
                try:
                    response = future.result()
                except AttemptFailure as exc:
                    self.attempt_failed(chunk, node, attempt, exc, retry_wait)
                    continue
                self.attempt_succeeded(node, attempt)
                self.write_chunk(chunk, node, attempt, response)

            now = time.monotonic()
            while retry_wait and retry_wait[0][0] <= now:
                pending.append(heapq.heappop(retry_wait)[2])

            for _ in range(len(pending)):
                if len(self.futures) >= self.worker_limit:
                    break
                now = time.monotonic()
                if not any(not node.quarantined and not node.trial_inflight
                           and now >= max(node.cooldown_until, node.retry_after_until)
                           and node.inflight < node.limit
                           and (not node.recovering or node.inflight == 0)
                           for node in self.nodes.values()):
                    break
                chunk = pending.popleft()
                node = self.choose_source(chunk)
                if node is None:
                    pending.append(chunk)
                    continue
                attempt = self.start_attempt(chunk, node)
                try:
                    future = self.executor.submit(self.fetch_chunk, chunk, node, attempt)
                except RuntimeError:
                    self.release_attempt(node)
                    raise
                self.futures[future] = (chunk, node, attempt)

            if self.futures:
                wait(self.futures, timeout=min(0.05, max(0.0, self.deadline - time.monotonic())),
                     return_when=FIRST_COMPLETED)
            elif pending or retry_wait:
                if all(node.quarantined for node in self.nodes.values()):
                    raise ProjectError("NO_VALID_SOURCE", "no valid CDN remains")
                # Cooldown/backoff waits remain interruptible and consume no
                # chunk attempt. The task deadline also bounds Retry-After.
                self.stop.wait(min(0.05, max(0.0, self.deadline - time.monotonic())))

    def verify_file(self):
        self.check()
        self.file.flush()
        self.file.seek(0)
        digest, size = hashlib.sha256(), 0
        while True:
            self.check()
            block = self.file.read(65536)
            if not block:
                break
            digest.update(block)
            size += len(block)
        if size != self.manifest["file_size"] or digest.hexdigest() != self.manifest["file_sha256"]:
            raise ProjectError("FILE_INTEGRITY_ERROR", "whole file does not match manifest")
        os.fsync(self.file.fileno())

    def shutdown_requests(self):
        self.request_stop.set()
        self.measurement_stop.set()
        if self.measurement_thread is not None and self.measurement_thread.ident is not None:
            self.measurement_thread.join()
        if self.executor is not None:
            self.executor.shutdown(wait=True, cancel_futures=True)
        for chunk, node, _ in self.futures.values():
            self.release_attempt(node)
            if chunk.phase == "IN_FLIGHT":
                chunk.phase = "RETRY_WAIT"
        self.futures.clear()
        self.transport.close()

    def run(self):
        self.client.events.emit("download_started", request_id=self.request_id, file_id=self.file_id)
        try:
            self.check()
            # Reject unsafe output paths before any data or directory writes.
            self.output_target()
            self.load_manifest()
            self.create_temporary()
            try:
                self.start_measurements()
                self.download_chunks()
            finally:
                self.shutdown_requests()
            self.verify_file()
            self.file.close()
            self.file = None
            if self.output_target() != self.target:
                raise ProjectError("INVALID_OUTPUT_PATH", "output path changed")
            self.check()  # Last cancellation/deadline check before atomic commit.
            os.replace(self.temp_path, self.target)
            self.temp_path = None
        finally:
            self.shutdown_requests()
            if self.file is not None:
                self.file.close()
            if self.temp_path is not None:
                self.temp_path.unlink(missing_ok=True)
            self.client.summary_log.emit("recovery_summary", request_id=self.request_id, file_id=self.file_id,
                                    verified_chunks=self.verified, attempts=self.attempts,
                                    failed_attempts=self.failed_attempts,
                                    retry_budget_per_chunk=self.options.max_attempts,
                                    quarantined_cdns=[n.node_id for n in self.nodes.values() if n.quarantined])
            self.client.summary_log.emit("measurement_summary", request_id=self.request_id, file_id=self.file_id,
                                    estimates=self.measurements.snapshot(),
                                    completed_chunks={n.node_id: n.completed for n in self.nodes.values()})
        self.client._completed_fields = {
            "file_size": self.manifest["file_size"],
            "file_sha256": self.manifest["file_sha256"], "output_path": str(self.target),
        }
        return True


def run_download(client, command):
    return DownloadTask(client, command).run()
