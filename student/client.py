import threading
import time
from framework.common import Events, ProjectError, base_url, resolve_path, valid_id
from framework.client_runtime import main
from student.download import run_download
from student.state import DownloadOptions
from student.util import EventBudget


class Client:
    mode = "student"

    def __init__(self, config, base):
        self.config = config
        self.base = base
        self.node_id = valid_id(config.get("client_id", "client"))
        self.strategy = config.get("strategy", "adaptive")
        if self.strategy not in ("adaptive", "fixed", "round_robin"):
            raise ProjectError("INVALID_CONFIG", "strategy must be adaptive, fixed or round_robin")
        self.options = DownloadOptions.from_config(config)
        self.max_inflight = self.options.max_inflight
        self.origin_url = base_url(config["origin_url"])
        sources = config["cdns"]
        if not isinstance(sources, list) or not 1 <= len(sources) <= 8:
            raise ProjectError("INVALID_CONFIG", "cdns must contain 1 to 8 sources")
        self.sources = []
        seen = set()
        for source in sources:
            if not isinstance(source, dict):
                raise ProjectError("INVALID_CONFIG", "CDN entry must be an object")
            node_id = valid_id(source["id"])
            if node_id in seen:
                raise ProjectError("INVALID_CONFIG", "CDN IDs must be distinct")
            seen.add(node_id)
            self.sources.append({"id": node_id, "url": base_url(source["url"])})
        if self.strategy == "fixed":
            selected = config.get("fixed_cdn_id", self.sources[0]["id"])
            self.sources = [source for source in self.sources if source["id"] == selected]
            if not self.sources:
                raise ProjectError("INVALID_CONFIG", "fixed_cdn_id is not a configured source")
        self.output_root = resolve_path(base, config.get("output_root", "output"))
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.events = Events("client", self.node_id)
        self.log = EventBudget(self.events)
        self.summary_log = EventBudget(self.events)
        self.busy = threading.Event()
        self.cancelled = threading.Event()

    def download(self, command):
        command = {**command, "_accepted_at": time.monotonic()}
        self._completed_fields = {}
        try:
            ok = run_download(self, command)
        except (ProjectError, OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
            self.busy.clear()
            self.events.emit("download_failed", request_id=command["request_id"],
                             file_id=command.get("file_id"),
                             code=exc.code if isinstance(exc, ProjectError) else "LOCAL_ERROR",
                             message=str(exc))
            return False
        # No task cleanup follows the terminal event: a caller may immediately
        # send the next DOWNLOAD when it sees download_complete/download_failed.
        self.busy.clear()
        if ok:
            self.events.emit("download_complete", request_id=command["request_id"],
                             file_id=command.get("file_id"), **self._completed_fields)
        else:
            self.events.emit("download_failed", request_id=command["request_id"],
                             file_id=command.get("file_id"), code="LOCAL_ERROR",
                             message="download did not commit its output")
        return bool(ok)


if __name__ == "__main__":
    raise SystemExit(main(Client))
