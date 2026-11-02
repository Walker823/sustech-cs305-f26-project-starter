import threading
from framework.common import Events, ProjectError, integer, resolve_path, valid_id
from framework.client_runtime import main
from student.download import run_download


class Client:
    mode = "student"

    def __init__(self, config, base):
        self.config = config
        self.base = base
        self.node_id = valid_id(config.get("client_id", "client"))
        self.strategy = config.get("strategy", "adaptive")
        self.max_inflight = integer(config.get("max_inflight_chunks", 4), "max_inflight_chunks", 1, 32)
        self.output_root = resolve_path(base, config.get("output_root", "output"))
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.events = Events("client", self.node_id)
        self.busy = threading.Event()
        self.cancelled = threading.Event()

    def download(self, command):
        try:
            return run_download(self, command)
        except (ProjectError, OSError, ValueError, KeyError, TypeError) as exc:
            self.events.emit("download_failed", request_id=command["request_id"],
                             file_id=command.get("file_id"),
                             code=exc.code if isinstance(exc, ProjectError) else "LOCAL_ERROR",
                             message=str(exc))
            return False
        finally:
            self.busy.clear()


if __name__ == "__main__":
    raise SystemExit(main(Client))
