"""Test-only paired proxies with independent rates and an optional shared link."""
import argparse
import json
from pathlib import Path
import signal
import sys
import threading

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from tools.fault_proxy import Link, Proxy  # noqa: E402


class Bottleneck:
    def __init__(self, local, shared, rate):
        self.local, self.shared, self.rate = local, shared, rate

    def take(self, size, rate, stop):
        self.local.take(size, rate, stop)
        self.shared.take(size, self.rate, stop)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    config = json.loads(Path(args.config).read_text())
    stopped = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stopped.set())
    signal.signal(signal.SIGINT, lambda *_: stopped.set())
    shared = Link()
    servers, threads = [], []
    try:
        for entry in config["entries"]:
            server = Proxy(("127.0.0.1", 0), entry["upstream"], entry["faults"], entry["log"])
            if config.get("shared_rate_mbps"):
                server.link = Bottleneck(server.link, shared, config["shared_rate_mbps"] * 1e6 / 8)
            servers.append(server)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": 0.05})
            thread.start()
            threads.append(thread)
        ports = [server.server_address[1] for server in servers]
        print(json.dumps({"event": "ready", "port": ports[0], "ports": ports}), flush=True)
        stopped.wait()
    finally:
        for server in servers:
            server.stop.set()
            server.shutdown()
            server.server_close()
        for thread in threads:
            thread.join()


if __name__ == "__main__":
    main()
