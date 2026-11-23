"""Real SYN-drop checks. Refuses to modify the host network namespace.

Run as root via: unshare --net -- python3 -B tests/connect_timeout.py
"""
import json
import os
from pathlib import Path
import subprocess
import threading
import time

from extended_tests import require
from framework.common import ProjectError
from student.transport import HTTPTransport


def main():
    if os.geteuid() != 0 or os.readlink("/proc/self/ns/net") == os.readlink("/proc/1/ns/net"):
        raise SystemExit("Use root + unshare --net; host firewall modification is refused.")
    subprocess.run(["ip", "link", "set", "lo", "up"], check=True)
    rule = ["INPUT", "-d", "127.0.0.2", "-p", "tcp", "--syn", "--dport", "48765", "-j", "DROP"]
    subprocess.run(["iptables", "-A", *rule], check=True)
    results = []
    transport = HTTPTransport()
    try:
        for name, timeout, budget, cancel_after, expected in (
            ("connect_timeout", 0.15, 2.0, None, "NETWORK_TIMEOUT"),
            ("overall_during_connect", 5.0, 0.12, None, "DOWNLOAD_DEADLINE"),
            ("cancel_during_connect", 5.0, 2.0, 0.04, "CANCELLED"),
        ):
            stop = threading.Event()
            timer = threading.Timer(cancel_after, stop.set) if cancel_after else None
            if timer:
                timer.start()
            started = time.monotonic()
            try:
                transport.get_bytes("http://127.0.0.2:48765", "/probe", connect_timeout_s=timeout,
                                    read_idle_timeout_s=1, absolute_deadline=started + budget,
                                    stop_event=stop)
            except ProjectError as exc:
                require(exc.code == expected, f"{name}: {exc.code} != {expected}")
            else:
                raise AssertionError("SYN-drop connection unexpectedly succeeded")
            elapsed = time.monotonic() - started
            require(elapsed < 0.5, name + " was not bounded/prompt")
            results.append({"case": name, "code": expected, "elapsed_s": round(elapsed, 4)})
            if timer:
                timer.join()
        counters = subprocess.run(["iptables", "-L", "INPUT", "-nvx"], check=True,
                                  capture_output=True, text=True).stdout
        drops = sum(int(line.split()[0]) for line in counters.splitlines()
                    if "DROP" in line and "48765" in line)
        require(drops >= 3, "SYN-drop scenario did not trigger")
        report = {"passed": True, "dropped_syn_packets": drops, "checks": results}
        output = Path(__file__).resolve().parents[1] / "runtime/connect-timeout-results.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(report), flush=True)
    finally:
        transport.close()
        subprocess.run(["iptables", "-D", *rule], check=True)
    print("PASS real_connect_timeout: isolated SYN drop; connect/overall/cancellation budgets")


if __name__ == "__main__":
    main()
