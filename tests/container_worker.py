"""Trusted test wrapper, mounted read-only; never included in submission ZIP."""
import hashlib
import json
import os
from pathlib import Path
import resource
import signal
import subprocess
import sys
import time


def integer_file(path):
    try:
        return int(Path(path).read_text().strip())
    except (OSError, ValueError):
        return None


def main():
    config = json.loads(Path("/run/project-config.json").read_text())
    seed = config.get("_test_seed_bytes", 0)
    if seed:
        with Path("/output/result.bin").open("wb") as stream:
            for offset in range(0, seed, 65536):
                stream.write(b"Z" * min(65536, seed - offset))
    started = time.monotonic()
    process = subprocess.Popen([sys.executable, "-u", *sys.argv[1:]])
    signal.signal(signal.SIGTERM, lambda sig, _: process.send_signal(sig) if process.poll() is None else None)
    signal.signal(signal.SIGINT, lambda sig, _: process.send_signal(sig) if process.poll() is None else None)
    threads = fds = rss = 0
    while process.poll() is None:
        try:
            status = (Path("/proc") / str(process.pid) / "status").read_text()
            values = {line.split(":", 1)[0]: line.split(":", 1)[1].strip() for line in status.splitlines() if ":" in line}
            threads = max(threads, int(values.get("Threads", "0")))
            rss = max(rss, int(values.get("VmRSS", "0 kB").split()[0]) * 1024)
            fds = max(fds, len(list((Path("/proc") / str(process.pid) / "fd").iterdir())))
        except (OSError, ValueError):
            pass
        time.sleep(0.005)
    code = process.wait()
    output = {}
    for path in Path("/output").rglob("*"):
        if path.is_file() and not path.is_symlink():
            digest = hashlib.sha256()
            with path.open("rb") as stream:
                for data in iter(lambda: stream.read(65536), b""):
                    digest.update(data)
            output[str(path.relative_to("/output"))] = {"size": path.stat().st_size, "sha256": digest.hexdigest()}
    cgroup = Path("/sys/fs/cgroup")
    report = {"event": "resource_witness", "exit_code": code,
              "runtime_s": round(time.monotonic() - started, 4), "uid": os.getuid(),
              "python": sys.version.split()[0], "peak_child_threads": threads,
              "peak_child_fds": fds, "peak_child_rss_bytes": rss,
              "memory_peak_bytes": integer_file(cgroup / "memory.peak"),
              "pids_peak": integer_file(cgroup / "pids.peak"),
              "memory_limit": integer_file(cgroup / "memory.max"),
              "pids_limit": integer_file(cgroup / "pids.max"),
              "cpu_max": (cgroup / "cpu.max").read_text().strip(),
              "nofile_limit": list(resource.getrlimit(resource.RLIMIT_NOFILE)),
              "tmp_bytes": os.statvfs("/tmp").f_blocks * os.statvfs("/tmp").f_frsize,
              "output_bytes": os.statvfs("/output").f_blocks * os.statvfs("/output").f_frsize,
              "outputs": output}
    print(json.dumps(report), flush=True)
    return code if code >= 0 else 128 - code


if __name__ == "__main__":
    raise SystemExit(main())
