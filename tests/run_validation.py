"""Run the regular regression suite and retain logs plus source fingerprints."""
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]


def main():
    folder = ROOT / "runtime/validation-logs"
    folder.mkdir(parents=True, exist_ok=True)
    jobs = [("public", ["tools/public_tests.py"]),
            ("transport", ["tests/transport_checks.py"]),
            ("cache_optimizations", ["tests/cache_optimizations.py"])]
    jobs += [("extra_" + stage, ["tests/extended_tests.py", "--stage", stage])
             for stage in ("cdn", "download", "concurrency", "recovery", "interactive", "measurement")]
    results = []
    for name, arguments in jobs:
        started = time.monotonic()
        process = subprocess.Popen([sys.executable, "-u", *arguments], cwd=ROOT,
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
                                   start_new_session=True,
                                   env={**os.environ, "PYTHONDONTWRITEBYTECODE": "1"})
        try:
            stdout, stderr = process.communicate(timeout=60)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                stdout, stderr = process.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                stdout, stderr = process.communicate()
            stderr += "\nValidation job exceeded 60 seconds.\n"
        passed = process.returncode == 0
        (folder / (name + ".txt")).write_text(stdout + "\nSTDERR:\n" + stderr, encoding="utf-8")
        result = {"name": name, "passed": passed, "exit_code": process.returncode,
                  "elapsed_s": round(time.monotonic() - started, 3)}
        results.append(result)
        print(("PASS " if passed else "FAIL ") + name + ": " + str(result["elapsed_s"]) + "s", flush=True)
    report = {"passed": all(row["passed"] for row in results), "python": sys.version,
              "jobs": results,
              "student_sha256": {path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                                  for path in sorted((ROOT / "student").glob("*.py"))}}
    output = ROOT / "runtime/validation-results.json"
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Validation report: " + str(output), flush=True)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
