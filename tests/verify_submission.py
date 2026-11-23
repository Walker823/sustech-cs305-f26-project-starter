"""Audit the packaged sources against the last successful regression and image."""
import argparse
import ast
import hashlib
import json
from pathlib import Path
import stat
import subprocess
import sys
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def require(condition, message):
    if not condition:
        raise AssertionError(message)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--archive", type=Path, default=ROOT / "submission-v2.zip")
    parser.add_argument("--image", help="optional local Podman image to compare; requires Podman access")
    args = parser.parse_args()
    sources = {path.name: path.read_bytes() for path in sorted((ROOT / "student").glob("*.py"))}
    hashes = {name: hashlib.sha256(data).hexdigest() for name, data in sources.items()}
    validation = json.loads((ROOT / "runtime/validation-results.json").read_text())
    require(validation["passed"] and hashes == validation["student_sha256"],
            "package sources do not match the final successful regression")
    for name, data in sources.items():
        require(not (ROOT / "student" / name).is_symlink(), "source symlink")
        tree = ast.parse(data, filename=name)
        for node in ast.walk(tree):
            names = ([entry.name for entry in node.names] if isinstance(node, ast.Import)
                     else [node.module] if isinstance(node, ast.ImportFrom) and node.module else [])
            require(all(module.split(".")[0] in sys.stdlib_module_names | {"student", "framework"}
                        for module in names), "unexpected source dependency: " + name)
    original = json.loads((ROOT / "PUBLIC-MANIFEST.json").read_text())
    for name, digest in original.items():
        if not name.startswith("student/"):
            require(hashlib.sha256((ROOT / name).read_bytes()).hexdigest() == digest,
                    "teacher-owned file was modified: " + name)
    for path in (ROOT / ".vscode").glob("*.json"):
        json.loads(path.read_text(encoding="utf-8"))

    require(args.archive.stat().st_size <= 8 * 1024**2, "ZIP exceeds 8 MiB")
    with zipfile.ZipFile(args.archive) as archive:
        entries = archive.infolist()
        expected = {"submission.json"} | {"student/" + name for name in sources}
        require(len(entries) <= 64 and len(entries) == len(expected)
                and {entry.filename for entry in entries} == expected, "unexpected/duplicate ZIP entries")
        require(sum(entry.file_size for entry in entries) <= 4 * 1024**2, "expanded ZIP exceeds 4 MiB")
        require(archive.testzip() is None, "bad ZIP CRC")
        for entry in entries:
            require(entry.file_size <= 256 * 1024 and entry.file_size <= 200 * max(1, entry.compress_size),
                    "entry size/compression ratio exceeds protocol")
            require(not stat.S_ISLNK(entry.external_attr >> 16), "ZIP symlink")
        require(json.loads(archive.read("submission.json")) == {
            "format": "cs305-submission-v1", "client_entry": "student.client", "cdn_entry": "student.cdn",
        }, "invalid submission descriptor")
        for name, data in sources.items():
            require(archive.read("student/" + name) == data, "ZIP source mismatch: " + name)

    if args.image:
        code = ("import hashlib,json,pathlib; print(json.dumps({p.name:hashlib.sha256(p.read_bytes()).hexdigest() "
                "for p in pathlib.Path('/app/student').glob('*.py')}))")
        process = subprocess.run([
            "podman", "run", "--rm", "--network=none", "--read-only", "--cap-drop=all",
            "--security-opt=no-new-privileges", "--memory=256m", "--cpus=1", "--pids-limit=64",
            "--ulimit=nofile=128:128", "--env=PYTHONDONTWRITEBYTECODE=1",
            args.image, "python", "-c", code,
        ], check=True, capture_output=True, text=True)
        require(json.loads(process.stdout) == hashes, "tested container image differs from ZIP sources")
    report = {"passed": True, "archive": args.archive.name,
              "zip_bytes": args.archive.stat().st_size,
              "zip_sha256": hashlib.sha256(args.archive.read_bytes()).hexdigest(),
              "entry_count": len(entries), "student_sha256": hashes, "image": args.image,
              "teacher_files_unchanged": True, "matches_successful_regression": True}
    (ROOT / "runtime/submission-verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report), flush=True)
    print("PASS submission: format/limits/CRC; identical sources; stdlib imports; original teacher files"
          + ("; identical container image" if args.image else ""))


if __name__ == "__main__":
    main()
