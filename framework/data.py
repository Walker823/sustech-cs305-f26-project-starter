"""Deterministic local test data. File IDs are immutable within an experiment."""
from __future__ import annotations
import argparse
import hashlib
import json
from pathlib import Path
from framework.common import MAX_CHUNK_BYTES, integer, valid_id


def make_file(root: Path, file_id: str, size: int, chunk_size: int = 524288,
              seed: str = "cs305-m1") -> dict:
    valid_id(file_id)
    integer(size, "size", 0, 1024**4)
    integer(chunk_size, "chunk_size", 1, MAX_CHUNK_BYTES)
    folder = root / file_id
    folder.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256()
    chunks = []
    with (folder / "source.bin").open("wb") as source:
        for index, offset in enumerate(range(0, size, chunk_size)):
            length = min(chunk_size, size - offset)
            material = f"{seed}\0{file_id}\0{index}".encode()
            data = hashlib.shake_256(material).digest(length)
            (folder / f"{index}.bin").write_bytes(data)
            source.write(data)
            digest.update(data)
            chunks.append({"id": index, "size": length, "sha256": hashlib.sha256(data).hexdigest()})
    manifest = {"file_id": file_id, "file_size": size, "chunk_size": chunk_size,
                "file_sha256": digest.hexdigest(), "chunks": chunks}
    (folder / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description="生成可复现文件、chunk 和 manifest")
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--file-id", default="demo")
    parser.add_argument("--size", type=int, default=3 * 524288 + 137)
    parser.add_argument("--chunk-size", type=int, default=524288)
    parser.add_argument("--seed", default="cs305-m1")
    args = parser.parse_args()
    manifest = make_file(args.root, args.file_id, args.size, args.chunk_size, args.seed)
    print(json.dumps({"event": "data_created", "file_id": args.file_id,
                      "file_size": manifest["file_size"], "file_sha256": manifest["file_sha256"]}))


if __name__ == "__main__":
    main()
