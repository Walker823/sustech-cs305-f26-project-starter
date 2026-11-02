"""Teacher-owned origin. Audit files are not exposed as an HTTP API."""
from __future__ import annotations
import argparse
import hmac
import json
import sys
from pathlib import Path
from framework.common import (Events, Handler, ProjectError, Server, integer, load_config,
                           resolve_path, serve, sha256, valid_id, validate_manifest)


class Origin:
    def __init__(self, config: dict, base: Path):
        self.root = resolve_path(base, config["data_root"])
        self.token = config["chunk_access_token"]
        if not isinstance(self.token, str) or (len(self.token) < 16 or not self.token.isascii()):
            raise ProjectError("INVALID_CONFIG", "chunk_access_token must contain at least 16 characters")
        audit = resolve_path(base, config["audit_path"])
        self.events = Events("origin", config.get("node_id", "origin"), audit)

    def manifest(self, file_id: str) -> dict:
        valid_id(file_id)
        try:
            value = json.loads((self.root / file_id / "manifest.json").read_text(encoding="utf-8"))
        except FileNotFoundError as exc:
            raise ProjectError("FILE_NOT_FOUND", "unknown file_id", 404) from exc
        return validate_manifest(value, file_id)


class OriginHandler(Handler):
    def route(self, path: str, request_id: str) -> None:
        app: Origin = self.server.app
        if path == "/health":
            self.send_json(200, {"status": "ok", "role": "origin"})
            return
        fields = path.strip("/").split("/")
        if len(fields) == 2 and fields[0] == "manifest":
            manifest = app.manifest(fields[1])
            app.events.emit("origin_manifest_request", request_id=request_id, file_id=fields[1])
            self.send_json(200, manifest)
            return
        if len(fields) == 3 and fields[0] == "chunk":
            file_id = valid_id(fields[1])
            if not fields[2].isascii() or not fields[2].isdigit() or len(fields[2]) > 12:
                raise ProjectError("INVALID_CHUNK_ID", "chunk_id must be a nonnegative decimal integer")
            chunk_id = int(fields[2])
            token = self.headers.get("X-Origin-Token", "")
            if not hmac.compare_digest(token, app.token):
                app.events.emit("origin_access_denied", request_id=request_id,
                                file_id=file_id, chunk_id=chunk_id)
                raise ProjectError("ORIGIN_CHUNK_FORBIDDEN", "chunk access is reserved for CDN", 403)
            manifest = app.manifest(file_id)
            if chunk_id >= len(manifest["chunks"]):
                raise ProjectError("CHUNK_NOT_FOUND", "unknown chunk_id", 404)
            meta = manifest["chunks"][chunk_id]
            data = (app.root / file_id / f"{chunk_id}.bin").read_bytes()
            if len(data) != meta["size"] or sha256(data) != meta["sha256"]:
                raise ProjectError("ORIGIN_DATA_INVALID", "origin data does not match manifest", 500)
            # Count requests before writing. A request is NOT a proof of delivered bytes.
            app.events.emit("origin_chunk_request", request_id=request_id, file_id=file_id,
                            chunk_id=chunk_id, body_bytes_expected=len(data))
            sent = self.send_body(200, data, extra={"X-Chunk-SHA256": meta["sha256"],
                                                   "X-Request-ID": request_id})
            app.events.emit("origin_chunk_sent", request_id=request_id, file_id=file_id,
                            chunk_id=chunk_id, body_bytes_written=sent)
            return
        raise ProjectError("NOT_FOUND", "unknown endpoint", 404)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    args = parser.parse_args()
    try:
        config, base = load_config(args.config)
        app = Origin(config, base)
        server = Server((config.get("host", "127.0.0.1"), integer(config.get("port", 0), "port", 0, 65535)), OriginHandler)
        server.app = app
        serve(server, app.events)
        return 0
    except (ProjectError, KeyError, ValueError, OSError) as exc:
        print(f"origin startup failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
