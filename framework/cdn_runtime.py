from __future__ import annotations
import argparse, sys
from framework.common import Handler, ProjectError, Server, integer, load_config, serve, valid_id

class CDNHandler(Handler):

    def route(self, path: str, request_id: str) -> None:
        app: CDN = self.server.app
        if path == '/health':
            self.send_json(200, {'status': 'ok', 'role': 'cdn', 'cdn_id': app.node_id})
            return
        if path == '/probe':
            self.send_body(200, b'OK', 'text/plain', {'X-CDN-ID': app.node_id})
            return
        if path == '/status':
            with app.lock:
                active = app.active
            self.send_json(200, {'cdn_id': app.node_id, 'active_requests': active, 'max_active_requests': app.limit, 'queue_length': 0})
            return
        fields = path.strip('/').split('/')
        if len(fields) != 3 or fields[0] != 'chunk':
            raise ProjectError('NOT_FOUND', 'unknown endpoint', 404)
        file_id = valid_id(fields[1])
        if not fields[2].isascii() or not fields[2].isdigit() or len(fields[2]) > 12:
            raise ProjectError('INVALID_CHUNK_ID', 'invalid nonnegative chunk_id')
        chunk_id = int(fields[2])
        if not app.slots.acquire(blocking=False):
            self.send_json(503, {'error': {'code': 'BUSY', 'message': 'chunk service slots full'}}, {'Retry-After': '1'})
            return
        with app.lock:
            app.active += 1
        try:
            app.events.emit('chunk_request_started', request_id=request_id, file_id=file_id, chunk_id=chunk_id)
            data, cache_status, digest = app.get_chunk(file_id, chunk_id, request_id)
            sent = self.send_body(200, data, extra={'X-Cache': cache_status, 'X-CDN-ID': app.node_id, 'X-Chunk-SHA256': digest, 'X-Request-ID': request_id})
            app.events.emit('chunk_request_finished', request_id=request_id, file_id=file_id, chunk_id=chunk_id, body_bytes_written=sent)
        finally:
            with app.lock:
                app.active -= 1
            app.slots.release()

def main(cdn_type) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()
    try:
        config, _ = load_config(args.config)
        app = cdn_type(config)
        server = Server((config.get('host', '127.0.0.1'), integer(config.get('port', 0), 'port', 0, 65535)), CDNHandler)
        server.app = app
        serve(server, app.events)
        return 0
    except (ProjectError, KeyError, ValueError, OSError) as exc:
        print(f'cdn startup failed: {exc}', file=sys.stderr)
        return 2
