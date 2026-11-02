from __future__ import annotations
import argparse, json, shlex, sys, uuid
from concurrent.futures import ThreadPoolExecutor
from framework.common import ProjectError, load_config, valid_id

def parse_command(line: str) -> dict:
    line = line.strip()
    if line.startswith('{'):
        try:
            value = json.loads(line)
        except ValueError as exc:
            raise ProjectError('INVALID_COMMAND', 'invalid JSON command') from exc
        if not isinstance(value, dict):
            raise ProjectError('INVALID_COMMAND', 'expected JSON object')
    else:
        try:
            words = shlex.split(line)
        except ValueError as exc:
            raise ProjectError('INVALID_COMMAND', 'invalid quoted command') from exc
        if words == ['EXIT']:
            value = {'command': 'EXIT'}
        elif len(words) == 3 and words[0] == 'DOWNLOAD':
            value = {'command': 'DOWNLOAD', 'file_id': words[1], 'output_path': words[2]}
        else:
            raise ProjectError('INVALID_COMMAND', 'expected DOWNLOAD <file_id> <output_path> or EXIT')
    if value.get('command') not in ('DOWNLOAD', 'EXIT'):
        raise ProjectError('INVALID_COMMAND', 'unknown command')
    value['request_id'] = valid_id(value.get('request_id', uuid.uuid4().hex))
    return value

def main(client_type) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    parser.add_argument('--download', help='one-shot mode: file ID')
    parser.add_argument('--output', help='one-shot mode: output path within output_root')
    args = parser.parse_args()
    if bool(args.download) != bool(args.output):
        parser.error('--download and --output must be used together')
    try:
        config, base = load_config(args.config)
        client = client_type(config, base)
    except (ProjectError, KeyError, ValueError, OSError) as exc:
        print(f'client startup failed: {exc}', file=sys.stderr)
        return 2
    client.events.emit('ready', mode=getattr(client, 'mode', 'bounded-multi-cdn-m2'), strategy=client.strategy, max_inflight_chunks=client.max_inflight)
    if args.download:
        ok = client.download({'file_id': args.download, 'output_path': args.output, 'request_id': uuid.uuid4().hex})
        client.events.emit('stopped')
        return 0 if ok else 1
    executor = ThreadPoolExecutor(max_workers=1)
    current = None
    seen: set[str] = set()
    try:
        for line in sys.stdin:
            if not line.strip():
                continue
            try:
                cmd = parse_command(line)
                if cmd['request_id'] in seen:
                    raise ProjectError('DUPLICATE_REQUEST_ID', 'request_id already used')
                seen.add(cmd['request_id'])
                if cmd['command'] == 'EXIT':
                    client.events.emit('exit_pending', request_id=cmd['request_id'])
                    break
                if client.busy.is_set():
                    client.events.emit('command_error', request_id=cmd['request_id'], code='BUSY', message='one download task is already active')
                    continue
                client.busy.set()
                current = executor.submit(client.download, cmd)
            except ProjectError as exc:
                client.events.emit('command_error', code=exc.code, message=exc.message)
    except KeyboardInterrupt:
        client.cancelled.set()
    finally:
        executor.shutdown(wait=True)
        client.events.emit('stopped')
    return 0
