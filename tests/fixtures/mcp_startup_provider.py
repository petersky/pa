"""Isolated ACP wire fixture; never launches a real provider or accesses PA data."""
import json
import sys
import threading
import time

lock = threading.Lock()


def send(value):
    with lock:
        print(json.dumps(value), flush=True)


def startup():
    time.sleep(30)
    from pathlib import Path
    contract = json.loads(Path(__file__).with_name('codex_acp_1_11_mcp_events.json').read_text())
    start = {**contract['success'], 'sessionUpdate': 'tool_call', 'title': 'mcp.pa.list_items', 'kind': 'other', 'status': 'in_progress', 'rawOutput': None}
    for update in [*contract['failed'], start, contract['success']]:
        send({'jsonrpc': '2.0', 'method': 'session/update', 'params': {
            'sessionId': 'native-current', 'update': update,
        }})
        time.sleep(0.5)


def history_update():
    from pathlib import Path
    contract = json.loads(Path(__file__).with_name('codex_acp_1_11_mcp_events.json').read_text())
    return {**contract['success'], 'sessionUpdate': 'tool_call', 'title': 'mcp.pa.list_items', 'kind': 'other'}


def emit_update(update):
    send({'jsonrpc': '2.0', 'method': 'session/update', 'params': {
        'sessionId': 'native-current', 'update': update,
    }})


def after_load():
    time.sleep(0.2)
    emit_update(history_update())
    time.sleep(0.2)
    emit_update({**history_update(), 'toolCallId': 'live-after-load', 'status': 'in_progress', 'rawOutput': None})
    emit_update({**history_update(), 'toolCallId': 'live-after-load'})


for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion': 1, 'agentCapabilities': {}, 'agentInfo': {'name': 'isolated-test', 'version': '1'}}
    elif method == 'session/load':
        if '--explicit-success' not in sys.argv:
            emit_update(history_update())
        result = {}
    elif method == 'session/new':
        result = {'sessionId': 'native-current'}
    else:
        continue
    if method in {'session/new', 'session/load'} and '--explicit-success' in sys.argv:
        emit_update({'sessionUpdate': 'tool_call', 'toolCallId': 'mcp_startup.pa', 'title': 'PA MCP startup', 'status': 'completed'})
        if '--newer-failure' in sys.argv:
            emit_update({'sessionUpdate': 'tool_call', 'toolCallId': 'mcp_startup.pa', 'title': 'PA MCP startup', 'status': 'failed', 'content': [{'type': 'content', 'content': {'type': 'text', 'text': 'PA MCP failed to start: newer failure'}}]})
    send({'jsonrpc': '2.0', 'id': request['id'], 'result': result})
    if method == 'session/new' and '--explicit-success' not in sys.argv:
        threading.Thread(target=startup, daemon=True).start()

    if method == 'session/load' and '--explicit-success' not in sys.argv:
        threading.Thread(target=after_load, daemon=True).start()
