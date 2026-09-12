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



for line in sys.stdin:
    request = json.loads(line)
    method = request.get('method')
    if method == 'initialize':
        result = {'protocolVersion': 1, 'agentCapabilities': {}, 'agentInfo': {'name': 'isolated-test', 'version': '1'}}
    elif method == 'session/new':
        result = {'sessionId': 'native-current'}
    else:
        continue
    send({'jsonrpc': '2.0', 'id': request['id'], 'result': result})
    if method == 'session/new':
        threading.Thread(target=startup, daemon=True).start()
