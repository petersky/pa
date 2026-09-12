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
    for status, text in [('failed', 'PA MCP client timed out after 30 seconds'), ('completed', '')]:
        send({'jsonrpc': '2.0', 'method': 'session/update', 'params': {
            'sessionId': 'native-current', 'update': {
                'sessionUpdate': 'tool_call', 'toolCallId': 'mcp_startup.pa',
                'title': 'PA MCP startup', 'status': status,
                'content': [{'type': 'content', 'content': {'type': 'text', 'text': text}}],
            },
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
