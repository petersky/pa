"""Run with `uv run python -m tests.restart_browser_validation`.

Uses PA's browser HTTP operations and tool-free ACP fixture. No /recover,
queue/resume, or Retry live state operation is used. Requires local Chromium.
"""
import base64
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from uuid import uuid4

import httpx

ROOT = Path(__file__).resolve().parents[1]
ARTIFACTS = ROOT / '.dev' / 'restart-validation'
BASE = 'http://127.0.0.1:8097'


def main():
    with socket.socket() as probe:
        if probe.connect_ex(('127.0.0.1', 8097)) == 0:
            raise RuntimeError('Port 8097 is occupied; stop only the existing isolated fixture first.')
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    for key in ('PA_EXECUTION_CONTEXT', 'PA_SYNC_TOKEN_FILE'):
        env.pop(key, None)
    env['PA_OWNER_API_URL'] = BASE
    legacy = "--legacy-queued" in sys.argv
    calls = []
    processes = []
    logs = []
    client = httpx.Client(base_url=BASE, timeout=120)

    def call(method, path, body=None):
        calls.append((method, path))
        response = client.request(method, path, json=body, headers={
            'X-CSRF-Token': client.cookies.get('pa_csrf', ''),
            'Idempotency-Key': 'browser-validation-' + str(uuid4()),
        })
        response.raise_for_status()
        return response.json()

    def start(generation):
        log = open(ARTIFACTS / f'automated-browser-{generation}.log', 'w')
        logs.append(log)
        child_env = dict(env)
        if legacy and generation == 1:
            child_env["PA_FIXTURE_LEGACY_PRODUCER"] = "1"
        else:
            child_env.pop("PA_FIXTURE_LEGACY_PRODUCER", None)
        process = subprocess.Popen([
            sys.executable, '-m', 'uvicorn', 'tests.restart_browser_app:create_app',
            '--factory', '--host', '127.0.0.1', '--port', '8097',
            '--timeout-graceful-shutdown', '3',
        ], cwd=ROOT, env=child_env, stdout=log, stderr=subprocess.STDOUT)
        processes.append(process)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert process.poll() is None, 'Fixture exited during startup'
            try:
                if client.get('/api/ready', timeout=2).status_code == 200:
                    return process
            except httpx.HTTPError:
                pass
            time.sleep(.25)
        raise TimeoutError('Isolated fixture startup timed out')

    try:
        first = start(1)
        session = call('POST', '/api/agent/sessions', {
            'provider': 'codex', 'purpose': 'chat', 'model_id': 'fixture-balanced',
            'mode_id': 'default', 'title': 'Automated browser restart acceptance',
        })['session']
        sid, provider_id = session['id'], session['external_session_id']
        assert session['control_mode'] == 'human'
        browser = {'agent_session_id': sid}
        url = BASE + '/agent?session=' + sid
        call('POST', '/api/browser/attach', {**browser, 'url': url})
        snapshot = call('POST', '/api/browser/snapshot', browser)
        textarea = next(e['ref'] for e in snapshot['elements'] if e['tag'] == 'textarea')
        send = next(e['ref'] for e in snapshot['elements'] if e['tag'] == 'button' and e['text'] == 'Send')
        call('POST', '/api/browser/type', {**browser, 'ref': textarea, 'text': 'Initial browser fixture turn.'})
        call('POST', '/api/browser/click', {**browser, 'ref': send})
        # Close the inspection browser so its SSE connection cannot hold raw
        # uvicorn shutdown open. This is not an agent recovery operation.
        call('POST', '/api/browser/detach', browser)
        if legacy:
            seeded = call('POST', f'/fixture/queued-legacy-restart/{sid}', {})
            receipt = seeded['receipt']
            assert seeded['agent_env']['PA_EXECUTION_CONTEXT']
        else:
            receipt = call('POST', f'/api/agent/sessions/{sid}/restart-handoffs', {
                'continuation_prompt': 'Continue this exact human chat once automatically.',
                'idempotency_key': 'browser-restart-' + str(uuid4()),
            })
        first.wait(timeout=60)
        start(2)
        deadline = time.monotonic() + 45
        while True:
            state = call('GET', f'/api/agent/sessions/{sid}')
            delivered = next(r for r in state['restart_handoffs'] if r['id'] == receipt['id'])
            if delivered['status'] == 'continuation_delivered':
                break
            assert time.monotonic() < deadline, delivered
            time.sleep(.25)
        assert state['session']['external_session_id'] == provider_id
        assert state['session']['control_mode'] == 'human'
        if legacy:
            assert [p['id'] for p in state['queue']] == ['held-automation']
            assert state['session']['execution_binding'] == seeded['binding']
            assert delivered['execution_binding'] == seeded['binding']
            assert not any(k in seeded['binding'] for k in ('dispatch_id', 'realm_id', 'principal_id'))
        else:
            assert not state['queue']
        history = call('GET', f'/api/agent/history/{sid}?limit=250')['events']
        prompt_id = receipt['continuation_prompt_id']
        assert sum(e['event_type'] == 'user_message' and e['payload'].get('id') == prompt_id for e in history) == 1
        assert sum(e['event_type'] == 'turn_completed' and e['payload'].get('queued_prompt_id') == prompt_id for e in history) == 1
        starts = [e for e in history if e['event_type'] == 'session_started']
        assert len(starts) == 2
        assert {e['payload']['external_session_id'] for e in starts} == {provider_id}
        call('POST', '/api/browser/attach', {**browser, 'url': url})
        snapshot = call('POST', '/api/browser/snapshot', browser)
        assert 'Continue this exact human chat once automatically.' in snapshot['document']['body_text']
        screenshot = call('POST', '/api/browser/screenshot', browser)
        (ARTIFACTS / 'automated-browser-final.png').write_bytes(base64.b64decode(screenshot['data_base64']))
        call('POST', '/api/browser/detach', browser)
        assert not any('/recover' in path or '/queue/resume' in path for _, path in calls)
        assert not any(e['event_type'] == 'user_message' and e['payload'].get('id') == 'held-automation' for e in history)
        evidence = {'session_id': sid, 'provider_id': provider_id, 'receipt': delivered,
                    'calls': calls, 'provider_starts': len(starts), 'control_mode': 'human'}
        if legacy:
            evidence['legacy_seed'] = seeded
        (ARTIFACTS / ('legacy-browser-evidence.json' if legacy else 'automated-browser-evidence.json')).write_text(json.dumps(evidence, indent=2))
        print(json.dumps({'session_id': sid, 'receipt_id': receipt['id'], 'result': 'passed'}))
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
        client.close()
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
