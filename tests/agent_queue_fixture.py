"""Isolated queue UI with real runtime enqueue/reorder and SSE; no provider."""
import tempfile
from pathlib import Path
from unittest.mock import Mock

import uvicorn
from fastapi import Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

from pa.instance.quiesce import QueuedPrompt
from pa.modules.agent_chat import session_events
from tests.agent_chat_live_fixture import build_fixture


def build_queue_fixture(directory):
    app = build_fixture(directory)
    replaced = {'/', '/api/agent/sessions/{session_id}', '/api/agent/{path:path}'}
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', '') not in replaced]
    runtime = app.state.ctx.require_service('agent_sessions').get('live-fixture')
    runtime._start_drain = Mock()
    runtime.pause_queue()
    runtime.enqueue('Automated update', source='pr-supervisor', prompt_id='auto')
    runtime.enqueue('First human message', source='ui', prompt_id='human-1')
    runtime.enqueue('Second human message <img src=x onerror=alert(1)>', source='ui', prompt_id='human-2')
    runtime.enqueue('Correlated approval response', source='notification-response', prompt_id='response')
    app.state.runtime = runtime
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / 'src/pa/server/templates'))
    widget = env.get_template('partials/agent/chat-widget.html').render(session_id='live-fixture', current_instance_id='live-fixture', telemetry_enabled=False)

    @app.get('/', response_class=HTMLResponse)
    async def page():
        return '''<!doctype html><html lang="en" data-theme="pa" data-appearance="light" data-pa-instance-id="live-fixture"><title>Isolated queue cues</title>
        <meta name="viewport" content="width=device-width,initial-scale=1"><link rel="stylesheet" href="/static/style.css"><link rel="stylesheet" href="/static/themes/pa/light.css"><style>body{padding:.5rem}.agent-chat-widget{height:85vh;min-height:0}</style>
        <script src="/static/js/agent-chat-drafts.js" defer></script><script src="/static/js/agent-chat-draft-widget.js" defer></script>
        <script src="/static/js/session-recovery.js" defer></script><script src="/static/js/agent-chat.js" defer></script>
        <h1>Queue cue fixture</h1><ul data-agent-session-list data-agent-enabled="true"></ul><main class="page-agent">''' + widget + '''</main>
        <button onclick="fetch('/fixture/running',{method:'POST'})">Simulate active reply</button>
        <button onclick="fetch('/fixture/held',{method:'POST'})">Simulate human control</button></html>'''

    @app.get('/api/agent/sessions/{session_id}/events')
    async def events(request: Request, session_id: str):
        return await session_events(request, session_id)

    @app.get('/api/agent/sessions/{session_id}')
    async def snapshot(session_id: str):
        return {**runtime.snapshot(include_transcript=False), 'connected': True}

    @app.post('/api/agent/sessions/{session_id}/queue/{action}')
    async def action(session_id: str, action: str, request: Request):
        if action == 'reorder':
            runtime.reorder_queue((await request.json())['prompt_ids'])
        elif action == 'pause': runtime.pause_queue()
        elif action == 'resume': runtime.resume_queue()
        return {'queue_paused': runtime.queue_paused}

    @app.post('/fixture/{state}')
    async def state(state: str):
        if state == 'running':
            runtime.resume_queue()
            runtime._in_flight = QueuedPrompt(message='Current reply')
            runtime._append_transcript('user_message', {'message':'Current reply'})
        elif state == 'held':
            runtime.reorder_queue(['auto', 'human-1', 'human-2', 'response'])
            await runtime.set_control_mode('human')
        return {'ok':True}

    return app


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='pa-queue-cues-') as directory:
        uvicorn.run(build_queue_fixture(Path(directory)), host='127.0.0.1', port=8093, log_level='warning')
