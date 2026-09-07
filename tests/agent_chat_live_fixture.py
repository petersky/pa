"""Tool-free isolated browser fixture using PA's real multiplex SSE handler.

Run with ``uv run python -m tests.agent_chat_live_fixture``. All persistence is
temporary. No provider, running PA service, or production conversation is used.
"""
import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_chat import multiplexed_session_events, get_agent_session_history

PROGRESS = "Fixture progress: the live connection delivered this message."
FINAL = "Fixture final: all assistant text arrived without refreshing."


def build_fixture(directory: Path):
    app = FastAPI()
    app.state.stream_epoch = 0
    app.state.reconnect_cursors = []
    store = CardProjection(directory / "fixture.db")
    settings = Settings(data_dir=directory / "data", workspace_root=directory / "workspaces", instance_id="live-fixture", agent_enabled=False)
    manager = AgentSessionManager(settings, store)
    manager.async_runtime = AsyncRuntime()
    manager._startup_complete = True
    sessions = []
    for sid in ("live-fixture", "other-fixture"):
        session = store.save_session(AgentSession(id=sid, agent_name="codex", purpose="chat"))
        runtime = AgentSessionRuntime(manager, session)
        manager._runtimes[sid] = runtime
        sessions.append(session)
    app.state.ctx = SimpleNamespace(
        settings=settings, store=store, services={"agent_sessions": manager},
        require_service=lambda _: manager,
    )
    root = Path(__file__).parents[1]
    env = Environment(loader=FileSystemLoader(root / "src/pa/server/templates"))
    widget = env.get_template("partials/agent/chat-widget.html").render(
        session_id="live-fixture", current_instance_id="live-fixture", telemetry_enabled=False,
    )

    @app.get("/", response_class=HTMLResponse)
    async def page():
        return '''<!doctype html><html data-pa-instance-id="live-fixture"><title>PA live fixture</title>
        <link rel="stylesheet" href="/static/style.css">
        <script src="/static/js/agent-chat-drafts.js" defer></script>
        <script src="/static/js/agent-chat-draft-widget.js" defer></script>
        <script src="/static/js/session-recovery.js" defer></script>
        <script src="/static/js/agent-chat.js" defer></script>
        <main class="page-agent"><h1>Isolated live delivery fixture</h1>
        <ul data-agent-session-list data-agent-enabled="true"></ul>''' + widget + '''
        <button id="disconnect" onclick="fetch('/fixture/disconnect',{method:'POST'})">Interrupt stream</button>
        <p id="network-result" role="status"></p>
        <p id="result" role="status">Send any fixture prompt. Expected progress and final must appear live.</p>
        </main><script>
        window.addEventListener('DOMContentLoaded',()=>{const p=window.PAAgentChat.AgentChatWidget.prototype;const original=p.applySnapshot;p.applySnapshot=function(s){try{return original.call(this,s);}catch(e){document.querySelector('#result').textContent=e.stack;throw e;}};});
        setInterval(()=>{const texts=Array.from(document.querySelectorAll('.acw-bubble-agent')).map(e=>e.dataset.markdown||'');
        if(texts.includes('PROGRESS_EXPECTED')&&texts.includes('FINAL_EXPECTED'))
        document.querySelector('#result').textContent='PASS exact progress and final received without refresh';
        if(texts.filter(t=>t==='Fixture reconnect: retained final replayed exactly once.').length===1)
        document.querySelector('#network-result').textContent='PASS reconnect replay delivered exact final once without refresh';},100);
        </script></html>'''.replace('PROGRESS_EXPECTED', PROGRESS).replace('FINAL_EXPECTED', FINAL)

    @app.get("/api/agent/session-events")
    async def stream(request: Request):
        epoch = app.state.stream_epoch
        app.state.reconnect_cursors.append(dict(request.query_params))
        response = await multiplexed_session_events(request)
        original = response.body_iterator

        async def interruptible():
            try:
                async for chunk in original:
                    if epoch != app.state.stream_epoch:
                        break
                    yield chunk
            finally:
                await original.aclose()

        response.body_iterator = interruptible()
        return response

    @app.post("/fixture/disconnect")
    async def disconnect():
        app.state.stream_epoch += 1
        async def retained_final():
            await asyncio.sleep(1.05)
            runtime = manager.get("live-fixture")
            runtime._append_transcript("agent_message_chunk", {
                "message_id": "reconnect", "phase": "final",
                "text": "Fixture reconnect: retained final replayed exactly once.",
            })
            runtime._append_transcript("turn_completed", {"stop_reason": "end_turn"})
            runtime._flush_transcript()
        app.state.delivery = asyncio.create_task(retained_final())
        return {"disconnected": True}

    @app.get("/fixture/evidence")
    async def evidence():
        return {"reconnect_cursors": app.state.reconnect_cursors}

    app.get("/api/agent/history/{session_id}")(get_agent_session_history)

    @app.get("/api/fleet/session-route/{session_id}")
    async def route(session_id: str):
        return {"state": "live", "live": True, "api_base": "/api/agent"}

    @app.get("/api/agent/sessions")
    async def listing():
        return [s.model_dump(mode="json") for s in sessions]

    @app.get("/api/agent/sessions/{session_id}")
    async def snapshot(session_id: str):
        return {"session": manager.get(session_id).session.model_dump(mode="json"),
                "connected": True, "prompting": False, "queue": []}

    @app.post("/api/agent/sessions/{session_id}/prompt")
    async def prompt(session_id: str, request: Request):
        body = await request.json()
        runtime = manager.get(session_id)
        runtime._append_transcript("user_message", {"message": body["message"], "id": body.get("client_prompt_id")})

        async def deliver():
            for phase, text in (("commentary", PROGRESS), ("final", FINAL)):
                await asyncio.sleep(0.3)
                runtime._append_transcript("agent_message_chunk", {"message_id": phase, "phase": phase, "text": text})
            runtime._append_transcript("turn_completed", {"stop_reason": "end_turn"})
            runtime._flush_transcript()

        task = asyncio.create_task(deliver())
        app.state.delivery = task
        return {"accepted": True, "started": True, "prompt_id": body.get("client_prompt_id"), "queue": []}

    @app.get("/api/agent/{path:path}")
    async def auxiliary(path: str):
        return {}

    app.mount("/static", StaticFiles(directory=root / "src/pa/server/static"))
    return app


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pa-live-fixture-") as directory:
        uvicorn.run(build_fixture(Path(directory)), host="127.0.0.1", port=18081)
