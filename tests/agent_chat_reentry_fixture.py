"""Long history and real PA SSE, with faults confined to temporary fixture data."""
import asyncio
import tempfile
from pathlib import Path

import uvicorn
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader

from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.agent_chat import get_agent_session_history
from tests.agent_chat_live_fixture import build_fixture
from tests.test_agent_chat_history_reconstruction import fixture_events


def build_reentry_fixture(directory):
    app = build_fixture(directory)
    replaced = {"/", "/api/agent/history/{session_id}", "/api/agent/sessions/{session_id}", "/api/agent/{path:path}"}
    app.router.routes = [r for r in app.router.routes if getattr(r, "path", "") not in replaced]
    manager = app.state.ctx.require_service("agent_sessions")
    manager.store.append_transcript_events(fixture_events("live-fixture"))
    manager._runtimes["live-fixture"] = AgentSessionRuntime(manager, manager.store.get_session("live-fixture"))
    app.state.history_fail = True
    app.state.history_delay = False
    app.state.history_requests = []
    app.state.append_count = 0
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / "src/pa/server/templates"))
    widget = env.get_template("partials/agent/chat-widget.html").render(
        session_id="live-fixture", current_instance_id="live-fixture", telemetry_enabled=False)

    @app.get("/", response_class=HTMLResponse)
    async def page():
        return '''<!doctype html><html data-pa-instance-id="live-fixture" data-pa-principal-id="fixture-user">
        <title>Isolated recent history recovery</title><link rel="stylesheet" href="/static/style.css">
        <style>body{padding:1rem}.agent-chat-widget{height:75vh;min-height:500px}#fixture-controls{display:flex;gap:.5rem}</style>
        <script src="/static/js/agent-chat-drafts.js" defer></script>
        <script src="/static/js/agent-chat-draft-widget.js" defer></script>
        <script src="/static/js/session-recovery.js" defer></script>
        <script src="/static/js/agent-chat.js" defer></script>
        <main class="page-agent"><h1>Recent history recovery · 3,442 retained events</h1>
        <div id="fixture-controls"><button id="recover">Recover history</button><button id="append">Append live message</button>
        <button id="other">Other thread</button><button id="source">Source thread</button><a href="/away">Navigate away</a></div>
        <p id="fixture-state" role="status"></p><ul data-agent-session-list data-agent-enabled="true"></ul>''' + widget + '''</main>
        <script>window.addEventListener('DOMContentLoaded',()=>{
          const w=()=>document.querySelector('[data-agent-chat]')._acw;
          document.querySelector('#recover').onclick=async()=>{await fetch('/fixture/control',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({fail:false})});w().historyPreserveWindow=false;w()._loadRecentHistory(w().sessionId,w().subscriptionGeneration).catch(()=>{});};
          document.querySelector('#append').onclick=()=>fetch('/fixture/append',{method:'POST'});
          document.querySelector('#other').onclick=()=>w().switchSession('other-fixture',true,'live-fixture');
          document.querySelector('#source').onclick=()=>w().switchSession('live-fixture',true,'live-fixture');
          setInterval(()=>{const x=w();if(x)document.querySelector('#fixture-state').textContent=JSON.stringify({session:x.sessionId,cursor:x.lastSeq,recentPending:!!x.recentBootstrap,readingOlder:!!x.historyPreserveWindow,hasNewer:x.hasNewer,events:x.transcriptEvents.length});},100);
        });</script></html>'''

    @app.get("/away", response_class=HTMLResponse)
    async def away():
        return '<h1>Away from chat</h1><a href="/">Return to conversation</a>'

    @app.get("/api/agent/history/{session_id}")
    async def history(request: Request, session_id: str, after_seq: int | None = None,
                      before_seq: int | None = None, limit: int = 250, message_boundaries: bool = False):
        app.state.history_requests.append({"session":session_id,"after":after_seq,"before":before_seq})
        if session_id == "live-fixture" and app.state.history_fail:
            return JSONResponse({"detail":"Isolated fixture history unavailable"}, status_code=503)
        result = await get_agent_session_history(request, session_id, after_seq, before_seq, limit, message_boundaries)
        if session_id == "live-fixture" and app.state.history_delay:
            app.state.history_delay = False
            await asyncio.sleep(.6)
        return result

    @app.get("/api/agent/sessions/{session_id}")
    async def snapshot(session_id: str):
        runtime = manager.get(session_id)
        return {"session":runtime.session.model_dump(mode="json"), "last_event_seq":runtime._seq,
                "connected":True, "prompting":False, "queue":[]}

    @app.post("/fixture/control")
    async def control(request: Request):
        body = await request.json()
        if "fail" in body: app.state.history_fail = body["fail"]
        if "delay" in body: app.state.history_delay = body["delay"]
        return {"ok":True}

    @app.post("/fixture/append")
    async def append():
        app.state.append_count += 1
        runtime = manager.get("live-fixture")
        runtime._append_transcript("agent_message_chunk", {"message_id":f"new-{app.state.append_count}",
            "phase":"final", "content_mode":"delta", "text":f"Newest fixture message {app.state.append_count}"})
        runtime._append_transcript("turn_completed", {})
        runtime._flush_transcript()
        await runtime._drain_transcripts(timeout=3)
        return {"seq":runtime._seq,"number":app.state.append_count}

    @app.get("/fixture/history-evidence")
    async def evidence():
        return {"requests":app.state.history_requests,"appended":app.state.append_count}

    @app.get("/api/agent/{path:path}")
    async def auxiliary(path: str):
        return {}
    return app


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pa-reentry-fixture-") as directory:
        uvicorn.run(build_reentry_fixture(Path(directory)), host="127.0.0.1", port=18083)
