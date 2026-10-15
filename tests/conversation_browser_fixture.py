"""Isolated real chat templates/JS with deterministic synthetic ACP events.

No external provider or live PA data is used. Run with uv run python -m
 tests.conversation_browser_fixture, then open http://127.0.0.1:18082/.
"""
import asyncio
import tempfile
from pathlib import Path
from uuid import uuid4

import uvicorn
from fastapi import Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionRuntime
from tests.agent_chat_live_fixture import build_fixture


def build_conversation_fixture(directory):
    app = build_fixture(directory)
    app.router.routes = [r for r in app.router.routes if getattr(r, "path", "") not in ("/", "/api/agent/{path:path}")]
    manager = app.state.ctx.require_service("agent_sessions")
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / "src/pa/server/templates"))
    env.globals["resolve_instance_identity"] = lambda value: {"id": value, "name": "Fixture", "label": "Fixture", "known": True}
    scripts = ''.join(f'<script src="/static/js/{name}.js" defer></script>' for name in (
        "agent-chat-drafts", "agent-chat-draft-widget", "session-recovery", "agent-terminal", "agent-chat"))

    def html(content):
        return '<!doctype html><html data-pa-instance-id="live-fixture"><title>Conversation fixture</title><link rel="stylesheet" href="/static/style.css">' + scripts + '<body>' + content + '</body></html>'

    @app.get("/", response_class=HTMLResponse)
    async def home():
        actions = env.get_template("partials/chrome-actions.html").render(agent_startup={"complete": True}, appearance="light")
        return html('<h1>Isolated PA Home</h1>' + actions)

    @app.get("/agent", response_class=HTMLResponse)
    async def chat(request: Request):
        return html(env.get_template("pages/agent.html").render(
            agent_enabled=True, agent_startup={"complete": True}, sessions=manager.store.list_sessions(),
            session_details={}, session_view="chats", session_id=request.query_params.get("session", ""),
            new_chat=bool(request.query_params.get("new")), instance_id="live-fixture", live_session_ids=[],
            pr_watches_by_session={}, terminal_output_char_limit=1000,
        ))

    @app.post("/api/agent/sessions")
    async def create(request: Request):
        body = await request.json()
        existing = manager.store.get_session_by_label(body.get("label", "default"))
        if existing:
            return {"session": existing.model_dump(mode="json"), "connected": True}
        session = manager.store.save_session(AgentSession(
            id=str(uuid4()), agent_name="codex", external_session_id="fixture-native",
            title=None, label=body.get("label"), purpose="chat", origin_instance_id="live-fixture"))
        runtime = AgentSessionRuntime(manager, session)
        manager._runtimes[session.id] = runtime
        await asyncio.sleep(0.2)
        return {"session": session.model_dump(mode="json"), "connected": True}

    @app.put("/api/agent/sessions/{sid}/title")
    async def rename(sid: str, request: Request):
        body = await request.json()
        session = await manager.name_session(sid, title=body.get("title"), automatic=body.get("automatic", False))
        return {"session": session.model_dump(mode="json")}

    @app.post("/fixture/events")
    async def emit(request: Request):
        sid = (await request.json())["session_id"]
        runtime = manager.get(sid)
        await runtime._on_acp_update("fixture-native", {"sessionUpdate": "session_info_update", "title": "Provider conversation title"})
        for event in (
            {"sessionUpdate": "tool_call", "toolCallId": "call", "title": "fixture command", "status": "in_progress", "rawInput": {"command": "echo fixture"}},
            {"sessionUpdate": "tool_call_update", "toolCallId": "call", "_meta": {"terminal_output_delta": {"data": "<script>window.BAD=true</script>\n" + "x" * 1100, "terminal_id": "call"}}},
            {"sessionUpdate": "tool_call_update", "toolCallId": "call", "status": "completed", "rawOutput": {"exit_code": 0}},
        ):
            await runtime._on_acp_update("fixture-native", event)
        await runtime._drain_transcripts()
        return {"ok": True}

    @app.get("/api/agent/{path:path}")
    async def auxiliary(path: str):
        return {}

    @app.get("/fixture", response_class=HTMLResponse)
    async def controls():
        return html('<form action="/fixture/events" method="post">Synthetic events are sent by the test driver.</form>')

    return app


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pa-conversation-fixture-") as tmp:
        uvicorn.run(build_conversation_fixture(Path(tmp)), host="127.0.0.1", port=18082)
