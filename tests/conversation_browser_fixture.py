"""Isolated real chat templates/JS with deterministic synthetic ACP events.

No external provider or live PA data is used. Run with uv run python -m
 tests.conversation_browser_fixture, then open http://127.0.0.1:18082/.
"""
import asyncio
import tempfile
import math
from pathlib import Path
from uuid import uuid4
from types import SimpleNamespace

import uvicorn
from fastapi import Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

from pa.domain.models import AgentSession, CardCreate
from pa.instance.agent_session import AgentSessionRuntime
from tests.agent_chat_live_fixture import build_fixture


def build_conversation_fixture(directory):
    app = build_fixture(directory)
    app.router.routes = [r for r in app.router.routes if getattr(r, "path", "") not in ("/", "/api/agent/{path:path}")]
    manager = app.state.ctx.require_service("agent_sessions")
    app.state.ctx.services["async_runtime"] = manager.async_runtime
    from pa.modules.conversation_issues import router as issues_router
    app.include_router(issues_router, prefix="/api")
    from pa.modules.conversation_threads import router as threads_router
    app.include_router(threads_router, prefix="/api")
    async def fixture_branch_create(**kwargs):
        await asyncio.sleep(0.2)
        session = manager.store.save_session(AgentSession(
            id=kwargs["session_id"], agent_name="codex", purpose="chat", mode_id="read-only",
            principal_id="user:local", origin_instance_id="live-fixture", label=kwargs["label"],
            initiating_workflow=kwargs["initiating_workflow"], status="idle"))
        runtime = AgentSessionRuntime(manager, session)
        manager._runtimes[session.id] = runtime
        return runtime
    manager.create_session = fixture_branch_create
    app.state.issue_dispatches = {}
    app.state.browser_timings = []
    async def fixture_dispatch(request, body):
        key = request.headers["idempotency-key"]
        app.state.issue_dispatches.setdefault(key, {"dispatch_id": str(uuid4())})
        await asyncio.sleep(0.4)
        return app.state.issue_dispatches[key]
    from pa.modules import fleet
    fleet.dispatch_fleet_work = fixture_dispatch

    @app.middleware("http")
    async def fixture_user(request, call_next):
        request.state.user = SimpleNamespace(role="user")
        request.state.principal_id = "user:local"
        return await call_next(request)

    for i in range(301):
        manager.store.create_card(CardCreate(title=f"History card {i}", summary="Representative retained summary. " * 6,
                                            body="Historical task detail. " * 100), via_log=False)
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / "src/pa/server/templates"))
    env.globals["resolve_instance_identity"] = lambda value: {"id": value, "name": "Fixture", "label": "Fixture", "known": True}
    scripts = ''.join(f'<script src="/static/js/{name}.js" defer></script>' for name in (
        "agent-chat-drafts", "agent-chat-draft-widget", "session-recovery", "agent-terminal", "conversation-issues", "conversation-threads", "agent-chat"))

    def html(content):
        version = (Path(__file__).parents[1] / "src/pa/server/static/style.css").stat().st_mtime_ns
        return f'<!doctype html><html data-pa-instance-id="live-fixture"><title>Conversation fixture</title><link rel="stylesheet" href="/static/style.css?v={version}">' + scripts + '<body>' + content + '</body></html>'

    @app.get("/", response_class=HTMLResponse)
    async def home():
        actions = env.get_template("partials/chrome-actions.html").render(agent_startup={"complete": True}, appearance="light")
        return html('<h1>Isolated PA Home</h1>' + actions)

    @app.get("/agent", response_class=HTMLResponse)
    async def chat(request: Request):
        page = env.get_template("pages/agent.html").render(
            agent_enabled=True, agent_startup={"complete": True}, sessions=manager.store.list_sessions(),
            session_details={}, session_view="chats", session_id=request.query_params.get("session", ""),
            new_chat=bool(request.query_params.get("new")), instance_id="live-fixture", live_session_ids=[],
            pr_watches_by_session={}, terminal_output_char_limit=1000,
        )
        if request.query_params.get("measure"):
            # Test-only browser measurement. Use the real focused composer and
            # send button; transport/tool latency is not counted as app latency.
            page += '''<script>document.addEventListener("DOMContentLoaded", () => {
              let sent = false, focus = null;
              const started = performance.now();
              const timer = setInterval(() => {
                const widget = document.querySelector("[data-agent-chat]")?._acw;
                if (widget && document.activeElement === widget.els.input && focus === null) focus = widget.composerReadyMs;
                if (widget && widget.sessionId && !widget.ownerResolutionPending && !sent && widget.lastSnapshot) {
                  sent = true; widget.els.input.value = "Synthetic browser timing sample";
                  widget.els.input.dispatchEvent(new Event("input", {bubbles: true})); widget.els.send.click();
                }
                if (widget && Number.isFinite(widget.submitAckMs) || performance.now() - started > 10000) {
                  clearInterval(timer);
                  const result = {focus_ms: focus, ack_ms: widget?.submitAckMs ?? null, sample: new URL(location.href).searchParams.get("sample")};
                  fetch("/fixture/timings", {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(result)}).then(() => {
                    const receipt = document.createElement("p"); receipt.textContent = "Benchmark recorded: " + JSON.stringify(result); document.body.appendChild(receipt);
                  });
                }
              }, 10);
            });</script>'''
        return html(page)

    @app.post("/fixture/timings")
    async def add_timing(request: Request):
        app.state.browser_timings.append(await request.json())
        return {"ok": True}

    @app.get("/fixture/timings")
    async def timings():
        values = app.state.browser_timings
        def p95(key):
            numbers = sorted(v[key] for v in values if isinstance(v.get(key), (int, float)))
            return round(numbers[math.ceil(len(numbers) * .95) - 1], 2) if numbers else None
        return {"samples": len(values), "failed": sum(v.get("focus_ms") is None or v.get("ack_ms") is None for v in values),
                "history_cards": 301, "provider": "synthetic", "composer_focus_p95_ms": p95("focus_ms"),
                "warm_prompt_ack_p95_ms": p95("ack_ms"), "raw": values}

    @app.post("/api/agent/sessions")
    async def create(request: Request):
        body = await request.json()
        existing = manager.store.get_session_by_label(body.get("label", "default"))
        if existing:
            return {"session": existing.model_dump(mode="json"), "connected": True}
        session = manager.store.save_session(AgentSession(
            id=str(uuid4()), agent_name="codex", external_session_id="fixture-native",
            title=None, label=body.get("label"), purpose="chat", origin_instance_id="live-fixture", mode_id="read-only"))
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

    @app.get("/fixture/issue-evidence")
    async def issue_evidence():
        return {"history_cards": 301, "new_cards": len(manager.store.list_cards()) - 301,
                "dispatches": len(app.state.issue_dispatches)}

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
