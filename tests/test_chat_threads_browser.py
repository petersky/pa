"""Grouped navigation uses real widget routing with concurrent PA runtimes."""
import asyncio
import json
import time
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import Request
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

from pa.acp.client import AgentConnection
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


def add_threads_fixture(app):
    ctx = app.state.ctx
    manager = ctx.require_service("instance_agent")
    # operator_app disables ACP startup. Admit only these synthetic test
    # runtimes; no provider subprocess or production lifecycle is involved.
    manager._accepting = True
    manager._quiescing = False
    for sid, title, workflow in [("thread-root", "Plan the release", {}),
        ("thread-sibling", "Review the tests", {"kind": "conversation_thread", "strategy": "empty", "chat_group_id": "thread-root", "source_session_id": "thread-root"})]:
        session = AgentSession(id=sid, title=title, agent_name="codex", purpose="chat", status="active",
            principal_id="user:local", origin_instance_id=ctx.settings.instance_id, mode_id="read-only", initiating_workflow=workflow)
        ctx.store.save_session(session)
        ctx.store.append_transcript_events([TranscriptEvent(session_id=sid, seq=1, event_type="user_message", payload={"message": "History only for " + sid})])
        runtime = AgentSessionRuntime(manager, session)
        connection = AgentConnection(ctx.settings, ctx.store)
        connection.session = session
        connection._conn = SimpleNamespace()
        runtime.connection = connection
        runtime._in_flight = QueuedPrompt(message="Running " + sid, session_id=sid)
        runtime._queue = [QueuedPrompt(message="Queued " + sid, session_id=sid)]
        manager._runtimes[sid] = runtime

    async def create(**kwargs):
        assert not set(kwargs) & {"cwd", "card_id", "dispatch_id", "execution_binding", "context_source_session_id"}
        session = AgentSession(id=kwargs["session_id"], agent_name="codex", purpose="chat", principal_id=kwargs["principal_id"],
            origin_instance_id=ctx.settings.instance_id, mode_id=kwargs["initial_configuration"].mode_id,
            initiating_workflow=kwargs["initiating_workflow"])
        ctx.store.save_session(session)
        runtime = AgentSessionRuntime(manager, session)
        manager._runtimes[session.id] = runtime
        return runtime
    manager.create_session = create

    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / "src/pa/server/templates"), autoescape=True)
    async def fixture(request: Request):
        sid = request.query_params.get("session", "thread-root")
        widget = env.get_template("partials/agent/chat-widget.html").render(session_id=sid, current_instance_id=ctx.settings.instance_id, auto_start=False)
        scripts = "".join(f'<script src="/static/js/{name}.js" defer></script>' for name in ["csrf", "agent-chat-drafts", "agent-chat-draft-widget", "conversation-threads", "agent-chat"])
        return HTMLResponse('<!doctype html><html lang="en" data-theme="pa" data-appearance="light" data-pa-instance-id="' + ctx.settings.instance_id + '" data-pa-principal-id="user:local"><meta name="viewport" content="width=device-width, initial-scale=1"><link rel="stylesheet" href="/static/style.css"><link rel="stylesheet" href="/static/themes/pa/light.css"><title>Parallel threads</title>' + scripts + '<body><main style="height:95vh">' + widget + '''</main><script>
addEventListener('DOMContentLoaded', () => {
  addEventListener('error', e => {window.fixtureError=e.message;});
  const widget = document.querySelector('[data-agent-chat]')._acw;
  const apply = widget.applySnapshot;
  widget.applySnapshot = function(snap) {try {return apply.call(this,snap);} catch(e) {window.fixtureError=e.stack;throw e;}};
  widget.openSession(widget.sessionId, widget.currentInstanceId);
});</script></body></html>''')
    app.add_api_route("/fixture/threads", fixture)
    app.router.routes.insert(0, app.router.routes.pop())
    return manager


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.asyncio
async def test_switch_and_reload_parallel_threads_keep_drafts_history_queues_and_permissions(operator_server, tmp_path):
    app, base = operator_server
    agent = add_threads_fixture(app)
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="thread-browser-test")
    scope = BrowserScope("user:test", "thread-browser-test", "thread-browser-test")
    try:
        await manager.attach(scope, url=base + "/fixture/threads", width=1280, height=900)
        page = manager.resolve(scope).page
        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression + "\n" + str(await page.evaluate("({error:window.fixtureError, body:document.body.innerText})")))
        def selected(sid):
            return "document.querySelector('[data-thread-list] [aria-current=page]')?.href.includes(" + json.dumps(sid) + ")"
        await wait_for(selected("thread-root"))
        assert (await page.evaluate("document.querySelector('[data-thread-list]').innerText")).count("Responding") == 2
        # A pending provider request stays with its owning thread through
        # selection/reload; navigating never answers it.
        root_runtime = agent.get("thread-root")
        root_runtime._pending_permissions["root-permission"] = object()
        root_runtime._permission_requests["root-permission"] = {"request_id": "root-permission", "tool_call": {"title": "Permission only for root"}, "options": []}
        await manager.type_text(scope, selector="[data-acw-input]", text="Draft for root")
        await manager.click(scope, selector='[data-thread-list] a[href*="thread-sibling"]')
        await wait_for(selected("thread-sibling"))
        await wait_for("document.querySelector('[data-acw-messages]').innerText.includes('History only for thread-sibling')")
        assert "History only for thread-root" not in await page.evaluate("document.querySelector('[data-acw-messages]').innerText")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == ""
        await manager.type_text(scope, selector="[data-acw-input]", text="Draft for sibling")
        await manager.click(scope, selector='[data-thread-list] a[href*="thread-root"]')
        await wait_for(selected("thread-root"))
        await wait_for("document.querySelector('[data-acw-permissions]').innerText.includes('Permission only for root')")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Draft for root"
        # Exercise the transition itself, before a target snapshot arrives.
        # Even a detached old button must never answer on the new thread.
        assert await page.evaluate('''(() => {
          const widget = document.querySelector('[data-agent-chat]')._acw;
          const permission = document.querySelector('[data-acw-permissions] button');
          const api = widget.api; window.permissionPosts = [];
          widget.api = function(path, options) {
            if (path.includes('/permissions/')) {window.permissionPosts.push(path);return Promise.resolve({});}
            return api.call(this,path,options);
          };
          widget.switchSession('thread-sibling', true, widget.currentInstanceId);
          permission.click();
          return document.querySelector('[data-acw-permissions]').childElementCount === 0 && window.permissionPosts.length === 0;
        })()''')
        await wait_for(selected("thread-sibling"))
        # Reload the fixture with the same selected durable session.
        await manager.open(scope, url=base + "/fixture/threads?session=thread-sibling")
        await wait_for(selected("thread-sibling"))
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Draft for sibling"
        assert "Permission only for root" not in await page.evaluate("document.querySelector('[data-acw-permissions]').innerText")
        assert "root-permission" in root_runtime._pending_permissions
        (tmp_path / "threads-desktop.png").write_bytes(await manager.screenshot(scope))
        await manager.resize(scope, width=390, height=844)
        assert await page.evaluate("document.querySelector('[data-thread-list]').scrollWidth > document.querySelector('[data-thread-list]').clientWidth")
        (tmp_path / "threads-narrow.png").write_bytes(await manager.screenshot(scope))
        for sid, runtime in agent._runtimes.items():
            assert runtime.prompting
            assert [q.message for q in runtime._queue] == ["Queued " + sid]
            assert runtime.session.mode_id == "read-only"
        await manager.click(scope, selector="[data-thread-empty]")
        await wait_for("document.querySelectorAll('[data-thread-list] a').length === 3")
        selected_id = await page.evaluate("document.querySelector('[data-agent-chat]')._acw.sessionId")
        assert selected_id not in {"thread-root", "thread-sibling"}
        assert agent.store.get_session(selected_id).initiating_workflow["chat_group_id"] == "thread-root"
        assert not agent.get(selected_id)._queue
        assert agent.get("thread-sibling").prompting
    finally:
        # Synthetic connections never launch or disconnect a provider process.
        for runtime in agent._runtimes.values():
            runtime.connection = None
            runtime._in_flight = None
            runtime._queue = []
            runtime._pending_permissions.clear()
        agent._runtimes.clear()
        await manager.close()
        await browser.close()
