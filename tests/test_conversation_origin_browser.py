"""Real branch/copy/empty HTTP paths rendered by the full chat page.

Only the external provider is synthetic; native forks use the real admission RPC
adapter. The running user's conversations and service data are never involved.
"""
import asyncio
import copy
import json
import time
from datetime import UTC, datetime
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import httpx

from pa.acp.client import AgentConnection
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import AgentSession, TranscriptEvent
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.conversation_issues import _preferences
from tests.test_chat_threads_browser import add_threads_fixture
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app
from tests.test_conversation_threads import CAPS


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.asyncio
async def test_fork_copy_fresh_source_navigation_reload_and_narrow_layout(operator_server, tmp_path, monkeypatch):
    app, base = operator_server
    ctx = app.state.ctx
    agent = add_threads_fixture(app)
    # Render the full controls while real provider startup stays disabled.
    from pa.modules import ui_shell
    context = ui_shell._agent_context
    def fixture_context(request):
        return {**context(request), "agent_enabled": True}
    pages = ctx.require_service("pages")
    monkeypatch.setitem(pages._by_path, "/agent", replace(pages.get("agent"), context_builder=fixture_context))
    source = agent.get("thread-root").session
    source.external_session_id = "provider-source"
    source.model_id = "fixture-model"
    source.title = ("Release plan <source> " + "long title " * 12).rstrip()
    source.config_json = {"values": {"reasoning": "xhigh", "fast-mode": "on"},
        "execution_selection": {"decision_id": "fixture-selection", "resolved_inputs": {
            "reasoning": {"intent": "required", "value": "xhigh"},
            "options.fast-mode": {"intent": "required", "value": "on"}}}}
    ctx.store.save_session(source)
    root_runtime = agent.get(source.id)
    root_runtime.connection._init_response = CAPS
    question = asyncio.get_running_loop().create_future()
    permission = asyncio.get_running_loop().create_future()
    root_runtime._pending_elicitations["source-question"] = question
    root_runtime._elicitation_requests["source-question"] = {"request_id": "source-question", "message": "Source-only question"}
    root_runtime._pending_permissions["source-permission"] = permission
    root_runtime._permission_requests["source-permission"] = {"request_id": "source-permission", "tool_call": {"title": "Source-only permission"}, "options": []}
    calls = []
    forks = AsyncMock(side_effect=lambda **kw: SimpleNamespace(session_id="fork-" + str(len(calls))))

    async def create(**kwargs):
        calls.append(kwargs)
        workflow = kwargs.get("initiating_workflow") or {}
        configuration = kwargs.get("initial_configuration")
        if workflow.get("strategy"):
            assert not set(kwargs) & {"cwd", "card_id", "dispatch_id", "context_source_session_id", "execution_binding"}
            assert configuration.model_id == source.model_id
            assert configuration.mode_id == source.mode_id
            assert kwargs["execution_preferences"] == _preferences(source)
        else:
            assert not workflow.get("source_session_id")
        sid = kwargs.get("session_id") or str(uuid4())
        session = AgentSession(id=sid, agent_name="codex", purpose="chat", status="active",
            principal_id=kwargs["principal_id"], origin_instance_id=ctx.settings.instance_id,
            model_id=source.model_id, mode_id=source.mode_id, label=kwargs.get("label"),
            cwd=str(tmp_path / "own-workspaces" / sid), initiating_workflow=workflow,
            config_json={"values": {"reasoning": "xhigh", "fast-mode": "on"},
                "configuration": {"state": "ready", "requested": configuration.as_dict() if configuration else {}}})
        if workflow.get("strategy"):
            session.config_json["execution_selection"] = copy.deepcopy(source.config_json["execution_selection"])
        ctx.store.save_session(session)
        runtime = AgentSessionRuntime(agent, session)
        connection = AgentConnection(ctx.settings, ctx.store)
        connection.session = session
        connection._init_response = CAPS
        connection._conn = SimpleNamespace(fork_session=forks)
        runtime.connection = connection
        if workflow.get("strategy") == "native":
            await connection._fork_conversation(session, {"cwd": session.cwd, "mcp_servers": []})
        agent._runtimes[session.id] = runtime
        return runtime
    agent.create_session = create
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="origin-test")
    scope = BrowserScope("user:test", "origin-test", "origin-test")
    try:
        await manager.attach(scope, url=base + "/agent?session=thread-root", width=1280, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            async with httpx.AsyncClient(timeout=4) as client:
                current = await page.evaluate("document.querySelector('[data-agent-chat]')._acw.sessionId")
                response = await client.get(base + "/api/agent/sessions/" + current)
                snap = response.json()
            diagnostics = await page.evaluate("""(() => {
              const w = document.querySelector('[data-agent-chat]')?._acw;
              if (!w) return 'Widget unavailable';
              const snap = __SNAP__;
              try {w.applySnapshot(snap); return {session: w.sessionId, origin: snap.session?.conversation_origin};}
              catch(e) {return {error: e.stack};}
            })()""".replace("__SNAP__", json.dumps(snap)))
            (tmp_path / "failure.png").write_bytes(await manager.screenshot(scope))
            pytest.fail(expression + "\n" + str(diagnostics) + "\n" + str(await page.evaluate("document.body.innerText")))

        def selected(sid):
            return "document.querySelector('[data-agent-chat]')?._acw?.lastSnapshot?.session.id === " + json.dumps(sid)

        async def open_branch_panel():
            tab_id = await page.evaluate("document.querySelector('[data-conversation-threads]').closest('[role=tabpanel]').getAttribute('aria-labelledby')")
            await manager.click(scope, selector="#" + tab_id)

        async def select_thread(sid):
            selector = '[data-thread-list] a[data-thread-id="' + sid + '"]'
            await wait_for("!!document.querySelector(" + json.dumps(selector) + ")")
            # Activate the rendered link directly so the refreshing horizontal
            # list cannot move a different item under CDP's pointer coordinates.
            await page.evaluate("document.querySelector(" + json.dumps(selector) + ").click()")
            await wait_for(selected(sid))

        await wait_for(selected(source.id))
        await manager.type_text(scope, selector="[data-acw-input]", text="Source draft stays here")
        await open_branch_panel()
        await wait_for("!document.querySelector('[data-thread-native]').disabled")
        await manager.click(scope, selector="[data-thread-native]")
        await wait_for("!!document.querySelector('[data-thread-status] a')")
        native_id = calls[-1]["session_id"]
        await manager.click(scope, selector="[data-thread-status] a")
        await wait_for(selected(native_id))
        await wait_for("!!document.querySelector('[data-origin-source] a')")
        assert await page.evaluate("""(() => {
          const link = document.querySelector('[data-origin-source] a'); link.focus();
          const widget = document.querySelector('[data-agent-chat]')._acw;
          widget.applySnapshot(widget.lastSnapshot);
          return document.activeElement === link && document.querySelector('[data-origin-source] a') === link;
        })()""")
        assert forks.await_count == 1
        assert forks.await_args.kwargs["session_id"] == "provider-source"
        assert forks.await_args.kwargs["cwd"] == agent.get(native_id).session.cwd
        assert "Branched from" in await page.evaluate("document.querySelector('[data-origin-source]').innerText")
        assert "Latest provider history was inherited at fork time" in await page.evaluate("document.querySelector('[data-origin-explanation]').innerText")
        assert await page.evaluate("document.querySelector('[data-origin-boundary]').innerText") == "New messages in this branch"
        assert await page.evaluate("document.querySelector('[data-acw-title]').innerText") == "Branch of " + source.title
        assert "History only for thread-root" not in await page.evaluate("document.querySelector('[data-acw-messages]').innerText")
        assert not await page.evaluate("document.querySelector('[data-agent-chat]')._acw.lastSnapshot.pending_elicitations.length")
        assert not await page.evaluate("document.querySelector('[data-agent-chat]')._acw.lastSnapshot.pending_permissions.length")
        await wait_for("document.querySelector('[data-session-id=" + json.dumps(native_id) + "] [data-session-origin]')?.textContent.includes('Branch')")
        assert not await page.evaluate("!!document.querySelector('[data-origin-source] source')")
        await manager.type_text(scope, selector="[data-acw-input]", text="Branch draft stays here")
        # A new branch message is durable in this session, never appended to its source.
        ctx.store.append_transcript_events([TranscriptEvent(session_id=native_id, seq=1,
            event_type="user_message", payload={"message": "New message only in branch"})])
        await manager.open(scope, url=base + "/agent?session=" + native_id)
        await wait_for(selected(native_id))
        await wait_for("document.querySelector('[data-acw-messages]').innerText.includes('New message only in branch')")
        await wait_for("!!document.querySelector('[data-origin-source] a')")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Branch draft stays here"
        # Source navigation uses normal widget switching, preserving both drafts.
        await page.evaluate("window.oldSourceLink = document.querySelector('[data-origin-source] a')")
        await manager.click(scope, selector="[data-origin-source] a")
        await wait_for(selected(source.id))
        await page.evaluate("window.oldSourceLink.click()")
        assert await page.evaluate(selected(source.id))
        assert await page.evaluate("document.querySelector('[data-conversation-origin]').hidden")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Source draft stays here"
        assert "New message only in branch" not in await page.evaluate("document.querySelector('[data-acw-messages]').innerText")
        await open_branch_panel()
        await wait_for("!document.querySelector('[data-thread-copy]').disabled")
        await manager.click(scope, selector="[data-thread-copy]")
        await wait_for("!!document.querySelector('[data-thread-status] a')")
        copy_id = calls[-1]["session_id"]
        await manager.click(scope, selector="[data-thread-status] a")
        await wait_for(selected(copy_id))
        await wait_for("document.querySelector('[data-origin-source]').textContent.includes('Context copied from')")
        assert "no hidden provider history" in await page.evaluate("document.querySelector('[data-origin-explanation]').innerText")
        assert "History only for thread-root" in calls[-1]["initiating_workflow"]["saved_excerpt"]
        assert forks.await_count == 1
        assert not ctx.store.list_transcript_events_before(copy_id, limit=100)
        # Empty new-thread path shares navigation only; no inherited history.
        await wait_for("!document.querySelector('[data-thread-empty]').disabled")
        await manager.click(scope, selector="[data-thread-empty]")
        await wait_for("document.querySelector('[data-conversation-origin]').dataset.strategy === 'empty'")
        empty_id = calls[-1]["session_id"]
        assert await page.evaluate("document.querySelector('[data-origin-source]').innerText") == "Fresh chat"
        assert "saved_excerpt" not in calls[-1]["initiating_workflow"]
        assert forks.await_count == 1
        # Archive keeps a usable source link; missing source suppresses the link.
        source.archived_at = datetime.now(UTC)
        ctx.store.save_session(source)
        await select_thread(native_id)
        await wait_for("document.querySelector('[data-origin-source]')?.textContent.includes('Archived source')")
        await manager.resize(scope, width=390, height=844)
        await manager.click(scope, selector="[data-agent-sidebar-toggle]")
        await wait_for("!!document.querySelector('[data-origin-source] a')")
        assert await page.evaluate("document.querySelector('[data-conversation-origin]').scrollWidth <= document.querySelector('[data-conversation-origin]').clientWidth")
        assert await page.evaluate("document.querySelector('[data-origin-source] a').getBoundingClientRect().right <= innerWidth")
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        assert await page.evaluate("document.querySelector('[data-origin-boundary]').getBoundingClientRect().bottom < innerHeight")
        assert await page.evaluate("document.querySelector('[data-acw-input]').getBoundingClientRect().bottom <= innerHeight")
        (tmp_path / "fork-origin-narrow.png").write_bytes(await manager.screenshot(scope))
        await manager.resize(scope, width=1280, height=1000)
        await manager.click(scope, selector="[data-agent-sidebar-toggle]")
        (tmp_path / "fork-origin-desktop.png").write_bytes(await manager.screenshot(scope))
        await manager.click(scope, selector="[data-origin-source] a")
        await wait_for(selected(source.id))
        assert "History only for thread-root" in await page.evaluate("document.querySelector('[data-acw-messages]').innerText")
        target = agent.get(native_id).session
        target.initiating_workflow["source_session_id"] = "unavailable-source"
        ctx.store.save_session(target)
        await select_thread(native_id)
        await wait_for("document.querySelector('[data-origin-source]')?.textContent.includes('Source unavailable')")
        assert not await page.evaluate("!!document.querySelector('[data-origin-source] a')")
        target.config_json["branch_admission"]["state"] = "outcome_unknown"
        ctx.store.save_session(target)
        await select_thread(empty_id)
        await select_thread(native_id)
        await wait_for("document.querySelector('[data-origin-explanation]')?.textContent.includes('not yet confirmed')")
        assert agent.get(source.id).prompting
        assert [q.message for q in agent.get(source.id)._queue] == ["Queued thread-root"]
        assert source.external_session_id == "provider-source"
        assert not question.done() and not permission.done()
        assert len({agent.get(sid).session.cwd for sid in [native_id, copy_id, empty_id]}) == 3
        # Ordinary New chat is a separate root, with no branch source at all.
        await manager.click(scope, selector='.agent-session-header-actions a[href="/agent?new=1"]')
        await wait_for("document.querySelector('[data-agent-chat]')?._acw?.lastSnapshot?.session.id && !document.querySelector('[data-agent-chat]')._acw.lastSnapshot.session.initiating_workflow.source_session_id")
        assert await page.evaluate("document.querySelector('[data-conversation-origin]').hidden")
        assert forks.await_count == 1
    finally:
        for runtime in agent._runtimes.values():
            runtime.connection = None
            runtime._in_flight = None
            runtime._queue = []
            runtime._pending_permissions.clear()
            runtime._pending_elicitations.clear()
        agent._runtimes.clear()
        await manager.close()
        await browser.close()
