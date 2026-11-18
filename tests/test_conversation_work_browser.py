"""Saved starts, readable results and native permission choices in PA's UI."""
import asyncio
import time

import pytest
from fastapi import HTTPException

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.conversation_issues import ConversationIssues, Issue, IssueEdit
from pa.domain.models import CardCreate, CardUpdate, ProjectCreate, TranscriptEvent
from tests.test_card_status_browser import operator_server
from tests.test_chat_threads_browser import add_threads_fixture
from tests.test_local_operator_acceptance import operator_app


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.asyncio
async def test_rich_specification_link_edit_conflict_and_retry(operator_server, tmp_path, monkeypatch):
    app, base = operator_server
    ctx = app.state.ctx
    agent = add_threads_fixture(app)
    session = agent.get("thread-root").session
    project = ctx.store.create_project(ProjectCreate(title="Rich specifications"))
    session.project_id = project.id
    ctx.store.save_session(session)
    specification = ctx.store.create_card(CardCreate(title="Full specification", body="S" * 6916, project_id=project.id))
    notes = ConversationIssues(ctx.store)
    notes.edit(session.id, IssueEdit(revision=0, issues=[Issue(id="spec", title="Concise discussion summary")]))
    calls = []
    async def dispatch(request, body):
        from pa.modules.fleet import _check_dispatch_preview
        _check_dispatch_preview(body, ctx.store.get_card(body.card_id))
        calls.append(body)
        return {"dispatch_id": "linked-specification"}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="rich-spec-browser")
    scope = BrowserScope("user:local", "rich-spec-browser", "rich-spec-browser")
    try:
        await manager.attach(scope, url=base + "/fixture/threads?session=thread-root", width=1280, height=1000)
        page = manager.resolve(scope).page
        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail(expression + "\n" + str(await page.evaluate("document.body.innerText")))
        await wait_for("!!document.querySelector('[data-issues-list] textarea')")
        await page.evaluate("document.querySelector('[data-conversation-issues]').open = true; document.querySelector('[data-issues-list] details').open = true")
        await page.evaluate("""(() => {
          const detail = document.querySelector('[data-issues-list] textarea');
          detail.value = 'x'.repeat(16001); detail.dispatchEvent(new Event('input'));
          document.querySelector('[data-issues-list] button').click();
        })()""")
        await wait_for("document.querySelector('[data-issues-status]').textContent.includes('text is unchanged')")
        assert await page.evaluate("document.querySelector('[data-issues-list] textarea').value.length") == 16001
        assert notes.read(session.id)["revision"] == 1
        await page.evaluate("""(() => {
          const detail = document.querySelector('[data-issues-list] textarea');
          detail.value = 'D'.repeat(6916); detail.dispatchEvent(new Event('input'));
          const card = document.querySelector('[aria-label="Specification card ID (optional)"]');
          card.value = """ + repr(specification.id) + """; card.dispatchEvent(new Event('input'));
          document.querySelector('[data-issues-list] button').click();
        })()""")
        await wait_for("document.querySelector('[data-issues-status]').textContent === 'Issue list saved.'")
        assert notes.read(session.id)["issues"][0]["card_id"] == specification.id
        assert len(ctx.store.list_cards()) == 1 and not calls
        await page.navigate_and_wait(base + "/fixture/threads?session=thread-root")
        await wait_for("document.querySelector('[data-issues-list] textarea')?.value.length === 6916")
        ctx.store.update_card(specification.id, CardUpdate(body="Changed specification"))
        await page.evaluate("document.querySelector('[data-conversation-issues]').open = true; document.querySelector('[data-issues-list] input[type=checkbox]').click(); document.querySelector('[data-issues-start]').click()")
        await wait_for("document.querySelector('[data-issues-status]').textContent.includes('Linked card material changed')")
        assert not calls and not notes.read(session.id)["requests"]
        await page.evaluate("document.querySelector('[data-issues-list] details').open = true; [...document.querySelectorAll('[data-issues-list] button')].find(b => b.textContent === 'Use current card version').click()")
        await wait_for("document.querySelector('[data-issues-status]').textContent === 'Issue list saved.'")
        await page.evaluate("document.querySelector('[data-issues-start]').click()")
        await wait_for("document.querySelector('[data-issues-receipts]').textContent.includes('Work admitted')")
        assert len(calls) == 1 and calls[0].card_id == specification.id
        assert len(ctx.store.list_cards()) == 1
        await page.navigate_and_wait(base + "/fixture/threads?session=thread-root")
        await wait_for("document.querySelector('[data-issues-receipts]').textContent.includes('Work admitted')")
        assert len(calls) == 1
        await page.evaluate("document.querySelector('[role=tablist] [role=tab] [data-issues-count]').closest('button').click(); document.querySelector('[data-issues-list] details').open = true")
        (tmp_path / "rich-specification-link.png").write_bytes(await manager.screenshot(scope))
    finally:
        for runtime in agent._runtimes.values():
            runtime.connection = None
            runtime._in_flight = None
            runtime._queue = []
        agent._runtimes.clear()
        await manager.close()
        await browser.close()


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.asyncio
async def test_one_click_retry_results_and_exact_native_permissions(operator_server, tmp_path, monkeypatch):
    app, base = operator_server
    ctx = app.state.ctx
    agent = add_threads_fixture(app)
    runtime = agent.get("thread-root")
    source = runtime.session
    project = ctx.store.create_project(ProjectCreate(title="Selected project"))
    source.project_id = project.id
    ctx.store.save_session(source)
    captured = ctx.store.create_card(CardCreate(title="Captured only", body="Creation only; do not implement yet."))
    notes = ConversationIssues(ctx.store)
    notes.edit(source.id, IssueEdit(revision=0, issues=[
        Issue(id="fix", title="Fix selected behavior", deferred=True),
        Issue(id="other", title="Unrelated idea", deferred=True),
    ]))
    calls, workers = [], {}
    async def dispatch(request, body):
        calls.append(body)
        if len(calls) == 1:
            raise HTTPException(409, {"code": "workspace_unavailable", "message": "The selected workspace_path is unavailable. Restore that workspace, then retry."})
        workers.setdefault(request.headers["idempotency-key"], "one-worker")
        return {"dispatch_id": workers[request.headers["idempotency-key"]]}
    monkeypatch.setattr("pa.modules.fleet.dispatch_fleet_work", dispatch)
    outcome = "Requirements captured in Inbox. Implementation remains unstarted; select work to begin."
    contract = {"contract": "pa.card-disposition/v1", "lane": "inbox", "outcome": outcome, "evidence": {"integration_required": False}}
    ctx.store.append_transcript_events([
        TranscriptEvent(session_id=source.id, seq=2, event_type="card_disposition", payload={
            "contract": contract, "authority_acknowledged": True, "status": "applied", "lane_after": "inbox",
            "reason": "Explicit pa.card-disposition/v1 disposition applied.",
        }),
    ])
    native = asyncio.get_running_loop().create_future()
    runtime._pending_permissions["native-boundary"] = native
    runtime._permission_requests["native-boundary"] = {
        "request_id": "native-boundary", "tool_call": {"title": "Write the selected file outside the workspace"},
        "options": [{"optionId": "provider-once", "name": "Permit once", "kind": "allow_once"},
                    {"optionId": "provider-deny", "name": "Deny write", "kind": "reject_once"}],
    }
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="work-browser")
    scope = BrowserScope("user:local", "work-browser", "work-browser")
    try:
        await manager.attach(scope, url=base + "/fixture/threads?session=thread-root", width=1280, height=1000)
        page = manager.resolve(scope).page
        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail(expression + "\n" + str(await page.evaluate("document.body.innerText")))
        await wait_for("!!document.querySelector('[data-issues-list] input')")
        assert not calls and ctx.store.get_card(captured.id).lane.value == "inbox"
        await wait_for("!!document.querySelector('[data-card-disposition-status]')")
        visible = await page.evaluate("document.querySelector('[data-card-disposition-status]').innerText")
        assert outcome in visible and "Card: Inbox" in visible
        assert "pa.card-disposition" not in visible and "authority acknowledged" not in visible
        assert not await page.evaluate("document.querySelector('[data-card-disposition-status] details').open")
        await wait_for("document.querySelectorAll('[data-req-id=\"native-boundary\"] button').length === 2")
        assert await page.evaluate("[...document.querySelectorAll('[data-req-id=\"native-boundary\"] button')].map(e=>e.textContent)") == ["Permit once", "Deny write"]
        assert not native.done(), "navigation cannot submit a permission default"
        await page.evaluate("""(() => {
          const widget = document.querySelector('[data-agent-chat]')._acw;
          const api = widget.api;
          widget.api = function(path, options) {
            const result = api.call(this, path, options);
            if (path.includes('/permissions/')) result.then(value => window.nativeReply = value, error => window.nativeError = error.message);
            return result;
          };
        })()""")
        # A stale native choice is a recoverable correlated error, not another
        # invented approval action or an answered permission.
        options = runtime._permission_requests["native-boundary"]["options"]
        runtime._permission_requests["native-boundary"]["options"] = options[:1]
        await page.evaluate("document.querySelector('[data-req-id=\"native-boundary\"] button:last-child').click()")
        await wait_for("!!window.nativeError")
        await wait_for("document.querySelector('[data-req-id=\"native-boundary\"] [role=alert]').textContent.includes('native-boundary')")
        assert not native.done()
        runtime._permission_requests["native-boundary"]["options"] = options
        await page.evaluate("window.nativeError = null")
        await page.evaluate("document.querySelector('[data-req-id=\"native-boundary\"] button:last-child').click()")
        await wait_for("!!window.nativeReply || !!window.nativeError")
        assert not await page.evaluate("window.nativeError || null")
        assert native.done() and native.result().outcome.option_id == "provider-deny"
        # The synthetic provider has no awaiting _on_permission finally block.
        runtime._pending_permissions.clear()
        runtime._permission_requests.clear()
        # The issues panel's ordinary UI submits the explicit selected start.
        await page.evaluate("document.querySelector('[data-conversation-issues]').open = true")
        await page.evaluate("document.querySelector('[data-issues-list] input[type=checkbox]').click(); document.querySelector('[data-issues-start]').click()")
        await wait_for("document.querySelector('[data-issues-receipts]').innerText.includes('workspace_path is unavailable')")
        assert len(calls) == 1 and not workers
        assert "Review start" not in await page.evaluate("document.querySelector('[data-issues-receipts]').innerText")
        saved = notes.read(source.id)["requests"][0]
        await page.navigate_and_wait(base + "/fixture/threads?session=thread-root")
        await wait_for("[...document.querySelectorAll('[data-issues-receipts] button')].some(e=>e.textContent==='Retry saved request')")
        await page.evaluate("[...document.querySelectorAll('[data-issues-receipts] button')].find(e=>e.textContent==='Retry saved request').click()")
        await wait_for("document.querySelector('[data-issues-receipts]').innerText.includes('Work admitted')")
        assert len(calls) == 2 and len(workers) == 1
        assert calls[0].message == calls[1].message and saved["request_id"] in calls[1].message
        await page.navigate_and_wait(base + "/fixture/threads?session=thread-root")
        await wait_for("document.querySelector('[data-issues-receipts]').innerText.includes('Work admitted')")
        assert len(calls) == 2 and len(notes.read(source.id)["requests"]) == 1
        assert not notes.read(source.id)["issues"][1].get("request_id")
        assert notes.read(source.id)["issues"][1]["deferred"]
        (tmp_path / "selected-work.png").write_bytes(await manager.screenshot(scope))
    finally:
        for owned in agent._runtimes.values():
            owned.connection = None
            owned._in_flight = None
            owned._queue = []
            owned._pending_permissions.clear()
            owned._permission_requests.clear()
        agent._runtimes.clear()
        await manager.close()
        await browser.close()
