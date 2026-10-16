"""State, result and action remain readable on real Home, Work and details."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.conversation_issues import ConversationIssues, IssueEdit, StartIssues
from pa.domain.models import AgentSession, CardCreate
from pa.execution.dispatch import DispatchRecord
from tests.test_card_status_browser import operator_server, operator_app  # noqa: F401


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_status_actions_desktop_narrow_and_zoom(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    ready = store.create_card(CardCreate(title="Unstarted preview", auto_enrich=False))
    blocked = store.create_card(CardCreate(title="Status and actions", auto_enrich=False))
    failed = store.create_card(CardCreate(title="Failed workspace", auto_enrich=False))
    store.create_card(CardCreate(title="Delivered result", lane="done", auto_enrich=False))
    store.save_session(AgentSession(id="source-chat", agent_name="codex", purpose="chat", status="prompting"))
    for card in (ready, blocked):
        store.link_session_card("source-chat", card.id, make_primary=False)
    notes = ConversationIssues(store)
    notes.edit("source-chat", IssueEdit(revision=0, issues=[{"id": "selected", "title": "Status and actions"}]))
    notes.admit("source-chat", StartIssues(request_id="original-start", revision=1, issue_ids=["selected"]), {"realm_id": "default"})
    notes.update_request("source-chat", "original-start", state="blocked", groups=[{"title": blocked.title, "card_id": blocked.id}],
        error={"code": "materialization_preflight_required", "message": "workspace_preflight_failed"})
    app.state.ctx.services["dispatch_store"].put(DispatchRecord(
        dispatch_id="failed-attempt", mutation_id="failed-attempt", card_id=failed.id,
        authority_instance_id=app.state.ctx.settings.instance_id, authority_url=base,
        target_instance_id=app.state.ctx.settings.instance_id, state="failed", last_error="missing_worktree_lease"))
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="status-test")
    scope = BrowserScope("user:local", "status-browser", "status-test")
    evidence = Path(".dev/status-actions")
    evidence.mkdir(parents=True, exist_ok=True)
    try:
        await manager.attach(scope, url=base, width=1440, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition did not become true: " + expression)

        async def assert_layout(selector):
            result = await page.evaluate(f'''(() => {{
              const nodes=[...document.querySelectorAll({json.dumps(selector)})].filter(e=>e.getClientRects().length);
              return {{count:nodes.length, overflow:nodes.filter(e=>e.scrollWidth>e.clientWidth+2).map(e=>e.className),
                clipped:nodes.filter(e=>e.scrollHeight>e.clientHeight+2 && getComputedStyle(e).overflowY==='hidden').map(e=>e.className)}};
            }})()''')
            assert result["count"] and not result["overflow"] and not result["clipped"], result

        for width, height in [(1440, 1000), (390, 844)]:
            await manager.resize(scope, width=width, height=height)
            await page.navigate_and_wait(base)
            await wait_for("!!document.querySelector('[data-attention-state=\"start_blocked\"]')")
            assert "Review start" in await page.evaluate("document.querySelector('[data-attention-state=\"start_blocked\"]').innerText")
            text = await page.evaluate("document.querySelector('[data-home-attention-queue]').innerText")
            assert "No action required" not in text and "missing_worktree_lease" not in text
            await assert_layout(".attention-queue-card, .attention-card-summary, .attention-card-actions")
            (evidence / f"home-{width}.png").write_bytes(await page.screenshot())

            await page.navigate_and_wait(base + "/work")
            await wait_for(f"!!document.querySelector('.compact-card[data-card-id=\"{ready.id}\"]')")
            await page.evaluate("document.querySelector('[data-board-lane=\"inbox\"]').click(); document.querySelector('[data-work-board]').scrollIntoView()")
            ready_text = await page.evaluate(f"document.querySelector('.compact-card[data-card-id=\"{ready.id}\"]').innerText")
            assert "not started" in ready_text.lower() and "Open agent" not in ready_text
            await assert_layout(".compact-card, .work-outcome, .compact-card h3")
            (evidence / f"work-{width}.png").write_bytes(await page.screenshot())

            await manager.click(scope, selector=f'.compact-card[data-card-id="{blocked.id}"] h3 a')
            await wait_for("!!document.querySelector('[data-card-live-progress]')")
            text = await page.evaluate("document.querySelector('[data-card-detail]').innerText")
            assert text.lower().count("start blocked") == 1 and text.count("Review start") == 1
            assert "workspace_preflight_failed" not in text
            await assert_layout(".card-detail-header, .card-live-progress, .work-outcome")
            (evidence / f"card-{width}.png").write_bytes(await page.screenshot())
            await page.evaluate("document.querySelector('.card-live-progress details').open=true")
            assert "original-start" in await page.evaluate("document.querySelector('.card-live-progress').innerText")
            await page.evaluate("document.querySelector('.card-live-progress details').open=false")
            await page.evaluate("document.querySelector('[data-card-tab=\"agent\"]').click()")
            await wait_for("!!document.querySelector('[data-card-tab-panel=\"agent\"] [data-card-tab-loaded]')")
            agent = await page.evaluate("document.querySelector('[data-card-tab-panel=\"agent\"]').innerText")
            assert "Source conversation" in agent and "No agent sessions yet" in agent
            await manager.click(scope, selector="[data-card-dialog-close]")
        await wait_for("!!document.querySelector('.compact-card')")
        await page.evaluate("document.querySelector('[data-board-lane=\"inbox\"]').click()")
        await page.evaluate("document.documentElement.style.zoom='2'")
        await assert_layout(".compact-card, .work-outcome, .compact-card h3")
    finally:
        await manager.close()
        await browser.close()
