"""Restored conversation receipts open the real Work card dialog."""
import asyncio
import time
from pathlib import Path

import pytest
from fastapi.responses import HTMLResponse
from jinja2 import Environment, FileSystemLoader

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.conversation_issues import ConversationIssues, Issue, IssueEdit, StartIssues
from pa.domain.models import AgentSession, CardCreate
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


def seed_receipt(app, realm):
    store = app.state.ctx.store
    card = store.create_card(CardCreate(title="Retained conversation card", realm_id=realm, lane="done"))
    store.save_session(AgentSession(id="retained-chat", agent_name="codex", purpose="chat", realm_id=realm))
    notes = ConversationIssues(store)
    notes.edit("retained-chat", IssueEdit(revision=0, issues=[Issue(id="one", title=card.title)]))
    notes.admit("retained-chat", StartIssues(request_id="saved-start", revision=1, issue_ids=["one"]),
                {"realm_id": realm, "instance_id": "original-owner"})
    saved = notes.read("retained-chat")
    groups = saved["requests"][0]["groups"]
    groups[0].update(card_id=card.id, state="admitted")
    notes.update_request("retained-chat", "saved-start", state="admitted", groups=groups)
    return card


def add_receipt_fixture(app, realm):
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / "src/pa/server/templates"))
    widget = env.get_template("partials/agent/chat-widget.html").render(session_id="retained-chat")

    async def conversation():
        if not hasattr(app.state, "navigation_card"):
            app.state.navigation_card = seed_receipt(app, realm)
        return HTMLResponse('''<!doctype html><title>Retained conversation</title>
<script src="/static/js/conversation-issues.js" defer></script>''' + widget + '''
<script>addEventListener('DOMContentLoaded', () => {
  const widget = {root: document.querySelector('[data-agent-chat]'), sessionId: 'retained-chat',
    api: path => fetch('/api/agent' + path).then(r => {if (!r.ok) throw Error(r.status); return r.json();})};
  const controller = new PAConversationIssues(widget);
  controller.panel.open = true;
});</script>''')

    app.add_api_route("/fixture/issues", conversation, response_class=HTMLResponse)
    # The real UI shell owns a final catch-all page route.
    app.router.routes.insert(0, app.router.routes.pop())


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.parametrize("realm", ["default", "research"])
@pytest.mark.asyncio
async def test_restored_open_card_link_reaches_work_dialog(operator_server, tmp_path, realm):
    app, base = operator_server
    add_receipt_fixture(app, realm)
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="card-navigation-test")
    scope = BrowserScope("user:test", "card-navigation-test", "card-navigation-test")
    try:
        await manager.attach(scope, url=base + "/fixture/issues")
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("!!document.querySelector('[data-issues-receipts] a')")
        card = app.state.navigation_card
        expected = f"/work?card={card.id}&realm={realm}"
        assert await page.evaluate("document.querySelector('[data-issues-receipts] a').href") == base + expected
        await manager.click(scope, selector="[data-issues-receipts] a")
        await wait_for("!!document.querySelector('[data-card-detail]')")
        assert await page.evaluate("location.pathname + location.search") == expected
        assert await page.evaluate("document.querySelector('[data-card-detail]').dataset.cardId") == card.id
        assert await page.evaluate("document.querySelector('[data-card-detail]').dataset.cardRealm") == realm
        assert "Retained conversation card" in await page.evaluate("document.querySelector('[data-card-detail]').textContent")
        # Reloading the original conversation renders the same saved receipt.
        await manager.open(scope, url=base + "/fixture/issues")
        await wait_for("!!document.querySelector('[data-issues-receipts] a')")
        assert await page.evaluate("document.querySelector('[data-issues-receipts] a').href") == base + expected
        assert len(app.state.ctx.store.list_cards(realm_id=realm)) == 1
        assert len(ConversationIssues(app.state.ctx.store).read("retained-chat")["requests"]) == 1
    finally:
        await manager.close()
        await browser.close()
