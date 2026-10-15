"""Real shared-shell navigation and human acceptance using throwaway data only."""
from __future__ import annotations

import asyncio
import json
import time
from urllib.parse import parse_qs, urlsplit

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_local_operator_acceptance import operator_app, make_card
from tests.test_card_status_browser import operator_server


@pytest.fixture
def navigation_card(operator_server):
    # Seed outside the test's asyncio loop; the threaded server owns listeners.
    app, _ = operator_server
    card = make_card(app)
    user = app.state.ctx.require_service("users").create_user(
        "reviewer", "test-browser-password", display_name="Test Reviewer")
    return card, user


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_discover_sign_in_return_accept_reopen_and_logout(navigation_card, operator_server, tmp_path):
    app, base = operator_server
    card, user = navigation_card
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="auth-nav-test")
    scope = BrowserScope("user:test", "auth-nav-browser", "auth-nav-test")
    try:
        await manager.attach(scope, url=base + "/", width=390, height=844)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition did not become true: " + expression)

        async def layout(selector):
            return await page.evaluate('''(() => {
              const e=document.querySelector(''' + json.dumps(selector) + ''');
              const r=e.getBoundingClientRect();
              const top=document.elementFromPoint(r.x+r.width/2,r.y+r.height/2);
              const actions=document.querySelector('.chrome-actions').getBoundingClientRect();
              const nav=Array.from(document.querySelectorAll('.top-nav,.responsive-nav'))
                .map(e=>e.getBoundingClientRect()).filter(r=>r.width>0)[0];
              return {visible:r.width>0 && r.height>=44 && r.left>=0 && r.right<=innerWidth,
                clickable:(e===top || e.contains(top)) && nav.right<=actions.left,
                overflow:document.documentElement.scrollWidth>innerWidth,
                rect:{x:r.x,y:r.y,width:r.width,height:r.height}};
            })()''')

        await wait_for("!!document.querySelector('.chrome-actions [data-sign-in]') && !!window.htmx")
        for theme in ("light", "dark"):
            await page.evaluate('''new Promise(resolve => {
              const link=document.querySelector('#pa-theme-variant');
              link.onload=resolve;
              document.documentElement.dataset.appearance=''' + json.dumps(theme) + ''';
              link.href='/static/themes/pa/''' + theme + '''.css';
            })''')
            for width in (320, 390, 720, 820, 1180, 1280, 1440, 1760, 1920):
                await manager.resize(scope, width=width, height=900)
                measured = await layout(".chrome-actions [data-sign-in]")
                assert measured["visible"] and measured["clickable"] and not measured["overflow"], (theme, width, measured)

        await manager.resize(scope, width=390, height=844)
        # Discover login on Home without knowing /login. Invalid credentials keep
        # the local return destination and cannot produce a receipt.
        await manager.click(scope, selector=".chrome-actions [data-sign-in]")
        await wait_for("location.pathname === '/login'")
        await manager.type_text(scope, selector='input[name="username"]', text="reviewer")
        await manager.type_text(scope, selector='input[name="password"]', text="incorrect")
        await manager.click(scope, selector='button[type="submit"]')
        await wait_for("document.querySelector('[role=alert]')?.textContent.includes('Invalid credentials')")
        assert await page.evaluate("document.querySelector('[name=next]').value") == "/"
        assert app.state.ctx.store.get_card(card.id).completion_evidence == card.completion_evidence

        # The shared shell survives HTMX navigation; its link must capture Settings.
        await page.navigate_and_wait(base + "/")
        await manager.click(scope, selector="[data-responsive-nav] summary")
        await manager.click(scope, selector='.responsive-nav-menu a[href="/settings"]')
        await wait_for("location.pathname === '/settings'")
        await manager.click(scope, selector=".chrome-actions [data-sign-in]")
        await wait_for("location.pathname === '/login'")
        assert await page.evaluate("document.querySelector('[name=next]').value") == "/settings"
        # The required Home/card journey uses the existing card deep link.
        await page.navigate_and_wait(base + f"/?card={card.id}")
        await wait_for("!!document.querySelector('.card-detail-actions [data-sign-in]')")
        await manager.click(scope, selector=".card-detail-actions [data-sign-in]")
        await wait_for("location.pathname === '/login'")
        destination = await page.evaluate("document.querySelector('[name=next]').value")
        assert destination.startswith("/?") and f"card={card.id}" in destination and "realm=default" in destination
        await manager.type_text(scope, selector='input[name="username"]', text="reviewer")
        await manager.type_text(scope, selector='input[name="password"]', text="test-browser-password")
        await manager.click(scope, selector='button[type="submit"]')
        await wait_for("location.pathname === '/' && !!document.querySelector('[data-card-status-form]')")
        returned = await page.evaluate("location.pathname+location.search+location.hash")
        assert urlsplit(returned).path == urlsplit(destination).path
        assert parse_qs(urlsplit(returned).query) == parse_qs(urlsplit(destination).query)
        assert not await page.evaluate("!!document.querySelector('.card-detail-actions [data-sign-in]')")
        # Login alone did not accept the card. The actual status control does.
        assert app.state.ctx.store.get_card(card.id).completion_evidence == card.completion_evidence
        await page.evaluate('''(() => {const s=document.querySelector('[data-card-status-form] select');
          s.value='done';s.dispatchEvent(new Event('change',{bubbles:true}));})()''')
        await wait_for("document.querySelector('[data-card-detail]')?.dataset.cardLane === 'done'")
        stored = app.state.ctx.store.get_card(card.id)
        assert stored.completion_evidence[:-1] == card.completion_evidence
        receipt = stored.completion_evidence[-1]
        assert (receipt.actor, receipt.actor_kind, receipt.outcome) == (f"user:{user.id}", "human", "human_override")
        assert receipt.recorded_at and receipt.idempotency_key
        assert stored.completion_requirement == card.completion_requirement
        await page.navigate_and_wait(base + destination)
        await wait_for("document.querySelector('[data-card-status-form] select')?.value === 'done'")
        await manager.click(scope, selector="[data-card-dialog-close]")
        await wait_for("!document.querySelector('#card-detail-dialog').open")
        for width in (320, 390, 820, 1440):
            await manager.resize(scope, width=width, height=900)
            measured = await layout("[data-account-menu] summary")
            assert measured["visible"] and measured["clickable"] and not measured["overflow"], (width, measured)
        await manager.resize(scope, width=390, height=844)
        await manager.click(scope, selector="[data-account-menu] summary")
        assert await page.evaluate("document.querySelector('.account-panel').innerText.includes('Test Reviewer')")
        assert (await layout("[data-sign-out] button"))["clickable"]
        await manager.click(scope, selector="[data-sign-out] button")
        await wait_for("!!document.querySelector('.chrome-actions [data-sign-in]') && !document.querySelector('[data-sign-out]')")
        assert await page.evaluate("location.pathname") == "/"
        await asyncio.to_thread(app.state.ctx.store.rebuild_from_log, "default")
        assert app.state.ctx.store.get_card(card.id) == stored
    finally:
        await manager.close()
        await browser.close()
