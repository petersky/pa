"""User journeys over long synthetic history and PA's actual multiplex SSE."""
import asyncio
import json
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.agent_chat_reentry_fixture import build_reentry_fixture
from tests.test_card_status_browser import operator_server


@pytest.fixture
def operator_app(tmp_path):
    return build_reentry_fixture(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_recent_reentry_failed_history_live_preview_anchors_and_thread_isolation(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="reentry-test")
    scope = BrowserScope("user:test", "reentry-test", "reentry-test")
    client = httpx.AsyncClient(base_url=base)
    try:
        await manager.attach(scope, url=base, width=1280, height=1000)
        page = manager.resolve(scope).page
        w = "document.querySelector('[data-agent-chat]')._acw"
        async def wait_for(expression):
            deadline = time.monotonic()+12
            while time.monotonic()<deadline:
                if await page.evaluate(expression): return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)
        async def texts():
            return await page.evaluate("[...document.querySelectorAll('.acw-bubble-agent')].map(e=>e.dataset.markdown)")
        async def latest(number):
            await wait_for(f"[...document.querySelectorAll('.acw-bubble-agent')].some(e=>e.dataset.markdown==='Newest fixture message {number}') && !{w}.recentBootstrap")
            assert (await texts()).count(f"Newest fixture message {number}") == 1

        await wait_for(f"!!{w}.olderError && {w}.externalEventTransport")
        assert (await texts()) == []
        await client.post("/fixture/append")
        await wait_for("document.querySelector('[data-acw-live-preview]').textContent.includes('Newest fixture message 1')")
        assert "partial" in await page.evaluate("document.querySelector('[data-acw-history-notice-text]').textContent")
        assert "first prompt" not in await page.evaluate("document.querySelector('[data-acw-placeholder]').textContent")
        assert not any(r["after"] == 0 for r in app.state.history_requests)
        await manager.click(scope, selector="#recover")
        await latest(1)
        assert await page.evaluate(f"{w}.isNearBottom()")

        await manager.type_text(scope, selector="[data-acw-input]", text="Unsent source draft")
        await page.evaluate(f"{w}.drafts.flush({{force:true}})")
        await manager.click(scope, selector="#other")
        await wait_for(f"{w}.sessionId==='other-fixture' && !{w}.recentBootstrap")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == ""
        await client.post("/fixture/append")
        await manager.click(scope, selector="#source")
        await latest(2)
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Unsent source draft"
        assert await page.evaluate(f"{w}.isNearBottom()")

        await manager.click(scope, selector="[data-acw-load-older]")
        await wait_for(f"!{w}.loadingOlder && {w}.historyPreserveWindow")
        await page.evaluate(f"{w}.els.messages.scrollTop=45")
        anchor = await page.evaluate(f"{w}._captureHistoryAnchor()")
        await manager.click(scope, selector="#other")
        await wait_for(f"{w}.sessionId==='other-fixture' && !{w}.recentBootstrap")
        await client.post("/fixture/append")
        await manager.click(scope, selector="#source")
        await wait_for(f"!{w}.recentBootstrap && {w}.historyPreserveWindow && {w}.hasNewer")
        restored_anchor = await page.evaluate(f"{w}._captureHistoryAnchor()")
        assert restored_anchor["seq"] == anchor["seq"]
        assert abs(restored_anchor["offset"]-anchor["offset"]) < 3
        await manager.click(scope, selector="[data-acw-latest]")
        await latest(3)

        await manager.click(scope, selector="a[href='/away']")
        await client.post("/fixture/append")
        await manager.back(scope)
        await latest(4)
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Unsent source draft"
        await page.evaluate("history.forward();true")
        await wait_for("location.pathname==='/away'")
        await manager.back(scope)
        await latest(4)
        await page.command("Page.reload")
        await latest(4)
        assert await page.evaluate(f"{w}.isNearBottom()")

        # Existing visible text survives another failed tail; SSE remains visible
        # in the explicitly incomplete preview, and recovers into one ordered row.
        await client.post("/fixture/control", json={"fail":True})
        await page.evaluate(f"{w}._loadRecentHistory({w}.sessionId,{w}.subscriptionGeneration).catch(()=>null);'started'", await_promise=False)
        await wait_for(f"!!{w}.olderError")
        assert "Newest fixture message 4" in await texts()
        await client.post("/fixture/append")
        await wait_for("document.querySelector('[data-acw-live-preview]').textContent.includes('Newest fixture message 5')")
        await manager.click(scope, selector="#recover")
        await latest(5)

        # A late result cannot paint into the next conversation, even though the
        # server completed its real read before delaying the fixture response.
        await client.post("/fixture/control", json={"delay":True})
        await page.evaluate(f"{w}._loadRecentHistory({w}.sessionId,{w}.subscriptionGeneration).catch(()=>null);'started'", await_promise=False)
        await asyncio.sleep(.1)
        await manager.click(scope, selector="#other")
        await wait_for(f"{w}.sessionId==='other-fixture' && !{w}.recentBootstrap")
        await asyncio.sleep(.7)
        assert not any("Newest fixture" in text for text in await texts())
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == ""
        await manager.click(scope, selector="#source")
        await latest(5)
        seqs = await page.evaluate(f"{w}.transcriptEvents.map(e=>e.seq)")
        assert seqs == sorted(set(seqs))
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Unsent source draft"
    finally:
        await client.aclose()
        await manager.close()
        await browser.close()
