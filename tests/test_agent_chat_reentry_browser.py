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
        app.state.history_start_delay = .9
        await manager.attach(scope, url="about:blank", width=1280, height=1000)
        page = manager.resolve(scope).page
        await page.command("Page.enable")
        await page.command("Page.addScriptToEvaluateOnNewDocument", {"source": """
          (()=>{
            window.historyPaint={falseEmpty:false,connectedWhilePending:false,firstUsefulMs:null};
            new MutationObserver(()=>{
              const root=document.querySelector('[data-agent-chat]'),w=root?._acw;if(!w)return;
              const p=window.historyPaint;
              p.falseEmpty ||= /first prompt/.test(root.querySelector('[data-acw-placeholder]')?.textContent||'');
              p.connectedWhilePending ||= !!w.recentBootstrap && !!w.lastSnapshot?.connected;
              if(p.firstUsefulMs===null && root.querySelector('.acw-bubble-agent')){
                requestAnimationFrame(()=>requestAnimationFrame(()=>{p.firstUsefulMs ??= performance.now();}));
              }
            }).observe(document,{childList:true,subtree:true,characterData:true,attributes:true});
          })();
        """})
        await page.navigate_and_wait(base)
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
        assert await page.evaluate("window.historyPaint.connectedWhilePending")
        assert not await page.evaluate("window.historyPaint.falseEmpty")
        assert (await texts()) == []
        await client.post("/fixture/append")
        await wait_for("document.querySelector('[data-acw-live-preview]').textContent.includes('Newest fixture message 1')")
        assert "partial" in await page.evaluate("document.querySelector('[data-acw-history-notice-text]').textContent")
        assert "first prompt" not in await page.evaluate("document.querySelector('[data-acw-placeholder]').textContent")
        assert not any(r["after"] == 0 for r in app.state.history_requests)
        app.state.history_start_delay = 0
        recovery_start = await page.evaluate("performance.now()")
        await manager.click(scope, selector="#recover")
        await latest(1)
        await wait_for("window.historyPaint.firstUsefulMs!==null")
        assert not await page.evaluate("window.historyPaint.falseEmpty")
        # Recent reads are bounded; a failed initial page never walks the archive.
        assert await page.evaluate("document.querySelector('[data-acw-load-older-label]').textContent") == "Load older messages"
        assert len(app.state.history_requests) <= 3
        assert all(r["before"] is None and r["after"] is None for r in app.state.history_requests)
        print(json.dumps({"history_recovery_paint_ms":await page.evaluate("window.historyPaint.firstUsefulMs")-recovery_start,
                          "initial_history_requests":len(app.state.history_requests),"retained_events":3442}))
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

        # Restoration/layout alone cannot fetch older pages. An upward wheel at
        # the top fetches one page and retains the first visible message anchor.
        assert not any(r["before"] is not None for r in app.state.history_requests)
        await page.evaluate(f"{w}.els.messages.scrollTop=0")
        await asyncio.sleep(.15)
        assert not any(r["before"] is not None for r in app.state.history_requests)
        before_prepend = await page.evaluate(f"{w}._captureHistoryAnchor()")
        await manager.scroll(scope, selector="[data-acw-messages]", delta_y=-80)
        await wait_for(f"!{w}.loadingOlder && {w}.historyPreserveWindow")
        assert len([r for r in app.state.history_requests if r["before"] is not None]) == 1
        # At the absolute top the older row may fill space previously occupied
        # by the paging button. The original message must retain its position.
        after_offset = await page.evaluate(f"""(()=>{{const c={w}.els.messages;
          const row=[...c.querySelectorAll('.acw-msg[data-source-seq]')].find(e=>e.dataset.sourceSeq==={json.dumps(before_prepend['seq'])});
          return row.getBoundingClientRect().top-c.getBoundingClientRect().top;}})()""")
        assert abs(after_offset-before_prepend["offset"]) < 3
        await asyncio.sleep(.2)
        assert len([r for r in app.state.history_requests if r["before"] is not None]) == 1
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
