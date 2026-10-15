"""Rendered queue ordering, keyboard actions, mobile containment and safe text."""
import asyncio
import time
import os
from pathlib import Path

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.agent_queue_fixture import build_queue_fixture
from tests.test_card_status_browser import operator_server


@pytest.fixture
def operator_app(tmp_path):
    return build_queue_fixture(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason='managed Chromium is not installed')
@pytest.mark.asyncio
async def test_queue_order_states_keyboard_and_mobile(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / 'browser')
    manager = BrowserSessionManager(browser, instance_id='queue-test')
    scope = BrowserScope('user:test', 'queue-test', 'queue-test')
    try:
        await manager.attach(scope, url=base, width=1280, height=900)
        page = manager.resolve(scope).page
        async def wait(expression):
            deadline = time.monotonic()+12
            while time.monotonic()<deadline:
                if await page.evaluate(expression): return
                await asyncio.sleep(.05)
            pytest.fail(expression + ' ' + str(await page.evaluate("({focus:document.activeElement?.outerHTML, errors:window.queueErrors, queue:document.querySelector('[data-agent-chat]')._acw.lastSnapshot?.queue})")))
        await page.evaluate("window.queueErrors=[];window.addEventListener('unhandledrejection',e=>queueErrors.push(String(e.reason)))")
        rows = "[...document.querySelectorAll('[data-acw-queue-list] li')]"
        await wait(f"{rows}.length===4 && !document.querySelector('[data-agent-chat]')._acw.recentBootstrap")
        assert await page.evaluate(f'{rows}.map(r=>r.dataset.id)') == ['human-1','human-2','response','auto']
        assert await page.evaluate(f'{rows}.map(r=>r.querySelector(".acw-queue-order").textContent)') == ['Next · 1','2','3','4']
        assert await page.evaluate(f'{rows}.map(r=>r.querySelector(".acw-queue-source").textContent)') == ['Human','Human','Response','Automated']
        assert await page.evaluate("document.querySelector('[data-acw-queue-status]').textContent.includes('Resume')")
        assert await page.evaluate("document.querySelector('[data-acw-queue-status]').getAttribute('role')") == 'status'
        assert not await page.evaluate("!!document.querySelector('[data-acw-queue-list] img')")
        # Activate the actual keyboard button; the endpoint preserves explicit order.
        await page.evaluate("document.querySelector('li[data-id=human-2] [data-queue-action=up]').focus()")
        await manager.press(scope, key='Space')
        await wait(f'{rows}[0].dataset.id==="human-2"')
        assert [q.id for q in app.state.runtime._queue][:2] == ['human-2','human-1']
        assert await page.evaluate("document.activeElement.closest('li').dataset.id") == 'human-2'
        assert await page.evaluate("document.activeElement.getAttribute('aria-label')") == 'Remove Human message at queue position 1'
        async with httpx.AsyncClient(base_url=base) as client:
            response = await client.post('/fixture/running')
            assert response.status_code == 200, response.text
            await page.evaluate("document.querySelector('[data-agent-chat]')._acw.refreshQueue()")
            await wait("document.querySelector('[data-acw-queue-status]').textContent.includes('current reply')")
            response = await client.post('/fixture/held')
            assert response.status_code == 200, response.text
            await page.evaluate("document.querySelector('[data-agent-chat]')._acw.refreshQueue()")
        await wait(f'{rows}[0].textContent.includes("Held during human control")')
        assert await page.evaluate(f'{rows}[0].dataset.next') == 'false'
        assert await page.evaluate(f'{rows}[1].querySelector(".acw-queue-order").textContent') == 'Next · 2'
        await manager.resize(scope, width=360, height=740)
        await wait('window.innerWidth===360')
        assert await page.evaluate('document.documentElement.scrollWidth <= window.innerWidth')
        assert await page.evaluate(f'{rows}.every(r=>r.scrollWidth<=r.clientWidth)')
        if os.environ.get('PA_TEST_EVIDENCE_DIR'):
            evidence_dir=Path(os.environ['PA_TEST_EVIDENCE_DIR']); evidence_dir.mkdir(parents=True,exist_ok=True)
            (evidence_dir/'queue-mobile.png').write_bytes(await manager.screenshot(scope))
        assert await page.evaluate(f'{rows}.every(r=>[...r.querySelectorAll("button")].every(b=>!!b.getAttribute("aria-label")))')
        # Restoration fetches the same authoritative eligibility and ordering.
        await page.navigate_and_wait(base)
        await wait(f'{rows}.length===4 && {rows}[1].dataset.next==="true"')
        assert await page.evaluate(f'{rows}[0].dataset.id') == 'auto'
    finally:
        await manager.detach(scope)
        await browser.close()
