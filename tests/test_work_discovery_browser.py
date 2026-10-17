"""Work discovery through real HTMX navigation, keyboard and touch input."""
from __future__ import annotations

import asyncio
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import CardCreate
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


@pytest.fixture
def work_server(operator_server):
    app, base = operator_server
    store = app.state.ctx.store
    store.create_card(CardCreate(title="Find launch card", tags=["alpha", "ops & qa"], lane="active"))
    store.create_card(CardCreate(title="Other card", tags=["alpha"], lane="inbox"))
    return base


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_work_disclosure_chips_results_and_responsive_layout(work_server, tmp_path):
    base = work_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="work-test")
    scope = BrowserScope("user:test", "work-browser", "work-test")
    try:
        await manager.attach(scope, url=base + "/work", width=1440, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            state = await page.evaluate("({input:document.querySelector('[data-facet-input]')?.value, status:document.querySelector('[data-facet-status]')?.textContent, options:document.querySelector('[data-facet-options]')?.innerHTML, tags:[...document.querySelectorAll('input[name=tag]')].map(e=>e.value),active:document.activeElement?.outerHTML})")
            (tmp_path / "work-failure.png").write_bytes(await page.screenshot())
            pytest.fail("Browser condition did not become true: " + expression + "\n" + repr(state))

        await wait_for("document.querySelector('[data-work-results]')?.textContent === '2 cards'")
        assert not await page.evaluate("document.querySelector('details.work-filter-disclosure').open")
        (tmp_path / "work-desktop.png").write_bytes(await page.screenshot())
        assert await page.evaluate("document.querySelector('[data-work-board]').getBoundingClientRect().top < 450")
        # Native disclosure works from the keyboard; hidden controls stay out of Tab order.
        await page.evaluate("document.querySelector('.work-filter-disclosure summary').focus()")
        assert await page.evaluate("document.activeElement.tagName") == "SUMMARY"
        # Send the Return character as well as the key for native activation.
        await page.command("Input.dispatchKeyEvent", {"type": "keyDown", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13, "text": "\r"})
        await page.command("Input.dispatchKeyEvent", {"type": "keyUp", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13})
        await wait_for("document.querySelector('.work-filter-disclosure').open")
        await manager.press(scope, key="Tab")
        # macOS may skip select controls unless full keyboard access is enabled.
        await wait_for("document.querySelector('.work-filter-disclosure').contains(document.activeElement)")
        assert await page.evaluate("document.activeElement.name || document.activeElement.getAttribute('data-facet-input')") is not None
        for width in (320, 390, 720, 820, 1024, 1440):
            await manager.resize(scope, width=width, height=900)
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth && document.querySelector('.page-main').scrollWidth <= document.querySelector('.page-main').clientWidth"), width
            assert await page.evaluate("document.querySelector('.work-filter-disclosure summary').getBoundingClientRect().height >= 44")
        await manager.resize(scope, width=1440, height=900)
        await manager.type_text(scope, selector='[data-facet-input]', text="ops")
        await wait_for("document.querySelector('[data-facet-options] [role=option]')?.textContent.includes('ops & qa')")
        await manager.press(scope, key="ArrowDown")
        await manager.press(scope, key="Enter")
        await manager.click(scope, selector='.work-filter-primary button[type=submit]')
        await wait_for("!!document.querySelector('[data-applied-filter=tag]') && document.querySelector('[data-work-results]')?.textContent === '1 card'")
        assert not await page.evaluate("document.querySelector('.work-filter-disclosure').open")
        assert await page.evaluate("new URL(location.href).searchParams.get('tag')") == "ops & qa"
        await manager.type_text(scope, selector='input[name=q]', text="Find", submit=True)
        await wait_for("!!document.querySelector('[data-applied-filter=q]')")
        # Applying Search with the disclosure closed retains its hidden selected tag.
        assert await page.evaluate("new URL(location.href).searchParams.get('tag')") == "ops & qa"
        await manager.click(scope, selector='[data-applied-filter=tag]')
        await wait_for("!document.querySelector('[data-applied-filter=tag]') && !!document.querySelector('[data-applied-filter=q]')")
        await page.navigate_and_wait(base + "/work?tag=alpha&tag=ops%20%26%20qa&tag_mode=or&lane=waiting")
        await wait_for("document.querySelector('[data-work-results]')?.textContent === '2 cards'")
        for width in (320, 390, 720, 1024, 1440):
            await manager.resize(scope, width=width, height=900)
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth && document.querySelector('.page-main').scrollWidth <= document.querySelector('.page-main').clientWidth"), width
        await manager.resize(scope, width=390, height=844)
        (tmp_path / "work-mobile.png").write_bytes(await page.screenshot())
        assert await page.evaluate("document.querySelector('[data-board-lane=waiting]').getAttribute('aria-pressed')") == "true"
        # A real touch tap opens the disclosure and selects a facet option.
        await page.command("Emulation.setTouchEmulationEnabled", {"enabled": True})

        async def tap(selector):
            await page.evaluate("document.querySelector(" + repr(selector) + ").scrollIntoView({block:'center', behavior:'instant'})")
            point = await page.evaluate("(() => {const e=document.querySelector(" + repr(selector) + ");const r=e.getBoundingClientRect();return {x:r.x+r.width/2,y:r.y+r.height/2};})()")
            await page.command("Input.dispatchTouchEvent", {"type": "touchStart", "touchPoints": [point]})
            await page.command("Input.dispatchTouchEvent", {"type": "touchEnd", "touchPoints": []})

        await tap('.work-filter-disclosure summary')
        await wait_for("document.querySelector('.work-filter-disclosure').open")
        # Remove a draft selection, then add it through the touch combobox.
        await tap('[data-remove-tag="ops & qa"]')
        await wait_for("!document.querySelector('[data-remove-tag=\"ops & qa\"]')")
        await tap('[data-facet-input]')
        await wait_for("document.activeElement.matches('[data-facet-input]')")
        await page.command("Input.insertText", {"text": "ops"})
        await wait_for("document.querySelector('[data-facet-options] [role=option]')?.textContent.includes('ops & qa')")
        await tap('[data-facet-options] [role=option]')
        await wait_for("!!document.querySelector('[data-remove-tag=\"ops & qa\"]')")
        await tap('.filter-actions a')
        await wait_for("!document.querySelector('[data-applied-filter]') && document.querySelector('[data-work-results]')?.textContent === '2 cards'")
    finally:
        await browser.close()
