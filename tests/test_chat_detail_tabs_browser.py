"""Rendered chat controls, real keyboard input, and retained panel functionality."""
import asyncio
import time

import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.chat_detail_tabs_fixture import build_detail_tabs_fixture
from tests.test_card_status_browser import operator_server


@pytest.fixture
def operator_app():
    return build_detail_tabs_fixture()


@pytest.mark.skipif(not _browser_executable(), reason="Chrome is required")
@pytest.mark.asyncio
async def test_chat_detail_tabs_layout_keyboard_and_existing_actions(operator_server, tmp_path):
    _, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="detail-tabs-test")
    scope = BrowserScope("user:test", "detail-tabs-test", "detail-tabs-test")
    try:
        await manager.attach(scope, url=base, width=1440, height=1000)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 8
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.025)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("window.fixtureReady")
        await page.evaluate("window.tabs = [...document.querySelectorAll('#primary-widget [role=tab]')]; window.panels = [...document.querySelectorAll('#primary-widget [role=tabpanel]')]")
        assert await page.evaluate("tabs.map(t => t.textContent.trim())") == ["Execution selection", "Timing details", "Branch from here", "Issues"]
        assert await page.evaluate("panels.every(p => p.hidden && !p.querySelector('details').open)")
        assert await page.evaluate("new Set([...document.querySelectorAll('[role=tab], [role=tabpanel]')].map(e => e.id)).size") == 16
        assert await page.evaluate("tabs.every((t,i) => t.getAttribute('aria-controls') === panels[i].id && panels[i].getAttribute('aria-labelledby') === t.id)")
        assert await page.evaluate("document.querySelector('#primary-widget [data-acw-details]').previousElementSibling.matches('[data-session-telemetry]')")
        assert await page.evaluate("[...document.querySelectorAll('#primary-widget .acw-rail-button')].map(b => [b.title,b.getAttribute('aria-label')])") == [["Tools", "Tool activity"], ["Terminal", "Terminal"], ["Plans", "Session plans"]]
        assert await page.evaluate("document.querySelector('[data-acw-terminal-toggle]').textContent") == ">_"

        for theme in ("light", "dark"):
            await page.evaluate(f"document.documentElement.dataset.appearance='{theme}'; document.querySelector('#theme').href='/static/themes/pa/{theme}.css'")
            for width in (390, 820, 1440):
                await manager.resize(scope, width=width, height=1000)
                measurements = await page.evaluate("""(() => {
                  const row = document.querySelector('#primary-widget [role=tablist]');
                  const chat = document.querySelector('.chrome-new-chat').getBoundingClientRect();
                  const card = document.querySelector('[data-new-card-open]').getBoundingClientRect();
                  return {sameHeight: !chat.height || chat.height === card.height,
                    oneLine: tabs.every(t => t.offsetTop === tabs[0].offsetTop),
                    overflows: row.scrollWidth > row.clientWidth,
                    scrollable: getComputedStyle(row).overflowX === 'auto',
                    pageFits: document.documentElement.scrollWidth <= innerWidth};
                })()""")
                assert measurements["sameHeight"] and measurements["oneLine"] and measurements["pageFits"], measurements
                if width == 390:
                    assert measurements["overflows"] and measurements["scrollable"], measurements

        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(1)")
        await wait_for("panels[0].querySelector('details').open && !!panels[0].querySelector('select')")
        await page.evaluate("panels[0].querySelector('select').value='\"fixture-model\"'; panels[0].querySelector('select').dispatchEvent(new Event('change'))")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(2)")
        assert await page.evaluate("panels[0].hidden && panels[1].querySelector('details').open && !panels[1].hidden")
        assert "12" in await page.evaluate("document.querySelector('[data-acw-timings]').textContent")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(2)")
        assert await page.evaluate("panels.every(p => p.hidden) && tabs.every(t => t.getAttribute('aria-selected') === 'false')")
        await manager.press(scope, key="ArrowRight")
        assert await page.evaluate("document.activeElement === tabs[2] && panels.every(p => p.hidden)")
        # A complete CDP Enter event needs CR text for native button activation
        # on macOS; PA's named-key helper currently omits it.
        await page.command("Input.dispatchKeyEvent", {"type": "keyDown", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13, "text": "\r"})
        await page.command("Input.dispatchKeyEvent", {"type": "keyUp", "key": "Enter", "code": "Enter", "windowsVirtualKeyCode": 13})
        await wait_for("panels[2].querySelector('details').open && !document.querySelector('[data-thread-native]').disabled")
        await manager.click(scope, selector="#primary-widget [data-thread-copy]")
        await wait_for("document.querySelector('[data-thread-status] a')?.getAttribute('href') === '/agent?session=fixture-branch'")

        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(4)")
        await wait_for("tabs[3].textContent.includes('(1)')")
        await manager.type_text(scope, selector="#primary-widget [data-issues-add-text]", text="Second issue")
        await manager.click(scope, selector="#primary-widget [data-issues-add]")
        await wait_for("tabs[3].textContent.includes('(2)')")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(4)")
        assert await page.evaluate("panels.every(p => p.hidden) && tabs[3].textContent.includes('(2)')")
        await manager.press(scope, key="Home")
        assert await page.evaluate("document.activeElement === tabs[0]")
        await manager.press(scope, key="Space")
        assert await page.evaluate("panels[0].querySelector('details').open && panels[0].querySelector('select').value === '\"fixture-model\"'")
        await manager.click(scope, selector="#primary-widget [data-native-settings] button[type=submit]")
        await wait_for("document.querySelector('[data-native-settings-status]').textContent.includes('applied')")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(1)")
        await manager.press(scope, key="End")
        assert await page.evaluate("document.activeElement === tabs[3]")
        await manager.press(scope, key="ArrowRight")
        assert await page.evaluate("document.activeElement === tabs[0]")
        await manager.press(scope, key="ArrowLeft")
        assert await page.evaluate("document.activeElement === tabs[3]")
        await manager.press(scope, key="Space")
        await wait_for("panels[3].querySelector('details').open")
        await manager.press(scope, key="Space")
        await wait_for("panels[3].hidden")
        assert await page.evaluate("[...document.querySelectorAll('#other-widget [role=tabpanel]')].every(p => p.hidden)")

        # Snapshots must update inside the panel without inserting a fifth tab,
        # reopening collapsed content, or losing IDs after a session change.
        await page.evaluate("fixtureWidget.sessionId='next'; fixtureWidget.issues.reset(); fixtureSnapshot.session.id='next'; PAExecutionSelection.renderSession(fixtureWidget,fixtureSnapshot)")
        assert await page.evaluate("tabs[3].textContent.trim() === 'Issues' && panels.every(p => p.hidden) && document.querySelectorAll('#primary-widget [role=tab]').length === 4")
        await page.evaluate("PAExecutionSelection.renderSession(fixtureWidget, {session: {id:'next'}})")
        assert await page.evaluate("!document.querySelector('[data-selection-empty]').hidden && document.querySelector('[data-session-execution-selection]').hidden")
    finally:
        await manager.close()
        await browser.close()
