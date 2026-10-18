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

        await wait_for("window.fixtureReady && document.querySelector('[data-issues-count]').textContent === '(1)'")
        await page.evaluate("window.tabs = [...document.querySelectorAll('#primary-widget [role=tab]')]; window.panels = [...document.querySelectorAll('#primary-widget [role=tabpanel]')]")
        assert await page.evaluate("tabs.map(t => t.textContent.trim())") == ["Details", "Branch from here", "Issues (1)"]
        assert await page.evaluate("panels.every(p => p.hidden && !p.querySelector('details').open)")
        assert await page.evaluate("new Set([...document.querySelectorAll('[role=tab], [role=tabpanel]')].map(e => e.id)).size") == 12
        assert await page.evaluate("tabs.every((t,i) => t.getAttribute('aria-controls') === panels[i].id && panels[i].getAttribute('aria-labelledby') === t.id)")
        assert await page.evaluate("document.querySelector('[data-session-telemetry]').closest('[data-acw-technical-details]') !== null")
        assert await page.evaluate("document.querySelector('[data-acw-effective-settings]').textContent") == "codex · confirmed-model · workspace-write"
        assert await page.evaluate("document.querySelector('[data-issues-count]').getAttribute('aria-live')") == "polite"
        tree = await page.command("Accessibility.getFullAXTree")
        accessible_tabs = [node.get("name", {}).get("value", "") for node in tree["nodes"] if node.get("role", {}).get("value") == "tab"]
        assert any(" ".join(name.split()) == "Issues (1)" for name in accessible_tabs), accessible_tabs

        # External changes refresh the closed panel without stealing the composer.
        await manager.type_text(scope, selector="#primary-widget [data-acw-input]", text="Keep this draft")
        await page.evaluate("fixtureIssues.revision++; fixtureIssues.issues.push({id:'external', title:'Added elsewhere'});")
        await wait_for("tabs[2].textContent.includes('(2 · updated)')")
        assert await page.evaluate("panels.every(p => p.hidden) && document.activeElement.matches('[data-acw-input]')")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "Keep this draft"
        await page.evaluate("fixtureIssues.revision++; fixtureIssues.issues[0].title='Edited elsewhere'")
        await wait_for("document.querySelector('[data-issues-list] input[aria-label=\"Issue title\"]').value === 'Edited elsewhere'")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(3)")
        await wait_for("tabs[2].textContent.trim() === 'Issues (2)'")
        await manager.type_text(scope, selector="#primary-widget [data-issues-add-text]", text="Third issue")
        await manager.click(scope, selector="#primary-widget [data-issues-add]")
        await wait_for("tabs[2].textContent.includes('(3)')")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(3)")

        for theme in ("light", "dark"):
            await page.evaluate(f"document.documentElement.dataset.appearance='{theme}'; document.querySelector('#theme').href='/static/themes/pa/{theme}.css'")
            for width in (390, 820, 1440):
                await manager.resize(scope, width=width, height=1000)
                assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                assert await page.evaluate("tabs.every(t => t.offsetTop === tabs[0].offsetTop)")

        await manager.press(scope, key="Home")
        assert await page.evaluate("document.activeElement === tabs[0]")
        await manager.press(scope, key="Space")
        await wait_for("!panels[0].hidden")
        await manager.click(scope, selector="#primary-widget [data-acw-execution-panel] > summary")
        await wait_for("!!panels[0].querySelector('select')")
        await page.evaluate("panels[0].querySelector('select').value='\"fixture-model\"'; panels[0].querySelector('select').dispatchEvent(new Event('change'))")
        await manager.click(scope, selector="#primary-widget [data-native-settings] button[type=submit]")
        await wait_for("document.querySelector('[data-native-settings-status]').textContent.includes('applied')")
        assert "12" in await page.evaluate("document.querySelector('[data-acw-timings]').textContent")
        await manager.click(scope, selector="#primary-widget [data-acw-tool-toggle]")
        assert await page.evaluate("!document.querySelector('[data-acw-tool-flyout]').hidden")
        await manager.click(scope, selector="#primary-widget [data-acw-tool-flyout] [data-acw-flyout-close]")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(1)")
        await manager.press(scope, key="ArrowRight")
        assert await page.evaluate("document.activeElement === tabs[1] && panels.every(p => p.hidden)")
        await manager.press(scope, key="Space")
        await wait_for("!document.querySelector('[data-thread-native]').disabled")
        await manager.click(scope, selector="#primary-widget [data-thread-copy]")
        await wait_for("document.querySelector('[data-thread-status] a')?.getAttribute('href') === '/agent?session=fixture-branch'")
        await manager.click(scope, selector="#primary-widget [role=tab]:nth-child(2)")
        await manager.press(scope, key="End")
        assert await page.evaluate("document.activeElement === tabs[2]")
        await manager.press(scope, key="ArrowRight")
        assert await page.evaluate("document.activeElement === tabs[0]")
        assert await page.evaluate("[...document.querySelectorAll('#other-widget [role=tabpanel]')].every(p => p.hidden)")
        await page.evaluate("fixtureWidget.sessionId='next'; fixtureWidget.issues.reset(); fixtureSnapshot.session.id='next'; PAExecutionSelection.renderSession(fixtureWidget,fixtureSnapshot)")
        assert await page.evaluate("tabs[2].textContent.trim() === 'Issues (loading…)' && panels.every(p => p.hidden)")
    finally:
        await manager.close()
        await browser.close()
