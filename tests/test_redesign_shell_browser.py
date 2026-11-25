"""Shared rail, persisted density, and unassigned global work in the real app."""
import asyncio
import time

import pytest
from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import CardCreate, ProjectCreate
from tests.test_card_status_browser import operator_server
from tests.test_local_operator_acceptance import operator_app


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_redesign_shell_navigation_appearance_and_global_scope(operator_server, tmp_path):
    app, base = operator_server
    store = app.state.ctx.store
    project = store.create_project(ProjectCreate(title="Redesign project"))
    store.create_card(CardCreate(title="Unassigned global fixture", lane="inbox"))
    store.create_card(CardCreate(title="Project global fixture", project_id=project.id, lane="inbox"))
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="redesign-test")
    scope = BrowserScope("user:test", "redesign", "redesign-test")
    try:
        await manager.attach(scope, url=base + '/work?group=lane', width=1440, height=1000)
        page = manager.resolve(scope).page
        async def wait(expression):
            end = time.monotonic() + 15
            while time.monotonic() < end:
                if await page.evaluate(expression):
                    await asyncio.sleep(.1)
                    return
                await asyncio.sleep(.05)
            pytest.fail(expression)
        await wait("document.querySelector('[data-work-results]')?.textContent.includes('2 cards')")
        assert await page.evaluate("document.querySelector('.workspace-rail').getBoundingClientRect().width") == 184
        assert await page.evaluate("document.querySelector('#pa-theme-toggle').previousElementSibling.getAttribute('href')") == '/settings'
        # Actual htmx navigation, never wholesale document replacement.
        await page.evaluate("window.redesignSentinel = 1")
        await manager.click(scope, selector='.workspace-rail a[href="/settings"]')
        await wait("!!document.querySelector('[data-density-preference]')")
        await page.evaluate("var s=document.querySelector('[data-density-preference]');s.value='compact';s.dispatchEvent(new Event('change'))")
        assert await page.evaluate("localStorage.getItem('pa.density')") == 'compact'
        await page.evaluate("PATheme.setAppearance('dark')")
        await wait("document.documentElement.dataset.appearance === 'dark'")
        await manager.click(scope, selector='.workspace-rail a[href="/work"]')
        await wait("!!document.querySelector('[data-work-board]')")
        assert await page.evaluate("window.redesignSentinel") == 1
        assert await page.evaluate("new URL(location.href).searchParams.has('project')") is False
        await manager.open(scope, url=base + '/work?group=lane')
        await wait("document.querySelector('[data-work-results]')?.textContent.includes('2 cards')")
        assert await page.evaluate("document.documentElement.dataset.density") == 'compact'
        for width in (1440, 720, 390, 320):
            await manager.resize(scope, width=width, height=1000)
            assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth"), width
            if width <= 720:
                await manager.click(scope, selector='[data-workspace-menu]')
                assert await page.evaluate("document.querySelector('[data-workspace-menu]').getAttribute('aria-expanded')") == 'true'
                await manager.press(scope, key='Escape')
                assert await page.evaluate("document.activeElement.hasAttribute('data-workspace-menu')")
                assert not await page.evaluate("document.body.classList.contains('is-navigation-open')")
            (tmp_path / f'redesign-{width}.png').write_bytes(await page.screenshot())
        # 200% zoom reflow equivalent: half of the desktop CSS viewport.
        await manager.resize(scope, width=720, height=500)
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        await page.evaluate("PATheme.setAppearance('light')")
        await wait("document.documentElement.dataset.appearance === 'light'")
    finally:
        await browser.close()
