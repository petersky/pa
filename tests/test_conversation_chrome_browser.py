"""Initial and live issue discovery through the full PA page and real APIs."""
import asyncio
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_card_status_browser import operator_server
from tests.visualization_chat_fixture import build_visualization_app


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_real_pa_initial_closed_issues_reload_and_recovery(operator_server, tmp_path):
    _, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="conversation-chrome-test")
    scope = BrowserScope("user:test", "conversation-chrome", "conversation-chrome-test")
    async with httpx.AsyncClient(base_url=base) as client:
        await client.get("/fixture")  # Seeds the existing canonical renderer fixture.
        await client.get("/api/health")
        headers = {"X-CSRF-Token": client.cookies.get("pa_csrf"), "Origin": base}
        url = "/api/agent/sessions/viz-session/issues"
        state = (await client.get(url)).json()
        issues = [{"id": "one", "title": "First issue"}, {"id": "two", "title": "Second issue"}]
        result = await client.put(url, headers=headers, json={"revision": state["revision"], "issues": issues})
        assert result.status_code == 200, result.text
        try:
            await manager.attach(scope, url=base + "/agent?session=viz-session", width=1280, height=1000)
            page = manager.resolve(scope).page

            async def wait_for(expression):
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    if await page.evaluate(expression):
                        return
                    await asyncio.sleep(.05)
                pytest.fail("Browser condition timed out: " + expression + "\n" + str(await page.evaluate("document.body.innerText.slice(-1600)")))

            await wait_for("document.querySelector('[data-issues-count]')?.textContent === '(2)'")
            assert await page.evaluate("!document.querySelector('[data-conversation-issues]').open")
            assert await page.evaluate("[...document.querySelectorAll('[role=tab]')].map(e=>e.textContent.trim())") == ["Details", "Branch from here", "Issues (2)"]
            await wait_for("document.querySelectorAll('.acw-visualization iframe').length === 3")

            state = result.json()
            issues.append({"id": "three", "title": "Added while closed"})
            result = await client.put(url, headers=headers, json={"revision": state["revision"], "issues": issues})
            assert result.status_code == 200, result.text
            await wait_for("document.querySelector('[data-issues-count]').textContent === '(3 · updated)'")
            assert await page.evaluate("!document.querySelector('[data-conversation-issues]').open")
            await wait_for("!document.querySelector('[data-acw-recovery]').hidden")
            assert await page.evaluate("document.querySelector('[data-acw-recovery-action]').textContent.includes('Live controls')")
            assert await page.evaluate("document.querySelector('[data-acw-input]').disabled")
            assert await page.evaluate("[...document.querySelectorAll('[data-acw-recovery] button')].filter(b=>!b.hidden).length") == 1
            for width, height in ((390, 844), (1280, 1000)):
                await manager.resize(scope, width=width, height=height)
                assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
                assert await page.evaluate("document.querySelector('[role=tablist]').getBoundingClientRect().height >= 30")
            await page.navigate_and_wait(base + "/agent?session=viz-session")
            await wait_for("document.querySelector('[data-issues-count]')?.textContent === '(3)'")
            await manager.click(scope, selector="[role=tab]:nth-child(3)")
            assert await page.evaluate("document.querySelector('[data-issues-count]').textContent") == "(3)"
            assert await page.evaluate("document.querySelectorAll('[data-issues-list] li').length") == 3
            # Browser interaction can outlive the setup client's idle socket.
            # Verify the persisted authority with a fresh connection, retaining
            # the same cookies and a single request with the default deadline.
            async with httpx.AsyncClient(base_url=base, cookies=client.cookies) as authority:
                response = await authority.get(url)
                assert response.status_code == 200, response.text
                assert response.json()["requests"] == [], "discovery never starts work"
        finally:
            await manager.close()
            await browser.close()
