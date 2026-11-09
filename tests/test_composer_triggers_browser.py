"""`/` and `$` menus in the real chat widget."""
import asyncio
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from tests.test_card_status_browser import operator_server  # noqa: F401
from tests.visualization_chat_fixture import build_visualization_app

CATALOG = {
    "generation": 1,
    "commands": [
        {"name": "review", "origin": "provider", "provider": "codex", "kind": "command", "description": "Review changes"},
        {"name": "rename", "origin": "provider", "provider": "codex", "kind": "command", "description": "Rename", "input_hint": "new name"},
        {"name": "pa:mode", "origin": "pa", "kind": "command", "description": "Mode", "input_hint": "default | plan", "input_required": True},
        {"name": "$seed", "origin": "provider", "provider": "codex", "kind": "skill", "description": "Make a seed"},
        {"name": "$google-drive:google-docs", "origin": "provider", "provider": "codex", "kind": "skill", "description": "Docs"},
    ],
}


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_slash_and_dollar_menus_complete_text_without_running_anything(operator_server, tmp_path):  # noqa: F811
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="composer-triggers-test")
    scope = BrowserScope("user:test", "composer-triggers", "composer-triggers-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    try:
        await manager.attach(scope, url=base + "/agent?session=viz-session", width=1280, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("!!(document.querySelector('[data-agent-chat]')._acw && document.querySelector('[data-acw-input]'))")
        await page.evaluate(f"""(() => {{ const w = document.querySelector('[data-agent-chat]')._acw;
            w.commandCatalog = {CATALOG!r}; w.commandCatalogSession = w.sessionId; }})()""".replace("True", "true"))

        # The widget code is idempotent with respect to a fixed catalog; type through the real input handler.
        type_text = """(([text, caret]) => { const i = document.querySelector('[data-acw-input]'); i.disabled = false;
            i.value = text; i.setSelectionRange(caret, caret); i.dispatchEvent(new Event('input', {bubbles: true})); })"""
        titles = "[...document.querySelectorAll('[data-acw-command-options] .acw-command-title')].map(e => e.textContent)"
        menu_open = "!document.querySelector('[data-acw-command-menu]').hidden"

        await page.evaluate(f"{type_text}(['/', 1])")
        assert await page.evaluate(menu_open)
        assert await page.evaluate(titles) == ["/pa:mode", "/rename", "/review"]  # skills never appear under "/"

        await page.evaluate(f"{type_text}(['$', 1])")
        assert await page.evaluate(titles) == ["$google-drive:google-docs", "$seed"]

        await page.evaluate(f"{type_text}(['$zzz', 4])")
        assert not await page.evaluate(menu_open)  # quiet when nothing matches: "$zzz" is ordinary text
        await page.evaluate(f"{type_text}(['costs $5', 8])")
        assert not await page.evaluate(menu_open)
        await page.evaluate(f"{type_text}(['/etc/hosts', 10])")
        assert await page.evaluate(titles) == []  # a path is not a command, and Enter will send it

        # A skill is completed in place, mid-sentence, without running anything.
        await page.evaluate(f"{type_text}(['use $se now', 7])")
        assert await page.evaluate(titles) == ["$seed"]
        await page.evaluate("document.querySelector('[data-acw-command-options] .acw-command-option').click()")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "use $seed now"
        assert await page.evaluate("document.querySelector('[data-acw-input]').selectionStart") == 10
        assert not await page.evaluate(menu_open)

        # A command with optional input waits for it instead of running immediately.
        await page.evaluate(f"{type_text}(['/ren', 4])")
        assert "Optional input: new name" in await page.evaluate("document.querySelector('.acw-command-input').textContent")
        await page.evaluate("document.querySelector('[data-acw-command-options] .acw-command-option').click()")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "/rename "
        assert await page.evaluate("document.body.textContent.includes('Running /rename')") is False

        # Tab completes the highlighted skill as well.
        await page.evaluate(f"{type_text}(['$goo', 4])")
        await page.evaluate("document.querySelector('[data-acw-input]').dispatchEvent(new KeyboardEvent('keydown', {key: 'Tab', bubbles: true, cancelable: true}))")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "$google-drive:google-docs "
    finally:
        await manager.close()
        await browser.close()
