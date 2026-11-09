"""Rendered chat chrome and bubbles through the full PA page."""
import asyncio
import base64
import json
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import TranscriptEvent
from pa.instance.quiesce import ImageAttachment
from pa.modules.agent_chat import _store_chat_images
from tests.test_card_status_browser import operator_server
from tests.visualization_chat_fixture import build_visualization_app

PIXEL = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="
SCHEDULED = json.dumps({
    "payload": "Report the cleanup state.", "id": 12345678901234567890, "a": 1, "b": 2, "c": 3, "d": 4,
    "e": 5, "f": 6, "g": 7, "h": 8, "i": 9, "j": 10, "k": 11,
})


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_bubbles_show_json_images_copy_and_visible_selection(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="chat-bubbles-test")
    scope = BrowserScope("user:test", "chat-bubbles", "chat-bubbles-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    store = app.state.ctx.store
    image = ImageAttachment(name="shot.png", mime_type="image/png", data=PIXEL)
    assert base64.b64decode(PIXEL)
    _store_chat_images(app.state.ctx.settings.data_dir / "chat-images" / "viz-session", [image])
    store.append_transcript_events([
        TranscriptEvent(session_id="viz-session", seq=100, event_type="user_message",
                        payload={"id": "json", "message": SCHEDULED, "images": [], "source": "ui"}),
        TranscriptEvent(session_id="viz-session", seq=101, event_type="user_message",
                        payload={"id": "img", "message": "With picture", "images": [image.public_dict()], "source": "ui"}),
        TranscriptEvent(session_id="viz-session", seq=102, event_type="user_message",
                        payload={"id": "old", "message": "Older", "images": [{"name": "old.png", "mime_type": "image/png"}], "source": "ui"}),
    ])
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

        await wait_for("[...document.querySelectorAll('.acw-json-block summary')].some(s => s.textContent.includes('Report the cleanup state.'))")
        await wait_for("[...document.querySelectorAll('.acw-message-images img')].some(i => i.naturalWidth === 1)")
        await wait_for("[...document.querySelectorAll('.acw-message-image-name')].some(s => s.textContent === 'old.png')")
        assert await page.evaluate("document.querySelector('.acw-json-block').open") is False

        # The stored markdown is what is copied, not the formatted presentation.
        await page.evaluate("""window.copied=[]; Object.defineProperty(navigator,'clipboard',{configurable:true,
            value:{writeText:t=>{window.copied.push(t);return Promise.resolve()}}})""")
        await page.evaluate("[...document.querySelectorAll('.acw-msg')].find(r => r.querySelector('.acw-json-block')).querySelector('[data-acw-copy-markdown]').click()")
        await wait_for("window.copied.length === 1 || document.querySelector('[data-copy-state]')")
        if await page.evaluate("isSecureContext"):
            assert await page.evaluate("window.copied[0]") == SCHEDULED
        assert await page.evaluate("document.querySelector('[data-copy-state]').dataset.copyState") == "copied"
        rows = await page.evaluate("[...document.querySelectorAll('.acw-msg')].filter(r => !r.querySelector('[data-acw-copy-markdown]')).length")
        assert rows == 0

        # A highlight has to differ from what it sits on.
        assert await page.evaluate("""(() => { const b=document.querySelector('.acw-bubble-user');
            return getComputedStyle(b,'::selection').backgroundColor !== getComputedStyle(b).backgroundColor })()""")

        # One-row composer; the Stop control only exists while there is a turn to stop.
        assert await page.evaluate("document.querySelector('[data-acw-stop]').getBoundingClientRect().height") == 0
        await page.evaluate("document.querySelector('[data-acw-input]').disabled = false")
        heights = await page.evaluate("""(() => { const i=document.querySelector('[data-acw-input]'), a=i.getBoundingClientRect().height;
            i.value='a\\nb\\nc\\nd'; i.dispatchEvent(new Event('input',{bubbles:true}));
            return [a, i.getBoundingClientRect().height] })()""")
        assert heights[1] > heights[0]

        # Narrow screens: the conversation only, with sessions on request.
        await manager.resize(scope, width=390, height=844)
        await wait_for("getComputedStyle(document.querySelector('.page-sidebar-left')).display === 'none'")
        assert await page.evaluate("document.documentElement.scrollWidth <= innerWidth")
        assert await page.evaluate("document.querySelector('[data-acw-input]').getBoundingClientRect().bottom <= innerHeight")
        # Layout settles a moment after the resize on a busy machine, so wait for it.
        await wait_for("document.querySelector('[data-acw-messages]').getBoundingClientRect().height > 250")
        await page.evaluate("document.querySelector('[data-agent-sidebar-toggle]').click()")
        await wait_for("getComputedStyle(document.querySelector('.page-sidebar-left')).display !== 'none'")
        await page.evaluate("document.querySelector('[data-agent-session-open]').click()")
        await wait_for("getComputedStyle(document.querySelector('.page-sidebar-left')).display === 'none'")
    finally:
        await manager.close()
        await browser.close()


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_session_links_open_the_chat_in_a_dialog_and_clean_up(operator_server, tmp_path):
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="chat-modal-test")
    scope = BrowserScope("user:test", "chat-modal", "chat-modal-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    app.state.ctx.store.append_transcript_events([
        TranscriptEvent(session_id="viz-session", seq=200, event_type="user_message",
                        payload={"id": "modal", "message": "Seen from the dialog", "images": [], "source": "ui"}),
    ])
    try:
        await manager.attach(scope, url=base + "/settings", width=1280, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        # A new-tab link keeps its normal behaviour; a plain link opens the dialog.
        await page.evaluate("""(() => { const n = document.createElement('a'); n.id = 'newtab'; n.href = '/agent?session=viz-session';
            n.target = '_blank'; n.addEventListener('click', e => e.preventDefault()); document.body.prepend(n);
            n.click();
            const a = document.createElement('a'); a.id = 'go'; a.href = '/agent?session=viz-session';
            a.textContent = 'Open'; document.body.prepend(a); })()""")
        assert await page.evaluate("document.getElementById('chat-modal-dialog').open") is False

        await page.evaluate("document.getElementById('go').click()")
        await wait_for("document.getElementById('chat-modal-dialog').open")
        await wait_for("(() => { const r = document.querySelector('#chat-modal-dialog [data-agent-chat]'); return !!(r && r._acw && r._acw.sessionId === 'viz-session') })()")
        await wait_for("document.querySelector('#chat-modal-dialog .acw-messages').textContent.includes('Seen from the dialog')")
        # Ids and draft keys are scoped to the dialog's own conversation.
        assert await page.evaluate("document.querySelector('#chat-modal-dialog [data-acw-input]').id") == "acw-input-modal-viz-session"
        assert await page.evaluate("document.querySelector('#chat-modal-dialog [data-chat-modal-full]').getAttribute('href')") == "/agent?session=viz-session"

        layout = """(() => { const d = document.getElementById('chat-modal-dialog').getBoundingClientRect(),
            m = document.querySelector('#chat-modal-dialog .acw-messages').getBoundingClientRect(),
            i = document.querySelector('#chat-modal-dialog [data-acw-input]').getBoundingClientRect();
            return {messages: m.height, inside: i.bottom <= d.bottom + 1 && i.top >= d.top && m.bottom <= i.top + 1} })()"""
        found = await page.evaluate(layout)
        assert found["messages"] > 150 and found["inside"], found
        await page.command("Emulation.setDeviceMetricsOverride", {"width": 390, "height": 760, "deviceScaleFactor": 1, "mobile": True})
        found = await page.evaluate(layout)
        assert found["messages"] > 100 and found["inside"], found
        assert await page.evaluate("document.getElementById('chat-modal-dialog').getBoundingClientRect().width") == 390
        await page.command("Emulation.clearDeviceMetricsOverride")
        for kind in ("keyDown", "keyUp"):
            await page.command("Input.dispatchKeyEvent", {"type": kind, "key": "Escape", "code": "Escape",
                                                          "windowsVirtualKeyCode": 27, "nativeVirtualKeyCode": 27})
        await wait_for("!document.getElementById('chat-modal-dialog').open")
        # Teardown and focus restore run from the dialog's asynchronous close event.
        await wait_for("document.getElementById('chat-modal-dialog-content').childElementCount === 0")
        await wait_for("document.activeElement && document.activeElement.id === 'go'")
    finally:
        await manager.close()
        await browser.close()
