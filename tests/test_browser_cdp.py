"""CDP session lifetime and observable viewport regressions."""

import asyncio
import json
import struct
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from websockets.exceptions import WebSocketException

from pa.browser.cdp import CdpError, CdpPage
from pa.browser.manager import BrowserManager, _browser_executable


@pytest.mark.asyncio
async def test_commands_share_connection_and_correlate_responses():
    websocket = AsyncMock()
    websocket.recv.side_effect = [
        json.dumps({"method": "Page.frameResized"}),
        json.dumps({"id": 99, "result": {"ignored": True}}),
        json.dumps({"id": 1, "result": {"first": True}}),
        json.dumps({"id": 2, "error": {"message": "invalid parameter"}}),
        json.dumps({"id": 3, "result": {"third": True}}),
    ]
    page = CdpPage("http://browser", "target")
    with (
        patch("pa.browser.cdp.page_target", AsyncMock(return_value={
            "id": "target", "webSocketDebuggerUrl": "ws://browser/target",
        })),
        patch("pa.browser.cdp.websockets.connect", AsyncMock(return_value=websocket)) as connect,
    ):
        assert await page.command("First") == {"first": True}
        with pytest.raises(CdpError, match="invalid parameter"):
            await page.command("Invalid")
        assert await page.command("Third") == {"third": True}
        connect.assert_awaited_once()
        websocket.close.assert_not_awaited()
        await page.close()
        await page.close()
        websocket.close.assert_awaited_once()


@pytest.mark.parametrize("failure", [TimeoutError, asyncio.CancelledError, OSError, WebSocketException])
@pytest.mark.asyncio
async def test_interrupted_commands_disconnect_without_replaying(failure):
    broken, replacement = AsyncMock(), AsyncMock()
    broken.recv.side_effect = failure
    replacement.recv.return_value = json.dumps({"id": 2, "result": {"ok": True}})
    page = CdpPage("http://browser", "target")
    with (
        patch("pa.browser.cdp.page_target", AsyncMock(return_value={
            "id": "target", "webSocketDebuggerUrl": "ws://browser/target",
        })),
        patch("pa.browser.cdp.websockets.connect", AsyncMock(side_effect=[broken, replacement])) as connect,
    ):
        with pytest.raises(CdpError if failure is TimeoutError else failure):
            await page.command("Input.insertText", {"text": "once"})
        broken.send.assert_awaited_once()
        broken.close.assert_awaited_once()
        connect.assert_awaited_once()
        assert await page.command("Runtime.evaluate") == {"ok": True}
        assert json.loads(replacement.send.call_args.args[0])["method"] == "Runtime.evaluate"
        await page.close()
        replacement.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_failed_initial_resize_closes_transport_and_process(tmp_path):
    browser = BrowserManager(tmp_path)
    process = SimpleNamespace(returncode=None)
    browser._client = SimpleNamespace(get=AsyncMock(return_value=SimpleNamespace(
        json=lambda: [{"type": "page", "id": "target"}],
    )))
    with (
        patch("pa.browser.manager._browser_executable", return_value="chrome"),
        patch("pa.browser.manager.asyncio.create_subprocess_exec", AsyncMock(return_value=process)),
        patch.object(CdpPage, "resize", AsyncMock(side_effect=CdpError("resize failed"))),
        patch.object(CdpPage, "close", AsyncMock()) as close,
        patch.object(browser, "_terminate_process", AsyncMock()) as terminate,
    ):
        with pytest.raises(CdpError, match="resize failed"):
            await browser.attach("failed-start")
        close.assert_awaited_once()
        terminate.assert_awaited_once_with(process)
        assert browser.get("failed-start") is None


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_viewport_survives_commands_navigation_and_screenshot(tmp_path):
    browser = BrowserManager(tmp_path)
    try:
        attachment = await browser.attach("viewport", width=1440, height=900)
        page = attachment.page
        await attachment.resize(390, 844, device_scale_factor=2)
        expected = {"width": 390, "height": 844, "device_scale_factor": 2}
        assert await page.viewport() == expected
        await page.navigate_and_wait("data:text/html,<title>Resized</title><button>Test</button>")
        assert await page.viewport() == expected
        assert await page.evaluate("matchMedia('(max-width: 400px)').matches") is True
        png = await page.screenshot()
        assert struct.unpack(">II", png[16:24]) == (780, 1688)
        state = await attachment.state()
        assert {key: state[key] for key in expected} == expected
        await attachment.resize(800, 600)
        assert await page.viewport() == {"width": 800, "height": 600, "device_scale_factor": 1}
        with patch.object(page, "close", wraps=page.close) as close:
            await browser.detach("viewport")
            close.assert_awaited_once()
        assert attachment.process.returncode is not None
        assert browser.get("viewport") is None
    finally:
        await browser.close()
