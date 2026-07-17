import os
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa.browser.cdp import CdpError, CdpPage, validate_browser_url
from pa.browser.manager import BrowserAttachment, BrowserManager, _browser_executable
from pa.instance.agent_session import AgentSessionRuntime


class BrowserUrlTests(unittest.TestCase):
    def test_allows_web_and_blank_urls(self):
        for url in ("https://example.com", "http://127.0.0.1:8080", "about:blank"):
            self.assertEqual(validate_browser_url(url), url)

    def test_rejects_privileged_and_script_urls(self):
        for url in ("file:///etc/passwd", "javascript:alert(1)", "chrome://settings"):
            with self.assertRaises(CdpError):
                validate_browser_url(url)

    def test_executable_override(self):
        with patch.dict(os.environ, {"PA_BROWSER_EXECUTABLE": __file__}):
            self.assertEqual(_browser_executable(), __file__)


class BrowserAttachmentTests(unittest.IsolatedAsyncioTestCase):
    async def test_environment_and_public_state(self):
        process = AsyncMock()
        process.returncode = None
        attachment = BrowserAttachment(
            id="attachment-1",
            session_id="session-1",
            endpoint="http://127.0.0.1:9222",
            target_id="target-1",
            process=process,
            profile_dir=Path("/tmp/profile"),
        )
        with (
            patch.object(CdpPage, "metadata", AsyncMock(return_value={"target_id": "target-1", "title": "PA", "url": "https://example.com"})),
            patch.object(CdpPage, "viewport", AsyncMock(return_value={"width": 1600, "height": 1000, "device_scale_factor": 2})),
        ):
            state = await attachment.state()
            self.assertEqual(state["url"], "https://example.com")
            self.assertEqual((state["width"], state["height"]), (1600, 1000))
            self.assertEqual(state["device_scale_factor"], 2)
        self.assertEqual(attachment.environment()["PA_BROWSER_TARGET_ID"], "target-1")

    async def test_resize_updates_attachment_attributes(self):
        process = AsyncMock()
        process.returncode = None
        attachment = BrowserAttachment(
            id="attachment-1",
            session_id="session-1",
            endpoint="http://127.0.0.1:9222",
            target_id="target-1",
            process=process,
            profile_dir=Path("/tmp/profile"),
        )
        with patch.object(CdpPage, "resize", AsyncMock()) as resize:
            await attachment.resize(1920, 1080, device_scale_factor=2)
        resize.assert_awaited_once_with(1920, 1080, device_scale_factor=2)
        self.assertEqual((attachment.width, attachment.height), (1920, 1080))
        self.assertEqual(attachment.device_scale_factor, 2)


class BrowserManagerTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_browser_can_be_resized(self):
        manager = BrowserManager(Path("/tmp/pa-browser-test"))
        process = AsyncMock()
        process.returncode = None
        attachment = BrowserAttachment(
            id="attachment-1",
            session_id="session-1",
            endpoint="http://127.0.0.1:9222",
            target_id="target-1",
            process=process,
            profile_dir=Path("/tmp/profile"),
        )
        manager._attachments["session-1"] = attachment
        with patch.object(attachment, "resize", AsyncMock()) as resize:
            result = await manager.attach("session-1", width=1600, height=1000)
        self.assertIs(result, attachment)
        resize.assert_awaited_once_with(1600, 1000, device_scale_factor=1)


class BrowserSessionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_resize_persists_browser_config(self):
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        attachment = SimpleNamespace(
            id="attachment-1",
            width=1920,
            height=1080,
            device_scale_factor=2,
            resize=AsyncMock(),
            state=AsyncMock(return_value={"attached": True, "url": "https://example.com"}),
        )
        runtime.manager = SimpleNamespace(browser=SimpleNamespace(get=lambda _session_id: attachment))
        runtime.session = SimpleNamespace(
            id="session-1",
            config_json={"browser": {"attached": True}, "other": "kept"},
        )
        runtime.store = SimpleNamespace(save_session=MagicMock())
        runtime._append_transcript = MagicMock()
        runtime._flush_transcript = MagicMock()

        state = await runtime.resize_browser(1920, 1080, device_scale_factor=2)

        self.assertTrue(state["attached"])
        self.assertEqual(runtime.session.config_json["browser"]["width"], 1920)
        self.assertEqual(runtime.session.config_json["browser"]["height"], 1080)
        self.assertEqual(runtime.session.config_json["browser"]["device_scale_factor"], 2)
        self.assertEqual(runtime.session.config_json["other"], "kept")
        runtime.store.save_session.assert_called_once_with(runtime.session)
