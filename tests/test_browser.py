import asyncio
import os
import signal
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa.browser.cdp import CdpError, CdpPage, validate_browser_url
from pa.browser.manager import BrowserAttachment, BrowserManager, _browser_executable
from pa.instance.agent_session import AgentSessionRuntime
from pa.modules.browser import BrowserModule, McpBrowserController


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
            patch.object(
                CdpPage,
                "metadata",
                AsyncMock(
                    return_value={
                        "target_id": "target-1",
                        "title": "PA",
                        "url": "https://example.com",
                    }
                ),
            ),
            patch.object(
                CdpPage,
                "viewport",
                AsyncMock(
                    return_value={
                        "width": 1600,
                        "height": 1000,
                        "device_scale_factor": 2,
                    }
                ),
            ),
        ):
            state = await attachment.state()
            self.assertEqual(state["url"], "https://example.com")
            self.assertEqual((state["width"], state["height"]), (1600, 1000))
            self.assertEqual(state["device_scale_factor"], 2)
        self.assertEqual(attachment.environment()["PA_BROWSER_TARGET_ID"], "target-1")
        self.assertEqual(attachment.environment()["PA_BROWSER_SESSION_ID"], "session-1")

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

    async def test_cancelled_startup_terminates_the_browser_process_group(self):
        started = asyncio.Event()

        class WaitingClient:
            async def get(self, _url):
                started.set()
                await asyncio.Event().wait()

        process = SimpleNamespace(
            pid=4321,
            returncode=None,
            wait=AsyncMock(return_value=0),
        )
        with tempfile.TemporaryDirectory() as tmp:
            manager = BrowserManager(Path(tmp))
            manager._client = WaitingClient()
            with (
                patch("pa.browser.manager._browser_executable", return_value="chrome"),
                patch("pa.browser.manager._free_port", return_value=9222),
                patch(
                    "pa.browser.manager.asyncio.create_subprocess_exec",
                    AsyncMock(return_value=process),
                ),
                patch("pa.browser.manager.os.killpg") as killpg,
            ):
                task = asyncio.create_task(manager.attach("session-cancel"))
                await asyncio.wait_for(started.wait(), timeout=1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            killpg.assert_called_once_with(4321, signal.SIGTERM)
            process.wait.assert_awaited_once()
            self.assertNotIn("session-cancel", manager._attachments)


class BrowserSessionRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_resize_persists_browser_config(self):
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        attachment = SimpleNamespace(
            id="attachment-1",
            width=1920,
            height=1080,
            device_scale_factor=2,
            resize=AsyncMock(),
            state=AsyncMock(
                return_value={"attached": True, "url": "https://example.com"}
            ),
        )
        runtime.manager = SimpleNamespace(
            browser=SimpleNamespace(get=lambda _session_id: attachment)
        )
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
        self.assertEqual(
            runtime.session.config_json["browser"]["device_scale_factor"], 2
        )
        self.assertEqual(runtime.session.config_json["other"], "kept")
        runtime.store.save_session.assert_called_once_with(runtime.session)

    def test_save_merges_external_browser_with_in_memory_options(self):
        runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
        persisted = SimpleNamespace(
            config_json={"browser": {"attached": True, "width": 1920}}
        )
        runtime.session = SimpleNamespace(
            id="session-1",
            config_json={
                "browser": {"attached": True, "width": 800},
                "options": ["new"],
            },
        )
        runtime.store = SimpleNamespace(
            get_session=MagicMock(return_value=persisted),
            save_session=MagicMock(),
        )

        runtime._save_session_preserving_external_browser()

        self.assertEqual(runtime.session.config_json["browser"]["width"], 1920)
        self.assertEqual(runtime.session.config_json["options"], ["new"])
        runtime.store.save_session.assert_called_once_with(runtime.session)


class McpBrowserControllerTests(unittest.IsolatedAsyncioTestCase):
    async def test_session_state_reads_live_viewport_and_persists_resize(self):
        session = SimpleNamespace(
            config_json={"browser": {"url": "https://old.example"}}
        )
        store = SimpleNamespace(
            get_session=MagicMock(return_value=session),
            save_session=MagicMock(),
        )
        controller = McpBrowserController(Path("/tmp/pa-browser-test"), store)
        page = SimpleNamespace(
            metadata=AsyncMock(return_value={"url": "https://example.com"}),
            viewport=AsyncMock(
                return_value={"width": 1920, "height": 1080, "device_scale_factor": 2}
            ),
        )
        browser_env = {
            "PA_BROWSER_CDP_URL": "http://127.0.0.1:9222",
            "PA_BROWSER_TARGET_ID": "target-1",
            "PA_BROWSER_ATTACHMENT_ID": "attachment-1",
            "PA_BROWSER_SESSION_ID": "session-1",
        }
        with (
            patch.dict(os.environ, browser_env, clear=False),
            patch.object(controller, "page", return_value=page),
        ):
            state = await controller.state()
            controller.persist_session_attributes(url=state["url"])

        self.assertEqual(state["width"], 1920)
        self.assertEqual(state["height"], 1080)
        self.assertEqual(session.config_json["browser"]["width"], 1920)
        self.assertEqual(session.config_json["browser"]["url"], "https://example.com")
        store.save_session.assert_called_once_with(session)

    async def test_default_attach_persistence_preserves_saved_url(self):
        session = SimpleNamespace(
            config_json={"browser": {"url": "https://saved.example"}}
        )
        store = SimpleNamespace(
            get_session=MagicMock(return_value=session),
            save_session=MagicMock(),
        )
        controller = McpBrowserController(Path("/tmp/pa-browser-test"), store)
        controller.attributes = {"width": 1440, "height": 900, "device_scale_factor": 1}
        with patch.dict(
            os.environ, {"PA_BROWSER_SESSION_ID": "session-1"}, clear=False
        ):
            controller.persist_session_attributes(url=None)

        self.assertEqual(session.config_json["browser"]["url"], "https://saved.example")


class BrowserDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    async def test_manager_creates_page_target_when_chromium_starts_empty(self) -> None:
        class EmptyClient:
            async def get(self, _url):
                return SimpleNamespace(json=list)

            async def put(self, _url):
                return SimpleNamespace(
                    json=lambda: {"id": "target-new", "type": "page"}
                )

            async def aclose(self):
                return None

        process = SimpleNamespace(
            pid=4321, returncode=None, wait=AsyncMock(return_value=0)
        )
        with tempfile.TemporaryDirectory() as tmp:
            manager = BrowserManager(Path(tmp))
            manager._client = EmptyClient()
            with (
                patch("pa.browser.manager._browser_executable", return_value="chrome"),
                patch("pa.browser.manager._free_port", return_value=9222),
                patch(
                    "pa.browser.manager.asyncio.create_subprocess_exec",
                    AsyncMock(return_value=process),
                ),
                patch("pa.browser.manager.asyncio.sleep", AsyncMock()),
                patch.object(BrowserAttachment, "resize", AsyncMock()),
            ):
                attachment = await manager.attach("session-empty")
            self.assertEqual(attachment.target_id, "target-new")

    async def test_attach_failure_has_actionable_diagnostic(self) -> None:
        class EmptyClient:
            async def get(self, _url):
                return SimpleNamespace(json=list)

            async def put(self, _url):
                raise RuntimeError("unavailable")

        process = SimpleNamespace(
            pid=4321, returncode=1, wait=AsyncMock(return_value=1)
        )
        with tempfile.TemporaryDirectory() as tmp:
            manager = BrowserManager(Path(tmp))
            manager._client = EmptyClient()
            with (
                patch("pa.browser.manager._browser_executable", return_value="chrome"),
                patch("pa.browser.manager._free_port", return_value=9222),
                patch(
                    "pa.browser.manager.asyncio.create_subprocess_exec",
                    AsyncMock(return_value=process),
                ),
                self.assertRaisesRegex(
                    RuntimeError,
                    "supports --headless=new.*profile directory is writable",
                ),
            ):
                await manager.attach("session-failed")

    async def test_snapshot_reports_empty_page_and_rejects_error_page(self) -> None:
        class FakeMcp:
            def __init__(self):
                self.functions = {}

            def tool(self):
                def register(fn):
                    self.functions[fn.__name__] = fn
                    return fn

                return register

        mcp = FakeMcp()
        ctx = MagicMock()
        BrowserModule().register_mcp(mcp, ctx)
        page = SimpleNamespace(
            metadata=AsyncMock(return_value={"url": "https://pa.test", "title": "PA"}),
            evaluate=AsyncMock(
                return_value={
                    "document": {
                        "ready_state": "complete",
                        "url": "https://pa.test",
                        "title": "PA",
                        "body_text": "",
                    },
                    "elements": [],
                }
            ),
        )
        with patch.object(
            McpBrowserController, "ensure_page", AsyncMock(return_value=page)
        ):
            payload = __import__("json").loads(
                await mcp.functions["browser_snapshot"]()
            )
        self.assertEqual(payload["diagnostic"]["code"], "empty_snapshot")

        page.evaluate.return_value["document"]["url"] = "chrome-error://chromewebdata/"
        with (
            patch.object(
                McpBrowserController, "ensure_page", AsyncMock(return_value=page)
            ),
            self.assertRaises(CdpError),
        ):
            await mcp.functions["browser_snapshot"]()
