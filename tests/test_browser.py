import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa.browser.cdp import CdpError, CdpPage, validate_browser_url
from pa.browser.manager import (
    BROWSER_STARTUP_MAX_ATTEMPTS,
    BROWSER_STARTUP_TIMEOUT_SECONDS,
    BrowserAttachment,
    BrowserManager,
    _browser_executable,
    _owned_browsers,
    _register_browser,
    _registry_dir,
    _terminate_owned_browsers_at_exit,
    reap_orphaned_browsers,
)
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
                patch("pa.browser.manager.reap_orphaned_browsers"),
                patch("pa.browser.manager._register_browser"),
            ):
                task = asyncio.create_task(manager.attach("session-cancel"))
                await asyncio.wait_for(started.wait(), timeout=1)
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

            killpg.assert_called_once_with(4321, signal.SIGTERM)
            process.wait.assert_awaited_once()
            self.assertNotIn("session-cancel", manager._attachments)


def _fake_browser(profile_dir: Path) -> subprocess.Popen:
    """A stand-in process whose command line looks like a launched browser."""
    return subprocess.Popen(
        [
            sys.executable,
            "-c",
            "import time; time.sleep(120)",
            "--headless=new",
            f"--user-data-dir={profile_dir}",
        ],
        start_new_session=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


class OrphanedBrowserTests(unittest.TestCase):
    """Browsers whose owner died must not survive to hijack the user's Chrome."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.registry = Path(self.tmp.name) / "registry"
        patcher = patch.dict(os.environ, {"PA_BROWSER_REGISTRY_DIR": str(self.registry)})
        patcher.start()
        self.addCleanup(patcher.stop)
        self.processes: list[subprocess.Popen] = []
        self.addCleanup(self._kill_processes)
        _owned_browsers.clear()
        self.addCleanup(_owned_browsers.clear)

    def _kill_processes(self):
        for process in self.processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    def _launch(self, name: str) -> tuple[subprocess.Popen, Path]:
        profile = Path(self.tmp.name) / name
        process = _fake_browser(profile)
        self.processes.append(process)
        return process, profile

    def _record(self, browser, profile, *, owner_pid, owner_started=""):
        self.registry.mkdir(parents=True, exist_ok=True)
        (self.registry / f"{browser.pid}.json").write_text(
            json.dumps(
                {
                    "pid": browser.pid,
                    "profile_dir": str(profile),
                    "owner_pid": owner_pid,
                    "owner_started": owner_started,
                }
            )
        )

    def _dead_pid(self) -> int:
        finished = subprocess.Popen([sys.executable, "-c", "pass"])
        finished.wait()
        return finished.pid

    def test_registry_dir_is_private_to_the_user_by_default(self):
        with patch.dict(os.environ):
            os.environ.pop("PA_BROWSER_REGISTRY_DIR")
            self.assertIn("pa-browser-registry", str(_registry_dir()))

    def test_reaps_browser_whose_owner_is_gone(self):
        browser, profile = self._launch("orphan")
        self._record(browser, profile, owner_pid=self._dead_pid())

        reaped = reap_orphaned_browsers()

        self.assertEqual(reaped, [browser.pid])
        self.assertIsNotNone(browser.wait(timeout=10))
        self.assertEqual(list(self.registry.glob("*.json")), [])

    def test_keeps_browser_whose_owner_is_still_running(self):
        browser, profile = self._launch("live-owner")
        owner = subprocess.Popen(["sleep", "60"])
        self.addCleanup(lambda: (owner.kill(), owner.wait()))
        started = subprocess.run(
            ["ps", "-o", "lstart=", "-p", str(owner.pid)],
            capture_output=True,
            text=True,
        ).stdout.strip()
        self._record(browser, profile, owner_pid=owner.pid, owner_started=started)

        self.assertEqual(reap_orphaned_browsers(), [])

        self.assertIsNone(browser.poll())
        self.assertEqual(len(list(self.registry.glob("*.json"))), 1)

    def test_owner_pid_reuse_is_not_mistaken_for_a_live_owner(self):
        browser, profile = self._launch("reused-owner")
        # A live pid, but with a different start time than the recorded owner:
        # the original owner died and the pid now belongs to something else.
        stranger = subprocess.Popen(["sleep", "60"])
        self.addCleanup(lambda: (stranger.kill(), stranger.wait()))
        self._record(
            browser,
            profile,
            owner_pid=stranger.pid,
            owner_started="Mon Jan  1 00:00:00 2001",
        )

        self.assertEqual(reap_orphaned_browsers(), [browser.pid])

        self.assertIsNotNone(browser.wait(timeout=10))
        self.assertIsNone(stranger.poll())

    def test_never_signals_a_reused_pid_that_is_not_the_browser(self):
        bystander, _ = self._launch("bystander")
        self._record(
            bystander,
            Path(self.tmp.name) / "some-other-profile",
            owner_pid=self._dead_pid(),
        )

        self.assertEqual(reap_orphaned_browsers(), [])

        self.assertIsNone(bystander.poll())
        self.assertEqual(list(self.registry.glob("*.json")), [])

    def test_corrupt_registry_entries_are_discarded(self):
        self.registry.mkdir(parents=True)
        (self.registry / "garbage.json").write_text("{not json")
        self.assertEqual(reap_orphaned_browsers(), [])
        self.assertEqual(list(self.registry.glob("*.json")), [])

    def test_exit_hook_terminates_browsers_this_process_still_owns(self):
        browser, profile = self._launch("owned")
        _register_browser(browser.pid, profile)
        self.assertEqual(len(list(self.registry.glob("*.json"))), 1)

        _terminate_owned_browsers_at_exit()

        self.assertIsNotNone(browser.wait(timeout=10))
        self.assertEqual(list(self.registry.glob("*.json")), [])
        self.assertEqual(_owned_browsers, {})


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
            config_json={"browser": {"attached": True, "width": 1920}},
            archived_at="archived", archive_reason="user_archive", pinned_at="pinned",
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
        self.assertEqual(runtime.session.archived_at, "archived")
        self.assertEqual(runtime.session.archive_reason, "user_archive")
        self.assertEqual(runtime.session.pinned_at, "pinned")
        runtime.store.save_session.assert_called_once_with(
            runtime.session, expected_connection_id=""
        )


class BrowserDiagnosticTests(unittest.IsolatedAsyncioTestCase):
    def test_startup_timeout_allows_loaded_hosts_more_than_six_seconds(self) -> None:
        self.assertGreaterEqual(BROWSER_STARTUP_TIMEOUT_SECONDS, 12)
        self.assertGreaterEqual(BROWSER_STARTUP_MAX_ATTEMPTS, 100)

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
