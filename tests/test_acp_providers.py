"""Unit tests for ACP provider registry and selection cascade."""

from __future__ import annotations

import json
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock, patch

from fastapi import HTTPException

from pa.acp.providers.base import ProviderConfigureBody
from pa.acp.providers.codex import _codex_auth_status
from pa.acp.providers.codex_auth import (
    CodexLoginJob,
    CodexLoginJobStore,
    LoginState,
    redact_login_output,
)
from pa.acp.providers.registry import (
    DEFAULT_PROVIDER_ID,
    get_provider,
    list_provider_ids,
)
from pa.acp.providers.resolve import resolve_agent_provider, resolve_provider_id
from pa.acp.surfaces import (
    SURFACE_CHAT_CARD,
    SURFACE_CHAT_DEFAULT,
    AgentInvocationContext,
    surface_for_label,
)
from pa.config import Settings
from pa.core.preferences import SurfaceAgentPrefs, get_preferences_store
from pa.modules.agent_providers import LoginBody, start_provider_login


class AcpProviderTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.settings = Settings(data_dir=self.data_dir, agent_provider="cursor")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_builtin_providers_registered(self) -> None:
        ids = set(list_provider_ids())
        self.assertIn("cursor", ids)
        self.assertIn("codex", ids)
        self.assertEqual(get_provider("cursor").display_name, "Cursor")

    def test_surface_for_label(self) -> None:
        self.assertEqual(surface_for_label("default"), SURFACE_CHAT_DEFAULT)
        self.assertEqual(surface_for_label("card:abc"), SURFACE_CHAT_CARD)
        self.assertEqual(surface_for_label(None, project_id="p1"), "project")

    def test_resolve_defaults_to_cursor(self) -> None:
        ctx = AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "cursor")
        self.assertIn(source, {"instance", "default"})

    def test_resolve_user_overrides_instance(self) -> None:
        get_preferences_store(self.data_dir, user_id="alice").update(
            agent_provider="codex"
        )
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_DEFAULT, principal_id="user:alice"
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "user")

    def test_resolve_surface_overrides_user(self) -> None:
        get_preferences_store(self.data_dir, user_id="alice").update(
            agent_provider="cursor",
            agent_surfaces={
                SURFACE_CHAT_CARD: SurfaceAgentPrefs(provider="codex"),
            },
        )
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_CARD, principal_id="user:alice"
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "surface")

    def test_resolve_explicit_override_wins(self) -> None:
        ctx = AgentInvocationContext(
            surface=SURFACE_CHAT_DEFAULT,
            principal_id="user:alice",
            provider_override="codex",
        )
        pid, source = resolve_provider_id(self.settings, ctx)
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "override")

    def test_resolve_project_tool_config(self) -> None:
        ctx = AgentInvocationContext(surface="project", project_id="p1")
        pid, source = resolve_provider_id(
            self.settings, ctx, project_tool_config={"agent_provider": "codex"}
        )
        self.assertEqual(pid, "codex")
        self.assertEqual(source, "project")

    def test_cursor_spawn_defaults(self) -> None:
        resolved = resolve_agent_provider(
            self.settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.provider_id, DEFAULT_PROVIDER_ID)
        self.assertEqual(resolved.spec.command, "agent")
        self.assertEqual(resolved.spec.args, ["acp"])

    def test_codex_spawn_without_override(self) -> None:
        settings = Settings(data_dir=self.data_dir, agent_provider="codex")
        resolved = resolve_agent_provider(
            settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.provider_id, "codex")
        cmd = resolved.spec.command
        self.assertTrue(
            cmd in {"codex-acp", "npx"}
            or cmd.endswith("codex-acp")
            or cmd.endswith("/npx")
            or cmd.endswith("\\npx"),
            f"unexpected command: {cmd!r}",
        )

    def test_command_override(self) -> None:
        settings = Settings(
            data_dir=self.data_dir,
            agent_provider="codex",
            agent_command="custom-acp",
            agent_args=["--flag"],
        )
        resolved = resolve_agent_provider(
            settings, AgentInvocationContext(surface=SURFACE_CHAT_DEFAULT)
        )
        self.assertEqual(resolved.spec.command, "custom-acp")
        self.assertEqual(resolved.spec.args, ["--flag"])

    def test_codex_configure_persists_meta(self) -> None:
        provider = get_provider("codex")
        status = provider.configure(
            self.data_dir,
            ProviderConfigureBody(
                env={"INITIAL_AGENT_MODE": "agent"},
                secrets={"CODEX_API_KEY": "sk-test"},
                no_browser=True,
            ),
        )
        self.assertEqual(status.id, "codex")
        meta = json.loads(
            (self.data_dir / "agent_providers" / "codex.json").read_text()
        )
        self.assertEqual(meta["env"]["NO_BROWSER"], "1")
        creds = json.loads((self.data_dir / "integrations" / "codex.json").read_text())
        self.assertEqual(creds["CODEX_API_KEY"], "sk-test")

    def test_codex_status_recognizes_chatgpt_login(self) -> None:
        completed = __import__("subprocess").CompletedProcess(
            ["codex", "login", "status"], 0, "Logged in using ChatGPT\n", ""
        )
        with patch("pa.acp.providers.codex.subprocess.run", return_value=completed):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertTrue(configured)
        self.assertEqual(method, "chatgpt_oauth")
        self.assertIn("ChatGPT", message)
        self.assertIsNone(error)

    def test_codex_status_prefers_target_api_key_without_exposing_it(self) -> None:
        secret = "sk-test-never-return"
        configured, method, message, error = _codex_auth_status(
            None, creds={"CODEX_API_KEY": secret}, env={}
        )
        self.assertTrue(configured)
        self.assertEqual(method, "api_key")
        self.assertNotIn(secret, message)
        self.assertIsNone(error)

    def test_codex_status_handles_timeout(self) -> None:
        import subprocess

        with patch(
            "pa.acp.providers.codex.subprocess.run",
            side_effect=subprocess.TimeoutExpired(["codex", "login", "status"], 10),
        ):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertFalse(configured)
        self.assertEqual(method, "unknown")
        self.assertIn("timed out", message)
        self.assertIn("timed out", error or "")

    def test_codex_status_handles_logout_and_malformed_credentials(self) -> None:
        import subprocess

        logged_out = subprocess.CompletedProcess(
            ["codex", "login", "status"], 1, "Not logged in\n", ""
        )
        malformed_secret = "refresh_token=must-not-leak"
        malformed = subprocess.CompletedProcess(
            ["codex", "login", "status"], 1, "", malformed_secret
        )
        with patch(
            "pa.acp.providers.codex.subprocess.run",
            side_effect=[logged_out, malformed],
        ):
            logged_out_status = _codex_auth_status("/usr/bin/codex", creds={}, env={})
            malformed_status = _codex_auth_status("/usr/bin/codex", creds={}, env={})
        self.assertEqual(logged_out_status[:2], (False, "none"))
        self.assertEqual(malformed_status[:2], (False, "unknown"))
        self.assertNotIn(malformed_secret, " ".join(str(v) for v in malformed_status))

    def test_codex_status_unknown_success_is_not_marked_configured(self) -> None:
        import subprocess

        unknown = subprocess.CompletedProcess(
            ["codex", "login", "status"], 0, "Future login method\n", ""
        )
        with patch("pa.acp.providers.codex.subprocess.run", return_value=unknown):
            configured, method, message, error = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertFalse(configured)
        self.assertEqual(method, "unknown")
        self.assertIn("unknown", message)
        self.assertIsNone(error)

    def test_unscoped_process_api_key_does_not_mask_chatgpt_login(self) -> None:
        import subprocess

        chatgpt = subprocess.CompletedProcess(
            ["codex", "login", "status"], 0, "Logged in using ChatGPT\n", ""
        )
        with (
            patch.dict("os.environ", {"OPENAI_API_KEY": "unrelated-service-key"}),
            patch("pa.acp.providers.codex.subprocess.run", return_value=chatgpt),
        ):
            configured, method, _, _ = _codex_auth_status(
                "/usr/bin/codex", creds={}, env={}
            )
        self.assertTrue(configured)
        self.assertEqual(method, "chatgpt_oauth")

    def test_login_output_redacts_credentials_but_keeps_device_instructions(
        self,
    ) -> None:
        self.assertEqual(
            redact_login_output("access_token=very-secret"), "access_token=[redacted]"
        )
        self.assertIn(
            "[redacted]",
            redact_login_output("Bearer abcdefghijklmnopqrstuvwxyz0123456789"),
        )
        instructions = redact_login_output(
            "Open https://auth.openai.com/device and enter ABCD-EFGH"
        )
        self.assertIn("https://auth.openai.com/device", instructions)
        self.assertIn("ABCD-EFGH", instructions)
        authorization_instructions = redact_login_output(
            "Open the authorization page https://auth.openai.com/device and enter ABCD-EFGH"
        )
        self.assertIn("https://auth.openai.com/device", authorization_instructions)
        self.assertIn("ABCD-EFGH", authorization_instructions)

    def test_login_store_atomically_allows_only_one_active_job(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        barrier = threading.Barrier(2)

        def create_job():
            barrier.wait()
            try:
                return store.create()
            except ValueError:
                return None

        with ThreadPoolExecutor(max_workers=2) as executor:
            jobs = list(executor.map(lambda _: create_job(), range(2)))
        self.assertEqual(sum(job is not None for job in jobs), 1)

    def test_separate_worker_store_respects_active_disk_lease(self) -> None:
        first = CodexLoginJobStore(self.data_dir)
        job = first.create()
        second = CodexLoginJobStore(self.data_dir)
        self.assertEqual(second.latest_active().job_id, job.job_id)
        with self.assertRaisesRegex(ValueError, "already active"):
            second.create()

    def test_separate_worker_cancellation_reaches_process_owner(self) -> None:
        owner = CodexLoginJobStore(self.data_dir)
        job = owner.create()
        other_worker = CodexLoginJobStore(self.data_dir)
        other_worker.cancel(job.job_id)
        owner._refresh_cancelled(job)
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_cancel_before_process_registration_still_terminates_and_reaps(
        self,
    ) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        constructing = threading.Event()
        release = threading.Event()
        reaped = threading.Event()

        class FakeStdout:
            def readline(self):
                return ""

            def __iter__(self):
                return iter(())

        class FakeProcess:
            stdout = FakeStdout()
            pid = 12345
            returncode = None

            def poll(self):
                return self.returncode

            def wait(self, timeout=None):
                reaped.set()
                return self.returncode

        class FakeSelector:
            def register(self, *args):
                return None

            def select(self, timeout=None):
                return []

            def close(self):
                return None

        process = FakeProcess()

        def construct(*args, **kwargs):
            constructing.set()
            release.wait(timeout=2)
            return process

        def terminate(proc):
            proc.returncode = -15
            proc.wait(timeout=3)

        with (
            patch(
                "pa.acp.providers.codex_auth.subprocess.Popen", side_effect=construct
            ),
            patch(
                "pa.acp.providers.codex_auth.selectors.DefaultSelector", FakeSelector
            ),
            patch(
                "pa.acp.providers.codex_auth._terminate_process", side_effect=terminate
            ) as terminate_mock,
        ):
            store.start(job, "/custom/codex")
            self.assertTrue(constructing.wait(timeout=2))
            store.cancel(job.job_id)
            release.set()
            self.assertTrue(reaped.wait(timeout=2))
            deadline = time.monotonic() + 2
            while job.job_id in store._processes and time.monotonic() < deadline:
                time.sleep(0.01)
        terminate_mock.assert_called()
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_cancel_before_worker_start_never_launches_codex(self) -> None:
        store = CodexLoginJobStore(self.data_dir)
        job = store.create()
        store.cancel(job.job_id)
        with patch("pa.acp.providers.codex_auth.subprocess.Popen") as popen:
            store._run(job.job_id, "/custom/codex")
        popen.assert_not_called()
        self.assertEqual(job.state, LoginState.CANCELLED)

    def test_sigkill_fallback_reaps_login_process(self) -> None:
        import subprocess

        from pa.acp.providers.codex_auth import _terminate_process

        process = MagicMock()
        process.pid = 12345
        process.poll.return_value = None
        process.wait.side_effect = [
            subprocess.TimeoutExpired(["codex"], 3),
            -9,
        ]
        with patch("pa.acp.providers.codex_auth.os.killpg") as killpg:
            _terminate_process(process)
        self.assertEqual(process.wait.call_count, 2)
        self.assertEqual(killpg.call_count, 2)

    def test_windows_termination_uses_process_methods_and_reaps(self) -> None:
        import subprocess

        from pa.acp.providers.codex_auth import _terminate_process

        process = MagicMock()
        process.poll.return_value = None
        process.wait.side_effect = [subprocess.TimeoutExpired(["codex"], 3), -9]
        with patch("pa.acp.providers.codex_auth.os.name", "nt"):
            _terminate_process(process)
        process.terminate.assert_called_once()
        process.kill.assert_called_once()
        self.assertEqual(process.wait.call_count, 2)

    def test_resolve_codex_cli_honors_configured_executable(self) -> None:
        from pa.acp.providers.codex_auth import resolve_codex_cli

        configured = self.data_dir / "custom-codex"
        configured.write_text("#!/bin/sh\n")
        configured.chmod(0o700)
        self.assertEqual(resolve_codex_cli(str(configured)), str(configured))

    def test_login_store_marks_active_snapshot_interrupted_on_restart(self) -> None:
        directory = self.data_dir / "agent_provider_jobs" / "codex"
        directory.mkdir(parents=True)
        job = CodexLoginJob(
            job_id="job-1",
            state=LoginState.WAITING_FOR_USER,
            created_at="2026-01-01T00:00:00+00:00",
            updated_at="2026-01-01T00:00:00+00:00",
            expires_at="2026-01-01T00:10:00+00:00",
            timeout_seconds=600,
        )
        (directory / "job-1.json").write_text(job.model_dump_json())
        loaded = CodexLoginJobStore(self.data_dir).get("job-1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.state, LoginState.INTERRUPTED)

    def test_login_api_requires_explicit_consent_without_starting_process(self) -> None:
        request = MagicMock()
        request.app.state.ctx.settings.data_dir = self.data_dir
        with (
            patch("pa.modules.agent_providers.resolve_codex_cli") as resolve,
            self.assertRaises(HTTPException) as raised,
        ):
            start_provider_login(request, "codex", LoginBody(consent=False))
        self.assertEqual(raised.exception.status_code, 400)
        resolve.assert_not_called()

    def test_login_api_missing_cli_is_actionable(self) -> None:
        request = MagicMock()
        request.app.state.ctx.settings.data_dir = self.data_dir
        with (
            patch("pa.modules.agent_providers.resolve_codex_cli", return_value=None),
            self.assertRaises(HTTPException) as raised,
        ):
            start_provider_login(request, "codex", LoginBody(consent=True))
        self.assertEqual(raised.exception.status_code, 409)
        self.assertIn("not installed", str(raised.exception.detail))

    def test_unknown_provider_raises(self) -> None:
        with self.assertRaises(KeyError):
            get_provider("nope")


if __name__ == "__main__":
    unittest.main()
