"""Tests for host service installation."""

from __future__ import annotations

import asyncio
import json
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from pa.acp.providers.metadata import save_credentials
from pa.cli import service
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.logging import configure_logging
from pa.domain.instance_config import InstanceConfig, save_instance_config


class InstallPlistTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.plist = Path(self._tmp.name) / service.PLIST_NAME
        self.settings = MagicMock()
        self.pa_bin = Path("/usr/local/bin/pa")

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _install(self, content: bytes) -> Path:
        with (
            patch.object(service, "_is_darwin", return_value=True),
            patch.object(service, "_launch_agents_dir", return_value=self.plist.parent),
            patch.object(service, "_plist_path", return_value=self.plist),
            patch.object(service, "render_plist", return_value=content),
        ):
            return service.install_plist(self.settings, self.pa_bin)

    def test_does_not_rewrite_unchanged_plist(self) -> None:
        content = b"existing launch agent"
        self.plist.write_bytes(content)
        original_mtime = self.plist.stat().st_mtime_ns

        result = self._install(content)

        self.assertEqual(result, self.plist)
        self.assertEqual(self.plist.stat().st_mtime_ns, original_mtime)

    def test_writes_changed_plist(self) -> None:
        self.plist.write_bytes(b"old launch agent")

        result = self._install(b"updated launch agent")

        self.assertEqual(result, self.plist)
        self.assertEqual(self.plist.read_bytes(), b"updated launch agent")

    def test_launchd_unit_has_bounded_restart_and_resource_controls(self) -> None:
        settings = Settings(data_dir=Path(self._tmp.name))
        rendered = service.render_plist(settings, self.pa_bin).decode()
        for control in (
            "ThrottleInterval",
            "ExitTimeOut",
            "ProcessType",
            "SoftResourceLimits",
            "HardResourceLimits",
        ):
            self.assertIn(control, rendered)
        self.assertIn("<key>KeepAlive</key>\n    <true/>", rendered)
        self.assertIn("<key>ExitTimeOut</key>\n    <integer>300</integer>", rendered)

    def test_in_service_systemd_restart_uses_detached_transient_unit(self) -> None:
        accepted = MagicMock(returncode=0, stderr="", stdout="queued")
        progress = MagicMock()
        with (
            patch.object(service, "_is_darwin", return_value=False),
            patch.object(service, "_is_linux", return_value=True),
            patch.object(service, "find_service_binary", return_value=self.pa_bin),
            patch.object(service, "install_service") as install,
            patch.object(service, "_run_systemctl", return_value=accepted) as systemctl,
            patch.object(
                service, "_run_systemd_run", return_value=accepted
            ) as systemd_run,
        ):
            diagnostic = service.request_restart(
                self.settings, progress=progress, operation_id="operation-123"
            )

        install.assert_called_once_with(self.settings, self.pa_bin)
        systemctl.assert_called_once_with("daemon-reload")
        args = systemd_run.call_args.args
        self.assertEqual(
            args[:2], ("--unit", service._systemd_restart_unit_name("operation-123"))
        )
        self.assertIn("--collect", args)
        self.assertIn("--no-block", args)
        self.assertIn("Type=oneshot", args)
        self.assertIn("systemctl --user restart --no-block pa-server.service", args[-1])
        self.assertEqual(diagnostic.state, "restart_requested")
        self.assertEqual(diagnostic.backend, "systemd")
        self.assertEqual(diagnostic.exit_code, 0)
        self.assertIsNone(diagnostic.signal)
        self.assertEqual(diagnostic.stdout, "queued")
        self.assertGreaterEqual(diagnostic.duration_ms, 0)
        self.assertGreaterEqual(progress.call_count, 2)

    def test_systemd_dispatch_signal_loss_requires_verification_not_failure(
        self,
    ) -> None:
        reload = MagicMock(returncode=0, stderr="", stdout="")
        interrupted = MagicMock(returncode=-15, stderr="teardown", stdout="accepted")
        with (
            patch.object(service, "_is_darwin", return_value=False),
            patch.object(service, "_is_linux", return_value=True),
            patch.object(service, "find_service_binary", return_value=None),
            patch.object(service, "_run_systemctl", return_value=reload),
            patch.object(service, "_run_systemd_run", return_value=interrupted),
        ):
            diagnostic = service.request_restart(
                self.settings, operation_id="operation-123"
            )

        self.assertEqual(diagnostic.state, "restart_response_lost")
        self.assertIsNone(diagnostic.exit_code)
        self.assertEqual(diagnostic.signal, 15)
        self.assertEqual(diagnostic.stdout, "accepted")
        self.assertEqual(diagnostic.stderr, "teardown")

    def test_systemd_rejection_is_confirmed_and_diagnostically_complete(self) -> None:
        reload = MagicMock(returncode=0, stderr="", stdout="")
        rejected = MagicMock(
            returncode=1,
            stderr="sync_token=super-secret permission denied",
            stdout="manager reply",
        )
        with (
            patch.object(service, "_is_darwin", return_value=False),
            patch.object(service, "_is_linux", return_value=True),
            patch.object(service, "find_service_binary", return_value=None),
            patch.object(service, "_run_systemctl", return_value=reload),
            patch.object(service, "_run_systemd_run", return_value=rejected),
        ):
            with self.assertRaises(service.RestartRejectedError) as raised:
                service.request_restart(self.settings, operation_id="operation-123")

        diagnostic = raised.exception.diagnostic
        self.assertEqual(diagnostic.state, "restart_rejected")
        self.assertEqual(diagnostic.backend, "systemd")
        self.assertEqual(diagnostic.exit_code, 1)
        self.assertIsNone(diagnostic.signal)
        self.assertEqual(diagnostic.stdout, "manager reply")
        self.assertIn("[redacted]", diagnostic.stderr)
        self.assertNotIn("super-secret", diagnostic.stderr)
        self.assertTrue(diagnostic.started_at)
        self.assertTrue(diagnostic.completed_at)

    def test_in_service_launchd_restart_schedules_rebootstrap(self) -> None:
        settings = Settings(data_dir=Path(self._tmp.name))
        with (
            patch.object(service, "_is_darwin", return_value=True),
            patch.object(service, "_is_linux", return_value=False),
            patch.object(service, "_plist_path", return_value=self.plist),
            patch.object(service, "find_service_binary", return_value=self.pa_bin),
            patch.object(service, "install_service") as install,
            patch.object(service, "_schedule_launchd_rebootstrap") as schedule,
            patch.object(service, "_run_launchctl") as launchctl,
        ):
            self.plist.write_text("installed")
            diagnostic = service.request_restart(settings)

        self.assertEqual(diagnostic.state, "restart_requested")
        self.assertEqual(diagnostic.backend, "launchd")
        self.assertEqual(diagnostic.exit_code, 0)
        install.assert_called_once_with(settings, self.pa_bin)
        schedule.assert_called_once_with(
            self.plist,
            log_path=Path(self._tmp.name) / "logs" / "service-rebootstrap.log",
        )
        launchctl.assert_not_called()

    def test_launchd_rebootstrap_script_waits_for_bootout_and_retries(self) -> None:
        log_path = Path(self._tmp.name) / "rebootstrap.log"
        with patch.object(service.subprocess, "Popen") as popen:
            service._schedule_launchd_rebootstrap(self.plist, log_path=log_path)

        script = popen.call_args.args[0][2]
        self.assertIn("deadline=$(( $(date +%s) + 300 ))", script)
        self.assertIn("for delay in 0.5 1 1.5 2 3 4 5 6", script)
        self.assertIn('*"Could not find service"*) return 1 ;;', script)
        self.assertIn("job_loaded", script)
        self.assertIn(str(log_path), script)


class AutonomousHostControlsTests(unittest.TestCase):
    def test_shutdown_snapshots_open_sessions_after_transport_loss(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            runtime = MagicMock(_closed=False)
            agent = MagicMock(connected=False, prompting=False)
            agent.list_runtimes.return_value = [runtime]
            agent.quiesce = AsyncMock()
            agent.stop = AsyncMock()
            ctx = MagicMock()
            ctx.settings = Settings(data_dir=Path(tmp))
            ctx.hooks.emit = AsyncMock()
            ctx.services = {"instance_agent": agent}
            registry = MagicMock(modules=[])
            kernel = Kernel(ctx, registry)

            with patch("pa.instance.quiesce.consume_skip_quiesce", return_value=False):
                asyncio.run(kernel.shutdown(MagicMock()))

            agent.quiesce.assert_awaited_once()
            agent.stop.assert_awaited_once()

    def test_shutdown_skip_quiesce_uses_fast_runtime_stop(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            agent = MagicMock(connected=True, prompting=False)
            agent.list_runtimes.return_value = [MagicMock(_closed=False)]
            agent.quiesce = AsyncMock()
            agent.stop = AsyncMock()
            ctx = MagicMock()
            ctx.settings = Settings(data_dir=Path(tmp))
            ctx.hooks.emit = AsyncMock()
            ctx.services = {"instance_agent": agent}
            kernel = Kernel(ctx, MagicMock(modules=[]))

            with patch("pa.instance.quiesce.consume_skip_quiesce", return_value=True):
                asyncio.run(kernel.shutdown(MagicMock()))

            agent.quiesce.assert_not_awaited()
            agent.stop.assert_awaited_once_with(fast=True)

    def test_secret_files_are_owner_only_even_when_replacing_loose_files(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            config = root / "config.json"
            config.write_text("{}")
            config.chmod(0o644)
            save_instance_config(root, InstanceConfig(sync_token="secret"))
            credentials = root / "integrations" / "codex.json"
            save_credentials(root, "codex", {"CODEX_API_KEY": "secret"})
            self.assertEqual(config.stat().st_mode & 0o777, 0o600)
            self.assertEqual(credentials.stat().st_mode & 0o777, 0o600)

    def test_structured_log_is_persistent_and_machine_readable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            configure_logging(settings)
            logging.getLogger("pa.smoke").warning(
                "health degraded api_key=%s", "sk_test-secret-value"
            )
            for handler in logging.getLogger().handlers:
                handler.flush()
            payload = json.loads((settings.data_dir / "logs" / "pa.jsonl").read_text())
            self.assertEqual(payload["logger"], "pa.smoke")
            self.assertEqual(payload["message"], "health degraded api_key=[redacted]")
            self.assertNotIn("sk_test-secret-value", json.dumps(payload))
            self.assertEqual(payload["level"], "WARNING")
            self.assertEqual(logging.getLogger("httpx").level, logging.WARNING)

    def test_service_only_no_start_preserves_authority_migration_barrier(self) -> None:
        from pa.cli.main import app

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            with (
                patch("pa.cli.main.get_settings", return_value=settings),
                patch("pa.cli.service.service_supported", return_value=True),
                patch(
                    "pa.cli.service.find_service_binary",
                    return_value=Path("/bin/pa"),
                ),
                patch(
                    "pa.cli.service.install_service",
                    return_value=Path("/tmp/pa.plist"),
                ),
                patch(
                    "pa.cli.service.get_status",
                    return_value=MagicMock(backend="launchd"),
                ),
                patch("pa.cli.service.bootstrap") as bootstrap,
                patch("pa.install.runner.record_install"),
            ):
                result = CliRunner().invoke(
                    app, ["install", "--service-only", "--no-start"]
                )
        self.assertEqual(result.exit_code, 0, result.output)
        bootstrap.assert_not_called()
        self.assertIn("Service left stopped", result.output)


if __name__ == "__main__":
    unittest.main()
