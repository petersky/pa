"""Tests for host service installation."""

from __future__ import annotations

import tempfile
import unittest
import asyncio
import json
import logging
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from typer.testing import CliRunner

from pa.cli import service
from pa.config import Settings
from pa.core.logging import configure_logging
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.acp.providers.metadata import save_credentials
from pa.core.kernel import Kernel


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

    def test_in_service_systemd_restart_is_non_blocking(self) -> None:
        accepted = MagicMock(returncode=0, stderr="", stdout="")
        progress = MagicMock()
        with (
            patch.object(service, "_is_darwin", return_value=False),
            patch.object(service, "_is_linux", return_value=True),
            patch.object(service, "_run_systemctl", return_value=accepted) as systemctl,
        ):
            service.request_restart(self.settings, progress=progress)

        systemctl.assert_called_once_with(
            "restart", "--no-block", service.SYSTEMD_UNIT
        )
        self.assertGreaterEqual(progress.call_count, 2)

    def test_in_service_launchd_restart_keeps_job_loaded(self) -> None:
        accepted = MagicMock(returncode=0, stderr="", stdout="")
        with (
            patch.object(service, "_is_darwin", return_value=True),
            patch.object(service, "_plist_path", return_value=self.plist),
            patch.object(service, "_run_launchctl", return_value=accepted) as launchctl,
        ):
            self.plist.write_text("installed")
            service.request_restart(self.settings)

        launchctl.assert_called_once_with("kickstart", "-k", service._domain_target())


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

            with patch(
                "pa.instance.quiesce.consume_skip_quiesce", return_value=False
            ):
                asyncio.run(kernel.shutdown(MagicMock()))

            agent.quiesce.assert_awaited_once()
            agent.stop.assert_awaited_once()

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
