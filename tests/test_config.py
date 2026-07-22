"""Settings loading and data-directory isolation tests."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from pa.config import get_settings, reset_settings
from pa.domain.instance_config import InstanceConfig, save_instance_config


class DataDirIsolationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        root = Path(self._tmp.name)
        self.home = root / "home"
        self.default_data_dir = self.home / ".pa"
        self.isolated_data_dir = root / "isolated"
        self.home.mkdir()
        self.production_config = InstanceConfig(
            instance_id="production-instance",
            instance_name="production",
            fleet_id="production-fleet",
            peers=["https://production.invalid"],
            sync_token="production-token",
            session_secret="",
        )
        save_instance_config(self.default_data_dir, self.production_config)
        self.production_bytes = (self.default_data_dir / "config.json").read_bytes()

    def tearDown(self) -> None:
        reset_settings()
        self._tmp.cleanup()

    def _environment(self) -> dict[str, str]:
        return {
            "HOME": str(self.home),
            "PA_DATA_DIR": str(self.isolated_data_dir),
        }

    def assert_production_config_unchanged(self) -> None:
        self.assertEqual(
            (self.default_data_dir / "config.json").read_bytes(),
            self.production_bytes,
        )

    def test_get_settings_loads_only_config_from_pa_data_dir(self) -> None:
        isolated_config = InstanceConfig(
            instance_id="isolated-instance",
            instance_name="isolated",
            fleet_id="isolated-fleet",
            peers=["https://isolated.invalid"],
            sync_token="isolated-token",
            session_secret="isolated-secret",
            pr_supervisor_authority_url="https://always-on-mini.invalid",
        )
        save_instance_config(self.isolated_data_dir, isolated_config)

        with patch.dict("os.environ", self._environment(), clear=True):
            reset_settings()
            settings = get_settings()

        self.assertEqual(settings.data_dir, self.isolated_data_dir)
        self.assertEqual(settings.instance_id, "isolated-instance")
        self.assertEqual(settings.fleet_id, "isolated-fleet")
        self.assertEqual(settings.peers, ["https://isolated.invalid"])
        self.assertEqual(settings.sync_token, "isolated-token")
        self.assertEqual(
            settings.pr_supervisor_authority_url,
            "https://always-on-mini.invalid",
        )
        self.assert_production_config_unchanged()

    def test_get_settings_does_not_mutate_default_config(self) -> None:
        with patch.dict("os.environ", self._environment(), clear=True):
            reset_settings()
            settings = get_settings()

        self.assertEqual(settings.data_dir, self.isolated_data_dir)
        self.assertNotEqual(settings.instance_id, "production-instance")
        self.assertNotEqual(settings.fleet_id, "production-fleet")
        self.assertEqual(settings.peers, [])
        self.assertEqual(settings.sync_token, "")
        self.assertEqual(
            settings.workspace_root,
            (
                self.isolated_data_dir.parent
                / f"{self.isolated_data_dir.name}-workspaces"
            ).resolve(),
        )
        self.assertFalse(settings.workspace_root.is_relative_to(settings.data_dir))
        self.assertFalse((self.isolated_data_dir / "config.json").exists())
        self.assert_production_config_unchanged()

    def test_workspace_root_must_be_outside_data_dir(self) -> None:
        with self.assertRaisesRegex(ValueError, "outside data_dir"):
            from pa.config import Settings

            Settings(
                data_dir=self.isolated_data_dir,
                workspace_root=self.isolated_data_dir / "agents",
            )
        with self.assertRaisesRegex(ValueError, "outside data_dir"):
            Settings(
                data_dir=self.isolated_data_dir,
                workspace_root=self.isolated_data_dir.parent,
            )

    def test_init_uses_pa_data_dir_without_touching_default_config(self) -> None:
        from pa.cli.main import app

        result = CliRunner().invoke(
            app,
            ["init", "--name", "development"],
            env=self._environment(),
        )

        self.assertEqual(result.exit_code, 0, result.output)
        config = json.loads((self.isolated_data_dir / "config.json").read_text())
        self.assertEqual(config["instance_name"], "development")
        self.assertEqual(config["data_dir"], str(self.isolated_data_dir))
        self.assert_production_config_unchanged()

    def test_serve_uses_isolated_config_without_touching_default_config(self) -> None:
        from pa.cli.main import app

        save_instance_config(
            self.isolated_data_dir,
            InstanceConfig(
                instance_name="development",
                host="127.0.0.2",
                session_secret="isolated-secret",
            ),
        )
        with (
            patch("pa.cli.main.uvicorn.Config") as config,
            patch("pa.server.shutdown.ShutdownAwareServer") as server,
        ):
            result = CliRunner().invoke(app, ["serve"], env=self._environment())

        self.assertEqual(result.exit_code, 0, result.output)
        config.assert_called_once()
        self.assertEqual(config.call_args.kwargs["host"], "127.0.0.2")
        self.assertEqual(config.call_args.kwargs["timeout_graceful_shutdown"], 10)
        server.return_value.run.assert_called_once()
        self.assert_production_config_unchanged()


if __name__ == "__main__":
    unittest.main()
