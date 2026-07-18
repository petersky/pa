"""Tests for validated config.json editing."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from typer.testing import CliRunner

from pa.domain.config_edit import (
    ConfigError,
    add_config_value,
    remove_config_value,
    set_config_value,
    unset_config_value,
)
from pa.domain.instance_config import InstanceConfig, save_instance_config


class ConfigEditTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        save_instance_config(
            self.data_dir,
            InstanceConfig(
                instance_name="test",
                data_dir=str(self.data_dir),
                host="127.0.0.1",
                subscribed_realms=["personal"],
                peers=[],
            ),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_set_host_all_interfaces(self) -> None:
        result = set_config_value(self.data_dir, "host", "0.0.0.0")
        self.assertEqual(result.after, "0.0.0.0")
        self.assertTrue(result.restart_required)
        data = json.loads((self.data_dir / "config.json").read_text())
        self.assertEqual(data["host"], "0.0.0.0")

    def test_set_host_rejects_url(self) -> None:
        with self.assertRaises(ConfigError):
            set_config_value(self.data_dir, "host", "http://0.0.0.0")

    def test_set_instance_url_rejects_loopback(self) -> None:
        with self.assertRaises(ConfigError):
            set_config_value(self.data_dir, "instance_url", "http://127.0.0.1:8080")

    def test_set_instance_url_ok(self) -> None:
        result = set_config_value(self.data_dir, "instance_url", "http://mini:8080/")
        self.assertEqual(result.after, "http://mini:8080")

    def test_set_pr_supervisor_authority_validates_url(self) -> None:
        result = set_config_value(
            self.data_dir,
            "pr_supervisor_authority_url",
            "http://always-on-mini:8080/",
        )
        self.assertEqual(result.after, "http://always-on-mini:8080")
        with self.assertRaises(ConfigError):
            set_config_value(
                self.data_dir, "pr_supervisor_authority_url", "always-on-mini"
            )

    def test_set_release_track(self) -> None:
        result = set_config_value(self.data_dir, "release_track", "beta")
        self.assertEqual(result.after, "beta")
        with self.assertRaises(ConfigError):
            set_config_value(self.data_dir, "release_track", "nightly")

    def test_set_bool(self) -> None:
        result = set_config_value(self.data_dir, "relay_enabled", "true")
        self.assertTrue(result.after)

    def test_add_remove_peers(self) -> None:
        add_config_value(self.data_dir, "peers", "http://macbook:8080")
        result = add_config_value(self.data_dir, "peers", "http://studio:8080")
        self.assertEqual(
            result.after,
            ["http://macbook:8080", "http://studio:8080"],
        )
        with self.assertRaises(ConfigError):
            add_config_value(self.data_dir, "peers", "http://macbook:8080")
        removed = remove_config_value(self.data_dir, "peers", "http://macbook:8080")
        self.assertEqual(removed.after, ["http://studio:8080"])

    def test_add_rejects_non_list_key(self) -> None:
        with self.assertRaises(ConfigError):
            add_config_value(self.data_dir, "host", "0.0.0.0")

    def test_readonly_instance_id(self) -> None:
        with self.assertRaises(ConfigError):
            set_config_value(self.data_dir, "instance_id", "nope")

    def test_unset_host(self) -> None:
        result = unset_config_value(self.data_dir, "host")
        self.assertEqual(result.after, "")

    def test_set_list_via_json(self) -> None:
        result = set_config_value(
            self.data_dir,
            "capabilities",
            '["agent", "relay"]',
        )
        self.assertEqual(result.after, ["agent", "relay"])

    def test_subscribed_realms_cannot_be_empty(self) -> None:
        with self.assertRaises(ConfigError):
            set_config_value(self.data_dir, "subscribed_realms", "")


class ConfigCliTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        save_instance_config(
            self.data_dir,
            InstanceConfig(
                instance_name="cli-test",
                data_dir=str(self.data_dir),
                host="127.0.0.1",
                subscribed_realms=["default"],
            ),
        )
        self.runner = CliRunner()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cli_set_and_get(self) -> None:
        from pa.cli.main import app

        with patch("pa.cli.config_cmd._data_dir", return_value=self.data_dir):
            with patch(
                "pa.domain.config_edit.refresh_after_mutate", return_value=False
            ):
                set_result = self.runner.invoke(
                    app, ["config", "set", "host", "0.0.0.0"]
                )
                self.assertEqual(set_result.exit_code, 0, set_result.output)
                self.assertIn("0.0.0.0", set_result.output)
                get_result = self.runner.invoke(app, ["config", "get", "host"])
                self.assertEqual(get_result.exit_code, 0, get_result.output)
                self.assertEqual(get_result.output.strip(), "0.0.0.0")

    def test_cli_rejects_invalid(self) -> None:
        from pa.cli.main import app

        with patch("pa.cli.config_cmd._data_dir", return_value=self.data_dir):
            result = self.runner.invoke(
                app, ["config", "set", "instance_url", "http://localhost:8080"]
            )
            self.assertNotEqual(result.exit_code, 0)
            self.assertIn("localhost", result.output)

    def test_cli_show(self) -> None:
        from pa.cli.main import app

        with patch("pa.cli.config_cmd._data_dir", return_value=self.data_dir):
            result = self.runner.invoke(app, ["config", "show", "--json"])
            self.assertEqual(result.exit_code, 0, result.output)
            data = json.loads(result.output)
            self.assertEqual(data["instance_name"], "cli-test")


if __name__ == "__main__":
    unittest.main()
