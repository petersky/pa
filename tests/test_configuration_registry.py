"""Coverage and behavior tests for the shared PA configuration registry."""

from __future__ import annotations

import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.testclient import TestClient
from typer.testing import CliRunner

from pa.config import Settings, get_settings, reset_settings
from pa.configuration.registry import (
    ALIASES,
    ENVIRONMENT_ONLY,
    REGISTRY,
    SETTINGS,
)
from pa.configuration.service import (
    apply_update,
    audit_events,
    configuration_snapshot,
    diff_update,
    human_coverage_report,
    schema_document,
    validate_update,
)
from pa.domain.config_edit import ConfigError, config_revision, list_field_specs
from pa.domain.instance_config import (
    InstanceConfig,
    load_instance_config,
    save_instance_config,
)


class RegistryCoverageTests(unittest.TestCase):
    def test_runtime_and_persisted_models_have_exact_registry_coverage(self) -> None:
        self.assertEqual(set(Settings.model_fields), set(SETTINGS))
        self.assertEqual(set(InstanceConfig.model_fields), set(SETTINGS))
        self.assertEqual(
            {definition.key for definition in list_field_specs()}, set(SETTINGS)
        )

    def test_registry_defaults_match_runtime_settings_defaults(self) -> None:
        dynamic = {"instance_id", "fleet_id", "session_secret", "data_dir"}
        for key, definition in SETTINGS.items():
            if key in dynamic:
                continue
            with self.subTest(key=key):
                value = Settings.model_fields[key].get_default(
                    call_default_factory=True
                )
                self.assertEqual(value, definition.default)

    def test_editable_settings_have_all_required_surface_coverage(self) -> None:
        required = {"web", "cli", "api", "mcp", "environment", "config_file"}
        for definition in SETTINGS.values():
            with self.subTest(key=definition.key):
                self.assertEqual(set(definition.surfaces), required)
                if definition.editable:
                    for surface in ("web", "cli", "api", "mcp", "config_file"):
                        self.assertTrue(definition.surfaces[surface].write)
                else:
                    self.assertTrue(definition.rationale)

    def test_environment_only_controls_are_hidden_or_readonly_with_rationale(
        self,
    ) -> None:
        for definition in ENVIRONMENT_ONLY.values():
            with self.subTest(key=definition.key):
                self.assertIn(definition.exposure, {"hidden", "read_only"})
                self.assertFalse(definition.editable)
                self.assertTrue(definition.rationale)
                self.assertEqual(definition.sources, ("environment",))

    def test_python_runtime_environment_references_are_inventoried(self) -> None:
        repository = Path(__file__).parents[1]
        roots = (repository / "src" / "pa", repository / "scripts")
        referenced: set[str] = set()
        for root in roots:
            for source in root.rglob("*"):
                if source.is_file() and source.suffix in {".py", ".sh"}:
                    referenced.update(
                        re.findall(r"\bPA_[A-Z][A-Z0-9_]+\b", source.read_text())
                    )
        generated_placeholders = {
            "PA_BIN",
            "PA_LOG_DIR",
            "PA_DOCUMENT_STATE__",
            "PA_LOCATOR__",
        }
        known = {
            name for definition in REGISTRY.values() for name in definition.environment
        }
        self.assertEqual(referenced - generated_placeholders - known, set())

    def test_documented_environment_references_are_known(self) -> None:
        root = Path(__file__).parents[1]
        referenced: set[str] = set()
        for source in [root / "README.md", *(root / "docs").rglob("*.md")]:
            referenced.update(re.findall(r"\bPA_[A-Z][A-Z0-9_]+\b", source.read_text()))
        known = {
            name for definition in REGISTRY.values() for name in definition.environment
        }
        self.assertEqual(referenced - {"PA_POST_TURN_"} - known, set())

    def test_aliases_have_canonical_migration_metadata(self) -> None:
        for alias, canonical in ALIASES.items():
            with self.subTest(alias=alias):
                definition = SETTINGS[canonical]
                self.assertIn(alias, definition.aliases)
                self.assertTrue(definition.migration)
        for definition in ENVIRONMENT_ONLY.values():
            if definition.deprecated:
                self.assertTrue(definition.replacement)
                self.assertTrue(definition.migration)

    def test_schema_and_human_report_are_complete(self) -> None:
        schema = schema_document()
        self.assertEqual(schema["schema_version"], 1)
        self.assertEqual(len(schema["settings"]), len(REGISTRY))
        report = human_coverage_report()
        self.assertIn(f"Registry settings: {len(REGISTRY)}", report)
        self.assertIn("| sync_token |", report)
        self.assertIn("| local_api_token |", report)

    def test_card_summary_provider_keys_are_editable_write_only_secrets(self) -> None:
        keys = (
            "card_summary_api_key",
            "card_summary_anthropic_api_key",
            "card_summary_minimax_api_key",
        )
        provider = SETTINGS["card_summary_provider"]
        self.assertEqual(provider.allowed, ("openai", "anthropic", "minimax"))
        self.assertTrue(provider.editable)
        for key in keys:
            definition = SETTINGS[key]
            with self.subTest(key=key):
                self.assertTrue(definition.secret)
                self.assertEqual(definition.exposure, "editable")
                self.assertTrue(definition.surfaces["web"].write)
                self.assertTrue(definition.surfaces["web"].read)
                self.assertEqual(definition.apply, "restart")
                self.assertNotEqual(definition.exposure, "hidden")


class ConfigurationApplicationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.config = InstanceConfig(
            instance_id="instance-test",
            instance_name="config-test",
            data_dir=str(self.data_dir),
            host="127.0.0.1",
            port=8099,
            zone="local",
            subscribed_realms=["default"],
            sync_token="old-secret",
        )
        save_instance_config(self.data_dir, self.config)
        self.settings = Settings(
            data_dir=self.data_dir,
            instance_id="instance-test",
            instance_name="config-test",
            host="127.0.0.1",
            port=8099,
            zone="local",
            subscribed_realms=["default"],
            sync_token="old-secret",
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_snapshot_distinguishes_configured_effective_default_and_source(
        self,
    ) -> None:
        snapshot = configuration_snapshot(
            self.settings, environment={"PA_LOG_LEVEL": "DEBUG"}
        )
        zone = next(item for item in snapshot["settings"] if item["key"] == "zone")
        logging = next(
            item for item in snapshot["settings"] if item["key"] == "log_level"
        )
        secret = next(
            item for item in snapshot["settings"] if item["key"] == "sync_token"
        )
        self.assertTrue(zone["configured"])
        self.assertEqual(zone["configured_value"], "local")
        self.assertEqual(zone["effective_value"], "local")
        self.assertEqual(zone["source"], "config_file")
        self.assertFalse(logging["configured"])
        self.assertEqual(logging["source"], "environment")
        self.assertEqual(secret["configured_value"], "<redacted>")
        self.assertEqual(secret["effective_value"], "<redacted>")

    def test_invalid_multi_setting_update_does_not_write(self) -> None:
        before = (self.data_dir / "config.json").read_bytes()
        with self.assertRaises(ConfigError):
            validate_update(
                self.data_dir,
                {"zone": "west", "port": 70000},
            )
        self.assertEqual((self.data_dir / "config.json").read_bytes(), before)

    def test_audit_failure_rolls_back_persisted_update(self) -> None:
        before = (self.data_dir / "config.json").read_bytes()
        base = load_instance_config(self.data_dir)
        with (
            patch(
                "pa.configuration.service._append_audit",
                side_effect=OSError("disk full"),
            ),
            self.assertRaisesRegex(ConfigError, "rolled back"),
        ):
            apply_update(
                self.settings,
                {"zone": "west"},
                [],
                expected_revision=config_revision(base),
                idempotency_key="audit-failure",
                principal_id="user:test",
                interface="api",
            )
        self.assertEqual((self.data_dir / "config.json").read_bytes(), before)

    def test_secret_update_is_redacted_audited_and_idempotent(self) -> None:
        revision = config_revision(load_instance_config(self.data_dir))
        first = apply_update(
            self.settings,
            {"sync_token": "new-secret-value"},
            [],
            expected_revision=revision,
            idempotency_key="test-secret-1",
            principal_id="user:test",
            interface="api",
        )
        self.assertIn("sync_token", first.changed)
        persisted = load_instance_config(self.data_dir)
        self.assertEqual(persisted.sync_token, "new-secret-value")
        events = audit_events(self.data_dir)
        encoded = json.dumps(events)
        self.assertNotIn("old-secret", encoded)
        self.assertNotIn("new-secret-value", encoded)
        self.assertEqual(events[-1]["secret_keys"], ["sync_token"])

        duplicate = apply_update(
            self.settings,
            {"sync_token": "new-secret-value"},
            [],
            expected_revision="stale-is-ignored-for-duplicate",
            idempotency_key="test-secret-1",
            principal_id="user:test",
            interface="api",
        )
        self.assertTrue(duplicate.duplicate)
        self.assertEqual(
            load_instance_config(self.data_dir).sync_token, "new-secret-value"
        )
        self.assertEqual(len(audit_events(self.data_dir)), 1)

        with self.assertRaisesRegex(ConfigError, "different configuration patch"):
            apply_update(
                self.settings,
                {"sync_token": "different-value"},
                [],
                expected_revision="stale-is-ignored-for-duplicate",
                idempotency_key="test-secret-1",
                principal_id="user:test",
                interface="api",
            )

    def test_card_summary_keys_are_redacted_in_snapshot_diff_and_audit(self) -> None:
        fake_keys = {
            "card_summary_api_key": "never-expose-openai",
            "card_summary_anthropic_api_key": "never-expose-anthropic",
            "card_summary_minimax_api_key": "never-expose-minimax",
        }
        diff = diff_update(self.data_dir, fake_keys, [])
        self.assertNotIn("never-expose", json.dumps(diff))
        self.assertTrue(diff["restart_required"])
        revision = config_revision(load_instance_config(self.data_dir))
        result = apply_update(
            self.settings,
            fake_keys,
            [],
            expected_revision=revision,
            idempotency_key="card-summary-keys",
            principal_id="user:test",
            interface="web",
        )
        self.assertEqual(set(result.changed), set(fake_keys))
        persisted = load_instance_config(self.data_dir)
        self.assertEqual(persisted.card_summary_api_key, "never-expose-openai")
        snapshot = configuration_snapshot(
            Settings(
                data_dir=self.data_dir,
                instance_id="instance-test",
                **fake_keys,
            )
        )
        encoded = json.dumps(snapshot) + json.dumps(audit_events(self.data_dir))
        for key, value in fake_keys.items():
            row = next(item for item in snapshot["settings"] if item["key"] == key)
            self.assertEqual(row["configured_value"], "<redacted>")
            self.assertEqual(row["effective_value"], "<redacted>")
            self.assertNotIn(value, encoded)

    def test_live_reload_and_restart_boundaries_are_explicit(self) -> None:
        base = load_instance_config(self.data_dir)
        result = apply_update(
            self.settings,
            {
                "agent_session_sweep_seconds": 45.0,
                "zone": "west",
                "host": "0.0.0.0",
            },
            [],
            expected_revision=config_revision(base),
            idempotency_key="apply-boundaries",
            principal_id="user:test",
            interface="web",
        )
        self.assertEqual(
            result.changed,
            {"agent_session_sweep_seconds", "zone", "host"},
        )
        self.assertEqual(result.restart, {"host"})
        self.assertEqual(result.reload - result.restart, {"zone"})
        self.assertEqual(self.settings.agent_session_sweep_seconds, 45.0)
        self.assertEqual(self.settings.zone, "local")
        self.assertEqual(self.settings.host, "127.0.0.1")
        snapshot = configuration_snapshot(self.settings)
        zone = next(item for item in snapshot["settings"] if item["key"] == "zone")
        host = next(item for item in snapshot["settings"] if item["key"] == "host")
        self.assertTrue(zone["pending_apply"])
        self.assertEqual(zone["source"], "runtime_override")
        self.assertTrue(host["pending_apply"])

    def test_secret_requires_explicit_clear(self) -> None:
        with self.assertRaisesRegex(ConfigError, "explicit clear"):
            diff_update(self.data_dir, {"sync_token": ""})
        diff = diff_update(self.data_dir, {}, ["sync_token"])
        self.assertEqual(diff["changes"][0]["after"], None)

    def test_optional_capacity_can_reset_to_inherited(self) -> None:
        base = load_instance_config(self.data_dir)
        first = apply_update(
            self.settings,
            {"dispatch_capacity": 8},
            [],
            expected_revision=config_revision(base),
            idempotency_key="capacity-set",
            principal_id="user:test",
            interface="api",
        )
        reset = apply_update(
            self.settings,
            {},
            ["dispatch_capacity"],
            expected_revision=config_revision(first.config),
            idempotency_key="capacity-clear",
            principal_id="user:test",
            interface="api",
        )
        self.assertIsNone(reset.config.dispatch_capacity)
        self.assertNotIn(
            "dispatch_capacity",
            json.loads((self.data_dir / "config.json").read_text()),
        )

    def test_unknown_keys_survive_scoped_updates(self) -> None:
        path = self.data_dir / "config.json"
        raw = json.loads(path.read_text())
        raw["future_provider_option"] = {"enabled": True}
        path.write_text(json.dumps(raw))
        base = load_instance_config(self.data_dir)
        apply_update(
            self.settings,
            {"zone": "west"},
            [],
            expected_revision=config_revision(base),
            idempotency_key="unknown-preservation",
            principal_id="user:test",
            interface="cli",
        )
        persisted = json.loads(path.read_text())
        self.assertEqual(persisted["future_provider_option"], {"enabled": True})
        snapshot = configuration_snapshot(self.settings)
        self.assertEqual(snapshot["unknown"][0]["key"], "future_provider_option")

    def test_deprecated_key_migrates_to_canonical_runtime_value(self) -> None:
        path = self.data_dir / "config.json"
        raw = json.loads(path.read_text())
        raw.pop("release_track", None)
        raw["update_channel"] = "beta"
        path.write_text(json.dumps(raw))
        loaded = load_instance_config(self.data_dir)
        self.assertEqual(loaded.release_track, "beta")
        snapshot = configuration_snapshot(self.settings)
        self.assertEqual(snapshot["deprecated"][0]["key"], "update_channel")
        self.assertEqual(snapshot["deprecated"][0]["canonical_key"], "release_track")

    def test_config_file_precedes_environment_for_persisted_keys(self) -> None:
        environment = {
            "HOME": str(self.data_dir / "home"),
            "PA_DATA_DIR": str(self.data_dir),
            "PA_ZONE": "environment-zone",
        }
        with patch.dict(os.environ, environment, clear=True):
            reset_settings()
            settings = get_settings()
        self.assertEqual(settings.zone, "local")
        reset_settings()


class ConfigurationCliContractTests(unittest.TestCase):
    def test_schema_json_is_stable_and_does_not_require_a_running_server(self) -> None:
        from pa.cli.main import app

        result = CliRunner().invoke(app, ["config", "schema", "--json"])
        self.assertEqual(result.exit_code, 0, result.output)
        value = json.loads(result.output)
        self.assertEqual(value["schema_version"], 1)
        self.assertEqual(len(value["settings"]), len(REGISTRY))

    def test_openapi_exposes_schema_validation_diff_update_and_audit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app = (
                __import__("pa.core.kernel", fromlist=["Kernel"])
                .Kernel.boot(settings=Settings(data_dir=Path(tmp)))
                .build_app()
            )
            paths = app.openapi()["paths"]
        self.assertIn("/api/configuration/schema", paths)
        self.assertIn("/api/configuration", paths)
        self.assertIn("/api/configuration/validate", paths)
        self.assertIn("/api/configuration/diff", paths)
        self.assertIn("/api/configuration/audit", paths)
        source = (
            Path(__file__).parents[1] / "src" / "pa" / "modules" / "instance.py"
        ).read_text()
        for tool in (
            "configuration_schema",
            "configuration_list",
            "configuration_validate",
            "configuration_diff",
            "configuration_update",
            "configuration_audit",
        ):
            self.assertIn(f"def {tool}(", source)

    def test_settings_shell_budget_and_repeated_navigation_defer_slow_sections(
        self,
    ) -> None:
        from pa.core.kernel import Kernel

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="settings-performance",
                instance_name="settings-performance",
                session_secret="settings-performance-secret",
                agent_enabled=False,
                peers=[],
            )
            app = Kernel.boot(settings=settings).build_app()
            with (
                patch(
                    "pa.configuration.service.configuration_snapshot"
                ) as configuration,
                patch("pa.status.info.build_status_snapshot") as status,
                TestClient(app) as client,
            ):
                for _ in range(20):
                    response = client.get("/settings")
                    self.assertEqual(response.status_code, 200)
                    self.assertLess(len(response.content), 150_000)
                    self.assertIn("shell;dur=", response.headers["server-timing"])
                    self.assertIn(
                        "settings-section;dur=", response.headers["server-timing"]
                    )
                    self.assertIn("template;dur=", response.headers["server-timing"])
                    self.assertEqual(response.headers["x-pa-settings-section"], "agent")
                configuration.assert_not_called()
                status.assert_not_called()

    def test_http_patch_validates_revision_is_idempotent_and_audited(self) -> None:
        from pa.core.kernel import Kernel

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            save_instance_config(
                data_dir,
                InstanceConfig(
                    instance_id="http-config",
                    instance_name="http-config",
                    data_dir=str(data_dir),
                    zone="local",
                    subscribed_realms=["default"],
                    session_secret="http-test-secret",
                ),
            )
            settings = Settings(
                data_dir=data_dir,
                instance_id="http-config",
                instance_name="http-config",
                zone="local",
                subscribed_realms=["default"],
                session_secret="http-test-secret",
                agent_enabled=False,
                peers=[],
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                page = client.get("/settings?section=configuration")
                self.assertEqual(page.status_code, 200, page.text)
                self.assertIn("Review staged changes", page.text)
                initial = client.get("/api/configuration")
                self.assertEqual(initial.status_code, 200, initial.text)
                remote = client.get("/api/configuration?target=older-peer")
                self.assertEqual(remote.status_code, 409, remote.text)
                self.assertEqual(
                    remote.json()["detail"]["code"],
                    "remote_configuration_unsupported",
                )
                csrf = client.cookies.get("pa_csrf")
                headers = {"X-CSRF-Token": csrf}
                payload = {
                    "changes": {"zone": "west"},
                    "expected_revision": initial.json()["revision"],
                    "idempotency_key": "http-config-1",
                    "interface": "api",
                }
                changed = client.patch(
                    "/api/configuration", json=payload, headers=headers
                )
                self.assertEqual(changed.status_code, 200, changed.text)
                self.assertEqual(changed.json()["changed"], ["zone"])
                duplicate = client.patch(
                    "/api/configuration", json=payload, headers=headers
                )
                self.assertEqual(duplicate.status_code, 200, duplicate.text)
                self.assertTrue(duplicate.json()["duplicate"])
                audit = client.get("/api/configuration/audit")
                self.assertEqual(audit.status_code, 200, audit.text)
                self.assertEqual(len(audit.json()["events"]), 1)

    def test_web_page_has_accessible_filter_review_and_secret_workflows(self) -> None:
        template = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "templates"
            / "pages"
            / "settings.html"
        ).read_text()
        self.assertIn('role="search" aria-label="Filter configuration"', template)
        self.assertIn("Review staged changes", template)
        self.assertIn("data-configuration-secret-replace", template)
        self.assertIn("data-configuration-clear", template)
        self.assertIn("configuration.unknown", template)
        self.assertIn("configuration.deprecated", template)


if __name__ == "__main__":
    unittest.main()
