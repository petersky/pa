from __future__ import annotations

import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from mcp.server.mcpserver import MCPServer
from starlette.testclient import TestClient
from typer.testing import CliRunner

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.modules.backups import BackupsModule
from pa.backup.service import BackupError


class BackupSurfaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_mcp_exposes_complete_backup_and_restore_operations(self) -> None:
        mcp = MCPServer("backup-contract")
        BackupsModule().register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace()))
        tools = {tool.name: tool for tool in await mcp.list_tools()}
        self.assertTrue(
            {
                "backup_status",
                "backup_list",
                "backup_run",
                "backup_inspect",
                "backup_verify",
                "backup_delete",
                "backup_export",
                "backup_update_config",
                "backup_restore_initiate",
                "backup_restore_status",
            }.issubset(tools)
        )
        run_schema = tools["backup_run"].input_schema
        self.assertEqual(run_schema["required"], ["idempotency_key"])
        self.assertEqual(run_schema["properties"]["preserve_existing"]["type"], "boolean")
        self.assertFalse(run_schema["properties"]["preserve_existing"]["default"])
        listing = tools["backup_list"].input_schema
        self.assertEqual(listing["properties"]["offset"]["default"], 0)
        self.assertEqual(listing["properties"]["limit"]["default"], 50)
        self.assertEqual(listing.get("required", []), [])
        self.assertIn(
            "confirm_instance_id",
            tools["backup_restore_initiate"].input_schema["required"],
        )


class BackupHttpAndUiContractTests(unittest.TestCase):
    def test_manual_and_verify_use_dedicated_worker_and_protect_existing_archives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            app = Kernel.boot(settings=Settings(
                data_dir=root / "data", workspace_root=root / "workspaces",
                backup_destination_dir=destination, backup_enabled=False,
                backup_retention_count=1, agent_enabled=False,
            )).build_app()
            with TestClient(app) as client:
                client.get("/api/backups/status")
                headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
                service = app.state.ctx.require_service("backup_service")
                threads = []
                original = service.run_backup

                def run(**kwargs):
                    threads.append(threading.current_thread().name)
                    return original(**kwargs)

                original_verify = service.verify_backup

                def verify(*args, **kwargs):
                    threads.append(threading.current_thread().name)
                    return original_verify(*args, **kwargs)

                with patch.object(service, "run_backup", side_effect=run), patch.object(service, "verify_backup", side_effect=verify):
                    first = client.post("/api/backups", json={"idempotency_key": "first"}, headers=headers)
                    self.assertEqual(first.status_code, 201, first.text)
                    second = client.post("/api/backups", json={"idempotency_key": "second", "preserve_existing": True}, headers=headers)
                    self.assertEqual(second.status_code, 201, second.text)
                    self.assertEqual(second.json()["pruned_backup_ids"], [])
                    checked = client.post(f"/api/backups/{first.json()['backup_id']}/verify", headers=headers)
                    self.assertEqual(checked.status_code, 200, checked.text)
                self.assertEqual(len(service.list_backups()), 2)
                self.assertTrue(threads)
                self.assertTrue(all(name.startswith("pa-backup-io") for name in threads), threads)
                self.assertEqual(service.config.retention_count, 1)

    def test_backups_page_and_api_are_paginated_fast_and_do_not_open_archives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            for index in range(125):
                (destination / f"archive-{uuid4()}.pa-backup.tgz").write_bytes(b"unverified fixture")
            app = Kernel.boot(settings=Settings(
                data_dir=root / "data", workspace_root=root / "workspaces",
                backup_destination_dir=destination, backup_enabled=False,
                agent_enabled=False,
            )).build_app()
            with TestClient(app) as client:
                service = app.state.ctx.require_service("backup_service")
                self.enterContext(patch.object(service, "_read_manifest", side_effect=AssertionError("no archive scan")))
                with patch.object(service.backend, "list_archives", wraps=service.backend.list_archives) as inventory:
                    started = time.monotonic()
                    response = client.get("/settings?section=backups")
                    elapsed = time.monotonic() - started
                self.assertEqual(response.status_code, 200, response.text[:1000])
                self.assertEqual(inventory.call_count, 1)
                self.assertLess(elapsed, 2.0)
                self.assertEqual(response.text.count("data-backup-verify="), 50)
                self.assertIn("backup_offset=50", response.text)
                self.assertIn("unverified", response.text)
                self.assertIn("Select Verify to check integrity", response.text)
                self.assertIn("Last known good recovery point", response.text)
                page = client.get("/api/backups?offset=100&limit=50")
                self.assertEqual(page.status_code, 200)
                self.assertEqual(len(page.json()), 25)
                self.assertTrue(all("manifest" not in row for row in page.json()))
                self.assertEqual(client.get("/api/backups?limit=201").status_code, 422)
                self.assertEqual(client.get("/api/backups?offset=-1").status_code, 422)
                status = client.get("/api/backups/status")
                self.assertEqual(status.status_code, 200)
                self.assertEqual(status.json()["backup_count"], 125)

    def test_repeated_backup_failure_creates_one_durable_notification(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            app = Kernel.boot(settings=Settings(
                data_dir=root / "data", workspace_root=root / "workspaces",
                backup_destination_dir=destination, backup_enabled=False,
                backup_alert_after_failures=2, agent_enabled=False,
            )).build_app()
            with TestClient(app):
                service = app.state.ctx.require_service("backup_service")
                self.enterContext(patch.object(service, "_snapshot", side_effect=BackupError("fixture", "isolated failure")))
                service.run_backup(idempotency_key="one")
                service.run_backup(idempotency_key="two")
                service.run_backup(idempotency_key="three")
                notices = app.state.ctx.store.list_notifications(realm_id="default", notification_type="service_health")
                self.assertEqual(len(notices), 1)
                self.assertEqual(notices[0].coalesced_count, 1)
                self.assertEqual(notices[0].actions[0].href, "/settings?section=backups")
                self.assertTrue(service.status()["automatic_backups_paused"])

    def test_api_routes_include_status_crud_export_and_guarded_restore(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            kernel = Kernel.boot(
                settings=Settings(
                    data_dir=root / "data",
                    workspace_root=root / "workspaces",
                    backup_destination_dir=destination,
                    backup_run_on_startup=False,
                    agent_enabled=False,
                )
            )
            app = kernel.build_app()
            routes = {
                (method, route.path)
                for route in app.routes
                for method in getattr(route, "methods", set())
            }
        expected = {
            ("GET", "/api/backups/status"),
            ("GET", "/api/backups"),
            ("GET", "/api/backups/config"),
            ("PATCH", "/api/backups/config"),
            ("POST", "/api/backups"),
            ("GET", "/api/backups/{backup_id}"),
            ("POST", "/api/backups/{backup_id}/verify"),
            ("DELETE", "/api/backups/{backup_id}"),
            ("GET", "/api/backups/{backup_id}/download"),
            ("GET", "/api/backups/{backup_id}/export-info"),
            ("POST", "/api/backups/restores"),
            ("GET", "/api/backups/restores/{restore_id}"),
        }
        self.assertTrue(expected.issubset(routes), expected - routes)

    def test_schema_driven_configuration_updates_live_backup_service(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            data_dir = root / "data"
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            save_instance_config(
                data_dir,
                InstanceConfig(
                    instance_id="backup-config-http",
                    instance_name="backup-config-http",
                    data_dir=str(data_dir),
                    backup_destination_dir=str(destination),
                    backup_run_on_startup=False,
                    agent_enabled=False,
                    session_secret="backup-config-http-secret",
                ),
            )
            settings = Settings(
                instance_id="backup-config-http",
                instance_name="backup-config-http",
                data_dir=data_dir,
                workspace_root=root / "workspaces",
                backup_destination_dir=destination,
                backup_run_on_startup=False,
                agent_enabled=False,
                session_secret="backup-config-http-secret",
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                initial = client.get("/api/configuration")
                self.assertEqual(initial.status_code, 200, initial.text)
                csrf = client.cookies.get("pa_csrf")
                headers = {"X-CSRF-Token": csrf}
                invalid = client.post(
                    "/api/configuration/validate",
                    json={
                        "changes": {
                            "backup_destination_dir": str(data_dir),
                        }
                    },
                    headers=headers,
                )
                self.assertEqual(invalid.status_code, 422, invalid.text)
                changed = client.patch(
                    "/api/configuration",
                    json={
                        "changes": {"backup_interval_seconds": 900},
                        "expected_revision": initial.json()["revision"],
                        "idempotency_key": "backup-config-http-1",
                        "interface": "api",
                    },
                    headers=headers,
                )
                self.assertEqual(changed.status_code, 200, changed.text)
                self.assertEqual(changed.json()["changed"], ["backup_interval_seconds"])
                service = app.state.ctx.require_service("backup_service")
                self.assertEqual(service.config.interval_seconds, 900)
                self.assertIsNotNone(service.status()["next_scheduled_run"])

    def test_settings_ui_has_policy_history_verification_and_restore_warning(
        self,
    ) -> None:
        source = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "templates"
            / "pages"
            / "settings.html"
        ).read_text()
        for marker in (
            'data-section="backups"',
            "pa-backup-config-form",
            "pa-backup-run",
            "data-backup-verify",
            "data-backup-delete",
            "data-backup-restore",
            'name="io_limit_mib_per_second"',
            'name="concurrency"',
            'name="jitter_seconds"',
            "Restore requires maintenance",
            "Configured value sources",
            "Recent runs",
        ):
            self.assertIn(marker, source)

    def test_cli_offers_noninteractive_and_guarded_interactive_flows(self) -> None:
        from pa.cli.main import app

        runner = CliRunner()
        result = runner.invoke(app, ["backup", "--help"])
        self.assertEqual(result.exit_code, 0, result.output)
        for command in (
            "status",
            "run",
            "list",
            "inspect",
            "verify",
            "delete",
            "config",
            "export",
            "restore-initiate",
            "restore-status",
            "restore",
        ):
            self.assertIn(command, result.output)


if __name__ == "__main__":
    unittest.main()
