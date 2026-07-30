from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from mcp.server.fastmcp import FastMCP
from starlette.testclient import TestClient
from typer.testing import CliRunner

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.modules.backups import BackupsModule


class BackupSurfaceTests(unittest.IsolatedAsyncioTestCase):
    async def test_mcp_exposes_complete_backup_and_restore_operations(self) -> None:
        mcp = FastMCP("backup-contract")
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
        self.assertIn("idempotency_key", tools["backup_run"].inputSchema["required"])
        self.assertIn(
            "confirm_instance_id",
            tools["backup_restore_initiate"].inputSchema["required"],
        )


class BackupHttpAndUiContractTests(unittest.TestCase):
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
