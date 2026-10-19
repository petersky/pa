from __future__ import annotations

import asyncio
import contextlib
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from starlette.testclient import TestClient
from typer.testing import CliRunner

from pa.backup.service import BackupError, BackupService
from pa.config import Settings
from pa.core.kernel import Kernel


@contextlib.contextmanager
def isolated_backup():
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        destination = root / "backups"
        destination.mkdir(mode=0o700)
        settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                            backup_destination_dir=destination, backup_enabled=False,
                            backup_retention_count=1, agent_enabled=False)
        app = Kernel.boot(settings=settings).build_app()
        with TestClient(app) as client:
            client.get("/api/backups/status")
            yield client, app.state.ctx.require_service("backup_service"), settings


def completed(client, key):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        response = client.get("/api/backups/runs", params={"idempotency_key": key})
        assert response.status_code == 200, response.text
        if response.json()["finished_at"]:
            return response.json()
        time.sleep(0.01)
    raise AssertionError("Isolated backup did not finish")


class BackupAdmissionTests(unittest.TestCase):
    def test_receipt_precedes_slow_capture_and_retries_do_not_queue_work(self):
        with isolated_backup() as (client, service, _settings):
            entered, release = threading.Event(), threading.Event()
            original = service._snapshot

            def slow_snapshot(*args):
                entered.set()
                if not release.wait(10):
                    raise AssertionError("fixture release missing")
                return original(*args)

            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            seed = client.post("/api/backups", json={"idempotency_key": "seed"}, headers=headers)
            self.assertEqual(seed.status_code, 201, seed.text)
            try:
                with patch.object(service, "_snapshot", side_effect=slow_snapshot):
                    first = client.post("/api/backups", json={"idempotency_key": "long", "wait": False,
                                                            "preserve_existing": True}, headers=headers)
                    self.assertEqual(first.status_code, 202, first.text)
                    self.assertIsNone(first.json()["finished_at"])
                    self.assertTrue(entered.wait(2))
                    durable = json.loads(service.state_path.read_text())
                    self.assertEqual(durable["idempotency"]["long"], first.json()["id"])
                    with patch.object(service, "_read_manifest", side_effect=AssertionError("no archive scan")):
                        status = client.get("/api/backups/runs", params={"idempotency_key": "long"})
                        self.assertEqual(status.status_code, 200, status.text)
                        self.assertEqual(status.json()["id"], first.json()["id"])
                        repeated = client.post("/api/backups", json={"idempotency_key": "long", "wait": False}, headers=headers)
                        self.assertEqual(repeated.status_code, 202, repeated.text)
                        self.assertEqual(repeated.json()["id"], first.json()["id"])
                        overlap = client.post("/api/backups", json={"idempotency_key": "other", "wait": False}, headers=headers)
                        self.assertEqual(overlap.status_code, 409, overlap.text)
                        self.assertEqual(client.get("/api/backups/runs", params={"idempotency_key": "other"}).status_code, 404)
                    self.assertEqual(len(service._state.runs), 2)
            finally:
                release.set()
            result = completed(client, "long")
            self.assertEqual(result["status"], "success", result)
            self.assertTrue(result["integrity_verified"])
            self.assertEqual(len(service._state.runs), 2)
            self.assertEqual(result["pruned_backup_ids"], [])
            self.assertEqual({item.backup_id for item in service.list_backups()},
                             {seed.json()["backup_id"], result["backup_id"]})

    def test_failure_remains_the_same_durable_operation_after_restart(self):
        with isolated_backup() as (client, service, settings):
            headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
            with patch.object(service, "_snapshot", side_effect=BackupError("fixture_failure", "isolated failure")):
                response = client.post("/api/backups", json={"idempotency_key": "failed", "wait": False}, headers=headers)
                self.assertEqual(response.status_code, 202, response.text)
                result = completed(client, "failed")
            self.assertEqual(result["status"], "failed")
            self.assertIn("fixture_failure", result["failure_reason"])
            again = client.post("/api/backups", json={"idempotency_key": "failed", "wait": False}, headers=headers)
            self.assertEqual(again.json()["id"], result["id"])
            self.assertEqual(len(service._state.runs), 1)
            reopened = BackupService(settings, None)
            try:
                self.assertEqual(reopened.find_run("failed").id, result["id"])
                self.assertEqual(reopened.find_run("failed").status, "failed")
            finally:
                asyncio.run(reopened.stop())

    def test_cli_wait_polls_original_key_and_no_wait_returns_receipt(self):
        from pa.cli.main import app

        runner = CliRunner()
        admitted = {"id": "original", "status": "running", "finished_at": None}
        final = {"id": "original", "status": "success", "finished_at": "2026-01-01T00:00:00Z"}
        with patch("pa.cli.backup._api", side_effect=[admitted, final]) as api, patch("pa.cli.backup.time.sleep"):
            result = runner.invoke(app, ["backup", "run", "--idempotency-key", "stable", "--preserve-existing"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(api.call_count, 2)
        self.assertEqual(api.call_args_list[0].kwargs["json_body"], {"idempotency_key": "stable", "preserve_existing": True, "wait": False})
        self.assertEqual(api.call_args_list[1].args, ("GET", "/api/backups/runs"))
        self.assertEqual(api.call_args_list[1].kwargs["params"], {"idempotency_key": "stable"})
        with patch("pa.cli.backup._api", return_value=admitted) as api:
            result = runner.invoke(app, ["backup", "run", "--idempotency-key", "stable", "--no-wait"])
        self.assertEqual(result.exit_code, 0, result.output)
        self.assertEqual(api.call_count, 1)


class BackupAdmissionCancellationTests(unittest.IsolatedAsyncioTestCase):
    async def test_disconnected_caller_does_not_cancel_the_persisted_capture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            destination = root / "backups"
            destination.mkdir(mode=0o700)
            settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                                backup_destination_dir=destination, backup_enabled=False,
                                agent_enabled=False)
            kernel = Kernel.boot(settings=settings)
            service = kernel.ctx.require_service("backup_service")
            entered, release = threading.Event(), threading.Event()
            original = service._save_state
            calls = 0

            def slow_persist():
                nonlocal calls
                calls += 1
                if calls == 1:
                    entered.set()
                    if not release.wait(5):
                        raise AssertionError("fixture release missing")
                original()

            try:
                with patch.object(service, "_save_state", side_effect=slow_persist):
                    caller = asyncio.create_task(service.start_backup("disconnected"))
                    self.assertTrue(await asyncio.to_thread(entered.wait, 2))
                    caller.cancel()
                    with self.assertRaises(asyncio.CancelledError):
                        await caller
                    self.assertTrue(service._maintenance_pending)
                    release.set()
                    for _ in range(500):
                        result = service.find_run("disconnected")
                        if result and result.finished_at:
                            break
                        await asyncio.sleep(0.01)
                    self.assertIsNotNone(result.finished_at)
                    self.assertEqual(result.status, "success", result.failure_reason)
                    repeated = await service.start_backup("disconnected")
                    self.assertEqual(repeated.id, result.id)
                    self.assertEqual(len(service._state.runs), 1)
            finally:
                release.set()
                await service.stop()
