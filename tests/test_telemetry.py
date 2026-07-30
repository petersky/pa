from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.telemetry.collector import (
    LinuxCollector,
    MacOSCollector,
    ProcessIdentity,
    ResourceCollector,
    SessionTarget,
    build_collector,
)
from pa.telemetry.models import Metric, MetricQuality, TelemetryQuery, TelemetrySample
from pa.telemetry.service import TelemetryService
from pa.telemetry.storage import TelemetryStorage


def sample(
    timestamp: datetime,
    *,
    scope_id: str = "instance-a",
    scope_type: str = "instance",
    restart_id: str = "restart-a",
    principal_id: str | None = None,
    value: float = 25,
) -> TelemetrySample:
    return TelemetrySample(
        timestamp=timestamp,
        instance_id="instance-a",
        instance_name="Alpha",
        scope_type=scope_type,
        scope_id=scope_id,
        restart_id=restart_id,
        principal_id=principal_id,
        metrics={
            "cpu.utilization": Metric(
                value=value,
                unit="percent",
                quality=MetricQuality.MEASURED,
                source="test",
            )
        },
    )


class TelemetryConfigTests(unittest.TestCase):
    def test_defaults_use_a_separate_bounded_database(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            self.assertEqual(
                settings.telemetry_database_path, Path(tmp).resolve() / "telemetry.db"
            )
            self.assertNotEqual(settings.telemetry_database_path, settings.db_path)
            self.assertGreater(settings.telemetry_max_database_bytes, 0)

    def test_unsafe_interval_and_retention_combinations_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaisesRegex(ValueError, "persistence_interval"):
                Settings(
                    data_dir=Path(tmp),
                    telemetry_live_interval_seconds=30,
                    telemetry_persistence_interval_seconds=10,
                )
            with self.assertRaisesRegex(ValueError, "rollup_retention"):
                Settings(
                    data_dir=Path(tmp),
                    telemetry_raw_retention_hours=100,
                    telemetry_rollup_retention_hours=10,
                )
            with self.assertRaisesRegex(ValueError, "metadata and sync authority"):
                Settings(
                    data_dir=Path(tmp),
                    telemetry_database_path=Path(tmp) / "pa.db",
                )
            with self.assertRaisesRegex(ValueError, "metadata and sync authority"):
                Settings(
                    data_dir=Path(tmp),
                    telemetry_database_path=Path(tmp) / "sync_refs.json",
                )
            with self.assertRaisesRegex(ValueError, "metadata and sync authority"):
                Settings(
                    data_dir=Path(tmp),
                    telemetry_database_path=Path(tmp) / "objects" / "telemetry.db",
                )


class CollectorTests(unittest.TestCase):
    def test_linux_and_macos_factories_share_normalized_units(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            kwargs = {
                "instance_id": "i",
                "instance_name": "I",
                "database_path": Path(tmp) / "telemetry.db",
            }
            with patch("pa.telemetry.collector.platform.system", return_value="Linux"):
                linux = build_collector(**kwargs)
            with patch("pa.telemetry.collector.platform.system", return_value="Darwin"):
                mac = build_collector(**kwargs)
            self.assertIsInstance(linux, LinuxCollector)
            self.assertIsInstance(mac, MacOSCollector)
            first = linux.collect(restart_id="r")
            time.sleep(0.01)
            second = linux.collect(restart_id="r")
            metrics = second[0].metrics
            self.assertEqual(metrics["cpu.utilization"].unit, "percent")
            self.assertEqual(metrics["memory.total"].unit, "bytes")
            self.assertEqual(metrics["disk.read_throughput"].unit, "bytes/second")
            self.assertEqual(metrics["network.ingress"].unit, "bytes/second")
            self.assertEqual(first[0].scope_type, "instance")

    def test_session_tree_has_explicit_quality_and_no_fabricated_network(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            collector = ResourceCollector(
                instance_id="i",
                instance_name="I",
                database_path=Path(tmp) / "telemetry.db",
            )
            target = SessionTarget(session_id="s", root_pid=os.getpid())
            collector.collect(restart_id="r", sessions=[target])
            time.sleep(0.01)
            result = collector.collect(restart_id="r", sessions=[target])[1]
            self.assertEqual(result.ownership, "verified_root_and_process_tree")
            self.assertGreaterEqual(result.metrics["session.processes"].value, 1)
            self.assertGreaterEqual(result.metrics["session.tasks"].value, 1)
            self.assertEqual(
                result.metrics["session.network_ingress"].quality,
                MetricQuality.UNSUPPORTED,
            )
            public = result.public_dict()
            self.assertNotIn("root_pid", public)
            serialized = str(public).lower()
            self.assertNotIn("command", serialized)
            self.assertNotIn("environment", serialized)

    def test_pid_reuse_and_exit_do_not_preserve_unverified_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            collector = ResourceCollector(
                instance_id="i",
                instance_name="I",
                database_path=Path(tmp) / "telemetry.db",
            )
            collector._owned["s"] = {
                ProcessIdentity(os.getpid(), time.time() - 100_000)
            }
            result = collector.collect(
                restart_id="r",
                sessions=[SessionTarget(session_id="s", root_pid=999_999_999)],
            )[1]
            self.assertEqual(result.ownership, "unavailable")
            self.assertIsNone(result.metrics["session.processes"].value)
            self.assertEqual(
                result.metrics["session.processes"].quality,
                MetricQuality.UNAVAILABLE,
            )

    def test_same_pid_with_changed_creation_time_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            collector = ResourceCollector(
                instance_id="i",
                instance_name="I",
                database_path=Path(tmp) / "telemetry.db",
            )
            collector._roots["s"] = ProcessIdentity(os.getpid(), time.time() - 100_000)
            result = collector.collect(
                restart_id="r",
                sessions=[SessionTarget(session_id="s", root_pid=os.getpid())],
            )[1]
            self.assertEqual(result.ownership, "unavailable")
            self.assertIsNone(result.metrics["session.memory_rss"].value)

    def test_exact_descendant_evidence_survives_root_exit_without_pid_guessing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            collector = ResourceCollector(
                instance_id="i",
                instance_name="I",
                database_path=Path(tmp) / "telemetry.db",
            )
            live = SessionTarget(session_id="s", root_pid=os.getpid())
            collector.collect(restart_id="r", sessions=[live])
            orphan = SessionTarget(session_id="s", root_pid=999_999_999)
            result = collector.collect(restart_id="r", sessions=[orphan])[1]
            self.assertEqual(result.ownership, "root_exited_retained_exact_descendants")
            self.assertGreaterEqual(result.metrics["session.processes"].value, 1)


class StorageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.path = Path(self.tmp.name) / "telemetry.db"
        self.storage = TelemetryStorage(self.path)
        self.now = datetime.now(UTC)

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_schema_is_isolated_from_metadata_and_sync_tables(self) -> None:
        self.assertNotEqual(self.path.name, "pa.db")
        with sqlite3.connect(self.path) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                )
            }
        self.assertIn("samples", tables)
        self.assertIn("rollup_metrics", tables)
        self.assertNotIn("events", tables)
        self.assertNotIn("sync_refs", tables)
        self.assertEqual(self.storage.status()["sync_authority"], "excluded")

    def test_corrupt_database_is_quarantined_without_stopping_collection(self) -> None:
        path = self.path.parent / "corrupt.db"
        path.write_bytes(b"not sqlite")
        recovered = TelemetryStorage(path)
        self.assertTrue(path.exists())
        self.assertTrue(list(path.parent.glob("corrupt.db.corrupt-*")))
        self.assertIn("quarantined", recovered.last_error)
        recovered.insert_samples([sample(self.now)])

    def test_samples_survive_an_independent_storage_restart(self) -> None:
        self.storage.insert_samples([sample(self.now)])
        reopened = TelemetryStorage(self.path)
        result = reopened.query(
            TelemetryQuery(
                start=self.now - timedelta(seconds=1),
                end=self.now + timedelta(seconds=1),
            )
        )
        self.assertEqual(len(result["series"]), 1)
        self.assertEqual(result["series"][0]["points"][0]["avg"], 25)

    def test_query_reports_restart_and_missing_quality(self) -> None:
        same_bucket = self.now.replace(second=1, microsecond=0)
        one = sample(same_bucket, restart_id="one", value=10)
        two = sample(same_bucket + timedelta(seconds=2), restart_id="two", value=30)
        two.metrics["network.ingress"] = Metric(
            value=None,
            unit="bytes/second",
            quality=MetricQuality.UNAVAILABLE,
            source="test",
            detail="gap",
        )
        self.storage.insert_samples([one, two])
        result = self.storage.query(
            TelemetryQuery(
                start=same_bucket - timedelta(minutes=1),
                end=same_bucket + timedelta(minutes=1),
                bucket_seconds=60,
            )
        )
        cpu = next(
            item for item in result["series"] if item["metric"] == "cpu.utilization"
        )
        self.assertEqual(cpu["points"][0]["avg"], 20)
        self.assertTrue(cpu["points"][0]["restart"])
        network = next(
            item for item in result["series"] if item["metric"] == "network.ingress"
        )
        self.assertEqual(network["points"][0]["value_count"], 0)
        self.assertEqual(network["points"][0]["quality"], "unavailable")

    def test_age_pruning_rolls_up_before_raw_deletion(self) -> None:
        old = self.now - timedelta(hours=2)
        self.storage.insert_samples(
            [sample(old + timedelta(seconds=index), value=index) for index in range(5)]
        )
        result = self.storage.prune(
            raw_retention_hours=1,
            rollup_retention_hours=24,
            max_database_bytes=16 * 1024 * 1024,
            now=self.now,
        )
        self.assertEqual(result["raw_samples_pruned"], 5)
        status = self.storage.status()
        self.assertEqual(status["raw_samples"], 0)
        self.assertGreater(status["rollup_rows"], 0)
        query = self.storage.query(
            TelemetryQuery(
                start=old - timedelta(minutes=1),
                end=self.now,
                metrics=["cpu.utilization"],
                bucket_seconds=60,
            )
        )
        self.assertEqual(query["bucket_seconds"], 300)
        self.assertEqual(query["series"][0]["points"][0]["value_count"], 5)

    def test_size_pruning_is_oldest_first_and_bounded(self) -> None:
        self.storage.insert_samples(
            [
                sample(self.now - timedelta(minutes=index), value=index)
                for index in range(20, 0, -1)
            ]
        )
        result = self.storage.prune(
            raw_retention_hours=100,
            rollup_retention_hours=100,
            max_database_bytes=1,
            now=self.now,
        )
        self.assertGreater(result["size_pressure_batches"], 0)
        self.assertEqual(self.storage.status()["raw_samples"], 0)

    def test_principal_filter_hides_other_sessions_but_keeps_instances(self) -> None:
        self.storage.insert_samples(
            [
                sample(self.now, scope_id="instance-a"),
                sample(
                    self.now,
                    scope_id="owned",
                    scope_type="session",
                    principal_id="user:one",
                ),
                sample(
                    self.now,
                    scope_id="hidden",
                    scope_type="session",
                    principal_id="user:two",
                ),
            ]
        )
        result = self.storage.query(
            TelemetryQuery(
                start=self.now - timedelta(seconds=1),
                end=self.now + timedelta(seconds=1),
                visible_principal_id="user:one",
            )
        )
        self.assertEqual(
            {item["scope_id"] for item in result["series"]},
            {"instance-a", "owned"},
        )

    def test_many_scopes_and_long_history_remain_server_aggregated(self) -> None:
        samples = []
        for session in range(40):
            for point in range(60):
                samples.append(
                    sample(
                        self.now - timedelta(minutes=point),
                        scope_id=f"session-{session}",
                        scope_type="session",
                        principal_id="user:one",
                        value=(session + point) % 100,
                    )
                )
        self.storage.insert_samples(samples)
        started = time.monotonic()
        result = self.storage.query(
            TelemetryQuery(
                start=self.now - timedelta(hours=24),
                end=self.now + timedelta(seconds=1),
                scope_type="session",
                bucket_seconds=300,
                visible_principal_id="user:one",
            )
        )
        self.assertLess(time.monotonic() - started, 3)
        self.assertEqual(len(result["series"]), 40)
        self.assertLessEqual(
            max(len(series["points"]) for series in result["series"]), 13
        )


class FailingStorage:
    def insert_samples(self, _samples) -> int:
        raise OSError("disk unavailable")

    def status(self) -> dict:
        return {}


class SamplerTests(unittest.IsolatedAsyncioTestCase):
    async def test_runtime_disable_can_be_reenabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), telemetry_enabled=False)
            service = TelemetryService(
                settings,
                storage=TelemetryStorage(Path(tmp) / "telemetry.db"),
            )
            await service.stop()
            settings.telemetry_enabled = True
            await service.start()
            self.assertIsNotNone(service._sample_task)
            await service.stop()
            await service.start()
            self.assertIsNotNone(service._sample_task)
            await service.stop(close=True)

    async def test_backpressure_drops_without_blocking(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                telemetry_enabled=False,
            )
            service = TelemetryService(
                settings,
                storage=TelemetryStorage(Path(tmp) / "telemetry.db"),
                queue_size=1,
            )
            batch = [sample(datetime.now(UTC))]
            service._enqueue(batch)
            service._enqueue(batch)
            self.assertEqual(service.dropped_samples, 1)
            await service.stop(close=True)

    async def test_storage_failure_is_counted_and_writer_survives(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), telemetry_enabled=False)
            service = TelemetryService(settings, storage=FailingStorage(), queue_size=2)
            service._writer_task = asyncio.create_task(service._writer_loop())
            service._enqueue([sample(datetime.now(UTC))])
            await asyncio.wait_for(service._queue.join(), timeout=2)
            self.assertEqual(service.storage_failures, 1)
            self.assertEqual(service.dropped_samples, 1)
            await service.stop(close=True)


class TelemetryUITests(unittest.TestCase):
    def test_reports_and_live_surfaces_expose_gaps_and_quality(self) -> None:
        root = Path(__file__).parents[1]
        template = (root / "src/pa/server/templates/pages/reports.html").read_text()
        script = (root / "src/pa/server/static/js/telemetry.js").read_text()
        shell = (root / "src/pa/server/templates/shell.html").read_text()
        chat = (
            root / "src/pa/server/templates/partials/agent/chat-widget.html"
        ).read_text()
        self.assertIn("Fleet instances", template)
        self.assertIn("data-chart-cursor", template)
        self.assertIn("ArrowLeft", script)
        self.assertIn("sampling gap", script)
        self.assertIn("unsupported", script)
        self.assertIn("js/telemetry.js", shell)
        self.assertIn("data-session-telemetry", chat)


class TelemetryAPITests(unittest.TestCase):
    def test_endpoints_enforce_principal_visibility_and_bounded_ranges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                auth_required=True,
                agent_enabled=False,
                telemetry_enabled=False,
            )
            kernel = Kernel.boot(settings=settings)
            app = kernel.build_app()
            users: UserDirectory = kernel.ctx.require_service("users")
            first = users.ensure_default_user()
            second = users.create_user("second", "secret")
            now = datetime.now(UTC)
            storage: TelemetryStorage = kernel.ctx.require_service("telemetry_storage")
            storage.insert_samples(
                [
                    sample(now),
                    sample(
                        now,
                        scope_type="session",
                        scope_id="first-session",
                        principal_id=f"user:{first.id}",
                    ),
                    sample(
                        now,
                        scope_type="session",
                        scope_id="second-session",
                        principal_id=f"user:{second.id}",
                    ),
                ]
            )
            headers = {"Authorization": f"Bearer {second.cli_token}"}
            with TestClient(app) as client:
                response = client.post(
                    "/api/telemetry/query",
                    headers=headers,
                    json={"range": "1h"},
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertEqual(
                    {item["scope_id"] for item in response.json()["series"]},
                    {"instance-a", "second-session"},
                )
                admin = client.post(
                    "/api/telemetry/query",
                    headers={"Authorization": f"Bearer {first.cli_token}"},
                    json={"range": "1h"},
                )
                self.assertEqual(admin.status_code, 200, admin.text)
                self.assertEqual(
                    {item["scope_id"] for item in admin.json()["series"]},
                    {"instance-a", "first-session", "second-session"},
                )
                too_long = client.post(
                    "/api/telemetry/query",
                    headers=headers,
                    json={
                        "start": (now - timedelta(days=32)).isoformat(),
                        "end": now.isoformat(),
                    },
                )
                self.assertEqual(too_long.status_code, 422)
                export = client.get("/api/telemetry/export?range=15m", headers=headers)
                self.assertEqual(export.status_code, 200)
                serialized = export.text.lower()
                self.assertNotIn("command", serialized)
                self.assertNotIn("fleet-secret", serialized)

    def test_instance_bearer_cannot_query_session_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                auth_required=True,
                sync_token="fleet-secret",
                agent_enabled=False,
                telemetry_enabled=False,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                response = client.post(
                    "/api/telemetry/query",
                    headers={"Authorization": "Bearer fleet-secret"},
                    json={"range": "1h", "scope_type": "session"},
                )
                self.assertEqual(response.status_code, 200, response.text)
                self.assertTrue(
                    all(
                        item["scope_type"] == "instance"
                        for item in response.json()["series"]
                    )
                )
