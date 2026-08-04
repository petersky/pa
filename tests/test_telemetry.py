from __future__ import annotations

import asyncio
import os
import re
import shutil
import sqlite3
import subprocess
import tempfile
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from fastapi.testclient import TestClient

from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.config_edit import ConfigError, validate_config_changes
from pa.domain.instance_config import InstanceConfig
from pa.modules.telemetry import QueryBody, fleet_query
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

    def test_schema_driven_configuration_enforces_telemetry_invariants(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = InstanceConfig(data_dir=tmp)
            with self.assertRaisesRegex(ConfigError, "persistence_interval"):
                validate_config_changes(
                    base,
                    {
                        "telemetry_live_interval_seconds": 30.0,
                        "telemetry_persistence_interval_seconds": 10.0,
                    },
                )
            with self.assertRaisesRegex(ConfigError, "metadata and sync authority"):
                validate_config_changes(
                    base,
                    {"telemetry_database_path": str(Path(tmp) / "sync_refs.json")},
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

    def test_query_preserves_zero_and_emits_typed_gap_ranges(self) -> None:
        start = datetime(2026, 1, 1, tzinfo=UTC)
        observed = sample(start + timedelta(minutes=1), value=0)
        unavailable_sample = sample(start + timedelta(minutes=3), value=5)
        unavailable_sample.metrics["cpu.utilization"] = Metric(
            value=None,
            unit="percent",
            quality=MetricQuality.UNAVAILABLE,
            source="test",
        )
        unsupported_sample = sample(start + timedelta(minutes=4), value=5)
        unsupported_sample.metrics["cpu.utilization"] = Metric(
            value=None,
            unit="percent",
            quality=MetricQuality.UNSUPPORTED,
            source="test",
        )
        self.storage.insert_samples([observed, unavailable_sample, unsupported_sample])

        result = self.storage.query(
            TelemetryQuery(
                start=start,
                end=start + timedelta(minutes=6),
                bucket_seconds=60,
            )
        )
        series = result["series"][0]
        self.assertEqual(series["points"][0]["avg"], 0)
        self.assertEqual(series["points"][0]["observation"], "genuine_zero")
        self.assertIsNone(series["points"][0]["missing_reason"])
        self.assertEqual(
            [point["observation"] for point in series["points"][1:]],
            ["missing", "missing"],
        )
        reasons = [gap["reason"] for gap in series["gaps"]]
        self.assertEqual(
            reasons,
            [
                "no_sample",
                "no_sample",
                "temporarily_unavailable",
                "unsupported",
                "stale",
            ],
        )
        self.assertTrue(all(gap["start"] < gap["end"] for gap in series["gaps"]))
        exported = self.storage.export(
            TelemetryQuery(
                start=start,
                end=start + timedelta(minutes=6),
                bucket_seconds=60,
            )
        )
        self.assertEqual(exported["series"][0]["points"], series["points"])
        self.assertEqual(exported["series"][0]["gaps"], series["gaps"])

    def test_all_unavailable_series_never_has_an_observed_value(self) -> None:
        timestamp = datetime(2026, 1, 1, tzinfo=UTC)
        item = sample(timestamp)
        item.metrics["cpu.utilization"] = Metric(
            value=None,
            unit="percent",
            quality=MetricQuality.UNAVAILABLE,
            source="test",
        )
        self.storage.insert_samples([item])
        result = self.storage.query(
            TelemetryQuery(
                start=timestamp,
                end=timestamp + timedelta(minutes=1),
                bucket_seconds=60,
            )
        )
        point = result["series"][0]["points"][0]
        self.assertIsNone(point["avg"])
        self.assertEqual(point["value_count"], 0)
        self.assertEqual(point["observation"], "missing")
        self.assertEqual(point["missing_reason"], "temporarily_unavailable")
        self.assertEqual(
            result["series"][0]["gaps"][0]["reason"],
            "temporarily_unavailable",
        )

    def test_mixed_quality_preserves_measured_value_raw_and_rolled(self) -> None:
        bucket_start = (self.now - timedelta(days=10)).replace(second=0, microsecond=0)
        bucket_start -= timedelta(minutes=bucket_start.minute % 5)
        measured = sample(bucket_start + timedelta(seconds=10), value=5)
        unavailable = sample(bucket_start + timedelta(seconds=20), value=99)
        unavailable.metrics["cpu.utilization"] = Metric(
            value=None,
            unit="percent",
            quality=MetricQuality.UNAVAILABLE,
            source="test",
        )
        self.storage.insert_samples([measured, unavailable])
        query = TelemetryQuery(
            start=bucket_start,
            end=bucket_start + timedelta(minutes=5),
            bucket_seconds=300,
        )

        raw = self.storage.query(query)
        self.storage.prune(
            raw_retention_hours=24,
            rollup_retention_hours=24 * 30,
            max_database_bytes=16 * 1024 * 1024,
            now=self.now,
        )
        rolled = self.storage.query(query)

        for result in (raw, rolled):
            point = result["series"][0]["points"][0]
            self.assertEqual(point["avg"], 5)
            self.assertEqual(point["value_count"], 1)
            self.assertEqual(point["sample_count"], 2)
            self.assertEqual(point["missing_count"], 1)
            self.assertEqual(point["quality"], "unavailable")
            self.assertEqual(point["observation"], "observed")
            self.assertIsNone(point["missing_reason"])
            self.assertEqual(point["partial_reason"], "temporarily_unavailable")
            self.assertIn(
                {
                    "reason": "temporarily_unavailable",
                    "start": bucket_start.isoformat(),
                    "end": (bucket_start + timedelta(minutes=5)).isoformat(),
                    "partial": True,
                },
                result["series"][0]["gaps"],
            )

    def test_rollups_keep_same_metric_with_different_units_separate(self) -> None:
        bucket_start = (self.now - timedelta(days=10)).replace(second=0, microsecond=0)
        bucket_start -= timedelta(minutes=bucket_start.minute % 5)

        def drift_sample(offset: int, value: float, unit: str) -> TelemetrySample:
            item = sample(bucket_start + timedelta(seconds=offset))
            item.metrics = {
                "drift.metric": Metric(
                    value=value,
                    unit=unit,
                    quality=MetricQuality.MEASURED,
                    source="test",
                )
            }
            return item

        self.storage.insert_samples(
            [drift_sample(10, 1, "percent"), drift_sample(20, 1000, "bytes")]
        )
        query = TelemetryQuery(
            start=bucket_start,
            end=bucket_start + timedelta(minutes=5),
            metrics=["drift.metric"],
            bucket_seconds=300,
        )
        raw = self.storage.query(query)
        self.storage.prune(
            raw_retention_hours=24,
            rollup_retention_hours=24 * 30,
            max_database_bytes=16 * 1024 * 1024,
            now=self.now,
        )
        rolled = self.storage.query(query)

        def values(result: dict) -> dict[str, float]:
            return {
                series["unit"]: series["points"][0]["avg"]
                for series in result["series"]
            }

        self.assertEqual(values(raw), {"percent": 1, "bytes": 1000})
        self.assertEqual(values(rolled), values(raw))
        with sqlite3.connect(self.path) as conn:
            primary_key = [
                row[1]
                for row in conn.execute("PRAGMA table_info(rollup_metrics)")
                if row[5]
            ]
        self.assertEqual(primary_key[-2:], ["metric", "unit"])

    def test_unitless_rollup_schema_migrates_without_future_unit_collision(
        self,
    ) -> None:
        legacy_path = self.path.parent / "legacy-rollup.db"
        bucket_start = (self.now - timedelta(days=10)).replace(second=0, microsecond=0)
        bucket_start -= timedelta(minutes=bucket_start.minute % 5)
        with sqlite3.connect(legacy_path) as conn:
            conn.executescript(
                """
                CREATE TABLE rollup_metrics (
                    bucket_start REAL NOT NULL, bucket_seconds INTEGER NOT NULL,
                    instance_id TEXT NOT NULL, instance_name TEXT NOT NULL,
                    scope_type TEXT NOT NULL, scope_id TEXT NOT NULL,
                    provider_id TEXT NOT NULL DEFAULT '',
                    card_id TEXT NOT NULL DEFAULT '',
                    project_id TEXT NOT NULL DEFAULT '',
                    realm_id TEXT NOT NULL DEFAULT '',
                    principal_id TEXT NOT NULL DEFAULT '',
                    metric TEXT NOT NULL, unit TEXT NOT NULL,
                    quality_rank INTEGER NOT NULL, value_sum REAL,
                    value_min REAL, value_max REAL, value_last REAL,
                    value_count INTEGER NOT NULL, sample_count INTEGER NOT NULL,
                    restart_ids TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY(
                        bucket_start,bucket_seconds,instance_id,scope_type,scope_id,
                        provider_id,card_id,project_id,realm_id,principal_id,metric
                    )
                );
                """
            )
            conn.execute(
                """
                INSERT INTO rollup_metrics VALUES(
                    ?,300,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
                )
                """,
                (
                    bucket_start.timestamp(),
                    "instance-a",
                    "Alpha",
                    "instance",
                    "instance-a",
                    "",
                    "",
                    "",
                    "",
                    "",
                    "drift.metric",
                    "percent",
                    0,
                    1,
                    1,
                    1,
                    1,
                    1,
                    1,
                    "restart-a",
                ),
            )

        migrated = TelemetryStorage(legacy_path)
        with sqlite3.connect(legacy_path) as conn:
            primary_key = [
                row[1]
                for row in conn.execute("PRAGMA table_info(rollup_metrics)")
                if row[5]
            ]
            schema_version = conn.execute(
                "SELECT value FROM telemetry_meta WHERE key='schema_version'"
            ).fetchone()[0]
        self.assertEqual(primary_key[-2:], ["metric", "unit"])
        self.assertEqual(schema_version, "3")
        result = migrated.query(
            TelemetryQuery(
                start=bucket_start,
                end=bucket_start + timedelta(minutes=5),
                metrics=["drift.metric"],
                bucket_seconds=300,
            )
        )
        self.assertEqual(result["series"][0]["unit"], "percent")
        self.assertEqual(result["series"][0]["points"][0]["avg"], 1)

    def test_partial_first_and_last_buckets_stay_inside_requested_domain(self) -> None:
        bucket_start = (self.now - timedelta(days=10)).replace(second=0, microsecond=0)
        bucket_start -= timedelta(minutes=bucket_start.minute % 5)
        self.storage.insert_samples(
            [
                sample(bucket_start + timedelta(minutes=1), value=1),
                sample(bucket_start + timedelta(minutes=5, seconds=10), value=2),
            ]
        )
        start = bucket_start + timedelta(seconds=30)
        end = bucket_start + timedelta(minutes=5, seconds=30)
        query = TelemetryQuery(start=start, end=end, bucket_seconds=300)
        raw = self.storage.query(query)
        self.storage.prune(
            raw_retention_hours=24,
            rollup_retention_hours=24 * 30,
            max_database_bytes=16 * 1024 * 1024,
            now=self.now,
        )
        rolled = self.storage.query(query)

        for result in (raw, rolled):
            points = result["series"][0]["points"]
            self.assertEqual(
                [point["timestamp"] for point in points],
                [start.isoformat(), (bucket_start + timedelta(minutes=5)).isoformat()],
            )
            self.assertEqual(points[0]["interval_start"], start.isoformat())
            self.assertEqual(points[-1]["interval_end"], end.isoformat())
            self.assertTrue(all(point["partial_bucket"] for point in points))
            for point in points:
                self.assertGreaterEqual(
                    datetime.fromisoformat(point["timestamp"]), start
                )
                self.assertLessEqual(datetime.fromisoformat(point["timestamp"]), end)
                self.assertGreaterEqual(
                    datetime.fromisoformat(point["first_timestamp"]), start
                )
                self.assertLessEqual(
                    datetime.fromisoformat(point["last_timestamp"]), end
                )

    def test_restart_and_missing_semantics_survive_rollup_queries(self) -> None:
        timestamp = (self.now - timedelta(days=10)).replace(second=0, microsecond=0)
        timestamp -= timedelta(minutes=timestamp.minute % 5)
        timestamp += timedelta(minutes=4, seconds=50)
        samples = []
        for offset, restart_id in ((0, "before"), (20, "after")):
            item = TelemetrySample(
                timestamp=timestamp + timedelta(seconds=offset),
                instance_id="instance-a",
                instance_name="Alpha",
                scope_type="instance",
                scope_id="instance-a",
                restart_id=restart_id,
                metrics={
                    "zero.metric": Metric(
                        value=0,
                        unit="count",
                        quality=MetricQuality.MEASURED,
                        source="test",
                    ),
                    "missing.metric": Metric(
                        value=None,
                        unit="count",
                        quality=MetricQuality.UNAVAILABLE,
                        source="test",
                    ),
                    "restart.metric": Metric(
                        value=10 + offset,
                        unit="count",
                        quality=MetricQuality.MEASURED,
                        source="test",
                    ),
                },
            )
            samples.append(item)
        self.storage.insert_samples(samples)
        query = TelemetryQuery(
            start=self.now - timedelta(days=12),
            end=self.now - timedelta(days=8),
            bucket_seconds=300,
        )
        raw = self.storage.query(query)
        self.storage.prune(
            raw_retention_hours=24,
            rollup_retention_hours=24 * 30,
            max_database_bytes=16 * 1024 * 1024,
            now=self.now,
        )
        rolled = self.storage.query(query)

        def semantics(result: dict) -> dict:
            return {
                series["metric"]: {
                    "observations": [
                        point["observation"] for point in series["points"]
                    ],
                    "missing_reasons": [
                        point["missing_reason"] for point in series["points"]
                    ],
                    "restart": [point["restart"] for point in series["points"]],
                    "gap_reasons": [gap["reason"] for gap in series["gaps"]],
                }
                for series in result["series"]
            }

        self.assertEqual(semantics(rolled), semantics(raw))
        self.assertEqual(
            semantics(rolled)["zero.metric"]["observations"],
            ["genuine_zero", "genuine_zero"],
        )
        self.assertEqual(
            semantics(rolled)["missing.metric"]["missing_reasons"],
            ["temporarily_unavailable", "temporarily_unavailable"],
        )
        self.assertEqual(
            semantics(rolled)["restart.metric"]["restart"],
            [False, True],
        )

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

    def test_empty_zero_single_and_sparse_series_have_explicit_diagnostics(
        self,
    ) -> None:
        empty = self.storage.query(
            TelemetryQuery(
                start=self.now - timedelta(hours=1), end=self.now, bucket_seconds=60
            )
        )
        self.assertEqual(empty["series"], [])
        self.assertEqual(empty["diagnostics"]["point_count"], 0)
        self.storage.insert_samples(
            [
                sample(self.now - timedelta(minutes=2), value=0),
                sample(self.now, value=0),
            ]
        )
        result = self.storage.query(
            TelemetryQuery(
                start=self.now - timedelta(minutes=3),
                end=self.now + timedelta(seconds=1),
                bucket_seconds=60,
            )
        )
        points = result["series"][0]["points"]
        self.assertEqual([point["avg"] for point in points], [0, 0])
        self.assertEqual(result["diagnostics"]["series_count"], 1)
        self.assertEqual(result["diagnostics"]["point_count"], 2)
        self.assertEqual(result["diagnostics"]["bucket_count"], 2)
        self.assertIsNotNone(result["diagnostics"]["collection_freshness"])

    def test_non_finite_aggregate_is_dropped_and_counted(self) -> None:
        malformed = sample(self.now, value=float("inf"))
        self.storage.insert_samples([malformed])
        result = self.storage.query(
            TelemetryQuery(
                start=self.now - timedelta(seconds=1),
                end=self.now + timedelta(seconds=1),
                bucket_seconds=60,
            )
        )
        self.assertEqual(result["series"], [])
        self.assertEqual(result["diagnostics"]["dropped_invalid_samples"], 1)

    def test_timezone_boundary_is_serialized_in_utc_bucket_order(self) -> None:
        boundary = datetime.fromisoformat("2026-11-01T01:59:30-07:00")
        self.storage.insert_samples(
            [
                sample(boundary, value=1),
                sample(boundary + timedelta(minutes=2), value=2),
            ]
        )
        result = self.storage.query(
            TelemetryQuery(
                start=boundary - timedelta(minutes=1),
                end=boundary + timedelta(minutes=3),
                bucket_seconds=60,
            )
        )
        timestamps = [point["timestamp"] for point in result["series"][0]["points"]]
        self.assertEqual(timestamps, sorted(timestamps))
        self.assertTrue(all(timestamp.endswith("+00:00") for timestamp in timestamps))


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
        self.assertIn("data-chart-points", template)
        self.assertIn("data-report-diagnostics", template)
        self.assertIn("ArrowLeft", script)
        self.assertIn("sampling gap", script)
        self.assertNotIn("ResizeObserver", script)
        self.assertIn("AbortController", script)
        self.assertIn("Report could not be loaded", script)
        self.assertIn("Number.isFinite", script)
        self.assertIn("unsupported", script)
        self.assertIn("MAX_CHART_PATH_POINTS", script)
        self.assertIn("ACCESSIBLE_PAGE_SIZE", script)
        self.assertIn("data-telemetry-table-prev", template)
        self.assertIn("data-telemetry-table-next", template)
        self.assertIn("js/telemetry.js", shell)
        self.assertIn("data-session-telemetry", chat)

    @unittest.skipUnless(
        shutil.which("node"), "node is required for telemetry UI tests"
    )
    def test_browser_model_preserves_missing_values_and_shared_domain(self) -> None:
        root = Path(__file__).parents[1]
        script = root / "src/pa/server/static/js/telemetry.js"
        program = r"""
const assert = require("assert");
global.document = {
  body: null,
  hidden: false,
  addEventListener: function () {},
  querySelector: function () { return null; }
};
global.window = {
  location: {href: "http://localhost/reports"},
  setInterval: function () {}
};
const model = require(process.argv[1]);
const start = "2026-01-01T00:00:00Z";
const end = "2026-01-01T00:10:00Z";
const zero = {timestamp: "2026-01-01T00:02:00Z", avg: 0, value_count: 1, quality: "measured"};
const unavailable = {timestamp: "2026-01-01T00:03:00Z", avg: null, value_count: 0, quality: "unavailable"};
const unsupported = {timestamp: "2026-01-01T00:04:00Z", avg: null, value_count: 0, quality: "unsupported"};
const later = {timestamp: "2026-01-01T00:05:00Z", avg: 5, value_count: 1, quality: "measured"};
assert.strictEqual(model.normalizeObservation(zero).state, "genuine_zero");
assert.strictEqual(model.normalizeObservation(unavailable).value, null);
assert.strictEqual(model.normalizeObservation(unavailable).reason, "temporarily_unavailable");
assert.strictEqual(model.normalizeObservation(unsupported).reason, "unsupported");
const segments = model.lineSegments([zero, unavailable, later], 800, 220, 0, 5, 60, start, end);
assert.strictEqual(segments.length, 2);
assert.deepStrictEqual(segments.map(function (segment) {
  return segment.map(function (point) { return point.observation.value; });
}), [[0], [5]]);
const sameTimeA = model.lineSegments([zero], 800, 220, 0, 5, 60, start, end)[0][0].x;
const sameTimeB = model.lineSegments([
  {timestamp: zero.timestamp, avg: 4, value_count: 1, quality: "measured"}
], 800, 220, 0, 5, 60, start, end)[0][0].x;
assert.strictEqual(sameTimeA, sameTimeB);
assert.ok(sameTimeA > 42 && sameTimeA < 784);
const series = {
  instance_name: "Alpha", scope_id: "instance-a", metric: "cpu.utilization",
  unit: "percent", points: [zero, later],
  gaps: [{reason: "no_sample", start: "2026-01-01T00:03:00Z", end: "2026-01-01T00:05:00Z"}]
};
assert.ok(model.cursorValue(series, Date.parse(zero.timestamp)).includes("genuine measured zero"));
assert.strictEqual(model.cursorValue(series, Date.parse("2026-01-01T00:03:00Z")), "No sample");
assert.deepStrictEqual(
  ["no_sample", "unsupported", "temporarily_unavailable", "stale", "restart", "peer_failure"].map(model.gapLabel),
  ["No sample", "Unsupported", "Temporarily unavailable", "Stale", "Collector restart", "Peer failure"]
);
const mixed = {
  timestamp: "2026-01-01T00:06:00Z", avg: 5, value_count: 1, sample_count: 2,
  quality: "unavailable", partial_reason: "temporarily_unavailable"
};
assert.strictEqual(model.normalizeObservation(mixed).reason, null);
assert.strictEqual(model.normalizeObservation(mixed).partialReason, "temporarily_unavailable");
assert.strictEqual(model.lineSegments([mixed], 800, 220, 0, 5, 60, start, end)[0][0].observation.value, 5);
assert.ok(model.cursorValue({
  instance_name: "Alpha", metric: "cpu.utilization", unit: "percent", points: [mixed], gaps: []
}, Date.parse(mixed.timestamp)).includes("partial: temporarily unavailable"));
assert.strictEqual(model.timeX(Date.parse("2025-12-31T23:59:59Z"), start, end, 800), null);
assert.strictEqual(model.timeX(Date.parse("2026-01-01T00:10:01Z"), start, end, 800), null);
assert.strictEqual(model.lineSegments([
  {timestamp: "2025-12-31T23:59:59Z", avg: 99, value_count: 1, quality: "measured"},
  zero
], 800, 220, 0, 99, 60, start, end)[0].length, 1);
const accessible = {
  start: start, end: end, failures: [], series: [{
    instance_name: "Alpha", scope_id: "instance-a", metric: "cpu.utilization", unit: "percent",
    points: [
      {timestamp: "2025-12-31T23:59:59Z", avg: 99, value_count: 1, quality: "measured"}, mixed
    ],
    gaps: [{reason: "stale", start: "2025-12-31T23:59:00Z", end: "2026-01-01T00:01:00Z"}]
  }]
};
const firstPage = model.accessiblePage(accessible, 0, 1);
const secondPage = model.accessiblePage(accessible, 1, 1);
assert.strictEqual(firstPage.counts.total, 2);
assert.strictEqual(firstPage.rows.length, 1);
assert.ok(firstPage.rows[0].values[3].includes("partial: temporarily unavailable"));
assert.strictEqual(secondPage.rows.length, 1);
assert.strictEqual(secondPage.rows[0].gapReason, "stale");
"""
        result = subprocess.run(
            [shutil.which("node"), "-e", program, str(script)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    @unittest.skipUnless(
        shutil.which("node"), "node is required for telemetry UI tests"
    )
    def test_browser_model_bounds_production_cardinality(self) -> None:
        root = Path(__file__).parents[1]
        script = root / "src/pa/server/static/js/telemetry.js"
        program = r"""
const assert = require("assert");
global.document = {
  body: null, hidden: false, addEventListener: function () {},
  querySelector: function () { return null; }
};
global.window = {
  location: {href: "http://localhost/reports"}, setInterval: function () {}
};
const model = require(process.argv[1]);
const start = "2026-01-01T00:00:00Z";
const end = "2026-01-01T06:00:00Z";
const startMs = Date.parse(start);
const series = [];
for (let instance = 0; instance < 27; instance += 1) {
  for (let metric = 0; metric < 24; metric += 1) {
    const points = [];
    for (let bucket = 0; bucket < 360; bucket += 1) {
      points.push({
        timestamp: new Date(startMs + bucket * 60000).toISOString(),
        avg: (instance + metric + bucket) % 100,
        value_count: 1, sample_count: 1, quality: "measured"
      });
    }
    series.push({
      instance_name: "Instance " + instance, scope_id: "instance-" + instance,
      metric: "metric." + metric, unit: "count", points: points,
      gaps: [{
        reason: "stale", start: "2026-01-01T02:00:00Z",
        end: "2026-01-01T02:01:00Z"
      }]
    });
  }
}
const data = {start: start, end: end, series: series, failures: []};
const first = model.accessiblePage(data, 0, model.ACCESSIBLE_PAGE_SIZE);
const second = model.accessiblePage(data, 1, model.ACCESSIBLE_PAGE_SIZE);
assert.strictEqual(first.counts.observations, 27 * 24 * 360);
assert.strictEqual(first.counts.gaps, 27 * 24);
assert.strictEqual(first.rows.length, model.ACCESSIBLE_PAGE_SIZE);
assert.strictEqual(second.rows.length, model.ACCESSIBLE_PAGE_SIZE);
assert.notDeepStrictEqual(first.rows[0].values, second.rows[0].values);
const chartSeries = series.slice(0, 54);
const pathBudget = Math.max(1, Math.floor(model.MAX_CHART_PATH_POINTS / chartSeries.length));
const markerBudget = Math.floor(model.MAX_CHART_MARKERS / chartSeries.length);
let pathPoints = 0; let markers = 0;
chartSeries.forEach(function (item) {
  const raw = model.lineSegments(item.points, 800, 220, 0, 100, 60, start, end);
  const bounded = model.boundedSegments(raw, pathBudget);
  pathPoints += bounded.flat().length;
  markers += model.downsamplePoints(bounded.flat(), markerBudget).length;
});
assert.ok(pathPoints <= model.MAX_CHART_PATH_POINTS);
assert.ok(markers <= model.MAX_CHART_MARKERS);
"""
        result = subprocess.run(
            [shutil.which("node"), "-e", program, str(script)],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_report_panels_are_exact_unit_facets_with_accessible_output(self) -> None:
        root = Path(__file__).parents[1]
        template = (root / "src/pa/server/templates/pages/reports.html").read_text()
        script = (root / "src/pa/server/static/js/telemetry.js").read_text()
        panels = re.findall(
            r"\('[^']+', '[^']+', '([^']+)', '([^']+)'\)",
            template,
        )
        expected_units = {
            "cpu.utilization": "percent",
            "pa.cpu": "percent",
            "session.cpu": "percent_of_one_core",
            "agents.concurrent": "sessions",
            "memory.utilization": "percent",
            "swap.utilization": "percent",
            "pa.memory_rss": "bytes",
            "session.memory_rss": "bytes",
            "disk.read_throughput": "bytes/second",
            "disk.write_throughput": "bytes/second",
            "session.disk_read": "bytes/second",
            "session.disk_write": "bytes/second",
            "disk.read_iops": "operations/second",
            "disk.write_iops": "operations/second",
            "disk.latency": "milliseconds/operation",
            "network.ingress": "bytes/second",
            "network.egress": "bytes/second",
            "session.network_ingress": "bytes/second",
            "session.network_egress": "bytes/second",
            "network.connections": "connections",
            "network.errors": "errors",
            "session.processes": "processes",
            "session.tasks": "threads",
            "pa.threads": "threads",
        }
        configured = {}
        for unit, metrics in panels:
            for metric_name in metrics.split(","):
                self.assertEqual(expected_units[metric_name], unit)
                configured[metric_name] = unit
        self.assertEqual(configured, expected_units)
        self.assertIn("data-unit=", template)
        self.assertIn("data-telemetry-table-body", template)
        self.assertIn("Value or gap reason", template)
        self.assertIn("series.unit === unit", script)
        self.assertIn("genuine measured zero", script)
        self.assertIn("drawAccessibleTable(report, data, 0)", script)


class TelemetryAPITests(unittest.TestCase):
    def test_fleet_query_pins_peer_domain_and_types_peer_failure(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            telemetry = SimpleNamespace(
                storage=TelemetryStorage(Path(tmp) / "telemetry.db")
            )
            services = {
                "fleet_http_client": object(),
                "fleet_registry": SimpleNamespace(
                    list_instances=lambda: [
                        SimpleNamespace(instance_id="peer-a", url="http://peer-a")
                    ]
                ),
            }
            ctx = SimpleNamespace(
                settings=SimpleNamespace(instance_id="instance-a", sync_token=""),
                services=services,
                require_service=lambda name: (
                    telemetry if name == "telemetry" else services[name]
                ),
            )
            request = SimpleNamespace(
                app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
                state=SimpleNamespace(
                    instance_authenticated=False,
                    user_authenticated=True,
                ),
            )
            start = datetime(2026, 1, 1, tzinfo=UTC)
            end = start + timedelta(hours=1)
            with patch(
                "pa.modules.telemetry._peer_json",
                side_effect=OSError("peer unavailable"),
            ) as peer_query:
                response = asyncio.run(
                    fleet_query(request, QueryBody(start=start, end=end))
                )

            self.assertEqual(
                response["failures"],
                [
                    {
                        "instance_id": "peer-a",
                        "state": "unavailable",
                        "reason": "peer_failure",
                        "start": start.isoformat(),
                        "end": end.isoformat(),
                    }
                ],
            )
            remote_body = peer_query.await_args.kwargs["body"]
            self.assertEqual(remote_body["start"], start.isoformat())
            self.assertEqual(remote_body["end"], end.isoformat())

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
