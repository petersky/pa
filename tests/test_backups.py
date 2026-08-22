from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import sqlite3
import tarfile
import tempfile
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pa.backup.service import (
    BackupError,
    BackupService,
    config_from_settings,
    validate_destination,
)
from pa.config import Settings, get_settings, reset_settings
from pa.domain.models import CardCreate, TranscriptEvent
from pa.domain.store import Store
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@contextlib.contextmanager
def _connect(path: Path, **kwargs):
    connection = sqlite3.connect(path, **kwargs)
    with contextlib.closing(connection), connection:
        yield connection


class BackupTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.data_dir = self.root / "data"
        self.destination = self.root / "backups"
        self.destination.mkdir(mode=0o700)
        self.settings = Settings(
            instance_id="11111111-1111-1111-1111-111111111111",
            instance_name="backup-test",
            data_dir=self.data_dir,
            workspace_root=self.root / "workspaces",
            backup_destination_dir=self.destination,
            backup_run_on_startup=False,
            backup_jitter_seconds=0,
        )
        self.settings.ensure_dirs()
        objects = ObjectStore(self.settings.objects_dir)
        event_log = EventLog(objects, self.settings.data_dir, self.settings.instance_id)
        self.store = Store(self.settings.db_path, objects, event_log)
        self.service = BackupService(self.settings, self.store)

    def tearDown(self) -> None:
        reset_settings()
        self.tmp.cleanup()

    def _run(self, key: str = "test"):
        result = self.service.run_backup(idempotency_key=key)
        self.assertEqual(result.status, "success", result.failure_reason)
        self.assertIsNotNone(result.backup_id)
        return result

    def test_six_hour_defaults_and_private_sibling_destination(self) -> None:
        settings = Settings(
            data_dir=self.root / "other-data",
            workspace_root=self.root / "other-workspaces",
        )
        config = config_from_settings(settings)
        self.assertTrue(config.enabled)
        self.assertEqual(config.interval_seconds, 6 * 60 * 60)
        self.assertEqual(
            config.destination_dir, (self.root / "other-data-backups").resolve()
        )
        self.assertEqual(config.concurrency, 1)

    def test_destination_rejects_live_recursive_and_parent_paths(self) -> None:
        for candidate in (
            self.settings.data_dir,
            self.settings.db_path,
            self.settings.data_dir / "backups",
            self.settings.data_dir.parent,
        ):
            with self.subTest(candidate=candidate), self.assertRaises(BackupError):
                validate_destination(self.settings, candidate)

    def test_online_backup_is_verified_atomic_private_and_excludes_wal(self) -> None:
        self.store.object_store.put(b"immutable event object")
        with _connect(self.settings.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE backup_test (value TEXT)")
            conn.execute("INSERT INTO backup_test VALUES ('before')")
        result = self._run()
        records = self.service.list_backups(verify=True)
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertTrue(record.verified, record.verification_error)
        self.assertEqual(record.backup_id, result.backup_id)
        self.assertEqual(record.path.stat().st_mode & 0o777, 0o600)
        self.assertTrue(record.manifest.consistent)
        names = {item.path for item in record.manifest.files}
        self.assertIn("projection.sqlite3", names)
        self.assertIn("sync_refs.json", names)
        self.assertFalse(any(name.endswith(("-wal", "-shm")) for name in names))
        self.assertIn("secrets", record.manifest.excluded)
        self.assertIn("attachments", record.manifest.excluded)

    def test_online_backup_survives_concurrent_wal_writes(self) -> None:
        with _connect(self.settings.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("CREATE TABLE concurrent_values (value INTEGER)")
        stop = threading.Event()
        failures: list[Exception] = []

        def writer() -> None:
            value = 0
            try:
                while not stop.is_set():
                    with _connect(self.settings.db_path, timeout=5) as conn:
                        conn.execute(
                            "INSERT INTO concurrent_values VALUES (?)", (value,)
                        )
                    value += 1
            except (OSError, sqlite3.Error) as exc:  # pragma: no cover
                failures.append(exc)

        thread = threading.Thread(target=writer)
        thread.start()
        try:
            time.sleep(0.02)
            result = self._run()
        finally:
            stop.set()
            thread.join(timeout=5)
        self.assertFalse(failures)
        self.assertTrue(self.service.verify_backup(result.backup_id).verified)

    def test_backup_verifies_transcript_index_and_cold_objects(self) -> None:
        self.store.append_transcript_events([
            TranscriptEvent(
                session_id="backup-session",
                seq=1,
                event_type="turn_completed",
                payload={"text": "result " * 2000},
            )
        ])
        result = self._run("transcript-cold")
        record = self.service.list_backups(verify=True)[0]
        names = {item.path for item in record.manifest.files}
        self.assertIn("transcripts.sqlite3", names)
        self.assertTrue(any(name.startswith("transcript_objects/") for name in names))
        self.assertTrue(record.verified, record.verification_error)

    def test_reachable_event_graph_is_verified_and_missing_object_rejected(
        self,
    ) -> None:
        self.store.create_card(CardCreate(title="durable metadata"))
        good = self._run("event-graph")
        self.assertTrue(self.service.verify_backup(good.backup_id).verified)
        head = self.store.event_log.get_head("default")
        commit = self.store.event_log.get_commit(head)
        event_path = (
            self.settings.objects_dir
            / commit.event_hashes[0][:2]
            / (commit.event_hashes[0][2:])
        )
        event_path.unlink()
        failed = self.service.run_backup(idempotency_key="missing-event")
        self.assertEqual(failed.status, "failed")
        self.assertIn("event_graph_incomplete", failed.failure_reason)
        self.assertEqual(len(self.service.list_backups()), 1)

    def test_overlap_is_skipped_and_idempotency_returns_original_run(self) -> None:
        self.service._run_lock.acquire()
        try:
            skipped = self.service.run_backup(idempotency_key="overlap")
        finally:
            self.service._run_lock.release()
        self.assertEqual(skipped.status, "skipped")
        self.assertIn("already running", skipped.failure_reason)
        again = self.service.run_backup(idempotency_key="overlap")
        self.assertEqual(again.id, skipped.id)

    def test_cross_process_destination_lock_skips_overlap(self) -> None:
        lock_path = self.destination / ".pa-backup.lock"
        fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            skipped = self.service.run_backup(idempotency_key="process-overlap")
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)
        self.assertEqual(skipped.status, "skipped")
        self.assertIn("another process", skipped.failure_reason)
        self.assertEqual(self.service.status()["consecutive_failures"], 0)

    def test_restart_idempotency_reuses_durable_run(self) -> None:
        original = self._run("durable-key")
        restarted = BackupService(self.settings, self.store)
        repeated = restarted.run_backup(idempotency_key="durable-key")
        self.assertEqual(repeated.id, original.id)
        self.assertEqual(len(restarted.list_backups()), 1)

    def test_interrupted_atomic_publish_leaves_no_visible_archive(self) -> None:
        with patch.object(
            self.service.backend,
            "publish",
            side_effect=OSError("simulated disconnect"),
        ):
            result = self.service.run_backup(idempotency_key="publish-crash")
        self.assertEqual(result.status, "failed")
        self.assertEqual(self.service.list_backups(), [])
        self.assertEqual(list(self.destination.glob(".pa-backup-*.tmp")), [])

    def test_count_retention_is_deterministic_and_failure_preserves_good_copy(
        self,
    ) -> None:
        config = self.service.config.model_copy(update={"retention_count": 2})
        self.service.apply_config(config)
        first = self._run("one")
        self._run("two")
        third = self._run("three")
        records = self.service.list_backups()
        self.assertEqual(len(records), 2)
        self.assertEqual(
            {item.backup_id for item in records},
            {third.backup_id, self.service.run_backup(idempotency_key="two").backup_id},
        )
        with patch.object(
            self.service,
            "_snapshot",
            side_effect=BackupError("injected", "new backup failed"),
        ):
            failed = self.service.run_backup(idempotency_key="failure")
        self.assertEqual(failed.status, "failed")
        self.assertEqual(len(self.service.list_backups()), 2)
        self.assertNotIn(first.backup_id, {item.backup_id for item in records})

    def test_age_and_size_retention_never_delete_last_verified(self) -> None:
        only = self._run("only")
        config = self.service.config.model_copy(
            update={
                "retention_max_age_seconds": 60,
                "retention_max_total_bytes": 1024,
            }
        )
        self.service.apply_config(config)
        records = self.service.list_backups()
        old = datetime(2000, 1, 1, tzinfo=UTC)
        records[0].manifest.created_at = old
        # The archive remains immutable; pruning uses its verified manifest and
        # still preserves a single recovery point regardless of age/size.
        self.assertEqual(self.service.prune(), [])
        self.assertEqual(self.service.list_backups()[0].backup_id, only.backup_id)

    def test_prune_reuses_durable_verification_records(self) -> None:
        self._run("verified")

        with patch.object(self.service, "verify_backup") as verify:
            self.assertEqual(self.service.prune(), [])

        verify.assert_not_called()

    def test_age_retention_removes_oldest_verified_backup(self) -> None:
        first = self._run("age-one")
        record = self.service.inspect_backup(first.backup_id)
        with tempfile.TemporaryDirectory() as tmp:
            extracted = Path(tmp)
            with tarfile.open(record.path, "r:*") as archive:
                archive.extractall(extracted, filter="data")
            manifest_path = extracted / "manifest.json"
            manifest = json.loads(manifest_path.read_text())
            manifest["created_at"] = "2000-01-01T00:00:00Z"
            manifest_path.write_text(json.dumps(manifest, sort_keys=True))
            replacement = extracted / "replacement.tgz"
            with tarfile.open(replacement, "w:gz") as archive:
                for item in sorted(extracted.iterdir()):
                    if item != replacement:
                        archive.add(item, arcname=item.name, recursive=True)
            os.replace(replacement, record.path)
        self.assertTrue(self.service.verify_backup(first.backup_id).verified)
        self.service.apply_config(
            self.service.config.model_copy(update={"retention_max_age_seconds": 60})
        )
        second = self._run("age-two")
        self.assertEqual(
            [item.backup_id for item in self.service.list_backups()],
            [second.backup_id],
        )

    def test_compression_can_be_disabled(self) -> None:
        self.service.apply_config(
            self.service.config.model_copy(update={"compression": False})
        )
        backup = self._run("uncompressed")
        record = self.service.inspect_backup(backup.backup_id)
        self.assertEqual(record.path.suffix, ".tar")
        self.assertFalse(record.manifest.compressed)

    def test_size_retention_removes_oldest_verified_backup(self) -> None:
        first = self._run("size-one")
        first_size = self.service.list_backups()[0].size_bytes
        self.service.apply_config(
            self.service.config.model_copy(
                update={"retention_max_total_bytes": first_size + 128}
            )
        )
        second = self._run("size-two")
        records = self.service.list_backups()
        self.assertEqual([item.backup_id for item in records], [second.backup_id])
        self.assertNotEqual(first.backup_id, second.backup_id)

    def test_missing_and_unsafe_permission_destinations_fail_visibly(self) -> None:
        missing = self.root / "missing"
        config = self.service.config.model_copy(update={"destination_dir": missing})
        self.service.apply_config(config)
        failed = self.service.run_backup(idempotency_key="missing")
        self.assertEqual(failed.status, "failed")
        self.assertIn("destination_missing", failed.failure_reason)

        exposed = self.root / "exposed"
        exposed.mkdir(mode=0o755)
        self.service.apply_config(
            self.service.config.model_copy(update={"destination_dir": exposed})
        )
        failed = self.service.run_backup(idempotency_key="exposed")
        self.assertEqual(failed.status, "failed")
        self.assertIn("destination_unhealthy", failed.failure_reason)

        readonly = self.root / "readonly"
        readonly.mkdir(mode=0o700)
        readonly.chmod(0o500)
        try:
            self.service.apply_config(
                self.service.config.model_copy(update={"destination_dir": readonly})
            )
            failed = self.service.run_backup(idempotency_key="readonly")
            self.assertEqual(failed.status, "failed")
            self.assertIn("destination_unhealthy", failed.failure_reason)
        finally:
            readonly.chmod(0o700)

    def test_full_destination_failure_is_visible_and_preserves_prior_backup(
        self,
    ) -> None:
        good = self._run("before-full")
        with patch.object(
            self.service,
            "_archive",
            side_effect=OSError(28, "No space left on device"),
        ):
            failed = self.service.run_backup(idempotency_key="full")
        self.assertEqual(failed.status, "failed")
        self.assertIn("No space left", failed.failure_reason)
        self.assertEqual(
            [item.backup_id for item in self.service.list_backups()],
            [good.backup_id],
        )

    def test_corrupted_archive_is_detected(self) -> None:
        result = self._run()
        path = self.service.download_path(result.backup_id)
        path.write_bytes(b"corruption")
        record = self.service.verify_backup(result.backup_id)
        self.assertFalse(record.verified)
        self.assertIsNotNone(record.verification_error)
        self.assertEqual(record.backup_id, result.backup_id)
        self.assertFalse(self.service.list_backups()[0].verified)

    def test_corrupt_explicit_backup_can_be_deleted_when_good_copy_remains(
        self,
    ) -> None:
        corrupt = self._run("corrupt-for-delete")
        path = self.service.download_path(corrupt.backup_id)
        self._run("good-after-corrupt")
        path.write_bytes(b"corruption")
        self.service.delete_backup(corrupt.backup_id)
        records = self.service.list_backups(verify=True)
        self.assertEqual(len(records), 1)
        self.assertTrue(records[0].verified)

    def test_projection_sync_mismatch_fails_without_publishing(self) -> None:
        (self.settings.data_dir / "sync_refs.json").write_text(
            json.dumps({f"default/{self.settings.instance_id}": "a" * 64})
        )
        self.store.event_log.reload_refs()
        result = self.service.run_backup(idempotency_key="mismatch")
        self.assertEqual(result.status, "failed")
        self.assertIn("projection_sync_mismatch", result.failure_reason)
        self.assertEqual(self.service.list_backups(), [])

    def test_guarded_compatible_restore_creates_pre_restore_backup(self) -> None:
        with _connect(self.settings.db_path) as conn:
            conn.execute("CREATE TABLE restore_values (value TEXT)")
            conn.execute("INSERT INTO restore_values VALUES ('backup')")
        backup = self._run("restore-source")
        request = self.service.initiate_restore(
            backup.backup_id,
            requested_by="user:test",
            confirm_instance_id=self.settings.instance_id,
        )
        self.assertEqual(request.status, "maintenance_required")
        with _connect(self.settings.db_path) as conn:
            conn.execute("INSERT INTO restore_values VALUES ('newer')")
        offline = BackupService(self.settings, None)
        restored = offline.restore_offline(backup.backup_id, request_id=request.id)
        self.assertEqual(restored.status, "success", restored.failure_reason)
        self.assertIsNotNone(restored.pre_restore_backup_id)
        with _connect(self.settings.db_path) as conn:
            values = [
                row[0]
                for row in conn.execute(
                    "SELECT value FROM restore_values ORDER BY rowid"
                )
            ]
        self.assertEqual(values, ["backup"])

    def test_interrupted_restore_rolls_back_current_state(self) -> None:
        with _connect(self.settings.db_path) as conn:
            conn.execute("CREATE TABLE rollback_values (value TEXT)")
            conn.execute("INSERT INTO rollback_values VALUES ('backup')")
        backup = self._run("rollback-source")
        with _connect(self.settings.db_path) as conn:
            conn.execute("INSERT INTO rollback_values VALUES ('current')")
        real_replace = os.replace

        def interrupt(source, target):
            source_path = Path(source)
            if source_path.name == "projection.sqlite3" and ".pa-restore-stage-" in str(
                source_path.parent
            ):
                raise OSError("simulated interrupted restore")
            return real_replace(source, target)

        offline = BackupService(self.settings, None)
        with patch("pa.backup.service.os.replace", side_effect=interrupt):
            restored = offline.restore_offline(backup.backup_id)
        self.assertEqual(restored.status, "failed")
        with _connect(self.settings.db_path) as conn:
            values = [
                row[0]
                for row in conn.execute(
                    "SELECT value FROM rollback_values ORDER BY rowid"
                )
            ]
        self.assertEqual(values, ["backup", "current"])
        self.assertTrue(restored.pre_restore_backup_id)

    def test_post_restore_head_mismatch_requires_supported_reconciliation(self) -> None:
        backup = self._run("post-mismatch")
        real_replace = os.replace

        def inject_mismatch(source, target):
            result = real_replace(source, target)
            source_path = Path(source)
            if source_path.name == "objects" and ".pa-restore-stage-" in str(
                source_path.parent
            ):
                refs = self.settings.data_dir / "sync_refs.json"
                refs.write_text(
                    json.dumps({f"default/{self.settings.instance_id}": "b" * 64})
                )
            return result

        offline = BackupService(self.settings, None)
        with patch("pa.backup.service.os.replace", side_effect=inject_mismatch):
            restored = offline.restore_offline(backup.backup_id)
        self.assertEqual(restored.status, "reconciliation_required")
        self.assertEqual(restored.reconciliation_realms, ["default"])
        self.assertTrue(any("sync reconcile" in item for item in restored.instructions))

    def test_last_verified_backup_cannot_be_deleted(self) -> None:
        backup = self._run("last-good")
        with self.assertRaisesRegex(BackupError, "last known-good"):
            self.service.delete_backup(backup.backup_id)

    def test_schedule_records_bounded_jitter_and_next_run(self) -> None:
        self.service.config = self.service.config.model_copy(
            update={"interval_seconds": 21600, "jitter_seconds": 300}
        )
        before = datetime.now(UTC)
        scheduled = self.service._schedule_next(base=before)
        delta = (scheduled - before).total_seconds()
        self.assertGreaterEqual(delta, 21600)
        self.assertLessEqual(delta, 21900)
        restarted = BackupService(self.settings, self.store)
        persisted = restarted.status()["next_scheduled_run"]
        self.assertEqual(datetime.fromisoformat(persisted), scheduled)

    def test_incompatible_schema_is_rejected_before_live_state_changes(self) -> None:
        backup = self._run("schema")
        with _connect(self.settings.db_path) as conn:
            conn.execute("CREATE TABLE incompatible_change (id INTEGER)")
        offline = BackupService(self.settings, None)
        result = offline.restore_offline(backup.backup_id)
        self.assertEqual(result.status, "failed")
        self.assertIn("schema_incompatible", result.failure_reason)
        self.assertIn("Live PA state was not changed.", result.instructions)
        with _connect(self.settings.db_path) as conn:
            self.assertIsNotNone(
                conn.execute(
                    "SELECT name FROM sqlite_master WHERE name='incompatible_change'"
                ).fetchone()
            )

    def test_configuration_reports_source_and_alert_threshold(self) -> None:
        status = self.service.status()
        self.assertEqual(status["effective"]["interval_seconds"], 21600)
        self.assertEqual(status["sources"]["interval_seconds"], "default")
        self.assertFalse(status["alerting"]["active"])

    def test_service_environment_uses_explicit_data_dir_on_macos_and_linux(
        self,
    ) -> None:
        for platform in ("darwin", "linux"):
            with (
                self.subTest(platform=platform),
                patch.dict(
                    os.environ,
                    {
                        "PA_DATA_DIR": str(self.data_dir),
                        "PA_BACKUP_DESTINATION_DIR": str(self.destination),
                    },
                    clear=True,
                ),
            ):
                settings = Settings()
                self.assertEqual(settings.data_dir, self.data_dir)
                self.assertEqual(
                    settings.backup_destination_dir, self.destination.resolve()
                )

    def test_legacy_config_does_not_mask_backup_environment_values(self) -> None:
        legacy = {
            "instance_id": self.settings.instance_id,
            "instance_name": "legacy",
            "data_dir": str(self.data_dir),
        }
        (self.data_dir / "config.json").write_text(json.dumps(legacy))
        with patch.dict(
            os.environ,
            {
                "PA_DATA_DIR": str(self.data_dir),
                "PA_BACKUP_INTERVAL_SECONDS": "900",
                "PA_BACKUP_DESTINATION_DIR": str(self.destination),
            },
            clear=True,
        ):
            reset_settings()
            loaded = get_settings()
        self.assertEqual(loaded.backup_interval_seconds, 900)
        self.assertEqual(loaded.backup_destination_dir, self.destination.resolve())
        reset_settings()


class BackupSchedulerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        root = Path(self.tmp.name)
        destination = root / "backups"
        destination.mkdir(mode=0o700)
        self.settings = Settings(
            instance_id="22222222-2222-2222-2222-222222222222",
            data_dir=root / "data",
            workspace_root=root / "workspaces",
            backup_destination_dir=destination,
            backup_run_on_startup=False,
        )
        self.settings.ensure_dirs()
        objects = ObjectStore(self.settings.objects_dir)
        self.store = Store(
            self.settings.db_path,
            objects,
            EventLog(objects, self.settings.data_dir, self.settings.instance_id),
        )
        self.service = BackupService(self.settings, self.store)

    async def asyncTearDown(self) -> None:
        await self.service.stop()
        self.tmp.cleanup()

    async def test_disabled_scheduler_can_be_enabled_and_rescheduled(self) -> None:
        class Runtime:
            async def run_blocking(self, *_args, **_kwargs):
                return None

        self.service.apply_config(
            self.service.config.model_copy(update={"enabled": False})
        )
        await self.service.start(Runtime())
        self.assertIsNone(self.service.status()["next_scheduled_run"])
        self.service.apply_config(
            self.service.config.model_copy(
                update={"enabled": True, "interval_seconds": 120}
            )
        )
        await asyncio.sleep(0)
        self.assertIsNotNone(self.service.status()["next_scheduled_run"])

    async def test_startup_minimum_age_controls_startup_run(self) -> None:
        calls: list[str] = []

        class Runtime:
            async def run_blocking(self, operation, *_args, **_kwargs):
                calls.append(operation)

        self.service.apply_config(
            self.service.config.model_copy(
                update={
                    "enabled": True,
                    "run_on_startup": True,
                    "startup_min_age_seconds": 3600,
                }
            )
        )
        await self.service.start(Runtime())
        await asyncio.sleep(0.01)
        self.assertIn("backup.startup", calls)

    async def test_overdue_intervals_coalesce_into_one_scheduled_attempt(self) -> None:
        calls: list[str] = []

        class Runtime:
            async def run_blocking(self, operation, *_args, **_kwargs):
                calls.append(operation)

        self.service.apply_config(
            self.service.config.model_copy(
                update={"enabled": True, "interval_seconds": 60}
            )
        )
        self.service._state.next_scheduled_run = datetime.now(UTC) - timedelta(
            minutes=5
        )
        self.service._save_state()
        await self.service.start(Runtime())
        await asyncio.sleep(0.02)
        self.assertEqual(calls, ["backup.scheduled"])
        next_run = datetime.fromisoformat(self.service.status()["next_scheduled_run"])
        self.assertGreater(next_run, datetime.now(UTC))


if __name__ == "__main__":
    unittest.main()
