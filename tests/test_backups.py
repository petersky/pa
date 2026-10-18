from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import io
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
from unittest.mock import Mock, patch
from uuid import uuid4

from pa.backup.service import (
    BackupError,
    BackupService,
    config_from_settings,
    validate_destination,
)
from pa.backup.models import MAX_MANIFEST_BYTES, BackupManifest, ManifestFile
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

    def test_snapshot_copy_allows_writes_and_survives_transcript_gc(self) -> None:
        self.store.create_card(CardCreate(title="captured before backup"))
        self.store.append_transcript_events([TranscriptEvent(
            session_id="snapshot-gc", seq=1, event_type="turn_completed",
            payload={"text": "cold payload " * 2000},
        )])
        copying, release = threading.Event(), threading.Event()
        original = self.service._copy_file
        results, failures = [], []

        def copy(source, target):
            copying.set()
            if not release.wait(5):
                raise RuntimeError("fixture copy was not released")
            original(source, target)  # Real fsync; no durability mock.

        def backup():
            try:
                results.append(self.service.run_backup(idempotency_key="snapshot-gc"))
            except Exception as exc:
                failures.append(exc)

        def mutate():
            try:
                self.store.create_card(CardCreate(title="written during copy"))
                self.store.transcripts.prune(["snapshot-gc"], keep_audit=False)
            except Exception as exc:
                failures.append(exc)

        with patch.object(self.service, "_copy_file", side_effect=copy):
            worker = threading.Thread(target=backup)
            writer = threading.Thread(target=mutate)
            worker.start()
            try:
                self.assertTrue(copying.wait(5))
                writer.start()
                writer.join(2)
                self.assertFalse(writer.is_alive(), "object copying held a PA writer lock")
                self.assertFalse(list((self.data_dir / "transcript_objects").glob("[0-9a-f][0-9a-f]/*.zlib")))
            finally:
                release.set()
                worker.join(10)
                if writer.ident is not None:
                    writer.join(5)
        self.assertFalse(worker.is_alive())
        self.assertFalse(failures, failures)
        self.assertEqual(results[0].status, "success", results[0].failure_reason)
        record = self.service.verify_backup(results[0].backup_id)
        self.assertTrue(record.verified, record.verification_error)
        self.assertNotEqual(record.manifest.durable_heads["default"], self.store.event_log.get_head("default"))
        self.assertFalse(list((self.data_dir / "transcript_objects").glob(".pa-backup-pin-*")))

    def test_snapshot_pin_failure_cleans_up_and_preserves_recovery_point(self) -> None:
        good = self._run("before-pin-failure")
        self.store.append_transcript_events([TranscriptEvent(
            session_id="pin-failure", seq=1, event_type="turn_completed",
            payload={"text": "cold payload " * 2000},
        )])
        with patch("pa.backup.service.os.link", side_effect=OSError("isolated pin failure")):
            failed = self.service.run_backup(idempotency_key="pin-failure")
        self.assertEqual(failed.status, "failed")
        self.assertEqual([row.backup_id for row in self.service.list_backups()], [good.backup_id])
        self.assertTrue(self.service.verify_backup(good.backup_id).verified)
        self.assertFalse(list((self.data_dir / "transcript_objects").glob(".pa-backup-pin-*")))

    def test_sqlite_copy_releases_writer_locks_and_retains_captured_cold_objects(self) -> None:
        self.store.create_card(CardCreate(title="captured before database copy"))
        self.store.append_transcript_events([TranscriptEvent(
            session_id="database-copy-gc", seq=1, event_type="turn_completed",
            payload={"text": "captured cold payload " * 2000},
        )])
        copying, release = threading.Event(), threading.Event()
        original = self.service._online_sqlite_backup
        results, failures = [], []
        def copy(target, **kwargs):
            copying.set()
            if not release.wait(5):
                raise RuntimeError("isolated database copy not released")
            return original(target, **kwargs)
        def backup():
            results.append(self.service.run_backup(idempotency_key="database-copy-gc"))
        def mutate():
            try:
                self.store.create_card(CardCreate(title="written during database copy"))
                self.store.transcripts.prune(["database-copy-gc"], keep_audit=False)
            except Exception as exc:
                failures.append(exc)
        with patch.object(self.service, "_online_sqlite_backup", side_effect=copy):
            worker = threading.Thread(target=backup)
            writer = threading.Thread(target=mutate)
            worker.start()
            try:
                self.assertTrue(copying.wait(5))
                writer.start(); writer.join(2)
                self.assertFalse(writer.is_alive(), "SQLite disk copy held writer locks")
                self.assertTrue(list(self.store.transcripts.objects_dir.glob("*/*.zlib")))
            finally:
                release.set(); worker.join(15)
                if writer.ident is not None:
                    writer.join(5)
        self.assertFalse(failures, failures)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0].status, "success", results[0].failure_reason)
        record = self.service.verify_backup(results[0].backup_id)
        self.assertTrue(record.verified, record.verification_error)
        self.assertNotEqual(record.manifest.durable_heads["default"], self.store.event_log.get_head("default"))
        self.assertGreater(self.store.transcripts.collect_unreferenced_objects(), 0)

    def test_missing_captured_cold_reference_is_disclosed_without_rewriting_history(self) -> None:
        good = self._run("before-missing-cold")
        self.store.append_transcript_events([TranscriptEvent(
            session_id="missing-cold", seq=1, event_type="turn_completed",
            payload={"text": "missing cold payload " * 2000},
        )])
        for path in self.store.transcripts.objects_dir.glob("*/*.zlib"):
            path.unlink()  # Disposable fixture only; never alter live recovery data.
        self.store.create_card(CardCreate(title="Available current data is still copied"))
        with patch.object(self.service, "_copy_file", wraps=self.service._copy_file) as copied, \
             patch.object(self.service, "_archive", wraps=self.service._archive) as archived:
            limited = self.service.run_backup(idempotency_key="missing-cold-preflight")
        self.assertEqual(limited.status, "success")
        self.assertTrue(limited.integrity_verified)
        self.assertFalse(limited.data_complete)
        self.assertEqual(limited.verification, "failed")  # Never an intact proof.
        self.assertEqual(limited.missing_transcript_object_count, 1)
        self.assertGreater(copied.call_count, 0)
        self.assertEqual(archived.call_count, 1)
        self.assertIn("snapshot", limited.phase_seconds)
        self.assertEqual({row.backup_id for row in self.service.list_backups()}, {good.backup_id, limited.backup_id})
        self.assertEqual(len(self.store.transcripts.referenced_hashes()), 1)

    def test_cold_pinning_releases_writer_locks_and_gc_resumes_after_pins(self) -> None:
        self.store.append_transcript_events([TranscriptEvent(
            session_id="pin-retention", seq=1, event_type="turn_completed",
            payload={"text": "cold pin payload " * 2000},
        )])
        pinning, release = threading.Event(), threading.Event()
        original = os.link
        results = []
        def link(*args, **kwargs):
            pinning.set()
            if not release.wait(5):
                raise RuntimeError("isolated pin not released")
            return original(*args, **kwargs)
        def mutate():
            self.store.create_card(CardCreate(title="written while pinning"))
            self.store.transcripts.prune(["pin-retention"], keep_audit=False)
        with patch("pa.backup.service.os.link", side_effect=link):
            worker = threading.Thread(target=lambda: results.append(self.service.run_backup(idempotency_key="pin-retention")))
            writer = threading.Thread(target=mutate)
            worker.start()
            try:
                self.assertTrue(pinning.wait(5))
                writer.start(); writer.join(2)
                self.assertFalse(writer.is_alive(), "hard-link disk I/O held writer locks")
                self.assertTrue(list(self.store.transcripts.objects_dir.glob("*/*.zlib")))
            finally:
                release.set(); worker.join(15)
                if writer.ident is not None:
                    writer.join(5)
        self.assertFalse(worker.is_alive())
        self.assertEqual(results[0].status, "success", results[0].failure_reason)
        self.assertTrue(self.service.verify_backup(results[0].backup_id).verified)
        self.assertEqual(self.store.transcripts.collect_unreferenced_objects(), 1)

    def test_rollback_journal_snapshot_uses_safe_fallback_without_changing_mode(self) -> None:
        self.store.close_thread_connection()
        with _connect(self.settings.db_path) as conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode=DELETE").fetchone()[0], "delete")
        self._run("legacy-rollback-journal")
        with _connect(self.settings.db_path) as conn:
            self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        self.assertEqual(self.service._state.metrics["backup_snapshot_locked_sqlite_fallback_total"], 1)

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
        records[0].created_at = old
        # Even an expired archive exceeding the size budget remains protected.
        with patch.object(self.service, "list_backups", return_value=records):
            self.assertEqual(self.service.prune(), [])
        self.assertEqual(self.service.prune(), [])
        self.assertEqual(self.service.list_backups()[0].backup_id, only.backup_id)

    def test_prune_reuses_durable_verification_records(self) -> None:
        self._run("verified")

        with patch.object(self.service, "verify_backup") as verify:
            self.assertEqual(self.service.prune(), [])

        verify.assert_not_called()

    def test_normal_run_only_verifies_new_artifact(self) -> None:
        self._run("retained-one")
        self._run("retained-two")
        real_verify = self.service.verify_backup
        verified_paths: list[Path] = []

        def observe(backup_id, *, path=None):
            verified_paths.append(path)
            return real_verify(backup_id, path=path)

        with patch.object(self.service, "verify_backup", side_effect=observe):
            run = self.service.run_backup(idempotency_key="constant-work")

        self.assertEqual(run.status, "success")
        self.assertEqual(len(verified_paths), 1)
        self.assertIn(".pa-backup-", verified_paths[0].name)
        self.assertTrue(verified_paths[0].name.endswith(".tmp"))
        self.assertEqual(
            set(run.phase_seconds),
            {"snapshot", "archive", "verify", "publish", "prune"},
        )

    def test_deep_scrub_detects_corruption_without_normal_run_reread(self) -> None:
        corrupt = self._run("scrub-corrupt")
        self._run("scrub-good")
        self.service.download_path(corrupt.backup_id).write_bytes(b"corrupt")

        results = self.service.deep_scrub()

        self.assertFalse(results[corrupt.backup_id])
        self.assertTrue(any(results.values()))
        self.assertIsNotNone(self.service.status()["last_scrub"])

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
        # A caller retaining the projection must reopen the restored inode,
        # rather than reading or writing the pre-restore database connection.
        with self.store._conn() as conn:
            self.assertEqual(
                [row[0] for row in conn.execute("SELECT value FROM restore_values ORDER BY rowid")],
                ["backup"],
            )

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

    def test_large_v1_manifest_round_trips_real_object_inventory(self) -> None:
        # v1's real per-object paths/size/hash entries exceed the former 2 MiB
        # bound at realistic histories, even with tiny immutable payloads.
        for index in range(12_000):
            payload = f"isolated backup object {index}".encode()
            digest = hashlib.sha256(payload).hexdigest()
            path = self.settings.objects_dir / digest[:2] / digest[2:]
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)
        for compressed in (True, False):
            with self.subTest(compressed=compressed):
                self.service.config.compression = compressed
                # Durability is covered by the small atomic-publication tests;
                # avoid 24,000 physical fsyncs in the scale regression fixture.
                with patch("pa.backup.service.os.fsync"):
                    result = self._run(f"large-{compressed}")
                record = self.service.list_backups()[0]
                self.assertEqual(record.backup_id, result.backup_id)
                self.assertTrue(record.verified)
                with tarfile.open(record.path, "r:*") as archive:
                    member = archive.next()
                    self.assertEqual(member.name, "manifest.json")
                    self.assertGreater(member.size, 2 * 1024 * 1024)
                    self.assertLessEqual(member.size, MAX_MANIFEST_BYTES)
                manifest = self.service._read_manifest(record.path)
                self.assertGreaterEqual(len(manifest.files), 12_002)

    def test_manifest_writer_limit_fails_before_publication_preserving_good(self) -> None:
        good = self._run("before-limit")
        with patch("pa.backup.service.MAX_MANIFEST_BYTES", 512):
            failed = self.service.run_backup(idempotency_key="over-limit")
        self.assertEqual(failed.status, "failed")
        self.assertIn("manifest_limit_exceeded", failed.failure_reason)
        self.assertEqual([item.backup_id for item in self.service.list_backups()], [good.backup_id])
        self.assertEqual(set(self.service._state.verifications), {good.backup_id})

    def test_reader_rejects_declared_oversized_manifest_without_reading_payload(self) -> None:
        path = self.root / "oversize.tar"
        member = tarfile.TarInfo("manifest.json")
        member.size = MAX_MANIFEST_BYTES + 1
        path.write_bytes(member.tobuf() + bytes(1024))
        with self.assertRaisesRegex(BackupError, "byte limit"):
            self.service._read_manifest(path)

    def test_inventory_never_opens_archives_and_invalidates_changed_bytes(self) -> None:
        good = self._run("inventory-good")
        record = self.service.list_backups()[0]
        restarted = BackupService(self.settings, self.store)
        with patch("pa.backup.service.tarfile.open", side_effect=AssertionError("inventory must not open archives")):
            self.assertTrue(restarted.list_backups()[0].verified)
            self.assertEqual(restarted.status()["last_known_good"]["backup_id"], good.backup_id)
            record.path.write_bytes(b"damaged")
            changed = restarted.list_backups()[0]
            self.assertFalse(changed.verified)
            self.assertEqual(changed.verification_status, "unverified")
            self.assertIsNone(restarted.status()["last_known_good"])
        failed = restarted.verify_backup(good.backup_id)
        self.assertEqual(failed.verification_status, "failed")
        self.assertIn("archive_unreadable", restarted.list_backups()[0].verification_error)

    def test_cold_legacy_and_unknown_inventory_is_unverified_without_scan(self) -> None:
        backup_id = str(uuid4())
        path = self.destination / f"old-{backup_id}.pa-backup.tgz"
        path.write_bytes(b"unknown bytes")
        unknown = self.destination / "imported.pa-backup.tar"
        unknown.write_bytes(b"damaged import")
        self.service._state.verifications[backup_id] = {"verified": True}
        with patch.object(self.service, "_read_manifest", side_effect=AssertionError("no implicit scan")):
            self.assertTrue(all(item.verification_status == "unverified" for item in self.service.list_backups()))
            self.assertEqual(self.service.prune(), [])
        self.assertEqual(self.service.verify_backup(f"unreadable:{unknown.name}").verification_status, "failed")
        self.assertEqual(len(self.service.list_backups()), 2)

    def test_status_is_fast_and_bounded_with_legacy_failure_history(self) -> None:
        for index in range(200):
            (self.destination / f"archive-{uuid4()}.pa-backup.tgz").write_bytes(b"fixture")
        self.service._state.verifications = {str(uuid4()): {"verified": False, "error": "old temporary failure"} for _ in range(2000)}
        before = self.service.state_path.read_bytes() if self.service.state_path.exists() else None
        with patch.object(self.service, "_read_manifest", side_effect=AssertionError("no scan")):
            started = time.monotonic()
            overview = self.service.overview(limit=50)
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0)
        self.assertEqual(overview["status"]["backup_count"], 200)
        self.assertEqual(len(overview["records"]), 50)
        self.assertEqual(overview["pagination"]["next_offset"], 50)
        self.assertNotIn("verifications", overview["status"])
        self.assertLess(len(json.dumps(overview)), 60_000)
        self.assertEqual(before, self.service.state_path.read_bytes() if self.service.state_path.exists() else None)

    def test_concurrent_verification_reports_in_progress_without_blocking_status(self) -> None:
        good = self._run("concurrent-verification")
        entered, release = threading.Event(), threading.Event()
        original = self.service._read_manifest
        def blocked(path):
            entered.set()
            self.assertTrue(release.wait(5))
            return original(path)
        with patch.object(self.service, "_read_manifest", side_effect=blocked):
            thread = threading.Thread(target=self.service.verify_backup, args=(good.backup_id,))
            thread.start()
            try:
                self.assertTrue(entered.wait(5))
                self.assertEqual(self.service.list_backups()[0].verification_status, "in_progress")
                self.assertEqual(self.service.status()["backup_count"], 1)
                with self.assertRaisesRegex(BackupError, "already being verified"):
                    self.service.verify_backup(good.backup_id)
            finally:
                release.set()
                thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertTrue(self.service.list_backups()[0].verified)

    def test_failure_threshold_pauses_automatic_attempts_and_notifies_once(self) -> None:
        notifier = Mock()
        self.service.failure_notifier = notifier
        with patch.object(self.service, "_snapshot", side_effect=BackupError("injected", "fixture failure")) as snapshot:
            for index in range(3):
                self.assertEqual(self.service.run_backup(trigger="scheduled", idempotency_key=f"fail-{index}").status, "failed")
            for index in range(5):
                self.assertEqual(self.service.run_backup(trigger="scheduled").status, "skipped")
            self.assertEqual(snapshot.call_count, 3)
        notifier.assert_called_once()
        paused = self.service.status()
        self.assertTrue(paused["automatic_backups_paused"])
        self.assertIsNone(paused["next_scheduled_run"])
        self.assertEqual(paused["next_actor"], "Administrator")
        restarted = BackupService(self.settings, self.store)
        restarted.failure_notifier = notifier
        self.assertEqual(restarted.run_backup(trigger="startup").status, "skipped")
        notifier.assert_called_once()
        self.assertEqual(restarted.run_backup(idempotency_key="manual-recovery").status, "success")
        self.assertFalse(restarted.status()["automatic_backups_paused"])
        self.assertIsNotNone(restarted.status()["next_scheduled_run"])

    def test_offline_failure_pause_does_not_suppress_later_server_notification(self) -> None:
        self.service._state.consecutive_failures = self.service.config.alert_after_failures
        self.service._notify_repeated_failure()
        self.assertIsNone(self.service._state.failure_alert_key)
        notifier = Mock()
        self.service.failure_notifier = notifier
        self.service._notify_repeated_failure()
        self.service._notify_repeated_failure()
        notifier.assert_called_once()

    def test_manifest_rejects_duplicate_paths_traversal_and_bad_hash(self) -> None:
        record = self.service.inspect_backup(self._run("manifest-validation").backup_id)
        raw = record.manifest.model_dump()
        raw["files"].append(dict(raw["files"][0]))
        with self.assertRaisesRegex(ValueError, "duplicate"):
            BackupManifest.model_validate(raw)
        for path in ("../escape", "/absolute", "objects/../escape", "./relative"):
            with self.subTest(path=path), self.assertRaises(ValueError):
                ManifestFile(path=path, size_bytes=0, sha256="a" * 64)
        with self.assertRaises(ValueError):
            ManifestFile(path="projection.sqlite3", size_bytes=0, sha256="invalid")

    def test_legacy_manifest_order_is_compatible_and_duplicate_members_fail(self) -> None:
        good = self._run("legacy-order")
        record = self.service.inspect_backup(good.backup_id)
        # v1 permits metadata after payloads. Preserve that compatibility while
        # rejecting duplicate archive members without relaxing content checks.
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            with tarfile.open(record.path, "r:*") as archive:
                archive.extractall(root, filter="data")
            with tarfile.open(record.path, "w:gz") as archive:
                for path in sorted(root.rglob("*"), key=lambda item: (item.name == "manifest.json", item.as_posix())):
                    archive.add(path, arcname=path.relative_to(root).as_posix(), recursive=False)
            self.assertTrue(self.service.verify_backup(good.backup_id).verified)
            with tarfile.open(record.path, "w:gz") as archive:
                for path in sorted(root.rglob("*")):
                    archive.add(path, arcname=path.relative_to(root).as_posix(), recursive=False)
                archive.add(root / "projection.sqlite3", arcname="projection.sqlite3")
            failed = self.service.verify_backup(good.backup_id)
            self.assertFalse(failed.verified)
            self.assertIn("duplicate", failed.verification_error)

    def test_change_during_verification_cannot_be_cached_as_verified(self) -> None:
        good = self._run("changing-archive")
        record = self.service.list_backups()[0]
        from pa.backup.service import _integrity
        changed = False
        def mutate_after_extract(path, level):
            nonlocal changed
            _integrity(path, level)
            if not changed:
                changed = True
                record.path.write_bytes(b"changed while verifying")
        with patch("pa.backup.service._integrity", side_effect=mutate_after_extract):
            result = self.service.verify_backup(good.backup_id)
        self.assertFalse(result.verified)
        self.assertIn("archive_changed", result.verification_error)
        self.assertFalse(self.service.list_backups()[0].verified)

    def test_verification_preserves_path_link_file_set_and_checksum_guards(self) -> None:
        good = self._run("guard-fixture")
        record = self.service.list_backups()[0]
        with tarfile.open(record.path, "r:*") as archive:
            payloads = [
                (member, archive.extractfile(member).read() if member.isfile() else None)
                for member in archive.getmembers()
            ]
        for guard in ("traversal", "symlink", "unexpected_file", "checksum"):
            with self.subTest(guard=guard):
                with tarfile.open(record.path, "w:gz") as archive:
                    for member, payload in payloads:
                        if guard == "checksum" and member.name == "sync_refs.json":
                            payload = b"x" * len(payload)
                        archive.addfile(member, io.BytesIO(payload) if payload is not None else None)
                    if guard != "checksum":
                        extra = tarfile.TarInfo("../escape" if guard == "traversal" else "extra")
                        if guard == "symlink":
                            extra.type = tarfile.SYMTYPE
                            extra.linkname = "projection.sqlite3"
                        archive.addfile(extra)
                failed = self.service.verify_backup(good.backup_id)
                self.assertFalse(failed.verified)
                expected = "archive_unsafe" if guard in {"traversal", "symlink"} else "archive_file_set_mismatch" if guard == "unexpected_file" else "archive_checksum_mismatch"
                self.assertIn(expected, failed.verification_error)

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

    async def test_manual_io_cancellation_keeps_worker_reserved_until_finished(self) -> None:
        entered, release = threading.Event(), threading.Event()
        threads = []

        def blocked():
            threads.append(threading.current_thread().name)
            entered.set()
            release.wait(5)

        task = asyncio.create_task(self.service.run_io("backup.manual", blocked))
        try:
            self.assertTrue(await asyncio.to_thread(entered.wait, 2))
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
            with self.assertRaisesRegex(BackupError, "already running"):
                await self.service.run_io("backup.verify", lambda: None)
            self.assertIsNone(await self.service._run_maintenance("backup.scheduled", lambda: None))
            self.assertTrue(threads[0].startswith("pa-backup-io"))
        finally:
            release.set()
            for _ in range(100):
                if not self.service._maintenance_pending:
                    break
                await asyncio.sleep(0.01)
        self.assertEqual(await self.service.run_io("backup.verify", lambda: "verified"), "verified")

    async def test_manual_io_deadline_does_not_admit_overlapping_work(self) -> None:
        release = threading.Event()
        try:
            with self.assertRaisesRegex(BackupError, "still running"):
                await self.service.run_io("backup.manual", lambda: release.wait(5), timeout=0.01)
            with self.assertRaisesRegex(BackupError, "already running"):
                await self.service.run_io("backup.verify", lambda: None)
        finally:
            release.set()
            for _ in range(100):
                if not self.service._maintenance_pending:
                    break
                await asyncio.sleep(0.01)

    async def test_paused_scheduler_waits_without_startup_or_scrub_until_manual_recovery(self) -> None:
        self.service._state.consecutive_failures = self.service.config.alert_after_failures
        self.service._state.next_scrub_run = datetime.now(UTC) - timedelta(days=1)
        self.service.config.run_on_startup = True
        self.service.failure_notifier = Mock()
        with patch.object(self.service, "_run_maintenance") as maintenance:
            # No maintenance job is admitted while the persisted failure streak
            # awaits an administrator, including a previously overdue scrub.
            await self.service.start(None)
            await asyncio.sleep(0.02)
            maintenance.assert_not_called()
            self.assertFalse(self.service._scheduler_task.done())
            self.service._state.next_scrub_run = datetime.now(UTC) + timedelta(days=7)
            result = await asyncio.to_thread(self.service.run_backup, idempotency_key="recover-paused-scheduler")
            self.assertEqual(result.status, "success", result.failure_reason)
            await asyncio.sleep(0.02)
            self.assertFalse(self.service._config_event.is_set())
            self.assertFalse(self.service._scheduler_task.done())
            self.assertIsNotNone(self.service.status()["next_scheduled_run"])

    async def test_startup_minimum_age_controls_startup_run(self) -> None:
        calls: list[str] = []

        class Runtime:
            async def run_blocking(self, operation, *_args, **_kwargs):
                self.fail("backup I/O must not use the shared runtime")

        original = self.service._run_maintenance

        async def record(operation, call, **kwargs):
            calls.append(operation)
            return await original(operation, call, **kwargs)

        self.service._run_maintenance = record

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
                self.fail("backup I/O must not use the shared runtime")

        async def record(operation, _call, **_kwargs):
            calls.append(operation)

        self.service._run_maintenance = record

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
        for _ in range(50):
            if calls:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(calls, ["backup.scheduled"])
        next_run = datetime.fromisoformat(self.service.status()["next_scheduled_run"])
        self.assertGreater(next_run, datetime.now(UTC))


if __name__ == "__main__":
    unittest.main()
