from __future__ import annotations

import asyncio
import contextlib
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import sqlite3
import tarfile
import tempfile
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pa import __version__
from pa.backup.backend import LocalFilesystemBackend
from pa.backup.models import (
    BackupConfig,
    BackupManifest,
    BackupRecord,
    BackupRun,
    BackupState,
    ManifestFile,
    RestoreRequest,
    validate_backup_destination_path,
)
from pa.core.io import atomic_write_json

if TYPE_CHECKING:
    from pa.config import Settings
    from pa.domain.store import Store

logger = logging.getLogger(__name__)
_SAFE_NAME = re.compile(r"[^A-Za-z0-9._-]+")
_BACKUP_ID_IN_NAME = re.compile(
    r"-([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
    r"\.pa-backup\.(?:tgz|tar)$"
)
_CHUNK = 1024 * 1024


class BackupError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code

    def detail(self) -> dict[str, str]:
        return {"code": self.code, "message": str(self)}


def default_destination(settings: Settings) -> Path:
    return settings.data_dir.parent / f"{settings.data_dir.name}-backups"


def config_from_settings(settings: Settings) -> BackupConfig:
    destination = settings.backup_destination_dir or default_destination(settings)
    return BackupConfig(
        enabled=settings.backup_enabled,
        interval_seconds=settings.backup_interval_seconds,
        retention_count=settings.backup_retention_count,
        retention_max_age_seconds=settings.backup_retention_max_age_seconds,
        retention_max_total_bytes=settings.backup_retention_max_total_bytes,
        destination_dir=destination,
        run_on_startup=settings.backup_run_on_startup,
        startup_min_age_seconds=settings.backup_startup_min_age_seconds,
        verification_level=settings.backup_verification_level,
        compression=settings.backup_compression,
        io_limit_mib_per_second=settings.backup_io_limit_mib_per_second,
        concurrency=settings.backup_concurrency,
        alert_after_failures=settings.backup_alert_after_failures,
        jitter_seconds=settings.backup_jitter_seconds,
        scrub_interval_seconds=settings.backup_scrub_interval_seconds,
    )


def validate_destination(settings: Settings, destination: Path) -> Path:
    try:
        return validate_backup_destination_path(settings.data_dir, destination)
    except ValueError as exc:
        code = (
            "unsafe_recursive_destination"
            if "contain" in str(exc) or "inside" in str(exc)
            else "unsafe_destination"
        )
        raise BackupError(code, str(exc)) from exc


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(_CHUNK):
            digest.update(chunk)
    return digest.hexdigest()


def _backup_id_from_name(path: Path) -> str | None:
    match = _BACKUP_ID_IN_NAME.search(path.name)
    return match.group(1) if match else None


def _schema_info(path: Path) -> tuple[int, str]:
    try:
        with contextlib.closing(
            sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        ) as conn:
            version = int(conn.execute("PRAGMA user_version").fetchone()[0])
            rows = conn.execute(
                """
                SELECT type, name, tbl_name, sql
                FROM sqlite_master
                WHERE sql IS NOT NULL
                ORDER BY type, name
                """
            ).fetchall()
    except sqlite3.Error as exc:
        raise BackupError(
            "database_unreadable", f"cannot read backup schema: {exc}"
        ) from exc
    canonical = json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()
    return version, hashlib.sha256(canonical).hexdigest()


def _integrity(path: Path, level: str) -> None:
    pragma = "quick_check" if level == "quick" else "integrity_check"
    try:
        with contextlib.closing(
            sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        ) as conn:
            rows = [str(row[0]) for row in conn.execute(f"PRAGMA {pragma}").fetchall()]
    except sqlite3.Error as exc:
        raise BackupError(
            "integrity_check_failed", f"SQLite {pragma} failed: {exc}"
        ) from exc
    if rows != ["ok"]:
        raise BackupError(
            "integrity_check_failed",
            f"SQLite {pragma} reported: {'; '.join(rows[:10])}",
        )


def _projection_heads(path: Path) -> dict[str, str]:
    try:
        with contextlib.closing(
            sqlite3.connect(f"file:{path}?mode=ro", uri=True)
        ) as conn:
            rows = conn.execute(
                "SELECT realm_id, head_hash FROM sync_projection_heads ORDER BY realm_id"
            ).fetchall()
    except sqlite3.Error as exc:
        raise BackupError(
            "projection_heads_unreadable",
            f"cannot read projection checkpoints: {exc}",
        ) from exc
    return {str(realm): str(head) for realm, head in rows}


def _verify_event_graph(objects_root: Path, refs: dict[str, str]) -> None:
    """Prove every durable ref has a readable commit/event ancestry."""

    pending = list(refs.values())
    seen: set[str] = set()
    while pending:
        object_id = pending.pop()
        if object_id in seen:
            continue
        if not re.fullmatch(r"[0-9a-f]{64}", object_id):
            raise BackupError(
                "sync_ref_invalid", "durable ref contains an invalid hash"
            )
        seen.add(object_id)
        path = objects_root / object_id[:2] / object_id[2:]
        if not path.is_file():
            raise BackupError(
                "event_graph_incomplete",
                f"durable event graph is missing object {object_id[:12]}",
            )
        try:
            value = json.loads(path.read_text())
        except (OSError, ValueError) as exc:
            raise BackupError(
                "event_object_unreadable",
                f"event-log object {object_id[:12]} is unreadable",
            ) from exc
        if not isinstance(value, dict) or not {
            "parent_hashes",
            "event_hashes",
        }.issubset(value):
            raise BackupError(
                "event_commit_invalid",
                f"durable head object {object_id[:12]} is not a commit",
            )
        linked = [*value["parent_hashes"], *value["event_hashes"]]
        for linked_id in linked:
            if not isinstance(linked_id, str) or not re.fullmatch(
                r"[0-9a-f]{64}", linked_id
            ):
                raise BackupError(
                    "event_commit_invalid",
                    f"commit {object_id[:12]} contains an invalid object link",
                )
            linked_path = objects_root / linked_id[:2] / linked_id[2:]
            if not linked_path.is_file():
                raise BackupError(
                    "event_graph_incomplete",
                    f"commit {object_id[:12]} references missing object "
                    f"{linked_id[:12]}",
                )
        pending.extend(
            linked_id for linked_id in value["parent_hashes"] if linked_id not in seen
        )


def _verify_transcript_store(database: Path, objects_root: Path) -> None:
    if not database.exists():
        return
    with contextlib.closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as conn:
        refs = conn.execute("SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL").fetchall()
    for (digest,) in refs:
        path = objects_root / str(digest)[:2] / f"{str(digest)[2:]}.zlib"
        if not path.is_file():
            raise BackupError("transcript_object_missing", f"cold transcript object {str(digest)[:12]} is missing")
        try:
            raw = zlib.decompress(path.read_bytes())
        except (OSError, zlib.error) as exc:
            raise BackupError("transcript_object_corrupt", f"cold transcript object {str(digest)[:12]} is unreadable") from exc
        if hashlib.sha256(raw).hexdigest() != digest:
            raise BackupError("transcript_object_corrupt", f"cold transcript object {str(digest)[:12]} failed content hash")


class BackupService:
    def __init__(self, settings: Settings, store: Store | None) -> None:
        self.settings = settings
        self.store = store
        self.config = config_from_settings(settings)
        validate_destination(settings, self.config.destination_dir)
        self.backend = LocalFilesystemBackend(self.config.destination_dir)
        self.state_path = settings.data_dir / "backup_state.json"
        self.audit_path = settings.data_dir / "backup_audit.jsonl"
        self._run_lock = threading.Lock()
        self._state_lock = threading.RLock()
        self._stop_event: asyncio.Event | None = None
        self._config_event: asyncio.Event | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._scheduler_task: asyncio.Task | None = None
        # Backup I/O must never occupy the shared request/sync worker pool.
        self._maintenance_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="pa-backup-io"
        )
        self._maintenance_pending = False
        self._state = self._load_state()

    def _load_state(self) -> BackupState:
        if not self.state_path.exists():
            return BackupState()
        try:
            state = BackupState.model_validate_json(self.state_path.read_text())
            interrupted = False
            for run in state.runs:
                if run.status == "running":
                    run.status = "failed"
                    run.verification = "failed"
                    run.failure_reason = "interrupted_by_restart"
                    run.finished_at = datetime.now(UTC)
                    run.duration_seconds = max(
                        0.0,
                        (run.finished_at - run.started_at).total_seconds(),
                    )
                    state.consecutive_failures += 1
                    interrupted = True
            if interrupted:
                atomic_write_json(
                    self.state_path, state.model_dump(mode="json"), mode=0o600
                )
            return state
        except OSError, ValueError:
            logger.exception("backup.state_load_failed")
            return BackupState()

    def _save_state(self) -> None:
        with self._state_lock:
            self._state.runs = self._state.runs[-100:]
            self._state.restores = self._state.restores[-100:]
            retained_run_ids = {run.id for run in self._state.runs}
            self._state.idempotency = {
                key: run_id
                for key, run_id in self._state.idempotency.items()
                if run_id in retained_run_ids
            }
            atomic_write_json(
                self.state_path,
                self._state.model_dump(mode="json"),
                mode=0o600,
            )

    def _audit(self, event: str, **fields: Any) -> None:
        record = {
            "event": event,
            "timestamp": datetime.now(UTC).isoformat(),
            "instance_id": self.settings.instance_id,
            **fields,
        }
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with os.fdopen(
            os.open(
                self.audit_path,
                os.O_WRONLY | os.O_APPEND | os.O_CREAT,
                0o600,
            ),
            "a",
            encoding="utf-8",
        ) as handle:
            os.fchmod(handle.fileno(), 0o600)
            handle.write(json.dumps(record, sort_keys=True, default=str) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        logger.info("backup.event %s", json.dumps(record, sort_keys=True, default=str))

    def _metric(self, key: str, amount: float = 1) -> None:
        with self._state_lock:
            self._state.metrics[key] = self._state.metrics.get(key, 0) + amount

    def _ensure_default_destination(self) -> None:
        if self.config.destination_dir == default_destination(self.settings).resolve():
            self.config.destination_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
            os.chmod(self.config.destination_dir, 0o700)

    def _health_or_raise(self) -> dict:
        health = self.backend.health()
        if not health["exists"]:
            raise BackupError("destination_missing", str(health["error"]))
        if not health["writable"]:
            raise BackupError("destination_unhealthy", str(health["error"]))
        free_bytes = health.get("free_bytes")
        if free_bytes is not None and free_bytes < 100 * 1024 * 1024:
            self._metric("backup_destination_pressure_total")
            self._audit(
                "backup_destination_pressure",
                free_bytes=free_bytes,
                destination_backend=health["backend"],
            )
        return health

    def _acquire_destination_lock(self) -> int:
        path = self.config.destination_dir / ".pa-backup.lock"
        fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        os.fchmod(fd, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            raise BackupError(
                "backup_overlap_process",
                "another process is already backing up this destination",
            ) from None
        return fd

    def _copy_file(self, source: Path, target: Path) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        copied = 0
        with source.open("rb") as inp, target.open("xb") as out:
            while chunk := inp.read(_CHUNK):
                out.write(chunk)
                copied += len(chunk)
                limit = self.config.io_limit_mib_per_second
                if limit:
                    expected = copied / (limit * 1024 * 1024)
                    delay = expected - (time.monotonic() - started)
                    if delay > 0:
                        time.sleep(min(delay, 1.0))
            out.flush()
            os.fsync(out.fileno())
        os.chmod(target, 0o600)

    def _online_sqlite_backup(self, target: Path, *, source_path: Path | None = None) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with (
                contextlib.closing(
                    sqlite3.connect(source_path or self.settings.db_path, timeout=30)
                ) as source,
                contextlib.closing(sqlite3.connect(target)) as destination,
            ):
                source.execute("PRAGMA busy_timeout=30000")
                source.backup(destination, pages=256, sleep=0.01)
                # The snapshot is a standalone database. Normalize it away
                # from WAL mode so no transient sidecars are created or needed.
                destination.execute("PRAGMA journal_mode=DELETE")
                destination.commit()
        except sqlite3.Error as exc:
            raise BackupError(
                "online_snapshot_failed", f"SQLite online backup failed: {exc}"
            ) from exc
        os.chmod(target, 0o600)

    def _snapshot(self, staging: Path) -> BackupManifest:
        lock = (
            self.store.mutation()
            if self.store is not None
            else contextlib.nullcontext()
        )
        transcript_lock = (
            self.store.transcripts._lock
            if self.store is not None and hasattr(self.store, "transcripts")
            else contextlib.nullcontext()
        )
        with lock, transcript_lock:
            projection = staging / "projection.sqlite3"
            self._online_sqlite_backup(projection)
            _integrity(projection, self.config.verification_level)
            transcript_source = self.settings.db_path.with_name(
                f"{self.settings.db_path.stem}.transcripts.db"
            )
            if transcript_source.exists():
                transcript_target = staging / "transcripts.sqlite3"
                self._online_sqlite_backup(transcript_target, source_path=transcript_source)
                _integrity(transcript_target, self.config.verification_level)
                cold_target = staging / "transcript_objects"
                cold_target.mkdir(mode=0o700)
                cold_source = self.settings.data_dir / "transcript_objects"
                if cold_source.exists():
                    for source in sorted(cold_source.glob("*/*.zlib")):
                        self._copy_file(source, cold_target / source.relative_to(cold_source))

            refs: dict[str, str] = {}
            if self.store is not None and self.store.event_log is not None:
                for ref in self.store.event_log.list_refs():
                    refs[f"{ref.realm_id}/{ref.instance_id}"] = ref.head_hash
            else:
                refs_path = self.settings.data_dir / "sync_refs.json"
                if refs_path.exists():
                    try:
                        refs = {
                            str(key): str(value)
                            for key, value in json.loads(refs_path.read_text()).items()
                        }
                    except (OSError, ValueError) as exc:
                        raise BackupError(
                            "sync_refs_unreadable", f"cannot read sync refs: {exc}"
                        ) from exc
            atomic_write_json(staging / "sync_refs.json", refs, mode=0o600)

            objects_target = staging / "objects"
            objects_target.mkdir(mode=0o700)
            if self.settings.objects_dir.exists():
                for source in sorted(self.settings.objects_dir.rglob("*")):
                    if not source.is_file():
                        continue
                    relative = source.relative_to(self.settings.objects_dir)
                    object_id = "".join(relative.parts)
                    if not re.fullmatch(r"[0-9a-f]{64}", object_id):
                        continue
                    if _sha256(source) != object_id:
                        raise BackupError(
                            "event_object_corrupt",
                            f"event-log object {object_id[:12]} failed its content hash",
                        )
                    self._copy_file(source, objects_target / relative)

        durable = {
            key.split("/", 1)[0]: head
            for key, head in refs.items()
            if "/" in key and key.split("/", 1)[1] == self.settings.instance_id
        }
        projected = _projection_heads(projection)
        consistent = projected == durable
        if not consistent:
            mismatched = sorted(set(projected) | set(durable))
            raise BackupError(
                "projection_sync_mismatch",
                "projection checkpoints differ from durable refs for "
                + ", ".join(mismatched)
                + "; run PA sync reconciliation before retrying",
            )
        schema_version, fingerprint = _schema_info(projection)
        manifest = BackupManifest(
            instance_id=self.settings.instance_id,
            instance_name=self.settings.instance_name,
            pa_version=__version__,
            projection_schema_version=schema_version,
            projection_schema_fingerprint=fingerprint,
            verification_level=self.config.verification_level,
            compressed=self.config.compression,
            durable_heads=durable,
            projection_heads=projected,
            consistent=consistent,
        )
        files: list[ManifestFile] = []
        for path in sorted(staging.rglob("*")):
            if path.is_file():
                files.append(
                    ManifestFile(
                        path=path.relative_to(staging).as_posix(),
                        size_bytes=path.stat().st_size,
                        sha256=_sha256(path),
                    )
                )
        manifest.files = files
        atomic_write_json(
            staging / "manifest.json",
            manifest.model_dump(mode="json"),
            mode=0o600,
        )
        return manifest

    def _archive(
        self,
        staging: Path,
        manifest: BackupManifest,
        phase_seconds: dict[str, float] | None = None,
    ) -> Path:
        suffix = ".pa-backup.tgz" if self.config.compression else ".pa-backup.tar"
        stamp = manifest.created_at.astimezone(UTC).strftime("%Y%m%dT%H%M%S.%fZ")
        instance = _SAFE_NAME.sub("-", self.settings.instance_id).strip("-")[:64]
        name = (
            f"{instance}-{stamp}-schema{manifest.projection_schema_version}-"
            f"{manifest.backup_id}{suffix}"
        )
        fd, temporary_name = tempfile.mkstemp(
            prefix=".pa-backup-", suffix=".tmp", dir=self.config.destination_dir
        )
        os.close(fd)
        temporary = Path(temporary_name)
        try:
            archive_started = time.monotonic()
            mode = "w:gz" if self.config.compression else "w"
            with tarfile.open(temporary, mode, format=tarfile.PAX_FORMAT) as archive:
                for path in sorted(staging.rglob("*")):
                    archive.add(
                        path,
                        arcname=path.relative_to(staging).as_posix(),
                        recursive=False,
                    )
            os.chmod(temporary, 0o600)
            if phase_seconds is not None:
                phase_seconds["archive"] = round(
                    time.monotonic() - archive_started, 6
                )
            verify_started = time.monotonic()
            verification = self.verify_backup(manifest.backup_id, path=temporary)
            if phase_seconds is not None:
                phase_seconds["verify"] = round(
                    time.monotonic() - verify_started, 6
                )
            if not verification.verified:
                raise BackupError(
                    "temporary_verification_failed",
                    verification.verification_error
                    or "temporary backup archive did not verify",
                )
            publish_started = time.monotonic()
            published = self.backend.publish(temporary, name)
            if phase_seconds is not None:
                phase_seconds["publish"] = round(
                    time.monotonic() - publish_started, 6
                )
            return published
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise

    def run_backup(
        self,
        *,
        trigger: str = "manual",
        idempotency_key: str | None = None,
        protected_backup_ids: set[str] | None = None,
        queue_wait_seconds: float = 0,
    ) -> BackupRun:
        if idempotency_key:
            with self._state_lock:
                existing_id = self._state.idempotency.get(idempotency_key)
                existing = next(
                    (run for run in self._state.runs if run.id == existing_id), None
                )
                if existing:
                    return existing
        if not self._run_lock.acquire(blocking=False):
            run = BackupRun(
                trigger=trigger,
                status="skipped",
                finished_at=datetime.now(UTC),
                failure_reason="another backup is already running",
                idempotency_key=idempotency_key,
            )
            run.duration_seconds = 0
            self._record_finished(run)
            self._audit("backup_skipped_overlap", run_id=run.id, trigger=trigger)
            return run
        started = time.monotonic()
        run = BackupRun(trigger=trigger, idempotency_key=idempotency_key)
        run.queue_wait_seconds = round(queue_wait_seconds, 6)
        with self._state_lock:
            self._state.runs.append(run)
            self._state.last_attempt = run.started_at
            if idempotency_key:
                self._state.idempotency[idempotency_key] = run.id
            self._save_state()
        self._audit("backup_started", run_id=run.id, trigger=trigger)
        self._metric("backup_started_total")
        staging_path: Path | None = None
        destination_lock_fd: int | None = None
        try:
            self._ensure_default_destination()
            self._health_or_raise()
            destination_lock_fd = self._acquire_destination_lock()
            staging_path = Path(
                tempfile.mkdtemp(
                    prefix=".pa-backup-stage-", dir=self.config.destination_dir
                )
            )
            os.chmod(staging_path, 0o700)
            phase_started = time.monotonic()
            manifest = self._snapshot(staging_path)
            run.phase_seconds["snapshot"] = round(time.monotonic() - phase_started, 6)
            archive = self._archive(staging_path, manifest, run.phase_seconds)
            # _archive fully verifies the temporary artifact before backend.publish
            # atomically renames those exact immutable bytes into visibility.
            run.status = "success"
            run.backup_id = manifest.backup_id
            run.verification = "verified"
            run.size_bytes = archive.stat().st_size
            phase_started = time.monotonic()
            run.pruned_backup_ids = self.prune(
                protected_backup_ids=protected_backup_ids
            )
            run.phase_seconds["prune"] = round(time.monotonic() - phase_started, 6)
            with self._state_lock:
                self._state.last_success = datetime.now(UTC)
                self._state.consecutive_failures = 0
            self._metric("backup_success_total")
            self._metric("backup_bytes_total", run.size_bytes)
            self._audit(
                "backup_succeeded",
                run_id=run.id,
                backup_id=run.backup_id,
                size_bytes=run.size_bytes,
                pruned_backup_ids=run.pruned_backup_ids,
                queue_wait_seconds=run.queue_wait_seconds,
                phase_seconds=run.phase_seconds,
            )
        except BackupError as exc:
            if exc.code == "backup_overlap_process":
                run.status = "skipped"
                run.failure_reason = str(exc)
                self._metric("backup_overlap_process_total")
                self._audit(
                    "backup_skipped_overlap",
                    run_id=run.id,
                    trigger=trigger,
                    scope="destination",
                )
            else:
                run.status = "failed"
                run.verification = "failed"
                run.failure_reason = f"{exc.code}: {exc}"
                with self._state_lock:
                    self._state.consecutive_failures += 1
                self._metric("backup_failure_total")
                self._audit(
                    "backup_failed",
                    run_id=run.id,
                    reason=run.failure_reason,
                    consecutive_failures=self._state.consecutive_failures,
                )
        except Exception as exc:
            run.status = "failed"
            run.verification = "failed"
            if isinstance(exc, OSError):
                run.failure_reason = f"{type(exc).__name__}: {exc}"
            else:
                run.failure_reason = f"backup_failed: {type(exc).__name__}"
                logger.exception("backup.failed")
            with self._state_lock:
                self._state.consecutive_failures += 1
            self._metric("backup_failure_total")
            self._audit(
                "backup_failed",
                run_id=run.id,
                reason=run.failure_reason,
                consecutive_failures=self._state.consecutive_failures,
            )
        finally:
            if staging_path is not None:
                shutil.rmtree(staging_path, ignore_errors=True)
            if destination_lock_fd is not None:
                fcntl.flock(destination_lock_fd, fcntl.LOCK_UN)
                os.close(destination_lock_fd)
            run.finished_at = datetime.now(UTC)
            run.duration_seconds = round(time.monotonic() - started, 6)
            self._record_finished(run)
            self._run_lock.release()
        return run

    def _record_finished(self, run: BackupRun) -> None:
        with self._state_lock:
            existing = next(
                (i for i, item in enumerate(self._state.runs) if item.id == run.id),
                None,
            )
            if existing is None:
                self._state.runs.append(run)
            else:
                self._state.runs[existing] = run
            if run.idempotency_key:
                self._state.idempotency[run.idempotency_key] = run.id
            self._save_state()

    def _read_manifest(self, path: Path) -> BackupManifest:
        try:
            with tarfile.open(path, "r:*") as archive:
                member = archive.getmember("manifest.json")
                if not member.isfile() or member.size > 2 * 1024 * 1024:
                    raise BackupError(
                        "manifest_invalid", "manifest is missing or too large"
                    )
                handle = archive.extractfile(member)
                if handle is None:
                    raise BackupError("manifest_invalid", "manifest is unreadable")
                return BackupManifest.model_validate_json(handle.read())
        except (tarfile.TarError, KeyError, ValueError, OSError) as exc:
            if isinstance(exc, BackupError):
                raise
            raise BackupError(
                "archive_unreadable", f"cannot read backup archive: {exc}"
            ) from exc

    def _path_for_backup(self, backup_id: str) -> Path:
        matches: list[Path] = []
        for path in self.backend.list_archives():
            try:
                manifest = self._read_manifest(path)
            except BackupError:
                if _backup_id_from_name(path) == backup_id:
                    matches.append(path)
                continue
            if manifest.backup_id == backup_id:
                matches.append(path)
        if not matches:
            raise BackupError("backup_not_found", f"backup {backup_id} was not found")
        if len(matches) > 1:
            raise BackupError("backup_collision", f"backup {backup_id} is not unique")
        return matches[0]

    @staticmethod
    def _validate_member(member: tarfile.TarInfo) -> None:
        pure = PurePosixPath(member.name)
        if pure.is_absolute() or ".." in pure.parts or member.issym() or member.islnk():
            raise BackupError("archive_unsafe", f"unsafe archive member: {member.name}")
        if not (member.isfile() or member.isdir()):
            raise BackupError(
                "archive_unsafe", f"unsupported archive member: {member.name}"
            )

    def verify_backup(
        self, backup_id: str, *, path: Path | None = None
    ) -> BackupRecord:
        path = path or self._path_for_backup(backup_id)
        manifest: BackupManifest | None = None
        try:
            manifest = self._read_manifest(path)
            if manifest.backup_id != backup_id:
                raise BackupError(
                    "backup_id_mismatch", "archive manifest ID does not match"
                )
            if not manifest.consistent:
                raise BackupError(
                    "projection_sync_mismatch",
                    "backup manifest records inconsistent projection and durable heads",
                )
            expected = {item.path: item for item in manifest.files}
            with tempfile.TemporaryDirectory(prefix="pa-backup-verify-") as tmp:
                root = Path(tmp)
                with tarfile.open(path, "r:*") as archive:
                    members = archive.getmembers()
                    for member in members:
                        self._validate_member(member)
                    names = {m.name for m in members if m.isfile()}
                    if names != set(expected) | {"manifest.json"}:
                        raise BackupError(
                            "archive_file_set_mismatch",
                            "archive file set differs from the signed manifest",
                        )
                    archive.extractall(root, members=members, filter="data")
                for relative, item in expected.items():
                    candidate = root / relative
                    if (
                        candidate.stat().st_size != item.size_bytes
                        or _sha256(candidate) != item.sha256
                    ):
                        raise BackupError(
                            "archive_checksum_mismatch",
                            f"backup member {relative} failed checksum verification",
                        )
                    parts = PurePosixPath(relative).parts
                    if parts and parts[0] == "objects":
                        object_id = "".join(parts[1:])
                        if (
                            not re.fullmatch(r"[0-9a-f]{64}", object_id)
                            or item.sha256 != object_id
                        ):
                            raise BackupError(
                                "event_object_corrupt",
                                f"event-log object path {relative} does not match its hash",
                            )
                    if parts and parts[0] == "transcript_objects":
                        object_id = parts[1] + Path(parts[2]).stem if len(parts) == 3 else ""
                        try:
                            raw = zlib.decompress(candidate.read_bytes())
                        except (OSError, zlib.error) as exc:
                            raise BackupError("transcript_object_corrupt", f"cold transcript object {relative} is unreadable") from exc
                        if hashlib.sha256(raw).hexdigest() != object_id:
                            raise BackupError("transcript_object_corrupt", f"cold transcript object {relative} failed content hash")
                projection = root / "projection.sqlite3"
                _integrity(projection, manifest.verification_level)
                transcript_projection = root / "transcripts.sqlite3"
                if transcript_projection.exists():
                    _integrity(transcript_projection, manifest.verification_level)
                    _verify_transcript_store(transcript_projection, root / "transcript_objects")
                version, fingerprint = _schema_info(projection)
                if (
                    version != manifest.projection_schema_version
                    or fingerprint != manifest.projection_schema_fingerprint
                ):
                    raise BackupError(
                        "schema_manifest_mismatch",
                        "readable projection schema differs from the manifest",
                    )
                projected = _projection_heads(projection)
                if projected != manifest.projection_heads:
                    raise BackupError(
                        "projection_manifest_mismatch",
                        "projection checkpoints differ from the manifest",
                    )
                refs = json.loads((root / "sync_refs.json").read_text())
                durable = {
                    key.split("/", 1)[0]: head
                    for key, head in refs.items()
                    if "/" in key and key.split("/", 1)[1] == manifest.instance_id
                }
                if durable != manifest.durable_heads:
                    raise BackupError(
                        "sync_refs_manifest_mismatch",
                        "durable refs differ from the manifest",
                    )
                _verify_event_graph(root / "objects", refs)
            self._metric("backup_verification_success_total")
            with self._state_lock:
                self._state.verifications[backup_id] = {
                    "verified": True,
                    "verified_at": datetime.now(UTC).isoformat(),
                    "error": None,
                }
                self._save_state()
            self._audit("backup_verified", backup_id=backup_id)
            return BackupRecord(
                backup_id=backup_id,
                path=path,
                created_at=manifest.created_at,
                size_bytes=path.stat().st_size,
                verified=True,
                manifest=manifest,
            )
        except Exception as exc:  # noqa: BLE001 - restore must always roll back
            reason = (
                f"{exc.code}: {exc}"
                if isinstance(exc, BackupError)
                else f"{type(exc).__name__}: {exc}"
            )
            self._metric("backup_verification_failure_total")
            with self._state_lock:
                self._state.verifications[backup_id] = {
                    "verified": False,
                    "verified_at": datetime.now(UTC).isoformat(),
                    "error": reason,
                }
                self._save_state()
            self._audit(
                "backup_verification_failed", backup_id=backup_id, reason=reason
            )
            return BackupRecord(
                backup_id=backup_id,
                path=path,
                created_at=manifest.created_at
                if manifest
                else datetime.fromtimestamp(path.stat().st_mtime, UTC),
                size_bytes=path.stat().st_size,
                verified=False,
                verification_error=reason,
                manifest=manifest,
            )

    def list_backups(self, *, verify: bool = False) -> list[BackupRecord]:
        records: list[BackupRecord] = []
        for path in self.backend.list_archives():
            try:
                manifest = self._read_manifest(path)
                if verify:
                    record = self.verify_backup(manifest.backup_id, path=path)
                else:
                    cached = self._state.verifications.get(manifest.backup_id, {})
                    record = BackupRecord(
                        backup_id=manifest.backup_id,
                        path=path,
                        created_at=manifest.created_at,
                        size_bytes=path.stat().st_size,
                        verified=bool(cached.get("verified")),
                        verification_error=cached.get("error")
                        or (
                            None
                            if cached.get("verified")
                            else "verification has not been recorded"
                        ),
                        manifest=manifest,
                    )
            except BackupError as exc:
                record = BackupRecord(
                    backup_id=_backup_id_from_name(path) or f"unreadable:{path.name}",
                    path=path,
                    created_at=datetime.fromtimestamp(path.stat().st_mtime, UTC),
                    size_bytes=path.stat().st_size,
                    verified=False,
                    verification_error=f"{exc.code}: {exc}",
                )
            records.append(record)
        return sorted(
            records, key=lambda item: (item.created_at, item.backup_id), reverse=True
        )

    def prune(self, *, protected_backup_ids: set[str] | None = None) -> list[str]:
        protected_backup_ids = protected_backup_ids or set()
        # Every published archive is fully verified before it is recorded, and
        # restore/delete paths perform their own fresh verification.  Retention
        # only needs that durable verification record; re-extracting every
        # retained archive here makes each scheduled backup scale with the
        # entire backup history (and can saturate a worker for minutes).
        records = [item for item in self.list_backups() if item.verified]
        records.sort(key=lambda item: (item.created_at, item.backup_id))
        if len(records) <= 1:
            return []
        now = datetime.now(UTC)
        doomed: set[str] = set()
        while len(records) - len(doomed) > self.config.retention_count:
            candidate = next(
                (
                    item
                    for item in records
                    if item.backup_id not in doomed
                    and item.backup_id not in protected_backup_ids
                ),
                None,
            )
            if candidate is None:
                break
            doomed.add(candidate.backup_id)
        if self.config.retention_max_age_seconds is not None:
            cutoff = now - timedelta(seconds=self.config.retention_max_age_seconds)
            for item in records:
                if (
                    item.backup_id not in protected_backup_ids
                    and item.created_at < cutoff
                    and len(records) - len(doomed) > 1
                ):
                    doomed.add(item.backup_id)
        if self.config.retention_max_total_bytes is not None:
            total = sum(
                item.size_bytes for item in records if item.backup_id not in doomed
            )
            for item in records:
                if total <= self.config.retention_max_total_bytes:
                    break
                if (
                    item.backup_id in doomed
                    or item.backup_id in protected_backup_ids
                    or len(records) - len(doomed) <= 1
                ):
                    continue
                doomed.add(item.backup_id)
                total -= item.size_bytes
        deleted: list[str] = []
        for item in records:
            if item.backup_id not in doomed:
                continue
            self.backend.delete(item.path)
            deleted.append(item.backup_id)
            with self._state_lock:
                self._state.verifications.pop(item.backup_id, None)
            self._metric("backup_pruned_total")
            self._audit(
                "backup_pruned",
                backup_id=item.backup_id,
                reason="deterministic retention policy",
            )
        if deleted:
            self._save_state()
        return deleted

    def deep_scrub(self) -> dict[str, bool]:
        """Freshly verify every retained archive outside the normal backup path."""
        started = time.monotonic()
        results: dict[str, bool] = {}
        self._audit("backup_scrub_started")
        for record in self.list_backups():
            results[record.backup_id] = self.verify_backup(
                record.backup_id, path=record.path
            ).verified
        elapsed = round(time.monotonic() - started, 6)
        with self._state_lock:
            self._state.last_scrub = datetime.now(UTC)
            self._state.next_scrub_run = self._state.last_scrub + timedelta(
                seconds=self.config.scrub_interval_seconds
            )
            self._state.last_scrub_results = results
            self._save_state()
        self._metric("backup_scrub_total")
        self._metric("backup_scrub_seconds_total", elapsed)
        self._audit(
            "backup_scrub_finished",
            duration_seconds=elapsed,
            checked=len(results),
            corrupt=sum(not verified for verified in results.values()),
        )
        return results

    async def _run_maintenance(self, operation: str, call: Any, **kwargs: Any) -> Any:
        """Submit at most one maintenance job to PA's isolated I/O lane."""
        if self._maintenance_pending:
            self._metric("backup_maintenance_coalesced_total")
            self._audit("backup_maintenance_coalesced", operation=operation)
            return None
        self._maintenance_pending = True
        queued_at = time.monotonic()

        def invoke() -> Any:
            wait = time.monotonic() - queued_at
            if call == self.run_backup:
                kwargs["queue_wait_seconds"] = wait
            self._metric(f"{operation}_queue_wait_seconds_total", wait)
            return call(**kwargs)

        try:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(self._maintenance_executor, invoke)
        finally:
            self._maintenance_pending = False

    def delete_backup(self, backup_id: str) -> None:
        target_path = self._path_for_backup(backup_id)
        target = self.verify_backup(backup_id, path=target_path)
        verified = [item for item in self.list_backups(verify=True) if item.verified]
        remaining_verified = [item for item in verified if item.backup_id != backup_id]
        if not remaining_verified:
            raise BackupError(
                "last_known_good",
                "refusing deletion because no other backup can remain as the "
                "last known-good recovery point",
            )
        self.backend.delete(target_path)
        with self._state_lock:
            self._state.verifications.pop(backup_id, None)
            self._save_state()
        self._metric("backup_deleted_total")
        self._audit(
            "backup_deleted",
            backup_id=backup_id,
            verified=target.verified,
            reason="explicit operator request",
        )

    def inspect_backup(self, backup_id: str) -> BackupRecord:
        return self.verify_backup(backup_id)

    def download_path(self, backup_id: str) -> Path:
        record = self.verify_backup(backup_id)
        if not record.verified:
            raise BackupError("backup_not_verified", "backup is not safe to export")
        return record.path

    def status(self) -> dict[str, Any]:
        health = self.backend.health()
        backups = self.list_backups()
        used = sum(item.size_bytes for item in backups)
        with self._state_lock:
            state = self._state.model_dump(mode="json")
        sources = self._config_sources()
        return {
            "instance_id": self.settings.instance_id,
            "configured": self.config.model_dump(mode="json"),
            "effective": self.config.model_dump(mode="json"),
            "sources": sources,
            "destination_health": health,
            "storage_used_bytes": used,
            "backup_count": len(backups),
            "verified_backup_count": sum(1 for item in backups if item.verified),
            "alerting": {
                "active": self._state.consecutive_failures
                >= self.config.alert_after_failures,
                "threshold": self.config.alert_after_failures,
                "consecutive_failures": self._state.consecutive_failures,
            },
            **state,
        }

    def _config_sources(self) -> dict[str, str]:
        persisted: dict[str, Any] = {}
        path = self.settings.data_dir / "config.json"
        try:
            persisted = json.loads(path.read_text())
        except OSError, ValueError:
            pass
        result: dict[str, str] = {}
        for field in BackupConfig.model_fields:
            settings_name = "backup_" + field
            env_name = "PA_" + settings_name.upper()
            if settings_name in persisted:
                result[field] = "config.json"
            elif env_name in os.environ:
                result[field] = f"environment:{env_name}"
            else:
                result[field] = "default"
        return result

    def apply_config(self, config: BackupConfig) -> None:
        validate_destination(self.settings, config.destination_dir)
        self.config = config
        self.backend = LocalFilesystemBackend(config.destination_dir)
        if config.enabled:
            self._schedule_next()
        else:
            with self._state_lock:
                self._state.next_scheduled_run = None
                self._save_state()
        if self._loop and self._config_event:
            self._loop.call_soon_threadsafe(self._config_event.set)
        self._audit("backup_configuration_updated")

    def initiate_restore(
        self, backup_id: str, *, requested_by: str, confirm_instance_id: str
    ) -> RestoreRequest:
        record = self.verify_backup(backup_id)
        if not record.verified or not record.manifest:
            raise BackupError(
                "backup_not_verified", "backup must verify before restore"
            )
        if record.manifest.instance_id != self.settings.instance_id:
            raise BackupError(
                "instance_mismatch",
                "backup was created by a different PA instance",
            )
        if confirm_instance_id != self.settings.instance_id:
            raise BackupError(
                "confirmation_mismatch",
                "instance confirmation does not match this writer",
            )
        current_version, current_fingerprint = _schema_info(self.settings.db_path)
        if (
            current_version != record.manifest.projection_schema_version
            or current_fingerprint != record.manifest.projection_schema_fingerprint
        ):
            raise BackupError(
                "schema_incompatible",
                "backup schema is incompatible with the installed projection schema",
            )
        request = RestoreRequest(
            backup_id=backup_id,
            instance_id=self.settings.instance_id,
            requested_by=requested_by,
            instructions=[
                "Stop the PA service and verify it is stopped.",
                f"Run: pa backup restore {backup_id} --request-id {uuid4()}",
                "Start PA and inspect `pa sync status` for every subscribed realm.",
                "If heads differ, run the supported `pa sync reconcile` workflow.",
            ],
        )
        request.instructions[1] = (
            f"Run: pa backup restore {backup_id} --request-id {request.id}"
        )
        with self._state_lock:
            self._state.restores.append(request)
            self._save_state()
        self._metric("restore_requested_total")
        self._audit(
            "restore_requested",
            restore_id=request.id,
            backup_id=backup_id,
            requested_by=requested_by,
        )
        return request

    def get_restore(self, restore_id: str) -> RestoreRequest:
        with self._state_lock:
            request = next(
                (item for item in self._state.restores if item.id == restore_id), None
            )
        if request is None:
            raise BackupError(
                "restore_not_found", f"restore {restore_id} was not found"
            )
        return request

    def _set_restore(self, request: RestoreRequest) -> None:
        with self._state_lock:
            index = next(
                (
                    i
                    for i, item in enumerate(self._state.restores)
                    if item.id == request.id
                ),
                None,
            )
            if index is None:
                self._state.restores.append(request)
            else:
                self._state.restores[index] = request
            self._save_state()

    def restore_offline(
        self, backup_id: str, *, request_id: str | None = None
    ) -> RestoreRequest:
        request: RestoreRequest
        if request_id:
            request = self.get_restore(request_id)
            if request.backup_id != backup_id:
                raise BackupError(
                    "restore_request_mismatch", "restore request targets another backup"
                )
        else:
            request = RestoreRequest(
                backup_id=backup_id,
                instance_id=self.settings.instance_id,
                requested_by="cli:offline",
            )
        try:
            record = self.verify_backup(backup_id)
            if not record.verified or not record.manifest:
                raise BackupError(
                    "backup_not_verified", "backup must verify before restore"
                )
            if record.manifest.instance_id != self.settings.instance_id:
                raise BackupError(
                    "instance_mismatch", "backup belongs to another PA instance"
                )
            current_version, current_fingerprint = _schema_info(self.settings.db_path)
            if current_version != record.manifest.projection_schema_version:
                raise BackupError(
                    "schema_incompatible",
                    "backup projection schema version is incompatible with this instance",
                )
            # A fingerprint mismatch at the same explicit schema version means the
            # application has no declared migration contract for this archive.
            if current_fingerprint != record.manifest.projection_schema_fingerprint:
                raise BackupError(
                    "schema_incompatible",
                    "backup schema fingerprint differs from the installed projection schema",
                )
        except BackupError as exc:
            request.status = "failed"
            request.failure_reason = f"{exc.code}: {exc}"
            request.finished_at = datetime.now(UTC)
            request.instructions = [
                "Live PA state was not changed.",
                "Inspect the backup manifest and installed schema before retrying.",
                "Keep the writer stopped if compatibility remains uncertain.",
            ]
            self._metric("restore_failure_total")
            self._set_restore(request)
            self._audit(
                "restore_failed",
                restore_id=request.id,
                backup_id=backup_id,
                reason=request.failure_reason,
                live_state_changed=False,
            )
            return request
        request.status = "running"
        self._set_restore(request)
        self._audit("restore_started", restore_id=request.id, backup_id=backup_id)
        pre = self.run_backup(
            trigger="pre_restore",
            idempotency_key=f"pre-restore:{request.id}",
            protected_backup_ids={backup_id},
        )
        if pre.status != "success" or not pre.backup_id:
            request.status = "failed"
            request.failure_reason = (
                "pre-restore backup failed; live state was not changed"
            )
            request.finished_at = datetime.now(UTC)
            self._set_restore(request)
            self._audit(
                "restore_failed",
                restore_id=request.id,
                reason=request.failure_reason,
            )
            return request
        request.pre_restore_backup_id = pre.backup_id

        archive = record.path
        rollback_root = Path(
            tempfile.mkdtemp(
                prefix=".pa-restore-rollback-", dir=self.settings.data_dir.parent
            )
        )
        extract_root = Path(
            tempfile.mkdtemp(
                prefix=".pa-restore-stage-", dir=self.settings.data_dir.parent
            )
        )
        moved: list[tuple[Path, Path]] = []
        try:
            with tarfile.open(archive, "r:*") as tar:
                members = tar.getmembers()
                for member in members:
                    self._validate_member(member)
                tar.extractall(extract_root, members=members, filter="data")
            _integrity(
                extract_root / "projection.sqlite3", record.manifest.verification_level
            )

            for live in (
                self.settings.db_path,
                self.settings.db_path.with_name(f"{self.settings.db_path.stem}.transcripts.db"),
                self.settings.data_dir / "transcript_objects",
                self.settings.data_dir / "sync_refs.json",
                self.settings.objects_dir,
            ):
                if live.exists():
                    saved = rollback_root / live.name
                    os.replace(live, saved)
                    moved.append((live, saved))
            os.replace(extract_root / "projection.sqlite3", self.settings.db_path)
            os.chmod(self.settings.db_path, 0o600)
            refs = json.loads((extract_root / "sync_refs.json").read_text())
            atomic_write_json(
                self.settings.data_dir / "sync_refs.json",
                refs,
                mode=0o600,
            )
            os.replace(extract_root / "objects", self.settings.objects_dir)
            transcript_snapshot = extract_root / "transcripts.sqlite3"
            if transcript_snapshot.exists():
                transcript_live = self.settings.db_path.with_name(f"{self.settings.db_path.stem}.transcripts.db")
                os.replace(transcript_snapshot, transcript_live)
                os.chmod(transcript_live, 0o600)
                cold_snapshot = extract_root / "transcript_objects"
                if cold_snapshot.exists():
                    os.replace(cold_snapshot, self.settings.data_dir / "transcript_objects")
                _integrity(transcript_live, record.manifest.verification_level)
                _verify_transcript_store(transcript_live, self.settings.data_dir / "transcript_objects")
            _integrity(self.settings.db_path, record.manifest.verification_level)
            try:
                restored_refs = json.loads(
                    (self.settings.data_dir / "sync_refs.json").read_text()
                )
            except (OSError, ValueError) as exc:
                raise BackupError(
                    "post_restore_refs_unreadable",
                    f"restored durable refs are unreadable: {exc}",
                ) from exc
            refs_for_instance = {
                key.split("/", 1)[0]: head
                for key, head in restored_refs.items()
                if "/" in key and key.split("/", 1)[1] == self.settings.instance_id
            }
            projected = _projection_heads(self.settings.db_path)
            mismatches = sorted(
                realm
                for realm in set(projected) | set(refs_for_instance)
                if projected.get(realm) != refs_for_instance.get(realm)
            )
            request.reconciliation_realms = mismatches
            request.status = "reconciliation_required" if mismatches else "success"
            request.finished_at = datetime.now(UTC)
            request.instructions = (
                [
                    f"Start PA, then run `pa sync reconcile --realm {realm}`.",
                    "Do not force sync refs or select a divergent realm head.",
                ]
                for realm in mismatches
            )
            if mismatches:
                request.instructions = [
                    item for pair in request.instructions for item in pair
                ]
            else:
                request.instructions = [
                    "Start PA and verify `pa sync status` before resuming writes."
                ]
            self._metric("restore_success_total")
            self._audit(
                "restore_succeeded",
                restore_id=request.id,
                backup_id=backup_id,
                pre_restore_backup_id=pre.backup_id,
                reconciliation_realms=mismatches,
            )
        except Exception as exc:  # noqa: BLE001 - restore must always roll back
            for live, saved in reversed(moved):
                try:
                    if live.is_dir():
                        shutil.rmtree(live)
                    else:
                        live.unlink(missing_ok=True)
                    if saved.exists():
                        os.replace(saved, live)
                except OSError:
                    logger.exception("restore.rollback_failed")
            request.status = "failed"
            request.failure_reason = (
                f"{exc.code}: {exc}"
                if isinstance(exc, BackupError)
                else f"{type(exc).__name__}: {exc}"
            )
            request.instructions = [
                f"Current state was preserved as backup {pre.backup_id}.",
                f"Rollback staging remains at {rollback_root}.",
                "Keep PA stopped and inspect backup audit history before retrying.",
            ]
            request.finished_at = datetime.now(UTC)
            self._metric("restore_failure_total")
            self._audit(
                "restore_failed",
                restore_id=request.id,
                reason=request.failure_reason,
                recovery_path=str(rollback_root),
            )
        else:
            shutil.rmtree(rollback_root, ignore_errors=True)
        finally:
            shutil.rmtree(extract_root, ignore_errors=True)
            self._set_restore(request)
        return request

    def _jitter(self) -> int:
        bound = min(
            self.config.jitter_seconds, max(0, self.config.interval_seconds // 10)
        )
        if bound == 0:
            return 0
        digest = hashlib.sha256(
            f"{self.settings.instance_id}:{datetime.now(UTC).date()}".encode()
        ).digest()
        return int.from_bytes(digest[:4], "big") % (bound + 1)

    def _schedule_next(self, *, base: datetime | None = None) -> datetime:
        base = base or datetime.now(UTC)
        next_run = base + timedelta(
            seconds=self.config.interval_seconds + self._jitter()
        )
        with self._state_lock:
            self._state.next_scheduled_run = next_run
            self._save_state()
        return next_run

    async def start(self, async_runtime: Any) -> None:
        self._ensure_default_destination()
        if self.config.destination_dir.is_dir():
            cleanup_lock_fd: int | None = None
            try:
                cleanup_lock_fd = self._acquire_destination_lock()
                self._cleanup_interrupted_publish()
            except BackupError as exc:
                if exc.code != "backup_overlap_process":
                    raise
                self._audit("backup_cleanup_skipped_active_process")
            finally:
                if cleanup_lock_fd is not None:
                    fcntl.flock(cleanup_lock_fd, fcntl.LOCK_UN)
                    os.close(cleanup_lock_fd)
        if self._scheduler_task:
            return
        self._loop = asyncio.get_running_loop()
        self._stop_event = asyncio.Event()
        self._config_event = asyncio.Event()
        if self._state.next_scrub_run is None:
            with self._state_lock:
                self._state.next_scrub_run = datetime.now(UTC) + timedelta(
                    seconds=self.config.scrub_interval_seconds
                )
                self._save_state()

        async def scheduler() -> None:
            now = datetime.now(UTC)
            last = self._state.last_success
            startup_due = (
                self.config.enabled
                and self.config.run_on_startup
                and (
                    last is None
                    or (now - last).total_seconds()
                    >= self.config.startup_min_age_seconds
                )
            )
            if startup_due:
                await self._run_maintenance(
                    "backup.startup",
                    self.run_backup,
                    trigger="startup",
                    idempotency_key=f"startup:{self.settings.instance_id}:{now.date()}",
                )
            while self._stop_event and not self._stop_event.is_set():
                if self.config.enabled:
                    next_run = self._state.next_scheduled_run or self._schedule_next()
                else:
                    with self._state_lock:
                        self._state.next_scheduled_run = None
                        self._save_state()
                    next_run = None
                scrub_run = self._state.next_scrub_run
                due_times = [item for item in (next_run, scrub_run) if item is not None]
                timeout = max(
                    0.0, (min(due_times) - datetime.now(UTC)).total_seconds()
                )
                outcome = await self._wait_for_schedule_change(timeout)
                if outcome == "stop":
                    break
                if outcome == "config":
                    continue
                now = datetime.now(UTC)
                if scrub_run is not None and now >= scrub_run:
                    await self._run_maintenance("backup.scrub", self.deep_scrub)
                if next_run is not None and now >= next_run:
                    if now > next_run + timedelta(seconds=self.config.interval_seconds):
                        self._metric("backup_missed_schedule_total")
                        self._audit(
                            "backup_schedule_missed", scheduled_for=next_run.isoformat()
                        )
                    await self._run_maintenance(
                        "backup.scheduled",
                        self.run_backup,
                        trigger="scheduled",
                        idempotency_key=f"scheduled:{int(next_run.timestamp())}",
                    )
                    # Coalesce downtime or a slow job into one attempt instead of
                    # replaying every elapsed interval in a restart storm.
                    self._schedule_next(base=max(next_run, datetime.now(UTC)))

        self._scheduler_task = asyncio.create_task(
            scheduler(), name="pa-metadata-backup-scheduler"
        )

    async def _wait_for_schedule_change(self, timeout: float | None) -> str:
        if not self._stop_event or not self._config_event:
            return "stop"
        stop_task = asyncio.create_task(self._stop_event.wait())
        config_task = asyncio.create_task(self._config_event.wait())
        done, pending = await asyncio.wait(
            {stop_task, config_task},
            timeout=timeout,
            return_when=asyncio.FIRST_COMPLETED,
        )
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        if stop_task in done and stop_task.result():
            return "stop"
        if config_task in done and config_task.result():
            self._config_event.clear()
            return "config"
        return "timeout"

    def _cleanup_interrupted_publish(self) -> None:
        destination = self.config.destination_dir
        if not destination.is_dir():
            return
        removed: list[str] = []
        for path in sorted(destination.glob(".pa-backup-*.tmp")):
            if path.is_file():
                path.unlink(missing_ok=True)
                removed.append(path.name)
        for path in sorted(destination.glob(".pa-backup-stage-*")):
            if path.is_dir():
                shutil.rmtree(path, ignore_errors=True)
                removed.append(path.name)
        if removed:
            self._metric("backup_interrupted_publish_cleanup_total", len(removed))
            self._audit("backup_interrupted_publish_cleaned", count=len(removed))
            self._save_state()

    async def stop(self) -> None:
        if self._stop_event:
            self._stop_event.set()
        if self._scheduler_task:
            try:
                await asyncio.wait_for(self._scheduler_task, timeout=10)
            except TimeoutError:
                self._scheduler_task.cancel()
            self._scheduler_task = None
        self._loop = None
        self._config_event = None
        self._maintenance_executor.shutdown(wait=False, cancel_futures=True)
