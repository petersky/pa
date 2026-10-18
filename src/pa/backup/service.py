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
    MAX_MANIFEST_BYTES,
    MAX_MANIFEST_FILES,
    BackupConfig,
    BackupManifest,
    BackupRecord,
    BackupRun,
    BackupState,
    ManifestFile,
    MissingTranscriptObject,
    RestoreRequest,
    validate_backup_destination_path,
)
from pa.core.io import atomic_write_json
from pa.core.operation_budget import measured_lock

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


def _missing_transcript_objects(database: Path, present: set[str]) -> list[MissingTranscriptObject]:
    """Describe absences from the captured index, without changing references."""
    missing: list[MissingTranscriptObject] = []
    with contextlib.closing(sqlite3.connect(database.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        rows = conn.execute("""
            SELECT e.cold_hash,COUNT(*),o.codec,o.uncompressed_bytes,o.compressed_bytes,o.ref_count,
                   SUM(CASE WHEN e.payload_hash!=e.cold_hash THEN 1 ELSE 0 END)
            FROM transcript_events AS e LEFT JOIN transcript_objects AS o ON o.hash=e.cold_hash
            WHERE e.cold_hash IS NOT NULL GROUP BY e.cold_hash ORDER BY e.cold_hash
        """)
        for index, (digest, count, codec, raw_bytes, stored_bytes, ref_count, mismatches) in enumerate(rows):
            if index >= MAX_MANIFEST_FILES:
                raise BackupError("manifest_limit_exceeded", "too many referenced transcript objects")
            if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest) or mismatches:
                raise BackupError("transcript_object_corrupt", "captured transcript index has invalid content hashes")
            if digest in present:
                continue
            if codec != "zlib" or ref_count != count:
                raise BackupError("transcript_object_corrupt", "missing transcript payload has inconsistent index metadata")
            try:
                missing.append(MissingTranscriptObject(
                    sha256=digest, uncompressed_bytes=raw_bytes,
                    compressed_bytes=stored_bytes, reference_count=count,
                ))
            except ValueError as exc:
                raise BackupError("transcript_object_corrupt", "missing transcript payload has invalid size metadata") from exc
    return missing


def _coverage(manifest: BackupManifest | None) -> dict[str, Any]:
    missing = manifest.missing_transcript_objects if manifest else []
    count = len(missing)
    size = sum(item.uncompressed_bytes for item in missing)
    return {
        "data_complete": manifest.data_complete if manifest else None,
        "missing_transcript_object_count": count,
        "missing_transcript_bytes": size,
        "coverage_warning": (
            f"{count} historical transcript payload(s), {size} decoded bytes, were absent from the source. "
            "Available data and all references are preserved. This is not a fully intact recovery point."
            if count else None
        ),
        "missing_data_fingerprint": hashlib.sha256(json.dumps(
            [item.model_dump(mode="json") for item in missing], sort_keys=True,
        ).encode()).hexdigest() if count else None,
    }


def _verify_transcript_store(database: Path, objects_root: Path,
                             declared_missing: list[MissingTranscriptObject] | None = None) -> None:
    if not database.exists():
        if declared_missing:
            raise BackupError("transcript_missing_declaration_mismatch", "missing-data declarations require a captured transcript index")
        return
    present = {path.parent.name + path.stem for path in objects_root.glob("*/*.zlib") if path.is_file()}
    actual_missing = _missing_transcript_objects(database, present)
    declarations = {item.sha256: item for item in declared_missing or []}
    if {item.sha256: item for item in actual_missing} != declarations:
        if actual_missing and not declarations:
            raise BackupError("transcript_object_missing", "cold transcript payload is missing without an explicit limitation declaration")
        raise BackupError("transcript_missing_declaration_mismatch", "declared transcript limitations differ from the captured index and files")
    with contextlib.closing(sqlite3.connect(f"file:{database}?mode=ro", uri=True)) as conn:
        refs = conn.execute("SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL").fetchall()
    for (digest,) in refs:
        path = objects_root / str(digest)[:2] / f"{str(digest)[2:]}.zlib"
        if digest in declarations:
            if path.exists():
                raise BackupError("transcript_missing_declaration_mismatch", "a declared absent payload has an archive member")
            continue
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
        self._verifying: set[str] = set()
        self.failure_notifier: Any = None
        self.missing_data_notifier: Any = None
        self._failure_log_key: str | None = None
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

    def _online_sqlite_backup(
        self, target: Path, *, source_path: Path | None = None,
        source_connection: sqlite3.Connection | None = None,
    ) -> None:
        target.parent.mkdir(parents=True, exist_ok=True)
        try:
            with (
                (contextlib.nullcontext(source_connection) if source_connection is not None
                 else contextlib.closing(sqlite3.connect(source_path or self.settings.db_path, timeout=30))) as source,
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

    def _prepare_sqlite_snapshot(
        self, target: Path, source_path: Path, readers: contextlib.ExitStack,
        copies: list[tuple[Path, sqlite3.Connection]],
    ) -> None:
        """Fix the SQLite read view under writer locks; copy WAL pages later."""
        started = time.monotonic()
        try:
            source = readers.enter_context(contextlib.closing(sqlite3.connect(
                source_path.resolve().as_uri() + "?mode=ro", uri=True, timeout=30,
            )))
            if source.execute("PRAGMA journal_mode").fetchone()[0].lower() != "wal":
                # A rollback-journal read would block writer commits during the
                # deferred copy. Preserve the existing locked-copy behavior for
                # standalone/legacy databases without changing their journal mode.
                self._metric("backup_snapshot_locked_sqlite_fallback_total")
                self._online_sqlite_backup(target, source_connection=source)
                return
            source.execute("BEGIN")
            # BEGIN alone is deferred. Reading a real database page fixes the WAL
            # end mark before PA's projection/ref/transcript locks are released.
            source.execute("SELECT name FROM sqlite_schema LIMIT 1").fetchone()
            copies.append((target, source))
        except sqlite3.Error as exc:
            raise BackupError("online_snapshot_failed", f"SQLite read snapshot failed: {exc}") from exc
        finally:
            self._metric(f"backup_snapshot_{target.stem}_anchor_seconds_total", time.monotonic() - started)

    def _pin_cold_objects(self, staging: Path, pins: contextlib.ExitStack,
                          cold_files: list[tuple[Path, Path]]) -> None:
        started = time.monotonic()
        before = len(cold_files)
        try:
            cold_source = self.settings.db_path.parent / "transcript_objects"
            if self.store is not None and hasattr(self.store, "transcripts"):
                cold_source = self.store.transcripts.objects_dir
            if not cold_source.exists():
                return
            pin_root = Path(pins.enter_context(tempfile.TemporaryDirectory(
                prefix=".pa-backup-pin-", dir=cold_source,
            )))
            for source in sorted(cold_source.glob("*/*.zlib")):
                relative = source.relative_to(cold_source)
                pinned = pin_root / relative
                pinned.parent.mkdir(parents=True, exist_ok=True)
                os.link(source, pinned)
                cold_files.append((pinned, staging / "transcript_objects" / relative))
        finally:
            self._metric("backup_snapshot_cold_pin_seconds_total", time.monotonic() - started)
            self._metric("backup_snapshot_cold_pin_files_total", len(cold_files) - before)

    def _preflight_cold_references(self, database: Path, cold_files: list[tuple[Path, Path]]) -> list[MissingTranscriptObject]:
        """Record exact source absences; corruption and incomplete pinning fail."""
        captured = {target.parent.name + target.stem for _, target in cold_files}
        missing = _missing_transcript_objects(database, captured)
        source = getattr(getattr(self.store, "transcripts", None), "objects_dir",
                         self.settings.db_path.parent / "transcript_objects")
        for item in missing:
            path = source / item.sha256[:2] / f"{item.sha256[2:]}.zlib"
            try:
                path.lstat()
            except FileNotFoundError:
                continue
            raise BackupError("transcript_inventory_changed", "a source payload exists but was not pinned; no missing-data declaration is allowed")
        return missing

    def _snapshot(self, staging: Path) -> BackupManifest:
        lock = (
            self.store.mutation()
            if self.store is not None
            else contextlib.nullcontext()
        )
        transcript_lock = (
            measured_lock(self.store.transcripts._lock, name="transcript.write", track_deadline=False)
            if self.store is not None and hasattr(self.store, "transcripts")
            else contextlib.nullcontext()
        )
        # Anchor mutable databases and refs together. WAL read views preserve that
        # point while writers continue; a short GC retention lease keeps cold
        # payloads alive until independent source-filesystem pins exist.
        with contextlib.ExitStack() as pins:
            cold_files: list[tuple[Path, Path]] = []
            object_files: list[tuple[Path, Path, str]] = []
            copies: list[tuple[Path, sqlite3.Connection]] = []
            with contextlib.ExitStack() as retention:
                with contextlib.ExitStack() as readers:
                    lock_started = time.monotonic()
                    with lock, transcript_lock:
                        self._metric("backup_snapshot_lock_wait_seconds_total", time.monotonic() - lock_started)
                        capture_started = time.monotonic()
                        try:
                            defer_pins = hasattr(getattr(self.store, "transcripts", None), "retain_objects")
                            if defer_pins:
                                retention.enter_context(self.store.transcripts.retain_objects())
                            projection, transcript_target, refs = self._capture_snapshot(
                                staging, pins, cold_files, object_files, readers, copies, defer_pins,
                            )
                        finally:
                            self._metric("backup_snapshot_lock_hold_seconds_total", time.monotonic() - capture_started)
                    for target, source in copies:
                        started = time.monotonic()
                        try:
                            self._online_sqlite_backup(target, source_connection=source)
                        finally:
                            self._metric(f"backup_snapshot_{target.stem}_copy_seconds_total", time.monotonic() - started)
                # Read transactions are closed before the cold inventory walk.
                # GC is retained, but publication and ordinary writers can run.
                if defer_pins and transcript_target is not None:
                    self._pin_cold_objects(staging, pins, cold_files)

            missing_objects: list[MissingTranscriptObject] = []
            _integrity(projection, self.config.verification_level)
            if transcript_target is not None:
                _integrity(transcript_target, self.config.verification_level)
                missing_objects = self._preflight_cold_references(transcript_target, cold_files)
            atomic_write_json(staging / "sync_refs.json", refs, mode=0o600)
            for source, target in cold_files:
                self._copy_file(source, target)
            for source, target, object_id in object_files:
                self._copy_file(source, target)
                if _sha256(target) != object_id:
                    raise BackupError(
                        "event_object_corrupt",
                        f"event-log object {object_id[:12]} failed its content hash",
                    )

        return self._snapshot_manifest(staging, projection, refs, missing_objects)

    def _capture_snapshot(
        self, staging: Path, pins: contextlib.ExitStack,
        cold_files: list[tuple[Path, Path]],
        object_files: list[tuple[Path, Path, str]],
        readers: contextlib.ExitStack,
        copies: list[tuple[Path, sqlite3.Connection]],
        defer_pins: bool,
    ) -> tuple[Path, Path | None, dict[str, str]]:
        """Called only under the store and transcript writer locks."""
        projection = staging / "projection.sqlite3"
        self._prepare_sqlite_snapshot(projection, self.settings.db_path, readers, copies)
        transcript_target = None
        transcript_source = self.settings.db_path.with_name(
            f"{self.settings.db_path.stem}.transcripts.db"
        )
        if transcript_source.exists():
            transcript_target = staging / "transcripts.sqlite3"
            self._prepare_sqlite_snapshot(transcript_target, transcript_source, readers, copies)
            cold_target = staging / "transcript_objects"
            cold_target.mkdir(mode=0o700)
            if not defer_pins:
                self._pin_cold_objects(staging, pins, cold_files)

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

        objects_target = staging / "objects"
        objects_target.mkdir(mode=0o700)
        # DirEntry caches file type information. Path.rglob + is_file performs
        # a separate stat per object while blocking every interactive writer.
        # ObjectStore's canonical layout is exactly two hex characters / 62.
        if self.settings.objects_dir.exists():
            with os.scandir(self.settings.objects_dir) as directories:
                for directory in sorted(directories, key=lambda item: item.name):
                    if not re.fullmatch(r"[0-9a-f]{2}", directory.name) or not directory.is_dir(follow_symlinks=False):
                        continue
                    with os.scandir(directory.path) as entries:
                        for entry in sorted(entries, key=lambda item: item.name):
                            if not re.fullmatch(r"[0-9a-f]{62}", entry.name) or not entry.is_file(follow_symlinks=False):
                                continue
                            relative = Path(directory.name) / entry.name
                            object_files.append((Path(entry.path), objects_target / relative, directory.name + entry.name))
        return projection, transcript_target, refs

    def _snapshot_manifest(
        self, staging: Path, projection: Path, refs: dict[str, str],
        missing_objects: list[MissingTranscriptObject] | None = None,
    ) -> BackupManifest:
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
        metadata = dict(
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
                if len(files) >= MAX_MANIFEST_FILES:
                    raise BackupError(
                        "manifest_limit_exceeded", "backup exceeds the manifest file limit"
                    )
                files.append(
                    ManifestFile(
                        path=path.relative_to(staging).as_posix(),
                        size_bytes=path.stat().st_size,
                        sha256=_sha256(path),
                    )
                )
        limited = bool(missing_objects)
        manifest = BackupManifest(
            **metadata, files=files, missing_transcript_objects=missing_objects or [],
            format=f"pa.metadata-backup/v{2 if limited else 1}",
            format_version=2 if limited else 1,
        )
        atomic_write_json(
            staging / "manifest.json",
            manifest.model_dump(mode="json"),
            mode=0o600,
        )
        if (staging / "manifest.json").stat().st_size > MAX_MANIFEST_BYTES:
            raise BackupError(
                "manifest_limit_exceeded", "backup exceeds the manifest byte limit"
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
                # New archives expose metadata before compressed payloads. Old v1
                # archives remain readable through the streaming reader below.
                archive.add(staging / "manifest.json", arcname="manifest.json")
                for path in sorted(staging.rglob("*")):
                    if path == staging / "manifest.json":
                        continue
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
            if not verification.integrity_verified:
                raise BackupError(
                    "temporary_verification_failed",
                    verification.verification_error
                    or "temporary backup archive did not verify",
                )
            publish_started = time.monotonic()
            published = self.backend.publish(temporary, name)
            self._remember_verification(published, manifest.backup_id, manifest, None)
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
        preserve_existing: bool = False,
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
        if trigger in {"scheduled", "startup"} and self._automatic_paused():
            self._notify_repeated_failure()
            return BackupRun(
                trigger=trigger, status="skipped", finished_at=datetime.now(UTC),
                failure_reason="automatic backups paused after repeated failures; administrator action required",
            )
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
            protected_backup_ids = set(protected_backup_ids or ())
            if preserve_existing:
                protected_backup_ids.update(item.backup_id for item in self.list_backups())
            staging_path = Path(
                tempfile.mkdtemp(
                    prefix=".pa-backup-stage-", dir=self.config.destination_dir
                )
            )
            os.chmod(staging_path, 0o700)
            phase_started = time.monotonic()
            try:
                manifest = self._snapshot(staging_path)
            finally:
                run.phase_seconds["snapshot"] = round(time.monotonic() - phase_started, 6)
            archive = self._archive(staging_path, manifest, run.phase_seconds)
            # _archive fully verifies the temporary artifact before backend.publish
            # atomically renames those exact immutable bytes into visibility.
            run.status = "success"
            run.backup_id = manifest.backup_id
            run.integrity_verified = True
            for key, value in _coverage(manifest).items():
                setattr(run, key, value)
            run.verification = "verified" if manifest.data_complete else "failed"
            # Keep old state readers fail-closed: their verification flag still
            # means intact recovery, and their reason column shows the warning.
            run.failure_reason = run.coverage_warning
            run.size_bytes = archive.stat().st_size
            # A protected older recovery point must not make retention select
            # the archive this successful run has just published instead.
            protected_backup_ids.add(manifest.backup_id)
            if not manifest.data_complete:
                # A limited capture cannot replace any intact recovery point.
                protected_backup_ids.update(item.backup_id for item in self.list_backups() if item.verified)
            phase_started = time.monotonic()
            run.pruned_backup_ids = self.prune(
                protected_backup_ids=protected_backup_ids
            )
            run.phase_seconds["prune"] = round(time.monotonic() - phase_started, 6)
            with self._state_lock:
                self._state.last_completed = datetime.now(UTC)
                if manifest.data_complete:
                    self._state.last_success = self._state.last_completed
                    self._state.missing_data_alert_key = None
                self._state.consecutive_failures = 0
                self._state.failure_alert_key = None
            self._metric("backup_success_total" if manifest.data_complete else "backup_limited_total")
            self._metric("backup_bytes_total", run.size_bytes)
            self._audit(
                "backup_succeeded" if manifest.data_complete else "backup_completed_with_missing_data",
                run_id=run.id,
                backup_id=run.backup_id,
                size_bytes=run.size_bytes,
                pruned_backup_ids=run.pruned_backup_ids,
                queue_wait_seconds=run.queue_wait_seconds,
                phase_seconds=run.phase_seconds,
                **_coverage(manifest),
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
        if run.status == "failed":
            self._notify_repeated_failure()
        elif run.status == "success" and self.config.enabled:
            self._schedule_next()
            if self._loop and self._config_event:
                self._loop.call_soon_threadsafe(self._config_event.set)
        if run.status == "success" and run.data_complete is False:
            self._notify_missing_data(run)
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
            with tarfile.open(path, "r|*") as archive:
                for member in archive:
                    self._validate_member(member)
                    if member.name != "manifest.json":
                        continue
                    if not member.isfile() or not 0 < member.size <= MAX_MANIFEST_BYTES:
                        raise BackupError(
                            "manifest_invalid",
                            "manifest exceeds the supported byte limit or is not a file",
                        )
                    handle = archive.extractfile(member)
                    if handle is None:
                        raise BackupError("manifest_invalid", "manifest is unreadable")
                    return BackupManifest.model_validate_json(
                        handle.read(MAX_MANIFEST_BYTES + 1)
                    )
            raise BackupError("manifest_invalid", "manifest is missing")
        except (tarfile.TarError, KeyError, ValueError, OSError) as exc:
            if isinstance(exc, BackupError):
                raise
            raise BackupError(
                "archive_unreadable", f"cannot read backup archive: {exc}"
            ) from exc

    def _path_for_backup(self, backup_id: str) -> Path:
        matches = [
            item.path for item in self.list_backups() if item.backup_id == backup_id
        ]
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
        with self._state_lock:
            if backup_id in self._verifying:
                raise BackupError("verification_in_progress", "this backup is already being verified")
            self._verifying.add(backup_id)
        try:
            return self._verify_backup(backup_id, path)
        finally:
            with self._state_lock:
                self._verifying.discard(backup_id)

    def _verify_backup(self, backup_id: str, path: Path) -> BackupRecord:
        manifest: BackupManifest | None = None
        identity = self._archive_identity(path)
        try:
            manifest = self._read_manifest(path)
            if manifest.backup_id != backup_id and backup_id != f"unreadable:{path.name}":
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
                    if len({m.name for m in members}) != len(members):
                        raise BackupError("archive_unsafe", "archive contains duplicate member names")
                    if names != set(expected) | {"manifest.json"}:
                        raise BackupError(
                            "archive_file_set_mismatch",
                            "archive file set differs from the signed manifest",
                        )
                    for member in members:
                        if (
                            member.isfile() and member.name != "manifest.json"
                            and member.size != expected[member.name].size_bytes
                        ):
                            raise BackupError(
                                "archive_checksum_mismatch",
                                "archive member size differs from manifest",
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
                    _verify_transcript_store(transcript_projection, root / "transcript_objects", manifest.missing_transcript_objects)
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
            if self._archive_identity(path) != identity:
                raise BackupError("archive_changed", "archive changed during verification; verify it again")
            self._metric("backup_verification_success_total" if manifest.data_complete else "backup_verification_limited_total")
            self._remember_verification(path, backup_id, manifest, None, identity=identity)
            self._audit("backup_verified" if manifest.data_complete else "backup_integrity_verified_with_missing_data",
                        backup_id=backup_id, **_coverage(manifest))
            return BackupRecord(
                backup_id=backup_id,
                path=path,
                created_at=manifest.created_at,
                size_bytes=path.stat().st_size,
                verified=manifest.data_complete,
                integrity_verified=True,
                verification_status="verified" if manifest.data_complete else "verified_with_missing_data",
                manifest=manifest,
                **_coverage(manifest),
            )
        except Exception as exc:  # noqa: BLE001 - restore must always roll back
            reason = (
                f"{exc.code}: {exc}"
                if isinstance(exc, BackupError)
                else f"{type(exc).__name__}: {exc}"
            )
            self._metric("backup_verification_failure_total")
            self._remember_verification(path, backup_id, manifest, reason, identity=identity)
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
                verification_status="failed",
                verification_error=reason,
                manifest=manifest,
                **_coverage(manifest),
            )

    @staticmethod
    def _archive_identity(path: Path) -> dict[str, int]:
        stat = path.stat()
        return {
            key: getattr(stat, "st_" + key)
            for key in ("dev", "ino", "size", "mtime_ns", "ctime_ns")
        }

    def _remember_verification(
        self, path: Path, backup_id: str, manifest: BackupManifest | None,
        error: str | None, *, identity: dict[str, int] | None = None,
    ) -> None:
        # Temporary failures are run history, never retained recovery points.
        if not path.name.endswith((".pa-backup.tgz", ".pa-backup.tar")):
            return
        with self._state_lock:
            self._state.verifications[backup_id] = {
                "verified": error is None and manifest is not None and manifest.data_complete,
                "integrity_verified": error is None and manifest is not None,
                "error": error,
                "verified_at": datetime.now(UTC).isoformat(),
                "created_at": manifest.created_at.isoformat() if manifest else None,
                "filename": path.name, "destination": str(path.parent.resolve()),
                "identity": identity or self._archive_identity(path),
                **_coverage(manifest),
            }
            self._save_state()

    def list_backups(
        self, *, verify: bool = False, offset: int = 0, limit: int | None = None,
    ) -> list[BackupRecord]:
        """Stat-only inventory. Manifest/integrity reads require explicit verification.

        Legacy verification entries without file identity are unverified until
        checked again; retention must never trust a replaced archive by UUID alone.
        """
        records: list[BackupRecord] = []
        with self._state_lock:
            cached_records = dict(self._state.verifications)
            verifying = set(self._verifying)
        for path in self.backend.list_archives():
            try:
                identity = self._archive_identity(path)
            except FileNotFoundError:
                continue  # concurrent publication/pruning; next read reconciles
            backup_id = _backup_id_from_name(path) or f"unreadable:{path.name}"
            cached = cached_records.get(backup_id, {})
            current = (
                cached.get("identity") == identity
                and cached.get("destination") == str(path.parent.resolve())
                and cached.get("filename") == path.name
            )
            state = "unverified"
            reason = "Verification has not been recorded for these archive bytes. Select Verify to check integrity."
            if current:
                state = ("verified" if cached.get("verified") else "verified_with_missing_data"
                         if cached.get("integrity_verified") and cached.get("data_complete") is False
                         and cached.get("missing_transcript_object_count", 0) > 0 else "failed")
                reason = cached.get("error")
            elif cached:
                reason = "Archive changed or has no recorded file identity. Select Verify to check integrity."
            if backup_id in verifying:
                state, reason = "in_progress", "Verification is running. PA will record the result."
            record = BackupRecord(
                backup_id=backup_id, path=path,
                created_at=(
                    cached["created_at"] if current and cached.get("created_at")
                    else datetime.fromtimestamp(identity["mtime_ns"] / 1e9, UTC)
                ),
                size_bytes=identity["size"], verified=state == "verified",
                integrity_verified=state in {"verified", "verified_with_missing_data"},
                verification_status=state, verification_error=reason,
                **({key: cached.get(key) for key in (
                    "data_complete", "missing_transcript_object_count", "missing_transcript_bytes",
                    "coverage_warning", "missing_data_fingerprint",
                ) if key in cached} if current else {}),
            )
            records.append(record)
        records = sorted(
            records, key=lambda item: (item.created_at, item.backup_id), reverse=True
        )
        page = records[offset:None if limit is None else offset + limit]
        if verify:
            return [self.verify_backup(item.backup_id, path=item.path) for item in page]
        return page

    def prune(self, *, protected_backup_ids: set[str] | None = None) -> list[str]:
        protected_backup_ids = protected_backup_ids or set()
        # Every published archive has an integrity proof before it is recorded;
        # incomplete historical coverage is explicit and never an intact point.
        # restore/delete paths perform their own fresh verification.  Retention
        # only needs that durable verification record; re-extracting every
        # retained archive here makes each scheduled backup scale with the
        # entire backup history (and can saturate a worker for minutes).
        inventory = self.list_backups()
        retained_ids = {item.backup_id for item in inventory}
        with self._state_lock:
            # Older versions recorded temporary attempts as archive verifications.
            # Compact only during maintenance, never as a side effect of status.
            self._state.verifications = {
                key: value for key, value in self._state.verifications.items()
                if key in retained_ids
            }
        records = [item for item in inventory if item.integrity_verified]
        # Preserve every intact point while the newest capture has a limitation.
        # Limited captures can still follow the unchanged retention bounds among
        # themselves; unknown/changed archives are never candidates.
        if records and not records[0].verified:
            protected_backup_ids = set(protected_backup_ids) | {item.backup_id for item in records if item.verified}
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
        """Refresh integrity/coverage proofs; legacy booleans mean fully intact."""
        started = time.monotonic()
        results: dict[str, bool] = {}
        corrupt = limited = 0
        self._audit("backup_scrub_started")
        for record in self.list_backups():
            checked = self.verify_backup(record.backup_id, path=record.path)
            results[record.backup_id] = checked.verified
            corrupt += not checked.integrity_verified
            limited += checked.integrity_verified and checked.data_complete is False
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
            corrupt=corrupt, limited=limited,
        )
        return results

    async def _run_maintenance(self, operation: str, call: Any, **kwargs: Any) -> Any:
        """Submit at most one maintenance job to PA's isolated I/O lane."""
        if self._maintenance_pending:
            self._metric("backup_maintenance_coalesced_total")
            self._audit("backup_maintenance_coalesced", operation=operation)
            return None
        return await self.run_io(operation, call, **kwargs)

    async def run_io(
        self, operation: str, call: Any, *args: Any,
        timeout: float | None = None, **kwargs: Any,
    ) -> Any:
        """Bound manual, verify and scheduled work to the same dedicated worker.

        Admission stays occupied until the thread actually finishes, including
        after a caller disconnects or times out. Never accumulate a job backlog.
        """
        if self._maintenance_pending:
            raise BackupError(
                "backup_io_busy",
                "Backup I/O is already running; inspect backup status before retrying.",
            )
        self._maintenance_pending = True
        queued_at = time.monotonic()

        def invoke() -> Any:
            wait = time.monotonic() - queued_at
            if call == self.run_backup:
                kwargs["queue_wait_seconds"] = wait
            self._metric(f"{operation}_queue_wait_seconds_total", wait)
            return call(*args, **kwargs)

        try:
            loop = asyncio.get_running_loop()
            future = loop.run_in_executor(self._maintenance_executor, invoke)
        except BaseException:
            self._maintenance_pending = False
            raise

        def finished(_future: Any) -> None:
            self._maintenance_pending = False
            # A disconnected caller may no longer be awaiting this future.
            # Retrieve its exception without changing what other awaiters see.
            if not _future.cancelled():
                _future.exception()

        future.add_done_callback(finished)
        try:
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except TimeoutError as exc:
            if future.done():
                raise  # The operation itself failed; it is not still running.
            raise BackupError(
                "backup_io_timeout",
                "Backup I/O is still running after the request deadline; "
                "inspect backup status before retrying.",
            ) from exc

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
        if not record.integrity_verified:
            raise BackupError("backup_not_verified", "backup is not safe to export")
        return record.path

    def status(self, *, backups: list[BackupRecord] | None = None) -> dict[str, Any]:
        health = self.backend.health(probe=False)
        backups = self.list_backups() if backups is None else backups
        used = sum(item.size_bytes for item in backups)
        with self._state_lock:
            state = self._state.model_dump(
                mode="json",
                exclude={"verifications", "idempotency", "last_scrub_results", "failure_alert_key", "missing_data_alert_key"},
            )
            paused = self._automatic_paused()
            running = bool(self._verifying) or any(
                run.status == "running" for run in self._state.runs
            )
        if paused or not self.config.enabled:
            state["next_scheduled_run"] = None
        good = next((item for item in backups if item.verified), None)
        completed = next((item for item in backups if item.integrity_verified), None)
        limited = completed if completed and completed.data_complete is False else None
        failed = next(
            (run for run in reversed(state["runs"]) if run["status"] == "failed"), None
        )
        if running:
            next_actor, next_action = "PA", "Wait for the running backup and verification result."
        elif paused:
            next_actor = "Administrator"
            next_action = (
                "Inspect the failure reason, correct the cause, then select Back up now. "
                "A successful backup resumes automatic attempts."
            )
        elif self.config.enabled:
            next_actor, next_action = "PA", (
                "Continue scheduled backups of available data; the historical limitation remains disclosed."
                if limited else "Wait for the next scheduled backup."
            )
        else:
            next_actor = "Administrator"
            next_action = "Enable scheduled backups or select Back up now."
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
            "limited_backup_count": sum(1 for item in backups if item.integrity_verified and item.data_complete is False),
            "last_known_good": good.public_dict() if good else None,
            "last_completed_backup": completed.public_dict() if completed else None,
            "historical_data_warning": limited.coverage_warning if limited else None,
            "backup_state": "in_progress" if running else "failed" if paused else "limited" if limited else "idle",
            "automatic_backups_paused": paused,
            "next_actor": next_actor,
            "next_action": next_action,
            "latest_failure_reason": failed["failure_reason"] if failed else None,
            "alerting": {
                "active": paused,
                "threshold": self.config.alert_after_failures,
                "consecutive_failures": state["consecutive_failures"],
            },
            **state,
        }

    def overview(self, *, offset: int = 0, limit: int = 50) -> dict[str, Any]:
        records = self.list_backups()
        return {
            "status": self.status(backups=records),
            "records": [item.public_dict() for item in records[offset:offset + limit]],
            "pagination": {
                "offset": offset, "limit": limit, "total": len(records),
                "previous_offset": max(0, offset - limit) if offset else None,
                "next_offset": offset + limit if offset + limit < len(records) else None,
            },
        }

    def _automatic_paused(self) -> bool:
        with self._state_lock:
            return self._state.consecutive_failures >= self.config.alert_after_failures

    def _notify_repeated_failure(self) -> None:
        with self._state_lock:
            if not self._automatic_paused():
                return
            key = f"backup-failures:{self.settings.instance_id}:{self._state.last_success or 'never'}"
            if self._state.failure_alert_key == key:
                return
            failure_count = self._state.consecutive_failures
        if self.failure_notifier is None:
            # Offline callers can pause scheduling, but must not suppress the
            # durable notification when the server next acquires this state.
            if self._failure_log_key != key:
                logger.error("backup.automatic_attempts_paused failures=%s next_actor=administrator", failure_count)
                self._failure_log_key = key
            return
        self._audit("backup_attention_required", consecutive_failures=failure_count, next_actor="administrator")
        try:
            self.failure_notifier(key, failure_count)
            with self._state_lock:
                self._state.failure_alert_key = key
                self._state.next_scheduled_run = None
                self._save_state()
        except Exception:
            # Keep the pause effective even if the notification backend is down.
            logger.exception("backup.failure_notification_failed")

    def _notify_missing_data(self, record: BackupRun) -> None:
        """One durable notice per limitation set, separate from I/O failures."""
        if record.data_complete is not False or not record.integrity_verified:
            return
        with self._state_lock:
            key = (f"backup-missing-data:{self.settings.instance_id}:"
                   f"{self._state.last_success or 'never'}:{record.missing_data_fingerprint}")
            if self._state.missing_data_alert_key == key:
                return
        if self.missing_data_notifier is None:
            logger.warning("backup.historical_data_missing count=%s bytes=%s",
                           record.missing_transcript_object_count, record.missing_transcript_bytes)
            return
        try:
            self._audit("backup_historical_data_disclosed", backup_id=record.backup_id,
                        missing_transcript_object_count=record.missing_transcript_object_count,
                        missing_transcript_bytes=record.missing_transcript_bytes)
            self.missing_data_notifier(key, record.coverage_warning)
            with self._state_lock:
                self._state.missing_data_alert_key = key
                self._save_state()
        except Exception:
            logger.exception("backup.missing_data_notification_failed")

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
        if record.integrity_verified and record.data_complete is False:
            raise BackupError("backup_contains_missing_data", "This limited archive cannot provide a fully intact restore. Historical missing-data declarations must remain visible; no live state was changed.")
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
            if record.integrity_verified and record.data_complete is False:
                raise BackupError("backup_contains_missing_data", "This limited archive cannot provide a fully intact restore. No live state was changed.")
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
        if pre.status != "success" or pre.verification != "verified" or not pre.backup_id:
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

            # Idle persistent readers can leave a WAL behind even after the
            # server writer has stopped. Drain it before replacing either DB,
            # otherwise its old pages can be replayed over the restored file.
            for database in (
                self.settings.db_path,
                self.settings.db_path.with_name(f"{self.settings.db_path.stem}.transcripts.db"),
            ):
                if database.exists():
                    with contextlib.closing(sqlite3.connect(database, timeout=1)) as conn:
                        if conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()[0]:
                            raise BackupError("database_busy", "Stop all database clients before restoring")

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
            completed = next((run for run in reversed(self._state.runs) if run.status == "success"), None)
            if completed and completed.data_complete is False:
                await self._run_maintenance("backup.missing_data_notice", self._notify_missing_data, completed)
            now = datetime.now(UTC)
            last = self._state.last_completed or self._state.last_success
            startup_due = (
                self.config.enabled
                and not self._automatic_paused()
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
                if self._automatic_paused():
                    self._notify_repeated_failure()
                if self.config.enabled and not self._automatic_paused():
                    next_run = self._state.next_scheduled_run or self._schedule_next()
                else:
                    with self._state_lock:
                        self._state.next_scheduled_run = None
                        self._save_state()
                    next_run = None
                scrub_run = None if self._automatic_paused() else self._state.next_scrub_run
                due_times = [item for item in (next_run, scrub_run) if item is not None]
                timeout = max(
                    0.0, (min(due_times) - datetime.now(UTC)).total_seconds()
                ) if due_times else None
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
                    if not self._automatic_paused():
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
