"""Maintained object-store catalog for O(1) sync status and GC planning.

The content-addressed filesystem remains authoritative for bytes. This SQLite
index records presence, size, mtime, and coarse type so hot status paths never
scandir the object store. It may be deleted and rebuilt offline.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Iterable, Iterator

SCHEMA_VERSION = 1

# Coarse object classes used for retention messaging.
CLASS_COMMIT = "commit"
CLASS_EVENT = "event"
CLASS_SNAPSHOT = "snapshot"
CLASS_EPOCH = "snapshot_epoch"
CLASS_AUXILIARY = "auxiliary"
CLASS_UNKNOWN = "unknown"

# Catalog is treated as covering the store once it contains at least this
# fraction of the DAG-reachable commit+event population (upgrade backfill).
COVERAGE_RATIO = 0.95
BACKFILL_BATCH = 500
BACKFILL_CHECKPOINT_EVERY = 250

RETENTION_REASONS = {
    CLASS_COMMIT: "reachable_realm_ancestry",
    CLASS_EVENT: "reachable_realm_ancestry",
    CLASS_SNAPSHOT: "compaction_or_recovery_evidence",
    CLASS_EPOCH: "snapshot_epoch_root",
    CLASS_AUXILIARY: "retained_auxiliary_object",
    CLASS_UNKNOWN: "unclassified_store_object",
    "unreachable": "diagnostic_or_conflict_ancestry",
    "other_realm": "reachable_from_other_realm_head",
    "pin": "active_peer_backup_or_recovery_pin",
}


class ObjectCatalog:
    """WAL-mode derived catalog beside the object store."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._cancel = threading.Event()
        self._initialize()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    @contextmanager
    def _db(self) -> Iterator[sqlite3.Connection]:
        conn = self._conn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _initialize(self) -> None:
        conn = self._conn()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS catalog_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS objects (
                    object_hash TEXT PRIMARY KEY,
                    size_bytes INTEGER NOT NULL,
                    mtime_ns INTEGER NOT NULL,
                    object_class TEXT NOT NULL,
                    realm_id TEXT,
                    recorded_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS ix_objects_class
                    ON objects(object_class);
                CREATE INDEX IF NOT EXISTS ix_objects_realm
                    ON objects(realm_id);
                CREATE INDEX IF NOT EXISTS ix_objects_mtime
                    ON objects(mtime_ns);
                CREATE TABLE IF NOT EXISTS realm_stats (
                    realm_id TEXT PRIMARY KEY,
                    commit_count INTEGER NOT NULL DEFAULT 0,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    auxiliary_count INTEGER NOT NULL DEFAULT 0,
                    unreachable_count INTEGER NOT NULL DEFAULT 0,
                    reachable_bytes INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    head_hash TEXT,
                    oldest_reachable_ns INTEGER,
                    newest_reachable_ns INTEGER
                );
                CREATE TABLE IF NOT EXISTS growth_samples (
                    sampled_at TEXT PRIMARY KEY,
                    object_count INTEGER NOT NULL,
                    total_bytes INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS rebuild_checkpoint (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    generation INTEGER NOT NULL,
                    cursor_hash TEXT,
                    scanned INTEGER NOT NULL DEFAULT 0,
                    recorded INTEGER NOT NULL DEFAULT 0,
                    target_count INTEGER,
                    cancel_requested INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL DEFAULT 'idle',
                    updated_at TEXT NOT NULL
                );
                """
            )
            conn.execute(
                "INSERT OR IGNORE INTO catalog_meta(key,value) VALUES('schema_version',?)",
                (str(SCHEMA_VERSION),),
            )
            conn.commit()
        finally:
            conn.close()

    def _meta_get(self, key: str) -> str | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT value FROM catalog_meta WHERE key=?", (key,)
            ).fetchone()
        return str(row[0]) if row else None

    def _meta_set(self, key: str, value: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO catalog_meta(key,value) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (key, value),
            )

    def request_cancel(self) -> None:
        self._cancel.set()
        try:
            with self._lock, self._conn() as conn:
                conn.execute(
                    """INSERT INTO rebuild_checkpoint(
                           id,generation,cursor_hash,scanned,recorded,target_count,
                           cancel_requested,state,updated_at)
                       VALUES(1,0,NULL,0,0,NULL,1,'cancelling',?)
                       ON CONFLICT(id) DO UPDATE SET
                         cancel_requested=1,
                         state='cancelling',
                         updated_at=excluded.updated_at""",
                    (datetime.now(UTC).isoformat(),),
                )
        except sqlite3.OperationalError:
            # Rebuild may hold the write lock; the in-memory event is enough
            # for same-process cancellation and the next checkpoint persists.
            pass

    def clear_cancel(self) -> None:
        self._cancel.clear()
        with self._lock, self._conn() as conn:
            conn.execute(
                """UPDATE rebuild_checkpoint
                   SET cancel_requested=0, state=CASE WHEN state='cancelling' THEN 'idle' ELSE state END,
                       updated_at=?
                   WHERE id=1""",
                (datetime.now(UTC).isoformat(),),
            )

    def _cancel_requested(self, conn: sqlite3.Connection) -> bool:
        if self._cancel.is_set():
            return True
        row = conn.execute(
            "SELECT cancel_requested FROM rebuild_checkpoint WHERE id=1"
        ).fetchone()
        return bool(row and int(row[0]))

    @staticmethod
    def classify_payload(raw: bytes) -> tuple[str, str | None]:
        """Best-effort type/realm classification without failing closed."""
        try:
            value = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
            return CLASS_UNKNOWN, None
        if not isinstance(value, dict):
            return CLASS_UNKNOWN, None
        realm_id = value.get("realm_id")
        realm = str(realm_id) if isinstance(realm_id, str) else None
        obj_type = value.get("type")
        if obj_type == "snapshot_epoch":
            return CLASS_EPOCH, realm
        if obj_type == "snapshot":
            return CLASS_SNAPSHOT, realm
        if "event_hashes" in value and "parent_hashes" in value:
            return CLASS_COMMIT, realm
        if "card_id" in value and ("type" in value or "payload" in value):
            return CLASS_EVENT, realm
        if obj_type:
            return CLASS_AUXILIARY, realm
        return CLASS_UNKNOWN, realm

    def record(self, object_hash: str, raw: bytes, *, mtime_ns: int | None = None) -> None:
        object_class, realm_id = self.classify_payload(raw)
        now = datetime.now(UTC).isoformat()
        stamp = mtime_ns if mtime_ns is not None else time.time_ns()
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO objects(object_hash,size_bytes,mtime_ns,object_class,realm_id,recorded_at)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(object_hash) DO UPDATE SET
                     size_bytes=excluded.size_bytes,
                     mtime_ns=excluded.mtime_ns,
                     object_class=excluded.object_class,
                     realm_id=excluded.realm_id,
                     recorded_at=excluded.recorded_at""",
                (object_hash, len(raw), stamp, object_class, realm_id, now),
            )

    def discard(self, object_hash: str) -> None:
        with self._lock, self._conn() as conn:
            conn.execute("DELETE FROM objects WHERE object_hash=?", (object_hash,))

    def has(self, object_hash: str) -> bool:
        with self._db() as conn:
            row = conn.execute(
                "SELECT 1 FROM objects WHERE object_hash=?", (object_hash,)
            ).fetchone()
        return row is not None

    def count(self) -> int:
        with self._db() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0])

    def total_bytes(self) -> int:
        with self._db() as conn:
            row = conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM objects").fetchone()
        return int(row[0])

    def age_bounds_ns(self) -> tuple[int | None, int | None]:
        with self._db() as conn:
            row = conn.execute(
                "SELECT MIN(mtime_ns), MAX(mtime_ns) FROM objects"
            ).fetchone()
        if row is None or row[0] is None:
            return None, None
        return int(row[0]), int(row[1])

    def sample_growth(self) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock, self._conn() as conn:
            count = int(conn.execute("SELECT COUNT(*) FROM objects").fetchone()[0])
            total = int(
                conn.execute("SELECT COALESCE(SUM(size_bytes),0) FROM objects").fetchone()[0]
            )
            conn.execute(
                """INSERT INTO growth_samples(sampled_at,object_count,total_bytes)
                   VALUES(?,?,?)
                   ON CONFLICT(sampled_at) DO UPDATE SET
                     object_count=excluded.object_count,
                     total_bytes=excluded.total_bytes""",
                (now, count, total),
            )
            # Keep a compact rolling window (about a week of hourly samples).
            conn.execute(
                """DELETE FROM growth_samples WHERE sampled_at NOT IN (
                     SELECT sampled_at FROM growth_samples
                     ORDER BY sampled_at DESC LIMIT 200
                   )"""
            )

    def growth_rate_per_hour(self) -> dict[str, float | None]:
        with self._db() as conn:
            rows = conn.execute(
                """SELECT sampled_at, object_count, total_bytes
                   FROM growth_samples ORDER BY sampled_at DESC LIMIT 2"""
            ).fetchall()
        if len(rows) < 2:
            return {"objects_per_hour": None, "bytes_per_hour": None}
        newest, older = rows[0], rows[1]
        try:
            t_new = datetime.fromisoformat(str(newest[0]))
            t_old = datetime.fromisoformat(str(older[0]))
        except ValueError:
            return {"objects_per_hour": None, "bytes_per_hour": None}
        hours = max((t_new - t_old).total_seconds() / 3600.0, 1e-6)
        return {
            "objects_per_hour": (int(newest[1]) - int(older[1])) / hours,
            "bytes_per_hour": (int(newest[2]) - int(older[2])) / hours,
        }

    def publish_realm_stats(
        self,
        realm_id: str,
        *,
        commit_count: int,
        event_count: int,
        auxiliary_count: int,
        unreachable_count: int,
        reachable_bytes: int,
        head_hash: str | None,
        oldest_reachable_ns: int | None,
        newest_reachable_ns: int | None,
    ) -> None:
        with self._lock, self._conn() as conn:
            conn.execute(
                """INSERT INTO realm_stats(
                       realm_id,commit_count,event_count,auxiliary_count,unreachable_count,
                       reachable_bytes,updated_at,head_hash,oldest_reachable_ns,newest_reachable_ns)
                   VALUES(?,?,?,?,?,?,?,?,?,?)
                   ON CONFLICT(realm_id) DO UPDATE SET
                       commit_count=excluded.commit_count,
                       event_count=excluded.event_count,
                       auxiliary_count=excluded.auxiliary_count,
                       unreachable_count=excluded.unreachable_count,
                       reachable_bytes=excluded.reachable_bytes,
                       updated_at=excluded.updated_at,
                       head_hash=excluded.head_hash,
                       oldest_reachable_ns=excluded.oldest_reachable_ns,
                       newest_reachable_ns=excluded.newest_reachable_ns""",
                (
                    realm_id,
                    commit_count,
                    event_count,
                    auxiliary_count,
                    unreachable_count,
                    reachable_bytes,
                    datetime.now(UTC).isoformat(),
                    head_hash,
                    oldest_reachable_ns,
                    newest_reachable_ns,
                ),
            )

    def realm_stats(self, realm_id: str) -> dict[str, Any] | None:
        with self._db() as conn:
            row = conn.execute(
                "SELECT * FROM realm_stats WHERE realm_id=?", (realm_id,)
            ).fetchone()
        return dict(row) if row else None

    def store_totals(self) -> dict[str, Any]:
        count = self.count()
        total = self.total_bytes()
        oldest, newest = self.age_bounds_ns()
        growth = self.growth_rate_per_hour()
        return {
            "object_count": count,
            "total_bytes": total,
            "oldest_mtime_ns": oldest,
            "newest_mtime_ns": newest,
            "growth": growth,
            "schema_version": SCHEMA_VERSION,
        }

    def iter_hashes(self, *, limit: int | None = None) -> list[str]:
        sql = "SELECT object_hash FROM objects ORDER BY object_hash"
        if limit is not None:
            sql += f" LIMIT {int(limit)}"
        with self._db() as conn:
            return [str(row[0]) for row in conn.execute(sql)]

    def hashes_missing_from(self, known: Iterable[str], *, limit: int = 512) -> list[str]:
        """Return up to `limit` catalog hashes not present in `known`."""
        known_set = set(known)
        missing: list[str] = []
        with self._db() as conn:
            for row in conn.execute("SELECT object_hash FROM objects"):
                object_hash = str(row[0])
                if object_hash in known_set:
                    continue
                missing.append(object_hash)
                if len(missing) >= limit:
                    break
        return missing

    def coverage(
        self, *, expected_reachable: int | None = None
    ) -> dict[str, Any]:
        """Compare catalog population to DAG-reachable commit+event counts."""
        catalog_count = self.count()
        expected = int(expected_reachable or 0)
        ratio = (catalog_count / expected) if expected > 0 else 1.0
        stale = expected > 0 and catalog_count < max(1, int(expected * COVERAGE_RATIO))
        last_rebuild = self._meta_get("last_full_rebuild_at")
        with self._db() as conn:
            checkpoint = conn.execute(
                "SELECT * FROM rebuild_checkpoint WHERE id=1"
            ).fetchone()
        state = str(checkpoint["state"]) if checkpoint else "idle"
        return {
            "catalog_count": catalog_count,
            "expected_reachable": expected,
            "coverage_ratio": ratio if expected > 0 else None,
            "stale": stale,
            "ready": not stale and state in {"idle", "ready", "completed"},
            "backfill_state": state,
            "last_full_rebuild_at": last_rebuild,
            "reason": (
                "catalog_below_dag_reachable_population"
                if stale
                else "catalog_covers_reachable_population"
            ),
        }

    def needs_backfill(self, *, expected_reachable: int) -> bool:
        return bool(self.coverage(expected_reachable=expected_reachable)["stale"])

    def rebuild_from_store(
        self,
        store: Any,
        *,
        batch_size: int = BACKFILL_BATCH,
        resume: bool = True,
        force: bool = False,
    ) -> dict[str, Any]:
        """Checkpointable offline rescan; not for hot request paths.

        Crash/cancel leaves ``rebuild_checkpoint`` so the next call with
        ``resume=True`` continues without wiping already-recorded rows.
        """
        hashes = sorted(store.list_hashes())
        now = datetime.now(UTC).isoformat()
        with self._lock, self._conn() as conn:
            checkpoint = conn.execute(
                "SELECT * FROM rebuild_checkpoint WHERE id=1"
            ).fetchone()
            generation = int(checkpoint["generation"]) + 1 if checkpoint else 1
            resume_from = None
            scanned = 0
            inserted = 0
            if (
                resume
                and not force
                and checkpoint
                and checkpoint["state"] in {"running", "cancelled"}
                and checkpoint["cursor_hash"]
                and int(checkpoint["target_count"] or 0) == len(hashes)
            ):
                resume_from = str(checkpoint["cursor_hash"])
                scanned = int(checkpoint["scanned"])
                inserted = int(checkpoint["recorded"])
                generation = int(checkpoint["generation"])
            else:
                conn.execute("DELETE FROM objects")
                inserted = 0
                scanned = 0
            self._cancel.clear()
            conn.execute(
                """INSERT INTO rebuild_checkpoint(
                       id,generation,cursor_hash,scanned,recorded,target_count,
                       cancel_requested,state,updated_at)
                   VALUES(1,?,?,?,?,?,0,'running',?)
                   ON CONFLICT(id) DO UPDATE SET
                     generation=excluded.generation,
                     cursor_hash=excluded.cursor_hash,
                     scanned=excluded.scanned,
                     recorded=excluded.recorded,
                     target_count=excluded.target_count,
                     cancel_requested=0,
                     state='running',
                     updated_at=excluded.updated_at""",
                (generation, resume_from, scanned, inserted, len(hashes), now),
            )
            conn.commit()

            skipping = resume_from is not None
            last_hash = resume_from
            for object_hash in hashes:
                if skipping:
                    if object_hash == resume_from:
                        skipping = False
                    continue
                if self._cancel_requested(conn):
                    conn.execute(
                        """UPDATE rebuild_checkpoint
                           SET cursor_hash=?, scanned=?, recorded=?,
                               state='cancelled', updated_at=? WHERE id=1""",
                        (
                            last_hash,
                            scanned,
                            inserted,
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                    conn.commit()
                    raise RuntimeError("object catalog rebuild cancelled")
                raw = store.get(object_hash)
                scanned += 1
                last_hash = object_hash
                if raw is None:
                    continue
                path = store._path_for(object_hash)
                try:
                    mtime_ns = path.stat().st_mtime_ns
                except OSError:
                    mtime_ns = time.time_ns()
                object_class, realm_id = self.classify_payload(raw)
                conn.execute(
                    """INSERT INTO objects(object_hash,size_bytes,mtime_ns,object_class,realm_id,recorded_at)
                       VALUES(?,?,?,?,?,?)
                       ON CONFLICT(object_hash) DO UPDATE SET
                         size_bytes=excluded.size_bytes,
                         mtime_ns=excluded.mtime_ns,
                         object_class=excluded.object_class,
                         realm_id=excluded.realm_id,
                         recorded_at=excluded.recorded_at""",
                    (
                        object_hash,
                        len(raw),
                        mtime_ns,
                        object_class,
                        realm_id,
                        datetime.now(UTC).isoformat(),
                    ),
                )
                inserted += 1
                if scanned % BACKFILL_CHECKPOINT_EVERY == 0 or scanned % batch_size == 0:
                    conn.execute(
                        """UPDATE rebuild_checkpoint
                           SET cursor_hash=?, scanned=?, recorded=?, updated_at=?
                           WHERE id=1""",
                        (
                            object_hash,
                            scanned,
                            inserted,
                            datetime.now(UTC).isoformat(),
                        ),
                    )
                    conn.commit()

            conn.execute(
                """UPDATE rebuild_checkpoint
                   SET cursor_hash=NULL, scanned=?, recorded=?, state='completed',
                       cancel_requested=0, updated_at=?
                   WHERE id=1""",
                (scanned, inserted, datetime.now(UTC).isoformat()),
            )
            conn.execute(
                """INSERT INTO catalog_meta(key,value) VALUES('last_full_rebuild_at',?)
                   ON CONFLICT(key) DO UPDATE SET value=excluded.value""",
                (datetime.now(UTC).isoformat(),),
            )
            conn.commit()
        self.sample_growth()
        return {
            "scanned": scanned,
            "recorded": inserted,
            "target_count": len(hashes),
            "resumed": resume_from is not None,
            "state": "completed",
        }

    def status_payload(
        self,
        realm_id: str,
        *,
        retention_defaults: dict[str, str] | None = None,
        expected_reachable: int | None = None,
    ) -> dict[str, Any]:
        """Assemble indexed status without scanning the object filesystem."""
        totals = self.store_totals()
        coverage = self.coverage(expected_reachable=expected_reachable)
        realm = self.realm_stats(realm_id) or {
            "commit_count": 0,
            "event_count": 0,
            "auxiliary_count": 0,
            "unreachable_count": 0,
            "reachable_bytes": 0,
            "head_hash": None,
            "oldest_reachable_ns": None,
            "newest_reachable_ns": None,
            "updated_at": None,
        }
        reasons = retention_defaults or RETENTION_REASONS
        unreachable_count = int(realm["unreachable_count"])
        unreachable_note = reasons["unreachable"]
        if coverage["stale"]:
            # Incomplete catalogs cannot compute unreachable accurately.
            unreachable_count = 0
            unreachable_note = "unknown_until_catalog_backfill"
        return {
            "catalog": {
                "ready": bool(coverage["ready"]),
                "stale": bool(coverage["stale"]),
                "schema_version": SCHEMA_VERSION,
                "updated_at": realm.get("updated_at"),
                "coverage": coverage,
                "backfill_hint": (
                    "POST /api/sync/index/maintenance action=catalog_rebuild"
                    if coverage["stale"]
                    else None
                ),
            },
            "store": {
                "object_count": totals["object_count"],
                "total_bytes": totals["total_bytes"],
                "oldest_mtime_ns": totals["oldest_mtime_ns"],
                "newest_mtime_ns": totals["newest_mtime_ns"],
                "growth": totals["growth"],
                "authoritative": bool(coverage["ready"]),
            },
            "realm": {
                "realm_id": realm_id,
                "reachable": {
                    "commits": int(realm["commit_count"]),
                    "events": int(realm["event_count"]),
                    "auxiliary": int(realm["auxiliary_count"]),
                    "bytes": int(realm["reachable_bytes"]),
                    "oldest_mtime_ns": realm.get("oldest_reachable_ns"),
                    "newest_mtime_ns": realm.get("newest_reachable_ns"),
                    "retained_because": reasons[CLASS_COMMIT],
                },
                "unreachable": {
                    "count": unreachable_count,
                    "retained_because": unreachable_note,
                },
                "classes": {
                    CLASS_COMMIT: {"retained_because": reasons[CLASS_COMMIT]},
                    CLASS_EVENT: {"retained_because": reasons[CLASS_EVENT]},
                    CLASS_SNAPSHOT: {"retained_because": reasons[CLASS_SNAPSHOT]},
                    CLASS_EPOCH: {"retained_because": reasons[CLASS_EPOCH]},
                    CLASS_AUXILIARY: {"retained_because": reasons[CLASS_AUXILIARY]},
                },
            },
            # Backward-compatible top-level count remains store-global but is
            # sourced from the catalog, not a live filesystem walk.
            "object_count": totals["object_count"],
        }
