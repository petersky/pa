"""Bounded hot transcript index with compressed content-addressed cold objects.

The metadata projection deliberately owns no transcript bodies.  This store uses
its own WAL database and a sibling object directory, so ACP ingestion cannot
hold the metadata database writer lock.  The legacy projection table is kept as
a bounded compatibility mirror; it is sufficient for an old binary to show
ordering and audit evidence and can be rebuilt from this store for rollback.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import zlib
from collections import Counter, defaultdict
from contextlib import contextmanager, nullcontext
from datetime import datetime
from glob import iglob
from itertools import islice
from pathlib import Path
from typing import Any, Iterator

from pa.core.operation_budget import measured_lock, report_work_progress
from pa.core.performance import PerformanceMetrics, retention_phase
from pa.core.sqlite_metrics import ObservedConnection, derived_connection
from pa.domain.models import TranscriptEvent

SCHEMA_VERSION = 1
MAX_HOT_PAYLOAD_BYTES = 2048
MAX_PAGE_SIZE = 1001
_SECRET_KEY = re.compile(
    r"(^|_)(authorization|cookie|credential|password|passwd|secret|token|api_key|permission_response)(_|$)",
    re.I,
)
_BEARER = re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{8,}")
_TOKEN = re.compile(r"\b(?:sk|gh[oprsu]|xox[abprs])-[-A-Za-z0-9_]{8,}\b", re.I)
_WIRE_KEYS = {"headers", "http_headers", "wire", "wire_envelope", "provider_envelope"}
_AUDIT_TYPES = {
    "session_closed", "turn_completed", "turn_failed", "command_result",
    "queue_enqueued", "user_message", "final_message",
}


def _canonical_json(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode()


def redact_payload(value: Any, *, key: str = "") -> Any:
    """Return deterministic, provider-neutral transcript content."""
    if _SECRET_KEY.search(key):
        return "[REDACTED]"
    if key.lower() in _WIRE_KEYS:
        return "[OMITTED_PROVIDER_ENVELOPE]"
    if isinstance(value, dict):
        # Selected/raw aliases duplicate the canonical field and often contain
        # an unredacted provider body.  Never retain them.
        result = {}
        for child_key, child in value.items():
            normalized = str(child_key)
            if normalized.lower() in {
                "raw", "raw_input", "raw_output", "selected_input", "selected_output",
                "input_raw", "output_raw",
            }:
                continue
            result[normalized] = redact_payload(child, key=normalized)
        return result
    if isinstance(value, list):
        return [redact_payload(item, key=key) for item in value]
    if isinstance(value, str):
        return _TOKEN.sub("[REDACTED_TOKEN]", _BEARER.sub("[REDACTED_AUTH]", value))
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)


class TranscriptStorage:
    def __init__(self, metadata_db_path: Path) -> None:
        stem = metadata_db_path.stem
        self.db_path = metadata_db_path.with_name(f"{stem}.transcripts.db")
        self.objects_dir = metadata_db_path.parent / "transcript_objects"
        self.objects_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._object_readers = 0
        self.performance = PerformanceMetrics()
        with sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30, factory=ObservedConnection)
        try:
            conn.row_factory = sqlite3.Row
            self._set_connection_progress(conn)
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA synchronous=NORMAL")
            return conn
        except BaseException:
            conn.close()
            raise

    @staticmethod
    def _set_connection_progress(conn: sqlite3.Connection) -> None:
        from pa.core.operation_budget import current_operation
        budgeted = current_operation.get() is not None
        conn.set_progress_handler(
            (lambda: report_work_progress() or 0) if budgeted else None,
            1000 if budgeted else 0,
        )

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        # Reuse only within this one worker invocation. Direct callers still
        # close on exit; the runtime owns and closes every scoped handle when
        # its actual worker finishes, including late completion after timeout.
        with derived_connection(self, self._new_connection) as conn:
            self._set_connection_progress(conn)
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript("""
                CREATE TABLE IF NOT EXISTS transcript_events (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    hot_payload TEXT NOT NULL DEFAULT '{}',
                    payload_hash TEXT NOT NULL,
                    cold_hash TEXT,
                    created_at TEXT NOT NULL,
                    schema_version INTEGER NOT NULL DEFAULT 1,
                    UNIQUE(session_id, seq)
                );
                CREATE INDEX IF NOT EXISTS idx_transcript_events_session_seq
                    ON transcript_events(session_id, seq);
                CREATE INDEX IF NOT EXISTS idx_transcript_events_session_type_seq
                    ON transcript_events(session_id, event_type, seq);
                CREATE INDEX IF NOT EXISTS idx_transcript_events_cold_hash
                    ON transcript_events(cold_hash) WHERE cold_hash IS NOT NULL;
                CREATE TABLE IF NOT EXISTS transcript_objects (
                    hash TEXT PRIMARY KEY,
                    codec TEXT NOT NULL,
                    uncompressed_bytes INTEGER NOT NULL,
                    compressed_bytes INTEGER NOT NULL,
                    ref_count INTEGER NOT NULL CHECK(ref_count >= 0),
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transcript_operations (
                    name TEXT PRIMARY KEY,
                    cursor TEXT,
                    state TEXT NOT NULL,
                    examined INTEGER NOT NULL DEFAULT 0,
                    changed INTEGER NOT NULL DEFAULT 0,
                    error TEXT,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS transcript_evidence (
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, seq)
                );
                PRAGMA user_version=1;
            """)

    def _object_path(self, digest: str) -> Path:
        return self.objects_dir / digest[:2] / f"{digest[2:]}.zlib"

    def _prepare_object(self, digest: str, compressed: bytes) -> int:
        """Publish bytes before acquiring SQLite's writer, under the GC lock."""
        path = self._object_path(digest)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(f".tmp-{os.getpid()}-{threading.get_ident()}")
            try:
                with tmp.open("xb") as handle:
                    handle.write(compressed)
                    handle.flush()
                    os.fsync(handle.fileno())
                os.replace(tmp, path)
            except FileExistsError:
                pass
            finally:
                tmp.unlink(missing_ok=True)
        return path.stat().st_size

    def _release_object(self, conn: sqlite3.Connection, digest: str | None) -> None:
        if digest:
            conn.execute("UPDATE transcript_objects SET ref_count=ref_count-1 WHERE hash=? AND ref_count>0", (digest,))

    @staticmethod
    def compatibility_payload(payload: dict, payload_hash: str, cold_hash: str | None) -> dict:
        marker = {"storage": "cold" if cold_hash else "transcript-v1", "payload_hash": payload_hash}
        if cold_hash:
            marker["cold_hash"] = cold_hash
        return marker

    def append(self, events: list[TranscriptEvent]) -> list[tuple[TranscriptEvent, dict]]:
        if not events:
            return []
        started = time.perf_counter()
        prepared = []
        objects = {}
        input_bytes = 0
        for event in events:
            payload = redact_payload(event.payload)
            raw = _canonical_json(payload)
            input_bytes += len(raw)
            payload_hash = hashlib.sha256(raw).hexdigest()
            cold_hash = payload_hash if len(raw) > MAX_HOT_PAYLOAD_BYTES else None
            hot = payload
            if cold_hash:
                if cold_hash not in objects:
                    objects[cold_hash] = (len(raw), zlib.compress(raw, level=9), event.created_at.isoformat())
                hot = {"storage": "cold", "payload_hash": payload_hash,
                       "bytes": len(raw), "preview": self._preview(payload)}
            prepared.append((event, payload, payload_hash, cold_hash, _canonical_json(hot).decode()))
        self.performance.record("prepare_ms", (time.perf_counter() - started) * 1000)
        self.performance.record("input_rows", len(events))
        self.performance.record("canonical_payload_bytes", input_bytes)
        mirrors: list[tuple[TranscriptEvent, dict]] = []
        waiting = time.perf_counter()
        with measured_lock(self._lock, name="transcript.write"):
            self.performance.record("python_lock_wait_ms", (time.perf_counter() - waiting) * 1000)
            file_started = time.perf_counter()
            sizes = {digest: self._prepare_object(digest, compressed)
                     for digest, (_, compressed, _) in objects.items()}
            self.performance.record("object_publish_ms", (time.perf_counter() - file_started) * 1000)
            with self._conn() as conn:
                acquiring = time.perf_counter()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                except sqlite3.OperationalError as exc:
                    if (getattr(exc, "sqlite_errorcode", 0) & 0xFF) in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                        self.performance.record("sqlite_busy_failures", 1)
                    raise
                finally:
                    # Includes SQLite busy-handler wait and BEGIN execution;
                    # this is acquisition latency, not an exact busy-only timer.
                    self.performance.record("sqlite_writer_acquire_ms", (time.perf_counter() - acquiring) * 1000)
                holding = time.perf_counter()
                changes = conn.total_changes
                try:
                    existing = {}
                    by_id = {}
                    seqs = defaultdict(set)
                    for event in events:
                        seqs[event.session_id].add(event.seq)
                    lookup_rows = 0
                    def remember(rows):
                        nonlocal lookup_rows
                        for row in rows:
                            lookup_rows += 1
                            key = (row["session_id"], int(row["seq"]))
                            existing[key] = (row["id"], row["cold_hash"])
                            by_id[row["id"]] = key
                    for session_id, sequences in seqs.items():
                        values = sorted(sequences)
                        for offset in range(0, len(values), 500):
                            chunk = values[offset:offset + 500]
                            marks = ",".join("?" for _ in chunk)
                            remember(conn.execute(
                                f"SELECT id,session_id,seq,cold_hash FROM transcript_events WHERE session_id=? AND seq IN ({marks})",
                                [session_id, *chunk],
                            ))
                    ids = list({event.id for event in events})
                    for offset in range(0, len(ids), 500):
                        chunk = ids[offset:offset + 500]
                        marks = ",".join("?" for _ in chunk)
                        remember(conn.execute(
                            f"SELECT id,session_id,seq,cold_hash FROM transcript_events WHERE id IN ({marks})", chunk,
                        ))
                    self.performance.record("lookup_rows_returned", lookup_rows)
                    deltas = Counter()
                    for event, payload, payload_hash, cold_hash, hot_json in prepared:
                        key = (event.session_id, event.seq)
                        replaced = {key}
                        if event.id in by_id:
                            replaced.add(by_id[event.id])
                        for old_key in replaced:
                            old = existing.pop(old_key, None)
                            if old:
                                by_id.pop(old[0], None)
                                if old[1]:
                                    deltas[old[1]] -= 1
                        existing[key] = (event.id, cold_hash)
                        by_id[event.id] = key
                        if cold_hash:
                            deltas[cold_hash] += 1
                        conn.execute(
                            """INSERT OR REPLACE INTO transcript_events
                               (id,session_id,seq,event_type,hot_payload,payload_hash,cold_hash,created_at,schema_version)
                               VALUES(?,?,?,?,?,?,?,?,?)""",
                            (event.id, event.session_id, event.seq, event.event_type,
                             hot_json, payload_hash, cold_hash, event.created_at.isoformat(), SCHEMA_VERSION),
                        )
                        canonical = event.model_copy(update={"payload": payload})
                        mirrors.append((canonical, self.compatibility_payload(payload, payload_hash, cold_hash)))
                    for digest, delta in deltas.items():
                        if delta > 0:
                            raw_size, _, now = objects[digest]
                            conn.execute(
                                """INSERT INTO transcript_objects(hash,codec,uncompressed_bytes,compressed_bytes,ref_count,created_at)
                                   VALUES(?, 'zlib', ?, ?, ?, ?)
                                   ON CONFLICT(hash) DO UPDATE SET ref_count=ref_count+excluded.ref_count""",
                                (digest, raw_size, sizes[digest], delta, now),
                            )
                        elif delta < 0:
                            conn.execute("UPDATE transcript_objects SET ref_count=ref_count+? WHERE hash=?", (delta, digest))
                    conn.commit()
                    self.performance.record("sqlite_rows_committed", conn.total_changes - changes)
                finally:
                    if conn.in_transaction:
                        conn.rollback()
                    self.performance.record("transaction_hold_ms", (time.perf_counter() - holding) * 1000)
                    self.performance.record("sqlite_row_changes_attempted", conn.total_changes - changes)
        return mirrors

    def recent_lifecycle_summaries(self, session_ids: list[str], *, limit: int = 100) -> dict:
        """Reuse one read connection for a page's bounded lifecycle lookups."""
        with self._conn() as conn:
            return {session_id: self.recent_lifecycle_summary(session_id, limit=limit, _connection=conn)
                    for session_id in session_ids}

    def recent_lifecycle_summary(self, session_id: str, *, limit: int = 100, _connection=None) -> dict:
        """Sidebar metadata without hydrating compressed transcript bodies."""
        with (nullcontext(_connection) if _connection is not None else self._conn()) as conn:
            rows = conn.execute(
                """SELECT event_type, hot_payload FROM (
                    SELECT event_type, hot_payload, seq FROM transcript_events
                    WHERE session_id=? ORDER BY seq DESC LIMIT ?
                ) WHERE event_type IN ('session_started','session_admission_failed','session_closed')
                ORDER BY seq DESC""",
                (session_id, min(MAX_PAGE_SIZE, max(1, limit))),
            ).fetchall()
        closure = next((json.loads(row["hot_payload"]) for row in rows
                        if row["event_type"] == "session_closed"), {})
        return {
            "provider_attempts": sum(row["event_type"] != "session_closed" for row in rows),
            "closure_reason": closure.get("reason"),
        }

    @staticmethod
    def _preview(payload: Any) -> Any:
        if isinstance(payload, dict):
            for key in ("text", "message", "summary", "status", "reason"):
                value = payload.get(key)
                if isinstance(value, str):
                    return {key: value[:256]}
            return {"keys": sorted(payload)[:16]}
        return str(payload)[:256]

    def _payload(self, row: sqlite3.Row) -> dict:
        hot = json.loads(row["hot_payload"] or "{}")
        digest = row["cold_hash"]
        if not digest:
            return hot
        path = self._object_path(digest)
        if not path.exists():
            return {**hot, "availability": "missing", "cold_hash": digest}
        try:
            raw = zlib.decompress(path.read_bytes())
        except (OSError, zlib.error):
            return {**hot, "availability": "corrupt", "cold_hash": digest}
        if hashlib.sha256(raw).hexdigest() != digest or hashlib.sha256(raw).hexdigest() != row["payload_hash"]:
            return {**hot, "availability": "corrupt", "cold_hash": digest}
        return json.loads(raw)

    def _event(self, row: sqlite3.Row) -> TranscriptEvent:
        return TranscriptEvent(id=row["id"], session_id=row["session_id"], seq=row["seq"],
            event_type=row["event_type"], payload=self._payload(row),
            created_at=datetime.fromisoformat(row["created_at"]))

    def list(self, session_id: str, *, after_seq: int = 0, limit: int = 500) -> list[TranscriptEvent]:
        limit = max(1, min(int(limit), MAX_PAGE_SIZE))
        with self._conn() as conn:
            rows = conn.execute("SELECT * FROM transcript_events WHERE session_id=? AND seq>? ORDER BY seq LIMIT ?", (session_id, after_seq, limit)).fetchall()
        return [self._event(row) for row in rows]

    def list_before(self, session_id: str, *, before_seq: int | None = None, limit: int = 500) -> list[TranscriptEvent]:
        limit = max(1, min(int(limit), MAX_PAGE_SIZE))
        clause, params = ("AND seq < ?", [session_id, before_seq]) if before_seq is not None else ("", [session_id])
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(f"SELECT * FROM (SELECT * FROM transcript_events WHERE session_id=? {clause} ORDER BY seq DESC LIMIT ?) ORDER BY seq", params).fetchall()
        return [self._event(row) for row in rows]

    def list_range(self, session_id: str, *, start_seq: int | None = None, end_seq: int | None = None, limit: int = MAX_PAGE_SIZE) -> list[TranscriptEvent]:
        sql, params = "SELECT * FROM transcript_events WHERE session_id=?", [session_id]
        if start_seq is not None: sql += " AND seq>=?"; params.append(start_seq)
        if end_seq is not None: sql += " AND seq<=?"; params.append(end_seq)
        sql += " ORDER BY seq LIMIT ?"; params.append(max(1, min(limit, MAX_PAGE_SIZE)))
        with self._conn() as conn: rows = conn.execute(sql, params).fetchall()
        return [self._event(row) for row in rows]

    def next_seq(self, session_id: str) -> int:
        with self._conn() as conn: row = conn.execute("SELECT COALESCE(MAX(seq),0) FROM transcript_events WHERE session_id=?", (session_id,)).fetchone()
        return int(row[0]) + 1

    def _find_prompt_event(
        self, session_id: str, prompt_id: str, types: tuple[str, ...],
        *, newest: bool, fields: tuple[str, ...],
    ) -> TranscriptEvent | None:
        """Search exact identity through bounded pages, without a history cutoff.

        Legacy/cold payloads do not expose prompt IDs in the hot index. Page by
        sequence until a match or exhaustion, rather than treating a recent
        window's absence as proof that a prompt was never accepted.
        """
        cursor = None
        order, comparison = ("DESC", "<") if newest else ("ASC", ">")
        marks = ",".join("?" for _ in types)
        while True:
            params = [session_id, *types]
            after = ""
            if cursor is not None:
                after = f" AND seq {comparison} ?"
                params.append(cursor)
            with self._conn() as conn:
                rows = conn.execute(
                    f"SELECT * FROM transcript_events WHERE session_id=? "
                    f"AND event_type IN ({marks}){after} ORDER BY seq {order} LIMIT 256",
                    params,
                ).fetchall()
            for row in rows:
                event = self._event(row)
                if any(event.payload.get(field) == prompt_id for field in fields):
                    return event
            if len(rows) < 256:
                return None
            cursor = rows[-1]["seq"]

    def find_prompt(self, session_id: str, prompt_id: str, *, queued_only: bool = False) -> TranscriptEvent | None:
        types = ("queue_enqueued",) if queued_only else ("queue_enqueued", "user_message")
        # Preserve the original acceptance even after dequeue/execution.
        return self._find_prompt_event(session_id, prompt_id, types,
                                       newest=False, fields=("id",))

    def find_prompt_completion(self, session_id: str, prompt_id: str) -> TranscriptEvent | None:
        return self._find_prompt_event(session_id, prompt_id, ("turn_completed",),
                                       newest=True, fields=("queued_prompt_id",))

    def find_prompt_lifecycle(self, session_id: str, prompt_id: str) -> TranscriptEvent | None:
        types = ("queue_enqueued", "queue_dequeued", "user_message", "prompt_blocked",
                 "turn_completed", "error", "connection_lost")
        return self._find_prompt_event(session_id, prompt_id, types, newest=True,
                                       fields=("id", "queued_prompt_id"))

    def prune(self, session_ids: list[str], *, keep_audit: bool = True) -> int:
        if not session_ids: return 0
        changed = 0
        with (retention_phase("retention_transaction"),
              measured_lock(self._lock, name="transcript.write"), self._conn() as conn):
            # Publication is fenced for this entire transaction, so discover
            # nonempty sessions once rather than querying every expired ID on
            # each sweep. A single JSON parameter avoids the variable limit.
            with retention_phase("retention_discovery"):
                existing = {row[0] for row in conn.execute(
                    """SELECT DISTINCT session_id FROM transcript_events
                       WHERE session_id IN (SELECT value FROM json_each(?))""",
                    (json.dumps(session_ids),),
                )}
            for session_id in dict.fromkeys(session_ids):
                if session_id not in existing:
                    continue
                # A leading WITH ... UPDATE does not start Python sqlite's
                # legacy implicit transaction. Begin before either mutation so
                # non-audit pruning also rolls references back with deletion.
                if not conn.in_transaction:
                    conn.execute("BEGIN")
                if keep_audit:
                    marks = ",".join("?" for _ in _AUDIT_TYPES)
                    conn.execute(f"""INSERT OR IGNORE INTO transcript_evidence(session_id,seq,event_type,payload_hash,created_at)
                        SELECT session_id,seq,event_type,payload_hash,created_at FROM transcript_events
                        WHERE session_id=? AND event_type IN ({marks}) ORDER BY seq DESC LIMIT 16""", [session_id, *_AUDIT_TYPES])
                # Same positive-only decrements as releasing each event, grouped
                # before deletion while the original publication lock and one
                # transaction still fence all affected references.
                conn.execute(
                    """WITH releases AS (
                           SELECT cold_hash,COUNT(*) AS amount FROM transcript_events
                           WHERE session_id=? AND cold_hash IS NOT NULL AND cold_hash!=''
                           GROUP BY cold_hash
                       )
                       UPDATE transcript_objects AS obj
                       SET ref_count=MAX(0,obj.ref_count-releases.amount)
                       FROM releases
                       WHERE obj.hash=releases.cold_hash AND obj.ref_count>0""",
                    (session_id,),
                )
                deleted = conn.execute(
                    "DELETE FROM transcript_events WHERE session_id=?", (session_id,)
                )
                changed += max(0, deleted.rowcount)
            conn.execute("DELETE FROM transcript_objects WHERE ref_count=0")
        with retention_phase("retention_object_collection"):
            self.collect_unreferenced_objects()
        return changed

    @contextmanager
    def retain_objects(self) -> Iterator[None]:
        """Keep immutable bytes alive while a reader captures its own pins.

        This does not hold the publication lock across reader disk I/O. Pruning
        can still commit metadata; GC leaves the bytes for its next normal pass.
        """
        with measured_lock(self._lock, name="transcript.write", track_deadline=False):
            self._object_readers += 1
        try:
            yield
        finally:
            with measured_lock(self._lock, name="transcript.write", track_deadline=False):
                self._object_readers -= 1

    def collect_unreferenced_objects(self) -> int:
        # Shares the publication lock so GC cannot remove newly published
        # bytes before reference commit. Walk outside that lock, then recheck
        # only each bounded batch; never hydrate the entire live object index.
        removed = 0
        with measured_lock(self._lock, name="transcript.write", track_deadline=False):
            if self._object_readers:
                return removed
        # Preserve hidden-name matching without constructing a full Path for
        # every live object, or parsing its parent and stem twice.
        paths = iglob("*/*.zlib", root_dir=self.objects_dir, include_hidden=True)
        while batch := list(islice(paths, 128)):
            digests = [
                directory + Path(name).stem
                for directory, name in map(os.path.split, batch)
            ]
            with measured_lock(self._lock, name="transcript.write"), self._conn() as conn:
                if self._object_readers:
                    return removed
                marks = ",".join("?" for _ in digests)
                live = {row[0] for row in conn.execute(
                    f"SELECT hash FROM transcript_objects WHERE ref_count>0 AND hash IN ({marks})", digests,
                )}
                for path, digest in zip(batch, digests):
                    if digest not in live:
                        (self.objects_dir / path).unlink(missing_ok=True)
                        removed += 1
        return removed

    def metrics(self) -> dict[str, Any]:
        with self._conn() as conn:
            event_count = conn.execute("SELECT COUNT(*) FROM transcript_events").fetchone()[0]
            evidence_count = conn.execute("SELECT COUNT(*) FROM transcript_evidence").fetchone()[0]
            objects = conn.execute("SELECT COUNT(*),COALESCE(SUM(uncompressed_bytes),0),COALESCE(SUM(compressed_bytes),0),COALESCE(SUM(ref_count),0) FROM transcript_objects").fetchone()
            integrity = conn.execute("PRAGMA quick_check").fetchone()[0]
            journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
        missing = sum(1 for digest in self.referenced_hashes() if not self._object_path(digest).exists())
        return {"schema_version": SCHEMA_VERSION, "events": event_count, "audit_evidence": evidence_count, "objects": objects[0],
            "uncompressed_bytes": objects[1], "compressed_bytes": objects[2], "references": objects[3],
            "deduplicated_references": max(0, objects[3] - objects[0]), "missing_objects": missing,
            "journal_mode": journal, "integrity": integrity, "redaction_policy": "canonical-v1",
            "hot_payload_limit": MAX_HOT_PAYLOAD_BYTES}

    def referenced_hashes(self) -> set[str]:
        with self._conn() as conn: return {r[0] for r in conn.execute("SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL")}

    def operation(self, name: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM transcript_operations WHERE name=?", (name,)).fetchone()
        return dict(row) if row else None

    def record_operation(self, name: str, *, cursor: str | None, state: str, examined: int, changed: int, error: str | None = None) -> None:
        with self._conn() as conn:
            conn.execute("""INSERT INTO transcript_operations(name,cursor,state,examined,changed,error,updated_at)
                VALUES(?,?,?,?,?,?,?) ON CONFLICT(name) DO UPDATE SET cursor=excluded.cursor,state=excluded.state,
                examined=transcript_operations.examined+excluded.examined,changed=transcript_operations.changed+excluded.changed,
                error=excluded.error,updated_at=excluded.updated_at""",
                (name, cursor, state, examined, changed, error, datetime.now().astimezone().isoformat()))
