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
import zlib
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

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
        with sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

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

    def _put_object(self, conn: sqlite3.Connection, raw: bytes, now: str) -> str:
        digest = hashlib.sha256(raw).hexdigest()
        path = self._object_path(digest)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            compressed = zlib.compress(raw, level=9)
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
        size = path.stat().st_size
        conn.execute(
            """INSERT INTO transcript_objects(hash,codec,uncompressed_bytes,compressed_bytes,ref_count,created_at)
               VALUES(?, 'zlib', ?, ?, 1, ?)
               ON CONFLICT(hash) DO UPDATE SET ref_count=ref_count+1""",
            (digest, len(raw), size, now),
        )
        return digest

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
        mirrors: list[tuple[TranscriptEvent, dict]] = []
        with self._lock, self._conn() as conn:
            cold_batch_counts: dict[str, int] = {}
            existing: dict[tuple[str, int], str | None] = {}
            for session_id in {event.session_id for event in events}:
                seqs = [event.seq for event in events if event.session_id == session_id]
                for row in conn.execute(
                    "SELECT session_id,seq,cold_hash FROM transcript_events WHERE session_id=? AND seq BETWEEN ? AND ?",
                    (session_id, min(seqs), max(seqs)),
                ):
                    existing[(row["session_id"], int(row["seq"]))] = row["cold_hash"]
            for event in events:
                payload = redact_payload(event.payload)
                raw = _canonical_json(payload)
                payload_hash = hashlib.sha256(raw).hexdigest()
                cold_hash = None
                old_hash = existing.get((event.session_id, event.seq))
                hot_payload = payload
                if len(raw) > MAX_HOT_PAYLOAD_BYTES:
                    digest = hashlib.sha256(raw).hexdigest()
                    if digest in cold_batch_counts:
                        cold_hash = digest
                    else:
                        cold_hash = self._put_object(conn, raw, event.created_at.isoformat())
                    cold_batch_counts[digest] = cold_batch_counts.get(digest, 0) + 1
                    hot_payload = {
                        "storage": "cold", "payload_hash": payload_hash,
                        "bytes": len(raw), "preview": self._preview(payload),
                    }
                conn.execute(
                    """INSERT OR REPLACE INTO transcript_events
                       (id,session_id,seq,event_type,hot_payload,payload_hash,cold_hash,created_at,schema_version)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    (event.id, event.session_id, event.seq, event.event_type,
                     _canonical_json(hot_payload).decode(), payload_hash, cold_hash,
                     event.created_at.isoformat(), SCHEMA_VERSION),
                )
                if old_hash:
                    self._release_object(conn, old_hash)
                canonical = event.model_copy(update={"payload": payload})
                mirrors.append((canonical, self.compatibility_payload(payload, payload_hash, cold_hash)))
            for digest, count in cold_batch_counts.items():
                if count > 1:
                    conn.execute("UPDATE transcript_objects SET ref_count=ref_count+? WHERE hash=?", (count - 1, digest))
        return mirrors

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

    def find_prompt(self, session_id: str, prompt_id: str, *, queued_only: bool = False) -> TranscriptEvent | None:
        types = ("queue_enqueued",) if queued_only else ("queue_enqueued", "user_message")
        # Payloads may be cold, so keep this bounded and compare canonical data.
        with self._conn() as conn:
            marks = ",".join("?" for _ in types)
            rows = conn.execute(f"SELECT * FROM transcript_events WHERE session_id=? AND event_type IN ({marks}) ORDER BY seq DESC LIMIT 1000", [session_id, *types]).fetchall()
        for row in rows:
            event = self._event(row)
            if event.payload.get("id") == prompt_id:
                return event
        return None

    def find_prompt_lifecycle(
        self, session_id: str, prompt_id: str
    ) -> TranscriptEvent | None:
        """Return the newest bounded lifecycle evidence for one prompt id."""
        types = (
            "queue_enqueued",
            "queue_dequeued",
            "user_message",
            "prompt_blocked",
            "turn_completed",
            "error",
            "connection_lost",
        )
        with self._conn() as conn:
            marks = ",".join("?" for _ in types)
            rows = conn.execute(
                f"SELECT * FROM transcript_events WHERE session_id=? "
                f"AND event_type IN ({marks}) ORDER BY seq DESC LIMIT 1000",
                [session_id, *types],
            ).fetchall()
        for row in rows:
            event = self._event(row)
            payload = event.payload or {}
            if payload.get("id") == prompt_id or payload.get("queued_prompt_id") == prompt_id:
                return event
        return None

    def prune(self, session_ids: list[str], *, keep_audit: bool = True) -> int:
        if not session_ids: return 0
        changed = 0
        with self._lock, self._conn() as conn:
            for session_id in session_ids:
                if keep_audit:
                    marks = ",".join("?" for _ in _AUDIT_TYPES)
                    conn.execute(f"""INSERT OR IGNORE INTO transcript_evidence(session_id,seq,event_type,payload_hash,created_at)
                        SELECT session_id,seq,event_type,payload_hash,created_at FROM transcript_events
                        WHERE session_id=? AND event_type IN ({marks}) ORDER BY seq DESC LIMIT 16""", [session_id, *_AUDIT_TYPES])
                rows = conn.execute("SELECT seq,cold_hash FROM transcript_events WHERE session_id=?", (session_id,)).fetchall()
                for row in rows:
                    self._release_object(conn, row["cold_hash"])
                    conn.execute("DELETE FROM transcript_events WHERE session_id=? AND seq=?", (session_id, row["seq"]))
                    changed += 1
            conn.execute("DELETE FROM transcript_objects WHERE ref_count=0")
        self.collect_unreferenced_objects()
        return changed

    def collect_unreferenced_objects(self) -> int:
        with self._conn() as conn: live = {r[0] for r in conn.execute("SELECT hash FROM transcript_objects WHERE ref_count>0")}
        removed = 0
        for path in self.objects_dir.glob("*/*.zlib"):
            digest = path.parent.name + path.stem
            if digest not in live: path.unlink(missing_ok=True); removed += 1
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
