"""Disposable SQLite index for the immutable sync DAG.

The object store and durable refs remain authoritative.  This database contains
only normalized provenance and decoded event metadata and may be deleted and
rebuilt at any time.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pa.domain.models import CardEvent, SyncCommit


SCHEMA_VERSION = 1


class DagIndex:
    """Realm-scoped, WAL-mode derived index with atomically published heads."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._build_lock = threading.Lock()
        self._initialize()

    def _conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=5.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        return conn

    def _initialize(self) -> None:
        with self._conn() as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS index_meta (
                    realm_id TEXT PRIMARY KEY,
                    schema_version INTEGER NOT NULL,
                    indexed_head TEXT,
                    generation INTEGER NOT NULL DEFAULT 0,
                    state TEXT NOT NULL,
                    last_success_at TEXT,
                    last_failure TEXT,
                    commit_count INTEGER NOT NULL DEFAULT 0,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    build_elapsed_ms REAL NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS commits (
                    realm_id TEXT NOT NULL,
                    commit_hash TEXT NOT NULL,
                    position INTEGER NOT NULL,
                    instance_id TEXT NOT NULL,
                    author_principal TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    PRIMARY KEY (realm_id, commit_hash),
                    UNIQUE (realm_id, position)
                );
                CREATE TABLE IF NOT EXISTS commit_parents (
                    realm_id TEXT NOT NULL,
                    commit_hash TEXT NOT NULL,
                    parent_hash TEXT NOT NULL,
                    parent_order INTEGER NOT NULL,
                    PRIMARY KEY (realm_id, commit_hash, parent_order)
                );
                CREATE INDEX IF NOT EXISTS ix_commit_parent
                    ON commit_parents(realm_id, parent_hash, commit_hash);
                CREATE TABLE IF NOT EXISTS reachability (
                    realm_id TEXT NOT NULL,
                    descendant_hash TEXT NOT NULL,
                    ancestor_hash TEXT NOT NULL,
                    PRIMARY KEY (realm_id, descendant_hash, ancestor_hash)
                ) WITHOUT ROWID;
                CREATE INDEX IF NOT EXISTS ix_reach_ancestor
                    ON reachability(realm_id, ancestor_hash, descendant_hash);
                CREATE TABLE IF NOT EXISTS entity_events (
                    realm_id TEXT NOT NULL,
                    commit_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    event_order INTEGER NOT NULL,
                    entity_type TEXT,
                    entity_id TEXT,
                    event_type TEXT NOT NULL,
                    idempotency_key TEXT,
                    event_json TEXT NOT NULL,
                    PRIMARY KEY (realm_id, commit_hash, event_order)
                );
                CREATE INDEX IF NOT EXISTS ix_entity_history
                    ON entity_events(realm_id, entity_type, entity_id, commit_hash, event_order);
                CREATE INDEX IF NOT EXISTS ix_operation_event
                    ON entity_events(realm_id, idempotency_key)
                    WHERE idempotency_key IS NOT NULL;
                """
            )

    def indexed_head(self, realm_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT indexed_head FROM index_meta WHERE realm_id=? AND state='ready'",
                (realm_id,),
            ).fetchone()
        return str(row[0]) if row and row[0] else None

    def contains_commit(self, realm_id: str, commit_hash: str) -> bool:
        with self._conn() as conn:
            return conn.execute(
                "SELECT 1 FROM commits WHERE realm_id=? AND commit_hash=?",
                (realm_id, commit_hash),
            ).fetchone() is not None

    @staticmethod
    def _ancestor_hashes(
        conn: sqlite3.Connection, realm_id: str, head: str, *, limit: int = 100_000
    ) -> set[str]:
        parents: dict[str, list[str]] = {}
        for row in conn.execute(
            """SELECT commit_hash,parent_hash FROM commit_parents
               WHERE realm_id=? ORDER BY commit_hash,parent_order""",
            (realm_id,),
        ):
            parents.setdefault(str(row[0]), []).append(str(row[1]))
        found: set[str] = set()
        stack = [head]
        while stack:
            current = stack.pop()
            if current in found:
                continue
            found.add(current)
            if len(found) > limit:
                raise RuntimeError("indexed ancestry limit exceeded")
            stack.extend(reversed(parents.get(current, [])))
        return found

    def status(self, realm_id: str, durable_head: str | None = None) -> dict[str, Any]:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM index_meta WHERE realm_id=?", (realm_id,)
            ).fetchone()
        if row is None:
            return {
                "state": "stale" if durable_head else "ready",
                "schema_version": SCHEMA_VERSION,
                "indexed_head": None,
                "durable_head": durable_head,
                "ready": durable_head is None,
                "commit_count": 0,
                "event_count": 0,
            }
        result = dict(row)
        result["durable_head"] = durable_head
        result["ready"] = result["state"] == "ready" and result["indexed_head"] == durable_head
        if result["state"] == "ready" and not result["ready"]:
            result["state"] = "stale"
        return result

    @staticmethod
    def _insert_commit(
        conn: sqlite3.Connection,
        realm_id: str,
        position: int,
        commit_hash: str,
        commit: SyncCommit,
        events: list[tuple[str, CardEvent]],
        event_entity: Callable[[CardEvent], tuple[str | None, str | None]],
    ) -> None:
        conn.execute(
            "INSERT INTO commits VALUES(?,?,?,?,?,?)",
            (
                realm_id,
                commit_hash,
                position,
                commit.instance_id,
                commit.author_principal,
                commit.timestamp.isoformat(),
            ),
        )
        conn.execute(
            "INSERT INTO reachability VALUES(?,?,?)",
            (realm_id, commit_hash, commit_hash),
        )
        for order, parent in enumerate(commit.parent_hashes):
            conn.execute(
                "INSERT INTO commit_parents VALUES(?,?,?,?)",
                (realm_id, commit_hash, parent, order),
            )
        for order, (event_hash, event) in enumerate(events):
            entity_type, entity_id = event_entity(event)
            conn.execute(
                "INSERT INTO entity_events VALUES(?,?,?,?,?,?,?,?,?)",
                (
                    realm_id,
                    commit_hash,
                    event_hash,
                    order,
                    entity_type,
                    entity_id,
                    event.type.value,
                    event.idempotency_key,
                    json.dumps(event.model_dump(mode="json"), separators=(",", ":")),
                ),
            )

    def advance_linear(
        self,
        realm_id: str,
        parent: str | None,
        commit_hash: str,
        commit: SyncCommit,
        events: list[tuple[str, CardEvent]],
        event_entity: Callable[[CardEvent], tuple[str | None, str | None]],
    ) -> bool:
        """Advance one local commit when the published index is exactly fenced."""
        with self._conn() as conn:
            meta = conn.execute(
                "SELECT indexed_head, commit_count, event_count FROM index_meta WHERE realm_id=?",
                (realm_id,),
            ).fetchone()
            indexed = str(meta[0]) if meta and meta[0] else None
            if indexed != parent:
                return False
            position = int(meta[1]) if meta else 0
            self._insert_commit(
                conn, realm_id, position, commit_hash, commit, events, event_entity
            )
            conn.execute(
                """INSERT INTO index_meta(
                       realm_id,schema_version,indexed_head,generation,state,last_success_at,
                       commit_count,event_count)
                   VALUES(?,?,?,?,?,?,?,?)
                   ON CONFLICT(realm_id) DO UPDATE SET
                       indexed_head=excluded.indexed_head,state='ready',
                       last_success_at=excluded.last_success_at,last_failure=NULL,
                       commit_count=excluded.commit_count,event_count=excluded.event_count""",
                (
                    realm_id,
                    SCHEMA_VERSION,
                    commit_hash,
                    1,
                    "ready",
                    datetime.now(UTC).isoformat(),
                    position + 1,
                    (int(meta[2]) if meta else 0) + len(events),
                ),
            )
        return True

    def rebuild(
        self,
        realm_id: str,
        head: str,
        commits: Iterable[tuple[str, SyncCommit, list[tuple[str, CardEvent]]]],
        event_entity: Callable[[CardEvent], tuple[str | None, str | None]],
        *,
        force: bool = False,
    ) -> None:
        """Coalesce a verified authoritative traversal into one publication."""
        started = time.perf_counter()
        with self._build_lock:
            with self._conn() as conn:
                current = conn.execute(
                    "SELECT indexed_head FROM index_meta WHERE realm_id=? AND state='ready'",
                    (realm_id,),
                ).fetchone()
                if current and current[0] == head and not force:
                    return
                generation = conn.execute(
                    "SELECT COALESCE(MAX(generation),0)+1 FROM index_meta WHERE realm_id=?",
                    (realm_id,),
                ).fetchone()[0]
                conn.execute(
                    """INSERT INTO index_meta(realm_id,schema_version,generation,state)
                       VALUES(?,?,?,'rebuilding') ON CONFLICT(realm_id) DO UPDATE SET
                       generation=excluded.generation,state='rebuilding',last_failure=NULL""",
                    (realm_id, SCHEMA_VERSION, generation),
                )
                for table in ("entity_events", "reachability", "commit_parents", "commits"):
                    conn.execute(f"DELETE FROM {table} WHERE realm_id=?", (realm_id,))
                event_count = 0
                position = 0
                for commit_hash, commit, events in commits:
                    if commit.realm_id != realm_id:
                        raise ValueError(f"commit {commit_hash} belongs to another realm")
                    self._insert_commit(
                        conn, realm_id, position, commit_hash, commit, events, event_entity
                    )
                    event_count += len(events)
                    position += 1
                conn.execute(
                    """UPDATE index_meta SET indexed_head=?,state='ready',last_success_at=?,
                       commit_count=?,event_count=?,build_elapsed_ms=? WHERE realm_id=?""",
                    (
                        head,
                        datetime.now(UTC).isoformat(),
                        position,
                        event_count,
                        (time.perf_counter() - started) * 1000,
                        realm_id,
                    ),
                )

    def mark_failed(self, realm_id: str, reason: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """INSERT INTO index_meta(realm_id,schema_version,state,last_failure)
                   VALUES(?,?,'failed',?) ON CONFLICT(realm_id) DO UPDATE SET
                   state='failed',last_failure=excluded.last_failure""",
                (realm_id, SCHEMA_VERSION, reason[:1000]),
            )

    def reset_realm(self, realm_id: str) -> None:
        """Discard one realm's derived records; canonical objects are untouched."""
        with self._build_lock, self._conn() as conn:
            for table in ("entity_events", "reachability", "commit_parents", "commits"):
                conn.execute(f"DELETE FROM {table} WHERE realm_id=?", (realm_id,))
            conn.execute("DELETE FROM index_meta WHERE realm_id=?", (realm_id,))

    def entity_rows(
        self, realm_id: str, head: str, entity: str, entity_id: str
    ) -> list[sqlite3.Row] | None:
        if not self.contains_commit(realm_id, head):
            return None
        with self._conn() as conn:
            current = conn.execute(
                "SELECT indexed_head FROM index_meta WHERE realm_id=? AND state='ready'",
                (realm_id,),
            ).fetchone()
            if current and current[0] == head:
                return conn.execute(
                    """SELECT e.*, c.position, c.instance_id, c.author_principal,
                              c.timestamp,
                              (SELECT GROUP_CONCAT(parent_hash, char(31)) FROM (
                                 SELECT parent_hash FROM commit_parents cp
                                 WHERE cp.realm_id=e.realm_id
                                   AND cp.commit_hash=e.commit_hash
                                 ORDER BY parent_order
                              )) AS parents
                       FROM entity_events e JOIN commits c
                         ON c.realm_id=e.realm_id AND c.commit_hash=e.commit_hash
                       WHERE e.realm_id=? AND e.entity_type=? AND e.entity_id=?
                       ORDER BY c.position,e.event_order""",
                    (realm_id, entity, entity_id),
                ).fetchall()
            ancestors = self._ancestor_hashes(conn, realm_id, head)
            rows = conn.execute(
                """SELECT e.*, c.position, c.instance_id, c.author_principal,
                          c.timestamp,
                          (SELECT GROUP_CONCAT(parent_hash, char(31)) FROM (
                             SELECT parent_hash FROM commit_parents cp
                             WHERE cp.realm_id=e.realm_id
                               AND cp.commit_hash=e.commit_hash
                             ORDER BY parent_order
                          )) AS parents
                   FROM entity_events e JOIN commits c
                     ON c.realm_id=e.realm_id AND c.commit_hash=e.commit_hash
                   WHERE e.realm_id=? AND e.entity_type=? AND e.entity_id=?
                   ORDER BY c.position,e.event_order""",
                (realm_id, entity, entity_id),
            ).fetchall()
            return [row for row in rows if str(row["commit_hash"]) in ancestors]

    def operation_rows(self, realm_id: str, head: str, key: str) -> list[sqlite3.Row] | None:
        if not self.contains_commit(realm_id, head):
            return None
        with self._conn() as conn:
            current = conn.execute(
                "SELECT indexed_head FROM index_meta WHERE realm_id=? AND state='ready'",
                (realm_id,),
            ).fetchone()
            if current and current[0] == head:
                return conn.execute(
                    """SELECT e.* FROM entity_events e JOIN commits c
                         ON c.realm_id=e.realm_id AND c.commit_hash=e.commit_hash
                       WHERE e.realm_id=? AND e.idempotency_key=?
                       ORDER BY c.position,e.event_order""",
                    (realm_id, key),
                ).fetchall()
            ancestors = self._ancestor_hashes(conn, realm_id, head)
            rows = conn.execute(
                """SELECT e.* FROM entity_events e
                   WHERE e.realm_id=? AND e.idempotency_key=?
                   ORDER BY (SELECT position FROM commits c WHERE c.realm_id=e.realm_id
                             AND c.commit_hash=e.commit_hash), e.event_order""",
                (realm_id, key),
            ).fetchall()
            return [row for row in rows if str(row["commit_hash"]) in ancestors]

    def is_ancestor(self, realm_id: str, ancestor: str, descendant: str) -> bool | None:
        if not self.contains_commit(realm_id, descendant):
            return None
        with self._conn() as conn:
            current = conn.execute(
                "SELECT indexed_head FROM index_meta WHERE realm_id=? AND state='ready'",
                (realm_id,),
            ).fetchone()
            if current and current[0] == descendant:
                return conn.execute(
                    "SELECT 1 FROM commits WHERE realm_id=? AND commit_hash=?",
                    (realm_id, ancestor),
                ).fetchone() is not None
            return ancestor in self._ancestor_hashes(conn, realm_id, descendant)
