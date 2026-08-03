from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

from pa.collaboration.models import (
    CollaborationPolicy,
    CommandCatalog,
    CommandResult,
    ModeTransitionRequest,
    ModeTransitionResult,
    PolicyDecision,
)


def request_fingerprint(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json", exclude={"idempotency_key"})
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


class IdempotencyConflict(ValueError):
    pass


class CollaborationStore:
    """Local authority ledger for collaboration policy and commands.

    The PA server is the sole writer. Fleet callers route to the session owner,
    so this database remains an authority-local recovery ledger rather than a
    second writer for realm/card data.
    """

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "collaboration.db"
        self._lock = RLock()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path, timeout=30)
        try:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA foreign_keys=ON")
            with conn:
                yield conn
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._lock, self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS collaboration_policies (
                    id TEXT PRIMARY KEY,
                    scope_type TEXT NOT NULL,
                    scope_id TEXT NOT NULL,
                    provider TEXT,
                    version INTEGER NOT NULL,
                    enabled INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collaboration_policy_scope
                    ON collaboration_policies(scope_type, scope_id, provider, enabled);

                CREATE TABLE IF NOT EXISTS collaboration_decisions (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    dispatch_id TEXT,
                    card_id TEXT,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_collaboration_decision_session
                    ON collaboration_decisions(session_id, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_collaboration_decision_dispatch
                    ON collaboration_decisions(dispatch_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS collaboration_mode_requests (
                    request_id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    request_payload TEXT NOT NULL,
                    result_payload TEXT NOT NULL,
                    pending INTEGER NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(session_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS idx_collaboration_pending
                    ON collaboration_mode_requests(session_id, pending, updated_at);

                CREATE TABLE IF NOT EXISTS session_command_catalogs (
                    session_id TEXT NOT NULL,
                    generation INTEGER NOT NULL,
                    connection_generation INTEGER NOT NULL,
                    provider TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    active INTEGER NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(session_id, generation)
                );
                CREATE INDEX IF NOT EXISTS idx_command_catalog_active
                    ON session_command_catalogs(session_id, active, generation DESC);

                CREATE TABLE IF NOT EXISTS session_command_results (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    request_payload TEXT NOT NULL,
                    result_payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(session_id, idempotency_key)
                );
                CREATE INDEX IF NOT EXISTS idx_command_results_session
                    ON session_command_results(session_id, created_at DESC);
                """
            )

    def list_policies(self) -> list[CollaborationPolicy]:
        with self._lock, self._conn() as conn:
            rows = conn.execute(
                "SELECT payload FROM collaboration_policies ORDER BY updated_at DESC, id"
            ).fetchall()
        return [CollaborationPolicy.model_validate_json(row["payload"]) for row in rows]

    def get_policy(self, policy_id: str) -> CollaborationPolicy | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT payload FROM collaboration_policies WHERE id=?", (policy_id,)
            ).fetchone()
        return CollaborationPolicy.model_validate_json(row["payload"]) if row else None

    def save_policy(
        self, policy: CollaborationPolicy, *, expected_version: int | None = None
    ) -> CollaborationPolicy:
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                "SELECT version FROM collaboration_policies WHERE id=?", (policy.id,)
            ).fetchone()
            observed = int(existing["version"]) if existing else None
            if expected_version is not None and observed != expected_version:
                raise ValueError(
                    f"stale policy version: expected {expected_version}, observed {observed}"
                )
            if existing:
                policy = policy.model_copy(
                    update={
                        "version": observed + 1,
                        "updated_at": datetime.now(UTC),
                    }
                )
            conn.execute(
                """
                INSERT OR REPLACE INTO collaboration_policies
                    (id, scope_type, scope_id, provider, version, enabled, payload, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    policy.id,
                    policy.scope_type.value,
                    policy.scope_id,
                    policy.provider,
                    policy.version,
                    int(policy.enabled),
                    policy.model_dump_json(),
                    policy.updated_at.isoformat(),
                ),
            )
        return policy

    def record_decision(
        self,
        decision: PolicyDecision,
        *,
        session_id: str | None = None,
        dispatch_id: str | None = None,
        card_id: str | None = None,
    ) -> PolicyDecision:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO collaboration_decisions
                    (id, session_id, dispatch_id, card_id, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    decision.id,
                    session_id,
                    dispatch_id,
                    card_id,
                    decision.model_dump_json(),
                    decision.decided_at.isoformat(),
                ),
            )
        return decision

    def latest_decision(
        self, *, session_id: str | None = None, dispatch_id: str | None = None
    ) -> PolicyDecision | None:
        if not session_id and not dispatch_id:
            return None
        column, value = (
            ("session_id", session_id) if session_id else ("dispatch_id", dispatch_id)
        )
        with self._lock, self._conn() as conn:
            row = conn.execute(
                f"SELECT payload FROM collaboration_decisions WHERE {column}=? ORDER BY created_at DESC LIMIT 1",
                (value,),
            ).fetchone()
        return PolicyDecision.model_validate_json(row["payload"]) if row else None

    def get_mode_request(
        self, session_id: str, idempotency_key: str
    ) -> tuple[str, ModeTransitionRequest, ModeTransitionResult] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT fingerprint, request_payload, result_payload
                FROM collaboration_mode_requests
                WHERE session_id=? AND idempotency_key=?
                """,
                (session_id, idempotency_key),
            ).fetchone()
        if not row:
            return None
        return (
            row["fingerprint"],
            ModeTransitionRequest.model_validate_json(row["request_payload"]),
            ModeTransitionResult.model_validate_json(row["result_payload"]),
        )

    def save_mode_request(
        self,
        request: ModeTransitionRequest,
        result: ModeTransitionResult,
    ) -> ModeTransitionResult:
        fingerprint = request_fingerprint(request)
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                """
                SELECT fingerprint, result_payload FROM collaboration_mode_requests
                WHERE session_id=? AND idempotency_key=?
                """,
                (request.session_id, request.idempotency_key),
            ).fetchone()
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise IdempotencyConflict(
                        "mode-transition idempotency key was reused for a different request"
                    )
                return ModeTransitionResult.model_validate_json(
                    existing["result_payload"]
                ).model_copy(update={"duplicate": True})
            conn.execute(
                """
                INSERT INTO collaboration_mode_requests
                    (request_id, session_id, idempotency_key, fingerprint,
                     request_payload, result_payload, pending, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.request_id,
                    request.session_id,
                    request.idempotency_key,
                    fingerprint,
                    request.model_dump_json(),
                    result.model_dump_json(),
                    int(result.pending),
                    datetime.now(UTC).isoformat(),
                ),
            )
        return result

    def update_mode_result(
        self, request: ModeTransitionRequest, result: ModeTransitionResult
    ) -> ModeTransitionResult:
        with self._lock, self._conn() as conn:
            conn.execute(
                """
                UPDATE collaboration_mode_requests
                SET result_payload=?, pending=?, updated_at=?
                WHERE session_id=? AND idempotency_key=?
                """,
                (
                    result.model_dump_json(),
                    int(result.pending),
                    datetime.now(UTC).isoformat(),
                    request.session_id,
                    request.idempotency_key,
                ),
            )
        return result

    def pending_mode_request(
        self, session_id: str
    ) -> tuple[ModeTransitionRequest, ModeTransitionResult] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT request_payload, result_payload
                FROM collaboration_mode_requests
                WHERE session_id=? AND pending=1
                ORDER BY updated_at ASC LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        if not row:
            return None
        return (
            ModeTransitionRequest.model_validate_json(row["request_payload"]),
            ModeTransitionResult.model_validate_json(row["result_payload"]),
        )

    def save_catalog(self, catalog: CommandCatalog) -> CommandCatalog:
        with self._lock, self._conn() as conn:
            conn.execute(
                "UPDATE session_command_catalogs SET active=0 WHERE session_id=?",
                (catalog.session_id,),
            )
            conn.execute(
                """
                INSERT OR REPLACE INTO session_command_catalogs
                    (session_id, generation, connection_generation, provider,
                     digest, active, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    catalog.session_id,
                    catalog.generation,
                    catalog.connection_generation,
                    catalog.provider,
                    catalog.digest,
                    int(catalog.active),
                    catalog.model_dump_json(),
                    catalog.created_at.isoformat(),
                ),
            )
        return catalog

    def next_catalog_generation(self, session_id: str) -> int:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(generation), 0) AS value FROM session_command_catalogs WHERE session_id=?",
                (session_id,),
            ).fetchone()
        return int(row["value"] if row else 0) + 1

    def active_catalog(self, session_id: str) -> CommandCatalog | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT payload FROM session_command_catalogs
                WHERE session_id=? AND active=1
                ORDER BY generation DESC LIMIT 1
                """,
                (session_id,),
            ).fetchone()
        return CommandCatalog.model_validate_json(row["payload"]) if row else None

    def get_command_result(
        self, session_id: str, idempotency_key: str
    ) -> tuple[str, CommandResult] | None:
        with self._lock, self._conn() as conn:
            row = conn.execute(
                """
                SELECT fingerprint, result_payload FROM session_command_results
                WHERE session_id=? AND idempotency_key=?
                """,
                (session_id, idempotency_key),
            ).fetchone()
        if not row:
            return None
        return row["fingerprint"], CommandResult.model_validate_json(
            row["result_payload"]
        )

    def save_command_result(
        self,
        request_payload: dict[str, Any],
        result: CommandResult,
        *,
        idempotency_key: str,
    ) -> CommandResult:
        fingerprint = request_fingerprint(request_payload)
        with self._lock, self._conn() as conn:
            existing = conn.execute(
                """
                SELECT fingerprint, result_payload FROM session_command_results
                WHERE session_id=? AND idempotency_key=?
                """,
                (result.session_id, idempotency_key),
            ).fetchone()
            if existing:
                if existing["fingerprint"] != fingerprint:
                    raise IdempotencyConflict(
                        "command idempotency key was reused for a different request"
                    )
                return CommandResult.model_validate_json(
                    existing["result_payload"]
                ).model_copy(update={"duplicate": True})
            conn.execute(
                """
                INSERT INTO session_command_results
                    (id, session_id, idempotency_key, fingerprint, request_payload,
                     result_payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    result.id,
                    result.session_id,
                    idempotency_key,
                    fingerprint,
                    json.dumps(request_payload, sort_keys=True, default=str),
                    result.model_dump_json(),
                    result.created_at.isoformat(),
                ),
            )
        return result
