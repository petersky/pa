from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterator

from pa.pr_supervisor.models import (
    GitHubCapability,
    LeaseGrant,
    PRWatch,
    PRWatchEvent,
    PRWatchStatus,
    utcnow,
)


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


class StaleFenceError(RuntimeError):
    pass


class PRSupervisorStore:
    """SQLite projection for watches, leases, audit events, and dispatch claims.

    The fleet-owner instance is the lease authority. SQLite BEGIN IMMEDIATE gives
    it an atomic compare-and-swap boundary; workers must present the returned
    monotonically increasing fence token on every state mutation.
    """

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _conn(self, *, immediate: bool = False) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.execute("PRAGMA journal_mode = WAL")
        if immediate:
            conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS pr_watches (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL,
                    project_id TEXT,
                    card_id TEXT,
                    repository TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    pr_url TEXT NOT NULL,
                    base_branch TEXT,
                    head_sha TEXT,
                    originating_instance_id TEXT,
                    originating_session_id TEXT,
                    originating_agent TEXT,
                    executor_cwd TEXT,
                    policy_json TEXT NOT NULL DEFAULT '{}',
                    required_capabilities_json TEXT NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'active',
                    owner_instance_id TEXT,
                    fence_token INTEGER NOT NULL DEFAULT 0,
                    lease_expires_at TEXT,
                    state_json TEXT NOT NULL DEFAULT '{}',
                    condition_fingerprint TEXT,
                    condition_version INTEGER NOT NULL DEFAULT 0,
                    stable_head_since TEXT,
                    stable_head_observations INTEGER NOT NULL DEFAULT 0,
                    next_poll_at TEXT NOT NULL,
                    poll_attempt INTEGER NOT NULL DEFAULT 0,
                    last_error TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    retired_at TEXT,
                    UNIQUE(realm_id, repository, pr_number)
                );
                CREATE INDEX IF NOT EXISTS idx_pr_watches_due
                    ON pr_watches(status, next_poll_at);
                CREATE INDEX IF NOT EXISTS idx_pr_watches_card
                    ON pr_watches(card_id);

                CREATE TABLE IF NOT EXISTS pr_watch_events (
                    id TEXT PRIMARY KEY,
                    watch_id TEXT NOT NULL,
                    event_key TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    head_sha TEXT,
                    condition_fingerprint TEXT,
                    source TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pr_watch_events_watch
                    ON pr_watch_events(watch_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS pr_dispatch_claims (
                    event_key TEXT PRIMARY KEY,
                    watch_id TEXT NOT NULL,
                    target_instance_id TEXT,
                    target_session_id TEXT,
                    state TEXT NOT NULL DEFAULT 'claimed',
                    detail TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pr_supervisor_instances (
                    instance_id TEXT PRIMARY KEY,
                    capability_json TEXT NOT NULL,
                    last_seen TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pr_supervisor_metrics (
                    name TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL
                );
                """
            )

    def upsert_watch(self, watch: PRWatch, *, preserve_lease: bool = True) -> PRWatch:
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch.id,)
            ).fetchone()
            if not row:
                row = conn.execute(
                    """
                    SELECT * FROM pr_watches
                    WHERE realm_id = ?
                      AND repository = ? COLLATE NOCASE
                      AND pr_number = ?
                    """,
                    (watch.realm_id, watch.repository, watch.pr_number),
                ).fetchone()
            existing = self._row_to_watch(row) if row else None
            if existing:
                skip_replica_state = preserve_lease and (
                    existing.updated_at > watch.updated_at
                    or (
                        existing.status
                        in {
                            PRWatchStatus.MERGED,
                            PRWatchStatus.CLOSED,
                            PRWatchStatus.RETIRED,
                        }
                        and watch.status
                        in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}
                    )
                )
                if skip_replica_state:
                    if watch.fence_token <= existing.fence_token:
                        return existing
                    # The replica's state is stale, but its fence generation is
                    # independently monotonic and must still advance the next
                    # authority's baseline.
                    replica_owner = watch.owner_instance_id
                    replica_fence = watch.fence_token
                    replica_expiry = watch.lease_expires_at
                    watch = existing.model_copy(deep=True)
                    watch.owner_instance_id = replica_owner
                    watch.fence_token = replica_fence
                    watch.lease_expires_at = replica_expiry
                watch.id = existing.id
                watch.created_at = existing.created_at
                if preserve_lease:
                    # Replicas form the next authority's durable fence baseline.
                    # Never decrease a token; carry owner/expiry from whichever
                    # record owns the greatest observed fencing generation.
                    if existing.fence_token >= watch.fence_token:
                        watch.owner_instance_id = existing.owner_instance_id
                        watch.fence_token = existing.fence_token
                        watch.lease_expires_at = existing.lease_expires_at
                if not watch.head_sha:
                    watch.head_sha = existing.head_sha
                if not watch.state:
                    watch.state = existing.state
                watch.condition_fingerprint = (
                    watch.condition_fingerprint or existing.condition_fingerprint
                )
                watch.condition_version = max(
                    watch.condition_version, existing.condition_version
                )
                watch.stable_head_since = (
                    watch.stable_head_since or existing.stable_head_since
                )
                watch.stable_head_observations = max(
                    watch.stable_head_observations,
                    existing.stable_head_observations,
                )
            watch.updated_at = utcnow()
            conn.execute(
                """
                INSERT OR REPLACE INTO pr_watches (
                    id, realm_id, project_id, card_id, repository, pr_number,
                    pr_url, base_branch, head_sha, originating_instance_id,
                    originating_session_id, originating_agent, executor_cwd,
                    policy_json, required_capabilities_json, status,
                    owner_instance_id, fence_token, lease_expires_at, state_json,
                    condition_fingerprint, condition_version, stable_head_since,
                    stable_head_observations, next_poll_at, poll_attempt,
                    last_error, created_at, updated_at, retired_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                self._watch_values(watch),
            )
        return self.get_watch(watch.id) or watch

    def _watch_values(self, watch: PRWatch) -> tuple[Any, ...]:
        return (
            watch.id,
            watch.realm_id,
            watch.project_id,
            watch.card_id,
            watch.repository,
            watch.pr_number,
            watch.pr_url,
            watch.base_branch,
            watch.head_sha,
            watch.originating_instance_id,
            watch.originating_session_id,
            watch.originating_agent,
            watch.executor_cwd,
            watch.policy.model_dump_json(),
            json.dumps(watch.required_capabilities),
            watch.status.value,
            watch.owner_instance_id,
            watch.fence_token,
            watch.lease_expires_at.isoformat() if watch.lease_expires_at else None,
            json.dumps(watch.state),
            watch.condition_fingerprint,
            watch.condition_version,
            watch.stable_head_since.isoformat() if watch.stable_head_since else None,
            watch.stable_head_observations,
            watch.next_poll_at.isoformat(),
            watch.poll_attempt,
            watch.last_error,
            watch.created_at.isoformat(),
            watch.updated_at.isoformat(),
            watch.retired_at.isoformat() if watch.retired_at else None,
        )

    def get_watch(self, watch_id: str) -> PRWatch | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
        return self._row_to_watch(row) if row else None

    def find_watch(
        self, realm_id: str, repository: str, pr_number: int
    ) -> PRWatch | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM pr_watches
                WHERE realm_id = ?
                  AND repository = ? COLLATE NOCASE
                  AND pr_number = ?
                """,
                (realm_id, repository, pr_number),
            ).fetchone()
        return self._row_to_watch(row) if row else None

    def find_watches(self, repository: str, pr_number: int) -> list[PRWatch]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pr_watches
                WHERE repository = ? COLLATE NOCASE AND pr_number = ?
                ORDER BY realm_id, created_at
                """,
                (repository, pr_number),
            ).fetchall()
        return [self._row_to_watch(row) for row in rows]

    def list_watches(
        self,
        *,
        realm_id: str | None = None,
        card_id: str | None = None,
        include_retired: bool = False,
    ) -> list[PRWatch]:
        query = "SELECT * FROM pr_watches WHERE 1=1"
        params: list[Any] = []
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        if card_id:
            query += " AND card_id = ?"
            params.append(card_id)
        if not include_retired:
            query += " AND status != 'retired'"
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_watch(row) for row in rows]

    def list_due(self, *, now: datetime | None = None) -> list[PRWatch]:
        now = now or utcnow()
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pr_watches
                WHERE status IN ('active', 'blocked') AND next_poll_at <= ?
                ORDER BY next_poll_at ASC
                """,
                (now.isoformat(),),
            ).fetchall()
        return [self._row_to_watch(row) for row in rows]

    def schedule_now(
        self,
        *,
        watch_id: str | None = None,
        repository: str | None = None,
        pr_number: int | None = None,
    ) -> int:
        if not watch_id and not (repository and pr_number):
            return 0
        now = utcnow().isoformat()
        with self._conn() as conn:
            if watch_id:
                cursor = conn.execute(
                    """
                    UPDATE pr_watches SET next_poll_at = ?, poll_attempt = 0,
                        updated_at = ?
                    WHERE id = ? AND status IN ('active', 'blocked')
                    """,
                    (now, now, watch_id),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE pr_watches SET next_poll_at = ?, poll_attempt = 0,
                        updated_at = ?
                    WHERE repository = ? COLLATE NOCASE AND pr_number = ?
                      AND status IN ('active', 'blocked')
                    """,
                    (now, now, repository, pr_number),
                )
        return cursor.rowcount

    def try_acquire_lease(
        self,
        watch_id: str,
        instance_id: str,
        *,
        ttl_seconds: int = 45,
        now: datetime | None = None,
        capability: GitHubCapability | None = None,
    ) -> LeaseGrant:
        now = now or utcnow()
        expires = now + timedelta(seconds=ttl_seconds)
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                return LeaseGrant(acquired=False, reason="watch_not_found")
            watch = self._row_to_watch(row)
            if watch.status not in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}:
                return LeaseGrant(acquired=False, reason="watch_inactive")
            if capability and not capability.supports(watch.repository):
                return LeaseGrant(acquired=False, reason="capability_ineligible")
            lease_active = (
                watch.owner_instance_id
                and watch.lease_expires_at
                and watch.lease_expires_at > now
            )
            if lease_active and watch.owner_instance_id != instance_id:
                return LeaseGrant(
                    acquired=False,
                    owner_instance_id=watch.owner_instance_id,
                    fence_token=watch.fence_token,
                    expires_at=watch.lease_expires_at,
                    reason="owned",
                )
            fence = watch.fence_token
            if watch.owner_instance_id != instance_id or not lease_active:
                fence += 1
            conn.execute(
                """
                UPDATE pr_watches
                SET owner_instance_id = ?, fence_token = ?, lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    instance_id,
                    fence,
                    expires.isoformat(),
                    now.isoformat(),
                    watch_id,
                ),
            )
        self.increment_metric("leases_acquired")
        return LeaseGrant(
            acquired=True,
            owner_instance_id=instance_id,
            fence_token=fence,
            expires_at=expires,
        )

    def release_lease(self, watch_id: str, instance_id: str, fence_token: int) -> bool:
        with self._conn(immediate=True) as conn:
            cursor = conn.execute(
                """
                UPDATE pr_watches
                SET owner_instance_id = NULL, lease_expires_at = NULL,
                    updated_at = ?
                WHERE id = ? AND owner_instance_id = ? AND fence_token = ?
                """,
                (utcnow().isoformat(), watch_id, instance_id, fence_token),
            )
        return cursor.rowcount == 1

    def update_observation(
        self,
        watch_id: str,
        *,
        owner_instance_id: str,
        fence_token: int,
        head_sha: str,
        base_branch: str,
        state: dict[str, Any],
        condition_fingerprint: str,
        next_poll_at: datetime,
        poll_attempt: int,
        last_error: str | None = None,
        now: datetime | None = None,
    ) -> PRWatch:
        now = now or utcnow()
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                raise KeyError(watch_id)
            watch = self._row_to_watch(row)
            if (
                watch.owner_instance_id != owner_instance_id
                or watch.fence_token != fence_token
                or not watch.lease_expires_at
                or watch.lease_expires_at <= now
            ):
                raise StaleFenceError(f"stale fence for watch {watch_id}")
            if watch.head_sha != head_sha:
                stable_since = now
                stable_observations = 1
                condition_version = 1
            else:
                stable_since = watch.stable_head_since or now
                stable_observations = watch.stable_head_observations + 1
                condition_version = watch.condition_version
                if condition_fingerprint != watch.condition_fingerprint:
                    condition_version += 1
            conn.execute(
                """
                UPDATE pr_watches
                SET head_sha = ?, base_branch = ?, state_json = ?,
                    condition_fingerprint = ?, condition_version = ?,
                    stable_head_since = ?, stable_head_observations = ?,
                    next_poll_at = ?, poll_attempt = ?, last_error = ?,
                    status = 'active', updated_at = ?
                WHERE id = ? AND owner_instance_id = ? AND fence_token = ?
                """,
                (
                    head_sha,
                    base_branch,
                    json.dumps(state),
                    condition_fingerprint,
                    condition_version,
                    stable_since.isoformat(),
                    stable_observations,
                    next_poll_at.isoformat(),
                    poll_attempt,
                    last_error,
                    now.isoformat(),
                    watch_id,
                    owner_instance_id,
                    fence_token,
                ),
            )
        updated = self.get_watch(watch_id)
        if not updated:
            raise KeyError(watch_id)
        return updated

    def mark_error(
        self,
        watch_id: str,
        message: str,
        *,
        next_poll_at: datetime,
        owner_instance_id: str | None = None,
        fence_token: int | None = None,
        visible_state: str = "error",
    ) -> PRWatch | None:
        now = utcnow()
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                return None
            watch = self._row_to_watch(row)
            if owner_instance_id is not None and (
                watch.owner_instance_id != owner_instance_id
                or watch.fence_token != fence_token
                or not watch.lease_expires_at
                or watch.lease_expires_at <= now
            ):
                raise StaleFenceError(f"stale fence for watch {watch_id}")
            state = dict(watch.state)
            state["supervisor_state"] = visible_state
            conn.execute(
                """
                UPDATE pr_watches
                SET state_json = ?, status = 'blocked', last_error = ?,
                    poll_attempt = poll_attempt + 1, next_poll_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    json.dumps(state),
                    message[:2000],
                    next_poll_at.isoformat(),
                    now.isoformat(),
                    watch_id,
                ),
            )
        self.increment_metric("poll_errors")
        return self.get_watch(watch_id)

    def set_terminal(
        self,
        watch_id: str,
        status: PRWatchStatus,
        *,
        state: dict[str, Any] | None = None,
        owner_instance_id: str | None = None,
        fence_token: int | None = None,
    ) -> PRWatch | None:
        now = utcnow()
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                return None
            watch = self._row_to_watch(row)
            if owner_instance_id is not None and (
                watch.owner_instance_id != owner_instance_id
                or watch.fence_token != fence_token
                or not watch.lease_expires_at
                or watch.lease_expires_at <= now
            ):
                raise StaleFenceError(f"stale fence for watch {watch_id}")
            merged_state = state if state is not None else watch.state
            conn.execute(
                """
                UPDATE pr_watches
                SET status = ?, state_json = ?, owner_instance_id = NULL,
                    lease_expires_at = NULL, retired_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    status.value,
                    json.dumps(merged_state),
                    now.isoformat() if status == PRWatchStatus.RETIRED else None,
                    now.isoformat(),
                    watch_id,
                ),
            )
        return self.get_watch(watch_id)

    def append_event(self, event: PRWatchEvent) -> bool:
        try:
            with self._conn(immediate=True) as conn:
                conn.execute(
                    """
                    INSERT INTO pr_watch_events (
                        id, watch_id, event_key, event_type, head_sha,
                        condition_fingerprint, source, payload_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.watch_id,
                        event.event_key,
                        event.event_type,
                        event.head_sha,
                        event.condition_fingerprint,
                        event.source,
                        json.dumps(event.payload),
                        event.created_at.isoformat(),
                    ),
                )
            self.increment_metric("audit_events")
            return True
        except sqlite3.IntegrityError:
            return False

    def list_events(self, watch_id: str, *, limit: int = 200) -> list[PRWatchEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM pr_watch_events
                WHERE watch_id = ? ORDER BY created_at DESC LIMIT ?
                """,
                (watch_id, limit),
            ).fetchall()
        return [
            PRWatchEvent(
                id=row["id"],
                watch_id=row["watch_id"],
                event_key=row["event_key"],
                event_type=row["event_type"],
                head_sha=row["head_sha"],
                condition_fingerprint=row["condition_fingerprint"],
                source=row["source"],
                payload=json.loads(row["payload_json"] or "{}"),
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]

    def claim_dispatch(
        self,
        event_key: str,
        watch_id: str,
        *,
        target_instance_id: str | None,
        target_session_id: str | None,
    ) -> bool:
        now = utcnow().isoformat()
        try:
            with self._conn(immediate=True) as conn:
                conn.execute(
                    """
                    INSERT INTO pr_dispatch_claims (
                        event_key, watch_id, target_instance_id, target_session_id,
                        state, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, 'claimed', ?, ?)
                    """,
                    (
                        event_key,
                        watch_id,
                        target_instance_id,
                        target_session_id,
                        now,
                        now,
                    ),
                )
            return True
        except sqlite3.IntegrityError:
            with self._conn(immediate=True) as conn:
                row = conn.execute(
                    "SELECT state FROM pr_dispatch_claims WHERE event_key = ?",
                    (event_key,),
                ).fetchone()
                if not row or row["state"] != "failed":
                    return False
                cursor = conn.execute(
                    """
                    UPDATE pr_dispatch_claims
                    SET state = 'claimed', target_instance_id = ?,
                        target_session_id = ?, detail = NULL, updated_at = ?
                    WHERE event_key = ? AND state = 'failed'
                    """,
                    (
                        target_instance_id,
                        target_session_id,
                        utcnow().isoformat(),
                        event_key,
                    ),
                )
            return cursor.rowcount == 1

    def finish_dispatch(self, event_key: str, *, state: str, detail: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE pr_dispatch_claims
                SET state = ?, detail = ?, updated_at = ? WHERE event_key = ?
                """,
                (state, detail[:2000], utcnow().isoformat(), event_key),
            )

    def save_capability(self, capability: GitHubCapability) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO pr_supervisor_instances
                (instance_id, capability_json, last_seen) VALUES (?, ?, ?)
                """,
                (
                    capability.instance_id,
                    capability.model_dump_json(),
                    capability.checked_at.isoformat(),
                ),
            )

    def list_capabilities(
        self, *, fresh_seconds: int = 120, now: datetime | None = None
    ) -> list[GitHubCapability]:
        now = now or utcnow()
        cutoff = now - timedelta(seconds=fresh_seconds)
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT capability_json FROM pr_supervisor_instances
                WHERE last_seen >= ? ORDER BY last_seen DESC
                """,
                (cutoff.isoformat(),),
            ).fetchall()
        return [
            GitHubCapability.model_validate_json(row["capability_json"]) for row in rows
        ]

    def increment_metric(self, name: str, amount: int = 1) -> None:
        now = utcnow().isoformat()
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO pr_supervisor_metrics (name, value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(name) DO UPDATE SET
                    value = value + excluded.value,
                    updated_at = excluded.updated_at
                """,
                (name, amount, now),
            )

    def metrics(self) -> dict[str, int]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT name, value FROM pr_supervisor_metrics"
            ).fetchall()
        values = {row["name"]: row["value"] for row in rows}
        values["active_watches"] = len(
            [
                watch
                for watch in self.list_watches()
                if watch.status in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}
            ]
        )
        return values

    @staticmethod
    def _row_to_watch(row: sqlite3.Row) -> PRWatch:
        return PRWatch(
            id=row["id"],
            realm_id=row["realm_id"],
            project_id=row["project_id"],
            card_id=row["card_id"],
            repository=row["repository"],
            pr_number=row["pr_number"],
            pr_url=row["pr_url"],
            base_branch=row["base_branch"],
            head_sha=row["head_sha"],
            originating_instance_id=row["originating_instance_id"],
            originating_session_id=row["originating_session_id"],
            originating_agent=row["originating_agent"],
            executor_cwd=row["executor_cwd"],
            policy=json.loads(row["policy_json"] or "{}"),
            required_capabilities=json.loads(row["required_capabilities_json"] or "[]"),
            status=row["status"],
            owner_instance_id=row["owner_instance_id"],
            fence_token=row["fence_token"],
            lease_expires_at=_dt(row["lease_expires_at"]),
            state=json.loads(row["state_json"] or "{}"),
            condition_fingerprint=row["condition_fingerprint"],
            condition_version=row["condition_version"],
            stable_head_since=_dt(row["stable_head_since"]),
            stable_head_observations=row["stable_head_observations"],
            next_poll_at=datetime.fromisoformat(row["next_poll_at"]),
            poll_attempt=row["poll_attempt"],
            last_error=row["last_error"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            retired_at=_dt(row["retired_at"]),
        )
