from __future__ import annotations

import json
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from pa.pr_supervisor.models import (
    GITHUB_TERMINAL_PR_WATCH_STATUSES,
    PR_WATCH_PROTOCOL_VERSION,
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
                    repository_id TEXT,
                    dispatch_id TEXT,
                    repository TEXT NOT NULL,
                    pr_number INTEGER NOT NULL,
                    pr_url TEXT NOT NULL,
                    base_branch TEXT,
                    head_sha TEXT,
                    originating_instance_id TEXT,
                    authority_instance_id TEXT,
                    originating_session_id TEXT,
                    originating_principal_id TEXT,
                    originating_agent TEXT,
                    executor_cwd TEXT,
                    provenance_version INTEGER NOT NULL DEFAULT 0,
                    creation_reason TEXT,
                    qualifying_evidence TEXT,
                    policy_json TEXT NOT NULL DEFAULT '{}',
                    required_capabilities_json TEXT NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'active',
                    owner_instance_id TEXT,
                    fence_token INTEGER NOT NULL DEFAULT 0,
                    lease_version INTEGER NOT NULL DEFAULT 0,
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
                CREATE INDEX IF NOT EXISTS idx_pr_watches_realm_card_updated
                    ON pr_watches(realm_id, card_id, updated_at DESC);

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
            columns = {
                row[1]
                for row in conn.execute("PRAGMA table_info(pr_watches)").fetchall()
            }
            for column, declaration in (
                ("repository_id", "TEXT"),
                ("dispatch_id", "TEXT"),
                ("authority_instance_id", "TEXT"),
                ("originating_principal_id", "TEXT"),
                ("provenance_version", "INTEGER NOT NULL DEFAULT 0"),
                ("creation_reason", "TEXT"),
                ("qualifying_evidence", "TEXT"),
                ("lease_version", "INTEGER NOT NULL DEFAULT 0"),
            ):
                if column not in columns:
                    conn.execute(
                        f"ALTER TABLE pr_watches ADD COLUMN {column} {declaration}"
                    )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_pr_watches_project_history "
                "ON pr_watches(realm_id, project_id, updated_at DESC)"
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
                    if (watch.fence_token, watch.lease_version) <= (
                        existing.fence_token,
                        existing.lease_version,
                    ):
                        return existing
                    # The replica's state is stale, but its fence generation is
                    # independently monotonic and must still advance the next
                    # authority's baseline.
                    replica_owner = watch.owner_instance_id
                    replica_fence = watch.fence_token
                    replica_version = watch.lease_version
                    replica_expiry = watch.lease_expires_at
                    watch = existing.model_copy(deep=True)
                    watch.fence_token = replica_fence
                    watch.lease_version = replica_version
                    if watch.terminal:
                        watch.owner_instance_id = None
                        watch.lease_expires_at = None
                    else:
                        watch.owner_instance_id = replica_owner
                        watch.lease_expires_at = replica_expiry
                watch.id = existing.id
                watch.created_at = existing.created_at
                if preserve_lease and (
                    existing.fence_token,
                    existing.lease_version,
                ) >= (watch.fence_token, watch.lease_version):
                    # Replicas form the next authority's durable fence baseline.
                    # Never decrease a token; carry owner/expiry from whichever
                    # record owns the greatest observed fencing generation.
                    watch.owner_instance_id = existing.owner_instance_id
                    watch.fence_token = existing.fence_token
                    watch.lease_version = existing.lease_version
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
            if watch.terminal or watch.retired_at is not None:
                watch.owner_instance_id = None
                watch.lease_expires_at = None
            watch.updated_at = utcnow()
            conn.execute(
                """
                INSERT OR REPLACE INTO pr_watches (
                    id, realm_id, project_id, card_id, repository_id, dispatch_id,
                    repository, pr_number,
                    pr_url, base_branch, head_sha, originating_instance_id, authority_instance_id,
                    originating_session_id, originating_principal_id,
                    originating_agent, executor_cwd, provenance_version,
                    creation_reason, qualifying_evidence,
                    policy_json, required_capabilities_json, status,
                    owner_instance_id, fence_token, lease_version, lease_expires_at, state_json,
                    condition_fingerprint, condition_version, stable_head_since,
                    stable_head_observations, next_poll_at, poll_attempt,
                    last_error, created_at, updated_at, retired_at
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?
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
            watch.repository_id,
            watch.dispatch_id,
            watch.repository,
            watch.pr_number,
            watch.pr_url,
            watch.base_branch,
            watch.head_sha,
            watch.originating_instance_id,
            watch.authority_instance_id,
            watch.originating_session_id,
            watch.originating_principal_id,
            watch.originating_agent,
            watch.executor_cwd,
            watch.provenance_version,
            watch.creation_reason,
            watch.qualifying_evidence,
            watch.policy.model_dump_json(),
            json.dumps(watch.required_capabilities),
            watch.status.value,
            watch.owner_instance_id,
            watch.fence_token,
            watch.lease_version,
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
            query += " AND retired_at IS NULL AND status IN ('active', 'blocked')"
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_watch(row) for row in rows]

    def count_project_watches(
        self,
        project_id: str,
        *,
        realm_id: str,
        card_ids: set[str] | None = None,
    ) -> int:
        """Count all watches related to a project without hydrating history."""
        query = "SELECT COUNT(*) AS total FROM pr_watches WHERE realm_id = ?"
        params: list[Any] = [realm_id]
        related = "project_id = ?"
        params.append(project_id)
        bounded_card_ids = sorted(card_ids or set())
        if bounded_card_ids:
            placeholders = ",".join("?" for _ in bounded_card_ids)
            related += f" OR card_id IN ({placeholders})"
            params.extend(bounded_card_ids)
        query += f" AND ({related})"
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["total"] if row else 0)

    def list_project_watches(
        self,
        project_id: str,
        *,
        realm_id: str,
        card_ids: set[str] | None = None,
        limit: int = 20,
        offset: int = 0,
    ) -> list[PRWatch]:
        """Load a bounded, stable page of all watches related to a project."""
        bounded_limit = max(1, min(int(limit), 100))
        bounded_offset = max(0, int(offset))
        query = "SELECT * FROM pr_watches WHERE realm_id = ?"
        params: list[Any] = [realm_id]
        related = "project_id = ?"
        params.append(project_id)
        bounded_card_ids = sorted(card_ids or set())
        if bounded_card_ids:
            placeholders = ",".join("?" for _ in bounded_card_ids)
            related += f" OR card_id IN ({placeholders})"
            params.extend(bounded_card_ids)
        query += f" AND ({related}) ORDER BY updated_at DESC, id DESC LIMIT ? OFFSET ?"
        params.extend((bounded_limit, bounded_offset))
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_watch(row) for row in rows]

    def list_watches_for_cards(
        self,
        card_ids: set[str],
        *,
        realm_id: str,
        include_retired: bool = False,
        per_card_limit: int = 5,
    ) -> list[PRWatch]:
        """Load bounded recent watch evidence only for projected Workshop cards."""
        if not card_ids or per_card_limit <= 0:
            return []
        placeholders = ",".join("?" for _ in card_ids)
        retired_clause = (
            ""
            if include_retired
            else "AND retired_at IS NULL AND status IN ('active', 'blocked')"
        )
        query = f"""
            SELECT * FROM (
                SELECT pr_watches.*,
                       ROW_NUMBER() OVER (
                           PARTITION BY card_id
                           ORDER BY updated_at DESC, id ASC
                       ) AS workshop_rank
                FROM pr_watches
                WHERE realm_id = ?
                  AND card_id IN ({placeholders})
                  {retired_clause}
            )
            WHERE workshop_rank <= ?
            ORDER BY updated_at DESC, id ASC
        """
        with self._conn() as conn:
            rows = conn.execute(
                query,
                (realm_id, *sorted(card_ids), per_card_limit),
            ).fetchall()
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
        renewal_window_seconds: int = 12,
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
            if watch.terminal or watch.retired_at is not None:
                return LeaseGrant(
                    acquired=False,
                    fence_token=watch.fence_token,
                    lease_version=watch.lease_version,
                    reason="watch_terminal",
                    terminal_status=watch.status,
                    protocol_version=2,
                )
            if watch.status not in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}:
                return LeaseGrant(acquired=False, reason="watch_inactive")
            if capability is None:
                return LeaseGrant(
                    acquired=False,
                    reason="capability_missing",
                    protocol_version=PR_WATCH_PROTOCOL_VERSION,
                )
            if capability.instance_id != instance_id:
                return LeaseGrant(
                    acquired=False,
                    reason="capability_identity_mismatch",
                    protocol_version=PR_WATCH_PROTOCOL_VERSION,
                )
            if capability.pr_watch_protocol_version < PR_WATCH_PROTOCOL_VERSION:
                return LeaseGrant(
                    acquired=False,
                    reason="protocol_upgrade_required",
                    protocol_version=PR_WATCH_PROTOCOL_VERSION,
                )
            if not capability.supports(watch.repository):
                return LeaseGrant(
                    acquired=False,
                    reason="capability_ineligible",
                    protocol_version=PR_WATCH_PROTOCOL_VERSION,
                )
            lease_active = (
                watch.owner_instance_id
                and watch.lease_expires_at
                and watch.lease_expires_at > now
            )
            remaining = (
                max(0.0, (watch.lease_expires_at - now).total_seconds())
                if lease_active and watch.lease_expires_at
                else 0.0
            )
            guards = watch.state.get("effect_authorizations") or {}
            effect_remaining = max(
                (
                    max(
                        0.0,
                        (
                            datetime.fromisoformat(str(guard["expires_at"])) - now
                        ).total_seconds(),
                    )
                    for guard in guards.values()
                    if isinstance(guard, dict)
                    and guard.get("state") == "prepared"
                    and guard.get("owner_instance_id") == watch.owner_instance_id
                    and int(guard.get("fence_token") or -1) == watch.fence_token
                    and int(guard.get("lease_version") or -1) == watch.lease_version
                    and guard.get("head_sha") == watch.head_sha
                    and guard.get("condition_fingerprint")
                    == watch.condition_fingerprint
                ),
                default=0.0,
            )
            if (
                effect_remaining > 0
                and watch.owner_instance_id
                and watch.owner_instance_id != instance_id
            ):
                return LeaseGrant(
                    acquired=False,
                    owner_instance_id=watch.owner_instance_id,
                    fence_token=watch.fence_token,
                    lease_version=watch.lease_version,
                    expires_at=watch.lease_expires_at,
                    reason="effect_in_progress",
                    lease_seconds_remaining=effect_remaining,
                    protocol_version=2,
                )
            if lease_active and watch.owner_instance_id != instance_id:
                return LeaseGrant(
                    acquired=False,
                    owner_instance_id=watch.owner_instance_id,
                    fence_token=watch.fence_token,
                    lease_version=watch.lease_version,
                    expires_at=watch.lease_expires_at,
                    reason="owned",
                    lease_seconds_remaining=remaining,
                    protocol_version=2,
                )
            if (
                lease_active
                and watch.owner_instance_id == instance_id
                and remaining > max(0, renewal_window_seconds)
            ):
                # Concurrent or eager same-owner requests are reads while the
                # authority still has enough time to survive one renewal delay.
                return LeaseGrant(
                    acquired=True,
                    owner_instance_id=instance_id,
                    fence_token=watch.fence_token,
                    lease_version=watch.lease_version,
                    expires_at=watch.lease_expires_at,
                    reason="lease_valid",
                    lease_seconds_remaining=remaining,
                    protocol_version=2,
                )
            fence = watch.fence_token
            if watch.owner_instance_id != instance_id or not lease_active:
                fence += 1
            lease_version = watch.lease_version + 1
            conn.execute(
                """
                UPDATE pr_watches
                SET owner_instance_id = ?, fence_token = ?, lease_version = ?, lease_expires_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    instance_id,
                    fence,
                    lease_version,
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
            lease_version=lease_version,
            expires_at=expires,
            reason="renewed" if lease_active else "acquired",
            lease_seconds_remaining=float(ttl_seconds),
            protocol_version=2,
        )

    def prepare_effect_authorization(
        self,
        watch_id: str,
        *,
        owner_instance_id: str,
        fence_token: int,
        lease_version: int,
        event_key: str,
        bindings: dict[str, Any],
        ttl_seconds: int = 20,
        now: datetime | None = None,
    ) -> tuple[PRWatch, dict[str, Any]]:
        now = now or utcnow()
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                raise KeyError(watch_id)
            watch = self._row_to_watch(row)
            if (
                not watch.actionable
                or watch.owner_instance_id != owner_instance_id
                or watch.fence_token != fence_token
                or watch.lease_version != lease_version
                or not watch.lease_expires_at
                or watch.lease_expires_at <= now
            ):
                raise StaleFenceError(f"stale effect fence for watch {watch_id}")
            expected = {
                "realm_id": watch.realm_id,
                "watch_id": watch.id,
                "repository": watch.repository,
                "pr_number": watch.pr_number,
                "head_sha": watch.head_sha,
                "condition_fingerprint": watch.condition_fingerprint,
                "condition_version": watch.condition_version,
                "owner_instance_id": watch.owner_instance_id,
                "fence_token": watch.fence_token,
                "lease_version": watch.lease_version,
            }
            if any(bindings.get(key) != value for key, value in expected.items()):
                raise StaleFenceError(f"effect binding changed for watch {watch_id}")
            state = dict(watch.state)
            authorizations = dict(state.get("effect_authorizations") or {})
            prior = authorizations.get(event_key)
            semantic = {**expected, **bindings, "event_key": event_key}
            if isinstance(prior, dict):
                # The event key identifies the external effect, while the lease
                # tuple identifies which compatible worker may deliver it now.
                # Once a prepared authorization expires, a successor must be
                # able to recover the same effect identity under its new fence.
                # Keep every effect/destination/policy binding immutable, but do
                # not make an expired worker lease part of the semantic identity.
                lease_keys = {"owner_instance_id", "fence_token", "lease_version"}
                stable_semantic = {
                    key: value
                    for key, value in semantic.items()
                    if key not in lease_keys
                }
                prior_stable_semantic = {
                    key: prior.get(key) for key in stable_semantic
                }
                if prior_stable_semantic != stable_semantic:
                    raise StaleFenceError(
                        f"event key belongs to different effect for watch {watch_id}"
                    )
                if prior.get("state") == "accepted" or (
                    prior.get("state") == "prepared"
                    and datetime.fromisoformat(str(prior["expires_at"])) > now
                ):
                    return watch, prior
            authorization = {
                **semantic,
                "id": str((prior or {}).get("id") or uuid4()),
                "state": "prepared",
                "protocol_version": 2,
                "issued_at": now.isoformat(),
                "expires_at": (
                    now + timedelta(seconds=max(1, ttl_seconds))
                ).isoformat(),
            }
            authorizations[event_key] = authorization
            if len(authorizations) > 50:
                keys = list(authorizations)[-50:]
                authorizations = {key: authorizations[key] for key in keys}
            state["effect_authorizations"] = authorizations
            cursor = conn.execute(
                """
                UPDATE pr_watches SET state_json = ?, updated_at = ?
                WHERE id = ? AND owner_instance_id = ? AND fence_token = ?
                  AND lease_version = ?
                """,
                (
                    json.dumps(state),
                    now.isoformat(),
                    watch_id,
                    owner_instance_id,
                    fence_token,
                    lease_version,
                ),
            )
            if cursor.rowcount != 1:
                raise StaleFenceError(f"stale effect fence for watch {watch_id}")
        updated = self.get_watch(watch_id)
        if not updated:
            raise KeyError(watch_id)
        return updated, authorization

    def finish_effect_authorization(
        self,
        watch_id: str,
        event_key: str,
        authorization_id: str,
        *,
        accepted: bool,
        detail: str,
    ) -> PRWatch:
        now = utcnow()
        with self._conn(immediate=True) as conn:
            row = conn.execute(
                "SELECT * FROM pr_watches WHERE id = ?", (watch_id,)
            ).fetchone()
            if not row:
                raise KeyError(watch_id)
            watch = self._row_to_watch(row)
            state = dict(watch.state)
            authorizations = dict(state.get("effect_authorizations") or {})
            authorization = dict(authorizations.get(event_key) or {})
            if authorization.get("id") != authorization_id:
                raise StaleFenceError(f"effect authorization changed for {watch_id}")
            authorization["state"] = "accepted" if accepted else "failed"
            authorization["result"] = detail[:500]
            authorization["completed_at"] = now.isoformat()
            authorizations[event_key] = authorization
            state["effect_authorizations"] = authorizations
            conn.execute(
                "UPDATE pr_watches SET state_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(state), now.isoformat(), watch_id),
            )
        updated = self.get_watch(watch_id)
        if not updated:
            raise KeyError(watch_id)
        return updated

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
            state = dict(state)
            if watch.state.get("effect_authorizations"):
                state["effect_authorizations"] = watch.state["effect_authorizations"]
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
            if watch.terminal or watch.retired_at is not None:
                return watch
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
        fence_token_baseline: int | None = None,
        retirement_reason: str | None = None,
        retired_at: datetime | None = None,
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
            effective_status = status
            if (
                status == PRWatchStatus.RETIRED
                and watch.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            ) or (
                status == PRWatchStatus.CLOSED and watch.status == PRWatchStatus.MERGED
            ):
                # Generic retirement and a late closed observation must never
                # erase a stronger terminal GitHub outcome.
                effective_status = watch.status
            merged_state = dict(state if state is not None else watch.state)
            retirement_at = watch.retired_at or retired_at or now
            existing_retirement = watch.state.get("retirement")
            if isinstance(existing_retirement, dict):
                merged_state["retirement"] = dict(existing_retirement)
                if effective_status != watch.status:
                    merged_state["retirement"]["terminal_status"] = (
                        effective_status.value
                    )
            else:
                merged_state["retirement"] = {
                    "reason": retirement_reason or "terminal_status",
                    "retired_at": retirement_at.isoformat(),
                    "terminal_status": effective_status.value,
                }
            effective_fence = max(watch.fence_token, fence_token_baseline or 0)
            if (
                effective_status == watch.status
                and merged_state == watch.state
                and watch.retired_at == retirement_at
                and watch.owner_instance_id is None
                and watch.lease_expires_at is None
                and watch.fence_token == effective_fence
            ):
                return watch
            conn.execute(
                """
                UPDATE pr_watches
                SET status = ?, state_json = ?, owner_instance_id = NULL,
                    fence_token = ?, lease_expires_at = NULL, retired_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    effective_status.value,
                    json.dumps(merged_state),
                    effective_fence,
                    retirement_at.isoformat(),
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
        now = utcnow()
        with self._conn(immediate=True) as conn:
            existing = conn.execute(
                "SELECT * FROM pr_dispatch_claims WHERE event_key = ?",
                (event_key,),
            ).fetchone()
            if existing:
                same_target = (
                    existing["watch_id"] == watch_id
                    and existing["target_instance_id"] == target_instance_id
                    and existing["target_session_id"] == target_session_id
                )
                stale_claim = existing["state"] == "claimed" and datetime.fromisoformat(
                    existing["updated_at"]
                ) <= now - timedelta(seconds=30)
                if not same_target or (
                    existing["state"] != "failed" and not stale_claim
                ):
                    return False
                conn.execute(
                    """
                    UPDATE pr_dispatch_claims
                    SET state = 'claimed', detail = NULL, updated_at = ?
                    WHERE event_key = ?
                    """,
                    (now.isoformat(), event_key),
                )
                return True
            try:
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
                        now.isoformat(),
                        now.isoformat(),
                    ),
                )
                return True
            except sqlite3.IntegrityError:
                return False

    def finish_dispatch(self, event_key: str, *, state: str, detail: str = "") -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE pr_dispatch_claims
                SET state = ?, detail = ?, updated_at = ? WHERE event_key = ?
                """,
                (state, detail[:2000], utcnow().isoformat(), event_key),
            )

    def list_dispatches(
        self, watch_id: str, *, limit: int = 100
    ) -> list[dict[str, Any]]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT event_key, watch_id, target_instance_id, target_session_id,
                          state, detail, created_at, updated_at
                   FROM pr_dispatch_claims WHERE watch_id = ?
                   ORDER BY created_at DESC LIMIT ?""",
                (watch_id, limit),
            ).fetchall()
        return [dict(row) for row in rows]

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
        watches = self.list_watches(include_retired=True)
        values["active_watches"] = len([watch for watch in watches if watch.actionable])
        values["historical_watches"] = len(
            [watch for watch in watches if not watch.actionable]
        )
        values["archived_watches"] = len(
            [watch for watch in watches if watch.retired_at is not None]
        )
        values["terminal_retirement_backlog"] = len(
            [
                watch
                for watch in watches
                if watch.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
                and watch.retired_at is None
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
            repository_id=row["repository_id"],
            dispatch_id=row["dispatch_id"],
            repository=row["repository"],
            pr_number=row["pr_number"],
            pr_url=row["pr_url"],
            base_branch=row["base_branch"],
            head_sha=row["head_sha"],
            originating_instance_id=row["originating_instance_id"],
            authority_instance_id=row["authority_instance_id"],
            originating_session_id=row["originating_session_id"],
            originating_principal_id=row["originating_principal_id"],
            originating_agent=row["originating_agent"],
            executor_cwd=row["executor_cwd"],
            provenance_version=row["provenance_version"],
            creation_reason=row["creation_reason"],
            qualifying_evidence=row["qualifying_evidence"],
            policy=json.loads(row["policy_json"] or "{}"),
            required_capabilities=json.loads(row["required_capabilities_json"] or "[]"),
            status=row["status"],
            owner_instance_id=row["owner_instance_id"],
            fence_token=row["fence_token"],
            lease_version=row["lease_version"],
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
