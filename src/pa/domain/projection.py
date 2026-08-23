"""Card projection service — applies events to SQLite."""

from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import TypeVar
from uuid import NAMESPACE_URL, uuid4, uuid5

from pydantic import ValidationError

from pa.domain.models import (
    AgentSession,
    Card,
    CardAttachment,
    CardCreate,
    CardEvent,
    CardKind,
    CardLane,
    CardSummarySource,
    CardSummaryStatus,
    CardUpdate,
    EventType,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    ItemUpdate,
    KnowledgeAuditEvent,
    KnowledgeEntry,
    KnowledgeProvenance,
    Project,
    ProjectCreate,
    ProjectMembership,
    ProjectRepo,
    ProjectRepository,
    ProjectStatus,
    ProjectUpdate,
    Repository,
    RepositoryCheckout,
    RepositoryCreate,
    RepositoryRemote,
    RepositoryStatus,
    RepositoryUpdate,
    RepositoryVisibility,
    RestartHandoff,
    TranscriptEvent,
    lane_from_legacy_status,
)
from pa.domain.transcript_storage import TranscriptStorage
from pa.domain.notifications import Notification
from pa.fleet.policy import (
    FleetPolicyAuditEvent,
    GroupLifecycle,
    InstanceGroup,
    InstanceGroupCreate,
    InstanceGroupUpdate,
    InstanceParticipationPolicy,
    PlacementDefault,
    default_scope_key,
)
from pa.sync.event_log import DagIndexStaleError, EventHistoryError, EventLog
from pa.workloads import WorkloadProfile, canonical_default_scope_key

T = TypeVar("T")


class CardVersionConflict(RuntimeError):
    def __init__(self, card_id: str, expected: datetime, actual: datetime) -> None:
        super().__init__(f"card {card_id} changed since version {expected.isoformat()}")
        self.card_id = card_id
        self.expected = expected
        self.actual = actual


class MutationOperationConflict(RuntimeError):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            "idempotency key already belongs to a different mutation payload"
        )
        self.idempotency_key = idempotency_key


class MutationOperationInProgress(RuntimeError):
    def __init__(self, idempotency_key: str, correlation_id: str | None) -> None:
        super().__init__("mutation is still in progress")
        self.idempotency_key = idempotency_key
        self.correlation_id = correlation_id


class MutationOperationFailed(RuntimeError):
    def __init__(self, idempotency_key: str, error_code: str | None) -> None:
        super().__init__("the recorded mutation failed before a durable outcome")
        self.idempotency_key = idempotency_key
        self.error_code = error_code


def _coerce_datetime(value: object) -> datetime | None:
    """Parse event-payload timestamps without inventing a new wall-clock time."""
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def serialized_mutation(method: Callable[..., T]) -> Callable[..., T]:
    """Keep ref advancement, projection application, and checkpoint ordered."""

    @wraps(method)
    def wrapped(self: CardProjection, *args, **kwargs):
        with self._mutation_lock:
            return method(self, *args, **kwargs)

    return wrapped


class CardProjection:
    def __init__(self, db_path: Path, event_log: EventLog | None = None) -> None:
        self.db_path = db_path
        self.event_log = event_log
        self._mutation_lock = threading.RLock()
        self._connection_local = threading.local()
        self._legacy_integrity_upgrade_required = False
        self._operation_owner = str(uuid4())
        self._replaying_from_log = False
        with sqlite3.connect(self.db_path, timeout=30) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
        self._init_db()
        self.transcripts = TranscriptStorage(db_path)
        self._migrate_legacy_transcripts()
        if self._legacy_integrity_upgrade_required and self.event_log:
            for realm in {ref.realm_id for ref in self.event_log.list_refs()}:
                self.rebuild_from_log(realm)

    @contextmanager
    def _conn(
        self, *, busy_timeout_ms: int = 30000
    ) -> Iterator[sqlite3.Connection]:
        current = getattr(self._connection_local, "connection", None)
        if current is not None:
            yield current
            return
        conn = sqlite3.connect(self.db_path, timeout=busy_timeout_ms / 1000)
        conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.row_factory = sqlite3.Row
        self._connection_local.connection = conn
        try:
            yield conn
            conn.commit()
        except BaseException:
            conn.rollback()
            raise
        finally:
            del self._connection_local.connection
            conn.close()

    @contextmanager
    def mutation(self) -> Iterator[None]:
        """Serialize a complete event-log and projection mutation."""
        with self._mutation_lock:
            yield

    def _init_db(self) -> None:
        with self._conn() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS cards (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL DEFAULT 'default',
                    kind TEXT NOT NULL DEFAULT 'task',
                    title TEXT NOT NULL,
                    body TEXT NOT NULL DEFAULT '',
                    summary TEXT NOT NULL DEFAULT '',
                    summary_source TEXT NOT NULL DEFAULT 'fallback',
                    summary_status TEXT NOT NULL DEFAULT 'pending',
                    summary_provider TEXT,
                    summary_model TEXT,
                    summary_auth_source TEXT,
                    summary_prompt_version TEXT,
                    summary_input_hash TEXT,
                    summary_failure TEXT,
                    summary_failure_code TEXT,
                    summary_attempt_count INTEGER NOT NULL DEFAULT 0,
                    summary_next_attempt_at TEXT,
                    summary_last_attempted_at TEXT,
                    summary_authority_instance_id TEXT,
                    summary_updated_at TEXT,
                    summary_stale INTEGER NOT NULL DEFAULT 0,
                    lane TEXT NOT NULL DEFAULT 'inbox',
                    parent_id TEXT,
                    tags TEXT NOT NULL DEFAULT '[]',
                    visibility TEXT NOT NULL DEFAULT 'realm',
                    owner_principal TEXT,
                    preferred_instance TEXT,
                    preferred_capabilities TEXT NOT NULL DEFAULT '[]',
                    lease_holder_instance TEXT,
                    lease_holder_principal TEXT,
                    lease_expires_at TEXT,
                    created_by_principal TEXT,
                    created_by_instance TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_cards_realm ON cards(realm_id);
                CREATE INDEX IF NOT EXISTS idx_cards_lane ON cards(lane);
                CREATE INDEX IF NOT EXISTS idx_cards_realm_lane_updated
                    ON cards(realm_id, lane, updated_at DESC);
                CREATE TABLE IF NOT EXISTS work_saved_views (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    name TEXT NOT NULL,
                    query TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(realm_id, principal_id, name)
                );
                CREATE INDEX IF NOT EXISTS idx_work_saved_views_scope
                    ON work_saved_views(realm_id, principal_id, name);
                CREATE TABLE IF NOT EXISTS work_saved_view_audit (
                    id TEXT PRIMARY KEY,
                    view_id TEXT NOT NULL,
                    principal_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    query TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL DEFAULT 'default',
                    title TEXT NOT NULL,
                    description TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active',
                    memberships TEXT NOT NULL DEFAULT '[]',
                    repos TEXT NOT NULL DEFAULT '[]',
                    agent_prompt TEXT NOT NULL DEFAULT '',
                    tool_config TEXT NOT NULL DEFAULT '{}',
                    tags TEXT NOT NULL DEFAULT '[]',
                    created_by_principal TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_projects_realm ON projects(realm_id);
                CREATE INDEX IF NOT EXISTS idx_projects_status ON projects(status);
                CREATE TABLE IF NOT EXISTS repositories (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL DEFAULT 'default',
                    url TEXT NOT NULL, name TEXT NOT NULL DEFAULT '',
                    remotes TEXT NOT NULL DEFAULT '[]', default_branch TEXT,
                    provider TEXT NOT NULL DEFAULT '', provider_repository_id TEXT,
                    provider_metadata TEXT NOT NULL DEFAULT '{}',
                    visibility TEXT NOT NULL DEFAULT 'realm',
                    status TEXT NOT NULL DEFAULT 'active', archived_at TEXT,
                    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
                    UNIQUE(realm_id, url)
                );
                CREATE TABLE IF NOT EXISTS project_repositories (
                    project_id TEXT NOT NULL, repository_id TEXT NOT NULL, branch TEXT,
                    PRIMARY KEY(project_id, repository_id)
                );
                CREATE TABLE IF NOT EXISTS repository_checkouts (
                    repository_id TEXT NOT NULL, instance_id TEXT NOT NULL,
                    path TEXT NOT NULL, branch TEXT,
                    PRIMARY KEY(repository_id, instance_id)
                );
                CREATE TABLE IF NOT EXISTS items (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    title TEXT NOT NULL,
                    body TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'open',
                    parent_id TEXT,
                    tags TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_sessions (
                    id TEXT PRIMARY KEY,
                    agent_name TEXT NOT NULL,
                    external_session_id TEXT,
                    origin_instance_id TEXT,
                    origin_instance_name TEXT,
                    authority_instance_id TEXT,
                    dispatch_id TEXT,
                    lifecycle_owner TEXT NOT NULL DEFAULT 'standalone',
                    realm_id TEXT NOT NULL DEFAULT 'default',
                    item_id TEXT,
                    card_id TEXT,
                    principal_id TEXT,
                    status TEXT NOT NULL DEFAULT 'idle',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_restart_handoffs (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL,
                    continuation_prompt TEXT NOT NULL,
                    continuation_prompt_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    card_id TEXT,
                    project_id TEXT,
                    instance_id TEXT,
                    execution_binding_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    delivered_at TEXT,
                    UNIQUE(session_id, idempotency_key),
                    UNIQUE(continuation_prompt_id)
                );
                CREATE TABLE IF NOT EXISTS agent_execution_binding_history (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    prior_binding_json TEXT NOT NULL,
                    binding_json TEXT NOT NULL,
                    changed_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS agent_session_cards (
                    session_id TEXT NOT NULL,
                    card_id TEXT NOT NULL,
                    realm_id TEXT NOT NULL DEFAULT 'default',
                    linked_by_principal TEXT,
                    linked_at TEXT NOT NULL,
                    retired_at TEXT,
                    retired_reason TEXT,
                    retired_by_principal TEXT,
                    PRIMARY KEY(session_id, card_id)
                );
                CREATE TABLE IF NOT EXISTS agent_session_card_history (
                    session_id TEXT NOT NULL,
                    card_id TEXT NOT NULL,
                    realm_id TEXT NOT NULL DEFAULT 'default',
                    linked_by_principal TEXT,
                    linked_at TEXT NOT NULL,
                    retired_at TEXT NOT NULL,
                    retired_reason TEXT,
                    retired_by_principal TEXT,
                    PRIMARY KEY(session_id, card_id, linked_at, retired_at)
                );
                CREATE TABLE IF NOT EXISTS knowledge (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    item_id TEXT,
                    card_id TEXT,
                    summary TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'manual',
                    source_url TEXT,
                    kind TEXT NOT NULL DEFAULT 'memory',
                    tier TEXT NOT NULL DEFAULT 'semantic',
                    status TEXT NOT NULL DEFAULT 'active',
                    scope TEXT NOT NULL DEFAULT 'realm',
                    owner TEXT,
                    confidence REAL,
                    sensitivity TEXT NOT NULL DEFAULT 'internal',
                    provenance_trust TEXT NOT NULL DEFAULT 'unverified',
                    supersedes_id TEXT,
                    review_at TEXT,
                    expires_at TEXT,
                    tags TEXT NOT NULL DEFAULT '[]',
                    content_hash TEXT NOT NULL DEFAULT '',
                    provenance TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge_audit_events (
                    id TEXT PRIMARY KEY,
                    knowledge_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_knowledge_audit_entry_time
                    ON knowledge_audit_events(knowledge_id, created_at);
                CREATE TABLE IF NOT EXISTS agent_transcript_events (
                    id TEXT PRIMARY KEY,
                    session_id TEXT NOT NULL,
                    seq INTEGER NOT NULL,
                    event_type TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    UNIQUE(session_id, seq)
                );
                CREATE INDEX IF NOT EXISTS idx_transcript_session_seq
                    ON agent_transcript_events(session_id, seq);
                CREATE TABLE IF NOT EXISTS sync_projection_heads (
                    realm_id TEXT PRIMARY KEY,
                    head_hash TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS projection_migrations (
                    name TEXT PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS mutation_operations (
                    idempotency_key TEXT PRIMARY KEY,
                    operation TEXT NOT NULL,
                    request_fingerprint TEXT NOT NULL,
                    realm_id TEXT NOT NULL,
                    state TEXT NOT NULL,
                    owner_token TEXT NOT NULL,
                    correlation_id TEXT,
                    event_id TEXT,
                    event_hash TEXT,
                    commit_hash TEXT,
                    result_json TEXT,
                    error_code TEXT,
                    recovery_state TEXT NOT NULL DEFAULT 'pending',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_mutation_operations_realm
                    ON mutation_operations(realm_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_mutation_operations_state_updated
                    ON mutation_operations(state, updated_at);
                CREATE TABLE IF NOT EXISTS instance_groups (
                    realm_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(realm_id, id)
                );
                CREATE INDEX IF NOT EXISTS idx_instance_groups_realm
                    ON instance_groups(realm_id);
                CREATE TABLE IF NOT EXISTS instance_participation_policies (
                    realm_id TEXT NOT NULL,
                    instance_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(realm_id, instance_id)
                );
                CREATE INDEX IF NOT EXISTS idx_instance_policies_realm
                    ON instance_participation_policies(realm_id);
                CREATE TABLE IF NOT EXISTS placement_defaults (
                    realm_id TEXT NOT NULL,
                    scope_key TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(realm_id, scope_key)
                );
                CREATE TABLE IF NOT EXISTS notifications (
                    realm_id TEXT NOT NULL,
                    id TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    type TEXT NOT NULL DEFAULT 'general',
                    priority TEXT NOT NULL DEFAULT 'normal',
                    visibility TEXT NOT NULL DEFAULT 'realm',
                    principal_id TEXT,
                    outstanding INTEGER NOT NULL DEFAULT 0,
                    unread INTEGER NOT NULL DEFAULT 1,
                    resolved INTEGER NOT NULL DEFAULT 0,
                    deduplication_key TEXT,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT,
                    payload TEXT NOT NULL,
                    PRIMARY KEY(realm_id, id)
                );
                CREATE INDEX IF NOT EXISTS idx_notifications_realm_updated
                    ON notifications(realm_id, updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_notifications_realm_outstanding
                    ON notifications(realm_id, outstanding, updated_at DESC);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_notifications_realm_dedup
                    ON notifications(realm_id, deduplication_key)
                    WHERE deduplication_key IS NOT NULL;
                CREATE TABLE IF NOT EXISTS notification_audit_events (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL,
                    notification_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    version INTEGER,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notification_audit_notice_time
                    ON notification_audit_events(notification_id, created_at);
                CREATE TABLE IF NOT EXISTS fleet_policy_audit_events (
                    id TEXT PRIMARY KEY,
                    realm_id TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    payload TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_fleet_policy_audit_realm_time
                    ON fleet_policy_audit_events(realm_id, created_at);
                """
            )
            self._migrate_items_to_cards(conn)
            self._migrate_schema(conn)
            self._migrate_project_repositories(conn)

            from pa.goals.projection import (
                goal_projection_requires_legacy_id_rebuild,
                init_goal_schema,
            )
            from pa.intake.projection import init_intake_schema
            from pa.limbic.projection import init_limbic_schema

            init_goal_schema(conn)
            if goal_projection_requires_legacy_id_rebuild(conn):
                self._legacy_integrity_upgrade_required = True
            init_intake_schema(conn)
            init_limbic_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        card_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(cards)").fetchall()
        }
        if "project_id" not in card_cols:
            conn.execute("ALTER TABLE cards ADD COLUMN project_id TEXT")
        for col, decl in (
            ("summary", "TEXT NOT NULL DEFAULT ''"),
            ("summary_source", "TEXT NOT NULL DEFAULT 'fallback'"),
            ("summary_updated_at", "TEXT"),
            ("summary_stale", "INTEGER NOT NULL DEFAULT 0"),
            ("summary_status", "TEXT NOT NULL DEFAULT 'pending'"),
            ("summary_provider", "TEXT"),
            ("summary_model", "TEXT"),
            ("summary_auth_source", "TEXT"),
            ("summary_prompt_version", "TEXT"),
            ("summary_input_hash", "TEXT"),
            ("summary_failure", "TEXT"),
            ("summary_failure_code", "TEXT"),
            ("summary_attempt_count", "INTEGER NOT NULL DEFAULT 0"),
            ("summary_next_attempt_at", "TEXT"),
            ("summary_last_attempted_at", "TEXT"),
            ("summary_authority_instance_id", "TEXT"),
            ("attachments", "TEXT NOT NULL DEFAULT '[]'"),
        ):
            if col not in card_cols:
                conn.execute(f"ALTER TABLE cards ADD COLUMN {col} {decl}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_summary_failure_time "
            "ON cards(summary_last_attempted_at DESC) "
            "WHERE summary_failure_code IS NOT NULL"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_summary_worker "
            "ON cards(summary_status, summary_source, summary_next_attempt_at, "
            "summary_attempt_count, updated_at) "
            "WHERE summary_source != 'manual'"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_summary_migration_page "
            "ON cards(updated_at DESC, id DESC) "
            "WHERE summary_source != 'manual' AND summary != ''"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_realm_project_updated "
            "ON cards(realm_id, project_id, updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_cards_realm_parent "
            "ON cards(realm_id, parent_id)"
        )

        notification_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(notifications)").fetchall()
        }
        for col, decl in (
            ("visibility", "TEXT NOT NULL DEFAULT 'realm'"),
            ("principal_id", "TEXT"),
            ("resolved", "INTEGER NOT NULL DEFAULT 0"),
        ):
            if col not in notification_cols:
                conn.execute(f"ALTER TABLE notifications ADD COLUMN {col} {decl}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notifications_realm_resolved "
            "ON notifications(realm_id, resolved, updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_notifications_realm_principal "
            "ON notifications(realm_id, visibility, principal_id, outstanding)"
        )

        session_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(agent_sessions)").fetchall()
        }
        if "card_id" not in session_cols:
            conn.execute("ALTER TABLE agent_sessions ADD COLUMN card_id TEXT")
            conn.execute(
                "UPDATE agent_sessions SET card_id = item_id WHERE card_id IS NULL AND item_id IS NOT NULL"
            )
        if "principal_id" not in session_cols:
            conn.execute("ALTER TABLE agent_sessions ADD COLUMN principal_id TEXT")
        if "project_id" not in session_cols:
            conn.execute("ALTER TABLE agent_sessions ADD COLUMN project_id TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_sessions_card_updated "
            "ON agent_sessions(card_id, updated_at DESC)"
        )
        for col, decl in (
            ("origin_instance_id", "TEXT"),
            ("origin_instance_name", "TEXT"),
            ("authority_instance_id", "TEXT"),
            ("dispatch_id", "TEXT"),
            ("lifecycle_owner", "TEXT NOT NULL DEFAULT 'standalone'"),
            ("realm_id", "TEXT NOT NULL DEFAULT 'default'"),
            ("cwd", "TEXT"),
            ("title", "TEXT"),
            ("label", "TEXT"),
            ("model_id", "TEXT"),
            ("mode_id", "TEXT"),
            ("config_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("metrics_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("execution_binding_json", "TEXT NOT NULL DEFAULT '{}'"),
        ):
            if col not in session_cols:
                conn.execute(f"ALTER TABLE agent_sessions ADD COLUMN {col} {decl}")
                if col == "lifecycle_owner":
                    conn.execute(
                        "UPDATE agent_sessions SET lifecycle_owner = 'dispatch' "
                        "WHERE dispatch_id IS NOT NULL"
                    )
        link_cols = {
            row[1]
            for row in conn.execute("PRAGMA table_info(agent_session_cards)").fetchall()
        }
        for col in ("retired_at", "retired_reason", "retired_by_principal"):
            if col not in link_cols:
                conn.execute(f"ALTER TABLE agent_session_cards ADD COLUMN {col} TEXT")
        conn.execute(
            """CREATE TABLE IF NOT EXISTS agent_session_card_history (
                   session_id TEXT NOT NULL,
                   card_id TEXT NOT NULL,
                   realm_id TEXT NOT NULL DEFAULT 'default',
                   linked_by_principal TEXT,
                   linked_at TEXT NOT NULL,
                   retired_at TEXT NOT NULL,
                   retired_reason TEXT,
                   retired_by_principal TEXT,
                   PRIMARY KEY(session_id, card_id, linked_at, retired_at)
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_sessions_project_history "
            "ON agent_sessions(realm_id, project_id, status, updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_sessions_status_updated "
            "ON agent_sessions(status, updated_at DESC)"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS agent_restart_handoffs (
                   id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                   idempotency_key TEXT NOT NULL, continuation_prompt TEXT NOT NULL,
                   continuation_prompt_id TEXT NOT NULL, status TEXT NOT NULL,
                   card_id TEXT, project_id TEXT, instance_id TEXT,
                   execution_binding_json TEXT NOT NULL DEFAULT '{}', error TEXT,
                   attempts INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL,
                   updated_at TEXT NOT NULL, delivered_at TEXT,
                   UNIQUE(session_id, idempotency_key), UNIQUE(continuation_prompt_id)
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_restart_handoffs_status_updated "
            "ON agent_restart_handoffs(status, updated_at)"
        )
        conn.execute(
            """CREATE TABLE IF NOT EXISTS agent_execution_binding_history (
                   id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                   reason TEXT NOT NULL, prior_binding_json TEXT NOT NULL,
                   binding_json TEXT NOT NULL, changed_at TEXT NOT NULL
               )"""
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_execution_binding_history_session "
            "ON agent_execution_binding_history(session_id, changed_at)"
        )
        # A browser default is a durable identity, not merely the most recently
        # touched row carrying a convenient label.  Older databases can contain
        # duplicates from reconnect races.  Keep the explicitly selected row
        # (or the oldest recoverable identity, which predates the replacement
        # regression) and retire only the duplicate label before enforcing the
        # invariant for future writers.
        conn.execute(
            """
            WITH ranked AS (
                SELECT id,
                       ROW_NUMBER() OVER (
                           ORDER BY
                               CASE WHEN json_extract(config_json,
                                   '$.browser_default_selected') = 1 THEN 0 ELSE 1 END,
                               CASE WHEN status = 'closed' THEN 2 ELSE 0 END,
                               created_at ASC,
                               id ASC
                       ) AS label_rank
                FROM agent_sessions
                WHERE label = 'default' AND status != 'closed'
            )
            UPDATE agent_sessions
            SET label = NULL
            WHERE id IN (SELECT id FROM ranked WHERE label_rank > 1)
            """
        )
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_agent_sessions_one_default "
            "ON agent_sessions(label) WHERE label = 'default' AND status != 'closed'"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_session_cards (
                session_id TEXT NOT NULL,
                card_id TEXT NOT NULL,
                realm_id TEXT NOT NULL DEFAULT 'default',
                linked_by_principal TEXT,
                linked_at TEXT NOT NULL,
                PRIMARY KEY(session_id, card_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_agent_session_cards_card "
            "ON agent_session_cards(realm_id, card_id, linked_at DESC)"
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO agent_session_cards
                (session_id, card_id, realm_id, linked_by_principal, linked_at)
            SELECT id, card_id, realm_id, principal_id, created_at
            FROM agent_sessions
            WHERE card_id IS NOT NULL
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS agent_transcript_events (
                id TEXT PRIMARY KEY,
                session_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                UNIQUE(session_id, seq)
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_transcript_session_seq
                ON agent_transcript_events(session_id, seq)
            """
        )

        knowledge_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(knowledge)").fetchall()
        }
        if knowledge_cols and "card_id" not in knowledge_cols:
            conn.execute("ALTER TABLE knowledge ADD COLUMN card_id TEXT")
            conn.execute(
                "UPDATE knowledge SET card_id = item_id WHERE card_id IS NULL AND item_id IS NOT NULL"
            )
        for col, decl in (
            ("source_url", "TEXT"),
            ("kind", "TEXT NOT NULL DEFAULT 'memory'"),
            ("tier", "TEXT NOT NULL DEFAULT 'semantic'"),
            ("status", "TEXT NOT NULL DEFAULT 'active'"),
            ("scope", "TEXT NOT NULL DEFAULT 'realm'"),
            ("owner", "TEXT"),
            ("confidence", "REAL"),
            ("sensitivity", "TEXT NOT NULL DEFAULT 'internal'"),
            ("provenance_trust", "TEXT NOT NULL DEFAULT 'unverified'"),
            ("supersedes_id", "TEXT"),
            ("review_at", "TEXT"),
            ("expires_at", "TEXT"),
            ("updated_at", "TEXT"),
            ("content_hash", "TEXT NOT NULL DEFAULT ''"),
            ("provenance", "TEXT"),
        ):
            if knowledge_cols and col not in knowledge_cols:
                conn.execute(f"ALTER TABLE knowledge ADD COLUMN {col} {decl}")
        if knowledge_cols and "provenance_trust" not in knowledge_cols:
            conn.execute("UPDATE knowledge SET provenance_trust = 'legacy'")
        if knowledge_cols and "updated_at" not in knowledge_cols:
            conn.execute("UPDATE knowledge SET updated_at = created_at")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_knowledge_card "
            "ON knowledge(card_id, updated_at DESC)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_knowledge_cursor "
            "ON knowledge(status, updated_at DESC, id DESC)"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS knowledge_audit_events (
                id TEXT PRIMARY KEY,
                knowledge_id TEXT NOT NULL,
                action TEXT NOT NULL,
                actor TEXT NOT NULL,
                payload TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_knowledge_audit_entry_time
            ON knowledge_audit_events(knowledge_id, created_at)
            """
        )

        repository_cols = {
            row[1] for row in conn.execute("PRAGMA table_info(repositories)").fetchall()
        }
        for col, decl in (
            ("remotes", "TEXT NOT NULL DEFAULT '[]'"),
            ("default_branch", "TEXT"),
            ("provider", "TEXT NOT NULL DEFAULT ''"),
            ("provider_repository_id", "TEXT"),
            ("provider_metadata", "TEXT NOT NULL DEFAULT '{}'"),
            ("visibility", "TEXT NOT NULL DEFAULT 'realm'"),
            ("status", "TEXT NOT NULL DEFAULT 'active'"),
            ("archived_at", "TEXT"),
        ):
            if col not in repository_cols:
                conn.execute(f"ALTER TABLE repositories ADD COLUMN {col} {decl}")
        for row in conn.execute("SELECT id, url, remotes FROM repositories").fetchall():
            if not json.loads(row["remotes"] or "[]"):
                conn.execute(
                    "UPDATE repositories SET remotes=? WHERE id=?",
                    (
                        json.dumps(
                            [
                                {
                                    "name": "origin",
                                    "fetch_url": row["url"],
                                    "push_url": row["url"],
                                }
                            ]
                        ),
                        row["id"],
                    ),
                )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_repositories_realm_status ON repositories(realm_id, status)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_mutation_operations_state_updated "
            "ON mutation_operations(state, updated_at)"
        )

    def _migrate_items_to_cards(self, conn: sqlite3.Connection) -> None:
        migration = "legacy_items_to_cards_v2_monotonic"
        if conn.execute(
            "SELECT 1 FROM projection_migrations WHERE name=?", (migration,)
        ).fetchone():
            return
        # Once a durable realm exists it is authoritative. Replaying the old
        # items table into an already-checkpointed projection can resurrect
        # status=open as lane=inbox without any event or provenance.
        if self.event_log and self.event_log.get_head("default"):
            self._legacy_integrity_upgrade_required = True
            conn.execute(
                "INSERT INTO projection_migrations (name, applied_at) VALUES (?, ?)",
                (migration, datetime.now(UTC).isoformat()),
            )
            return
        rows = conn.execute("SELECT * FROM items").fetchall()
        lane_map = {
            "open": "inbox",
            "active": "active",
            "blocked": "waiting",
            "done": "done",
            "archived": "done",
        }
        for row in rows:
            conn.execute(
                """
                INSERT OR IGNORE INTO cards
                (id, realm_id, kind, title, body, lane, parent_id, tags, created_at, updated_at)
                VALUES (?, 'default', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["id"],
                    row["kind"],
                    row["title"],
                    row["body"],
                    lane_map.get(row["status"], "inbox"),
                    row["parent_id"],
                    row["tags"],
                    row["created_at"],
                    row["updated_at"],
                ),
            )
        conn.execute(
            "INSERT INTO projection_migrations (name, applied_at) VALUES (?, ?)",
            (migration, datetime.now(UTC).isoformat()),
        )

    def _repository_id(self, realm_id: str, url: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"pa:{realm_id}:{url.strip()}"))

    @staticmethod
    def _default_repository_remotes(url: str) -> list[RepositoryRemote]:
        return [RepositoryRemote(name="origin", fetch_url=url, push_url=url)]

    @staticmethod
    def _repository_from_row(row: sqlite3.Row) -> Repository:
        data = dict(row)
        data.pop("project_branch", None)
        data["remotes"] = [
            RepositoryRemote.model_validate(remote)
            for remote in json.loads(data.get("remotes") or "[]")
        ]
        data["provider_metadata"] = json.loads(data.get("provider_metadata") or "{}")
        return Repository(**data)

    def _replace_project_repositories_conn(
        self, conn, project_id: str, realm_id: str, repos: list, instance_id: str
    ) -> None:
        conn.execute(
            "DELETE FROM project_repositories WHERE project_id = ?", (project_id,)
        )
        now = datetime.now(UTC).isoformat()
        for raw in repos:
            repo = ProjectRepo.model_validate(raw)
            url = repo.url.strip()
            repository_id = self._repository_id(realm_id, url)
            conn.execute(
                "INSERT OR IGNORE INTO repositories (id, realm_id, url, name, remotes, created_at, updated_at) VALUES (?, ?, ?, '', ?, ?, ?)",
                (
                    repository_id,
                    realm_id,
                    url,
                    json.dumps(
                        [
                            remote.model_dump(mode="json")
                            for remote in self._default_repository_remotes(url)
                        ]
                    ),
                    now,
                    now,
                ),
            )
            actual = conn.execute(
                "SELECT id FROM repositories WHERE realm_id=? AND url=?",
                (realm_id, url),
            ).fetchone()["id"]
            conn.execute(
                "INSERT OR REPLACE INTO project_repositories (project_id, repository_id, branch) VALUES (?, ?, ?)",
                (project_id, actual, repo.branch),
            )
            if repo.path:
                conn.execute(
                    "INSERT OR REPLACE INTO repository_checkouts (repository_id, instance_id, path, branch) VALUES (?, ?, ?, ?)",
                    (actual, instance_id, repo.path, repo.branch),
                )
        # Normalized rows are authoritative. Clearing the compatibility cache
        # prevents unlink/delete operations from resurrecting legacy entries.
        conn.execute("UPDATE projects SET repos='[]' WHERE id=?", (project_id,))

    def _migrate_project_repositories(self, conn) -> None:
        instance_id = self.event_log.instance_id if self.event_log else "local"
        for row in conn.execute("SELECT id, realm_id, repos FROM projects").fetchall():
            try:
                repos = json.loads(row["repos"] or "[]")
            except TypeError, json.JSONDecodeError:
                continue
            existing = conn.execute(
                "SELECT 1 FROM project_repositories WHERE project_id=? LIMIT 1",
                (row["id"],),
            ).fetchone()
            if not existing and repos:
                self._replace_project_repositories_conn(
                    conn, row["id"], row["realm_id"], repos, instance_id
                )
            elif existing and repos:
                conn.execute("UPDATE projects SET repos='[]' WHERE id=?", (row["id"],))

    def apply_event(self, event: CardEvent) -> None:
        if event.type == EventType.GOAL_UPSERTED:
            from pa.goals.projection import apply_goal_event

            apply_goal_event(self, event)
        elif event.type == EventType.GOAL_GOVERNANCE_UPSERTED:
            from pa.goals.projection import apply_goal_governance_event

            apply_goal_governance_event(self, event)
        elif event.type in {
            EventType.INTAKE_ENVELOPE_UPSERTED,
            EventType.CHANNEL_IDENTITY_UPSERTED,
        }:
            from pa.intake.projection import apply_intake_event

            apply_intake_event(self, event)
        elif event.type == EventType.LIMBIC_APPRAISED:
            from pa.limbic.projection import apply_limbic_event

            apply_limbic_event(self, event)
        elif event.type == EventType.MEMORY_RECORDED:
            from pa.limbic.projection import apply_memory_event

            apply_memory_event(self, event)
        elif event.type == EventType.CARD_CREATED:
            self._apply_created(event)
        elif event.type == EventType.CARD_UPSERTED:
            self._apply_upserted(event)
        elif event.type == EventType.CARD_UPDATED:
            self._apply_updated(event)
        elif event.type == EventType.CARD_DELETED:
            self._apply_deleted(event)
        elif event.type == EventType.ATTACHMENT_CREATED:
            self._apply_attachment_created(event)
        elif event.type == EventType.ATTACHMENT_REMOVED:
            self._apply_attachment_removed(event)
        elif event.type == EventType.LEASE_GRANTED:
            self._apply_lease(event)
        elif event.type == EventType.LEASE_RELEASED:
            self._apply_lease_release(event)
        elif event.type == EventType.PROJECT_CREATED:
            self._apply_project_created(event)
        elif event.type == EventType.PROJECT_UPDATED:
            self._apply_project_updated(event)
        elif event.type == EventType.PROJECT_ARCHIVED:
            self._apply_project_archived(event)
        elif event.type == EventType.REPOSITORY_CREATED:
            self._apply_repository_created(event)
        elif event.type == EventType.REPOSITORY_UPDATED:
            self._apply_repository_updated(event)
        elif event.type == EventType.REPOSITORY_DELETED:
            self._apply_repository_deleted(event)
        elif event.type == EventType.PROJECT_REPOSITORY_LINKED:
            self._apply_project_repository_linked(event)
        elif event.type == EventType.PROJECT_REPOSITORY_UNLINKED:
            self._apply_project_repository_unlinked(event)
        elif event.type == EventType.REPOSITORY_CHECKOUT_SET:
            self._apply_repository_checkout_set(event)
        elif event.type == EventType.REPOSITORY_CHECKOUT_REMOVED:
            self._apply_repository_checkout_removed(event)
        elif event.type in {
            EventType.INSTANCE_GROUP_CREATED,
            EventType.INSTANCE_GROUP_UPDATED,
            EventType.INSTANCE_GROUP_ARCHIVED,
            EventType.INSTANCE_GROUP_DELETED,
        }:
            self._apply_instance_group_event(event)
        elif event.type == EventType.INSTANCE_PARTICIPATION_POLICY_UPDATED:
            self._apply_instance_participation_policy_event(event)
        elif event.type in {
            EventType.PLACEMENT_DEFAULT_UPDATED,
            EventType.PLACEMENT_DEFAULT_DELETED,
        }:
            self._apply_placement_default_event(event)
        elif event.type in {
            EventType.NOTIFICATION_UPSERTED,
            EventType.NOTIFICATION_DELETED,
        }:
            self._apply_notification_event(event)

    def _apply_notification_event(self, event: CardEvent) -> None:
        notification_id = str(event.payload.get("id") or "")
        if not notification_id:
            return
        if event.type == EventType.NOTIFICATION_DELETED:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM notifications WHERE realm_id=? AND id=?",
                    (event.realm_id, notification_id),
                )
                conn.execute(
                    """
                    INSERT OR IGNORE INTO notification_audit_events
                    (id, realm_id, notification_id, action, actor, version, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        event.id,
                        event.realm_id,
                        notification_id,
                        event.type.value,
                        event.author_principal,
                        event.payload.get("version"),
                        event.timestamp.isoformat(),
                    ),
                )
            return
        with self._conn() as conn:
            row = conn.execute(
                "SELECT payload, version FROM notifications WHERE realm_id=? AND id=?",
                (event.realm_id, notification_id),
            ).fetchone()
            current = json.loads(row["payload"]) if row else {}
            prior = dict(current)
            incoming_version = int(event.payload.get("version") or 1)
            current_version = int(row["version"]) if row else 0
            if incoming_version < current_version:
                return
            current.update(event.payload)
            current["realm_id"] = event.realm_id
            try:
                notification = Notification.model_validate(current)
            except ValidationError:
                # A field-only automatic merge resolution can arrive before the
                # originating upsert on a partially materialized peer. The full
                # event will make the row valid when its history is replayed.
                return
            payload = json.dumps(notification.model_dump(mode="json"))
            action = "created" if not prior else "updated"
            if prior:
                if not prior.get("read_at") and notification.read_at:
                    action = "read"
                if not prior.get("acknowledged_at") and notification.acknowledged_at:
                    action = "acknowledged"
                if not prior.get("resolved_at") and notification.resolved_at:
                    action = "resolved"
                prior_interaction = prior.get("interaction") or {}
                if (
                    notification.interaction
                    and prior_interaction.get("state")
                    != notification.interaction.state.value
                ):
                    action = f"interaction.{notification.interaction.state.value}"
            conn.execute(
                """
                INSERT INTO notifications
                    (realm_id, id, version, type, priority, visibility,
                     principal_id, outstanding, unread, resolved,
                     deduplication_key, updated_at, expires_at, payload)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(realm_id, id) DO UPDATE SET
                    version=excluded.version,
                    type=excluded.type,
                    priority=excluded.priority,
                    visibility=excluded.visibility,
                    principal_id=excluded.principal_id,
                    outstanding=excluded.outstanding,
                    unread=excluded.unread,
                    resolved=excluded.resolved,
                    deduplication_key=excluded.deduplication_key,
                    updated_at=excluded.updated_at,
                    expires_at=excluded.expires_at,
                    payload=excluded.payload
                WHERE excluded.version >= notifications.version
                """,
                (
                    notification.realm_id,
                    notification.id,
                    notification.version,
                    notification.type.value,
                    notification.priority.value,
                    notification.visibility.value,
                    notification.principal_id,
                    int(notification.outstanding),
                    int(notification.read_at is None),
                    int(notification.resolved_at is not None),
                    notification.deduplication_key,
                    notification.updated_at.isoformat(),
                    notification.expires_at.isoformat()
                    if notification.expires_at
                    else None,
                    payload,
                ),
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO notification_audit_events
                (id, realm_id, notification_id, action, actor, version, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.realm_id,
                    notification.id,
                    action,
                    event.author_principal,
                    notification.version,
                    event.timestamp.isoformat(),
                ),
            )

    @serialized_mutation
    def replay_operation(
        self,
        *,
        idempotency_key: str,
        operation: str,
        request_fingerprint: str,
        realm_id: str,
    ) -> dict | None:
        """Replay an existing receipt without claiming a new operation."""
        key = idempotency_key.strip()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                (key,),
            ).fetchone()
        if row is None:
            self._restore_operation_from_log(
                idempotency_key=key,
                operation=operation,
                request_fingerprint=request_fingerprint,
                realm_id=realm_id,
            )
            with self._conn() as conn:
                row = conn.execute(
                    "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                    (key,),
                ).fetchone()
            if row is None:
                return None
        record = dict(row)
        if (
            record["operation"] != operation
            or record["request_fingerprint"] != request_fingerprint
            or record["realm_id"] != realm_id
        ):
            raise MutationOperationConflict(key)
        if record["state"] == "succeeded" and record.get("result_json"):
            return json.loads(record["result_json"])
        if record["state"] in {"committed", "pending", "failed"}:
            recovered = self._recover_operation(record)
            if recovered is not None:
                return recovered
        if (
            record["state"] == "pending"
            and record["owner_token"] == self._operation_owner
        ):
            raise MutationOperationInProgress(key, record.get("correlation_id"))
        return None

    @serialized_mutation
    def begin_operation(
        self,
        *,
        idempotency_key: str,
        operation: str,
        request_fingerprint: str,
        realm_id: str,
        correlation_id: str | None = None,
    ) -> dict | None:
        """Atomically claim a mutation or replay its authoritative result."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("Idempotency-Key cannot be empty")
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            has_receipt = conn.execute(
                "SELECT 1 FROM mutation_operations WHERE idempotency_key=?",
                (key,),
            ).fetchone()
        if has_receipt is None:
            self._restore_operation_from_log(
                idempotency_key=key,
                operation=operation,
                request_fingerprint=request_fingerprint,
                realm_id=realm_id,
            )
        with self._conn() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                (key,),
            ).fetchone()
            if row is None:
                conn.execute(
                    """
                    INSERT INTO mutation_operations
                    (idempotency_key, operation, request_fingerprint, realm_id,
                     state, owner_token, correlation_id, recovery_state,
                     created_at, updated_at)
                    VALUES (?, ?, ?, ?, 'pending', ?, ?, 'pending', ?, ?)
                    """,
                    (
                        key,
                        operation,
                        request_fingerprint,
                        realm_id,
                        self._operation_owner,
                        correlation_id,
                        now,
                        now,
                    ),
                )
                return None
            record = dict(row)

        if (
            record["operation"] != operation
            or record["request_fingerprint"] != request_fingerprint
            or record["realm_id"] != realm_id
        ):
            raise MutationOperationConflict(key)
        if record["state"] == "succeeded" and record.get("result_json"):
            return json.loads(record["result_json"])
        if record["state"] in {"committed", "pending", "failed"}:
            recovered = self._recover_operation(record)
            if recovered is not None:
                return recovered
        if (
            record["state"] == "pending"
            and record["owner_token"] == self._operation_owner
        ):
            raise MutationOperationInProgress(key, record.get("correlation_id"))

        with self._conn() as conn:
            claimed = conn.execute(
                """
                UPDATE mutation_operations SET state='pending', owner_token=?,
                    correlation_id=?, error_code=NULL, recovery_state='retrying',
                    updated_at=? WHERE idempotency_key=? AND state=?
                    AND owner_token=?
                """,
                (
                    self._operation_owner,
                    correlation_id,
                    now,
                    key,
                    record["state"],
                    record["owner_token"],
                ),
            )
            if claimed.rowcount != 1:
                current = conn.execute(
                    "SELECT correlation_id FROM mutation_operations "
                    "WHERE idempotency_key=?",
                    (key,),
                ).fetchone()
                raise MutationOperationInProgress(
                    key,
                    current["correlation_id"] if current else None,
                )
        return None

    def _restore_operation_from_log(
        self,
        *,
        idempotency_key: str,
        operation: str,
        request_fingerprint: str,
        realm_id: str,
    ) -> None:
        """Restore a durable receipt before admitting a same-key mutation."""
        if not self.event_log:
            return
        found = self.event_log.find_operation_event(realm_id, idempotency_key)
        if not found:
            return
        commit_hash, event_hash, event = found
        if (
            event.source_operation != operation
            or event.request_fingerprint != request_fingerprint
            or event.realm_id != realm_id
        ):
            raise MutationOperationConflict(idempotency_key)
        self.mark_operation_durable(event, commit_hash, event_hash=event_hash)

    def _recover_operation(self, record: dict) -> dict | None:
        if not self.event_log:
            return None
        found = self.event_log.find_operation_event(
            record["realm_id"], record["idempotency_key"]
        )
        if not found:
            return None
        commit_hash, event_hash, event = found
        if (
            event.source_operation != record["operation"]
            or event.request_fingerprint != record["request_fingerprint"]
            or event.realm_id != record["realm_id"]
        ):
            raise MutationOperationConflict(record["idempotency_key"])
        self.mark_operation_durable(event, commit_hash, event_hash=event_hash)

        result = self._hydrate_operation_result(
            event.operation_result, commit_hash
        )
        if result is None and event.card_id:
            result = self.event_log.entity_snapshot(
                commit_hash, "card", event.card_id
            )
        elif result is None and event.project_id:
            result = self.event_log.entity_snapshot(
                commit_hash, "project", event.project_id
            )
        if result is None:
            result = {
                "operation": record["operation"],
                "event_id": event.id,
                "commit_hash": commit_hash,
                "durable": True,
            }

        durable_head = self.event_log.get_head(record["realm_id"])
        if (
            durable_head
            and self.get_projection_head(record["realm_id"]) != durable_head
        ):
            self.rebuild_from_log(record["realm_id"])
        if not event.operation_result_complete:
            return None
        self.complete_operation(
            record["idempotency_key"],
            result,
            recovery_state="recovered_after_durable_append",
        )
        return result

    @staticmethod
    def _hydrate_operation_result(value, commit_hash: str):
        if isinstance(value, dict):
            if value == {"$pa_commit_hash": True}:
                return commit_hash
            return {
                key: CardProjection._hydrate_operation_result(item, commit_hash)
                for key, item in value.items()
            }
        if isinstance(value, list):
            return [
                CardProjection._hydrate_operation_result(item, commit_hash)
                for item in value
            ]
        return value

    @serialized_mutation
    def mark_operation_durable(
        self,
        event: CardEvent,
        commit_hash: str,
        *,
        event_hash: str | None = None,
    ) -> None:
        if not event.idempotency_key or not event.request_fingerprint:
            return
        now = datetime.now(UTC).isoformat()
        result_json = (
            json.dumps(event.operation_result, default=str)
            if event.operation_result is not None
            else None
        )
        replay_keys = getattr(self, "_replay_operation_keys_seen", None)
        first_replay_event = (
            replay_keys is not None and event.idempotency_key not in replay_keys
        )
        if replay_keys is not None:
            replay_keys.add(event.idempotency_key)
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT operation, request_fingerprint, realm_id, state, "
                "event_id, event_hash, commit_hash FROM mutation_operations "
                "WHERE idempotency_key=?",
                (event.idempotency_key,),
            ).fetchone()
            if (
                existing
                and (
                    existing["operation"] != event.source_operation
                    or existing["request_fingerprint"]
                    != event.request_fingerprint
                    or existing["realm_id"] != event.realm_id
                    or (
                        existing["event_id"] is not None
                        and existing["event_id"] != event.id
                    )
                )
            ):
                raise MutationOperationConflict(event.idempotency_key)
            if (
                not existing
                or first_replay_event
                or existing["event_id"] is None
            ):
                try:
                    self.event_log.validate_operation_event_origin(
                        commit_hash, event_hash or "", event
                    )
                except EventHistoryError as exc:
                    raise MutationOperationConflict(
                        event.idempotency_key
                    ) from exc
            if (
                existing
                and not first_replay_event
                and existing["event_id"] == event.id
                and existing["event_hash"]
                and event_hash
                and (
                    existing["event_hash"] != event_hash
                    or existing["commit_hash"] != commit_hash
                )
            ):
                if existing["event_hash"] == event_hash:
                    raise MutationOperationConflict(event.idempotency_key)
                previous = self.event_log.get_event(existing["event_hash"])
                if previous is None or not existing["commit_hash"]:
                    raise MutationOperationConflict(event.idempotency_key)
                try:
                    self.event_log.validate_operation_event_revision(
                        existing["commit_hash"],
                        existing["event_hash"],
                        previous,
                        commit_hash,
                        event_hash,
                        event,
                    )
                except EventHistoryError as exc:
                    raise MutationOperationConflict(
                        event.idempotency_key
                    ) from exc
            conn.execute(
                """
                INSERT INTO mutation_operations
                (idempotency_key, operation, request_fingerprint, realm_id,
                 state, owner_token, event_id, event_hash, commit_hash,
                 result_json, recovery_state, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'committed', ?, ?, ?, ?, ?,
                        'durable_append_complete', ?, ?)
                ON CONFLICT(idempotency_key) DO UPDATE SET
                    state=CASE
                        WHEN mutation_operations.state='succeeded'
                             AND mutation_operations.result_json IS NOT NULL
                        THEN mutation_operations.state
                        WHEN mutation_operations.state='pending'
                        THEN mutation_operations.state
                        ELSE 'committed'
                    END,
                    event_id=excluded.event_id,
                    event_hash=COALESCE(excluded.event_hash, event_hash),
                    commit_hash=excluded.commit_hash,
                    result_json=CASE
                        WHEN mutation_operations.state='succeeded'
                             AND mutation_operations.result_json IS NOT NULL
                        THEN mutation_operations.result_json
                        ELSE COALESCE(
                            excluded.result_json,
                            mutation_operations.result_json
                        )
                    END,
                    recovery_state=CASE
                        WHEN mutation_operations.state='succeeded'
                             AND mutation_operations.result_json IS NOT NULL
                        THEN mutation_operations.recovery_state
                        ELSE 'durable_append_complete'
                    END,
                    updated_at=excluded.updated_at
                """,
                (
                    event.idempotency_key,
                    event.source_operation,
                    event.request_fingerprint,
                    event.realm_id,
                    self._operation_owner,
                    event.id,
                    event_hash,
                    commit_hash,
                    result_json,
                    now,
                    now,
                ),
            )

    @serialized_mutation
    def complete_operation(
        self,
        idempotency_key: str,
        result: dict,
        *,
        recovery_state: str = "completed",
    ) -> None:
        with self._conn() as conn:
            updated = conn.execute(
                """
                UPDATE mutation_operations SET state='succeeded', result_json=?,
                    recovery_state=?, updated_at=? WHERE idempotency_key=?
                """,
                (
                    json.dumps(result, default=str),
                    recovery_state,
                    datetime.now(UTC).isoformat(),
                    idempotency_key,
                ),
            )
            if updated.rowcount != 1:
                raise MutationOperationFailed(
                    idempotency_key, "operation_receipt_missing"
                )

    @serialized_mutation
    def fail_operation(self, idempotency_key: str, error_code: str) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE mutation_operations SET state='failed', error_code=?,
                    recovery_state='safe_to_retry', updated_at=?
                WHERE idempotency_key=? AND state='pending'
                """,
                (error_code, datetime.now(UTC).isoformat(), idempotency_key),
            )

    def get_operation_outcome(
        self, idempotency_key: str, *, realm_id: str = "default"
    ) -> dict:
        """Read terminal receipts without joining the global mutation queue.

        Missing and non-terminal receipts can require event-log repair, so those
        cases are re-read and handled under the mutation lock.
        """
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        if row is not None:
            record = dict(row)
            if record["realm_id"] != realm_id:
                return self._operation_not_found(idempotency_key)
            if record["state"] == "succeeded" and record.get("result_json"):
                return self._operation_outcome(idempotency_key, record)

        with self._mutation_lock:
            return self._get_operation_outcome_repair(
                idempotency_key, realm_id=realm_id
            )

    def _get_operation_outcome_repair(
        self, idempotency_key: str, *, realm_id: str
    ) -> dict:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
        if row is None and self.event_log:
            found = self.event_log.find_operation_event(realm_id, idempotency_key)
            if found:
                commit_hash, event_hash, event = found
                self.mark_operation_durable(
                    event, commit_hash, event_hash=event_hash
                )
                with self._conn() as conn:
                    row = conn.execute(
                        "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                        (idempotency_key,),
                    ).fetchone()
        if row is None:
            return self._operation_not_found(idempotency_key)
        record = dict(row)
        if record["realm_id"] != realm_id:
            return self._operation_not_found(idempotency_key)
        if record["state"] in {"pending", "committed", "failed"}:
            recovered = self._recover_operation(record)
            if recovered is not None:
                with self._conn() as conn:
                    row = conn.execute(
                        "SELECT * FROM mutation_operations WHERE idempotency_key=?",
                        (idempotency_key,),
                    ).fetchone()
                record = dict(row)
        return self._operation_outcome(idempotency_key, record)

    @staticmethod
    def _operation_not_found(idempotency_key: str) -> dict:
        return {
            "idempotency_key": idempotency_key,
            "status": "not_found",
            "durable": False,
            "recovery_state": "safe_to_retry_with_same_key",
        }

    def _operation_outcome(self, idempotency_key: str, record: dict) -> dict:
        stale_pending = (
            record["state"] in {"pending", "failed"}
            and not record.get("commit_hash")
            and record["owner_token"] != self._operation_owner
        )
        durable_resumable = bool(record.get("commit_hash")) and (
            record["state"] == "committed"
            or (
                record["state"] in {"pending", "failed"}
                and record["owner_token"] != self._operation_owner
            )
        )
        result = (
            json.loads(record["result_json"])
            if record.get("result_json")
            else None
        )
        return {
            "idempotency_key": idempotency_key,
            "operation": record["operation"],
            "status": (
                "retryable"
                if stale_pending
                else "resumable"
                if durable_resumable
                else record["state"]
            ),
            "durable": bool(record.get("commit_hash")),
            "event_id": record.get("event_id"),
            "commit_hash": record.get("commit_hash"),
            "correlation_id": record.get("correlation_id"),
            "recovery_state": (
                "safe_to_retry_with_same_key"
                if stale_pending
                else "durable_append_resume_required"
                if durable_resumable
                else "in_progress"
                if record["state"] == "pending"
                else record["recovery_state"]
            ),
            "recovery_action": (
                "retry_same_operation_with_same_key"
                if stale_pending or durable_resumable
                else "get_operation_outcome"
                if record["state"] == "pending"
                else None
            ),
            "result": result,
            "error_code": record.get("error_code"),
        }

    @serialized_mutation
    def commit_event(self, event: CardEvent):
        """Append, project, and checkpoint one event as an ordered unit."""
        if not self.event_log:
            raise RuntimeError("Cannot commit an event without an event log")
        event, commit = self.event_log.append_event(
            event, on_commit=self._on_commit
        )
        self.mark_operation_durable(
            event, commit.hash, event_hash=commit.event_hashes[-1]
        )
        try:
            self.apply_event(event)
            self._record_projection_head(event.realm_id, commit.hash)
        except Exception:
            # A durable append is authoritative. Repair transient apply failures
            # immediately; a process crash is repaired by startup reconciliation.
            self.rebuild_from_log(event.realm_id)
            if self.get_projection_head(event.realm_id) != commit.hash:
                raise
        return commit

    def get_projection_head(self, realm_id: str) -> str | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT head_hash FROM sync_projection_heads WHERE realm_id = ?",
                (realm_id,),
            ).fetchone()
        return row["head_hash"] if row else None

    def _record_projection_head(
        self, realm_id: str, head_hash: str | None = None
    ) -> None:
        if not self.event_log:
            return
        head_hash = head_hash or self.event_log.get_head(realm_id)
        if not head_hash:
            return
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO sync_projection_heads (realm_id, head_hash, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(realm_id) DO UPDATE SET
                    head_hash = excluded.head_hash,
                    updated_at = excluded.updated_at
                """,
                (realm_id, head_hash, datetime.now(UTC).isoformat()),
            )

    def _record_fleet_policy_audit(
        self,
        event: CardEvent,
        *,
        entity_type: str,
        entity_id: str,
    ) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO fleet_policy_audit_events
                (id, realm_id, entity_type, entity_id, action, actor, payload,
                 created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.realm_id,
                    entity_type,
                    entity_id,
                    event.type.value,
                    event.author_principal,
                    json.dumps(
                        {
                            "author_instance": event.author_instance,
                            "event": event.payload,
                        }
                    ),
                    event.timestamp.isoformat(),
                ),
            )

    def _apply_instance_group_event(self, event: CardEvent) -> None:
        group_id = str(event.payload.get("id") or "")
        if not group_id:
            return
        if event.type == EventType.INSTANCE_GROUP_DELETED:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM instance_groups WHERE realm_id=? AND id=?",
                    (event.realm_id, group_id),
                )
            self._record_fleet_policy_audit(
                event, entity_type="instance_group", entity_id=group_id
            )
            return
        with self._conn() as conn:
            current = conn.execute(
                "SELECT payload FROM instance_groups WHERE realm_id=? AND id=?",
                (event.realm_id, group_id),
            ).fetchone()
            payload = json.loads(current["payload"]) if current else {}
            payload.update(event.payload)
            payload["realm_id"] = event.realm_id
            group = InstanceGroup.model_validate(payload)
            conn.execute(
                """
                INSERT INTO instance_groups (realm_id, id, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(realm_id, id) DO UPDATE SET payload=excluded.payload
                """,
                (
                    event.realm_id,
                    group.id,
                    json.dumps(group.model_dump(mode="json")),
                ),
            )
        self._record_fleet_policy_audit(
            event, entity_type="instance_group", entity_id=group_id
        )

    def _apply_instance_participation_policy_event(self, event: CardEvent) -> None:
        instance_id = str(event.payload.get("instance_id") or "")
        if not instance_id:
            return
        with self._conn() as conn:
            current = conn.execute(
                """
                SELECT payload FROM instance_participation_policies
                WHERE realm_id=? AND instance_id=?
                """,
                (event.realm_id, instance_id),
            ).fetchone()
            payload = json.loads(current["payload"]) if current else {}
            payload.update(event.payload)
            payload["realm_id"] = event.realm_id
            policy = InstanceParticipationPolicy.model_validate(payload)
            conn.execute(
                """
                INSERT INTO instance_participation_policies
                (realm_id, instance_id, payload) VALUES (?, ?, ?)
                ON CONFLICT(realm_id, instance_id)
                DO UPDATE SET payload=excluded.payload
                """,
                (
                    event.realm_id,
                    policy.instance_id,
                    json.dumps(policy.model_dump(mode="json")),
                ),
            )
        self._record_fleet_policy_audit(
            event,
            entity_type="instance_participation_policy",
            entity_id=instance_id,
        )

    def _apply_placement_default_event(self, event: CardEvent) -> None:
        raw_scope_key = event.payload.get("scope_key")
        scope_key = canonical_default_scope_key(
            event.payload.get("project_id"),
            event.payload.get("workload_profile"),
            raw_scope_key,
        )
        if event.type == EventType.PLACEMENT_DEFAULT_DELETED:
            with self._conn() as conn:
                conn.execute(
                    "DELETE FROM placement_defaults WHERE realm_id=? AND scope_key=?",
                    (event.realm_id, scope_key),
                )
            self._record_fleet_policy_audit(
                event, entity_type="placement_default", entity_id=scope_key
            )
            return
        with self._conn() as conn:
            current = conn.execute(
                "SELECT payload FROM placement_defaults WHERE realm_id=? AND scope_key=?",
                (event.realm_id, scope_key),
            ).fetchone()
            payload = json.loads(current["payload"]) if current else {}
            payload.update(event.payload)
            payload["realm_id"] = event.realm_id
            payload.pop("scope_key", None)
            if raw_scope_key and not event.payload.get("workload_profile"):
                parts = str(raw_scope_key).split(":", 3)
                if len(parts) == 4 and parts[0] == "project" and parts[2] == "profile":
                    payload["project_id"] = None if parts[1] == "*" else parts[1]
                    payload["workload_profile"] = (
                        None if parts[3] == "*" else parts[3]
                    )
            default = PlacementDefault.model_validate(payload)
            conn.execute(
                """
                INSERT INTO placement_defaults (realm_id, scope_key, payload)
                VALUES (?, ?, ?)
                ON CONFLICT(realm_id, scope_key)
                DO UPDATE SET payload=excluded.payload
                """,
                (
                    event.realm_id,
                    default.scope_key,
                    json.dumps(default.model_dump(mode="json")),
                ),
            )
        self._record_fleet_policy_audit(
            event, entity_type="placement_default", entity_id=scope_key
        )

    def _apply_repository_created(self, event: CardEvent) -> None:
        p = event.payload
        now = p.get("created_at") or event.timestamp.isoformat()
        url = p["url"].strip()
        remotes = p.get("remotes") or [
            remote.model_dump(mode="json")
            for remote in self._default_repository_remotes(url)
        ]
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO repositories (id, realm_id, url, name, remotes, default_branch, provider, provider_repository_id, provider_metadata, visibility, status, archived_at, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    p["id"],
                    event.realm_id,
                    url,
                    p.get("name", ""),
                    json.dumps(remotes),
                    p.get("default_branch"),
                    p.get("provider", ""),
                    p.get("provider_repository_id"),
                    json.dumps(p.get("provider_metadata") or {}),
                    p.get("visibility", RepositoryVisibility.REALM.value),
                    p.get("status", RepositoryStatus.ACTIVE.value),
                    p.get("archived_at"),
                    now,
                    p.get("updated_at", now),
                ),
            )

    def _apply_repository_updated(self, event: CardEvent) -> None:
        p = event.payload
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM repositories WHERE id=? AND realm_id=?",
                (p["id"], event.realm_id),
            ).fetchone()
            if row:
                conn.execute(
                    "UPDATE repositories SET name=?, remotes=?, default_branch=?, provider=?, provider_repository_id=?, provider_metadata=?, visibility=?, status=?, archived_at=?, updated_at=? WHERE id=?",
                    (
                        p.get("name", row["name"]),
                        json.dumps(p["remotes"]) if "remotes" in p else row["remotes"],
                        p.get("default_branch", row["default_branch"]),
                        p.get("provider", row["provider"]),
                        p.get("provider_repository_id", row["provider_repository_id"]),
                        json.dumps(p["provider_metadata"])
                        if "provider_metadata" in p
                        else row["provider_metadata"],
                        p.get("visibility", row["visibility"]),
                        p.get("status", row["status"]),
                        p.get("archived_at", row["archived_at"]),
                        event.timestamp.isoformat(),
                        p["id"],
                    ),
                )

    def _apply_repository_deleted(self, event: CardEvent) -> None:
        with self._conn() as conn:
            rid = event.payload["id"]
            project_ids = [
                row["project_id"]
                for row in conn.execute(
                    "SELECT project_id FROM project_repositories WHERE repository_id=?",
                    (rid,),
                ).fetchall()
            ]
            conn.execute(
                "DELETE FROM repository_checkouts WHERE repository_id=?", (rid,)
            )
            conn.execute(
                "DELETE FROM project_repositories WHERE repository_id=?", (rid,)
            )
            conn.execute(
                "DELETE FROM repositories WHERE id=? AND realm_id=?",
                (rid, event.realm_id),
            )
            for project_id in project_ids:
                conn.execute("UPDATE projects SET repos='[]' WHERE id=?", (project_id,))

    def _apply_project_repository_linked(self, event: CardEvent) -> None:
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO project_repositories (project_id, repository_id, branch) VALUES (?, ?, ?)",
                (
                    event.project_id,
                    event.payload["repository_id"],
                    event.payload.get("branch"),
                ),
            )

    def _apply_project_repository_unlinked(self, event: CardEvent) -> None:
        with self._conn() as conn:
            cur = conn.execute(
                "DELETE FROM project_repositories WHERE project_id=? AND repository_id=?",
                (event.project_id, event.payload["repository_id"]),
            )
            if cur.rowcount > 0:
                conn.execute(
                    "UPDATE projects SET repos='[]' WHERE id=?", (event.project_id,)
                )

    def _apply_repository_checkout_set(self, event: CardEvent) -> None:
        p = event.payload
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO repository_checkouts (repository_id, instance_id, path, branch) VALUES (?, ?, ?, ?)",
                (p["repository_id"], p["instance_id"], p["path"], p.get("branch")),
            )

    def _apply_repository_checkout_removed(self, event: CardEvent) -> None:
        p = event.payload
        with self._conn() as conn:
            conn.execute(
                "DELETE FROM repository_checkouts WHERE repository_id=? AND instance_id=?",
                (p["repository_id"], p["instance_id"]),
            )

    def _apply_created(self, event: CardEvent) -> None:
        if event.card_id:
            with self._conn() as conn:
                existing = conn.execute(
                    "SELECT 1 FROM cards WHERE id=? AND realm_id=?",
                    (event.card_id, event.realm_id),
                ).fetchone()
            if existing:
                return
        p = event.payload
        created_at = _coerce_datetime(p.get("created_at")) or datetime.now(UTC)
        updated_at = _coerce_datetime(p.get("updated_at")) or created_at
        summary = (p.get("summary") or "").strip()
        card = Card(
            id=p.get("id", event.card_id or str(uuid4())),
            realm_id=event.realm_id,
            kind=CardKind(p.get("kind", "task")),
            title=p.get("title", ""),
            body=p.get("body", ""),
            summary=summary,
            summary_source=CardSummarySource(
                p.get("summary_source", CardSummarySource.FALLBACK.value)
            ),
            summary_status=p.get("summary_status", "ready" if summary else "pending"),
            summary_provider=p.get("summary_provider"),
            summary_model=p.get("summary_model"),
            summary_auth_source=p.get("summary_auth_source"),
            summary_prompt_version=p.get("summary_prompt_version"),
            summary_input_hash=p.get("summary_input_hash"),
            summary_failure=p.get("summary_failure"),
            summary_failure_code=p.get("summary_failure_code"),
            summary_attempt_count=int(p.get("summary_attempt_count") or 0),
            summary_next_attempt_at=_coerce_datetime(p.get("summary_next_attempt_at")),
            summary_last_attempted_at=_coerce_datetime(
                p.get("summary_last_attempted_at")
            ),
            summary_authority_instance_id=p.get("summary_authority_instance_id"),
            summary_updated_at=(
                _coerce_datetime(p.get("summary_updated_at"))
                or (updated_at if summary else None)
            ),
            summary_stale=bool(p.get("summary_stale", False)),
            lane=CardLane(
                p.get("lane") or lane_from_legacy_status(p.get("status", "open")).value
            ),
            parent_id=p.get("parent_id"),
            project_id=p.get("project_id"),
            tags=p.get("tags", []),
            attachments=[
                CardAttachment.model_validate(value)
                for value in p.get("attachments", [])
            ],
            preferred_instance=p.get("preferred_instance"),
            preferred_capabilities=p.get("preferred_capabilities", []),
            lease_holder_instance=p.get("lease_holder_instance"),
            lease_holder_principal=p.get("lease_holder_principal"),
            lease_expires_at=_coerce_datetime(p.get("lease_expires_at")),
            created_by_principal=p.get("created_by_principal")
            or event.author_principal,
            created_by_instance=p.get("created_by_instance") or event.author_instance,
            created_at=created_at,
            updated_at=updated_at,
        )
        self._upsert_card(card)

    def _apply_upserted(self, event: CardEvent) -> None:
        payload = {**event.payload, "id": event.card_id, "realm_id": event.realm_id}
        try:
            card = Card.model_validate(payload)
        except ValidationError:
            return
        self._upsert_card(card)

    def _apply_project_created(self, event: CardEvent) -> None:
        p = event.payload
        project = Project(
            id=p.get("id", event.project_id or str(uuid4())),
            realm_id=event.realm_id,
            title=p.get("title", ""),
            description=p.get("description", ""),
            status=ProjectStatus(p.get("status", "active")),
            memberships=[
                ProjectMembership.model_validate(m) for m in p.get("memberships", [])
            ],
            repos=[ProjectRepo.model_validate(r) for r in p.get("repos", [])],
            agent_prompt=p.get("agent_prompt", ""),
            tool_config=p.get("tool_config", {}),
            tags=p.get("tags", []),
            created_by_principal=event.author_principal,
        )
        self._upsert_project(project)
        with self._conn() as conn:
            self._replace_project_repositories_conn(
                conn,
                project.id,
                project.realm_id,
                p.get("repos", []),
                event.author_instance,
            )

    def _apply_project_updated(self, event: CardEvent) -> None:
        if not event.project_id:
            return
        project = self.get_project(event.project_id, realm_id=event.realm_id)
        if not project:
            return
        for key, value in event.payload.items():
            if key == "status" and value is not None:
                project.status = ProjectStatus(value)
            elif key == "memberships" and value is not None:
                project.memberships = [
                    ProjectMembership.model_validate(m) for m in value
                ]
            elif key == "repos":
                continue
            elif hasattr(project, key):
                setattr(project, key, value)
        project.updated_at = datetime.now(UTC)
        self._upsert_project(project)
        if "repos" in event.payload:
            repos = event.payload.get("repos") or []
            with self._conn() as conn:
                has_normalized = conn.execute(
                    "SELECT 1 FROM project_repositories WHERE project_id=? LIMIT 1",
                    (project.id,),
                ).fetchone()
                if not repos:
                    conn.execute(
                        "DELETE FROM project_repositories WHERE project_id = ?",
                        (project.id,),
                    )
                    project.repos = []
                    conn.execute(
                        "UPDATE projects SET repos='[]' WHERE id=?", (project.id,)
                    )
                elif has_normalized:
                    conn.execute(
                        "UPDATE projects SET repos='[]' WHERE id=?", (project.id,)
                    )
                else:
                    project.repos = [ProjectRepo.model_validate(r) for r in repos]
                    self._replace_project_repositories_conn(
                        conn,
                        project.id,
                        project.realm_id,
                        repos,
                        event.author_instance,
                    )

    def _apply_project_archived(self, event: CardEvent) -> None:
        if not event.project_id:
            return
        project = self.get_project(event.project_id, realm_id=event.realm_id)
        if not project:
            return
        project.status = ProjectStatus.ARCHIVED
        project.updated_at = datetime.now(UTC)
        self._upsert_project(project)

    def _apply_updated(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        payload = dict(event.payload)
        # Histories written before cards became canonical used item ``status``.
        # Translate during projection without rewriting the durable event.
        if "lane" not in payload and "status" in payload:
            payload["lane"] = lane_from_legacy_status(payload["status"]).value
        if ({"title", "body"} & payload.keys()) and "summary" not in payload:
            card.summary_stale = True
            card.summary_status = CardSummaryStatus.STALE
            card.summary_input_hash = None
            card.summary_failure = None
            card.summary_failure_code = None
            card.summary_attempt_count = 0
            card.summary_next_attempt_at = None
            card.summary_last_attempted_at = None
            card.summary_authority_instance_id = None
        for key, value in payload.items():
            if key in {
                "created_at",
                "updated_at",
                "lease_expires_at",
                "summary_updated_at",
                "summary_next_attempt_at",
                "summary_last_attempted_at",
            }:
                continue
            if key == "kind":
                card.kind = CardKind(value)
            elif key == "lane":
                card.lane = CardLane(value)
            elif key == "summary_source":
                card.summary_source = CardSummarySource(value)
            elif key == "summary_status":
                card.summary_status = CardSummaryStatus(value)
            elif hasattr(card, key):
                setattr(card, key, value)
        if "lease_expires_at" in payload:
            card.lease_expires_at = _coerce_datetime(payload.get("lease_expires_at"))
        if "summary_updated_at" in payload:
            card.summary_updated_at = _coerce_datetime(
                payload.get("summary_updated_at")
            )
        if "summary_next_attempt_at" in payload:
            card.summary_next_attempt_at = _coerce_datetime(
                payload.get("summary_next_attempt_at")
            )
        if "summary_last_attempted_at" in payload:
            card.summary_last_attempted_at = _coerce_datetime(
                payload.get("summary_last_attempted_at")
            )
        # Prefer the authority stamp carried in the event so synced peers keep
        # an identical card_version for fleet dispatch materialization.
        card.updated_at = _coerce_datetime(payload.get("updated_at")) or datetime.now(
            UTC
        )
        self._upsert_card(card)

    def _apply_deleted(self, event: CardEvent) -> None:
        if event.card_id:
            self._delete_card_projection(event.card_id, realm_id=event.realm_id)

    def _apply_attachment_created(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        attachment = CardAttachment.model_validate(event.payload)
        existing = {item.attachment_id: item for item in card.attachments}
        existing[attachment.attachment_id] = attachment
        card.attachments = list(existing.values())
        card.updated_at = _coerce_datetime(
            event.payload.get("card_updated_at")
        ) or datetime.now(UTC)
        self._upsert_card(card)

    def _apply_attachment_removed(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        attachment_id = str(event.payload.get("attachment_id") or "")
        card.attachments = [
            item for item in card.attachments if item.attachment_id != attachment_id
        ]
        card.updated_at = _coerce_datetime(
            event.payload.get("card_updated_at")
        ) or datetime.now(UTC)
        self._upsert_card(card)

    def _delete_card_projection(
        self, card_id: str, *, realm_id: str | None = None
    ) -> bool:
        query = "DELETE FROM cards WHERE id = ?"
        params: list[str] = [card_id]
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        with self._conn() as conn:
            cur = conn.execute(query, params)
        return cur.rowcount > 0

    def _apply_lease(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        card.lease_holder_instance = event.payload.get("holder_instance")
        card.lease_holder_principal = event.payload.get("holder_principal")
        exp = event.payload.get("expires_at")
        card.lease_expires_at = (
            datetime.fromisoformat(exp) if isinstance(exp, str) and exp else None
        )
        card.updated_at = _coerce_datetime(
            event.payload.get("updated_at")
        ) or datetime.now(UTC)
        self._upsert_card(card)

    def _apply_lease_release(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        card.lease_holder_instance = None
        card.lease_holder_principal = None
        card.lease_expires_at = None
        card.updated_at = _coerce_datetime(
            event.payload.get("updated_at")
        ) or datetime.now(UTC)
        self._upsert_card(card)

    def _upsert_card(self, card: Card) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cards
                (id, realm_id, kind, title, body, summary, summary_source,
                 summary_updated_at, summary_stale, summary_status, summary_provider,
                 summary_model, summary_auth_source, summary_prompt_version,
                 summary_input_hash, summary_failure, summary_failure_code,
                 summary_attempt_count, summary_next_attempt_at,
                 summary_last_attempted_at, summary_authority_instance_id,
                 lane, parent_id, project_id, tags, attachments, visibility,
                 owner_principal, preferred_instance, preferred_capabilities,
                 lease_holder_instance, lease_holder_principal, lease_expires_at,
                 created_by_principal, created_by_instance, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card.id,
                    card.realm_id,
                    card.kind.value,
                    card.title,
                    card.body,
                    card.summary,
                    card.summary_source.value,
                    card.summary_updated_at.isoformat()
                    if card.summary_updated_at
                    else None,
                    int(card.summary_stale),
                    card.summary_status.value,
                    card.summary_provider,
                    card.summary_model,
                    card.summary_auth_source,
                    card.summary_prompt_version,
                    card.summary_input_hash,
                    card.summary_failure,
                    card.summary_failure_code,
                    card.summary_attempt_count,
                    card.summary_next_attempt_at.isoformat()
                    if card.summary_next_attempt_at
                    else None,
                    card.summary_last_attempted_at.isoformat()
                    if card.summary_last_attempted_at
                    else None,
                    card.summary_authority_instance_id,
                    card.lane.value,
                    card.parent_id,
                    card.project_id,
                    json.dumps(card.tags),
                    json.dumps(
                        [item.model_dump(mode="json") for item in card.attachments]
                    ),
                    card.visibility,
                    card.owner_principal,
                    card.preferred_instance,
                    json.dumps(card.preferred_capabilities),
                    card.lease_holder_instance,
                    card.lease_holder_principal,
                    card.lease_expires_at.isoformat()
                    if card.lease_expires_at
                    else None,
                    card.created_by_principal,
                    card.created_by_instance,
                    card.created_at.isoformat(),
                    card.updated_at.isoformat(),
                ),
            )

    @serialized_mutation
    def create_card(
        self,
        data: CardCreate,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
        via_log: bool = True,
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> Card:
        now = datetime.now(UTC)
        supplied_summary = data.summary.strip()
        card = Card(
            realm_id=data.realm_id,
            kind=data.kind,
            title=data.title,
            body=data.body,
            summary=supplied_summary,
            summary_source=(
                data.summary_source or CardSummarySource.MANUAL
                if supplied_summary
                else CardSummarySource.FALLBACK
            ),
            summary_status="ready" if supplied_summary else "pending",
            summary_updated_at=now if supplied_summary else None,
            lane=data.lane,
            parent_id=data.parent_id,
            project_id=data.project_id,
            tags=data.tags,
            preferred_instance=data.preferred_instance,
            preferred_capabilities=data.preferred_capabilities,
            created_by_principal=principal_id,
            created_by_instance=instance_id,
        )
        if via_log and self.event_log:
            event = CardEvent(
                type=EventType.CARD_CREATED,
                realm_id=card.realm_id,
                card_id=card.id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=card.model_dump(mode="json"),
                source_operation="card.create",
                field_intent=sorted(card.model_dump(mode="json")),
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                operation_result=card.model_dump(mode="json"),
            )
            self.commit_event(event)
        else:
            self._upsert_card(card)
        return card

    @serialized_mutation
    def add_attachment(
        self, attachment: CardAttachment, *, principal_id: str, instance_id: str
    ) -> Card:
        card = self.get_card(attachment.card_id, realm_id=attachment.realm_id)
        if not card:
            raise ValueError("Card not found")
        if len(card.attachments) >= 10:
            raise ValueError("A card can have at most 10 attachments")
        if (
            sum(item.size for item in card.attachments) + attachment.size
            > 100 * 1024 * 1024
        ):
            raise ValueError("Card attachments exceed the 100 MB total limit")
        payload = attachment.model_dump(mode="json")
        payload["card_updated_at"] = datetime.now(UTC).isoformat()
        event = CardEvent(
            type=EventType.ATTACHMENT_CREATED,
            realm_id=attachment.realm_id,
            card_id=attachment.card_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=payload,
        )
        self.commit_event(event)
        return self.get_card(attachment.card_id, realm_id=attachment.realm_id)

    @serialized_mutation
    def remove_attachment(
        self,
        card_id: str,
        attachment_id: str,
        *,
        realm_id: str,
        principal_id: str,
        instance_id: str,
    ) -> Card | None:
        card = self.get_card(card_id, realm_id=realm_id)
        if not card or not any(
            item.attachment_id == attachment_id for item in card.attachments
        ):
            return None
        event = CardEvent(
            type=EventType.ATTACHMENT_REMOVED,
            realm_id=realm_id,
            card_id=card_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload={
                "attachment_id": attachment_id,
                "card_updated_at": datetime.now(UTC).isoformat(),
            },
        )
        self.commit_event(event)
        return self.get_card(card_id, realm_id=realm_id)

    def _on_commit(self, commit) -> None:
        pass  # wired by Store

    def list_cards(
        self,
        realm_id: str | None = None,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
        project_id: str | None = None,
        parent_id: str | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Card]:
        query = "SELECT * FROM cards WHERE 1=1"
        params: list[str] = []
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        if lane:
            query += " AND lane = ?"
            params.append(lane.value)
        if kind:
            query += " AND kind = ?"
            params.append(kind.value)
        if project_id:
            query += " AND project_id = ?"
            params.append(project_id)
        if parent_id:
            query += " AND parent_id = ?"
            params.append(parent_id)
        query += " ORDER BY updated_at DESC"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([str(max(0, limit)), str(max(0, offset))])
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_card(row) for row in rows]

    def list_card_lanes(self, realm_id: str | None = None) -> dict[str, str]:
        """Return id → lane without materializing card bodies."""
        query = "SELECT id, lane FROM cards"
        params: list[str] = []
        if realm_id:
            query += " WHERE realm_id = ?"
            params.append(realm_id)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return {row["id"]: row["lane"] for row in rows}

    def find_card_attachment(
        self, attachment_id: str, filename: str
    ) -> tuple[Card, CardAttachment] | None:
        """Locate one attachment manifest without scanning every card."""
        sql = """
            SELECT cards.*
            FROM cards, json_each(cards.attachments) AS attachment
            WHERE json_extract(attachment.value, '$.attachment_id') = ?
              AND json_extract(attachment.value, '$.filename') = ?
            LIMIT 1
        """
        try:
            with self._conn() as conn:
                row = conn.execute(sql, (attachment_id, filename)).fetchone()
        except sqlite3.OperationalError:
            return None
        if row is None:
            return None
        card = self._row_to_card(row)
        item = next(
            (
                candidate
                for candidate in card.attachments
                if candidate.attachment_id == attachment_id
                and candidate.filename == filename
            ),
            None,
        )
        if item is None:
            return None
        return card, item

    def count_cards(
        self, *, realm_id: str | None = None, lane: CardLane | None = None
    ) -> int:
        query = "SELECT COUNT(*) AS count FROM cards WHERE 1=1"
        params: list[str] = []
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        if lane:
            query += " AND lane = ?"
            params.append(lane.value)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return int(row["count"] if row else 0)

    def _card_work_clauses(
        self,
        *,
        realm_id: str,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
        project_id: str | None = None,
        query: str = "",
        owner: str = "",
        instance: str = "",
        blocked: str = "",
        tags: list[str] | None = None,
        tag_mode: str = "and",
        updated_days: int | None = None,
    ) -> tuple[str, list[object]]:
        clauses = ["realm_id = ?"]
        params: list[object] = [realm_id]
        if lane:
            clauses.append("lane = ?")
            params.append(lane.value)
        if kind:
            clauses.append("kind = ?")
            params.append(kind.value)
        if project_id:
            clauses.append("project_id = ?")
            params.append(project_id)
        if query:
            clauses.append("LOWER(title || ' ' || summary || ' ' || body) LIKE ?")
            params.append(f"%{query.lower()}%")
        if owner:
            clauses.append("owner_principal = ?")
            params.append(owner)
        if instance:
            clauses.append("preferred_instance = ?")
            params.append(instance)
        if blocked == "blocked":
            clauses.append("lane = 'waiting'")
        elif blocked == "unblocked":
            clauses.append("lane != 'waiting'")
        selected_tags = list(dict.fromkeys(tags or []))
        if selected_tags:
            if tag_mode == "or":
                placeholders = ",".join("?" for _ in selected_tags)
                clauses.append(
                    "EXISTS (SELECT 1 FROM json_each(cards.tags) "
                    f"WHERE value IN ({placeholders}))"
                )
                params.extend(selected_tags)
            else:
                for selected_tag in selected_tags:
                    clauses.append(
                        "EXISTS (SELECT 1 FROM json_each(cards.tags) WHERE value = ?)"
                    )
                    params.append(selected_tag)
        if updated_days is not None:
            cutoff = datetime.now(UTC) - timedelta(days=updated_days)
            clauses.append("updated_at >= ?")
            params.append(cutoff.isoformat())
        return " AND ".join(clauses), params

    def list_card_work_projections(
        self,
        *,
        realm_id: str,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
        project_id: str | None = None,
        query: str = "",
        owner: str = "",
        instance: str = "",
        blocked: str = "",
        tag: str = "",
        tags: list[str] | None = None,
        tag_mode: str = "and",
        updated_days: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Card]:
        """Return a fixed-size, body-free page for lifecycle presentation."""
        where, params = self._card_work_clauses(
            realm_id=realm_id,
            lane=lane,
            kind=kind,
            project_id=project_id,
            query=query,
            owner=owner,
            instance=instance,
            blocked=blocked,
            tags=tags if tags is not None else ([tag] if tag else []),
            tag_mode=tag_mode,
            updated_days=updated_days,
        )
        bounded_limit = max(1, min(int(limit), 100))
        params.extend([bounded_limit, max(0, int(offset))])
        columns = """
            id, realm_id, kind, title, '' AS body,
            summary, summary_source, summary_status,
            summary_updated_at, summary_stale, lane, parent_id, project_id, tags,
            visibility, owner_principal, preferred_instance,
            preferred_capabilities, lease_holder_instance,
            lease_holder_principal, lease_expires_at,
            created_by_principal, created_by_instance,
            created_at, updated_at
        """
        sql = (
            f"SELECT {columns} FROM cards WHERE {where} "
            "ORDER BY updated_at DESC, id LIMIT ? OFFSET ?"
        )
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_card(row) for row in rows]

    def count_card_work_projections(
        self,
        *,
        realm_id: str,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
        project_id: str | None = None,
        query: str = "",
        owner: str = "",
        instance: str = "",
        blocked: str = "",
        tag: str = "",
        tags: list[str] | None = None,
        tag_mode: str = "and",
        updated_days: int | None = None,
    ) -> int:
        where, params = self._card_work_clauses(
            realm_id=realm_id,
            lane=lane,
            kind=kind,
            project_id=project_id,
            query=query,
            owner=owner,
            instance=instance,
            blocked=blocked,
            tags=tags if tags is not None else ([tag] if tag else []),
            tag_mode=tag_mode,
            updated_days=updated_days,
        )
        with self._conn() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS count FROM cards WHERE {where}", params
            ).fetchone()
        return int(row["count"] if row else 0)

    def list_card_filter_facets(self, *, realm_id: str) -> dict[str, list[str]]:
        """Distinct filter values without loading card bodies."""
        with self._conn() as conn:
            owners = [
                str(row["owner_principal"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT owner_principal FROM cards
                    WHERE realm_id = ? AND owner_principal IS NOT NULL
                      AND owner_principal != ''
                    ORDER BY owner_principal
                    """,
                    (realm_id,),
                )
            ]
            instances = [
                str(row["preferred_instance"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT preferred_instance FROM cards
                    WHERE realm_id = ? AND preferred_instance IS NOT NULL
                      AND preferred_instance != ''
                    ORDER BY preferred_instance
                    """,
                    (realm_id,),
                )
            ]
            tags = [
                str(row["value"])
                for row in conn.execute(
                    """
                    SELECT DISTINCT value FROM cards, json_each(cards.tags)
                    WHERE realm_id = ? AND value IS NOT NULL AND value != ''
                    ORDER BY value
                    """,
                    (realm_id,),
                )
            ]
        return {"owners": owners, "instances": instances, "tags": tags}

    def search_card_filter_facet(
        self, *, realm_id: str, facet: str, query: str = "", limit: int = 20
    ) -> list[dict[str, object]]:
        """Return a bounded, count-bearing facet page ranked for typeahead use."""
        bounded_limit = max(1, min(int(limit), 50))
        needle = query.strip().casefold()
        with self._conn() as conn:
            if facet == "tag":
                rows = conn.execute(
                    """
                    SELECT CAST(j.value AS TEXT) AS value, COUNT(*) AS count
                    FROM cards c, json_each(c.tags) j
                    WHERE c.realm_id = ? AND j.value IS NOT NULL AND j.value != ''
                      AND (? = '' OR LOWER(CAST(j.value AS TEXT)) LIKE ?)
                    GROUP BY j.value
                    ORDER BY CASE
                      WHEN LOWER(CAST(j.value AS TEXT)) = ? THEN 0
                      WHEN LOWER(CAST(j.value AS TEXT)) LIKE ? THEN 1 ELSE 2 END,
                      count DESC, LOWER(CAST(j.value AS TEXT)), CAST(j.value AS TEXT)
                    LIMIT ?
                    """,
                    (realm_id, needle, f"%{needle}%", needle, f"{needle}%", bounded_limit),
                ).fetchall()
            else:
                column = {"owner": "owner_principal", "instance": "preferred_instance"}.get(facet)
                if column is None:
                    raise ValueError("unsupported facet")
                rows = conn.execute(
                    f"""SELECT {column} AS value, COUNT(*) AS count FROM cards
                    WHERE realm_id = ? AND {column} IS NOT NULL AND {column} != ''
                      AND (? = '' OR LOWER({column}) LIKE ?)
                    GROUP BY {column}
                    ORDER BY CASE WHEN LOWER({column}) = ? THEN 0
                      WHEN LOWER({column}) LIKE ? THEN 1 ELSE 2 END,
                      count DESC, LOWER({column}), {column} LIMIT ?""",
                    (realm_id, needle, f"%{needle}%", needle, f"{needle}%", bounded_limit),
                ).fetchall()
        return [{"value": str(row["value"]), "count": int(row["count"])} for row in rows]

    def list_work_saved_views(self, *, realm_id: str, principal_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM work_saved_views WHERE realm_id = ? AND principal_id = ? ORDER BY LOWER(name), id",
                (realm_id, principal_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def save_work_view(
        self, *, view_id: str, realm_id: str, principal_id: str, name: str, query: str
    ) -> dict:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM work_saved_views WHERE realm_id=? AND principal_id=? AND name=?",
                (realm_id, principal_id, name),
            ).fetchone()
            if existing:
                view_id = str(existing["id"])
                version = int(existing["version"]) + 1
                conn.execute(
                    "UPDATE work_saved_views SET query=?, version=?, updated_at=? WHERE id=?",
                    (query, version, now, view_id),
                )
                action = "updated"
            else:
                version = 1
                conn.execute(
                    "INSERT INTO work_saved_views VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (view_id, realm_id, principal_id, name, query, version, now, now),
                )
                action = "created"
            conn.execute(
                "INSERT INTO work_saved_view_audit VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uuid4().hex, view_id, principal_id, action, version, query, now),
            )
            row = conn.execute("SELECT * FROM work_saved_views WHERE id=?", (view_id,)).fetchone()
        return dict(row)

    def delete_work_view(self, *, view_id: str, realm_id: str, principal_id: str) -> bool:
        now = datetime.now(UTC).isoformat()
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM work_saved_views WHERE id=? AND realm_id=? AND principal_id=?",
                (view_id, realm_id, principal_id),
            ).fetchone()
            if not row:
                return False
            conn.execute("DELETE FROM work_saved_views WHERE id=?", (view_id,))
            conn.execute(
                "INSERT INTO work_saved_view_audit VALUES (?, ?, ?, ?, ?, ?, ?)",
                (uuid4().hex, view_id, principal_id, "deleted", int(row["version"]), str(row["query"]), now),
            )
        return True

    def list_cards_by_ids(
        self, card_ids: list[str], *, realm_id: str
    ) -> list[Card]:
        """Hydrate only one already-paginated card-id page."""
        bounded_ids = list(dict.fromkeys(card_ids))[:100]
        if not bounded_ids:
            return []
        placeholders = ",".join("?" for _ in bounded_ids)
        with self._conn() as conn:
            rows = conn.execute(
                f"SELECT * FROM cards WHERE realm_id = ? AND id IN ({placeholders})",
                [realm_id, *bounded_ids],
            ).fetchall()
        return [self._row_to_card(row) for row in rows]

    def get_card(self, card_id: str, realm_id: str | None = None) -> Card | None:
        query = "SELECT * FROM cards WHERE id = ?"
        params: list[str] = [card_id]
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_card(row) if row else None

    def latest_summary_failure(self) -> Card | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM cards
                WHERE summary_failure_code IS NOT NULL
                  AND summary_last_attempted_at IS NOT NULL
                ORDER BY summary_last_attempted_at DESC
                LIMIT 1
                """
            ).fetchone()
        return self._row_to_card(row) if row else None

    def list_summary_worker_candidates(
        self,
        *,
        now: datetime,
        max_attempts: int,
        limit: int,
        legacy_only: bool = False,
        include_disabled: bool = False,
    ) -> list[Card]:
        """Return a bounded SQL projection of due retry and fallback work."""
        if limit <= 0:
            return []
        retryable_source = " AND summary_source = 'fallback'" if legacy_only else ""
        disabled_clause = ""
        if include_disabled:
            disabled_clause = " OR summary_status = 'disabled'"
            if legacy_only:
                disabled_clause = (
                    " OR (summary_status = 'disabled' AND summary_source = 'fallback')"
                )
        query = f"""
            SELECT * FROM cards
            WHERE summary_source != 'manual'
              AND COALESCE(summary_attempt_count, 0) < ?
              AND (summary_next_attempt_at IS NULL OR summary_next_attempt_at <= ?)
              AND (
                (summary_status IN ('pending', 'stale'){retryable_source})
                OR (
                  summary_source = 'fallback'
                  AND (
                    summary_status = 'ready'
                    OR (
                      summary_status = 'failed'
                      AND (
                        summary_failure_code IS NULL
                        OR summary_failure_code = 'unconfigured'
                      )
                    )
                  )
                )
                {disabled_clause}
              )
            ORDER BY
              CASE WHEN summary_next_attempt_at IS NULL THEN 0 ELSE 1 END,
              summary_next_attempt_at ASC,
              updated_at ASC,
              id ASC
            LIMIT ?
        """
        with self._conn() as conn:
            rows = conn.execute(
                query,
                (max(0, max_attempts), now.isoformat(), max(0, limit)),
            ).fetchall()
        return [self._row_to_card(row) for row in rows]

    def list_summary_migration_page(
        self,
        *,
        limit: int,
        cursor: tuple[datetime, str] | None = None,
    ) -> list[Card]:
        """Page non-manual summaries for bounded legacy-prefix detection."""
        if limit <= 0:
            return []
        query = """
            SELECT * FROM cards
            WHERE summary_source != 'manual' AND summary != ''
        """
        params: list[object] = []
        if cursor:
            updated_at, card_id = cursor
            query += """
              AND (updated_at < ? OR (updated_at = ? AND id < ?))
            """
            stamp = updated_at.isoformat()
            params.extend((stamp, stamp, card_id))
        query += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(max(0, limit))
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_card(row) for row in rows]

    def get_notification(
        self, notification_id: str, *, realm_id: str | None = None
    ) -> Notification | None:
        query = "SELECT payload FROM notifications WHERE id=?"
        params: list[object] = [notification_id]
        if realm_id:
            query += " AND realm_id=?"
            params.append(realm_id)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return Notification.model_validate_json(row["payload"]) if row else None

    def find_notification_by_dedup(
        self, realm_id: str, deduplication_key: str
    ) -> Notification | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT payload FROM notifications
                WHERE realm_id=? AND deduplication_key=?
                """,
                (realm_id, deduplication_key),
            ).fetchone()
        return Notification.model_validate_json(row["payload"]) if row else None

    def list_notifications(
        self,
        *,
        realm_id: str,
        principal_id: str | None = None,
        notification_type: str | None = None,
        priority: str | None = None,
        unread: bool | None = None,
        outstanding: bool | None = None,
        resolved: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> list[Notification]:
        query = "SELECT payload FROM notifications WHERE realm_id=?"
        params: list[object] = [realm_id]
        if principal_id is not None:
            query += " AND (visibility != 'principal' OR principal_id=?)"
            params.append(principal_id)
        if notification_type:
            query += " AND type=?"
            params.append(notification_type)
        if priority:
            query += " AND priority=?"
            params.append(priority)
        if resolved is not None:
            query += " AND resolved=?"
            params.append(int(resolved))
        if unread is not None:
            query += " AND unread=?"
            params.append(int(unread))
        if outstanding is not None:
            query += " AND outstanding=?"
            params.append(int(outstanding))
        query += " ORDER BY updated_at DESC, id LIMIT ? OFFSET ?"
        params.extend([max(1, min(limit, 200)), max(0, offset)])
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [Notification.model_validate_json(row["payload"]) for row in rows]

    def count_outstanding_notifications(
        self, *, realm_id: str, principal_id: str | None = None
    ) -> int:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) AS total
                FROM notifications
                WHERE realm_id=? AND outstanding=1
                  AND (visibility != 'principal' OR principal_id=?)
                """,
                (realm_id, principal_id),
            ).fetchone()
        return int(row["total"] if row else 0)

    @serialized_mutation
    def save_notification(
        self,
        notification: Notification,
        *,
        principal_id: str,
        instance_id: str,
    ) -> Notification:
        if (
            notification.interaction
            and notification.interaction.state.value == "answered"
            and not notification.interaction.response_principal
        ):
            notification.interaction.response_principal = principal_id
        event = CardEvent(
            type=EventType.NOTIFICATION_UPSERTED,
            realm_id=notification.realm_id,
            card_id=None,
            project_id=None,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=notification.model_dump(mode="json"),
        )
        if self.event_log:
            self.commit_event(event)
        else:
            self.apply_event(event)
        return notification

    @serialized_mutation
    def delete_notification(
        self,
        notification_id: str,
        *,
        realm_id: str,
        principal_id: str,
        instance_id: str,
    ) -> bool:
        current = self.get_notification(notification_id, realm_id=realm_id)
        if not current:
            return False
        event = CardEvent(
            type=EventType.NOTIFICATION_DELETED,
            realm_id=realm_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload={"id": notification_id, "version": current.version + 1},
        )
        if self.event_log:
            self.commit_event(event)
        else:
            self.apply_event(event)
        return True

    def list_notification_audit(
        self, notification_id: str, *, limit: int = 100
    ) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM notification_audit_events
                WHERE notification_id=? ORDER BY created_at DESC LIMIT ?
                """,
                (notification_id, max(1, min(limit, 500))),
            ).fetchall()
        return [dict(row) for row in rows]

    @serialized_mutation
    def update_card(
        self,
        card_id: str,
        data: CardUpdate,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
        idempotency_key: str | None = None,
        request_fingerprint: str | None = None,
    ) -> Card | None:
        card = self.get_card(card_id, realm_id=realm_id)
        if not card:
            return None
        if data.expected_version is not None:
            expected = data.expected_version
            if expected.tzinfo is None:
                expected = expected.replace(tzinfo=UTC)
            if expected.astimezone(UTC) != card.updated_at.astimezone(UTC):
                raise CardVersionConflict(card_id, expected, card.updated_at)
        updates = data.model_dump(
            exclude_unset=True, exclude={"expected_version", "field_intent"}
        )
        requested_fields = set(updates)
        if data.field_intent is not None:
            requested_fields = set(data.field_intent)
            updates = {
                key: value for key, value in updates.items() if key in requested_fields
            }
        now = datetime.now(UTC)
        payload = {}
        nullable_summary_fields = {
            "summary_failure",
            "summary_failure_code",
            "summary_input_hash",
            "summary_next_attempt_at",
            "summary_last_attempted_at",
            "summary_auth_source",
            "summary_authority_instance_id",
        }
        for key, value in updates.items():
            if key in {"kind", "lane"} and value is not None:
                payload[key] = value.value if hasattr(value, "value") else value
            elif key == "summary_source" and value is not None:
                payload["summary_source"] = (
                    value.value if hasattr(value, "value") else value
                )
            elif key in {"summary_next_attempt_at", "summary_last_attempted_at"}:
                payload[key] = value.isoformat() if value is not None else None
            elif value is not None or key in {"project_id", *nullable_summary_fields}:
                payload[key] = value
        if ({"title", "body"} & updates.keys()) and "summary" not in updates:
            payload.update(
                summary_stale=True,
                summary_status="stale",
                summary_input_hash=None,
                summary_failure=None,
                summary_failure_code=None,
                summary_attempt_count=0,
                summary_next_attempt_at=None,
                summary_last_attempted_at=None,
                summary_authority_instance_id=None,
            )
        if updates.get("summary") is not None:
            supplied_summary = updates["summary"].strip()
            payload.update(
                summary=supplied_summary,
                summary_source=(
                    payload.get("summary_source") or CardSummarySource.MANUAL.value
                    if supplied_summary
                    else CardSummarySource.FALLBACK.value
                ),
                summary_status="ready" if supplied_summary else "pending",
                summary_stale=updates.get("summary_stale", False),
                summary_updated_at=now.isoformat(),
                summary_failure=None,
                summary_failure_code=None,
                summary_attempt_count=0,
                summary_next_attempt_at=None,
                summary_last_attempted_at=None,
            )
        if self.event_log and payload:
            # Stamp the authority version into the durable event so every peer
            # projects the same updated_at used for dispatch card_version checks.
            payload["updated_at"] = now.isoformat()
            event = CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id=realm_id,
                card_id=card_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=payload,
                source_operation="card.update",
                causal_card_version=card.updated_at.isoformat(),
                field_intent=sorted(requested_fields),
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
            )
            self.commit_event(event)
            return self.get_card(card_id, realm_id=realm_id)
        for key, value in payload.items():
            if key == "summary_updated_at":
                card.summary_updated_at = _coerce_datetime(value)
            elif key == "summary_next_attempt_at":
                card.summary_next_attempt_at = _coerce_datetime(value)
            elif key == "summary_last_attempted_at":
                card.summary_last_attempted_at = _coerce_datetime(value)
            elif key == "summary_source":
                card.summary_source = CardSummarySource(value)
            elif key == "summary_status":
                card.summary_status = CardSummaryStatus(value)
            elif (
                key != "updated_at"
                and (
                    value is not None or key in {"project_id", *nullable_summary_fields}
                )
                and hasattr(card, key)
            ):
                setattr(card, key, value)
        card.updated_at = now
        self._upsert_card(card)
        return card

    @serialized_mutation
    def repair_legacy_card_history(
        self,
        card_ids: list[str],
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
        diagnose_only: bool = False,
    ) -> list[dict]:
        """Re-anchor projection cards whose canonical base is not reachable."""
        if not self.event_log:
            return []
        unique_ids = list(dict.fromkeys(card_ids))
        orphaned = self.event_log.orphaned_card_bases(realm_id, unique_ids)
        results: list[dict] = []
        for card_id in unique_ids:
            history_page = self.event_log.entity_history_page(
                realm_id, "card", card_id, limit=100_000
            )
            history = history_page["events"]
            diagnosed_head = history_page["head"]
            prior_repair = next(
                (
                    item
                    for item in history
                    if item["event"]["type"] == EventType.CARD_UPSERTED.value
                    and item["event"]["source_operation"]
                    == "repair.legacy_card_history"
                ),
                None,
            )
            if prior_repair:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "already_repaired",
                        "history_state": "reachable_canonical_history",
                        "commit_hash": prior_repair["commit_hash"],
                    }
                )
                continue
            canonical_base = next(
                (
                    item
                    for item in history
                    if item["projection_effect"] == "applied"
                    and item["event"]["type"]
                    in {
                        EventType.CARD_CREATED.value,
                        EventType.CARD_UPSERTED.value,
                    }
                ),
                None,
            )
            if canonical_base:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "canonical_history_present",
                        "history_state": "reachable_canonical_history",
                        "commit_hash": canonical_base["commit_hash"],
                    }
                )
                continue

            reachable_snapshot = (
                self.event_log.entity_snapshot(
                    diagnosed_head, "card", card_id
                )
                if diagnosed_head
                else None
            )
            card = self.get_card(card_id, realm_id=realm_id)
            candidate = card
            if candidate is None:
                with self._conn() as conn:
                    row = conn.execute(
                        "SELECT * FROM items WHERE id=?", (card_id,)
                    ).fetchone()
                if row:
                    candidate = Card(
                        id=card_id,
                        realm_id=realm_id,
                        kind=CardKind(row["kind"]),
                        title=row["title"],
                        body=row["body"],
                        lane=lane_from_legacy_status(row["status"]),
                        parent_id=row["parent_id"],
                        tags=json.loads(row["tags"] or "[]"),
                        created_at=_coerce_datetime(row["created_at"])
                        or datetime.now(UTC),
                        updated_at=_coerce_datetime(row["updated_at"])
                        or datetime.now(UTC),
                    )
            reachable_card = None
            if reachable_snapshot is not None:
                try:
                    reachable_card = Card.model_validate(reachable_snapshot)
                except ValidationError:
                    pass
            if reachable_card is not None:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "conflict",
                        "history_state": "reachable_card_without_canonical_base",
                        "reachable_snapshot": reachable_snapshot,
                        "projection_snapshot": (
                            candidate.model_dump(mode="json") if candidate else None
                        ),
                    }
                )
                continue
            if not candidate:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "no_projection_source",
                        "history_state": (
                            "orphaned_canonical_history"
                            if orphaned.get(card_id)
                            else "absent"
                        ),
                        "orphaned_bases": orphaned.get(card_id, []),
                    }
                )
                continue
            # Preserve reachable post-base legacy mutations in the full repair
            # snapshot.  A partial update cannot stand alone after a rebuild.
            for item in history:
                event = item["event"]
                if event["type"] not in {
                    EventType.CARD_UPDATED.value,
                    EventType.LEASE_GRANTED.value,
                    EventType.LEASE_RELEASED.value,
                }:
                    continue
                changes = dict(event["payload"])
                if "lane" not in changes and "status" in changes:
                    changes["lane"] = lane_from_legacy_status(
                        changes["status"]
                    ).value
                merged = candidate.model_dump(mode="json")
                merged.update(changes)
                candidate = Card.model_validate(merged)
            history_state = (
                "orphaned_canonical_history"
                if orphaned.get(card_id)
                else "projection_only_legacy_state"
            )
            if diagnose_only:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "repair_available",
                        "history_state": history_state,
                        "repair_origin": "authoritative_projection",
                        "orphaned_bases": orphaned.get(card_id, []),
                    }
                )
                continue
            payload = candidate.model_dump(mode="json")
            fingerprint = hashlib.sha256(
                json.dumps(payload, sort_keys=True).encode()
            ).hexdigest()
            repair = CardEvent(
                type=EventType.CARD_UPSERTED,
                realm_id=realm_id,
                card_id=card_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=payload,
                source_operation="repair.legacy_card_history",
                causal_card_version=candidate.updated_at.isoformat(),
                field_intent=sorted(payload),
                idempotency_key=f"repair-card-history:{realm_id}:{card_id}:{fingerprint}",
                request_fingerprint=fingerprint,
                operation_result={
                    "card_id": card_id,
                    "history_state": history_state,
                    "repair_origin": "authoritative_projection",
                    "orphaned_bases": orphaned.get(card_id, []),
                },
            )
            current_head = self.event_log.get_head(realm_id)
            if current_head != diagnosed_head:
                results.append(
                    {
                        "card_id": card_id,
                        "status": "concurrent_head_conflict",
                        "history_state": "head_advanced",
                        "diagnosed_head": diagnosed_head,
                        "current_head": current_head,
                    }
                )
                continue
            try:
                commit = self.commit_event(repair)
            except DagIndexStaleError:
                # Another server advanced the head after diagnosis.  Never
                # overwrite it: the retry is a fresh, explicit classification.
                refreshed = self.event_log.entity_history(
                    realm_id, "card", card_id
                )
                base = next(
                    (
                        item for item in refreshed
                        if item["event"]["type"] in {
                            EventType.CARD_CREATED.value,
                            EventType.CARD_UPSERTED.value,
                        }
                    ),
                    None,
                )
                results.append(
                    {
                        "card_id": card_id,
                        "status": "concurrent_head_conflict",
                        "history_state": (
                            "reachable_canonical_history" if base
                            else "head_advanced"
                        ),
                        "commit_hash": base["commit_hash"] if base else None,
                    }
                )
                continue
            results.append(
                {
                    "card_id": card_id,
                    "status": "repaired",
                    "history_state": history_state,
                    "repair_origin": "authoritative_projection",
                    "orphaned_bases": orphaned.get(card_id, []),
                    "lane": candidate.lane.value,
                    "commit_hash": commit.hash,
                }
            )
        return results

    @serialized_mutation
    def delete_card(
        self,
        card_id: str,
        *,
        realm_id: str | None = None,
        principal_id: str = "user:local",
        instance_id: str = "local",
        via_log: bool = True,
    ) -> bool:
        card = self.get_card(card_id, realm_id=realm_id)
        if not card:
            return False
        if via_log and self.event_log:
            event = CardEvent(
                type=EventType.CARD_DELETED,
                realm_id=card.realm_id,
                card_id=card_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload={},
            )
            self.commit_event(event)
            return True
        return self._delete_card_projection(card_id, realm_id=realm_id)

    def _upsert_project(self, project: Project) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO projects
                (id, realm_id, title, description, status, memberships, repos,
                 agent_prompt, tool_config, tags, created_by_principal, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    project.id,
                    project.realm_id,
                    project.title,
                    project.description,
                    project.status.value,
                    json.dumps([m.model_dump() for m in project.memberships]),
                    json.dumps([r.model_dump() for r in project.repos]),
                    project.agent_prompt,
                    json.dumps(project.tool_config),
                    json.dumps(project.tags),
                    project.created_by_principal,
                    project.created_at.isoformat(),
                    project.updated_at.isoformat(),
                ),
            )

    @serialized_mutation
    def create_project(
        self,
        data: ProjectCreate,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
        via_log: bool = True,
    ) -> Project:
        project = Project(
            realm_id=data.realm_id,
            title=data.title,
            description=data.description,
            repos=list(data.repos),
            agent_prompt=data.agent_prompt,
            tool_config=dict(data.tool_config),
            tags=data.tags,
            created_by_principal=principal_id,
        )
        if via_log and self.event_log:
            event = CardEvent(
                type=EventType.PROJECT_CREATED,
                realm_id=project.realm_id,
                project_id=project.id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=project.model_dump(mode="json"),
            )
            self.commit_event(event)
        else:
            self._upsert_project(project)
        return project

    def list_repositories(self, realm_id: str = "default") -> list[Repository]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM repositories WHERE realm_id=? ORDER BY name, url",
                (realm_id,),
            ).fetchall()
        return [self._repository_from_row(row) for row in rows]

    def get_repository(
        self, repository_id: str, realm_id: str = "default"
    ) -> Repository | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM repositories WHERE id=? AND realm_id=?",
                (repository_id, realm_id),
            ).fetchone()
        return self._repository_from_row(row) if row else None

    def list_project_repositories(
        self, project_id: str, *, realm_id: str = "default"
    ) -> list[tuple[Repository, ProjectRepository]]:
        """Return normalized repository links, including their requested branches."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT r.*, pr.branch AS project_branch
                   FROM project_repositories pr
                   JOIN repositories r ON r.id=pr.repository_id
                   WHERE pr.project_id=? AND r.realm_id=?
                   ORDER BY r.url""",
                (project_id, realm_id),
            ).fetchall()
        return [
            (
                self._repository_from_row(row),
                ProjectRepository(
                    project_id=project_id,
                    repository_id=row["id"],
                    branch=row["project_branch"],
                ),
            )
            for row in rows
        ]

    def _repository_event(
        self,
        event_type: EventType,
        realm_id: str,
        payload: dict,
        principal_id: str,
        instance_id: str,
        project_id: str | None = None,
    ) -> None:
        event = CardEvent(
            type=event_type,
            realm_id=realm_id,
            project_id=project_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=payload,
        )
        if self.event_log:
            self.commit_event(event)
        else:
            self.apply_event(event)

    def create_repository(
        self,
        data: RepositoryCreate,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Repository:
        url = data.url.strip()
        if not url:
            raise ValueError("repository URL is required")
        payload = data.model_dump(mode="json")
        payload["url"] = url
        if not payload["remotes"]:
            payload["remotes"] = [
                remote.model_dump(mode="json")
                for remote in self._default_repository_remotes(url)
            ]
        if payload["status"] == RepositoryStatus.ARCHIVED.value:
            payload["archived_at"] = datetime.now(UTC).isoformat()
        repository = Repository(id=self._repository_id(data.realm_id, url), **payload)
        self._repository_event(
            EventType.REPOSITORY_CREATED,
            data.realm_id,
            repository.model_dump(mode="json"),
            principal_id,
            instance_id,
        )
        return self.get_repository(repository.id, data.realm_id) or repository

    def update_repository(
        self,
        repository_id: str,
        data: RepositoryUpdate,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Repository | None:
        repository = self.get_repository(repository_id, realm_id)
        if not repository:
            return None
        updates = data.model_dump(exclude_unset=True, mode="json")
        new_url = updates.pop("url", None)
        if new_url is not None and new_url.strip() != repository.url:
            raise ValueError("repository URL is immutable")
        for key in (
            "name",
            "remotes",
            "provider",
            "provider_metadata",
            "visibility",
            "status",
        ):
            if updates.get(key) is None:
                updates.pop(key, None)
        if "remotes" in updates and not updates["remotes"]:
            updates["remotes"] = [
                remote.model_dump(mode="json")
                for remote in self._default_repository_remotes(repository.url)
            ]
        if "status" in updates:
            updates["archived_at"] = (
                datetime.now(UTC).isoformat()
                if updates["status"] == RepositoryStatus.ARCHIVED.value
                else None
            )
        payload = {"id": repository_id, **updates}
        self._repository_event(
            EventType.REPOSITORY_UPDATED, realm_id, payload, principal_id, instance_id
        )
        return self.get_repository(repository_id, realm_id)

    def delete_repository(
        self,
        repository_id: str,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> bool:
        if not self.get_repository(repository_id, realm_id):
            return False
        self._repository_event(
            EventType.REPOSITORY_DELETED,
            realm_id,
            {"id": repository_id},
            principal_id,
            instance_id,
        )
        return True

    def link_project_repository(
        self,
        project_id: str,
        repository_id: str,
        *,
        branch: str | None = None,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> bool:
        if not self.get_project(project_id, realm_id) or not self.get_repository(
            repository_id, realm_id
        ):
            return False
        self._repository_event(
            EventType.PROJECT_REPOSITORY_LINKED,
            realm_id,
            {"repository_id": repository_id, "branch": branch},
            principal_id,
            instance_id,
            project_id,
        )
        return True

    def unlink_project_repository(
        self,
        project_id: str,
        repository_id: str,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> bool:
        self._repository_event(
            EventType.PROJECT_REPOSITORY_UNLINKED,
            realm_id,
            {"repository_id": repository_id},
            principal_id,
            instance_id,
            project_id,
        )
        return True

    def set_repository_checkout(
        self,
        checkout: RepositoryCheckout,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> None:
        self._repository_event(
            EventType.REPOSITORY_CHECKOUT_SET,
            realm_id,
            checkout.model_dump(mode="json"),
            principal_id,
            instance_id,
        )

    def remove_repository_checkout(
        self,
        repository_id: str,
        checkout_instance_id: str,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> None:
        self._repository_event(
            EventType.REPOSITORY_CHECKOUT_REMOVED,
            realm_id,
            {"repository_id": repository_id, "instance_id": checkout_instance_id},
            principal_id,
            instance_id,
        )

    def project_working_directory(
        self, project_id: str, instance_id: str
    ) -> str | None:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT rc.path FROM project_repositories pr
                   JOIN repository_checkouts rc ON rc.repository_id=pr.repository_id
                   WHERE pr.project_id=? AND rc.instance_id=?""",
                (project_id, instance_id),
            ).fetchall()
        if len(rows) == 1:
            return rows[0]["path"]
        return None

    def list_repository_checkouts(self, repository_id: str) -> list[RepositoryCheckout]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT * FROM repository_checkouts WHERE repository_id=? ORDER BY instance_id",
                (repository_id,),
            ).fetchall()
        return [RepositoryCheckout(**dict(row)) for row in rows]

    def list_instance_groups(
        self, realm_id: str = "default", *, include_archived: bool = False
    ) -> list[InstanceGroup]:
        with self._conn() as conn:
            rows = conn.execute(
                "SELECT payload FROM instance_groups WHERE realm_id=?",
                (realm_id,),
            ).fetchall()
        groups = [
            InstanceGroup.model_validate(json.loads(row["payload"])) for row in rows
        ]
        if not include_archived:
            groups = [
                group
                for group in groups
                if group.lifecycle_state == GroupLifecycle.ACTIVE
            ]
        return sorted(groups, key=lambda group: (group.name.casefold(), group.id))

    def get_instance_group(
        self, group_id: str, realm_id: str = "default"
    ) -> InstanceGroup | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT payload FROM instance_groups WHERE realm_id=? AND id=?",
                (realm_id, group_id),
            ).fetchone()
        return InstanceGroup.model_validate(json.loads(row["payload"])) if row else None

    @serialized_mutation
    def create_instance_group(
        self,
        data: InstanceGroupCreate,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> InstanceGroup:
        duplicate = next(
            (
                group
                for group in self.list_instance_groups(
                    data.realm_id, include_archived=True
                )
                if group.name.casefold() == data.name.strip().casefold()
            ),
            None,
        )
        if duplicate:
            raise ValueError("an instance group with this name already exists")
        now = datetime.now(UTC)
        values = data.model_dump(mode="python")
        values["name"] = data.name.strip()
        group = InstanceGroup(
            **values,
            created_by=principal_id,
            updated_by=principal_id,
            created_at=now,
            updated_at=now,
        )
        event = CardEvent(
            type=EventType.INSTANCE_GROUP_CREATED,
            realm_id=group.realm_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=group.model_dump(mode="json"),
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return self.get_instance_group(group.id, group.realm_id) or group

    @serialized_mutation
    def update_instance_group(
        self,
        group_id: str,
        data: InstanceGroupUpdate,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> InstanceGroup | None:
        group = self.get_instance_group(group_id, realm_id)
        if not group:
            return None
        if data.expected_version is not None and data.expected_version != group.version:
            raise ValueError(
                f"instance group version changed: expected {data.expected_version}, "
                f"found {group.version}"
            )
        updates = data.model_dump(
            mode="python", exclude_unset=True, exclude={"expected_version"}
        )
        for key, value in updates.items():
            if value is not None:
                setattr(group, key, value)
        group.version += 1
        group.membership_generation += 1
        group.updated_by = principal_id
        group.updated_at = datetime.now(UTC)
        group = InstanceGroup.model_validate(group.model_dump(mode="python"))
        event_type = (
            EventType.INSTANCE_GROUP_ARCHIVED
            if group.lifecycle_state == GroupLifecycle.ARCHIVED
            else EventType.INSTANCE_GROUP_UPDATED
        )
        event = CardEvent(
            type=event_type,
            realm_id=realm_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=group.model_dump(mode="json"),
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return self.get_instance_group(group_id, realm_id)

    @serialized_mutation
    def delete_instance_group(
        self,
        group_id: str,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> bool:
        group = self.get_instance_group(group_id, realm_id)
        if not group:
            return False
        event = CardEvent(
            type=EventType.INSTANCE_GROUP_DELETED,
            realm_id=realm_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload={"id": group_id, "version": group.version + 1},
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return True

    def get_instance_participation_policy(
        self, instance_id: str, realm_id: str = "default"
    ) -> InstanceParticipationPolicy | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT payload FROM instance_participation_policies
                WHERE realm_id=? AND instance_id=?
                """,
                (realm_id, instance_id),
            ).fetchone()
        return (
            InstanceParticipationPolicy.model_validate(json.loads(row["payload"]))
            if row
            else None
        )

    def list_instance_participation_policies(
        self, realm_id: str = "default"
    ) -> list[InstanceParticipationPolicy]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT payload FROM instance_participation_policies
                WHERE realm_id=? ORDER BY instance_id
                """,
                (realm_id,),
            ).fetchall()
        return [
            InstanceParticipationPolicy.model_validate(json.loads(row["payload"]))
            for row in rows
        ]

    @serialized_mutation
    def set_instance_participation_policy(
        self,
        policy: InstanceParticipationPolicy,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> InstanceParticipationPolicy:
        current = self.get_instance_participation_policy(
            policy.instance_id, policy.realm_id
        )
        now = datetime.now(UTC)
        policy = policy.model_copy(deep=True)
        policy.version = current.version + 1 if current else max(1, policy.version)
        policy.created_at = current.created_at if current else now
        policy.updated_at = now
        policy.actor = principal_id
        event = CardEvent(
            type=EventType.INSTANCE_PARTICIPATION_POLICY_UPDATED,
            realm_id=policy.realm_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=policy.model_dump(mode="json"),
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return (
            self.get_instance_participation_policy(policy.instance_id, policy.realm_id)
            or policy
        )

    def list_placement_defaults(
        self, realm_id: str = "default"
    ) -> list[PlacementDefault]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT payload FROM placement_defaults
                WHERE realm_id=? ORDER BY scope_key
                """,
                (realm_id,),
            ).fetchall()
        return [
            PlacementDefault.model_validate(json.loads(row["payload"])) for row in rows
        ]

    @serialized_mutation
    def set_placement_default(
        self,
        default: PlacementDefault,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> PlacementDefault:
        current = next(
            (
                item
                for item in self.list_placement_defaults(default.realm_id)
                if item.scope_key == default.scope_key
            ),
            None,
        )
        now = datetime.now(UTC)
        default = default.model_copy(deep=True)
        default.version = current.version + 1 if current else max(1, default.version)
        default.created_at = current.created_at if current else now
        default.updated_at = now
        default.actor = principal_id
        payload = default.model_dump(mode="json")
        payload["scope_key"] = default.scope_key
        event = CardEvent(
            type=EventType.PLACEMENT_DEFAULT_UPDATED,
            realm_id=default.realm_id,
            project_id=default.project_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload=payload,
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return next(
            item
            for item in self.list_placement_defaults(default.realm_id)
            if item.scope_key == default.scope_key
        )

    @serialized_mutation
    def delete_placement_default(
        self,
        *,
        realm_id: str = "default",
        project_id: str | None = None,
        workload_profile: WorkloadProfile | None = None,
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> bool:
        scope_key = default_scope_key(project_id, workload_profile)
        if not any(
            item.scope_key == scope_key
            for item in self.list_placement_defaults(realm_id)
        ):
            return False
        event = CardEvent(
            type=EventType.PLACEMENT_DEFAULT_DELETED,
            realm_id=realm_id,
            project_id=project_id,
            author_principal=principal_id,
            author_instance=instance_id,
            payload={
                "scope_key": scope_key,
                "project_id": project_id,
                "workload_profile": workload_profile,
            },
        )
        self.commit_event(event) if self.event_log else self.apply_event(event)
        return True

    def list_fleet_policy_audit(
        self,
        realm_id: str = "default",
        *,
        entity_type: str | None = None,
        entity_id: str | None = None,
        limit: int = 200,
    ) -> list[FleetPolicyAuditEvent]:
        sql = "SELECT * FROM fleet_policy_audit_events WHERE realm_id=?"
        params: list[str | int] = [realm_id]
        if entity_type:
            sql += " AND entity_type=?"
            params.append(entity_type)
        if entity_id:
            sql += " AND entity_id=?"
            params.append(entity_id)
        sql += " ORDER BY created_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            FleetPolicyAuditEvent(
                id=row["id"],
                realm_id=row["realm_id"],
                entity_type=row["entity_type"],
                entity_id=row["entity_id"],
                action=row["action"],
                actor=row["actor"],
                payload=json.loads(row["payload"] or "{}"),
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]

    def list_projects(
        self,
        realm_id: str | None = None,
        status: ProjectStatus | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Project]:
        query = "SELECT * FROM projects WHERE 1=1"
        params: list[str] = []
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        if status:
            query += " AND status = ?"
            params.append(status.value)
        query += " ORDER BY updated_at DESC"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([str(max(0, limit)), str(max(0, offset))])
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_project(row) for row in rows]

    def get_project(
        self, project_id: str, realm_id: str | None = None
    ) -> Project | None:
        query = "SELECT * FROM projects WHERE id = ?"
        params: list[str] = [project_id]
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_project(row) if row else None

    @serialized_mutation
    def update_project(
        self,
        project_id: str,
        data: ProjectUpdate,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Project | None:
        project = self.get_project(project_id, realm_id=realm_id)
        if not project:
            return None
        updates = data.model_dump(exclude_unset=True)
        payload = {}
        for key, value in updates.items():
            if key == "status" and value is not None:
                payload["status"] = value.value if hasattr(value, "value") else value
            elif key in ("memberships", "repos") and value is not None:
                payload[key] = [
                    v.model_dump() if hasattr(v, "model_dump") else v for v in value
                ]
            elif value is not None:
                payload[key] = value
        if self.event_log and payload:
            event = CardEvent(
                type=EventType.PROJECT_UPDATED,
                realm_id=realm_id,
                project_id=project_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=payload,
            )
            self.commit_event(event)
            return self.get_project(project_id, realm_id=realm_id)
        for key, value in updates.items():
            if value is not None:
                setattr(project, key, value)
        project.updated_at = datetime.now(UTC)
        self._upsert_project(project)
        return project

    @serialized_mutation
    def archive_project(
        self,
        project_id: str,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Project | None:
        if not self.get_project(project_id, realm_id=realm_id):
            return None
        if self.event_log:
            event = CardEvent(
                type=EventType.PROJECT_ARCHIVED,
                realm_id=realm_id,
                project_id=project_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload={},
            )
            self.commit_event(event)
            return self.get_project(project_id, realm_id=realm_id)
        return self.update_project(
            project_id,
            ProjectUpdate(status=ProjectStatus.ARCHIVED),
            realm_id=realm_id,
            principal_id=principal_id,
            instance_id=instance_id,
        )

    def list_cards_for_project(
        self, project_id: str, realm_id: str | None = None
    ) -> list[Card]:
        return self.list_cards(realm_id=realm_id, project_id=project_id)

    def assign_card_to_project(
        self,
        card_id: str,
        project_id: str | None,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Card | None:
        return self.update_card(
            card_id,
            CardUpdate(project_id=project_id),
            realm_id=realm_id,
            principal_id=principal_id,
            instance_id=instance_id,
        )

    # Legacy item API
    def create_item(self, data: ItemCreate, **kwargs) -> Item:
        card = self.create_card(data.to_card_create(), **kwargs)
        return Item.from_card(card)

    def list_items(
        self, kind: ItemKind | None = None, status: ItemStatus | None = None
    ) -> list[Item]:
        lane = lane_from_legacy_status(status) if status else None
        cards = self.list_cards(
            kind=CardKind(kind.value) if kind else None,
            lane=lane,
        )
        return [Item.from_card(c) for c in cards]

    def get_item(self, item_id: str) -> Item | None:
        card = self.get_card(item_id)
        return Item.from_card(card) if card else None

    def update_item(self, item_id: str, data: ItemUpdate, **kwargs) -> Item | None:
        card = self.update_card(item_id, data.to_card_update(), **kwargs)
        return Item.from_card(card) if card else None

    def delete_item(self, item_id: str, **kwargs) -> bool:
        return self.delete_card(item_id, **kwargs)

    def save_session(self, session: AgentSession) -> AgentSession:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO agent_sessions
                (id, agent_name, external_session_id, origin_instance_id, origin_instance_name,
                 authority_instance_id, dispatch_id, realm_id,
                 lifecycle_owner,
                 item_id, card_id, project_id, principal_id,
                 status, cwd, title, label, model_id, mode_id, config_json, metrics_json,
                 execution_binding_json, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    agent_name=excluded.agent_name,
                    external_session_id=excluded.external_session_id,
                    origin_instance_id=excluded.origin_instance_id,
                    origin_instance_name=excluded.origin_instance_name,
                    authority_instance_id=excluded.authority_instance_id,
                    dispatch_id=excluded.dispatch_id,
                    realm_id=excluded.realm_id,
                    lifecycle_owner=excluded.lifecycle_owner,
                    item_id=excluded.item_id,
                    card_id=excluded.card_id,
                    project_id=excluded.project_id,
                    principal_id=excluded.principal_id,
                    status=excluded.status,
                    cwd=excluded.cwd,
                    title=excluded.title,
                    label=excluded.label,
                    model_id=excluded.model_id,
                    mode_id=excluded.mode_id,
                    config_json=excluded.config_json,
                    metrics_json=excluded.metrics_json,
                    execution_binding_json=CASE
                      WHEN agent_sessions.execution_binding_json != '{}' THEN agent_sessions.execution_binding_json
                      ELSE excluded.execution_binding_json END,
                    created_at=excluded.created_at,
                    updated_at=excluded.updated_at
                """,
                (
                    session.id,
                    session.agent_name,
                    session.external_session_id,
                    session.origin_instance_id,
                    session.origin_instance_name,
                    session.authority_instance_id,
                    session.dispatch_id,
                    session.realm_id,
                    session.lifecycle_owner,
                    session.item_id or session.card_id,
                    session.card_id or session.item_id,
                    session.project_id,
                    session.principal_id,
                    session.status,
                    session.cwd,
                    session.title,
                    session.label,
                    session.model_id,
                    session.mode_id,
                    json.dumps(session.config_json or {}),
                    json.dumps(session.metrics_json or {}),
                    json.dumps(session.execution_binding or {}),
                    session.created_at.isoformat(),
                    session.updated_at.isoformat(),
                ),
            )
            if session.card_id or session.item_id:
                self._archive_retired_session_card(
                    conn, session.id, session.card_id or session.item_id
                )
                conn.execute(
                    """
                    INSERT INTO agent_session_cards
                        (session_id, card_id, realm_id, linked_by_principal, linked_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(session_id, card_id) DO UPDATE SET
                        retired_at=NULL, retired_reason=NULL,
                        retired_by_principal=NULL
                    """,
                    (
                        session.id,
                        session.card_id or session.item_id,
                        session.realm_id,
                        session.principal_id,
                        session.created_at.isoformat(),
                    ),
                )
        return session

    @staticmethod
    def _binding_materialization_is_compatible(
        prior: dict, binding: dict
    ) -> bool:
        immutable_keys = (
            "version",
            "execution_card_id",
            "execution_project_id",
            "origin_instance_id",
        )
        return all(prior.get(key) == binding.get(key) for key in immutable_keys) and all(
            key in binding and binding.get(key) == value
            for key, value in prior.items()
        )

    def set_session_execution_binding(
        self,
        session_id: str,
        binding: dict,
        *,
        reason: str,
        expected_binding: dict | None = None,
    ) -> AgentSession:
        """Apply one audited, compare-and-set execution provenance transition."""
        allowed_reasons = {
            "workspace_binding_initialized",
            "workspace_materialized",
            "legacy_workspace_recovered",
            "stale_data_dir_cwd_removed",
        }
        if reason not in allowed_reasons:
            raise ValueError("Unsupported execution binding transition reason")
        normalized = dict(binding or {})
        changed_at = datetime.now(UTC)
        with self._mutation_lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_sessions WHERE id=?", (session_id,)
            ).fetchone()
            if row is None:
                raise ValueError("Session not found")
            session = self._row_to_session(row)
            prior = dict(session.execution_binding or {})
            if expected_binding is not None and prior != dict(expected_binding):
                raise ValueError("Execution binding changed before audited transition")
            if prior == normalized:
                return session
            if reason in {
                "workspace_binding_initialized",
                "legacy_workspace_recovered",
            }:
                permitted = not prior
            elif reason == "workspace_materialized":
                permitted = bool(prior) and self._binding_materialization_is_compatible(
                    prior, normalized
                )
            else:
                expected = dict(prior)
                expected.pop("cwd", None)
                permitted = normalized == expected and "cwd" in prior
            if not permitted:
                raise ValueError(
                    "Execution binding transition would retarget immutable provenance"
                )
            conn.execute(
                "UPDATE agent_sessions SET execution_binding_json=?, updated_at=? WHERE id=?",
                (json.dumps(normalized), changed_at.isoformat(), session_id),
            )
            conn.execute(
                """INSERT INTO agent_execution_binding_history
                   (id, session_id, reason, prior_binding_json, binding_json, changed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid4()),
                    session_id,
                    reason,
                    json.dumps(prior),
                    json.dumps(normalized),
                    changed_at.isoformat(),
                ),
            )
            refreshed = conn.execute(
                "SELECT * FROM agent_sessions WHERE id=?", (session_id,)
            ).fetchone()
        return self._row_to_session(refreshed)

    def list_session_execution_binding_history(self, session_id: str) -> list[dict]:
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT id, session_id, reason, prior_binding_json,
                          binding_json, changed_at
                   FROM agent_execution_binding_history
                   WHERE session_id=? ORDER BY changed_at, id""",
                (session_id,),
            ).fetchall()
        return [
            {
                "id": row["id"],
                "session_id": row["session_id"],
                "reason": row["reason"],
                "prior_binding": json.loads(row["prior_binding_json"]),
                "binding": json.loads(row["binding_json"]),
                "changed_at": row["changed_at"],
            }
            for row in rows
        ]

    @staticmethod
    def _archive_retired_session_card(
        conn: sqlite3.Connection, session_id: str, card_id: str
    ) -> None:
        """Preserve a completed association interval before reactivating its key."""
        conn.execute(
            """INSERT OR IGNORE INTO agent_session_card_history
                   (session_id, card_id, realm_id, linked_by_principal, linked_at,
                    retired_at, retired_reason, retired_by_principal)
               SELECT session_id, card_id, realm_id, linked_by_principal, linked_at,
                      retired_at, retired_reason, retired_by_principal
               FROM agent_session_cards
               WHERE session_id = ? AND card_id = ? AND retired_at IS NOT NULL""",
            (session_id, card_id),
        )

    def link_session_card(
        self,
        session_id: str,
        card_id: str,
        *,
        principal_id: str | None = None,
        make_primary: bool = True,
    ) -> AgentSession:
        """Associate a durable session and card without discarding older links."""
        linked_at = datetime.now(UTC)
        with self._mutation_lock, self._conn() as conn:
            session_row = conn.execute(
                "SELECT * FROM agent_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if session_row is None:
                raise ValueError("Session not found")
            session = self._row_to_session(session_row)
            card_row = conn.execute(
                "SELECT id, realm_id, project_id FROM cards WHERE id = ? AND realm_id = ?",
                (card_id, session.realm_id),
            ).fetchone()
            if card_row is None:
                raise ValueError("Card not found in the session realm")
            self._archive_retired_session_card(conn, session_id, card_id)
            conn.execute(
                """
                INSERT INTO agent_session_cards
                    (session_id, card_id, realm_id, linked_by_principal, linked_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(session_id, card_id) DO UPDATE SET
                    realm_id=excluded.realm_id,
                    linked_by_principal=excluded.linked_by_principal,
                    linked_at=excluded.linked_at,
                    retired_at=NULL,
                    retired_reason=NULL,
                    retired_by_principal=NULL
                """,
                (
                    session_id,
                    card_id,
                    session.realm_id,
                    principal_id,
                    linked_at.isoformat(),
                ),
            )
            if make_primary:
                conn.execute(
                    """
                    UPDATE agent_sessions
                    SET card_id = ?, item_id = ?, project_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        card_id,
                        card_id,
                        card_row["project_id"],
                        linked_at.isoformat(),
                        session_id,
                    ),
                )
            refreshed = conn.execute(
                "SELECT * FROM agent_sessions WHERE id = ?", (session_id,)
            ).fetchone()
        return self._row_to_session(refreshed)

    def unlink_session_card(
        self,
        session_id: str,
        card_id: str,
        *,
        reason: str = "manual_unlink",
        principal_id: str | None = None,
    ) -> AgentSession:
        """Retire one association while preserving its durable audit history."""
        updated_at = datetime.now(UTC)
        with self._mutation_lock, self._conn() as conn:
            session_row = conn.execute(
                "SELECT * FROM agent_sessions WHERE id = ?", (session_id,)
            ).fetchone()
            if session_row is None:
                raise ValueError("Session not found")
            session = self._row_to_session(session_row)
            conn.execute(
                """UPDATE agent_session_cards
                   SET retired_at = ?, retired_reason = ?, retired_by_principal = ?
                   WHERE session_id = ? AND card_id = ? AND retired_at IS NULL""",
                (updated_at.isoformat(), reason, principal_id, session_id, card_id),
            )
            if session.card_id == card_id:
                replacement = conn.execute(
                    """
                    SELECT links.card_id, cards.project_id
                    FROM agent_session_cards AS links
                    JOIN cards ON cards.id = links.card_id
                    WHERE links.session_id = ? AND links.realm_id = ?
                      AND links.retired_at IS NULL
                    ORDER BY links.linked_at DESC, links.card_id DESC
                    LIMIT 1
                    """,
                    (session_id, session.realm_id),
                ).fetchone()
                next_card_id = replacement["card_id"] if replacement else None
                next_project_id = replacement["project_id"] if replacement else None
                conn.execute(
                    """
                    UPDATE agent_sessions
                    SET card_id = ?, item_id = ?, project_id = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        next_card_id,
                        next_card_id,
                        next_project_id,
                        updated_at.isoformat(),
                        session_id,
                    ),
                )
            refreshed = conn.execute(
                "SELECT * FROM agent_sessions WHERE id = ?", (session_id,)
            ).fetchone()
        return self._row_to_session(refreshed)

    def list_card_ids_for_session(self, session_id: str) -> list[str]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT card_id FROM agent_session_cards
                WHERE session_id = ?
                  AND retired_at IS NULL
                ORDER BY linked_at ASC, card_id ASC
                """,
                (session_id,),
            ).fetchall()
        return [str(row["card_id"]) for row in rows]

    def list_cards_for_session(self, session_id: str) -> list[Card]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT cards.* FROM cards
                JOIN agent_session_cards AS links ON links.card_id = cards.id
                WHERE links.session_id = ?
                  AND links.retired_at IS NULL
                ORDER BY links.linked_at ASC, cards.id ASC
                """,
                (session_id,),
            ).fetchall()
        return [self._row_to_card(row) for row in rows]

    def list_session_card_history(self, session_id: str) -> list[dict]:
        """Return active and retired card links with their full provenance."""
        with self._conn() as conn:
            rows = conn.execute(
                """SELECT card_id, realm_id, linked_by_principal, linked_at,
                          retired_at, retired_reason, retired_by_principal
                   FROM agent_session_card_history WHERE session_id = ?
                   UNION ALL
                   SELECT card_id, realm_id, linked_by_principal, linked_at,
                          retired_at, retired_reason, retired_by_principal
                   FROM agent_session_cards WHERE session_id = ?
                   ORDER BY linked_at ASC, card_id ASC""",
                (session_id, session_id),
            ).fetchall()
        return [dict(row) for row in rows]

    def list_sessions(
        self,
        *,
        label: str | None = None,
        statuses: tuple[str, ...] | list[str] | None = None,
        exclude_statuses: tuple[str, ...] | list[str] | None = None,
    ) -> list[AgentSession]:
        query = "SELECT * FROM agent_sessions WHERE 1=1"
        params: list[str] = []
        if label is not None:
            query += " AND label = ?"
            params.append(label)
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            query += f" AND status IN ({placeholders})"
            params.extend(statuses)
        if exclude_statuses:
            placeholders = ",".join("?" for _ in exclude_statuses)
            query += f" AND status NOT IN ({placeholders})"
            params.extend(exclude_statuses)
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_session(row) for row in rows]

    def create_restart_handoff(self, handoff: RestartHandoff) -> RestartHandoff:
        """Insert once and serialize each session's nonterminal restart lifecycle."""
        with self._mutation_lock, self._conn() as conn:
            existing = conn.execute(
                "SELECT * FROM agent_restart_handoffs WHERE session_id=? AND idempotency_key=?",
                (handoff.session_id, handoff.idempotency_key),
            ).fetchone()
            if existing:
                prior = self._row_to_restart_handoff(existing)
                if prior.continuation_prompt != handoff.continuation_prompt:
                    raise ValueError(
                        "Restart handoff idempotency key was reused with different content"
                    )
                return prior
            active = conn.execute(
                """SELECT * FROM agent_restart_handoffs
                   WHERE session_id=? AND status NOT IN ('failed', 'continuation_delivered')
                   ORDER BY created_at, id LIMIT 1""",
                (handoff.session_id,),
            ).fetchone()
            if active:
                raise ValueError(
                    "Session already has a nonterminal restart handoff; retry with "
                    "the original idempotency key or wait for it to finish"
                )
            conn.execute(
                """INSERT INTO agent_restart_handoffs
                   (id, session_id, idempotency_key, continuation_prompt,
                    continuation_prompt_id, status, card_id, project_id, instance_id,
                    execution_binding_json, error, attempts, created_at, updated_at, delivered_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    handoff.id,
                    handoff.session_id,
                    handoff.idempotency_key,
                    handoff.continuation_prompt,
                    handoff.continuation_prompt_id,
                    handoff.status,
                    handoff.card_id,
                    handoff.project_id,
                    handoff.instance_id,
                    json.dumps(handoff.execution_binding or {}),
                    handoff.error,
                    handoff.attempts,
                    handoff.created_at.isoformat(),
                    handoff.updated_at.isoformat(),
                    handoff.delivered_at.isoformat() if handoff.delivered_at else None,
                ),
            )
        return handoff

    def get_restart_handoff(self, handoff_id: str) -> RestartHandoff | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_restart_handoffs WHERE id=?", (handoff_id,)
            ).fetchone()
        return self._row_to_restart_handoff(row) if row else None

    def list_restart_handoffs(
        self, *, session_id: str | None = None, statuses: tuple[str, ...] | None = None
    ) -> list[RestartHandoff]:
        query = "SELECT * FROM agent_restart_handoffs WHERE 1=1"
        params: list[str] = []
        if session_id:
            query += " AND session_id=?"
            params.append(session_id)
        if statuses:
            query += " AND status IN (" + ",".join("?" for _ in statuses) + ")"
            params.extend(statuses)
        query += " ORDER BY created_at ASC, id ASC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_restart_handoff(row) for row in rows]

    def update_restart_handoff(
        self,
        handoff_id: str,
        *,
        status: str,
        error: str | None = None,
        delivered: bool = False,
        increment_attempts: bool = False,
    ) -> RestartHandoff | None:
        now = datetime.now(UTC)
        with self._mutation_lock, self._conn() as conn:
            conn.execute(
                """UPDATE agent_restart_handoffs SET status=?, error=?, updated_at=?,
                   attempts=attempts+?, delivered_at=CASE WHEN ? THEN ? ELSE delivered_at END
                   WHERE id=?""",
                (
                    status,
                    error,
                    now.isoformat(),
                    int(increment_attempts),
                    int(delivered),
                    now.isoformat(),
                    handoff_id,
                ),
            )
            row = conn.execute(
                "SELECT * FROM agent_restart_handoffs WHERE id=?", (handoff_id,)
            ).fetchone()
        return self._row_to_restart_handoff(row) if row else None

    def retry_restart_handoff(
        self, handoff_id: str, *, session_id: str
    ) -> RestartHandoff:
        """Re-arm one failed receipt without changing its continuation identity."""
        now = datetime.now(UTC)
        with self._mutation_lock, self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_restart_handoffs WHERE id=? AND session_id=?",
                (handoff_id, session_id),
            ).fetchone()
            if row is None:
                raise ValueError("Restart handoff not found for this session")
            handoff = self._row_to_restart_handoff(row)
            if handoff.status in {
                "resuming",
                "continuation_queued",
                "continuation_delivered",
            }:
                return handoff
            if handoff.status != "failed":
                raise ValueError("Restart handoff is not retryable in its current state")
            competing = conn.execute(
                """SELECT id FROM agent_restart_handoffs
                   WHERE session_id=? AND id!=?
                     AND status NOT IN ('failed', 'continuation_delivered')
                   LIMIT 1""",
                (session_id, handoff_id),
            ).fetchone()
            if competing:
                raise ValueError("Session already has a nonterminal restart handoff")
            conn.execute(
                """UPDATE agent_restart_handoffs
                   SET status='resuming', error=NULL, updated_at=?
                   WHERE id=? AND status='failed'""",
                (now.isoformat(), handoff_id),
            )
            refreshed = conn.execute(
                "SELECT * FROM agent_restart_handoffs WHERE id=?", (handoff_id,)
            ).fetchone()
        return self._row_to_restart_handoff(refreshed)

    def list_session_audit_page(
        self,
        *,
        realm_id: str,
        limit: int = 25,
        before_updated_at: str | None = None,
        before_id: str | None = None,
    ) -> list[AgentSession]:
        """Return bounded operational history without loading transcript payloads."""
        sql = "SELECT * FROM agent_sessions WHERE realm_id = ?"
        params: list[str | int] = [realm_id]
        if before_updated_at and before_id:
            sql += " AND (updated_at < ? OR (updated_at = ? AND id < ?))"
            params.extend([before_updated_at, before_updated_at, before_id])
        sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_session(row) for row in rows]

    def list_session_statuses(self) -> dict[str, str]:
        """Return id → status without materializing session transcripts."""
        with self._conn() as conn:
            rows = conn.execute("SELECT id, status FROM agent_sessions").fetchall()
        return {row["id"]: row["status"] for row in rows}

    def list_sessions_for_workshop(
        self, *, realm_id: str, limit: int
    ) -> tuple[list[AgentSession], int]:
        """Return a bounded active-first session projection and its full count.

        Workshop does not need closed session history. Filtering it in SQLite keeps
        both row materialization and the fleet heartbeat bounded while the count
        makes any omitted active rows explicit.
        """
        active_statuses = (
            "working",
            "prompting",
            "queued",
            "active",
            "connected",
            "idle",
            "recoverable",
            "deferred",
        )
        placeholders = ",".join("?" for _ in active_statuses)
        params = (realm_id, *active_statuses)
        with self._conn() as conn:
            total = int(
                conn.execute(
                    f"""SELECT COUNT(*) FROM agent_sessions
                        WHERE realm_id = ? AND status IN ({placeholders})""",
                    params,
                ).fetchone()[0]
            )
            rows = conn.execute(
                f"""SELECT * FROM agent_sessions
                    WHERE realm_id = ? AND status IN ({placeholders})
                    ORDER BY
                      CASE status
                        WHEN 'working' THEN 0
                        WHEN 'prompting' THEN 0
                        WHEN 'queued' THEN 1
                        WHEN 'recoverable' THEN 2
                        WHEN 'deferred' THEN 2
                        ELSE 3
                      END,
                      updated_at DESC,
                      id ASC
                    LIMIT ?""",
                (*params, max(0, limit)),
            ).fetchall()
        return [self._row_to_session(row) for row in rows], total

    def list_sessions_for_cards(self, card_ids: set[str]) -> list[AgentSession]:
        """Load sessions only for cards currently rendered on a board page."""
        if not card_ids:
            return []
        placeholders = ",".join("?" for _ in card_ids)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT sessions.*, links.card_id AS linked_card_id
                FROM agent_sessions AS sessions
                JOIN agent_session_cards AS links ON links.session_id = sessions.id
                WHERE links.card_id IN ({placeholders})
                  AND links.retired_at IS NULL
                ORDER BY sessions.updated_at DESC
                """,
                tuple(card_ids),
            ).fetchall()
        return [
            self._row_to_session(row).model_copy(
                update={
                    "card_id": str(row["linked_card_id"]),
                    "item_id": str(row["linked_card_id"]),
                }
            )
            for row in rows
        ]

    def list_preferred_sessions_for_project_cards(
        self, project_id: str, *, realm_id: str = "default"
    ) -> list[AgentSession]:
        """Load one canonical preferred session for each card in a project."""
        with self._conn() as conn:
            rows = conn.execute(
                """
                WITH ranked AS (
                    SELECT sessions.*, links.card_id AS linked_card_id,
                           ROW_NUMBER() OVER (
                               PARTITION BY links.card_id
                               ORDER BY
                                   CASE
                                       WHEN sessions.status IN ('closed', 'quiesced')
                                       THEN 1
                                       ELSE 0
                                   END,
                                   sessions.updated_at DESC,
                                   sessions.id DESC
                           ) AS session_rank
                    FROM agent_sessions AS sessions
                    JOIN agent_session_cards AS links
                      ON links.session_id = sessions.id
                    JOIN cards ON cards.id = links.card_id
                    WHERE cards.realm_id = ?
                      AND cards.project_id = ?
                      AND sessions.realm_id = ?
                      AND links.retired_at IS NULL
                )
                SELECT * FROM ranked WHERE session_rank = 1
                ORDER BY updated_at DESC, id DESC
                """,
                (realm_id, project_id, realm_id),
            ).fetchall()
        return [
            self._row_to_session(row).model_copy(
                update={
                    "card_id": str(row["linked_card_id"]),
                    "item_id": str(row["linked_card_id"]),
                }
            )
            for row in rows
        ]

    def count_project_sessions(
        self,
        project_id: str,
        *,
        realm_id: str = "default",
        historical: bool,
    ) -> int:
        """Count a project's live or historical sessions without hydrating rows."""
        status_clause = (
            "status IN ('closed', 'quiesced')"
            if historical
            else "status NOT IN ('closed', 'quiesced')"
        )
        with self._conn() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS total
                FROM agent_sessions
                WHERE realm_id = ?
                  AND (
                      project_id = ?
                      OR EXISTS (
                          SELECT 1
                          FROM agent_session_cards AS links
                          JOIN cards ON cards.id = links.card_id
                          WHERE links.session_id = agent_sessions.id
                            AND links.retired_at IS NULL
                            AND cards.realm_id = ? AND cards.project_id = ?
                      )
                  )
                  AND {status_clause}
                """,
                (realm_id, project_id, realm_id, project_id),
            ).fetchone()
        return int(row["total"] if row else 0)

    def list_project_sessions(
        self,
        project_id: str,
        *,
        realm_id: str = "default",
        historical: bool,
        limit: int = 20,
        offset: int = 0,
    ) -> list[AgentSession]:
        """Load one bounded, stable page of a project's session history."""
        bounded_limit = max(1, min(int(limit), 100))
        bounded_offset = max(0, int(offset))
        status_clause = (
            "status IN ('closed', 'quiesced')"
            if historical
            else "status NOT IN ('closed', 'quiesced')"
        )
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT *
                FROM agent_sessions
                WHERE realm_id = ?
                  AND (
                      project_id = ?
                      OR EXISTS (
                          SELECT 1
                          FROM agent_session_cards AS links
                          JOIN cards ON cards.id = links.card_id
                          WHERE links.session_id = agent_sessions.id
                            AND links.retired_at IS NULL
                            AND cards.realm_id = ? AND cards.project_id = ?
                      )
                  )
                  AND {status_clause}
                ORDER BY updated_at DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (
                    realm_id,
                    project_id,
                    realm_id,
                    project_id,
                    bounded_limit,
                    bounded_offset,
                ),
            ).fetchall()
        return [self._row_to_session(row) for row in rows]

    def get_session(self, session_id: str) -> AgentSession | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM agent_sessions WHERE id = ?", (session_id,)
            ).fetchone()
        return self._row_to_session(row) if row else None

    def get_session_by_label(self, label: str) -> AgentSession | None:
        with self._conn() as conn:
            row = conn.execute(
                """
                SELECT * FROM agent_sessions
                WHERE label = ?
                ORDER BY
                    CASE WHEN status = 'closed' THEN 1 ELSE 0 END,
                    CASE WHEN json_extract(config_json,
                        '$.browser_default_selected') = 1 THEN 0 ELSE 1 END,
                    created_at ASC,
                    id ASC
                LIMIT 1
                """,
                (label,),
            ).fetchone()
        return self._row_to_session(row) if row else None

    def close_session(
        self,
        session_id: str,
        *,
        reason: str,
        closed_at: datetime | None = None,
        lock_timeout_seconds: float | None = None,
        busy_timeout_ms: int = 30000,
        diagnostics: dict[str, Any] | None = None,
    ) -> tuple[AgentSession | None, str | None]:
        """Close a durable session and idempotently append its audit event.

        The prior status is ``None`` when the session was already closed, which
        makes singleton and bulk closure idempotent.
        """
        closed_at = closed_at or datetime.now(UTC)
        lock_started = time.monotonic()
        if lock_timeout_seconds is None:
            acquired = self._mutation_lock.acquire()
        else:
            acquired = self._mutation_lock.acquire(timeout=lock_timeout_seconds)
        lock_wait_ms = (time.monotonic() - lock_started) * 1000
        if diagnostics is not None:
            diagnostics["lock_wait_ms"] = round(lock_wait_ms, 3)
        if not acquired:
            if diagnostics is not None:
                diagnostics["terminal_result"] = "lock_timeout"
            raise TimeoutError(
                f"session close mutation lock exceeded {lock_timeout_seconds:.3f}s"
            )
        try:
            with self._conn(busy_timeout_ms=busy_timeout_ms) as conn:
                row = conn.execute(
                    "SELECT * FROM agent_sessions WHERE id = ?",
                    (session_id,),
                ).fetchone()
                if row is None:
                    if diagnostics is not None:
                        diagnostics["terminal_result"] = "missing"
                    return None, None
                session = self._row_to_session(row)
                if session.status == "closed":
                    if diagnostics is not None:
                        diagnostics["terminal_result"] = "already_closed"
                    return session, None
                prior_status = session.status
                conn.execute(
                    "UPDATE agent_sessions SET status='closed', updated_at=? WHERE id=?",
                    (closed_at.isoformat(), session_id),
                )
                audit_event = TranscriptEvent(
                    session_id=session_id,
                    seq=self.transcripts.next_seq(session_id),
                    event_type="session_closed",
                    payload={"reason": reason, "prior_status": prior_status},
                    created_at=closed_at,
                )
                session.status = "closed"
                session.updated_at = closed_at
                if diagnostics is not None:
                    diagnostics["terminal_result"] = "closed"
        except sqlite3.OperationalError:
            if diagnostics is not None:
                diagnostics["terminal_result"] = "sqlite_timeout"
            raise
        finally:
            self._mutation_lock.release()
        # Separate WALs cannot form one SQLite transaction. Metadata is
        # authoritative; the deterministic audit sequence is idempotently
        # mirrored after releasing the hot metadata lock.
        self.append_transcript_events([audit_event])
        return session, prior_status

    def next_transcript_seq(self, session_id: str) -> int:
        return self.transcripts.next_seq(session_id)

    def append_transcript_events(
        self, events: list[TranscriptEvent]
    ) -> list[TranscriptEvent]:
        if not events:
            return events
        mirrors = self.transcripts.append(events)
        with self._conn() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO agent_transcript_events
                (id, session_id, seq, event_type, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        e.id,
                        e.session_id,
                        e.seq,
                        e.event_type,
                        json.dumps(payload),
                        e.created_at.isoformat(),
                    )
                    for e, payload in mirrors
                ],
            )
        return events

    def list_transcript_events(
        self,
        session_id: str,
        *,
        after_seq: int = 0,
        limit: int = 500,
    ) -> list[TranscriptEvent]:
        return self.transcripts.list(session_id, after_seq=after_seq, limit=limit)

    def get_prompt_acceptance(
        self, session_id: str, prompt_id: str
    ) -> TranscriptEvent | None:
        """Find a durable browser prompt admission by its stable client id."""
        return self.transcripts.find_prompt(session_id, prompt_id)

    def get_queued_prompt_acceptance(
        self, session_id: str, prompt_id: str
    ) -> TranscriptEvent | None:
        """Find the durable queue admission that records its accepted outcome."""
        return self.transcripts.find_prompt(session_id, prompt_id, queued_only=True)

    def list_transcript_events_before(
        self,
        session_id: str,
        *,
        before_seq: int | None = None,
        limit: int = 500,
    ) -> list[TranscriptEvent]:
        """Return the newest events before a cursor, ordered chronologically."""
        return self.transcripts.list_before(session_id, before_seq=before_seq, limit=limit)

    def add_knowledge(self, entry: KnowledgeEntry) -> KnowledgeEntry:
        if not entry.content_hash:
            entry.content_hash = hashlib.sha256(
                entry.summary.encode("utf-8")
            ).hexdigest()
        with self._conn() as conn:
            duplicate = conn.execute(
                """SELECT * FROM knowledge
                   WHERE (
                       (content_hash != '' AND content_hash = ?)
                       OR lower(trim(summary)) = lower(trim(?))
                   )
                     AND kind = ? AND scope = ? AND status IN ('active', 'review')
                   ORDER BY updated_at DESC LIMIT 1""",
                (entry.content_hash, entry.summary, entry.kind.value, entry.scope),
            ).fetchone()
            if duplicate:
                return self._row_to_knowledge(duplicate)
            conn.execute(
                """
                INSERT INTO knowledge (
                    id, session_id, item_id, card_id, summary, source, source_url,
                    kind, tier, status, scope, owner, confidence, sensitivity,
                    provenance_trust, supersedes_id,
                    review_at, expires_at, tags, content_hash, provenance,
                    created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.id,
                    entry.session_id,
                    entry.item_id or entry.card_id,
                    entry.card_id or entry.item_id,
                    entry.summary,
                    entry.source,
                    entry.source_url,
                    entry.kind.value,
                    entry.tier.value,
                    entry.status.value,
                    entry.scope,
                    entry.owner,
                    entry.confidence,
                    entry.sensitivity.value,
                    entry.provenance_trust,
                    entry.supersedes_id,
                    entry.review_at.isoformat() if entry.review_at else None,
                    entry.expires_at.isoformat() if entry.expires_at else None,
                    json.dumps(entry.tags),
                    entry.content_hash,
                    json.dumps(entry.provenance.model_dump(mode="json"))
                    if entry.provenance
                    else None,
                    entry.created_at.isoformat(),
                    entry.updated_at.isoformat(),
                ),
            )
        return entry

    def list_knowledge(
        self,
        item_id: str | None = None,
        limit: int = 50,
        *,
        search: str | None = None,
        kind: str | None = None,
        status: str | None = "active",
        scope: str | None = None,
        source: str | None = None,
        tier: str | None = None,
        sensitivity: str | None = None,
        provenance_trust: str | None = None,
        expiry: str | None = None,
        supersession: str | None = None,
        before_updated_at: str | None = None,
        before_id: str | None = None,
        curated_only: bool = False,
    ) -> list[KnowledgeEntry]:
        sql = "SELECT * FROM knowledge WHERE 1=1"
        params: list[str | int] = []
        if item_id:
            sql += " AND (item_id = ? OR card_id = ?)"
            params.extend([item_id, item_id])
        if search:
            sql += " AND (summary LIKE ? OR tags LIKE ? OR owner LIKE ?)"
            needle = f"%{search}%"
            params.extend([needle, needle, needle])
        if kind:
            sql += " AND kind = ?"
            params.append(kind)
        if status:
            sql += " AND status = ?"
            params.append(status)
        if scope:
            sql += " AND scope = ?"
            params.append(scope)
        if source:
            sql += " AND source = ?"
            params.append(source)
        if tier:
            sql += " AND tier = ?"
            params.append(tier)
        if sensitivity:
            sql += " AND sensitivity = ?"
            params.append(sensitivity)
        if provenance_trust:
            sql += " AND provenance_trust = ?"
            params.append(provenance_trust)
        if curated_only:
            sql += " AND source NOT IN ('session', 'acp_session', 'remote_dispatch', 'dispatch', 'transcript')"
        now = datetime.now(UTC).isoformat()
        if expiry == "expired":
            sql += " AND expires_at IS NOT NULL AND expires_at <= ?"
            params.append(now)
        elif expiry == "current":
            sql += " AND (expires_at IS NULL OR expires_at > ?)"
            params.append(now)
        if supersession == "supersedes":
            sql += " AND supersedes_id IS NOT NULL"
        elif supersession == "original":
            sql += " AND supersedes_id IS NULL"
        if before_updated_at and before_id:
            sql += " AND (updated_at < ? OR (updated_at = ? AND id < ?))"
            params.extend([before_updated_at, before_updated_at, before_id])
        sql += " ORDER BY updated_at DESC, id DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [self._row_to_knowledge(row) for row in rows]

    def get_knowledge(self, entry_id: str) -> KnowledgeEntry | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM knowledge WHERE id = ?", (entry_id,)
            ).fetchone()
        return self._row_to_knowledge(row) if row else None

    def update_knowledge(self, entry_id: str, update) -> KnowledgeEntry | None:
        current = self.get_knowledge(entry_id)
        if not current:
            return None
        changes = update.model_dump(exclude_unset=True)
        if not changes:
            return current
        for key, value in changes.items():
            setattr(current, key, value)
        if "summary" in changes:
            current.content_hash = hashlib.sha256(
                current.summary.encode("utf-8")
            ).hexdigest()
        current.updated_at = datetime.now(UTC)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE knowledge SET summary=?, source=?, source_url=?, kind=?, tier=?, status=?,
                    scope=?, owner=?, confidence=?, sensitivity=?, supersedes_id=?, review_at=?, expires_at=?,
                    tags=?, content_hash=?, updated_at=? WHERE id=?
                """,
                (
                    current.summary,
                    current.source,
                    current.source_url,
                    current.kind.value,
                    current.tier.value,
                    current.status.value,
                    current.scope,
                    current.owner,
                    current.confidence,
                    current.sensitivity.value,
                    current.supersedes_id,
                    current.review_at.isoformat() if current.review_at else None,
                    current.expires_at.isoformat() if current.expires_at else None,
                    json.dumps(current.tags),
                    current.content_hash,
                    current.updated_at.isoformat(),
                    entry_id,
                ),
            )
        return current

    def add_knowledge_audit(self, event: KnowledgeAuditEvent) -> KnowledgeAuditEvent:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO knowledge_audit_events
                (id, knowledge_id, action, actor, payload, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    event.id,
                    event.knowledge_id,
                    event.action,
                    event.actor,
                    json.dumps(event.payload),
                    event.created_at.isoformat(),
                ),
            )
        return event

    def list_knowledge_audit(
        self, knowledge_id: str, *, limit: int = 100
    ) -> list[KnowledgeAuditEvent]:
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM knowledge_audit_events
                WHERE knowledge_id = ?
                ORDER BY created_at DESC, id DESC LIMIT ?
                """,
                (knowledge_id, limit),
            ).fetchall()
        return [
            KnowledgeAuditEvent(
                id=row["id"],
                knowledge_id=row["knowledge_id"],
                action=row["action"],
                actor=row["actor"],
                payload=json.loads(row["payload"] or "{}"),
                created_at=datetime.fromisoformat(row["created_at"]),
            )
            for row in rows
        ]

    def list_transcript_events_range(
        self,
        session_id: str,
        *,
        start_seq: int | None = None,
        end_seq: int | None = None,
    ) -> list[TranscriptEvent]:
        return self.transcripts.list_range(
            session_id, start_seq=start_seq, end_seq=end_seq
        )

    def prune_closed_session_transcripts(self, *, before: datetime) -> int:
        """Delete transcript events for closed sessions older than ``before``.

        Session rows stay so history and lifecycle remain intact. Only the
        bulky per-turn payload is removed.
        """
        with self._conn() as conn:
            session_ids = [row[0] for row in conn.execute(
                "SELECT id FROM agent_sessions WHERE status='closed' AND updated_at < ?",
                (before.isoformat(),),
            )]
        deleted = self.transcripts.prune(session_ids, keep_audit=True)
        # Compatibility rows follow the same evidence-preserving policy.
        for session_id in session_ids:
            kept = {event.seq for event in self.transcripts.list_range(session_id)}
            with self._conn() as conn:
                rows = conn.execute("SELECT seq FROM agent_transcript_events WHERE session_id=?", (session_id,)).fetchall()
                for row in rows:
                    if int(row[0]) not in kept:
                        conn.execute("DELETE FROM agent_transcript_events WHERE session_id=? AND seq=?", (session_id, row[0]))
        return deleted

    def prune_mutation_operations(self, *, before: datetime) -> int:
        """Delete succeeded/failed mutation receipts older than ``before``."""
        with self._conn() as conn:
            cursor = conn.execute(
                """
                DELETE FROM mutation_operations
                WHERE state IN ('succeeded', 'failed') AND updated_at < ?
                """,
                (before.isoformat(),),
            )
            return max(0, cursor.rowcount)

    def optimize(self) -> dict[str, int]:
        """Run SQLite maintenance that does not rewrite the whole database."""
        with self._conn() as conn:
            conn.execute("PRAGMA optimize")
            checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
            page_count = int(conn.execute("PRAGMA page_count").fetchone()[0])
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
            freelist = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
        return {
            "page_count": page_count,
            "page_size": page_size,
            "freelist_count": freelist,
            "wal_busy": int(checkpoint[0]) if checkpoint else 0,
            "wal_log": int(checkpoint[1]) if checkpoint else 0,
            "wal_checkpointed": int(checkpoint[2]) if checkpoint else 0,
        }

    def transcript_storage_metrics(self) -> dict[str, object]:
        """Operator-visible storage, integrity, redaction and migration health."""
        metrics = self.transcripts.metrics()
        with self._conn() as conn:
            legacy = conn.execute("SELECT COUNT(*) FROM agent_transcript_events").fetchone()[0]
        metrics["compatibility_rows"] = int(legacy)
        operation = self.transcripts.operation("legacy-v1") or {}
        metrics["migration"] = operation.get("state", "pending")
        metrics["migration_examined"] = int(operation.get("examined", 0))
        metrics["migration_changed"] = int(operation.get("changed", 0))
        metrics["migration_error"] = operation.get("error")
        return metrics

    def _migrate_legacy_transcripts(self, *, batch_size: int = 500) -> None:
        """Resume an idempotent legacy canary and verify each canonical hash.

        The compatibility table is rewritten with bounded payload references as
        rows are successfully copied.  A crash leaves already-copied rows safe
        and a restart continues from the first missing sequence.
        """
        operation = self.transcripts.operation("legacy-v1") or {}
        if operation.get("state") == "complete":
            return
        cursor = json.loads(operation["cursor"]) if operation.get("cursor") else ["", -1]
        try:
            with self._conn() as conn:
                rows = conn.execute(
                    """SELECT * FROM agent_transcript_events
                       WHERE session_id > ? OR (session_id=? AND seq>?)
                       ORDER BY session_id,seq LIMIT ?""",
                    (cursor[0], cursor[0], cursor[1], batch_size),
                ).fetchall()
            # Avoid rescanning compatibility rows already represented in cold DB.
            pending = []
            for row in rows:
                if self.transcripts.list_range(row["session_id"], start_seq=int(row["seq"]), end_seq=int(row["seq"]), limit=1):
                    continue
                pending.append(self._row_to_transcript(row))
            if pending:
                self.append_transcript_events(pending)
            next_cursor = json.dumps([rows[-1]["session_id"], int(rows[-1]["seq"])]) if rows else operation.get("cursor")
            self.transcripts.record_operation("legacy-v1", cursor=next_cursor,
                state="complete" if len(rows) < batch_size else "running",
                examined=len(rows), changed=len(pending))
        except Exception as exc:
            self.transcripts.record_operation("legacy-v1", cursor=operation.get("cursor"), state="failed", examined=0, changed=0, error=str(exc))
            raise

    def migrate_legacy_transcripts(self, *, batch_size: int = 500) -> dict[str, object]:
        """Advance one crash-safe migration batch and return verified health."""
        self._migrate_legacy_transcripts(batch_size=batch_size)
        return self.transcript_storage_metrics()

    def rebuild_legacy_transcript_mirror(self, *, batch_size: int = 1000) -> int:
        """Independently rebuild the bounded downgrade mirror from v1 storage."""
        changed = 0
        with self.transcripts._conn() as source, self._conn() as target:
            rows = source.execute(
                "SELECT * FROM transcript_events ORDER BY session_id,seq LIMIT ?",
                (batch_size,),
            ).fetchall()
            for row in rows:
                payload = TranscriptStorage.compatibility_payload(
                    {}, row["payload_hash"], row["cold_hash"]
                )
                target.execute(
                    """INSERT OR REPLACE INTO agent_transcript_events
                       (id,session_id,seq,event_type,payload,created_at)
                       VALUES(?,?,?,?,?,?)""",
                    (row["id"], row["session_id"], row["seq"], row["event_type"],
                     json.dumps(payload), row["created_at"]),
                )
                changed += 1
        return changed

    @serialized_mutation
    def rebuild_from_log(self, realm_id: str) -> None:
        if not self.event_log:
            return
        head = self.event_log.get_head(realm_id)
        if not head:
            return
        # Keep the destructive reset, complete replay, and head checkpoint in
        # one SQLite transaction. Nested projection helpers reuse this thread's
        # connection, so concurrent readers retain the last-good snapshot.
        with self._conn() as conn:
            conn.execute("DELETE FROM cards WHERE realm_id = ?", (realm_id,))
            # Goal state and its event/index projections are all derived from the
            # same durable realm log.  Leaving any of them behind makes an ID
            # canonicalization rebuild append a second logical entity beside the
            # malformed legacy row.
            conn.execute("DELETE FROM durable_goals WHERE realm_id = ?", (realm_id,))
            conn.execute(
                "DELETE FROM durable_goal_events WHERE realm_id = ?", (realm_id,)
            )
            conn.execute(
                "DELETE FROM durable_goal_governance_entities WHERE realm_id = ?",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM durable_goal_governance_events WHERE realm_id = ?",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM durable_goal_projection_heads WHERE realm_id = ?",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM durable_goal_projection_conflicts WHERE realm_id = ?",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM project_repositories WHERE project_id IN (SELECT id FROM projects WHERE realm_id=?)",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM repository_checkouts WHERE repository_id IN (SELECT id FROM repositories WHERE realm_id=?)",
                (realm_id,),
            )
            conn.execute("DELETE FROM repositories WHERE realm_id = ?", (realm_id,))
            conn.execute("DELETE FROM projects WHERE realm_id = ?", (realm_id,))
            conn.execute("DELETE FROM instance_groups WHERE realm_id = ?", (realm_id,))
            conn.execute(
                "DELETE FROM instance_participation_policies WHERE realm_id = ?",
                (realm_id,),
            )
            conn.execute(
                "DELETE FROM placement_defaults WHERE realm_id = ?", (realm_id,)
            )
            conn.execute(
                "DELETE FROM fleet_policy_audit_events WHERE realm_id = ?",
                (realm_id,),
            )
            present_cards: set[tuple[str, str]] = set()

            def apply_replay_event(event: CardEvent) -> None:
                if event.card_id:
                    key = (event.realm_id, event.card_id)
                    if event.type == EventType.CARD_CREATED:
                        if key in present_cards:
                            return
                        present_cards.add(key)
                    elif event.type == EventType.CARD_DELETED:
                        present_cards.discard(key)
                    else:
                        present_cards.add(key)
                self.apply_event(event)

            def restore_receipt(
                commit_hash: str, event_hash: str, event: CardEvent
            ) -> None:
                self.mark_operation_durable(
                    event, commit_hash, event_hash=event_hash
                )

            self._replaying_from_log = True
            self._replay_operation_keys_seen: set[str] = set()
            try:
                self.event_log.apply_commit_chain(
                    head,
                    apply_replay_event,
                    provenance_handler=restore_receipt,
                )
            finally:
                self._replaying_from_log = False
                del self._replay_operation_keys_seen
            self._record_projection_head(realm_id, head)

    @serialized_mutation
    def catch_up_projection(self, realm_id: str, target_head: str) -> dict[str, Any]:
        """Atomically fast-forward a projection without replaying its history."""
        started = time.perf_counter()
        if not self.event_log:
            return {
                "commits_applied": 0,
                "rebuilt": False,
                "reason": "no_event_log",
                "sqlite_ms": 0.0,
            }
        current = self.get_projection_head(realm_id)
        if current == target_head:
            return {
                "commits_applied": 0,
                "rebuilt": False,
                "reason": "identical",
                "sqlite_ms": 0.0,
            }
        if not self.event_log.get_commit(target_head):
            raise ValueError(f"missing projection target {target_head}")
        if current is None or not self.event_log.get_commit(current):
            self.rebuild_from_log(realm_id)
            return {
                "commits_applied": 0,
                "rebuilt": True,
                "reason": "missing_projection_head",
                "sqlite_ms": round((time.perf_counter() - started) * 1000, 3),
            }
        if not self.event_log.is_ancestor(current, target_head):
            self.rebuild_from_log(realm_id)
            return {
                "commits_applied": 0,
                "rebuilt": True,
                "reason": "non_fast_forward",
                "sqlite_ms": round((time.perf_counter() - started) * 1000, 3),
            }

        applied = 0
        with self._conn():
            for commit_hash, commit in self.event_log._iter_commits_parent_first(
                target_head, stop={current}
            ):
                for event_hash in commit.event_hashes:
                    event = self.event_log.get_event(event_hash)
                    if event is None:
                        raise ValueError(f"missing event object {event_hash}")
                    self.mark_operation_durable(
                        event, commit_hash, event_hash=event_hash
                    )
                    self.apply_event(event)
                applied += 1
            self._record_projection_head(realm_id, target_head)
        return {
            "commits_applied": applied,
            "rebuilt": False,
            "reason": "fast_forward",
            "sqlite_ms": round((time.perf_counter() - started) * 1000, 3),
        }

    def _row_to_project(self, row: sqlite3.Row) -> Project:
        with self._conn() as conn:
            normalized = conn.execute(
                """SELECT r.url, pr.branch, rc.path
                   FROM project_repositories pr JOIN repositories r ON r.id=pr.repository_id
                   LEFT JOIN repository_checkouts rc ON rc.repository_id=r.id AND rc.instance_id=?
                   WHERE pr.project_id=? ORDER BY r.url""",
                (self.event_log.instance_id if self.event_log else "local", row["id"]),
            ).fetchall()
        repos = [
            ProjectRepo(url=r["url"], branch=r["branch"], path=r["path"])
            for r in normalized
        ]
        if not repos:
            repos = [ProjectRepo.model_validate(r) for r in json.loads(row["repos"])]
        return Project(
            id=row["id"],
            realm_id=row["realm_id"],
            title=row["title"],
            description=row["description"],
            status=ProjectStatus(row["status"]),
            memberships=[
                ProjectMembership.model_validate(m)
                for m in json.loads(row["memberships"])
            ],
            repos=repos,
            agent_prompt=row["agent_prompt"],
            tool_config=json.loads(row["tool_config"]),
            tags=json.loads(row["tags"]),
            created_by_principal=row["created_by_principal"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_card(row: sqlite3.Row) -> Card:
        keys = row.keys()
        return Card(
            id=row["id"],
            realm_id=row["realm_id"],
            kind=CardKind(row["kind"]),
            title=row["title"],
            body=row["body"],
            summary=row["summary"] if "summary" in keys else "",
            summary_source=CardSummarySource(
                row["summary_source"]
                if "summary_source" in keys
                else CardSummarySource.FALLBACK.value
            ),
            summary_updated_at=(
                datetime.fromisoformat(row["summary_updated_at"])
                if "summary_updated_at" in keys and row["summary_updated_at"]
                else None
            ),
            summary_stale=bool(row["summary_stale"])
            if "summary_stale" in keys
            else False,
            summary_status=row["summary_status"]
            if "summary_status" in keys
            else ("ready" if row["summary"] else "pending"),
            summary_provider=row["summary_provider"]
            if "summary_provider" in keys
            else None,
            summary_model=row["summary_model"] if "summary_model" in keys else None,
            summary_auth_source=row["summary_auth_source"]
            if "summary_auth_source" in keys
            else None,
            summary_prompt_version=row["summary_prompt_version"]
            if "summary_prompt_version" in keys
            else None,
            summary_input_hash=row["summary_input_hash"]
            if "summary_input_hash" in keys
            else None,
            summary_failure=row["summary_failure"]
            if "summary_failure" in keys
            else None,
            summary_failure_code=row["summary_failure_code"]
            if "summary_failure_code" in keys
            else None,
            summary_attempt_count=int(row["summary_attempt_count"] or 0)
            if "summary_attempt_count" in keys
            else 0,
            summary_next_attempt_at=(
                datetime.fromisoformat(row["summary_next_attempt_at"])
                if "summary_next_attempt_at" in keys and row["summary_next_attempt_at"]
                else None
            ),
            summary_last_attempted_at=(
                datetime.fromisoformat(row["summary_last_attempted_at"])
                if "summary_last_attempted_at" in keys
                and row["summary_last_attempted_at"]
                else None
            ),
            summary_authority_instance_id=row["summary_authority_instance_id"]
            if "summary_authority_instance_id" in keys
            else None,
            lane=CardLane(row["lane"]),
            parent_id=row["parent_id"],
            project_id=row["project_id"] if "project_id" in keys else None,
            tags=json.loads(row["tags"]),
            attachments=[
                CardAttachment.model_validate(value)
                for value in json.loads(
                    row["attachments"] if "attachments" in keys else "[]"
                )
            ],
            visibility=row["visibility"],
            owner_principal=row["owner_principal"],
            preferred_instance=row["preferred_instance"],
            preferred_capabilities=json.loads(row["preferred_capabilities"]),
            lease_holder_instance=row["lease_holder_instance"],
            lease_holder_principal=row["lease_holder_principal"],
            lease_expires_at=datetime.fromisoformat(row["lease_expires_at"])
            if row["lease_expires_at"]
            else None,
            created_by_principal=row["created_by_principal"],
            created_by_instance=row["created_by_instance"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_session(row: sqlite3.Row) -> AgentSession:
        keys = row.keys()

        def _json_col(name: str) -> dict:
            if name not in keys or row[name] is None:
                return {}
            raw = row[name]
            if isinstance(raw, dict):
                return raw
            try:
                return json.loads(raw)
            except json.JSONDecodeError, TypeError:
                return {}

        return AgentSession(
            id=row["id"],
            agent_name=row["agent_name"],
            external_session_id=row["external_session_id"],
            origin_instance_id=(
                row["origin_instance_id"] if "origin_instance_id" in keys else None
            ),
            origin_instance_name=(
                row["origin_instance_name"] if "origin_instance_name" in keys else None
            ),
            authority_instance_id=(
                row["authority_instance_id"]
                if "authority_instance_id" in keys
                else None
            ),
            dispatch_id=row["dispatch_id"] if "dispatch_id" in keys else None,
            lifecycle_owner=(
                row["lifecycle_owner"]
                if "lifecycle_owner" in keys
                else ("dispatch" if row["dispatch_id"] else "standalone")
            ),
            realm_id=(row["realm_id"] if "realm_id" in keys else "default"),
            item_id=row["item_id"],
            card_id=row["card_id"] if "card_id" in keys else row["item_id"],
            project_id=row["project_id"] if "project_id" in keys else None,
            principal_id=row["principal_id"] if "principal_id" in keys else None,
            status=row["status"],
            cwd=row["cwd"] if "cwd" in keys else None,
            title=row["title"] if "title" in keys else None,
            label=row["label"] if "label" in keys else None,
            model_id=row["model_id"] if "model_id" in keys else None,
            mode_id=row["mode_id"] if "mode_id" in keys else None,
            config_json=_json_col("config_json"),
            metrics_json=_json_col("metrics_json"),
            execution_binding=_json_col("execution_binding_json"),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_transcript(row: sqlite3.Row) -> TranscriptEvent:
        payload = row["payload"]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                payload = {}
        return TranscriptEvent(
            id=row["id"],
            session_id=row["session_id"],
            seq=int(row["seq"]),
            event_type=row["event_type"],
            payload=payload or {},
            created_at=datetime.fromisoformat(row["created_at"]),
        )

    @staticmethod
    def _row_to_restart_handoff(row: sqlite3.Row) -> RestartHandoff:
        return RestartHandoff(
            id=row["id"],
            session_id=row["session_id"],
            idempotency_key=row["idempotency_key"],
            continuation_prompt=row["continuation_prompt"],
            continuation_prompt_id=row["continuation_prompt_id"],
            status=row["status"],
            card_id=row["card_id"],
            project_id=row["project_id"],
            instance_id=row["instance_id"],
            execution_binding=json.loads(row["execution_binding_json"] or "{}"),
            error=row["error"],
            attempts=int(row["attempts"] or 0),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
            delivered_at=datetime.fromisoformat(row["delivered_at"])
            if row["delivered_at"] else None,
        )

    @staticmethod
    def _row_to_knowledge(row: sqlite3.Row) -> KnowledgeEntry:
        keys = row.keys()
        cid = row["card_id"] if "card_id" in keys else row["item_id"]
        return KnowledgeEntry(
            id=row["id"],
            session_id=row["session_id"],
            item_id=row["item_id"],
            card_id=cid,
            summary=row["summary"],
            source=row["source"],
            source_url=row["source_url"] if "source_url" in keys else None,
            kind=row["kind"] if "kind" in keys else "memory",
            tier=row["tier"] if "tier" in keys else "semantic",
            status=row["status"] if "status" in keys else "active",
            scope=row["scope"] if "scope" in keys else "realm",
            owner=row["owner"] if "owner" in keys else None,
            confidence=row["confidence"] if "confidence" in keys else None,
            sensitivity=row["sensitivity"] if "sensitivity" in keys else "internal",
            provenance_trust=row["provenance_trust"]
            if "provenance_trust" in keys
            else "unverified",
            supersedes_id=row["supersedes_id"] if "supersedes_id" in keys else None,
            review_at=datetime.fromisoformat(row["review_at"])
            if "review_at" in keys and row["review_at"]
            else None,
            expires_at=datetime.fromisoformat(row["expires_at"])
            if "expires_at" in keys and row["expires_at"]
            else None,
            tags=json.loads(row["tags"]),
            content_hash=row["content_hash"] if "content_hash" in keys else "",
            provenance=KnowledgeProvenance.model_validate(json.loads(row["provenance"]))
            if "provenance" in keys and row["provenance"]
            else None,
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"])
            if "updated_at" in keys and row["updated_at"]
            else datetime.fromisoformat(row["created_at"]),
        )
