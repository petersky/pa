"""Card projection service — applies events to SQLite."""

from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from functools import wraps
from typing import Callable, Iterator, TypeVar
from uuid import NAMESPACE_URL, uuid4, uuid5

from pa.domain.card_summaries import fallback_card_summary
from pa.domain.models import (
    AgentSession,
    Card,
    CardCreate,
    CardEvent,
    CardKind,
    CardLane,
    CardSummarySource,
    CardUpdate,
    EventType,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    ItemUpdate,
    KnowledgeEntry,
    Project,
    ProjectCreate,
    ProjectMembership,
    ProjectRepo,
    ProjectStatus,
    ProjectUpdate,
    Repository,
    RepositoryCheckout,
    RepositoryCreate,
    RepositoryUpdate,
    TranscriptEvent,
    _STATUS_TO_LANE,
)
from pa.sync.event_log import EventLog

T = TypeVar("T")


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
        self._init_db()

    @contextmanager
    def _conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.execute("PRAGMA busy_timeout = 30000")
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
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
                    item_id TEXT,
                    card_id TEXT,
                    principal_id TEXT,
                    status TEXT NOT NULL DEFAULT 'idle',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    item_id TEXT,
                    card_id TEXT,
                    summary TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'session',
                    tags TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL
                );
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
                """
            )
            self._migrate_items_to_cards(conn)
            self._migrate_schema(conn)
            self._migrate_project_repositories(conn)

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
        ):
            if col not in card_cols:
                conn.execute(f"ALTER TABLE cards ADD COLUMN {col} {decl}")
        for row in conn.execute(
            "SELECT id, body, summary, updated_at FROM cards"
        ).fetchall():
            if not (row["summary"] or "").strip():
                conn.execute(
                    """
                    UPDATE cards
                    SET summary=?, summary_source='fallback',
                        summary_updated_at=updated_at, summary_stale=0
                    WHERE id=?
                    """,
                    (fallback_card_summary(row["body"]), row["id"]),
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
        for col, decl in (
            ("cwd", "TEXT"),
            ("title", "TEXT"),
            ("label", "TEXT"),
            ("model_id", "TEXT"),
            ("mode_id", "TEXT"),
            ("config_json", "TEXT NOT NULL DEFAULT '{}'"),
            ("metrics_json", "TEXT NOT NULL DEFAULT '{}'"),
        ):
            if col not in session_cols:
                conn.execute(f"ALTER TABLE agent_sessions ADD COLUMN {col} {decl}")

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

    def _migrate_items_to_cards(self, conn: sqlite3.Connection) -> None:
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

    def _repository_id(self, realm_id: str, url: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"pa:{realm_id}:{url.strip()}"))

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
                "INSERT OR IGNORE INTO repositories (id, realm_id, url, name, created_at, updated_at) VALUES (?, ?, ?, '', ?, ?)",
                (repository_id, realm_id, url, now, now),
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
        if event.type == EventType.CARD_CREATED:
            self._apply_created(event)
        elif event.type == EventType.CARD_UPDATED:
            self._apply_updated(event)
        elif event.type == EventType.CARD_DELETED:
            self._apply_deleted(event)
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

    @serialized_mutation
    def commit_event(self, event: CardEvent):
        """Append, project, and checkpoint one event as an ordered unit."""
        if not self.event_log:
            raise RuntimeError("Cannot commit an event without an event log")
        _, commit = self.event_log.append_event(event, on_commit=self._on_commit)
        self.apply_event(event)
        self._record_projection_head(event.realm_id, commit.hash)
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

    def _apply_repository_created(self, event: CardEvent) -> None:
        p = event.payload
        now = p.get("created_at") or event.timestamp.isoformat()
        with self._conn() as conn:
            conn.execute(
                "INSERT OR IGNORE INTO repositories (id, realm_id, url, name, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    p["id"],
                    event.realm_id,
                    p["url"],
                    p.get("name", ""),
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
                    "UPDATE repositories SET name=?, updated_at=? WHERE id=?",
                    (
                        p.get("name", row["name"]),
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
        p = event.payload
        created_at = _coerce_datetime(p.get("created_at")) or datetime.now(UTC)
        updated_at = _coerce_datetime(p.get("updated_at")) or created_at
        summary = (p.get("summary") or "").strip() or fallback_card_summary(
            p.get("body", "")
        )
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
            summary_updated_at=_coerce_datetime(p.get("summary_updated_at"))
            or updated_at,
            summary_stale=bool(p.get("summary_stale", False)),
            lane=CardLane(p.get("lane", "inbox")),
            parent_id=p.get("parent_id"),
            project_id=p.get("project_id"),
            tags=p.get("tags", []),
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
        payload = event.payload
        if "body" in payload and "summary" not in payload:
            if card.summary_source == CardSummarySource.FALLBACK:
                card.summary = fallback_card_summary(payload.get("body", ""))
                card.summary_updated_at = _coerce_datetime(
                    payload.get("updated_at")
                ) or datetime.now(UTC)
                card.summary_stale = False
            else:
                card.summary_stale = True
        for key, value in payload.items():
            if key in {
                "created_at",
                "updated_at",
                "lease_expires_at",
                "summary_updated_at",
            }:
                continue
            if key == "lane":
                card.lane = CardLane(value)
            elif key == "summary_source":
                card.summary_source = CardSummarySource(value)
            elif hasattr(card, key):
                setattr(card, key, value)
        if "lease_expires_at" in payload:
            card.lease_expires_at = _coerce_datetime(payload.get("lease_expires_at"))
        if "summary_updated_at" in payload:
            card.summary_updated_at = _coerce_datetime(
                payload.get("summary_updated_at")
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
                 summary_updated_at, summary_stale, lane, parent_id, project_id, tags, visibility,
                 owner_principal, preferred_instance, preferred_capabilities,
                 lease_holder_instance, lease_holder_principal, lease_expires_at,
                 created_by_principal, created_by_instance, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    card.lane.value,
                    card.parent_id,
                    card.project_id,
                    json.dumps(card.tags),
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
    ) -> Card:
        now = datetime.now(UTC)
        supplied_summary = data.summary.strip()
        card = Card(
            realm_id=data.realm_id,
            kind=data.kind,
            title=data.title,
            body=data.body,
            summary=supplied_summary or fallback_card_summary(data.body),
            summary_source=(
                data.summary_source or CardSummarySource.MANUAL
                if supplied_summary
                else CardSummarySource.FALLBACK
            ),
            summary_updated_at=now,
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
            )
            self.commit_event(event)
        else:
            self._upsert_card(card)
        return card

    def _on_commit(self, commit) -> None:
        pass  # wired by Store

    def list_cards(
        self,
        realm_id: str | None = None,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
        project_id: str | None = None,
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
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
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

    @serialized_mutation
    def update_card(
        self,
        card_id: str,
        data: CardUpdate,
        *,
        realm_id: str = "default",
        principal_id: str = "user:local",
        instance_id: str = "local",
    ) -> Card | None:
        card = self.get_card(card_id, realm_id=realm_id)
        if not card:
            return None
        updates = data.model_dump(exclude_unset=True)
        now = datetime.now(UTC)
        payload = {}
        for key, value in updates.items():
            if key == "lane" and value is not None:
                payload["lane"] = value.value if hasattr(value, "value") else value
            elif key == "summary_source" and value is not None:
                payload["summary_source"] = (
                    value.value if hasattr(value, "value") else value
                )
            elif value is not None:
                payload[key] = value
        if data.body is not None and data.summary is None:
            if card.summary_source == CardSummarySource.FALLBACK:
                payload.update(
                    summary=fallback_card_summary(data.body),
                    summary_source=CardSummarySource.FALLBACK.value,
                    summary_stale=False,
                    summary_updated_at=now.isoformat(),
                )
            else:
                payload["summary_stale"] = True
        if data.summary is not None:
            supplied_summary = data.summary.strip()
            payload.update(
                summary=supplied_summary
                or fallback_card_summary(
                    data.body if data.body is not None else card.body
                ),
                summary_source=(
                    payload.get("summary_source") or CardSummarySource.MANUAL.value
                    if supplied_summary
                    else CardSummarySource.FALLBACK.value
                ),
                summary_stale=(
                    data.summary_stale if data.summary_stale is not None else False
                ),
                summary_updated_at=now.isoformat(),
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
            )
            self.commit_event(event)
            return self.get_card(card_id, realm_id=realm_id)
        for key, value in payload.items():
            if key == "summary_updated_at":
                card.summary_updated_at = _coerce_datetime(value)
            elif key == "summary_source":
                card.summary_source = CardSummarySource(value)
            elif key != "updated_at" and value is not None and hasattr(card, key):
                setattr(card, key, value)
        card.updated_at = now
        self._upsert_card(card)
        return card

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
        return [Repository(**dict(row)) for row in rows]

    def get_repository(
        self, repository_id: str, realm_id: str = "default"
    ) -> Repository | None:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT * FROM repositories WHERE id=? AND realm_id=?",
                (repository_id, realm_id),
            ).fetchone()
        return Repository(**dict(row)) if row else None

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
        repository = Repository(
            id=self._repository_id(data.realm_id, data.url), **data.model_dump()
        )
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
        updates = data.model_dump(exclude_unset=True, exclude_none=True)
        if "url" in updates and updates["url"] != repository.url:
            raise ValueError("repository URL is immutable")
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

    def list_projects(
        self,
        realm_id: str | None = None,
        status: ProjectStatus | None = None,
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
        lane = _STATUS_TO_LANE.get(status) if status else None
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
                INSERT OR REPLACE INTO agent_sessions
                (id, agent_name, external_session_id, item_id, card_id, project_id, principal_id,
                 status, cwd, title, label, model_id, mode_id, config_json, metrics_json,
                 created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.id,
                    session.agent_name,
                    session.external_session_id,
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
                    session.created_at.isoformat(),
                    session.updated_at.isoformat(),
                ),
            )
        return session

    def list_sessions(self, *, label: str | None = None) -> list[AgentSession]:
        with self._conn() as conn:
            if label is not None:
                rows = conn.execute(
                    "SELECT * FROM agent_sessions WHERE label = ? ORDER BY updated_at DESC",
                    (label,),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM agent_sessions ORDER BY updated_at DESC"
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
                WHERE label = ? AND status != 'closed'
                ORDER BY updated_at DESC LIMIT 1
                """,
                (label,),
            ).fetchone()
        return self._row_to_session(row) if row else None

    def next_transcript_seq(self, session_id: str) -> int:
        with self._conn() as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(seq), 0) AS max_seq FROM agent_transcript_events WHERE session_id = ?",
                (session_id,),
            ).fetchone()
        return int(row["max_seq"] if row else 0) + 1

    def append_transcript_events(
        self, events: list[TranscriptEvent]
    ) -> list[TranscriptEvent]:
        if not events:
            return events
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
                        json.dumps(e.payload),
                        e.created_at.isoformat(),
                    )
                    for e in events
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
        with self._conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM agent_transcript_events
                WHERE session_id = ? AND seq > ?
                ORDER BY seq ASC LIMIT ?
                """,
                (session_id, after_seq, limit),
            ).fetchall()
        return [self._row_to_transcript(row) for row in rows]

    def list_transcript_events_before(
        self,
        session_id: str,
        *,
        before_seq: int | None = None,
        limit: int = 500,
    ) -> list[TranscriptEvent]:
        """Return the newest events before a cursor, ordered chronologically."""
        params: list[str | int] = [session_id]
        cursor_clause = ""
        if before_seq is not None:
            cursor_clause = "AND seq < ?"
            params.append(before_seq)
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT * FROM (
                    SELECT * FROM agent_transcript_events
                    WHERE session_id = ? {cursor_clause}
                    ORDER BY seq DESC LIMIT ?
                ) ORDER BY seq ASC
                """,
                params,
            ).fetchall()
        return [self._row_to_transcript(row) for row in rows]

    def add_knowledge(self, entry: KnowledgeEntry) -> KnowledgeEntry:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO knowledge (id, session_id, item_id, card_id, summary, source, tags, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.id,
                    entry.session_id,
                    entry.item_id or entry.card_id,
                    entry.card_id or entry.item_id,
                    entry.summary,
                    entry.source,
                    json.dumps(entry.tags),
                    entry.created_at.isoformat(),
                ),
            )
        return entry

    def list_knowledge(
        self, item_id: str | None = None, limit: int = 50
    ) -> list[KnowledgeEntry]:
        query = "SELECT * FROM knowledge WHERE 1=1"
        params: list[str | int] = []
        if item_id:
            query += " AND (item_id = ? OR card_id = ?)"
            params.extend([item_id, item_id])
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_knowledge(row) for row in rows]

    @serialized_mutation
    def rebuild_from_log(self, realm_id: str) -> None:
        if not self.event_log:
            return
        head = self.event_log.get_head(realm_id)
        if not head:
            return
        with self._conn() as conn:
            conn.execute("DELETE FROM cards WHERE realm_id = ?", (realm_id,))
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
        self.event_log.apply_commit_chain(head, self.apply_event)
        self._record_projection_head(realm_id, head)

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
            summary=row["summary"]
            if "summary" in keys
            else fallback_card_summary(row["body"]),
            summary_source=CardSummarySource(
                row["summary_source"]
                if "summary_source" in keys
                else CardSummarySource.FALLBACK.value
            ),
            summary_updated_at=(
                datetime.fromisoformat(row["summary_updated_at"])
                if "summary_updated_at" in keys and row["summary_updated_at"]
                else datetime.fromisoformat(row["updated_at"])
            ),
            summary_stale=bool(row["summary_stale"])
            if "summary_stale" in keys
            else False,
            lane=CardLane(row["lane"]),
            parent_id=row["parent_id"],
            project_id=row["project_id"] if "project_id" in keys else None,
            tags=json.loads(row["tags"]),
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
            tags=json.loads(row["tags"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )
