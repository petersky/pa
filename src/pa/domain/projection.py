"""Card projection service — applies events to SQLite."""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator
from uuid import uuid4

from pa.domain.models import (
    AgentSession,
    Card,
    CardCreate,
    CardEvent,
    CardKind,
    CardLane,
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
    TranscriptEvent,
    _STATUS_TO_LANE,
)
from pa.sync.event_log import EventLog


class CardProjection:
    def __init__(self, db_path: Path, event_log: EventLog | None = None) -> None:
        self.db_path = db_path
        self.event_log = event_log
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
                """
            )
            self._migrate_items_to_cards(conn)
            self._migrate_schema(conn)

    def _migrate_schema(self, conn: sqlite3.Connection) -> None:
        card_cols = {row[1] for row in conn.execute("PRAGMA table_info(cards)").fetchall()}
        if "project_id" not in card_cols:
            conn.execute("ALTER TABLE cards ADD COLUMN project_id TEXT")

        session_cols = {row[1] for row in conn.execute("PRAGMA table_info(agent_sessions)").fetchall()}
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

        knowledge_cols = {row[1] for row in conn.execute("PRAGMA table_info(knowledge)").fetchall()}
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

    def _apply_created(self, event: CardEvent) -> None:
        p = event.payload
        card = Card(
            id=p.get("id", event.card_id or str(uuid4())),
            realm_id=event.realm_id,
            kind=CardKind(p.get("kind", "task")),
            title=p.get("title", ""),
            body=p.get("body", ""),
            lane=CardLane(p.get("lane", "inbox")),
            parent_id=p.get("parent_id"),
            project_id=p.get("project_id"),
            tags=p.get("tags", []),
            preferred_instance=p.get("preferred_instance"),
            preferred_capabilities=p.get("preferred_capabilities", []),
            created_by_principal=event.author_principal,
            created_by_instance=event.author_instance,
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
            memberships=[ProjectMembership.model_validate(m) for m in p.get("memberships", [])],
            repos=[ProjectRepo.model_validate(r) for r in p.get("repos", [])],
            agent_prompt=p.get("agent_prompt", ""),
            tool_config=p.get("tool_config", {}),
            tags=p.get("tags", []),
            created_by_principal=event.author_principal,
        )
        self._upsert_project(project)

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
                project.memberships = [ProjectMembership.model_validate(m) for m in value]
            elif key == "repos" and value is not None:
                project.repos = [ProjectRepo.model_validate(r) for r in value]
            elif hasattr(project, key):
                setattr(project, key, value)
        project.updated_at = datetime.now(UTC)
        self._upsert_project(project)

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
        for key, value in event.payload.items():
            if key == "lane":
                card.lane = CardLane(value)
            elif hasattr(card, key):
                setattr(card, key, value)
        card.updated_at = datetime.now(UTC)
        self._upsert_card(card)

    def _apply_deleted(self, event: CardEvent) -> None:
        if event.card_id:
            self.delete_card(event.card_id)

    def _apply_lease(self, event: CardEvent) -> None:
        if not event.card_id:
            return
        card = self.get_card(event.card_id, realm_id=event.realm_id)
        if not card:
            return
        card.lease_holder_instance = event.payload.get("holder_instance")
        card.lease_holder_principal = event.payload.get("holder_principal")
        exp = event.payload.get("expires_at")
        card.lease_expires_at = datetime.fromisoformat(exp) if exp else None
        card.updated_at = datetime.now(UTC)
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
        card.updated_at = datetime.now(UTC)
        self._upsert_card(card)

    def _upsert_card(self, card: Card) -> None:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cards
                (id, realm_id, kind, title, body, lane, parent_id, project_id, tags, visibility,
                 owner_principal, preferred_instance, preferred_capabilities,
                 lease_holder_instance, lease_holder_principal, lease_expires_at,
                 created_by_principal, created_by_instance, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card.id,
                    card.realm_id,
                    card.kind.value,
                    card.title,
                    card.body,
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
                    card.lease_expires_at.isoformat() if card.lease_expires_at else None,
                    card.created_by_principal,
                    card.created_by_instance,
                    card.created_at.isoformat(),
                    card.updated_at.isoformat(),
                ),
            )

    def create_card(
        self,
        data: CardCreate,
        *,
        principal_id: str = "user:local",
        instance_id: str = "local",
        via_log: bool = True,
    ) -> Card:
        card = Card(
            realm_id=data.realm_id,
            kind=data.kind,
            title=data.title,
            body=data.body,
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
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
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
        payload = {}
        for key, value in updates.items():
            if key == "lane" and value is not None:
                payload["lane"] = value.value if hasattr(value, "value") else value
            elif value is not None:
                payload[key] = value
        if self.event_log and payload:
            event = CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id=realm_id,
                card_id=card_id,
                author_principal=principal_id,
                author_instance=instance_id,
                payload=payload,
            )
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
            return self.get_card(card_id, realm_id=realm_id)
        for key, value in updates.items():
            if value is not None:
                setattr(card, key, value)
        card.updated_at = datetime.now(UTC)
        self._upsert_card(card)
        return card

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
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
            return True
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        return cur.rowcount > 0

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
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
        else:
            self._upsert_project(project)
        return project

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

    def get_project(self, project_id: str, realm_id: str | None = None) -> Project | None:
        query = "SELECT * FROM projects WHERE id = ?"
        params: list[str] = [project_id]
        if realm_id:
            query += " AND realm_id = ?"
            params.append(realm_id)
        with self._conn() as conn:
            row = conn.execute(query, params).fetchone()
        return self._row_to_project(row) if row else None

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
                payload[key] = [v.model_dump() if hasattr(v, "model_dump") else v for v in value]
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
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
            return self.get_project(project_id, realm_id=realm_id)
        for key, value in updates.items():
            if value is not None:
                setattr(project, key, value)
        project.updated_at = datetime.now(UTC)
        self._upsert_project(project)
        return project

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
            self.event_log.append_event(event, on_commit=self._on_commit)
            self.apply_event(event)
            return self.get_project(project_id, realm_id=realm_id)
        return self.update_project(
            project_id,
            ProjectUpdate(status=ProjectStatus.ARCHIVED),
            realm_id=realm_id,
            principal_id=principal_id,
            instance_id=instance_id,
        )

    def list_cards_for_project(self, project_id: str, realm_id: str | None = None) -> list[Card]:
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

    def list_items(self, kind: ItemKind | None = None, status: ItemStatus | None = None) -> list[Item]:
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

    def append_transcript_events(self, events: list[TranscriptEvent]) -> list[TranscriptEvent]:
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

    def list_knowledge(self, item_id: str | None = None, limit: int = 50) -> list[KnowledgeEntry]:
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

    def rebuild_from_log(self, realm_id: str) -> None:
        if not self.event_log:
            return
        head = self.event_log.get_head(realm_id)
        if not head:
            return
        with self._conn() as conn:
            conn.execute("DELETE FROM cards WHERE realm_id = ?", (realm_id,))
            conn.execute("DELETE FROM projects WHERE realm_id = ?", (realm_id,))
        self.event_log.apply_commit_chain(head, self.apply_event)

    @staticmethod
    def _row_to_project(row: sqlite3.Row) -> Project:
        return Project(
            id=row["id"],
            realm_id=row["realm_id"],
            title=row["title"],
            description=row["description"],
            status=ProjectStatus(row["status"]),
            memberships=[ProjectMembership.model_validate(m) for m in json.loads(row["memberships"])],
            repos=[ProjectRepo.model_validate(r) for r in json.loads(row["repos"])],
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
            except (json.JSONDecodeError, TypeError):
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
