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
        conn = sqlite3.connect(self.db_path)
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
                """
            )
            self._migrate_items_to_cards(conn)

    def _migrate_items_to_cards(self, conn: sqlite3.Connection) -> None:
        count = conn.execute("SELECT COUNT(*) FROM cards").fetchone()[0]
        if count > 0:
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
            tags=p.get("tags", []),
            preferred_instance=p.get("preferred_instance"),
            preferred_capabilities=p.get("preferred_capabilities", []),
            created_by_principal=event.author_principal,
            created_by_instance=event.author_instance,
        )
        self._upsert_card(card)

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
                (id, realm_id, kind, title, body, lane, parent_id, tags, visibility,
                 owner_principal, preferred_instance, preferred_capabilities,
                 lease_holder_instance, lease_holder_principal, lease_expires_at,
                 created_by_principal, created_by_instance, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    card.id,
                    card.realm_id,
                    card.kind.value,
                    card.title,
                    card.body,
                    card.lane.value,
                    card.parent_id,
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

    def delete_card(self, card_id: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM cards WHERE id = ?", (card_id,))
        return cur.rowcount > 0

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

    def delete_item(self, item_id: str) -> bool:
        return self.delete_card(item_id)

    def save_session(self, session: AgentSession) -> AgentSession:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO agent_sessions
                (id, agent_name, external_session_id, item_id, card_id, principal_id,
                 status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.id,
                    session.agent_name,
                    session.external_session_id,
                    session.item_id or session.card_id,
                    session.card_id or session.item_id,
                    session.principal_id,
                    session.status,
                    session.created_at.isoformat(),
                    session.updated_at.isoformat(),
                ),
            )
        return session

    def list_sessions(self) -> list[AgentSession]:
        with self._conn() as conn:
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
        self.event_log.apply_commit_chain(head, self.apply_event)

    @staticmethod
    def _row_to_card(row: sqlite3.Row) -> Card:
        return Card(
            id=row["id"],
            realm_id=row["realm_id"],
            kind=CardKind(row["kind"]),
            title=row["title"],
            body=row["body"],
            lane=CardLane(row["lane"]),
            parent_id=row["parent_id"],
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
        return AgentSession(
            id=row["id"],
            agent_name=row["agent_name"],
            external_session_id=row["external_session_id"],
            item_id=row["item_id"],
            card_id=row["card_id"] if "card_id" in keys else row["item_id"],
            principal_id=row["principal_id"] if "principal_id" in keys else None,
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
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
