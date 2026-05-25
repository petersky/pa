import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterator

from pa.domain.models import (
    AgentSession,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    ItemUpdate,
    KnowledgeEntry,
)


class Store:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
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
                    status TEXT NOT NULL DEFAULT 'idle',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS knowledge (
                    id TEXT PRIMARY KEY,
                    session_id TEXT,
                    item_id TEXT,
                    summary TEXT NOT NULL,
                    source TEXT NOT NULL DEFAULT 'session',
                    tags TEXT NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL
                );
                """
            )

    def create_item(self, data: ItemCreate) -> Item:
        item = Item(**data.model_dump())
        with self._conn() as conn:
            conn.execute(
                """
                INSERT INTO items (id, kind, title, body, status, parent_id, tags, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    item.id,
                    item.kind.value,
                    item.title,
                    item.body,
                    item.status.value,
                    item.parent_id,
                    json.dumps(item.tags),
                    item.created_at.isoformat(),
                    item.updated_at.isoformat(),
                ),
            )
        return item

    def list_items(
        self,
        kind: ItemKind | None = None,
        status: ItemStatus | None = None,
    ) -> list[Item]:
        query = "SELECT * FROM items WHERE 1=1"
        params: list[str] = []
        if kind:
            query += " AND kind = ?"
            params.append(kind.value)
        if status:
            query += " AND status = ?"
            params.append(status.value)
        query += " ORDER BY updated_at DESC"
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_item(row) for row in rows]

    def get_item(self, item_id: str) -> Item | None:
        with self._conn() as conn:
            row = conn.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
        return self._row_to_item(row) if row else None

    def update_item(self, item_id: str, data: ItemUpdate) -> Item | None:
        item = self.get_item(item_id)
        if not item:
            return None
        updates = data.model_dump(exclude_unset=True)
        for key, value in updates.items():
            setattr(item, key, value)
        item.updated_at = datetime.now(UTC)
        with self._conn() as conn:
            conn.execute(
                """
                UPDATE items
                SET title = ?, body = ?, status = ?, parent_id = ?, tags = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    item.title,
                    item.body,
                    item.status.value,
                    item.parent_id,
                    json.dumps(item.tags),
                    item.updated_at.isoformat(),
                    item.id,
                ),
            )
        return item

    def delete_item(self, item_id: str) -> bool:
        with self._conn() as conn:
            cur = conn.execute("DELETE FROM items WHERE id = ?", (item_id,))
        return cur.rowcount > 0

    def save_session(self, session: AgentSession) -> AgentSession:
        with self._conn() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO agent_sessions
                (id, agent_name, external_session_id, item_id, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    session.id,
                    session.agent_name,
                    session.external_session_id,
                    session.item_id,
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
                INSERT INTO knowledge (id, session_id, item_id, summary, source, tags, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    entry.id,
                    entry.session_id,
                    entry.item_id,
                    entry.summary,
                    entry.source,
                    json.dumps(entry.tags),
                    entry.created_at.isoformat(),
                ),
            )
        return entry

    def list_knowledge(
        self,
        item_id: str | None = None,
        limit: int = 50,
    ) -> list[KnowledgeEntry]:
        query = "SELECT * FROM knowledge WHERE 1=1"
        params: list[str | int] = []
        if item_id:
            query += " AND item_id = ?"
            params.append(item_id)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._conn() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_knowledge(row) for row in rows]

    @staticmethod
    def _row_to_item(row: sqlite3.Row) -> Item:
        return Item(
            id=row["id"],
            kind=ItemKind(row["kind"]),
            title=row["title"],
            body=row["body"],
            status=ItemStatus(row["status"]),
            parent_id=row["parent_id"],
            tags=json.loads(row["tags"]),
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_session(row: sqlite3.Row) -> AgentSession:
        return AgentSession(
            id=row["id"],
            agent_name=row["agent_name"],
            external_session_id=row["external_session_id"],
            item_id=row["item_id"],
            status=row["status"],
            created_at=datetime.fromisoformat(row["created_at"]),
            updated_at=datetime.fromisoformat(row["updated_at"]),
        )

    @staticmethod
    def _row_to_knowledge(row: sqlite3.Row) -> KnowledgeEntry:
        return KnowledgeEntry(
            id=row["id"],
            session_id=row["session_id"],
            item_id=row["item_id"],
            summary=row["summary"],
            source=row["source"],
            tags=json.loads(row["tags"]),
            created_at=datetime.fromisoformat(row["created_at"]),
        )


_store: Store | None = None


def get_store() -> Store:
    global _store
    if _store is None:
        from pa.config import get_settings

        _store = Store(get_settings().data_dir / "pa.db")
    return _store
