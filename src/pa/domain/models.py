from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field


class ItemKind(StrEnum):
    GOAL = "goal"
    TASK = "task"
    PROJECT = "project"
    CONCERN = "concern"


class ItemStatus(StrEnum):
    OPEN = "open"
    ACTIVE = "active"
    BLOCKED = "blocked"
    DONE = "done"
    ARCHIVED = "archived"


class Item(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    kind: ItemKind
    title: str
    body: str = ""
    status: ItemStatus = ItemStatus.OPEN
    parent_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ItemCreate(BaseModel):
    kind: ItemKind
    title: str
    body: str = ""
    status: ItemStatus = ItemStatus.OPEN
    parent_id: str | None = None
    tags: list[str] = Field(default_factory=list)


class ItemUpdate(BaseModel):
    title: str | None = None
    body: str | None = None
    status: ItemStatus | None = None
    parent_id: str | None = None
    tags: list[str] | None = None


class AgentSession(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    agent_name: str
    external_session_id: str | None = None
    item_id: str | None = None
    status: str = "idle"
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class KnowledgeEntry(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str | None = None
    item_id: str | None = None
    summary: str
    source: str = "session"
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class InstanceInfo(BaseModel):
    id: str
    name: str
    host: str
    port: int
    peers: list[str] = Field(default_factory=list)
