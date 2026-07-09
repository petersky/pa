from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from pydantic import BaseModel, Field


# --- Control plane (Fleet / Realm / Membership) ---


class RealmRole(StrEnum):
    VIEWER = "viewer"
    EDITOR = "editor"
    ADMIN = "admin"
    RELAY = "relay"


class PrincipalType(StrEnum):
    USER = "user"
    INSTANCE = "instance"
    FLEET = "fleet"


class Principal(BaseModel):
    type: PrincipalType
    id: str

    @classmethod
    def user(cls, user_id: str) -> Principal:
        return cls(type=PrincipalType.USER, id=user_id)

    @classmethod
    def instance(cls, instance_id: str) -> Principal:
        return cls(type=PrincipalType.INSTANCE, id=instance_id)

    def key(self) -> str:
        return f"{self.type.value}:{self.id}"


class Fleet(BaseModel):
    id: str
    owner_principal: str
    name: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Realm(BaseModel):
    id: str
    name: str = ""
    description: str = ""


class Membership(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str
    principal_type: PrincipalType
    principal_id: str
    role: RealmRole = RealmRole.EDITOR
    fleet_id: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class RealmInvite(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str
    role: RealmRole = RealmRole.EDITOR
    token: str
    expires_at: datetime | None = None
    created_by: str = ""
    accepted: bool = False


class RealmGrant(BaseModel):
    """Cross-realm card sharing grant (Phase 5)."""

    id: str = Field(default_factory=lambda: str(uuid4()))
    source_realm_id: str
    target_realm_id: str | None = None
    target_principal: str | None = None
    card_id: str | None = None
    tag_filter: str | None = None
    permissions: str = "read"
    expires_at: datetime | None = None


class PeerRouteMode(StrEnum):
    DIRECT = "direct"
    RELAY = "relay"


class PeerRoute(BaseModel):
    realm_id: str
    target_url: str
    target_instance_id: str | None = None
    zone: str = "default"
    mode: PeerRouteMode = PeerRouteMode.DIRECT
    relay_instance_id: str | None = None


class FleetInstance(BaseModel):
    instance_id: str
    name: str
    url: str
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    relay_enabled: bool = False
    last_seen: datetime | None = None
    healthy: bool = False


class FleetJoinToken(BaseModel):
    token: str
    fleet_id: str
    expires_at: datetime
    created_by: str = ""


# --- Projects (card containers) ---


class ProjectStatus(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class ProjectRepo(BaseModel):
    url: str
    branch: str | None = None
    path: str | None = None


class ProjectMembership(BaseModel):
    principal_id: str
    role: str = "editor"


class Project(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    title: str
    description: str = ""
    status: ProjectStatus = ProjectStatus.ACTIVE
    memberships: list[ProjectMembership] = Field(default_factory=list)
    repos: list[ProjectRepo] = Field(default_factory=list)
    agent_prompt: str = ""
    tool_config: dict = Field(default_factory=dict)
    tags: list[str] = Field(default_factory=list)
    created_by_principal: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class ProjectCreate(BaseModel):
    realm_id: str = "default"
    title: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    repos: list[ProjectRepo] = Field(default_factory=list)
    agent_prompt: str = ""
    tool_config: dict = Field(default_factory=dict)


class ProjectUpdate(BaseModel):
    title: str | None = None
    description: str | None = None
    status: ProjectStatus | None = None
    tags: list[str] | None = None
    repos: list[ProjectRepo] | None = None
    agent_prompt: str | None = None
    tool_config: dict | None = None
    memberships: list[ProjectMembership] | None = None


# --- Cards (data plane) ---


class CardLane(StrEnum):
    INBOX = "inbox"
    ACTIVE = "active"
    WAITING = "waiting"
    DONE = "done"


class CardKind(StrEnum):
    GOAL = "goal"
    TASK = "task"
    PROJECT = "project"
    CONCERN = "concern"


class Card(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    kind: CardKind = CardKind.TASK
    title: str
    body: str = ""
    lane: CardLane = CardLane.INBOX
    parent_id: str | None = None
    project_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    visibility: str = "realm"
    owner_principal: str | None = None
    preferred_instance: str | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)
    lease_holder_instance: str | None = None
    lease_holder_principal: str | None = None
    lease_expires_at: datetime | None = None
    created_by_principal: str | None = None
    created_by_instance: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class CardCreate(BaseModel):
    realm_id: str = "default"
    kind: CardKind = CardKind.TASK
    title: str
    body: str = ""
    lane: CardLane = CardLane.INBOX
    parent_id: str | None = None
    project_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    preferred_instance: str | None = None
    preferred_capabilities: list[str] = Field(default_factory=list)


class CardUpdate(BaseModel):
    title: str | None = None
    body: str | None = None
    lane: CardLane | None = None
    parent_id: str | None = None
    project_id: str | None = None
    tags: list[str] | None = None
    preferred_instance: str | None = None
    preferred_capabilities: list[str] | None = None


# --- Sync objects ---


class EventType(StrEnum):
    CARD_CREATED = "card_created"
    CARD_UPDATED = "card_updated"
    CARD_DELETED = "card_deleted"
    PROJECT_CREATED = "project_created"
    PROJECT_UPDATED = "project_updated"
    PROJECT_ARCHIVED = "project_archived"
    LEASE_GRANTED = "lease_granted"
    LEASE_RELEASED = "lease_released"
    AGENT_PROGRESS = "agent_progress"
    REALM_GRANT = "realm_grant"


class CardEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    type: EventType
    realm_id: str
    card_id: str | None = None
    project_id: str | None = None
    author_principal: str
    author_instance: str
    payload: dict = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SyncCommit(BaseModel):
    hash: str
    realm_id: str
    instance_id: str
    parent_hashes: list[str] = Field(default_factory=list)
    event_hashes: list[str] = Field(default_factory=list)
    author_principal: str = ""
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    signature: str | None = None


class SyncRef(BaseModel):
    realm_id: str
    instance_id: str
    head_hash: str


# --- Legacy Item aliases (backward compatibility) ---


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


_STATUS_TO_LANE = {
    ItemStatus.OPEN: CardLane.INBOX,
    ItemStatus.ACTIVE: CardLane.ACTIVE,
    ItemStatus.BLOCKED: CardLane.WAITING,
    ItemStatus.DONE: CardLane.DONE,
    ItemStatus.ARCHIVED: CardLane.DONE,
}

_LANE_TO_STATUS = {
    CardLane.INBOX: ItemStatus.OPEN,
    CardLane.ACTIVE: ItemStatus.ACTIVE,
    CardLane.WAITING: ItemStatus.BLOCKED,
    CardLane.DONE: ItemStatus.DONE,
}


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

    @classmethod
    def from_card(cls, card: Card) -> Item:
        return cls(
            id=card.id,
            kind=ItemKind(card.kind.value),
            title=card.title,
            body=card.body,
            status=_LANE_TO_STATUS.get(card.lane, ItemStatus.OPEN),
            parent_id=card.parent_id,
            tags=card.tags,
            created_at=card.created_at,
            updated_at=card.updated_at,
        )


class ItemCreate(BaseModel):
    kind: ItemKind
    title: str
    body: str = ""
    status: ItemStatus = ItemStatus.OPEN
    parent_id: str | None = None
    tags: list[str] = Field(default_factory=list)

    def to_card_create(self, realm_id: str = "default") -> CardCreate:
        return CardCreate(
            realm_id=realm_id,
            kind=CardKind(self.kind.value),
            title=self.title,
            body=self.body,
            lane=_STATUS_TO_LANE.get(self.status, CardLane.INBOX),
            parent_id=self.parent_id,
            tags=self.tags,
        )


class ItemUpdate(BaseModel):
    title: str | None = None
    body: str | None = None
    status: ItemStatus | None = None
    parent_id: str | None = None
    tags: list[str] | None = None

    def to_card_update(self) -> CardUpdate:
        lane = _STATUS_TO_LANE.get(self.status) if self.status else None
        return CardUpdate(
            title=self.title,
            body=self.body,
            lane=lane,
            parent_id=self.parent_id,
            tags=self.tags,
        )


class AgentSession(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    agent_name: str
    external_session_id: str | None = None
    item_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    principal_id: str | None = None
    status: str = "idle"
    cwd: str | None = None
    title: str | None = None
    label: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    config_json: dict = Field(default_factory=dict)
    metrics_json: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class TranscriptEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str
    seq: int
    event_type: str
    payload: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class KnowledgeEntry(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    session_id: str | None = None
    item_id: str | None = None
    card_id: str | None = None
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
    fleet_id: str = ""
    subscribed_realms: list[str] = Field(default_factory=list)
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    relay_enabled: bool = False
    agent_enabled: bool = True
    sync_head: dict[str, str] = Field(default_factory=dict)
