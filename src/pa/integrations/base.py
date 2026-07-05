"""External system integration contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable
from uuid import uuid4

from pydantic import BaseModel, Field


class SyncDirection(StrEnum):
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    BIDIRECTIONAL = "bidirectional"


class ExternalSystem(StrEnum):
    GITHUB_ISSUES = "github_issues"
    NOTION = "notion"
    JIRA = "jira"


class ExternalRef(BaseModel):
    system: ExternalSystem
    external_id: str
    url: str | None = None
    last_synced_at: datetime | None = None


class SyncBinding(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str
    pa_type: str  # card | project
    pa_id: str
    external_ref: ExternalRef
    direction: SyncDirection = SyncDirection.BIDIRECTIONAL
    field_map: dict = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


@runtime_checkable
class Connector(Protocol):
    system: ExternalSystem

    def configure(self, config: dict) -> None: ...

    async def pull(self, binding: SyncBinding) -> dict: ...

    async def push(self, binding: SyncBinding, pa_snapshot: dict) -> ExternalRef: ...
