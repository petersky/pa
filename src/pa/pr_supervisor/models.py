from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


def utcnow() -> datetime:
    return datetime.now(UTC)


class PRPolicy(BaseModel):
    """Project/repository policy copied onto a watch at association time."""

    ready_by_default: bool = True
    auto_notify: bool = True
    agent_merge_on_green: bool = True
    integration_branch: str | None = None
    required_checks: list[str] = Field(default_factory=list)
    allowed_neutral_conclusions: list[str] = Field(
        default_factory=lambda: ["neutral", "skipped"]
    )
    required_approvals: int | None = None
    stable_head_seconds: int = Field(default=15, ge=0, le=3600)
    stable_observations: int = Field(default=2, ge=1, le=20)
    poll_min_seconds: int = Field(default=15, ge=1, le=3600)
    poll_max_seconds: int = Field(default=300, ge=1, le=86400)

    @field_validator("poll_max_seconds")
    @classmethod
    def max_not_below_min(cls, value: int, info) -> int:
        minimum = (info.data or {}).get("poll_min_seconds", 15)
        if value < minimum:
            raise ValueError("poll_max_seconds must be >= poll_min_seconds")
        return value


class PRWatchStatus(StrEnum):
    ACTIVE = "active"
    BLOCKED = "blocked"
    MERGED = "merged"
    CLOSED = "closed"
    RETIRED = "retired"


class PRCheck(BaseModel):
    name: str
    status: str = "queued"
    conclusion: str | None = None
    required: bool = False
    details_url: str | None = None
    title: str | None = None
    summary: str | None = None
    text: str | None = None

    @property
    def terminal(self) -> bool:
        return self.status.lower() == "completed" or self.conclusion is not None


class ReviewThread(BaseModel):
    id: str
    resolved: bool = False
    outdated: bool = False
    path: str | None = None
    line: int | None = None
    url: str | None = None
    author: str | None = None
    body: str = ""

    @property
    def actionable(self) -> bool:
        return not self.resolved and not self.outdated


class PRSnapshot(BaseModel):
    repository: str
    number: int
    url: str
    state: str
    draft: bool
    head_sha: str
    confirmed_head_sha: str | None = None
    base_branch: str
    title: str = ""
    mergeable: bool | None = None
    mergeable_state: str | None = None
    merge_commit_sha: str | None = None
    review_decision: str | None = None
    approvals: int = 0
    required_approvals: int = 0
    branch_protection_known: bool = True
    required_checks_known: bool = True
    review_threads_known: bool = True
    checks: list[PRCheck] = Field(default_factory=list)
    review_threads: list[ReviewThread] = Field(default_factory=list)
    observed_at: datetime = Field(default_factory=utcnow)
    raw_urls: dict[str, str] = Field(default_factory=dict)

    @property
    def stale(self) -> bool:
        return bool(self.confirmed_head_sha and self.confirmed_head_sha != self.head_sha)

    @property
    def merged(self) -> bool:
        return self.state.lower() == "merged" or bool(self.merge_commit_sha)

    @property
    def closed(self) -> bool:
        return self.state.lower() == "closed" and not self.merged


class PRWatch(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    project_id: str | None = None
    card_id: str | None = None
    repository: str
    pr_number: int = Field(gt=0)
    pr_url: str
    base_branch: str | None = None
    head_sha: str | None = None
    originating_instance_id: str | None = None
    originating_session_id: str | None = None
    originating_agent: str | None = None
    executor_cwd: str | None = None
    policy: PRPolicy = Field(default_factory=PRPolicy)
    required_capabilities: list[str] = Field(default_factory=list)
    status: PRWatchStatus = PRWatchStatus.ACTIVE
    owner_instance_id: str | None = None
    fence_token: int = 0
    lease_expires_at: datetime | None = None
    state: dict[str, Any] = Field(default_factory=dict)
    condition_fingerprint: str | None = None
    condition_version: int = 0
    stable_head_since: datetime | None = None
    stable_head_observations: int = 0
    next_poll_at: datetime = Field(default_factory=utcnow)
    poll_attempt: int = 0
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)
    retired_at: datetime | None = None

    @field_validator("repository")
    @classmethod
    def normalize_repository(cls, value: str) -> str:
        normalized = value.strip().strip("/")
        if normalized.endswith(".git"):
            normalized = normalized[:-4]
        if normalized.startswith("https://github.com/"):
            normalized = normalized.removeprefix("https://github.com/")
        if len(normalized.split("/")) != 2:
            raise ValueError("repository must be owner/name")
        return normalized


class PRWatchEvent(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    watch_id: str
    event_key: str
    event_type: str
    head_sha: str | None = None
    condition_fingerprint: str | None = None
    source: str = "supervisor"
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=utcnow)


class GitHubCapability(BaseModel):
    instance_id: str
    authenticated: bool = False
    webhook_configured: bool = False
    token_source: str | None = None
    allowed_repositories: list[str] = Field(default_factory=list)
    capabilities: list[str] = Field(default_factory=list)
    state: str = "unauthenticated"
    detail: str | None = None
    checked_at: datetime = Field(default_factory=utcnow)

    def supports(self, repository: str) -> bool:
        if not self.authenticated:
            return False
        if self.allowed_repositories and repository not in self.allowed_repositories:
            return False
        return True


class GateResult(BaseModel):
    green: bool = False
    actionable: bool = False
    pending: bool = False
    reasons: list[str] = Field(default_factory=list)
    failing_checks: list[PRCheck] = Field(default_factory=list)
    pending_checks: list[PRCheck] = Field(default_factory=list)
    unresolved_threads: list[ReviewThread] = Field(default_factory=list)
    fingerprint: str


class LeaseGrant(BaseModel):
    acquired: bool
    owner_instance_id: str | None = None
    fence_token: int = 0
    expires_at: datetime | None = None
    reason: str | None = None
