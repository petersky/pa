from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

BACKUP_FORMAT_VERSION = 1


def validate_backup_destination_path(data_dir: Path, destination: Path) -> Path:
    data_dir = data_dir.expanduser().resolve()
    database = (data_dir / "pa.db").resolve()
    destination = destination.expanduser().resolve()
    if destination == data_dir or destination == database:
        raise ValueError(
            "backup destination cannot be the live PA data directory or database"
        )
    if destination in data_dir.parents:
        raise ValueError("backup destination cannot contain the live PA data directory")
    if data_dir in destination.parents:
        raise ValueError(
            "backup destination cannot be inside the live PA data directory"
        )
    return destination


class BackupConfig(BaseModel):
    enabled: bool = True
    interval_seconds: int = Field(default=6 * 60 * 60, ge=60, le=31 * 24 * 60 * 60)
    retention_count: int = Field(default=8, ge=1, le=10_000)
    retention_max_age_seconds: int | None = Field(
        default=None, ge=60, le=10 * 365 * 24 * 60 * 60
    )
    retention_max_total_bytes: int | None = Field(default=None, ge=1024)
    destination_dir: Path
    run_on_startup: bool = False
    startup_min_age_seconds: int = Field(default=60 * 60, ge=0, le=31 * 24 * 60 * 60)
    verification_level: Literal["quick", "full"] = "full"
    compression: bool = True
    io_limit_mib_per_second: float | None = Field(default=None, gt=0, le=10_000)
    concurrency: Literal[1] = 1
    alert_after_failures: int = Field(default=3, ge=1, le=1000)
    jitter_seconds: int = Field(default=300, ge=0, le=60 * 60)

    @field_validator("destination_dir")
    @classmethod
    def _expand_destination(cls, value: Path) -> Path:
        return value.expanduser().resolve()


class ManifestFile(BaseModel):
    path: str
    size_bytes: int = Field(ge=0)
    sha256: str


class BackupManifest(BaseModel):
    format: Literal["pa.metadata-backup/v1"] = "pa.metadata-backup/v1"
    format_version: Literal[1] = BACKUP_FORMAT_VERSION
    backup_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_id: str
    instance_name: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    pa_version: str
    projection_schema_version: int = Field(ge=0)
    projection_schema_fingerprint: str
    verification_level: Literal["quick", "full"]
    compressed: bool
    durable_heads: dict[str, str] = Field(default_factory=dict)
    projection_heads: dict[str, str] = Field(default_factory=dict)
    consistent: bool
    files: list[ManifestFile] = Field(default_factory=list)
    included: list[str] = Field(
        default_factory=lambda: [
            "projection_database_online_snapshot",
            "sync_refs",
            "event_log_objects",
        ]
    )
    excluded: dict[str, str] = Field(
        default_factory=lambda: {
            "instance_configuration": "excluded; recreate or preserve target config",
            "secrets": "excluded",
            "attachments": "excluded; recovered through normal attachment sync",
            "repositories": "excluded; instance-local external state",
            "worktrees": "excluded; instance-local external state",
            "pr_supervisor": "excluded; independently rebuildable control-plane state",
            "telemetry_and_logs": "excluded",
            "transient_wal_shm": "excluded; never treated as durable files",
            "caches_and_runtime_state": "excluded",
        }
    )

    @model_validator(mode="after")
    def _validate_consistency_claim(self) -> BackupManifest:
        expected = self.projection_heads == self.durable_heads
        if self.consistent != expected:
            raise ValueError("manifest consistency claim does not match recorded heads")
        return self


class BackupRun(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    trigger: Literal["scheduled", "startup", "manual", "pre_restore"]
    status: Literal["running", "success", "failed", "skipped"] = "running"
    backup_id: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
    duration_seconds: float | None = None
    size_bytes: int | None = None
    failure_reason: str | None = None
    idempotency_key: str | None = None
    verification: Literal["pending", "verified", "failed"] = "pending"
    pruned_backup_ids: list[str] = Field(default_factory=list)


class RestoreRequest(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    backup_id: str
    instance_id: str
    requested_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    requested_by: str
    status: Literal[
        "maintenance_required",
        "running",
        "success",
        "failed",
        "reconciliation_required",
    ] = "maintenance_required"
    finished_at: datetime | None = None
    failure_reason: str | None = None
    pre_restore_backup_id: str | None = None
    reconciliation_realms: list[str] = Field(default_factory=list)
    instructions: list[str] = Field(default_factory=list)


class BackupState(BaseModel):
    version: Literal[1] = 1
    next_scheduled_run: datetime | None = None
    last_attempt: datetime | None = None
    last_success: datetime | None = None
    consecutive_failures: int = 0
    runs: list[BackupRun] = Field(default_factory=list)
    idempotency: dict[str, str] = Field(default_factory=dict)
    restores: list[RestoreRequest] = Field(default_factory=list)
    verifications: dict[str, dict[str, Any]] = Field(default_factory=dict)
    metrics: dict[str, int | float] = Field(default_factory=dict)


class BackupRecord(BaseModel):
    backup_id: str
    path: Path
    created_at: datetime
    size_bytes: int
    verified: bool
    verification_error: str | None = None
    manifest: BackupManifest | None = None

    def public_dict(self, *, include_path: bool = False) -> dict[str, Any]:
        data = self.model_dump(mode="json")
        if not include_path:
            data.pop("path", None)
        return data
