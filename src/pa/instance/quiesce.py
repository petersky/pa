"""ACP session quiesce snapshots and prompt queue persistence."""

from __future__ import annotations

import base64
import binascii
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator

from pa.core.io import atomic_write_json


MAX_PROMPT_IMAGES = 4
MAX_IMAGE_BYTES = 10 * 1024 * 1024
MAX_TOTAL_IMAGE_BYTES = 20 * 1024 * 1024
SUPPORTED_IMAGE_TYPES = {"image/png", "image/jpeg", "image/gif", "image/webp"}


class ImageAttachment(BaseModel):
    name: str = Field(default="image", max_length=255)
    mime_type: str
    data: str

    @field_validator("mime_type")
    @classmethod
    def validate_mime_type(cls, value: str) -> str:
        normalized = value.lower().strip()
        if normalized not in SUPPORTED_IMAGE_TYPES:
            raise ValueError("unsupported image type")
        return normalized

    @field_validator("data")
    @classmethod
    def validate_data(cls, value: str) -> str:
        try:
            decoded = base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise ValueError("invalid base64 image data") from exc
        if not decoded:
            raise ValueError("image is empty")
        if len(decoded) > MAX_IMAGE_BYTES:
            raise ValueError("image exceeds 10 MB limit")
        return value

    @property
    def decoded_size(self) -> int:
        return len(base64.b64decode(self.data))

    def public_dict(self) -> dict[str, str]:
        return {"name": self.name, "mime_type": self.mime_type}


class QueuedPrompt(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    message: str
    images: list[ImageAttachment] = Field(default_factory=list, max_length=MAX_PROMPT_IMAGES)
    session_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    principal_id: str | None = None
    cwd: str | None = None
    agent_env: dict[str, str] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    source: str = "api"

    @model_validator(mode="after")
    def validate_total_image_size(self) -> QueuedPrompt:
        if sum(image.decoded_size for image in self.images) > MAX_TOTAL_IMAGE_BYTES:
            raise ValueError("images exceed 20 MB combined limit")
        return self

    def public_dict(self) -> dict[str, Any]:
        data = self.model_dump(mode="json", exclude={"images"})
        data["images"] = [image.public_dict() for image in self.images]
        return data


class SessionSnapshot(BaseModel):
    session_id: str | None = None
    external_session_id: str | None = None
    agent_name: str = "instance"
    status: str = "idle"
    cwd: str | None = None
    title: str | None = None
    label: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    principal_id: str | None = None
    prompting: bool = False
    queue_paused: bool = False
    queued_prompts: list[QueuedPrompt] = Field(default_factory=list)
    in_flight: QueuedPrompt | None = None


class QuiesceSnapshot(BaseModel):
    version: int = 1
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    reason: str = "restart"
    resume: bool = True
    sessions: list[SessionSnapshot] = Field(default_factory=list)
    queued_prompts: list[QueuedPrompt] = Field(default_factory=list)

    @property
    def active_count(self) -> int:
        return len(self.sessions)

    @property
    def prompting_count(self) -> int:
        return sum(1 for s in self.sessions if s.prompting or s.status == "prompting")

    @property
    def queued_count(self) -> int:
        per_session = sum(len(s.queued_prompts) for s in self.sessions)
        return per_session + len(self.queued_prompts)


def quiesce_path(data_dir: Path) -> Path:
    return data_dir / "agent_quiesce.json"


def skip_quiesce_path(data_dir: Path) -> Path:
    return data_dir / "agent_skip_quiesce"


def request_skip_quiesce(data_dir: Path) -> None:
    data_dir.mkdir(parents=True, exist_ok=True)
    skip_quiesce_path(data_dir).write_text("1\n")


def consume_skip_quiesce(data_dir: Path) -> bool:
    path = skip_quiesce_path(data_dir)
    if not path.exists():
        return False
    try:
        path.unlink()
    except OSError:
        pass
    return True


def load_quiesce_snapshot(data_dir: Path) -> QuiesceSnapshot | None:
    path = quiesce_path(data_dir)
    if not path.exists():
        return None
    try:
        return QuiesceSnapshot.model_validate_json(path.read_text())
    except (json.JSONDecodeError, ValueError):
        return None


def save_quiesce_snapshot(data_dir: Path, snapshot: QuiesceSnapshot) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = quiesce_path(data_dir)
    atomic_write_json(path, snapshot.model_dump(mode="json"))
    return path


def clear_quiesce_snapshot(data_dir: Path) -> None:
    path = quiesce_path(data_dir)
    if path.exists():
        path.unlink()


def mark_snapshot_no_resume(data_dir: Path) -> None:
    """Discard any pending quiesce snapshot so startup will not resume ACP state."""
    clear_quiesce_snapshot(data_dir)


class QuiesceProgress(BaseModel):
    phase: str = "idle"
    connected: bool = False
    prompting: bool = False
    quiescing: bool = False
    active_sessions: int = 0
    queued_prompts: int = 0
    message: str = ""
    done: bool = False
    error: str | None = None
    snapshot: dict[str, Any] | None = None
