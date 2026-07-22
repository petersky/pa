"""Persist non-secret ACP provider install/config metadata under data_dir."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json


class ProviderMetadata(BaseModel):
    provider_id: str
    install_method: str | None = None
    version: str | None = None
    command: str | None = None
    configured: bool = False
    env: dict[str, str] = Field(default_factory=dict)
    configuration: dict[str, Any] = Field(default_factory=dict)
    last_probe_at: str | None = None
    last_probe: dict[str, Any] | None = None
    updated_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


def providers_dir(data_dir: Path) -> Path:
    path = data_dir / "agent_providers"
    path.mkdir(parents=True, exist_ok=True)
    return path


def metadata_path(data_dir: Path, provider_id: str) -> Path:
    return providers_dir(data_dir) / f"{provider_id}.json"


def credentials_path(data_dir: Path, provider_id: str) -> Path:
    """Secrets for a provider — not synced across the fleet."""
    path = data_dir / "integrations"
    path.mkdir(parents=True, exist_ok=True)
    return path / f"{provider_id}.json"


def load_metadata(data_dir: Path, provider_id: str) -> ProviderMetadata | None:
    path = metadata_path(data_dir, provider_id)
    if not path.exists():
        return None
    try:
        return ProviderMetadata.model_validate(json.loads(path.read_text()))
    except json.JSONDecodeError, ValueError:
        return None


def save_metadata(data_dir: Path, meta: ProviderMetadata) -> Path:
    meta.updated_at = datetime.now(UTC).isoformat()
    path = metadata_path(data_dir, meta.provider_id)
    atomic_write_json(path, meta.model_dump())
    return path


def load_credentials(data_dir: Path, provider_id: str) -> dict[str, str]:
    path = credentials_path(data_dir, provider_id)
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text())
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items() if v is not None}
    except json.JSONDecodeError, ValueError, TypeError:
        pass
    return {}


def save_credentials(data_dir: Path, provider_id: str, secrets: dict[str, str]) -> Path:
    path = credentials_path(data_dir, provider_id)
    existing = load_credentials(data_dir, provider_id)
    existing.update({k: v for k, v in secrets.items() if v})
    atomic_write_json(path, existing, mode=0o600)
    return path


def merge_provider_env(data_dir: Path, provider_id: str) -> dict[str, str]:
    """Non-secret meta.env + credentials for subprocess overlay."""
    env: dict[str, str] = {}
    meta = load_metadata(data_dir, provider_id)
    if meta:
        env.update(meta.env)
    env.update(load_credentials(data_dir, provider_id))
    return env
