"""Install metadata persisted on the host."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from importlib.metadata import PackageNotFoundError, distribution
from pathlib import Path

from pydantic import BaseModel, Field

from pa import __version__
from pa.core.io import atomic_write_json


class InstallMetadata(BaseModel):
    version: str = __version__
    installed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    method: str = "uv-tool"
    channel: str = "release"
    pa_bin: str | None = None
    source_revision: str | None = None


def install_metadata_path(data_dir: Path) -> Path:
    return data_dir / "install.json"


def load_install_metadata(data_dir: Path) -> InstallMetadata | None:
    path = install_metadata_path(data_dir)
    if not path.exists():
        return None
    try:
        return InstallMetadata.model_validate_json(path.read_text())
    except json.JSONDecodeError, ValueError:
        return None


def save_install_metadata(data_dir: Path, metadata: InstallMetadata) -> InstallMetadata:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = install_metadata_path(data_dir)
    atomic_write_json(path, metadata.model_dump(mode="json"))
    return metadata


def running_source_revision() -> str | None:
    """Return the immutable VCS revision backing the running PA distribution."""
    try:
        direct_url = distribution("pa").read_text("direct_url.json")
        payload = json.loads(direct_url) if direct_url else {}
    except (PackageNotFoundError, json.JSONDecodeError, TypeError, ValueError):
        return None
    vcs_info = payload.get("vcs_info") if isinstance(payload, dict) else None
    if not isinstance(vcs_info, dict):
        return None
    revision = vcs_info.get("commit_id")
    return str(revision).lower() if revision else None
