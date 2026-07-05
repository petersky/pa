"""Persistent instance configuration (config.json)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field


class InstanceConfig(BaseModel):
    instance_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_name: str = "local"
    data_dir: str = ""
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    subscribed_realms: list[str] = Field(default_factory=lambda: ["default"])
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    relay_enabled: bool = False


def config_path(data_dir: Path) -> Path:
    return data_dir / "config.json"


def load_instance_config(data_dir: Path) -> InstanceConfig | None:
    path = config_path(data_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return InstanceConfig.model_validate(data)
    except (json.JSONDecodeError, ValueError):
        return None


def save_instance_config(data_dir: Path, config: InstanceConfig) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = config_path(data_dir)
    path.write_text(json.dumps(config.model_dump(), indent=2) + "\n")
    return path


def merge_config_into_settings(data_dir: Path, settings_dict: dict) -> dict:
    """Overlay config.json values onto settings kwargs."""
    loaded = load_instance_config(data_dir)
    if not loaded:
        return settings_dict
    mapping = {
        "instance_id": loaded.instance_id,
        "instance_name": loaded.instance_name,
        "fleet_id": loaded.fleet_id,
        "fleet_owner": loaded.fleet_owner,
        "subscribed_realms": loaded.subscribed_realms,
        "zone": loaded.zone,
        "capabilities": loaded.capabilities,
        "relay_enabled": loaded.relay_enabled,
    }
    for key, value in mapping.items():
        if key not in settings_dict or settings_dict.get(key) in (None, "", []):
            settings_dict[key] = value
    return settings_dict
