"""Persistent instance configuration (config.json)."""

from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json


class InstanceConfig(BaseModel):
    instance_id: str = Field(default_factory=lambda: str(uuid4()))
    instance_name: str = "local"
    data_dir: str = ""
    fleet_id: str = Field(default_factory=lambda: str(uuid4()))
    fleet_owner: str = "local"
    fleet_owner_url: str = ""
    instance_url: str = ""
    host: str = ""
    subscribed_realms: list[str] = Field(default_factory=lambda: ["default"])
    zone: str = "default"
    capabilities: list[str] = Field(default_factory=list)
    relay_enabled: bool = False
    peers: list[str] = Field(default_factory=list)
    release_track: str = "release"
    sync_token: str = ""
    session_secret: str = ""
    agent_provider: str = "cursor"
    agent_command: str | None = None
    agent_args: list[str] | None = None


def config_path(data_dir: Path) -> Path:
    return data_dir / "config.json"


def load_instance_config(data_dir: Path) -> InstanceConfig | None:
    path = config_path(data_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return InstanceConfig.model_validate(data)
    except json.JSONDecodeError, ValueError:
        return None


def save_instance_config(data_dir: Path, config: InstanceConfig) -> Path:
    data_dir.mkdir(parents=True, exist_ok=True)
    path = config_path(data_dir)
    # config.json contains the fleet sync token and session secret.
    atomic_write_json(path, config.model_dump(), mode=0o600)
    return path


def ensure_session_secret(data_dir: Path) -> str:
    """Return a stable session secret, persisting to config.json if needed."""
    config = load_instance_config(data_dir)
    if config and config.session_secret:
        return config.session_secret
    secret = str(uuid4())
    update_instance_config(data_dir, session_secret=secret)
    return secret


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
        "fleet_owner_url": loaded.fleet_owner_url,
        "instance_url": loaded.instance_url,
        "subscribed_realms": loaded.subscribed_realms,
        "zone": loaded.zone,
        "capabilities": loaded.capabilities,
        "relay_enabled": loaded.relay_enabled,
        "peers": loaded.peers,
        "release_track": loaded.release_track,
        "sync_token": loaded.sync_token,
        "session_secret": loaded.session_secret,
        "agent_provider": loaded.agent_provider,
        "agent_command": loaded.agent_command,
        "agent_args": loaded.agent_args,
    }
    if loaded.host:
        mapping["host"] = loaded.host
    for key, value in mapping.items():
        if key not in settings_dict or settings_dict.get(key) in (None, "", []):
            settings_dict[key] = value
    if loaded.session_secret:
        settings_dict["session_secret"] = loaded.session_secret
    return settings_dict


def update_instance_config(data_dir: Path, **updates: object) -> InstanceConfig:
    """Merge updates into config.json and return the result."""
    config = load_instance_config(data_dir) or InstanceConfig(data_dir=str(data_dir))
    data = config.model_dump()
    for key, value in updates.items():
        if value is not None:
            data[key] = value
    updated = InstanceConfig.model_validate(data)
    save_instance_config(data_dir, updated)
    return updated
