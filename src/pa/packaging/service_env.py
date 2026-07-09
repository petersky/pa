"""Environment variables embedded in host service units."""

from __future__ import annotations

import json

from pa.config import Settings
from pa.packaging.paths import build_service_path, resolve_executable


def _env_list(values: list[str]) -> str:
    return json.dumps(values)


def service_environment(settings: Settings) -> dict[str, str]:
    service_path = build_service_path()
    env: dict[str, str] = {
        "PATH": service_path,
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_HOST": settings.host,
        "PA_PORT": str(settings.port),
        "PA_INSTANCE_NAME": settings.instance_name,
        "PA_RELEASE_TRACK": settings.release_track,
        "PA_FLEET_ID": settings.fleet_id,
        "PA_ZONE": settings.zone,
    }
    agent_bin = resolve_executable(settings.agent_command, path=service_path)
    if agent_bin:
        env["PA_AGENT_COMMAND"] = str(agent_bin)
    if settings.agent_args:
        env["PA_AGENT_ARGS"] = _env_list(settings.agent_args)
    if settings.subscribed_realms:
        env["PA_SUBSCRIBED_REALMS"] = _env_list(settings.subscribed_realms)
    if settings.peers:
        env["PA_PEERS"] = _env_list(settings.peers)
    if settings.capabilities:
        env["PA_CAPABILITIES"] = _env_list(settings.capabilities)
    if settings.sync_token:
        env["PA_SYNC_TOKEN"] = settings.sync_token
    if settings.instance_url:
        env["PA_INSTANCE_URL"] = settings.instance_url
    if settings.fleet_owner_url:
        env["PA_FLEET_OWNER_URL"] = settings.fleet_owner_url
    if settings.relay_enabled:
        env["PA_RELAY_ENABLED"] = "true"
    if not settings.agent_enabled:
        env["PA_AGENT_ENABLED"] = "false"
    return env
