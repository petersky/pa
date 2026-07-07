"""Environment variables embedded in host service units."""

from __future__ import annotations

import json

from pa.config import Settings


def _env_list(values: list[str]) -> str:
    return json.dumps(values)


def service_environment(settings: Settings) -> dict[str, str]:
    env: dict[str, str] = {
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_HOST": settings.host,
        "PA_PORT": str(settings.port),
        "PA_INSTANCE_NAME": settings.instance_name,
        "PA_RELEASE_TRACK": settings.release_track,
        "PA_FLEET_ID": settings.fleet_id,
        "PA_ZONE": settings.zone,
    }
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
