"""Environment variables embedded in host service units."""

from __future__ import annotations

import json
import shlex
from urllib.parse import urlsplit, urlunsplit

from pa.config import Settings
from pa.packaging.paths import build_service_path, resolve_executable


def _env_list(values: list[str]) -> str:
    return json.dumps(values)


_UNORDERED_LIST_ENV = {
    "PA_CAPABILITIES",
    "PA_PEERS",
    "PA_SUBSCRIBED_REALMS",
    "PA_WEB_LISTENERS",
}


def _parse_env_list(value: str) -> list[str] | None:
    try:
        parsed = json.loads(value)
    except TypeError, ValueError:
        parsed = None
    if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed):
        return parsed
    try:
        return [item for item in shlex.split(value.replace(",", " ")) if item]
    except ValueError:
        return None


def _canonical_url(value: str) -> str:
    try:
        parts = urlsplit(value.strip())
    except ValueError:
        return value.strip()
    if not parts.scheme or not parts.netloc:
        return value.strip()
    host = (parts.hostname or "").lower()
    port = parts.port
    default_port = (parts.scheme.lower() == "http" and port == 80) or (
        parts.scheme.lower() == "https" and port == 443
    )
    netloc = host if not port or default_port else f"{host}:{port}"
    return urlunsplit(
        (
            parts.scheme.lower(),
            netloc,
            parts.path.rstrip("/"),
            parts.query,
            parts.fragment,
        )
    )


def service_values_equal(name: str, expected: str, actual: str | None) -> bool:
    """Compare service values after manager-independent canonicalization."""
    if actual is None:
        return False
    if name not in _UNORDERED_LIST_ENV:
        return actual == expected
    expected_items = _parse_env_list(expected)
    actual_items = _parse_env_list(actual)
    if expected_items is None or actual_items is None:
        return actual == expected
    canonical = _canonical_url if name in {"PA_PEERS"} else str.strip
    return sorted({canonical(item) for item in expected_items}) == sorted(
        {canonical(item) for item in actual_items}
    )


def service_environment(settings: Settings) -> dict[str, str]:
    service_path = build_service_path()
    env: dict[str, str] = {
        "PATH": service_path,
        "PA_DATA_DIR": str(settings.data_dir),
        "PA_HOST": settings.host,
        "PA_PORT": str(settings.port),
        "PA_WEB_LISTENERS": _env_list(settings.web_listeners),
        "PA_INSTANCE_NAME": settings.instance_name,
        "PA_RELEASE_TRACK": settings.release_track,
        "PA_FLEET_ID": settings.fleet_id,
        "PA_ZONE": settings.zone,
        "PA_LOG_ROTATION_MAX_BYTES": str(settings.log_rotation_max_bytes),
        "PA_LOG_ROTATION_INTERVAL_SECONDS": str(settings.log_rotation_interval_seconds),
        "PA_LOG_RETENTION_COUNT": str(settings.log_retention_count),
        "PA_LOG_RETENTION_MAX_AGE_SECONDS": str(settings.log_retention_max_age_seconds),
        "PA_LOG_RETENTION_MAX_TOTAL_BYTES": str(settings.log_retention_max_total_bytes),
        "PA_LOG_DISK_PRESSURE_FREE_BYTES": str(settings.log_disk_pressure_free_bytes),
    }
    if settings.agent_provider:
        env["PA_AGENT_PROVIDER"] = settings.agent_provider
    if settings.agent_command:
        agent_bin = resolve_executable(settings.agent_command, path=service_path)
        env["PA_AGENT_COMMAND"] = (
            str(agent_bin) if agent_bin else settings.agent_command
        )
    if settings.agent_args is not None:
        env["PA_AGENT_ARGS"] = _env_list(settings.agent_args)
    # Realms and peers are mutable, config.json-authoritative fleet state.  Do
    # not duplicate them in a long-lived service-manager environment where
    # stale values override config and list quoting varies by manager.
    if settings.capabilities:
        env["PA_CAPABILITIES"] = _env_list(settings.capabilities)
    # The sync token is also config.json-authoritative.  In particular, never
    # place it in a systemd unit where `systemctl show` exposes it.
    if settings.instance_url:
        env["PA_INSTANCE_URL"] = settings.instance_url
    if settings.fleet_owner_url:
        env["PA_FLEET_OWNER_URL"] = settings.fleet_owner_url
    if settings.pr_supervisor_authority_url:
        env["PA_PR_SUPERVISOR_AUTHORITY_URL"] = settings.pr_supervisor_authority_url
    if settings.relay_enabled:
        env["PA_RELAY_ENABLED"] = "true"
    if not settings.agent_enabled:
        env["PA_AGENT_ENABLED"] = "false"
    return env
