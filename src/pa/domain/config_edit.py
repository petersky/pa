"""Validated read/write helpers for instance config.json."""

from __future__ import annotations

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import ValidationError

from pa.domain.instance_config import (
    InstanceConfig,
    load_instance_config,
    save_instance_config,
)
from pa.update.registry import normalize_track

FieldKind = Literal[
    "str",
    "bool",
    "list_str",
    "optional_str",
    "optional_list_str",
]


class ConfigError(ValueError):
    """Invalid config key, value, or operation."""


class MutateOp(StrEnum):
    SET = "set"
    ADD = "add"
    REMOVE = "remove"
    UNSET = "unset"


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: FieldKind
    description: str
    editable: bool = True
    sensitive: bool = False
    list_ops: bool = False  # supports add/remove


# Keys that affect the host service unit environment.
SERVICE_KEYS = frozenset(
    {
        "host",
        "instance_name",
        "release_track",
        "fleet_id",
        "zone",
        "agent_provider",
        "agent_command",
        "agent_args",
        "subscribed_realms",
        "peers",
        "capabilities",
        "sync_token",
        "instance_url",
        "fleet_owner_url",
        "pr_supervisor_authority_url",
        "relay_enabled",
    }
)

# Changing bind host requires a process restart to take effect.
RESTART_KEYS = frozenset({"host"})

FIELD_SPECS: dict[str, FieldSpec] = {
    "instance_id": FieldSpec(
        "instance_id",
        "str",
        "Stable instance UUID (immutable)",
        editable=False,
    ),
    "instance_name": FieldSpec(
        "instance_name",
        "str",
        "Display name for this instance",
    ),
    "data_dir": FieldSpec(
        "data_dir",
        "str",
        "Data directory path (managed by PA)",
        editable=False,
    ),
    "fleet_id": FieldSpec(
        "fleet_id",
        "str",
        "Fleet UUID this instance belongs to",
    ),
    "fleet_owner": FieldSpec(
        "fleet_owner",
        "str",
        "Fleet owner instance id or 'local'",
    ),
    "fleet_owner_url": FieldSpec(
        "fleet_owner_url",
        "optional_str",
        "Owner base URL (set when joining a fleet)",
    ),
    "pr_supervisor_authority_url": FieldSpec(
        "pr_supervisor_authority_url",
        "optional_str",
        "Single fenced PR-supervisor lease authority URL (empty follows fleet owner)",
    ),
    "instance_url": FieldSpec(
        "instance_url",
        "optional_str",
        "Advertised URL (Tailscale/LAN hostname, not localhost)",
    ),
    "host": FieldSpec(
        "host",
        "optional_str",
        "Server bind address (e.g. 127.0.0.1 or 0.0.0.0)",
    ),
    "subscribed_realms": FieldSpec(
        "subscribed_realms",
        "list_str",
        "Realm ids this instance syncs",
        list_ops=True,
    ),
    "zone": FieldSpec(
        "zone",
        "str",
        "Network zone label",
    ),
    "capabilities": FieldSpec(
        "capabilities",
        "list_str",
        "Advertised capability tags",
        list_ops=True,
    ),
    "relay_enabled": FieldSpec(
        "relay_enabled",
        "bool",
        "Whether this instance relays for peers",
    ),
    "peers": FieldSpec(
        "peers",
        "list_str",
        "Peer instance base URLs",
        list_ops=True,
    ),
    "release_track": FieldSpec(
        "release_track",
        "str",
        "Update track: release, beta, alpha, dev, or pypi",
    ),
    "sync_token": FieldSpec(
        "sync_token",
        "optional_str",
        "Shared secret for inter-instance sync APIs",
        sensitive=True,
    ),
    "session_secret": FieldSpec(
        "session_secret",
        "optional_str",
        "Cookie/session signing secret",
        sensitive=True,
    ),
    "agent_provider": FieldSpec(
        "agent_provider",
        "str",
        "Default ACP provider (cursor, codex, openinterpreter, …)",
    ),
    "agent_command": FieldSpec(
        "agent_command",
        "optional_str",
        "Optional ACP spawn command override",
    ),
    "agent_args": FieldSpec(
        "agent_args",
        "optional_list_str",
        "Optional ACP spawn args override",
        list_ops=True,
    ),
}


def list_field_specs(*, editable_only: bool = False) -> list[FieldSpec]:
    specs = list(FIELD_SPECS.values())
    if editable_only:
        return [s for s in specs if s.editable]
    return specs


def get_field_spec(key: str) -> FieldSpec:
    if key not in FIELD_SPECS:
        known = ", ".join(sorted(FIELD_SPECS))
        raise ConfigError(f"Unknown config key '{key}'. Known keys: {known}")
    return FIELD_SPECS[key]


def require_config(data_dir: Path) -> InstanceConfig:
    config = load_instance_config(data_dir)
    if config is None:
        raise ConfigError(
            f"No config.json at {data_dir / 'config.json'} — run: pa init"
        )
    return config


def format_value(value: Any, *, reveal: bool = False, sensitive: bool = False) -> str:
    if value is None:
        return "(null)"
    if sensitive and not reveal:
        text = str(value)
        if not text:
            return "(empty)"
        if len(text) <= 4:
            return "****"
        return text[:2] + "…" + text[-2:]
    if isinstance(value, list):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def config_as_dict(config: InstanceConfig) -> dict[str, Any]:
    return config.model_dump()


def _parse_bool(raw: str) -> bool:
    text = raw.strip().lower()
    if text in ("1", "true", "yes", "on"):
        return True
    if text in ("0", "false", "no", "off"):
        return False
    raise ConfigError(f"Invalid boolean '{raw}' (use true/false)")


def _parse_list(raw: str) -> list[str]:
    text = raw.strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Invalid JSON list: {exc}") from exc
        if not isinstance(parsed, list) or not all(isinstance(x, str) for x in parsed):
            raise ConfigError("List value must be a JSON array of strings")
        return [x.strip() for x in parsed if x.strip()]
    return [part.strip() for part in text.split(",") if part.strip()]


def _is_nullish(raw: str) -> bool:
    return raw.strip().lower() in ("", "null", "none", "-")


def parse_value(spec: FieldSpec, raw: str) -> Any:
    """Parse a CLI string into a typed config value."""
    if spec.kind == "bool":
        return _parse_bool(raw)
    if spec.kind == "list_str":
        return _parse_list(raw)
    if spec.kind == "optional_list_str":
        if _is_nullish(raw):
            return None
        return _parse_list(raw)
    if spec.kind == "optional_str":
        if _is_nullish(raw):
            return ""
        return raw.strip()
    # str
    if not raw.strip():
        raise ConfigError(f"{spec.name} cannot be empty")
    return raw.strip()


def _validate_http_url(value: str, *, field: str, allow_empty: bool = True) -> str:
    text = value.strip().rstrip("/")
    if not text:
        if allow_empty:
            return ""
        raise ConfigError(f"{field} cannot be empty")
    parsed = urlparse(text)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ConfigError(f"{field} must be an http(s) URL like http://macbook:8080")
    return text


def _validate_instance_url(value: str) -> str:
    url = _validate_http_url(value, field="instance_url")
    if not url:
        return ""
    parsed = urlparse(url)
    if (parsed.hostname or "").lower() in ("127.0.0.1", "localhost", "::1"):
        raise ConfigError(
            "instance_url cannot be localhost/127.0.0.1 — use a Tailscale or LAN hostname"
        )
    return url


def _validate_host(value: str) -> str:
    bind = value.strip()
    if not bind:
        return ""
    if bind.lower() == "localhost":
        bind = "127.0.0.1"
    allowed = {"0.0.0.0", "127.0.0.1", "::", "::1"}
    if "://" in bind or " " in bind:
        raise ConfigError("host must be a bind address like 0.0.0.0 or 127.0.0.1")
    if bind not in allowed and not all(c.isalnum() or c in ".-:[]" for c in bind):
        raise ConfigError(f"invalid bind host: {bind}")
    return bind


def _validate_agent_provider(value: str) -> str:
    key = value.strip().lower()
    try:
        from pa.acp.providers.registry import get_provider, list_provider_ids

        get_provider(key)
    except Exception as exc:
        known = ", ".join(list_provider_ids())
        raise ConfigError(
            f"Unknown agent_provider '{value}'. Choose from: {known}"
        ) from exc
    return key


def validate_field_value(key: str, value: Any) -> Any:
    """Domain validation beyond pydantic types. Returns normalized value."""
    spec = get_field_spec(key)

    if spec.kind in ("list_str", "optional_list_str") and value is not None:
        if not isinstance(value, list) or not all(isinstance(x, str) for x in value):
            raise ConfigError(f"{key} must be a list of strings")
        value = [x.strip() for x in value if str(x).strip()]

    if key == "host":
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ConfigError("host must be a string")
        return _validate_host(value)

    if key == "instance_url":
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ConfigError("instance_url must be a string")
        return _validate_instance_url(value)

    if key == "fleet_owner_url":
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ConfigError("fleet_owner_url must be a string")
        return _validate_http_url(value, field="fleet_owner_url")

    if key == "pr_supervisor_authority_url":
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ConfigError("pr_supervisor_authority_url must be a string")
        return _validate_http_url(value, field="pr_supervisor_authority_url")

    if key == "peers":
        assert isinstance(value, list)
        return [
            _validate_http_url(p, field="peers item", allow_empty=False) for p in value
        ]

    if key == "release_track":
        if not isinstance(value, str):
            raise ConfigError("release_track must be a string")
        try:
            return normalize_track(value)
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc

    if key == "agent_provider":
        if not isinstance(value, str):
            raise ConfigError("agent_provider must be a string")
        return _validate_agent_provider(value)

    if key == "subscribed_realms":
        assert isinstance(value, list)
        if not value:
            raise ConfigError("subscribed_realms must contain at least one realm")
        return value

    if key in ("instance_name", "fleet_id", "fleet_owner", "zone") and isinstance(
        value, str
    ):
        if not value.strip():
            raise ConfigError(f"{key} cannot be empty")
        return value.strip()

    return value


def default_for_unset(spec: FieldSpec) -> Any:
    """Value used by `unset` (reset to empty/default)."""
    if not spec.editable:
        raise ConfigError(f"{spec.name} cannot be unset")
    defaults = InstanceConfig().model_dump()
    if spec.name in defaults:
        return defaults[spec.name]
    if spec.kind == "bool":
        return False
    if spec.kind in ("list_str",):
        return []
    if spec.kind in ("optional_list_str",):
        return None
    return ""


@dataclass
class MutateResult:
    config: InstanceConfig
    key: str
    op: MutateOp
    before: Any
    after: Any
    restart_required: bool
    service_keys_changed: bool


def _apply_validated(
    data_dir: Path, key: str, value: Any, *, op: MutateOp
) -> MutateResult:
    spec = get_field_spec(key)
    if not spec.editable:
        raise ConfigError(f"{key} is read-only")

    config = require_config(data_dir)
    before = getattr(config, key)
    normalized = validate_field_value(key, value)

    try:
        # Build a full candidate and let pydantic reject type errors.
        data = config.model_dump()
        data[key] = normalized
        candidate = InstanceConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"Invalid {key}: {exc.errors()[0].get('msg', exc)}") from exc

    save_instance_config(data_dir, candidate)
    after = getattr(candidate, key)
    return MutateResult(
        config=candidate,
        key=key,
        op=op,
        before=before,
        after=after,
        restart_required=key in RESTART_KEYS and before != after,
        service_keys_changed=key in SERVICE_KEYS and before != after,
    )


def set_config_value(data_dir: Path, key: str, raw: str) -> MutateResult:
    spec = get_field_spec(key)
    if not spec.editable:
        raise ConfigError(f"{key} is read-only")
    parsed = parse_value(spec, raw)
    return _apply_validated(data_dir, key, parsed, op=MutateOp.SET)


def add_config_value(data_dir: Path, key: str, raw: str) -> MutateResult:
    spec = get_field_spec(key)
    if not spec.list_ops:
        raise ConfigError(f"{key} does not support add (not a list field)")
    item = raw.strip()
    if not item:
        raise ConfigError("Value to add cannot be empty")

    config = require_config(data_dir)
    current = getattr(config, key)
    if current is None:
        current_list: list[str] = []
    elif isinstance(current, list):
        current_list = list(current)
    else:
        raise ConfigError(f"{key} is not a list")

    if item in current_list:
        raise ConfigError(f"{item!r} already in {key}")
    current_list.append(item)
    return _apply_validated(data_dir, key, current_list, op=MutateOp.ADD)


def remove_config_value(data_dir: Path, key: str, raw: str) -> MutateResult:
    spec = get_field_spec(key)
    if not spec.list_ops:
        raise ConfigError(f"{key} does not support remove (not a list field)")
    item = raw.strip()
    if not item:
        raise ConfigError("Value to remove cannot be empty")

    config = require_config(data_dir)
    current = getattr(config, key)
    if current is None:
        current_list: list[str] = []
    elif isinstance(current, list):
        current_list = list(current)
    else:
        raise ConfigError(f"{key} is not a list")

    if item not in current_list:
        raise ConfigError(f"{item!r} not found in {key}")
    current_list = [x for x in current_list if x != item]
    return _apply_validated(data_dir, key, current_list, op=MutateOp.REMOVE)


def unset_config_value(data_dir: Path, key: str) -> MutateResult:
    spec = get_field_spec(key)
    return _apply_validated(data_dir, key, default_for_unset(spec), op=MutateOp.UNSET)


def refresh_after_mutate(data_dir: Path, result: MutateResult) -> bool:
    """Rewrite service unit env when a service-relevant key changed. Returns True if refreshed."""
    if not result.service_keys_changed:
        return False
    try:
        from pa.config import Settings, reset_settings
        from pa.fleet.join import refresh_service_env

        reset_settings()
        settings = Settings(data_dir=data_dir)
        cfg = result.config
        settings.host = cfg.host or "127.0.0.1"
        settings.instance_url = cfg.instance_url
        settings.instance_name = cfg.instance_name
        settings.release_track = cfg.release_track
        settings.fleet_id = cfg.fleet_id
        settings.zone = cfg.zone
        settings.agent_provider = cfg.agent_provider
        settings.agent_command = cfg.agent_command
        settings.agent_args = cfg.agent_args
        settings.subscribed_realms = list(cfg.subscribed_realms)
        settings.peers = list(cfg.peers)
        settings.capabilities = list(cfg.capabilities)
        settings.sync_token = cfg.sync_token
        settings.fleet_owner_url = cfg.fleet_owner_url
        settings.pr_supervisor_authority_url = cfg.pr_supervisor_authority_url
        settings.relay_enabled = cfg.relay_enabled
        return refresh_service_env(settings)
    except Exception:
        return False
