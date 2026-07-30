"""Validated read/write helpers for instance config.json."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import ValidationError

from pa.configuration.registry import SETTINGS, SettingDefinition, get_setting
from pa.domain.instance_config import (
    InstanceConfig,
    load_instance_config,
    save_instance_config,
)
from pa.update.registry import normalize_track

FieldKind = Literal[
    "str",
    "optional_str",
    "path",
    "optional_path",
    "int",
    "optional_int",
    "float",
    "bool",
    "dict_int",
    "list_str",
    "optional_list_str",
]


class ConfigError(ValueError):
    """Invalid config key, value, or operation."""


class ConfigConflictError(ConfigError):
    """The persisted configuration changed after the editing snapshot."""


class MutateOp(StrEnum):
    SET = "set"
    ADD = "add"
    REMOVE = "remove"
    UNSET = "unset"


FieldSpec = SettingDefinition

# Apply behavior is registry-owned.  "reload" means service definition/runtime
# refresh without restarting PA; "restart" means the running process remains on
# the old effective value until an explicit safe restart.
SERVICE_KEYS = frozenset(
    key for key, spec in SETTINGS.items() if spec.apply in {"reload", "restart"}
)
RESTART_KEYS = frozenset(
    key for key, spec in SETTINGS.items() if spec.apply == "restart"
)
FIELD_SPECS: dict[str, FieldSpec] = SETTINGS


def list_field_specs(*, editable_only: bool = False) -> list[FieldSpec]:
    specs = sorted(
        FIELD_SPECS.values(), key=lambda spec: (spec.category, spec.order, spec.name)
    )
    if editable_only:
        return [s for s in specs if s.editable]
    return specs


def get_field_spec(key: str) -> FieldSpec:
    try:
        return get_setting(key)
    except KeyError as exc:
        raise ConfigError(str(exc)) from exc


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
        if not str(value):
            return "(empty)"
        return "<redacted>"
    if isinstance(value, list):
        return json.dumps(value)
    if isinstance(value, dict):
        return json.dumps(value, sort_keys=True)
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def config_as_dict(config: InstanceConfig) -> dict[str, Any]:
    return config.model_dump()


def config_revision(config: InstanceConfig) -> str:
    """Stable opaque revision used for optimistic concurrency."""
    payload = json.dumps(
        config.model_dump(mode="json", exclude_unset=True),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


def validate_config_changes(
    base: InstanceConfig, changes: dict[str, Any]
) -> InstanceConfig:
    """Validate a complete staged change set without writing it."""
    data = base.model_dump()
    for requested_key, value in changes.items():
        spec = get_field_spec(requested_key)
        if not spec.editable:
            raise ConfigError(f"{spec.name} is read-only")
        data[spec.name] = validate_field_value(spec.name, value)
    try:
        candidate = InstanceConfig.model_validate(data)
    except ValidationError as exc:
        errors = "; ".join(
            f"{'.'.join(str(part) for part in item.get('loc', ()))}: "
            f"{item.get('msg', 'invalid value')}"
            for item in exc.errors()
        )
        raise ConfigError(f"Invalid configuration: {errors}") from exc
    candidate.__pydantic_fields_set__ = base.model_fields_set | {
        get_field_spec(key).name for key in changes
    }
    if candidate.workspace_root:
        data_dir = Path(candidate.data_dir).expanduser().resolve()
        workspace = Path(candidate.workspace_root).expanduser().resolve()
        if (
            workspace == data_dir
            or data_dir in workspace.parents
            or workspace in data_dir.parents
        ):
            raise ConfigError("workspace_root must be outside data_dir")
    if bool(candidate.oidc_client_id) != bool(candidate.oidc_issuer):
        raise ConfigError("oidc_issuer and oidc_client_id must be configured together")
    return candidate


def apply_config_changes(
    data_dir: Path,
    changes: dict[str, Any],
    *,
    expected_revision: str,
    unset_keys: frozenset[str] = frozenset(),
) -> tuple[InstanceConfig, frozenset[str], frozenset[str]]:
    """Validate and atomically apply staged changes unless the snapshot is stale."""
    current = require_config(data_dir)
    if config_revision(current) != expected_revision:
        raise ConfigConflictError(
            "Configuration changed externally; refresh and review the merged values."
        )
    candidate = validate_config_changes(current, changes)
    candidate.__pydantic_fields_set__ -= {
        get_field_spec(key).name for key in unset_keys
    }
    changed = frozenset(
        get_field_spec(key).name
        for key in changes
        if getattr(current, get_field_spec(key).name)
        != getattr(candidate, get_field_spec(key).name)
    )
    if changed:
        save_instance_config(data_dir, candidate)
    return (
        candidate,
        changed & SERVICE_KEYS,
        changed & RESTART_KEYS,
    )


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
    if spec.kind in ("int", "optional_int"):
        if spec.kind == "optional_int" and _is_nullish(raw):
            return None
        try:
            return int(raw.strip())
        except ValueError as exc:
            raise ConfigError(f"Invalid integer '{raw}'") from exc
    if spec.kind == "float":
        try:
            return float(raw.strip())
        except ValueError as exc:
            raise ConfigError(f"Invalid number '{raw}'") from exc
    if spec.kind == "dict_int":
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise ConfigError(f"Invalid JSON object: {exc}") from exc
        if not isinstance(parsed, dict):
            raise ConfigError("Value must be a JSON object of integer limits")
        return parsed
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
    if spec.kind in ("path", "optional_path"):
        if spec.kind == "optional_path" and _is_nullish(raw):
            return None
        if not raw.strip():
            raise ConfigError(f"{spec.name} cannot be empty")
        return str(Path(raw.strip()).expanduser())
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

    if spec.kind == "float":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ConfigError(f"{key} must be a number")
        value = float(value)
    if spec.kind in ("int", "optional_int") and value is not None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError(f"{key} must be an integer")
    if spec.kind == "bool" and not isinstance(value, bool):
        raise ConfigError(f"{key} must be a boolean")
    if spec.kind in ("path", "optional_path") and value is not None:
        if not isinstance(value, (str, Path)):
            raise ConfigError(f"{key} must be a path")
        value = str(Path(value).expanduser())

    if key == "dispatch_capacity":
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            raise ConfigError("dispatch_capacity must be an integer from 1 to 256")
        if not 1 <= value <= 256:
            raise ConfigError("dispatch_capacity must be between 1 and 256")
        return value

    if key == "dispatch_provider_capacities":
        if not isinstance(value, dict):
            raise ConfigError(
                "dispatch_provider_capacities must be a JSON object of provider limits"
            )
        normalized: dict[str, int] = {}
        for provider, limit in value.items():
            provider_name = str(provider).strip().lower()
            if not provider_name:
                raise ConfigError("provider capacity names cannot be empty")
            if (
                isinstance(limit, bool)
                or not isinstance(limit, int)
                or not 1 <= limit <= 256
            ):
                raise ConfigError(
                    f"capacity for provider {provider!r} must be an integer from 1 to 256"
                )
            normalized[provider_name] = limit
        return normalized

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

    if key == "port":
        if not 1 <= value <= 65535:
            raise ConfigError("port must be between 1 and 65535")
        return value

    if key == "web_listeners":
        assert isinstance(value, list)
        from pa.server.listeners import parse_listener

        normalized_listeners: list[str] = []
        for listener in value:
            text = listener.strip()
            if not text:
                continue
            try:
                parse_listener(text, 8080)
            except ValueError as exc:
                raise ConfigError(f"Invalid web listener {listener!r}: {exc}") from exc
            normalized_listeners.append(text)
        return normalized_listeners

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

    if key in ("oidc_issuer",):
        if value is None:
            return ""
        if not isinstance(value, str):
            raise ConfigError(f"{key} must be a string")
        return _validate_http_url(value, field=key)

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

    if key == "log_level":
        if not isinstance(value, str):
            raise ConfigError("log_level must be a string")
        value = value.strip().upper()
        if value not in spec.allowed:
            raise ConfigError(f"log_level must be one of: {', '.join(spec.allowed)}")
        return value

    if key == "update_repo":
        if not isinstance(value, str):
            raise ConfigError("update_repo must be a string")
        parts = value.strip().split("/")
        if len(parts) != 2 or not all(parts):
            raise ConfigError("update_repo must be a GitHub owner/repository name")
        return value.strip()

    if key == "default_theme_id":
        if not isinstance(value, str) or not value.strip():
            raise ConfigError("default_theme_id cannot be empty")
        from pa.modules.theme import get_theme_catalog

        known = {item.id for item in get_theme_catalog()}
        if value.strip() not in known:
            raise ConfigError(
                f"Unknown default_theme_id {value!r}. Choose from: {', '.join(sorted(known))}"
            )
        return value.strip()

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

    if spec.allowed and isinstance(value, str):
        normalized = value.strip().lower()
        allowed = {item.lower(): item for item in spec.allowed}
        if normalized not in allowed:
            raise ConfigError(f"{key} must be one of: {', '.join(spec.allowed)}")
        return allowed[normalized]

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
    if spec.kind == "dict_int":
        return {}
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
    key = spec.name
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
    candidate.__pydantic_fields_set__ = config.model_fields_set | {key}
    if op == MutateOp.UNSET:
        candidate.__pydantic_fields_set__.discard(key)

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
    key = spec.name
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
    key = spec.name
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
        from pa.config import Settings
        from pa.fleet.join import refresh_service_env

        values = result.config.model_dump(exclude_unset=True)
        values["data_dir"] = data_dir
        settings = Settings(**values)
        return refresh_service_env(settings)
    except Exception:
        return False
