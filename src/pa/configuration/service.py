"""Configuration snapshots, validation, diffs, atomic updates, and audit events."""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import platform
import uuid
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from pa.configuration.registry import (
    ALIASES,
    CONFIGURATION_PRECEDENCE,
    REGISTRY,
    SETTINGS,
    SettingDefinition,
    get_setting,
    registry_metadata,
)
from pa.core.io import atomic_write_json
from pa.domain.config_edit import (
    ConfigConflictError,
    ConfigError,
    apply_config_changes,
    config_revision,
    default_for_unset,
    require_config,
    validate_config_changes,
    validate_field_value,
)
from pa.domain.instance_config import InstanceConfig, config_path, save_instance_config

AUDIT_FILE = "configuration-audit.jsonl"
IDEMPOTENCY_FILE = "configuration-idempotency.json"
LOCK_FILE = ".configuration.lock"
_SECRET_NAME_PARTS = ("token", "secret", "password", "credential", "cookie", "api_key")


def _json_value(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return value


def _redact(definition: SettingDefinition, value: Any) -> Any:
    if not definition.secret:
        return _json_value(value)
    return "<redacted>" if value not in (None, "") else None


def _unknown_redacted(key: str, value: Any) -> Any:
    if any(part in key.lower() for part in _SECRET_NAME_PARTS):
        return "<redacted>" if value not in (None, "") else None
    return _json_value(value)


def read_persisted_document(data_dir: Path) -> dict[str, Any]:
    path = config_path(data_dir)
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ConfigError(f"Cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ConfigError(f"Invalid {path}: expected a JSON object")
    return value


def explicit_environment(
    environment: Mapping[str, str] | None = None,
) -> tuple[dict[str, str], set[str]]:
    environment = os.environ if environment is None else environment
    values: dict[str, str] = {}
    names: set[str] = set()
    for definition in SETTINGS.values():
        for name in definition.environment:
            if name in environment:
                names.add(definition.key)
                values[definition.key] = name
                break
    return values, names


def configuration_snapshot(
    settings: Any,
    *,
    environment: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    persisted = read_persisted_document(settings.data_dir)
    config = require_config(settings.data_dir)
    env_sources, env_keys = explicit_environment(environment)
    rows: list[dict[str, Any]] = []
    known_persisted: set[str] = set()

    for definition in sorted(
        SETTINGS.values(), key=lambda item: (item.category, item.order, item.key)
    ):
        persisted_key = definition.key
        configured = definition.key in persisted
        if configured:
            known_persisted.add(definition.key)
        else:
            alias = next(
                (name for name in definition.aliases if name in persisted), None
            )
            if alias:
                persisted_key = alias
                configured = True
                known_persisted.add(alias)
        configured_value = persisted.get(persisted_key) if configured else None
        effective = getattr(settings, definition.key)
        pending_apply = (
            configured
            and definition.apply != "live"
            and _json_value(getattr(config, definition.key)) != _json_value(effective)
        )
        if pending_apply:
            source = "runtime_override"
            source_detail = f"running process; pending {definition.apply}"
        elif configured:
            source = "config_file"
            source_detail = f"config.json:{persisted_key}"
        elif definition.key in env_keys:
            source = "environment"
            source_detail = env_sources[definition.key]
        elif _json_value(effective) != _json_value(definition.default):
            source = "runtime_override"
            source_detail = "derived normalization/runtime state"
        else:
            source = "default"
            source_detail = "registry"
        applicable = "all" in definition.platforms or platform.system().lower() in {
            item.lower() for item in definition.platforms
        }
        rows.append(
            {
                **definition.metadata(),
                "configured": configured,
                "configured_key": persisted_key if configured else None,
                "configured_value": _redact(definition, configured_value),
                "effective_value": _redact(definition, effective),
                "default_value": _redact(definition, definition.default),
                "source": source,
                "source_detail": source_detail,
                "configured_source": (
                    f"config.json:{persisted_key}" if configured else None
                ),
                "overridden": configured and source != "config_file",
                "pending_apply": pending_apply,
                "applicable": applicable,
                "supported": applicable,
            }
        )

    unknown = [
        {"key": key, "value": _unknown_redacted(key, value)}
        for key, value in sorted(persisted.items())
        if key not in known_persisted and key not in SETTINGS and key not in ALIASES
    ]
    deprecated = [
        {
            "key": key,
            "canonical_key": ALIASES[key],
            "value": _unknown_redacted(key, value),
            "migration": SETTINGS[ALIASES[key]].migration,
        }
        for key, value in sorted(persisted.items())
        if key in ALIASES
    ]
    return {
        "schema_version": 1,
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "target": "local",
        "revision": config_revision(config),
        "precedence": list(CONFIGURATION_PRECEDENCE),
        "settings": rows,
        "unknown": unknown,
        "deprecated": deprecated,
        "platform": platform.system().lower(),
    }


def schema_document() -> dict[str, Any]:
    return registry_metadata()


def normalize_changes(
    changes: Mapping[str, Any] | None,
    clear: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for requested, value in (changes or {}).items():
        try:
            definition = get_setting(requested)
        except KeyError as exc:
            raise ConfigError(str(exc)) from exc
        if not definition.editable:
            raise ConfigError(
                f"{definition.key} is {definition.exposure}: {definition.rationale}"
            )
        if definition.key in normalized:
            raise ConfigError(f"Duplicate setting through alias: {requested}")
        if definition.secret and value in (None, ""):
            raise ConfigError(
                f"Use the explicit clear workflow to clear secret {definition.key}"
            )
        normalized[definition.key] = validate_field_value(definition.key, value)
    for requested in clear or ():
        try:
            definition = get_setting(requested)
        except KeyError as exc:
            raise ConfigError(str(exc)) from exc
        if not definition.editable:
            raise ConfigError(
                f"{definition.key} is {definition.exposure}: {definition.rationale}"
            )
        normalized[definition.key] = default_for_unset(definition)
    return normalized


def validate_update(
    data_dir: Path,
    changes: Mapping[str, Any] | None,
    clear: list[str] | tuple[str, ...] | None = None,
) -> tuple[InstanceConfig, dict[str, Any]]:
    base = require_config(data_dir)
    normalized = normalize_changes(changes, clear)
    candidate = validate_config_changes(base, normalized)
    return candidate, normalized


def diff_update(
    data_dir: Path,
    changes: Mapping[str, Any] | None,
    clear: list[str] | tuple[str, ...] | None = None,
) -> dict[str, Any]:
    base = require_config(data_dir)
    candidate, normalized = validate_update(data_dir, changes, clear)
    rows = []
    for key in normalized:
        definition = SETTINGS[key]
        before = getattr(base, key)
        after = getattr(candidate, key)
        if before == after:
            continue
        rows.append(
            {
                "key": key,
                "before": _redact(definition, before),
                "after": _redact(definition, after),
                "apply": definition.apply,
                "restart_required": definition.apply == "restart",
                "reload_required": definition.apply == "reload",
                "overridden_by_environment": False,
            }
        )
    return {
        "valid": True,
        "revision": config_revision(base),
        "changes": rows,
        "change_count": len(rows),
        "restart_required": any(row["restart_required"] for row in rows),
        "reload_required": any(row["reload_required"] for row in rows),
    }


@dataclass(frozen=True)
class ApplyResult:
    config: InstanceConfig
    changed: frozenset[str]
    reload: frozenset[str]
    restart: frozenset[str]
    duplicate: bool = False
    rename: dict[str, Any] | None = None


def _read_idempotency(data_dir: Path) -> dict[str, Any]:
    path = data_dir / IDEMPOTENCY_FILE
    if not path.exists():
        return {}
    try:
        value = json.loads(path.read_text())
        return value if isinstance(value, dict) else {}
    except OSError, json.JSONDecodeError:
        return {}


def _record_idempotency(
    data_dir: Path,
    idempotency_key: str,
    *,
    revision: str,
    changed: frozenset[str],
    request_digest: str,
) -> None:
    values = _read_idempotency(data_dir)
    values[idempotency_key] = {
        "revision": revision,
        "changed": sorted(changed),
        "request_digest": request_digest,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    if len(values) > 256:
        values = dict(list(values.items())[-256:])
    atomic_write_json(data_dir / IDEMPOTENCY_FILE, values, mode=0o600)


def _request_digest(
    changes: Mapping[str, Any],
    clear: list[str] | tuple[str, ...] | None,
) -> str:
    canonical_clear = sorted(get_setting(key).key for key in (clear or ()))
    payload = json.dumps(
        {
            "changes": _json_value(dict(sorted(changes.items()))),
            "clear": canonical_clear,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(payload).hexdigest()


@contextmanager
def _configuration_lock(data_dir: Path):
    data_dir.mkdir(parents=True, exist_ok=True)
    path = data_dir / LOCK_FILE
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _append_audit(
    data_dir: Path,
    *,
    principal_id: str,
    interface: str,
    idempotency_key: str,
    changed: frozenset[str],
    before_revision: str,
    after_revision: str,
) -> None:
    event = {
        "schema_version": 1,
        "id": str(uuid.uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "principal_id": principal_id,
        "interface": interface,
        "idempotency_key": idempotency_key,
        "keys": sorted(changed),
        "secret_keys": sorted(key for key in changed if SETTINGS[key].secret),
        "before_revision": before_revision,
        "after_revision": after_revision,
    }
    path = data_dir / AUDIT_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
    with os.fdopen(fd, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def apply_update(
    settings: Any,
    changes: Mapping[str, Any] | None,
    clear: list[str] | tuple[str, ...] | None,
    *,
    expected_revision: str,
    idempotency_key: str,
    principal_id: str,
    interface: str,
) -> ApplyResult:
    with _configuration_lock(settings.data_dir):
        return _apply_update_locked(
            settings,
            changes,
            clear,
            expected_revision=expected_revision,
            idempotency_key=idempotency_key,
            principal_id=principal_id,
            interface=interface,
        )


def _apply_update_locked(
    settings: Any,
    changes: Mapping[str, Any] | None,
    clear: list[str] | tuple[str, ...] | None,
    *,
    expected_revision: str,
    idempotency_key: str,
    principal_id: str,
    interface: str,
) -> ApplyResult:
    if not idempotency_key.strip():
        raise ConfigError("idempotency_key is required")
    normalized = normalize_changes(changes, clear)
    request_digest = _request_digest(normalized, clear)
    prior = _read_idempotency(settings.data_dir).get(idempotency_key)
    if prior:
        prior_digest = prior.get("request_digest")
        if prior_digest and prior_digest != request_digest:
            raise ConfigConflictError(
                "Idempotency key was already used for a different configuration patch."
            )
        changed = frozenset(prior.get("changed") or ())
        return ApplyResult(
            config=require_config(settings.data_dir),
            changed=changed,
            reload=frozenset(
                key for key in changed if SETTINGS[key].apply in {"reload", "restart"}
            ),
            restart=frozenset(
                key for key in changed if SETTINGS[key].apply == "restart"
            ),
            duplicate=True,
        )
    validate_update(settings.data_dir, changes, clear)
    before = require_config(settings.data_dir)
    before_revision = config_revision(before)
    idempotency_before = _read_idempotency(settings.data_dir)
    rename_registry = None
    rename_generation = None
    if (
        "instance_name" in normalized
        and normalized["instance_name"] != before.instance_name
    ):
        from pa.fleet.registry import FleetRegistry

        candidate_registry = FleetRegistry(settings.data_dir, before.fleet_id)
        try:
            FleetRegistry.normalize_name(str(normalized["instance_name"]))
            if candidate_registry.get_instance(before.instance_id):
                candidate_registry._validate_name_available(
                    before.instance_id, str(normalized["instance_name"])
                )
        except ValueError as exc:
            raise ConfigError(str(exc)) from exc
        if candidate_registry.get_instance(before.instance_id):
            rename_registry = candidate_registry
            rename_generation = candidate_registry.generation
    candidate, reload, restart = apply_config_changes(
        settings.data_dir,
        normalized,
        expected_revision=expected_revision,
        unset_keys=frozenset(get_setting(key).key for key in (clear or ())),
    )
    changed = frozenset(
        key for key in normalized if getattr(before, key) != getattr(candidate, key)
    )
    after_revision = config_revision(candidate)
    try:
        _record_idempotency(
            settings.data_dir,
            idempotency_key,
            revision=after_revision,
            changed=changed,
            request_digest=request_digest,
        )
        _append_audit(
            settings.data_dir,
            principal_id=principal_id,
            interface=interface,
            idempotency_key=idempotency_key,
            changed=changed,
            before_revision=before_revision,
            after_revision=after_revision,
        )
    except OSError as exc:
        # Do not claim an unaudited update.  The prior managed document is
        # restored atomically before surfacing the failure. Idempotency is
        # restored too, so a failed update cannot later be reported as applied.
        save_instance_config(settings.data_dir, before)
        atomic_write_json(
            settings.data_dir / IDEMPOTENCY_FILE,
            idempotency_before,
            mode=0o600,
        )
        raise ConfigError(
            f"Configuration audit/write failed; update rolled back: {exc}"
        ) from exc
    rename = None
    if rename_registry is not None:
        try:
            renamed = rename_registry.rename_instance(
                before.instance_id,
                candidate.instance_name,
                actor=principal_id,
                source=f"configuration.{interface}",
                expected_generation=rename_generation,
            )
            settings.instance_name = renamed.name
            from pa.fleet.join import refresh_service_env

            service_refreshed = refresh_service_env(settings)
            rename = {
                "state": "committed",
                "stable_instance_id": renamed.instance_id,
                "old_name": before.instance_name,
                "new_name": renamed.name,
                "generation": rename_registry.generation,
                "service_environment_refreshed": service_refreshed,
            }
        except (OSError, RuntimeError, ValueError) as exc:
            save_instance_config(settings.data_dir, before)
            atomic_write_json(
                settings.data_dir / IDEMPOTENCY_FILE,
                idempotency_before,
                mode=0o600,
            )
            raise ConfigError(
                "Canonical rename could not complete; configuration was rolled back "
                f"and the audit attempt was retained: {exc}"
            ) from exc
    # Live settings are safe to update only after config, audit, idempotency, and
    # canonical identity evidence are durable. Other reload/restart settings retain
    # their observed value.
    for key in changed:
        if SETTINGS[key].apply == "live":
            setattr(settings, key, getattr(candidate, key))
    return ApplyResult(candidate, changed, reload, restart, rename=rename)


def audit_events(data_dir: Path, *, limit: int = 100) -> list[dict[str, Any]]:
    path = data_dir / AUDIT_FILE
    if not path.exists():
        return []
    events: list[dict[str, Any]] = []
    for line in path.read_text(errors="replace").splitlines():
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            events.append(value)
    return events[-max(1, min(limit, 1000)) :]


def human_coverage_report() -> str:
    def access(definition: SettingDefinition, surface: str) -> str:
        item = definition.surfaces[surface]
        return "rw" if item.write else ("r" if item.read else "—")

    lines = [
        "# PA configuration coverage",
        "",
        f"Registry settings: {len(REGISTRY)}",
        "",
        "| Key | Category | Exposure | Web | CLI | API | MCP | Apply |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for definition in sorted(REGISTRY.values(), key=lambda item: item.key):
        lines.append(
            "| {key} | {category} | {exposure} | {web} | {cli} | {api} | {mcp} | {apply} |".format(
                key=definition.key,
                category=definition.category,
                exposure=definition.exposure,
                web=access(definition, "web"),
                cli=access(definition, "cli"),
                api=access(definition, "api"),
                mcp=access(definition, "mcp"),
                apply=definition.apply,
            )
        )
    return "\n".join(lines) + "\n"
