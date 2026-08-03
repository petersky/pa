from __future__ import annotations

import hashlib
import json
from typing import Any

from pa.collaboration.models import (
    CollaborationMode,
    CommandAvailability,
    CommandCatalog,
    CommandOrigin,
    SessionCommand,
)


def _first(value: dict[str, Any], *names: str, default: Any = None) -> Any:
    for name in names:
        if name in value:
            return value[name]
    return default


def normalize_command_action(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict) or not value:
        return None
    raw = dict(value)
    nested = _first(raw, "setConfigOption", "set_config_option")
    if isinstance(nested, dict):
        return {
            "type": "set_config_option",
            "config_id": _first(nested, "configId", "config_id", "id"),
            "value": _first(nested, "value", "currentValue", "current_value"),
            "raw": raw,
        }
    action_type = str(_first(raw, "type", "kind", "action", default="")).strip()
    normalized_type = action_type.replace("-", "_").replace(" ", "_").lower()
    if normalized_type in {"setconfigoption", "set_config_option"}:
        return {
            "type": "set_config_option",
            "config_id": _first(raw, "configId", "config_id", "optionId", "option_id"),
            "value": _first(raw, "value", "currentValue", "current_value"),
            "raw": raw,
        }
    if normalized_type in {"prompt", "forward", "send_prompt"}:
        return {"type": "forward_prompt", "raw": raw}
    return {"type": normalized_type or "provider_action", "raw": raw}


def normalize_provider_command(
    raw: Any, *, provider: str, index: int
) -> SessionCommand | None:
    if not isinstance(raw, dict):
        return None
    name = (
        str(_first(raw, "name", "command", "id", default="")).strip().removeprefix("/")
    )
    if not name:
        return None
    input_meta = _first(raw, "input", "argument", "arguments", default={})
    if not isinstance(input_meta, dict):
        input_meta = {}
    input_hint = _first(
        raw,
        "inputHint",
        "input_hint",
        "argumentHint",
        "argument_hint",
        default=_first(input_meta, "hint", "placeholder", "description"),
    )
    input_required = bool(
        _first(
            raw,
            "inputRequired",
            "input_required",
            "requiresInput",
            "requires_input",
            default=_first(input_meta, "required", default=False),
        )
    )
    available = _first(raw, "available", "enabled", default=True)
    disabled_reason = _first(raw, "disabledReason", "disabled_reason")
    action = normalize_command_action(
        _first(raw, "commandAction", "command_action", "action")
    )
    return SessionCommand(
        id=f"provider:{provider}:{name}:{index}",
        name=name,
        description=str(_first(raw, "description", "help", default="")),
        origin=CommandOrigin.PROVIDER,
        provider=provider,
        input_hint=str(input_hint) if input_hint is not None else None,
        input_required=input_required,
        arguments=(
            list(input_meta.get("properties") or [])
            if isinstance(input_meta.get("properties"), list)
            else [input_meta]
            if input_meta
            else []
        ),
        action=action,
        availability=(
            CommandAvailability.AVAILABLE
            if available and not disabled_reason
            else CommandAvailability.DISABLED
        ),
        disabled_reason=str(disabled_reason) if disabled_reason else None,
        capability_requirements=list(
            _first(raw, "capabilityRequirements", "capability_requirements", default=[])
            or []
        ),
        metadata={"provider_record": raw},
    )


def pa_native_commands() -> list[SessionCommand]:
    return [
        SessionCommand(
            id="pa:plan",
            name="pa:plan",
            description="Request a policy-approved transition to Plan mode for the next safe turn.",
            origin=CommandOrigin.PA,
            action={"type": "request_collaboration_mode", "mode": "plan"},
        ),
        SessionCommand(
            id="pa:implement",
            name="pa:implement",
            description="Request a policy-approved transition to Default implementation mode.",
            origin=CommandOrigin.PA,
            action={"type": "request_collaboration_mode", "mode": "default"},
        ),
        SessionCommand(
            id="pa:mode",
            name="pa:mode",
            description="Request a collaboration mode by name (default or plan).",
            origin=CommandOrigin.PA,
            input_hint="default | plan",
            input_required=True,
            action={"type": "request_collaboration_mode", "mode": "$argument"},
        ),
        SessionCommand(
            id="pa:status",
            name="pa:status",
            description="Show effective collaboration policy, mode, and pending transition.",
            origin=CommandOrigin.PA,
            action={"type": "collaboration_status"},
        ),
    ]


def build_catalog(
    *,
    session_id: str,
    provider: str,
    generation: int,
    connection_generation: int,
    provider_commands: list[Any],
) -> CommandCatalog:
    commands = [
        command
        for index, raw in enumerate(provider_commands)
        if (command := normalize_provider_command(raw, provider=provider, index=index))
        is not None
    ]
    # Provider commands own unqualified names. PA commands are namespaced, so
    # collisions remain deterministic and both records retain provenance.
    commands.extend(pa_native_commands())
    payload = [command.model_dump(mode="json") for command in commands]
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return CommandCatalog(
        session_id=session_id,
        provider=provider,
        generation=generation,
        connection_generation=connection_generation,
        commands=commands,
        digest=digest,
    )


def advertised_collaboration_modes(
    *, config_options: list[Any] | None, catalog: CommandCatalog | None
) -> list[CollaborationMode]:
    values: list[str] = []
    for raw in config_options or []:
        if not isinstance(raw, dict):
            continue
        option_id = str(_first(raw, "id", "configId", "config_id", default=""))
        if option_id not in {"collaboration_mode", "collaborationMode"}:
            continue
        raw_values = _first(
            raw,
            "options",
            "values",
            "availableValues",
            "available_values",
            default=[],
        )
        for item in raw_values or []:
            if isinstance(item, dict):
                item = _first(item, "value", "id", "name")
            if item is not None:
                values.append(str(item).lower())
    if catalog:
        for command in catalog.commands:
            action = command.action or {}
            if action.get("type") != "set_config_option":
                continue
            if action.get("config_id") not in {
                "collaboration_mode",
                "collaborationMode",
            }:
                continue
            value = str(action.get("value") or "").lower()
            if value:
                values.append(value)
    result: list[CollaborationMode] = []
    for value in values:
        try:
            mode = CollaborationMode(value)
        except ValueError:
            continue
        if mode not in result:
            result.append(mode)
    return result
