"""Provider-neutral ACP session configuration compatibility helpers."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping


class ACPConfigurationError(RuntimeError):
    """A requested setting cannot be applied and verified by this ACP agent."""


def _normalized(value: Any) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


_SEMANTIC_ALIASES = {
    "model": {"model", "models", "modelid", "languagemodel"},
    "mode": {"mode", "sessionmode", "agentmode", "interactionmode"},
    "reasoning": {
        "effort",
        "reasoning",
        "reasoningeffort",
        "reasoninglevel",
        "thought",
        "thoughteffort",
        "thoughtlevel",
        "thinking",
        "thinkingeffort",
        "thinkinglevel",
    },
}


def parse_model_selector(selector: str | None) -> tuple[str | None, str | None]:
    """Split a provider-neutral trailing ``model[reasoning]`` selector."""
    if selector is None:
        return None, None
    raw = str(selector).strip()
    if not raw:
        return None, None
    if raw.endswith("]"):
        model, separator, reasoning = raw[:-1].rpartition("[")
        if separator and model.strip() and reasoning.strip() and "[" not in reasoning:
            return model.strip(), reasoning.strip()
    return raw, None


@dataclass(frozen=True)
class SessionConfigurationRequest:
    """The complete configuration that must be admitted before prompting."""

    model_id: str | None = None
    mode_id: str | None = None
    reasoning: str | None = None
    model_provider: str | None = None
    config: dict[str, str | bool] = field(default_factory=dict)

    @classmethod
    def from_values(
        cls,
        *,
        model_id: str | None = None,
        mode_id: str | None = None,
        reasoning: str | None = None,
        model_provider: str | None = None,
        config: dict[str, str | bool] | None = None,
    ) -> SessionConfigurationRequest:
        parsed_model, selector_reasoning = parse_model_selector(model_id)
        explicit_reasoning = str(reasoning).strip() if reasoning else None
        if (
            selector_reasoning
            and explicit_reasoning
            and selector_reasoning != explicit_reasoning
        ):
            raise ACPConfigurationError(
                "ACP configuration compatibility error: the combined model selector "
                f"requests reasoning {selector_reasoning!r}, but reasoning "
                f"{explicit_reasoning!r} was also requested. Choose one value."
            )
        remaining = dict(config or {})
        resolved_provider = str(model_provider).strip() if model_provider else None
        for key in list(remaining):
            if _normalized(key) not in {"modelprovider", "provider"}:
                continue
            # Reserved for multi-backend ACP agents (e.g. OpenInterpreter).
            # Strip from generic ACP config options so it is not sent as an
            # unknown set_config_option id.
            value = str(remaining.pop(key)).strip()
            if resolved_provider and value and value != resolved_provider:
                raise ACPConfigurationError(
                    "ACP configuration compatibility error: conflicting "
                    f"model_provider values {resolved_provider!r} and {value!r}."
                )
            resolved_provider = value or resolved_provider
        resolved_reasoning = explicit_reasoning or selector_reasoning
        for key in list(remaining):
            if _normalized(key) not in _SEMANTIC_ALIASES["reasoning"]:
                continue
            value = str(remaining.pop(key)).strip()
            if resolved_reasoning and value != resolved_reasoning:
                raise ACPConfigurationError(
                    "ACP configuration compatibility error: conflicting reasoning "
                    f"values {resolved_reasoning!r} and {value!r} were requested."
                )
            resolved_reasoning = value
        return cls(
            model_id=parsed_model,
            mode_id=str(mode_id).strip() if mode_id else None,
            reasoning=resolved_reasoning,
            model_provider=resolved_provider,
            config=remaining,
        )

    @classmethod
    def from_dict(cls, value: dict[str, Any] | None) -> SessionConfigurationRequest:
        value = dict(value or {})
        return cls.from_values(
            model_id=value.get("model_id"),
            mode_id=value.get("mode_id"),
            reasoning=value.get("reasoning"),
            model_provider=value.get("model_provider"),
            config=value.get("config") or {},
        )

    @property
    def empty(self) -> bool:
        return not (
            self.model_id
            or self.mode_id
            or self.reasoning
            or self.model_provider
            or self.config
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "model_id": self.model_id,
            "mode_id": self.mode_id,
            "reasoning": self.reasoning,
            "model_provider": self.model_provider,
            "config": dict(sorted(self.config.items())),
        }

    def merged(self, patch: SessionConfigurationRequest) -> SessionConfigurationRequest:
        config = dict(self.config)
        config.update(patch.config)
        return SessionConfigurationRequest(
            model_id=patch.model_id or self.model_id,
            mode_id=patch.mode_id or self.mode_id,
            reasoning=patch.reasoning or self.reasoning,
            model_provider=patch.model_provider or self.model_provider,
            config=config,
        )


def option_id(option: dict[str, Any]) -> str | None:
    value = option.get("id") or option.get("configId") or option.get("config_id")
    return str(value) if value else None


def option_current_value(option: dict[str, Any]) -> str | bool | None:
    if "currentValue" in option:
        return option["currentValue"]
    return option.get("current_value")


def option_values(option: dict[str, Any]) -> set[str | bool]:
    values: set[str | bool] = set()

    def collect(entries: Iterable[Any]) -> None:
        for entry in entries:
            if not isinstance(entry, dict):
                continue
            if "value" in entry:
                values.add(entry["value"])
            nested = entry.get("options")
            if isinstance(nested, list):
                collect(nested)

    entries = option.get("options")
    if isinstance(entries, list):
        collect(entries)
    return values


def find_option(
    options: list[dict[str, Any]],
    semantic: str,
) -> dict[str, Any] | None:
    aliases = _SEMANTIC_ALIASES[semantic]
    matches: list[tuple[int, dict[str, Any]]] = []
    for option in options:
        oid = option_id(option)
        category = _normalized(option.get("category"))
        name = _normalized(option.get("name"))
        normalized_id = _normalized(oid)
        score = 0
        if category in aliases:
            score = 4
        elif normalized_id in aliases:
            score = 3
        elif name in aliases:
            score = 2
        elif any(alias and alias in name for alias in aliases):
            score = 1
        if score and oid:
            matches.append((score, option))
    if not matches:
        return None
    best_score = max(score for score, _ in matches)
    best = [option for score, option in matches if score == best_score]
    if len(best) != 1:
        ids = ", ".join(sorted(option_id(option) or "?" for option in best))
        raise ACPConfigurationError(
            f"ACP configuration compatibility error: the agent advertised multiple "
            f"{semantic} configuration options ({ids}); use an explicit config option id."
        )
    return best[0]


def find_option_by_id(
    options: list[dict[str, Any]], config_id: str
) -> dict[str, Any] | None:
    return next((option for option in options if option_id(option) == config_id), None)


def validate_option_value(
    option: dict[str, Any], value: str | bool, *, label: str
) -> None:
    option_type = option.get("type")
    if option_type == "boolean":
        if not isinstance(value, bool):
            raise ACPConfigurationError(
                f"ACP configuration compatibility error: {label} requires a boolean value."
            )
        return
    advertised = option_values(option)
    if advertised and value not in advertised:
        supported = ", ".join(sorted(str(item) for item in advertised))
        raise ACPConfigurationError(
            f"ACP configuration compatibility error: requested {label} value {value!r} "
            f"is not advertised by the agent. Supported values: {supported}."
        )


def advertised_state_values(
    state: dict[str, Any] | None,
    *,
    collection_names: tuple[str, ...],
    id_names: tuple[str, ...],
) -> set[str]:
    if not isinstance(state, dict):
        return set()
    entries: list[Any] = []
    for name in collection_names:
        value = state.get(name)
        if isinstance(value, list):
            entries = value
            break
    result: set[str] = set()
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        for name in id_names:
            value = entry.get(name)
            if value:
                result.add(str(value))
                break
    return result


def state_current_value(
    state: dict[str, Any] | None, names: tuple[str, ...]
) -> str | None:
    if not isinstance(state, dict):
        return None
    for name in names:
        value = state.get(name)
        if value:
            return str(value)
    return None


def _semantic_mapping_value(
    values: Mapping[str, Any] | None,
    semantic: str,
) -> str | bool | None:
    """Read one unambiguous semantic value without guessing between aliases."""
    if not isinstance(values, Mapping):
        return None
    aliases = _SEMANTIC_ALIASES[semantic]
    matches = [
        value
        for key, value in values.items()
        if _normalized(key) in aliases and value is not None and value != ""
    ]
    if not matches:
        return None
    first = matches[0]
    return first if all(value == first for value in matches[1:]) else None


def confirmed_session_configuration(
    config_json: Mapping[str, Any] | None,
    *,
    model_id: str | None = None,
    mode_id: str | None = None,
) -> dict[str, Any]:
    """Normalize provider-confirmed session metadata across ACP representations.

    ACP providers may report model and mode state through dedicated ``models`` /
    ``modes`` objects, semantic ``configOptions``, or both.  PA historically kept
    the dedicated alias even after a newer config-option receipt was persisted.
    This function deliberately considers only effective/provider-confirmed fields;
    the requested configuration is never used as display or runtime truth.
    """
    config = dict(config_json or {})
    options = [
        dict(item)
        for item in (config.get("options") or [])
        if isinstance(item, dict)
    ]
    values = dict(config.get("values") or {})
    for option in options:
        oid = option_id(option)
        current = option_current_value(option)
        if oid is not None and current is not None:
            values[oid] = current

    admission = dict(config.get("configuration") or {})
    effective = dict(admission.get("effective") or {})
    effective_config = dict(effective.get("config") or {})

    def option_value(semantic: str) -> str | bool | None:
        try:
            option = find_option(options, semantic)
        except ACPConfigurationError:
            # Ambiguous provider metadata is not confirmation.  Retain the last
            # unambiguous durable value instead of selecting an arbitrary option.
            option = None
        return option_current_value(option) if option is not None else None

    raw_model = (
        option_value("model")
        or _semantic_mapping_value(values, "model")
        or _semantic_mapping_value(effective_config, "model")
        or effective.get("model_id")
        or state_current_value(
            config.get("models"), ("currentModelId", "current_model_id")
        )
        or model_id
    )
    confirmed_model, selector_reasoning = parse_model_selector(
        str(raw_model) if raw_model is not None else None
    )
    raw_mode = (
        option_value("mode")
        or _semantic_mapping_value(values, "mode")
        or _semantic_mapping_value(effective_config, "mode")
        or effective.get("mode_id")
        or state_current_value(
            config.get("modes"), ("currentModeId", "current_mode_id")
        )
        or mode_id
    )
    raw_reasoning = (
        option_value("reasoning")
        or _semantic_mapping_value(values, "reasoning")
        or _semantic_mapping_value(effective_config, "reasoning")
        or effective.get("reasoning")
        or selector_reasoning
    )
    return {
        "model_id": confirmed_model,
        "mode_id": str(raw_mode) if raw_mode is not None else None,
        "reasoning": str(raw_reasoning) if raw_reasoning is not None else None,
        "values": values,
    }


def normalized_session_config_json(
    config_json: Mapping[str, Any] | None,
    *,
    model_id: str | None = None,
    mode_id: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a normalized copy plus its confirmed model/mode/effort fields."""
    config = dict(config_json or {})
    confirmed = confirmed_session_configuration(
        config, model_id=model_id, mode_id=mode_id
    )
    config["values"] = confirmed["values"]
    admission = dict(config.get("configuration") or {})
    if admission.get("state") == "ready" and admission.get("effective") is not None:
        effective = dict(admission.get("effective") or {})
        effective_config = dict(effective.get("config") or {})
        effective_config.update(confirmed["values"])
        effective["config"] = effective_config
        for key in ("model_id", "mode_id", "reasoning"):
            if confirmed[key] is not None:
                effective[key] = confirmed[key]
        admission["effective"] = effective
        config["configuration"] = admission
    return config, confirmed
