"""Prompt metadata, validation, rendering, and provider-size adapters."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field, model_validator

_PLACEHOLDER = re.compile(r"{{\s*([A-Za-z_][A-Za-z0-9_.]*)\s*}}")
_ANY_PLACEHOLDER = re.compile(r"{{.*?}}", re.DOTALL)
_SECRET_KEY = re.compile(
    r"(?:^|[_.-])(?:authorization|cookie|credential|password|secret|token|api[_-]?key)(?:$|[_.-])",
    re.IGNORECASE,
)
_SECRET_VALUE_PATTERNS = (
    re.compile(r"\b(?:sk|gh[opusr])_[A-Za-z0-9_-]{12,}\b"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]{12,}"),
    re.compile(r"(?i)\b(?:password|secret|token|api[_-]?key)\s*[:=]\s*[^\s,;]+"),
)


class PromptRenderError(ValueError):
    """A prompt cannot be rendered safely or within its provider limit."""


class PromptVariable(BaseModel):
    name: str
    type: Literal["string", "integer", "boolean", "object", "array"] = "string"
    description: str
    required: bool = True
    secret: bool = False
    audit: bool = True
    example: Any = None


class PromptDefinition(BaseModel):
    key: str
    purpose: str
    scope: Literal[
        "global",
        "session",
        "project",
        "card",
        "remote-dispatch",
        "pr-supervisor",
        "release",
    ]
    version: int = Field(ge=1)
    source: str = "pa:builtin"
    template: str
    variables: tuple[PromptVariable, ...] = ()
    max_characters: int = Field(default=131_072, ge=1)
    read_only: bool = True

    @model_validator(mode="after")
    def validate_schema(self) -> PromptDefinition:
        declared = {variable.name for variable in self.variables}
        placeholders = set(_PLACEHOLDER.findall(self.template))
        if _ANY_PLACEHOLDER.search(_PLACEHOLDER.sub("", self.template)):
            raise ValueError(f"prompt {self.key} contains an invalid placeholder")
        if placeholders != declared:
            missing = sorted(placeholders - declared)
            unused = sorted(declared - placeholders)
            raise ValueError(
                f"prompt {self.key} variable schema mismatch; "
                f"undeclared={missing}, unused={unused}"
            )
        if any(variable.secret for variable in self.variables):
            raise ValueError(f"prompt {self.key} declares a forbidden secret variable")
        if any(_SECRET_KEY.search(variable.name) for variable in self.variables):
            raise ValueError(f"prompt {self.key} declares a secret-like variable")
        return self


class ProviderPromptAdapter(BaseModel):
    provider: str
    max_characters: int
    context_reserve_characters: int = Field(default=0, ge=0)
    source: str = "pa:builtin"
    version: int = 1

    @model_validator(mode="after")
    def validate_context_reserve(self) -> ProviderPromptAdapter:
        if self.context_reserve_characters >= self.max_characters:
            raise ValueError("prompt context reserve must be smaller than the limit")
        return self


class RenderedPrompt(BaseModel):
    key: str
    version: int
    source: str
    scope: str
    provider: str
    text: str
    resolved_context: dict[str, Any]
    character_count: int

    def audit_record(self) -> dict[str, Any]:
        return self.model_dump(exclude={"text"}, mode="json")


def _lookup(context: Mapping[str, Any], path: str) -> Any:
    value: Any = context
    for part in path.split("."):
        if isinstance(value, BaseModel):
            value = getattr(value, part, None)
        elif isinstance(value, Mapping) and part in value:
            value = value[part]
        else:
            raise PromptRenderError(f"required prompt variable is missing: {path}")
    if value is None:
        raise PromptRenderError(f"required prompt variable is missing: {path}")
    return value


def _redact_text(value: str) -> str:
    result = value
    for pattern in _SECRET_VALUE_PATTERNS:
        result = pattern.sub("[REDACTED]", result)
    return result


def redact_value(value: Any, *, path: str = "") -> Any:
    if path and _SECRET_KEY.search(path):
        return "[REDACTED]"
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping):
        return {
            str(key): redact_value(item, path=f"{path}.{key}" if path else str(key))
            for key, item in value.items()
        }
    if isinstance(value, list | tuple):
        return [redact_value(item, path=path) for item in value]
    if isinstance(value, str):
        return _redact_text(value)
    return value


def _stringify(value: Any) -> str:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json")
    if isinstance(value, Mapping | list | tuple):
        return json.dumps(value, ensure_ascii=False, indent=2, default=str).replace(
            "<", "\\u003c"
        )
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _assign_nested(target: dict[str, Any], path: str, value: Any) -> None:
    node = target
    parts = path.split(".")
    for part in parts[:-1]:
        child = node.get(part)
        if not isinstance(child, dict):
            child = {}
            node[part] = child
        node = child
    node[parts[-1]] = value


class PromptRegistry:
    def __init__(self) -> None:
        self._definitions: dict[str, PromptDefinition] = {}
        self._adapters: dict[str, ProviderPromptAdapter] = {
            "default": ProviderPromptAdapter(
                provider="default",
                max_characters=131_072,
                context_reserve_characters=65_536,
            ),
            "cursor": ProviderPromptAdapter(
                provider="cursor",
                max_characters=131_072,
                context_reserve_characters=65_536,
            ),
            "codex": ProviderPromptAdapter(
                provider="codex",
                max_characters=262_144,
                context_reserve_characters=131_072,
            ),
        }

    def register(self, definition: PromptDefinition) -> None:
        if definition.key in self._definitions:
            raise ValueError(f"duplicate prompt key: {definition.key}")
        self._definitions[definition.key] = definition

    def get(self, key: str) -> PromptDefinition:
        try:
            return self._definitions[key]
        except KeyError as exc:
            raise PromptRenderError(f"unknown prompt key: {key}") from exc

    def all(self) -> list[PromptDefinition]:
        return sorted(self._definitions.values(), key=lambda item: item.key)

    def adapters(self) -> list[ProviderPromptAdapter]:
        return sorted(self._adapters.values(), key=lambda item: item.provider)

    def render(
        self,
        key: str,
        context: Mapping[str, Any] | None = None,
        *,
        provider: str | None = None,
        reserve_context: bool = False,
    ) -> RenderedPrompt:
        definition = self.get(key)
        supplied = context or {}
        values: dict[str, Any] = {}
        audit: dict[str, Any] = {}
        for variable in definition.variables:
            try:
                value = _lookup(supplied, variable.name)
            except PromptRenderError:
                if variable.required:
                    raise
                value = ""
            if _SECRET_KEY.search(variable.name):
                raise PromptRenderError(
                    f"secret fields are forbidden in prompts: {variable.name}"
                )
            safe_value = redact_value(value, path=variable.name)
            # Referenced values are rendered from the recursively redacted copy;
            # secret-shaped nested fields must never reach the provider.
            value = safe_value
            values[variable.name] = value
            if variable.audit:
                _assign_nested(audit, variable.name, safe_value)

        def replace(match: re.Match[str]) -> str:
            return _stringify(values[match.group(1)])

        rendered = _PLACEHOLDER.sub(replace, definition.template).strip()
        provider_id = (provider or "default").strip().lower() or "default"
        adapter = self._adapters.get(provider_id, self._adapters["default"])
        provider_limit = adapter.max_characters
        if reserve_context:
            provider_limit -= adapter.context_reserve_characters
        limit = min(definition.max_characters, provider_limit)
        if len(rendered) > limit:
            raise PromptRenderError(
                f"prompt {key} is {len(rendered)} characters; "
                f"provider {provider_id} limit is {limit}"
            )
        return RenderedPrompt(
            key=definition.key,
            version=definition.version,
            source=definition.source,
            scope=definition.scope,
            provider=provider_id,
            text=rendered,
            resolved_context=audit,
            character_count=len(rendered),
        )

    def catalog(self, *, provider: str = "default") -> list[dict[str, Any]]:
        rows = []
        for definition in self.all():
            synthetic = {
                variable.name: variable.example
                for variable in definition.variables
                if variable.example is not None
            }
            nested: dict[str, Any] = {}
            for path, value in synthetic.items():
                _assign_nested(nested, path, value)
            preview = self.render(definition.key, nested, provider=provider)
            rows.append(
                {
                    **definition.model_dump(mode="json", exclude={"template"}),
                    "provenance": "Built-in (read-only)",
                    "effective_template": definition.template,
                    "preview": preview.text,
                    "preview_context": preview.resolved_context,
                }
            )
        return rows
