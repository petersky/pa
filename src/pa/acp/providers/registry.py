"""Registry of built-in ACP agent providers."""

from __future__ import annotations

from typing import Iterable

from pa.acp.providers.base import AgentProvider, AgentProviderId
from pa.acp.providers.codex import CodexProvider
from pa.acp.providers.cursor import CursorProvider

_PROVIDERS: dict[str, AgentProvider] = {}


def _ensure_builtins() -> None:
    if _PROVIDERS:
        return
    for provider in (CursorProvider(), CodexProvider()):
        _PROVIDERS[provider.id] = provider


def register_provider(provider: AgentProvider) -> None:
    """Register or replace a provider (for plugins / future Cortex, etc.)."""
    _ensure_builtins()
    _PROVIDERS[provider.id] = provider


def list_providers() -> list[AgentProvider]:
    _ensure_builtins()
    return list(_PROVIDERS.values())


def list_provider_ids() -> list[str]:
    return [p.id for p in list_providers()]


def get_provider(provider_id: str) -> AgentProvider:
    _ensure_builtins()
    key = (provider_id or "").strip().lower()
    if key not in _PROVIDERS:
        known = ", ".join(sorted(_PROVIDERS))
        raise KeyError(f"Unknown ACP provider {provider_id!r}. Known: {known}")
    return _PROVIDERS[key]


def known_provider_ids() -> Iterable[str]:
    _ensure_builtins()
    return tuple(_PROVIDERS.keys())


DEFAULT_PROVIDER_ID = AgentProviderId.CURSOR.value
