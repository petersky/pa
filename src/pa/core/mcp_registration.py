"""Deterministic registration guards for the shared MCP tool surface."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from threading import Lock
from typing import Any


class DuplicateMcpToolError(RuntimeError):
    """Raised before an MCP implementation can silently replace a tool."""


@dataclass
class _ToolRegistry:
    origins: dict[str, str] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)


_REGISTRY_ATTRIBUTE = "_pa_mcp_tool_registration_registry"
_registry_creation_lock = Lock()


def _registry_for(delegate: Any) -> _ToolRegistry:
    """Keep the guard stable when registration is repeated on one server."""
    with _registry_creation_lock:
        registry = getattr(delegate, _REGISTRY_ATTRIBUTE, None)
        if isinstance(registry, _ToolRegistry):
            return registry
        registry = _ToolRegistry()
        try:
            setattr(delegate, _REGISTRY_ATTRIBUTE, registry)
        except (AttributeError, TypeError):
            # The production MCP server accepts private attributes. This fallback
            # still gives slot-based developer fakes deterministic protection for
            # one complete Kernel.register_mcp pass.
            pass
        return registry


class UniqueToolRegistrationProxy:
    """Reserve each tool name once before delegating to the MCP SDK."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self._registry = _registry_for(delegate)

    def tool(self, *args, **kwargs) -> Callable:
        register = self._delegate.tool(*args, **kwargs)
        explicit_name = kwargs.get("name")
        if explicit_name is None and args:
            explicit_name = args[0]

        def unique(fn: Callable) -> Callable:
            name = str(explicit_name or fn.__name__)
            origin = f"{fn.__module__}.{fn.__qualname__}"
            with self._registry.lock:
                previous = self._registry.origins.get(name)
                if previous is not None:
                    raise DuplicateMcpToolError(
                        "duplicate MCP tool registration "
                        f"for {name!r}: first={previous}, duplicate={origin}"
                    )
                self._registry.origins[name] = origin
            try:
                return register(fn)
            except BaseException:
                with self._registry.lock:
                    if self._registry.origins.get(name) == origin:
                        self._registry.origins.pop(name, None)
                raise

        return unique

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)
