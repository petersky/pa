from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pa.config import Settings
    from pa.core.hooks import HookBus
    from pa.domain.store import Store


@dataclass
class AppContext:
    """Shared runtime context passed to every module."""

    settings: Settings
    hooks: HookBus
    store: Store
    services: dict[str, Any] = field(default_factory=dict)

    def register_service(self, name: str, service: Any) -> None:
        self.services[name] = service

    def get_service(self, name: str) -> Any:
        return self.services[name]

    def require_service(self, name: str) -> Any:
        if name not in self.services:
            raise KeyError(f"Service not registered: {name}")
        return self.services[name]
