from __future__ import annotations

import importlib.metadata
import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from pa.core.contracts import Module

if TYPE_CHECKING:
    from pa.core.context import AppContext

logger = logging.getLogger(__name__)

ENTRYPOINT_GROUP = "pa.modules"


@dataclass
class LoadedModule:
    module: Module
    source: str


class ModuleRegistry:
    """Discovers, loads, and lifecycle-manages PA modules."""

    def __init__(self, ctx: AppContext) -> None:
        self.ctx = ctx
        self._loaded: list[LoadedModule] = []

    @property
    def modules(self) -> list[LoadedModule]:
        return list(self._loaded)

    def register(self, module: Module, *, source: str = "builtin") -> None:
        if any(entry.module.name == module.name for entry in self._loaded):
            raise ValueError(f"Module already registered: {module.name}")

        module.on_load(self.ctx)
        self._loaded.append(LoadedModule(module=module, source=source))
        logger.debug("Registered module %s (%s)", module.name, source)

    def load_entrypoints(self) -> None:
        try:
            eps = importlib.metadata.entry_points(group=ENTRYPOINT_GROUP)
        except TypeError:
            eps = importlib.metadata.entry_points().get(ENTRYPOINT_GROUP, [])

        for ep in eps:
            try:
                factory = ep.load()
                module = factory() if callable(factory) else factory
                if not isinstance(module, Module):
                    logger.warning(
                        "Entry point %s did not return a Module instance", ep.name
                    )
                    continue
                self.register(module, source=f"entrypoint:{ep.name}")
            except Exception:
                logger.exception("Failed to load module entry point %s", ep.name)

    def load_builtins(self) -> None:
        from pa.modules.agent_chat import AgentChatModule
        from pa.modules.agent_providers import AgentProvidersModule
        from pa.modules.auth import AuthModule
        from pa.modules.browser import BrowserModule
        from pa.modules.debug import DebugModule
        from pa.modules.fleet import FleetModule
        from pa.modules.files import FilesModule
        from pa.modules.integrations import IntegrationsModule
        from pa.modules.instance import InstanceModule
        from pa.modules.items import ItemsModule
        from pa.modules.projects import ProjectsModule
        from pa.modules.pr_supervisor import PRSupervisorModule
        from pa.modules.sync import SyncModule
        from pa.modules.theme import ThemeModule
        from pa.modules.trust import TrustModule
        from pa.modules.ui_shell import UiShellModule

        for module in (
            AuthModule(),
            FleetModule(),
            SyncModule(),
            IntegrationsModule(),
            ProjectsModule(),
            PRSupervisorModule(),
            TrustModule(),
            ItemsModule(),
            InstanceModule(),
            AgentChatModule(),
            BrowserModule(),
            AgentProvidersModule(),
            ThemeModule(),
            DebugModule(),
            FilesModule(),
            UiShellModule(),
        ):
            self.register(module, source="builtin")

    def load_all(self) -> None:
        self.load_builtins()
        self.load_entrypoints()

    def describe(self) -> list[dict]:
        return [
            {
                "name": entry.module.name,
                "version": entry.module.version,
                "description": entry.module.description,
                "source": entry.source,
            }
            for entry in self._loaded
        ]
