"""Stable contracts between PA core and modules/plugins."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from fastapi import APIRouter, FastAPI
    from mcp.server.fastmcp import FastMCP

    from pa.core.context import AppContext


class Module(ABC):
    """Extension point for PA capabilities.

    Built-in features and third-party plugins implement this interface.
    External packages register via the ``pa.modules`` entry-point group.
    """

    @property
    @abstractmethod
    def name(self) -> str: ...

    @property
    def version(self) -> str:
        return "0.0.0"

    @property
    def description(self) -> str:
        return ""

    def on_load(self, ctx: AppContext) -> None:
        """Called when the module is registered."""

    async def on_startup(self, app: FastAPI, ctx: AppContext) -> None:
        """Called during application startup."""

    async def on_shutdown(self, app: FastAPI, ctx: AppContext) -> None:
        """Called during application shutdown."""

    def api_routers(self) -> list[tuple[str, APIRouter, list[str] | None]]:
        """Return ``(prefix, router, tags)`` tuples for REST APIs."""

        return []

    def ui_routers(self) -> list[APIRouter]:
        """Return UI (HTMX) routers mounted at the app root."""

        return []

    def register_mcp(self, mcp: FastMCP, ctx: AppContext) -> None:
        """Register MCP tools/resources on the shared server."""

    def cli_commands(self) -> list[Any]:
        """Return Typer command callables to attach to ``pa``."""

        return []

    def static_mounts(self) -> list[tuple[str, str]]:
        """Return ``(url_path, filesystem_path)`` static directory mounts."""

        return []

    def template_dirs(self) -> list[str]:
        """Additional Jinja2 template search paths."""

        return []

    def ui_pages(self) -> list[Any]:
        """Register SPA pages via ``PageRegistry`` in ``on_load`` instead."""

        return []
