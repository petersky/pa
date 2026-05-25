from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from starlette.requests import Request


@dataclass(frozen=True)
class PageDefinition:
    """A routable SPA page in the PA web UI."""

    id: str
    path: str
    label: str
    icon: str
    template: str
    nav: bool = True
    nav_order: int = 100
    context_builder: Callable[[Request], dict[str, Any]] | None = None

    def build_context(self, request: Request) -> dict[str, Any]:
        if self.context_builder:
            return self.context_builder(request)
        return {}


@dataclass
class PageRegistry:
    """Collects pages from modules for the SPA router."""

    _pages: dict[str, PageDefinition] = field(default_factory=dict)
    _by_path: dict[str, PageDefinition] = field(default_factory=dict)

    def register(self, page: PageDefinition) -> None:
        if page.id in self._pages:
            raise ValueError(f"Page already registered: {page.id}")
        normalized = page.path if page.path.startswith("/") else f"/{page.path}"
        if normalized != "/" and normalized.endswith("/"):
            normalized = normalized.rstrip("/")
        if normalized in self._by_path:
            raise ValueError(f"Path already registered: {normalized}")

        page = PageDefinition(
            id=page.id,
            path=normalized,
            label=page.label,
            icon=page.icon,
            template=page.template,
            nav=page.nav,
            nav_order=page.nav_order,
            context_builder=page.context_builder,
        )
        self._pages[page.id] = page
        self._by_path[normalized] = page

    def get(self, page_id: str) -> PageDefinition | None:
        return self._pages.get(page_id)

    def get_by_path(self, path: str) -> PageDefinition | None:
        normalized = path if path.startswith("/") else f"/{path}"
        if normalized != "/" and normalized.endswith("/"):
            normalized = normalized.rstrip("/")
        return self._by_path.get(normalized)

    def nav_pages(self) -> list[PageDefinition]:
        return sorted(
            (p for p in self._pages.values() if p.nav),
            key=lambda p: (p.nav_order, p.label),
        )

    def all_pages(self) -> list[PageDefinition]:
        return list(self._pages.values())
