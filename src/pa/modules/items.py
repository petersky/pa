from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import ItemCreate, ItemKind, ItemStatus, ItemUpdate
from pa.domain.store import get_store

router = APIRouter()
ui_router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _items_context(request: Request, *, kind: ItemKind | None = None) -> dict:
    store = get_store()
    return {
        "items": store.list_items(kind=kind),
        "kinds": list(ItemKind),
        "statuses": list(ItemStatus),
    }


def _home_context(request: Request) -> dict:
    store = get_store()
    ctx = {
        **_items_context(request),
        "knowledge": store.list_knowledge(limit=10),
    }
    return ctx


def _knowledge_context(request: Request) -> dict:
    store = get_store()
    return {
        "knowledge": store.list_knowledge(limit=50),
        "items": store.list_items(),
    }


@router.get("/items")
def list_items(
    kind: ItemKind | None = None,
    status: ItemStatus | None = None,
) -> list[dict]:
    items = get_store().list_items(kind=kind, status=status)
    return [item.model_dump(mode="json") for item in items]


@router.post("/items", status_code=201)
def create_item(data: ItemCreate) -> dict:
    item = get_store().create_item(data)
    return item.model_dump(mode="json")


@router.get("/items/{item_id}")
def get_item(item_id: str) -> dict:
    item = get_store().get_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item.model_dump(mode="json")


@router.patch("/items/{item_id}")
def update_item(item_id: str, data: ItemUpdate) -> dict:
    item = get_store().update_item(item_id, data)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item.model_dump(mode="json")


@router.delete("/items/{item_id}", status_code=204)
def delete_item(item_id: str) -> None:
    if not get_store().delete_item(item_id):
        raise HTTPException(status_code=404, detail="Item not found")


@router.get("/knowledge")
def list_knowledge(item_id: str | None = None, limit: int = 50) -> list[dict]:
    entries = get_store().list_knowledge(item_id=item_id, limit=limit)
    return [entry.model_dump(mode="json") for entry in entries]


@ui_router.post("/items")
def create_item_ui(
    request: Request,
    kind: ItemKind = Form(...),
    title: str = Form(...),
    body: str = Form(""),
) -> RedirectResponse:
    get_store().create_item(ItemCreate(kind=kind, title=title, body=body))
    return RedirectResponse(url="/", status_code=303)


@ui_router.get("/partials/items", response_class=HTMLResponse)
def items_partial(request: Request, kind: ItemKind | None = None) -> HTMLResponse:
    items = get_store().list_items(kind=kind)
    return _templates(request).TemplateResponse(
        request,
        "partials/items.html",
        {"items": items},
    )


@ui_router.get("/partials/knowledge", response_class=HTMLResponse)
def knowledge_partial(request: Request) -> HTMLResponse:
    knowledge = get_store().list_knowledge(limit=20)
    return _templates(request).TemplateResponse(
        request,
        "partials/knowledge.html",
        {"knowledge": knowledge},
    )


class ItemsModule(Module):
    @property
    def name(self) -> str:
        return "items"

    @property
    def version(self) -> str:
        return "0.0.1"

    @property
    def description(self) -> str:
        return "Goals, tasks, projects, concerns, and knowledge capture"

    def on_load(self, ctx: AppContext) -> None:
        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="home",
                path="/",
                label="Home",
                icon="home",
                template="pages/home.html",
                nav_order=0,
                context_builder=_home_context,
            )
        )
        pages.register(
            PageDefinition(
                id="work",
                path="/work",
                label="Work",
                icon="work",
                template="pages/work.html",
                nav_order=10,
                context_builder=_items_context,
            )
        )
        pages.register(
            PageDefinition(
                id="knowledge",
                path="/knowledge",
                label="Knowledge",
                icon="knowledge",
                template="pages/knowledge.html",
                nav_order=20,
                context_builder=_knowledge_context,
            )
        )

    def api_routers(self):
        return [("/api", router, ["items"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        store = ctx.store

        @mcp.tool()
        def list_items(kind: str | None = None, status: str | None = None) -> list[dict]:
            """List goals, tasks, projects, and concerns."""
            items = store.list_items(
                kind=ItemKind(kind) if kind else None,
                status=ItemStatus(status) if status else None,
            )
            return [item.model_dump(mode="json") for item in items]

        @mcp.tool()
        def create_item(
            kind: str,
            title: str,
            body: str = "",
            status: str = "open",
            parent_id: str | None = None,
        ) -> dict:
            """Create a goal, task, project, or concern."""
            item = store.create_item(
                ItemCreate(
                    kind=ItemKind(kind),
                    title=title,
                    body=body,
                    status=ItemStatus(status),
                    parent_id=parent_id,
                )
            )
            return item.model_dump(mode="json")

        @mcp.tool()
        def get_item(item_id: str) -> dict | None:
            """Get a single item by ID."""
            item = store.get_item(item_id)
            return item.model_dump(mode="json") if item else None

        @mcp.tool()
        def list_knowledge(item_id: str | None = None, limit: int = 20) -> list[dict]:
            """List captured knowledge from agent sessions."""
            entries = store.list_knowledge(item_id=item_id, limit=limit)
            return [entry.model_dump(mode="json") for entry in entries]
