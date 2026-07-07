from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from pa.auth.middleware import get_principal_id
from pa.config import get_settings
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    CardCreate,
    CardKind,
    CardLane,
    CardUpdate,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    ItemUpdate,
)
from pa.domain.store import get_store

router = APIRouter()
ui_router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _active_realm(request: Request) -> str:
    return request.query_params.get("realm") or request.app.state.ctx.settings.primary_realm


def _active_project(request: Request) -> str | None:
    return request.query_params.get("project")


def _cards_context(request: Request, *, kind: CardKind | None = None, lane: CardLane | None = None) -> dict:
    store = get_store()
    realm = _active_realm(request)
    project_id = _active_project(request)
    cards = store.list_cards(
        realm_id=realm,
        kind=kind,
        lane=lane,
        project_id=project_id,
    )
    return {
        "cards": cards,
        "items": [Item.from_card(c) for c in cards],
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": store.list_projects(realm_id=realm),
        "realms": request.app.state.ctx.settings.subscribed_realms,
        "active_realm": realm,
        "active_project": project_id,
    }


def _items_context(request: Request, *, kind: ItemKind | None = None) -> dict:
    ctx = _cards_context(request, kind=CardKind(kind.value) if kind else None)
    ctx["kinds"] = list(ItemKind)
    ctx["statuses"] = list(ItemStatus)
    return ctx


def _home_context(request: Request) -> dict:
    store = get_store()
    realm = _active_realm(request)
    return {
        **_cards_context(request),
        "knowledge": store.list_knowledge(limit=10),
        "active_realm": realm,
    }


def _knowledge_context(request: Request) -> dict:
    store = get_store()
    realm = _active_realm(request)
    return {
        "knowledge": store.list_knowledge(limit=50),
        "cards": store.list_cards(realm_id=realm),
        "items": store.list_cards(realm_id=realm),
        "realms": get_settings().subscribed_realms,
        "active_realm": realm,
    }


@router.get("/cards")
def list_cards_api(
    request: Request,
    realm: str | None = None,
    lane: CardLane | None = None,
    kind: CardKind | None = None,
) -> list[dict]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    cards = get_store().list_cards(realm_id=realm_id, lane=lane, kind=kind)
    return [c.model_dump(mode="json") for c in cards]


@router.post("/cards", status_code=201)
def create_card_api(request: Request, data: CardCreate) -> dict:
    store = get_store()
    settings = request.app.state.ctx.settings
    card = store.create_card(
        data,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    return card.model_dump(mode="json")


@router.get("/cards/{card_id}")
def get_card_api(request: Request, card_id: str, realm: str | None = None) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return card.model_dump(mode="json")


@router.patch("/cards/{card_id}")
def update_card_api(request: Request, card_id: str, data: CardUpdate, realm: str | None = None) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    card = get_store().update_card(
        card_id,
        data,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return card.model_dump(mode="json")


@router.get("/items")
def list_items(kind: ItemKind | None = None, status: ItemStatus | None = None) -> list[dict]:
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
    realm = _active_realm(request)
    get_store().create_card(
        ItemCreate(kind=kind, title=title, body=body).to_card_create(realm),
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return RedirectResponse(url="/", status_code=303)


@ui_router.post("/cards")
def create_card_ui(
    request: Request,
    kind: CardKind = Form(CardKind.TASK),
    title: str = Form(...),
    body: str = Form(""),
    lane: CardLane = Form(CardLane.INBOX),
) -> RedirectResponse:
    realm = _active_realm(request)
    get_store().create_card(
        CardCreate(realm_id=realm, kind=kind, title=title, body=body, lane=lane),
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return RedirectResponse(url=f"/work?realm={realm}", status_code=303)


@ui_router.get("/partials/items", response_class=HTMLResponse)
def items_partial(request: Request, kind: ItemKind | None = None) -> HTMLResponse:
    realm = _active_realm(request)
    cards = get_store().list_cards(
        realm_id=realm,
        kind=CardKind(kind.value) if kind else None,
    )
    return _templates(request).TemplateResponse(
        request,
        "partials/items.html",
        {"items": [Item.from_card(c) for c in cards], "cards": cards},
    )


@ui_router.get("/partials/cards", response_class=HTMLResponse)
def cards_partial(
    request: Request,
    lane: CardLane | None = None,
    realm: str | None = None,
    project: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    project_id = project or _active_project(request)
    cards = get_store().list_cards(realm_id=realm_id, lane=lane, project_id=project_id)
    return _templates(request).TemplateResponse(
        request,
        "partials/cards.html",
        {"cards": cards, "lane": lane},
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
        return "0.2.0"

    @property
    def description(self) -> str:
        return "Cards (goals, tasks, projects, concerns) and knowledge capture"

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
        return [("/api", router, ["items", "cards"])]

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
        def list_cards(realm: str = "default", lane: str | None = None) -> list[dict]:
            """List cards in a realm."""
            cards = store.list_cards(
                realm_id=realm,
                lane=CardLane(lane) if lane else None,
            )
            return [c.model_dump(mode="json") for c in cards]

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
