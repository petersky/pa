from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from pa.auth.csrf import token_for_request
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
from pa.domain.session_selection import preferred_sessions_by_card

router = APIRouter()
ui_router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _active_realm(request: Request) -> str:
    return (
        request.query_params.get("realm")
        or request.app.state.ctx.settings.primary_realm
    )


def _active_project(request: Request) -> str | None:
    return request.query_params.get("project")


def _pr_watch_context(request: Request, card_id: str) -> dict:
    store = request.app.state.ctx.services.get("pr_supervisor_store")
    if not store:
        return {"pr_watches": [], "pr_watch_events": {}}
    watches = store.list_watches(card_id=card_id, include_retired=True)
    return {
        "pr_watches": watches,
        "pr_watch_events": {
            watch.id: store.list_events(watch.id, limit=20) for watch in watches
        },
    }


def _card_detail_context(request: Request, card) -> dict:
    store = get_store()
    realm_id = card.realm_id
    project = (
        store.get_project(card.project_id, realm_id=realm_id)
        if card.project_id
        else None
    )
    related_sessions = [
        session for session in store.list_sessions() if session.card_id == card.id
    ]
    return {
        "card": card,
        "project": project,
        "parent": (
            store.get_card(card.parent_id, realm_id=realm_id)
            if card.parent_id
            else None
        ),
        "children": [
            candidate
            for candidate in store.list_cards(realm_id=realm_id)
            if candidate.parent_id == card.id
        ],
        "related_sessions": related_sessions,
        "current_session": next(
            (session for session in related_sessions if session.status != "closed"),
            None,
        ),
        "card_knowledge": store.list_knowledge(item_id=card.id, limit=10),
        "lanes": list(CardLane),
        "csrf_token": token_for_request(request),
        "agent_enabled": request.app.state.ctx.settings.agent_enabled,
        **_pr_watch_context(request, card.id),
    }


def _cards_context(
    request: Request,
    *,
    kind: CardKind | None = None,
    lane: CardLane | None = None,
    apply_filters: bool = True,
) -> dict:
    store = get_store()
    realm = _active_realm(request)
    project_id = _active_project(request) if apply_filters else None
    if apply_filters and kind is None and request.query_params.get("kind"):
        try:
            kind = CardKind(request.query_params["kind"])
        except ValueError:
            kind = None
    cards = store.list_cards(
        realm_id=realm,
        kind=kind,
        lane=lane,
        project_id=project_id,
    )
    query = request.query_params.get("q", "").strip() if apply_filters else ""
    owner = request.query_params.get("owner", "").strip() if apply_filters else ""
    instance = request.query_params.get("instance", "").strip() if apply_filters else ""
    blocked = request.query_params.get("blocked", "").strip() if apply_filters else ""
    tag = request.query_params.get("tag", "").strip() if apply_filters else ""
    updated = request.query_params.get("updated", "").strip() if apply_filters else ""
    all_cards = store.list_cards(realm_id=realm)
    if query:
        needle = query.casefold()
        cards = [
            card
            for card in cards
            if needle in " ".join((card.title, card.summary, card.body)).casefold()
        ]
    if owner:
        cards = [card for card in cards if card.owner_principal == owner]
    if instance:
        cards = [card for card in cards if card.preferred_instance == instance]
    if blocked == "blocked":
        cards = [card for card in cards if card.lane == CardLane.WAITING]
    elif blocked == "unblocked":
        cards = [card for card in cards if card.lane != CardLane.WAITING]
    if tag:
        cards = [card for card in cards if tag in card.tags]
    if updated:
        try:
            cutoff = datetime.now(UTC) - timedelta(days=int(updated))
            cards = [card for card in cards if card.updated_at >= cutoff]
        except ValueError:
            updated = ""
    projects = store.list_projects(realm_id=realm)
    project_by_id = {project.id: project for project in projects}
    card_sessions = preferred_sessions_by_card(store.list_sessions())
    filter_params = {
        "realm": realm,
        "project": project_id or "",
        "q": query,
        "kind": kind.value if kind else "",
        "owner": owner,
        "instance": instance,
        "blocked": blocked,
        "tag": tag,
        "updated": updated,
    }
    return {
        "cards": cards,
        "items": [Item.from_card(c) for c in cards],
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": projects,
        "card_projects": {
            card.id: project_by_id.get(card.project_id) for card in cards
        },
        "card_sessions": card_sessions,
        "owners": sorted(
            {card.owner_principal for card in all_cards if card.owner_principal}
        ),
        "instances": sorted(
            {card.preferred_instance for card in all_cards if card.preferred_instance}
        ),
        "tags": sorted({tag for card in all_cards for tag in card.tags}),
        "filters": {
            "q": query,
            "project": project_id or "",
            "kind": kind.value if kind else "",
            "owner": owner,
            "instance": instance,
            "blocked": blocked,
            "tag": tag,
            "updated": updated,
        },
        "filter_query": urlencode(
            {key: value for key, value in filter_params.items() if value}
        ),
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
    realm = _active_realm(request)
    context = _cards_context(request, apply_filters=False)
    cards = context["cards"]
    ctx = request.app.state.ctx
    agent = ctx.services.get("instance_agent")
    fleet = ctx.services.get("fleet_registry")
    return {
        **context,
        "needs_attention": [
            card
            for card in cards
            if card.lane == CardLane.WAITING
            or (card.kind == CardKind.CONCERN and card.lane != CardLane.DONE)
        ][:6],
        "active_work": [card for card in cards if card.lane == CardLane.ACTIVE][:8],
        "recent_outcomes": [card for card in cards if card.lane == CardLane.DONE][:6],
        "agent_connected": bool(agent and agent.connected),
        "fleet_instances": fleet.list_instances() if fleet else [],
        "instance_name": ctx.settings.instance_name,
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
def update_card_api(
    request: Request, card_id: str, data: CardUpdate, realm: str | None = None
) -> dict:
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
def list_items(
    kind: ItemKind | None = None, status: ItemStatus | None = None
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


@ui_router.post("/items", response_model=None)
def create_item_ui(
    request: Request,
    kind: ItemKind = Form(...),
    title: str = Form(...),
    body: str = Form(""),
) -> HTMLResponse | RedirectResponse:
    realm = _active_realm(request)
    get_store().create_card(
        ItemCreate(kind=kind, title=title, body=body).to_card_create(realm),
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    if request.headers.get("HX-Request"):
        from pa.modules.ui_shell import render_page

        page = request.app.state.ctx.require_service("pages").get_by_path("/")
        if not page:
            raise HTTPException(status_code=404)
        return render_page(request, page)
    return RedirectResponse(url=f"/?realm={realm}", status_code=303)


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
    context = _cards_context(request, kind=CardKind(kind.value) if kind else None)
    return _templates(request).TemplateResponse(
        request,
        "partials/items.html",
        context,
    )


@ui_router.get("/partials/cards", response_class=HTMLResponse)
def cards_partial(
    request: Request,
    lane: CardLane | None = None,
    realm: str | None = None,
    project: str | None = None,
) -> HTMLResponse:
    context = _cards_context(request, lane=lane)
    return _templates(request).TemplateResponse(
        request,
        "partials/cards.html",
        {**context, "lane": lane},
    )


@ui_router.get("/partials/card-detail-empty", response_class=HTMLResponse)
def card_detail_empty(request: Request) -> HTMLResponse:
    return _templates(request).TemplateResponse(
        request,
        "partials/card-detail-empty.html",
        {},
    )


@ui_router.get("/partials/cards/{card_id}/detail", response_class=HTMLResponse)
def card_detail_partial(
    request: Request, card_id: str, realm: str | None = None
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    store = get_store()
    card = store.get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request,
        "partials/card-detail.html",
        _card_detail_context(request, card),
    )


@ui_router.post("/partials/cards/{card_id}", response_model=None)
def card_detail_update(
    request: Request,
    card_id: str,
    title: str = Form(...),
    body: str = Form(""),
    summary: str = Form(""),
    lane: CardLane = Form(...),
    realm: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    store = get_store()
    existing = store.get_card(card_id, realm_id=realm_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Card not found")
    changes = {}
    if title != existing.title:
        changes["title"] = title
    if body != existing.body:
        changes["body"] = body
    if summary.strip() != existing.summary:
        changes["summary"] = summary
    if lane != existing.lane:
        changes["lane"] = lane
    card = existing
    if changes:
        card = store.update_card(
            card_id,
            CardUpdate(**changes),
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request, "partials/card-detail.html", _card_detail_context(request, card)
    )


@ui_router.delete("/partials/cards/{card_id}", response_model=None)
def card_detail_delete(
    request: Request,
    card_id: str,
    realm: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    get_store().delete_card(
        card_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    return _templates(request).TemplateResponse(
        request,
        "partials/card-detail-empty.html",
        {},
    )


@ui_router.post("/partials/cards/{card_id}/move", response_model=None)
def card_lane_move(
    request: Request,
    card_id: str,
    lane: CardLane = Form(...),
    realm: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    get_store().update_card(
        card_id,
        CardUpdate(lane=lane),
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    return HTMLResponse("", status_code=204)


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
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def list_items(
            kind: str | None = None, status: str | None = None
        ) -> list[dict]:
            """List goals, tasks, projects, and concerns."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/items",
                params={"kind": kind, "status": status},
            )

        @mcp.tool()
        def list_cards(realm: str = "default", lane: str | None = None) -> list[dict]:
            """List cards in a realm."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/cards",
                params={"realm": realm, "lane": lane},
            )

        @mcp.tool()
        def create_item(
            kind: str,
            title: str,
            body: str = "",
            status: str = "open",
            parent_id: str | None = None,
        ) -> dict:
            """Create a goal, task, project, or concern."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/items",
                json={
                    "kind": kind,
                    "title": title,
                    "body": body,
                    "status": status,
                    "parent_id": parent_id,
                },
            )

        @mcp.tool()
        def get_item(item_id: str) -> dict | None:
            """Get a single item by ID."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/items/{item_id}",
                allow_not_found=True,
            )

        @mcp.tool()
        def list_knowledge(item_id: str | None = None, limit: int = 20) -> list[dict]:
            """List captured knowledge from agent sessions."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/knowledge",
                params={"item_id": item_id, "limit": limit},
            )
