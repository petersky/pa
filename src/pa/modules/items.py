from __future__ import annotations

import mimetypes
import os
import re
import shutil
from datetime import UTC, datetime, timedelta
from itertools import zip_longest
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit
from uuid import uuid4

from fastapi import APIRouter, File, Form, HTTPException, Request, Response, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from pa.auth.csrf import token_for_request
from pa.auth.middleware import get_principal_id
from pa.config import get_settings
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import (
    CardCreate,
    CardKind,
    CardLane,
    CardSummarySource,
    CardUpdate,
    Item,
    ItemCreate,
    ItemKind,
    ItemStatus,
    ItemUpdate,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeStatus,
    KnowledgeUpdate,
)
from pa.domain.session_selection import preferred_sessions_by_card
from pa.domain.store import get_store

router = APIRouter()
ui_router = APIRouter()

MAX_CARD_ATTACHMENTS = 10
MAX_CARD_ATTACHMENT_BYTES = 25 * 1024 * 1024
MAX_CARD_ATTACHMENTS_TOTAL_BYTES = 100 * 1024 * 1024
ATTACHMENT_ID_RE = re.compile(r"^[0-9a-f]{32}$")
SAFE_IMAGE_TYPES = {
    "image/avif",
    "image/gif",
    "image/jpeg",
    "image/png",
    "image/webp",
}


def _mark_legacy_item_api(response: Response) -> None:
    response.headers["Deprecation"] = "true"
    response.headers["Link"] = (
        "<https://github.com/petersky/pa/blob/main/docs/ITEM_CARD_MIGRATION.md>;"
        ' rel="deprecation"; type="text/markdown"'
    )


def _templates(request: Request):
    return request.app.state.templates


def _active_realm(request: Request) -> str:
    return (
        request.query_params.get("realm")
        or request.app.state.ctx.settings.primary_realm
    )


def _active_project(request: Request) -> str | None:
    return request.query_params.get("project")


def _attachment_root(request: Request) -> Path:
    return Path(request.app.state.ctx.settings.data_dir) / "card-attachments"


def _safe_attachment_filename(filename: str, index: int) -> str:
    basename = (filename or f"attachment-{index}").replace("\\", "/").split("/")[-1]
    cleaned = re.sub(r"[^\w.() -]+", "_", basename, flags=re.UNICODE).strip(" .")
    if not cleaned:
        cleaned = f"attachment-{index}"
    if len(cleaned) > 160:
        suffix = Path(cleaned).suffix[:20]
        cleaned = cleaned[: 160 - len(suffix)].rstrip(" .") + suffix
    return cleaned


def _unique_attachment_filename(filename: str, used: set[str]) -> str:
    candidate = filename
    stem = Path(filename).stem
    suffix = Path(filename).suffix
    counter = 2
    while candidate.casefold() in used:
        candidate = f"{stem}-{counter}{suffix}"
        counter += 1
    used.add(candidate.casefold())
    return candidate


def _attachment_media_type(filename: str, supplied: str | None) -> str:
    guessed, _ = mimetypes.guess_type(filename)
    return guessed or supplied or "application/octet-stream"


def _persist_card_attachments(
    root: Path,
    uploads: list[UploadFile],
) -> list[dict[str, str | int | Path]]:
    if len(uploads) > MAX_CARD_ATTACHMENTS:
        raise HTTPException(
            status_code=413,
            detail=f"A card can have at most {MAX_CARD_ATTACHMENTS} files",
        )
    if not uploads:
        return []

    root.mkdir(parents=True, exist_ok=True)
    attachment_id = uuid4().hex
    temporary = root / f".tmp-{attachment_id}"
    destination = root / attachment_id
    temporary.mkdir()
    total_size = 0
    used_names: set[str] = set()
    records: list[dict[str, str | int | Path]] = []
    try:
        for index, upload in enumerate(uploads, start=1):
            filename = _unique_attachment_filename(
                _safe_attachment_filename(upload.filename or "", index),
                used_names,
            )
            path = temporary / filename
            size = 0
            upload.file.seek(0)
            with path.open("xb") as handle:
                while chunk := upload.file.read(1024 * 1024):
                    size += len(chunk)
                    total_size += len(chunk)
                    if size > MAX_CARD_ATTACHMENT_BYTES:
                        raise HTTPException(
                            status_code=413,
                            detail=f"{filename} exceeds the 25 MB file limit",
                        )
                    if total_size > MAX_CARD_ATTACHMENTS_TOTAL_BYTES:
                        raise HTTPException(
                            status_code=413,
                            detail="Card attachments exceed the 100 MB total limit",
                        )
                    handle.write(chunk)
            media_type = _attachment_media_type(filename, upload.content_type)
            records.append(
                {
                    "attachment_id": attachment_id,
                    "filename": filename,
                    "media_type": media_type,
                    "size": size,
                    "path": destination / filename,
                    "url": (
                        f"/card-attachments/{attachment_id}/{quote(filename, safe='')}"
                    ),
                }
            )
        os.replace(temporary, destination)
        return records
    except BaseException:
        shutil.rmtree(temporary, ignore_errors=True)
        shutil.rmtree(destination, ignore_errors=True)
        raise


def _delete_attachment_batch(records: list[dict[str, str | int | Path]]) -> None:
    if not records:
        return
    first_path = records[0].get("path")
    if isinstance(first_path, Path):
        shutil.rmtree(first_path.parent, ignore_errors=True)


def _markdown_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("[", "\\[").replace("]", "\\]")


def _attachment_markup(record: dict[str, str | int | Path]) -> str:
    filename = str(record["filename"])
    label = _markdown_label(filename)
    url = str(record["url"])
    media_type = str(record["media_type"])
    if media_type in SAFE_IMAGE_TYPES:
        return f"![{label}]({url})"
    if media_type.startswith("video/"):
        return f'<video controls preload="metadata" src="{url}"></video>'
    if media_type.startswith("audio/"):
        return f'<audio controls preload="metadata" src="{url}"></audio>'
    return f"[{label}]({url})"


def _validated_link(raw_url: str) -> str:
    url = raw_url.strip()
    if not url:
        return ""
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise HTTPException(
            status_code=422,
            detail=f"Attachment link must be an http(s) URL: {url}",
        )
    return quote(url, safe=":/?#[]@!$&'+,;=%")


def _append_markdown_section(body: str, title: str, entries: list[str]) -> str:
    if not entries:
        return body
    section = f"## {title}\n\n" + "\n\n".join(entries)
    return f"{body.rstrip()}\n\n{section}".strip()


def _compose_card_body(
    body: str,
    *,
    link_urls: list[str],
    link_labels: list[str],
    attachments: list[dict[str, str | int | Path]],
    file_tokens: list[str],
) -> str:
    composed = body.strip()
    unreferenced: list[str] = []
    for index, record in enumerate(attachments):
        token = file_tokens[index].strip() if index < len(file_tokens) else ""
        marker = f"attachment:{token}" if token else ""
        if marker and marker in composed:
            composed = composed.replace(marker, str(record["url"]))
        else:
            unreferenced.append(_attachment_markup(record))

    links: list[str] = []
    for raw_url, raw_label in zip_longest(link_urls, link_labels, fillvalue=""):
        url = _validated_link(raw_url)
        if not url:
            continue
        parsed = urlsplit(url)
        label = _markdown_label(raw_label.strip() or parsed.netloc)
        links.append(f"[{label}]({url})")

    composed = _append_markdown_section(composed, "Links", links)
    return _append_markdown_section(composed, "Attachments", unreferenced)


def _comma_separated(value: str) -> list[str]:
    return list(
        dict.fromkeys(part.strip() for part in value.split(",") if part.strip())
    )


def _pr_watch_context(
    request: Request, card_id: str, *, include_events: bool = True
) -> dict:
    store = request.app.state.ctx.services.get("pr_supervisor_store")
    if not store:
        return {"pr_watches": [], "pr_watch_events": {}}
    watches = store.list_watches(card_id=card_id, include_retired=True)
    return {
        "pr_watches": watches,
        "pr_watch_events": {
            watch.id: store.list_events(watch.id, limit=20) for watch in watches
        }
        if include_events
        else {},
    }


def _card_summary_context(request: Request, card) -> dict:
    store = get_store()
    realm_id = card.realm_id
    project = (
        store.get_project(card.project_id, realm_id=realm_id)
        if card.project_id
        else None
    )
    watch_context = _pr_watch_context(request, card.id, include_events=False)
    critical_watch = next(
        (
            watch
            for watch in watch_context["pr_watches"]
            if watch.last_error or watch.status.value == "blocked"
        ),
        None,
    )
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
        "critical_watch": critical_watch,
        "lanes": list(CardLane),
        "csrf_token": token_for_request(request),
        **watch_context,
    }


def _card_agent_context(request: Request, card) -> dict:
    store = get_store()
    related_sessions = [
        session for session in store.list_sessions() if session.card_id == card.id
    ]
    current_session = next(
        (session for session in related_sessions if session.status != "closed"),
        None,
    )
    return {
        "card": card,
        "related_sessions": related_sessions,
        "current_session": current_session,
        "agent_enabled": request.app.state.ctx.settings.agent_enabled,
    }


def _card_activity_context(request: Request, card) -> dict:
    store = get_store()
    entries: list[dict] = []

    event_log = request.app.state.ctx.services.get("event_log")
    if event_log:
        head = event_log.get_head(card.realm_id)
        if head:

            def add_card_event(event) -> None:
                if event.card_id != card.id:
                    return
                fields = [
                    key
                    for key in event.payload
                    if key
                    not in {
                        "created_at",
                        "updated_at",
                        "summary_updated_at",
                        "summary_source",
                        "summary_stale",
                    }
                ]
                if event.type.value == "card_created":
                    label = "Card created"
                elif event.type.value == "card_updated":
                    label = "Card updated"
                    if fields:
                        label += ": " + ", ".join(
                            field.replace("_", " ") for field in fields
                        )
                else:
                    label = event.type.value.replace("_", " ").capitalize()
                entries.append(
                    {
                        "id": f"card-{event.id}",
                        "kind": "card",
                        "label": label,
                        "actor": event.author_principal,
                        "detail": event.author_instance,
                        "timestamp": event.timestamp,
                    }
                )

            event_log.apply_commit_chain(head, add_card_event)

    if not any(entry["label"] == "Card created" for entry in entries):
        entries.append(
            {
                "id": f"card-created-{card.id}",
                "kind": "card",
                "label": "Card created",
                "actor": card.created_by_principal or "unknown",
                "detail": card.created_by_instance or "",
                "timestamp": card.created_at,
            }
        )

    for session in store.list_sessions():
        if session.card_id != card.id:
            continue
        context = (session.config_json or {}).get("execution_context") or {}
        instance = context.get("instance") or {}
        entries.append(
            {
                "id": f"agent-{session.id}",
                "kind": "agent",
                "label": f"Agent session {session.status}",
                "actor": session.agent_name,
                "detail": instance.get("name") or session.title or session.label or "",
                "timestamp": session.updated_at,
            }
        )

    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    if dispatch_store:
        for dispatch in dispatch_store.list(limit=500):
            if dispatch.card_id != card.id:
                continue
            for event in dispatch.events:
                entries.append(
                    {
                        "id": f"dispatch-{dispatch.dispatch_id}-{event.seq}",
                        "kind": "dispatch",
                        "label": event.message,
                        "actor": dispatch.target_instance_name
                        or dispatch.target_instance_id,
                        "detail": event.state.replace("_", " "),
                        "timestamp": event.created_at,
                    }
                )

    watch_context = _pr_watch_context(request, card.id)
    for watch in watch_context["pr_watches"]:
        for event in watch_context["pr_watch_events"].get(watch.id, []):
            entries.append(
                {
                    "id": f"pr-{event.id}",
                    "kind": "pr",
                    "label": event.event_type.replace("_", " ").capitalize(),
                    "actor": event.source,
                    "detail": f"{watch.repository}#{watch.pr_number}",
                    "timestamp": event.created_at,
                }
            )

    for knowledge in store.list_knowledge(item_id=card.id, limit=100):
        entries.append(
            {
                "id": f"memory-{knowledge.id}",
                "kind": "memory",
                "label": knowledge.summary,
                "actor": knowledge.owner or knowledge.source,
                "detail": knowledge.kind.value,
                "timestamp": knowledge.created_at,
            }
        )

    return {
        "card": card,
        "activity": sorted(
            entries, key=lambda entry: entry["timestamp"], reverse=True
        ),
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


def _new_card_context(request: Request) -> dict:
    realm = _active_realm(request)
    ctx = request.app.state.ctx
    store = get_store()
    selected_project = _active_project(request)
    projects = store.list_projects(realm_id=realm)
    if selected_project not in {project.id for project in projects}:
        selected_project = None
    instances = [{"id": ctx.settings.instance_id, "name": ctx.settings.instance_name}]
    fleet = ctx.services.get("fleet_registry")
    if fleet:
        known = {ctx.settings.instance_id}
        for instance in fleet.list_instances():
            if instance.instance_id in known:
                continue
            known.add(instance.instance_id)
            instances.append(
                {
                    "id": instance.instance_id,
                    "name": instance.name or instance.instance_id,
                }
            )
    return {
        "active_realm": realm,
        "selected_project": selected_project,
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": projects,
        "parent_cards": store.list_cards(realm_id=realm),
        "instance_options": instances,
        "csrf_token": token_for_request(request),
        "max_attachments": MAX_CARD_ATTACHMENTS,
        "max_attachment_mb": MAX_CARD_ATTACHMENT_BYTES // (1024 * 1024),
    }


def _knowledge_context(request: Request) -> dict:
    store = get_store()
    realm = _active_realm(request)
    query = request.query_params.get("q", "").strip()
    kind = request.query_params.get("kind", "").strip() or None
    status = request.query_params.get("status", "active").strip() or None
    scope = request.query_params.get("scope", "").strip() or None
    return {
        "knowledge": store.list_knowledge(
            limit=100, search=query or None, kind=kind, status=status, scope=scope
        ),
        "cards": store.list_cards(realm_id=realm),
        "items": store.list_cards(realm_id=realm),
        "projects": store.list_projects(realm_id=realm),
        "knowledge_kinds": list(KnowledgeKind),
        "knowledge_statuses": list(KnowledgeStatus),
        "knowledge_filters": {
            "q": query,
            "kind": kind or "",
            "status": status or "",
            "scope": scope or "",
        },
        "promote_session_id": request.query_params.get("session", ""),
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
    response: Response,
    kind: ItemKind | None = None,
    status: ItemStatus | None = None,
) -> list[dict]:
    _mark_legacy_item_api(response)
    items = get_store().list_items(kind=kind, status=status)
    return [item.model_dump(mode="json") for item in items]


@router.post("/items", status_code=201)
def create_item(request: Request, response: Response, data: ItemCreate) -> dict:
    _mark_legacy_item_api(response)
    item = get_store().create_item(
        data,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return item.model_dump(mode="json")


@router.get("/items/{item_id}")
def get_item(response: Response, item_id: str) -> dict:
    _mark_legacy_item_api(response)
    item = get_store().get_item(item_id)
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item.model_dump(mode="json")


@router.patch("/items/{item_id}")
def update_item(
    request: Request, response: Response, item_id: str, data: ItemUpdate
) -> dict:
    _mark_legacy_item_api(response)
    item = get_store().update_item(
        item_id,
        data,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    if not item:
        raise HTTPException(status_code=404, detail="Item not found")
    return item.model_dump(mode="json")


@router.delete("/items/{item_id}", status_code=204)
def delete_item(request: Request, response: Response, item_id: str) -> None:
    _mark_legacy_item_api(response)
    if not get_store().delete_item(
        item_id,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    ):
        raise HTTPException(status_code=404, detail="Item not found")


@router.get("/knowledge")
def list_knowledge(
    item_id: str | None = None,
    limit: int = 50,
    q: str | None = None,
    kind: KnowledgeKind | None = None,
    status: KnowledgeStatus | None = KnowledgeStatus.ACTIVE,
    scope: str | None = None,
) -> list[dict]:
    entries = get_store().list_knowledge(
        item_id=item_id,
        limit=limit,
        search=q,
        kind=kind.value if kind else None,
        status=status.value if status else None,
        scope=scope,
    )
    return [entry.model_dump(mode="json") for entry in entries]


@router.post("/knowledge", status_code=201)
def create_knowledge(request: Request, data: KnowledgeEntry) -> dict:
    if not data.owner:
        data.owner = get_principal_id(request)
    return get_store().add_knowledge(data).model_dump(mode="json")


@router.patch("/knowledge/{entry_id}")
def update_knowledge(entry_id: str, data: KnowledgeUpdate) -> dict:
    entry = get_store().update_knowledge(entry_id, data)
    if not entry:
        raise HTTPException(status_code=404, detail="Memory not found")
    return entry.model_dump(mode="json")


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


@ui_router.get("/partials/cards/new", response_class=HTMLResponse)
def new_card_form(request: Request) -> HTMLResponse:
    return _templates(request).TemplateResponse(
        request,
        "partials/card-new.html",
        _new_card_context(request),
    )


@ui_router.post("/partials/cards/new", response_model=None)
async def create_card_modal_ui(
    request: Request,
    title: str = Form(...),
    body: str = Form(""),
    summary: str = Form(""),
    kind: CardKind = Form(CardKind.TASK),
    lane: CardLane = Form(CardLane.INBOX),
    project_id: str = Form(""),
    parent_id: str = Form(""),
    tags: str = Form(""),
    preferred_instance: str = Form(""),
    preferred_capabilities: str = Form(""),
    link_urls: list[str] | None = Form(None),
    link_labels: list[str] | None = Form(None),
    file_tokens: list[str] | None = Form(None),
    files: list[UploadFile] | None = File(None),
) -> JSONResponse:
    realm = _active_realm(request)
    cleaned_title = title.strip()
    if not cleaned_title:
        raise HTTPException(status_code=422, detail="Title is required")

    store = get_store()
    selected_project = project_id.strip() or None
    selected_parent = parent_id.strip() or None
    if selected_project and not store.get_project(selected_project, realm_id=realm):
        raise HTTPException(status_code=422, detail="Selected project was not found")
    if selected_parent and not store.get_card(selected_parent, realm_id=realm):
        raise HTTPException(
            status_code=422, detail="Selected parent card was not found"
        )

    uploads = [upload for upload in (files or []) if upload.filename]
    runtime = request.app.state.ctx.require_service("async_runtime")
    attachments: list[dict[str, str | int | Path]] = []
    try:
        attachments = await runtime.run_blocking(
            "cards.attachments.persist",
            _persist_card_attachments,
            _attachment_root(request),
            uploads,
            timeout=120.0,
        )
        composed_body = _compose_card_body(
            body,
            link_urls=link_urls or [],
            link_labels=link_labels or [],
            attachments=attachments,
            file_tokens=file_tokens or [],
        )
        cleaned_summary = summary.strip()
        settings = request.app.state.ctx.settings
        card = store.create_card(
            CardCreate(
                realm_id=realm,
                kind=kind,
                title=cleaned_title,
                body=composed_body,
                summary=cleaned_summary,
                summary_source=(CardSummarySource.MANUAL if cleaned_summary else None),
                lane=lane,
                parent_id=selected_parent,
                project_id=selected_project,
                tags=_comma_separated(tags),
                preferred_instance=preferred_instance.strip() or None,
                preferred_capabilities=_comma_separated(preferred_capabilities),
            ),
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
        )
    except BaseException:
        if attachments:
            await runtime.run_blocking(
                "cards.attachments.cleanup",
                _delete_attachment_batch,
                attachments,
                timeout=30.0,
            )
        raise
    finally:
        for upload in uploads:
            await upload.close()

    return JSONResponse(
        card.model_dump(mode="json"),
        status_code=201,
        headers={"Location": f"/cards/{card.id}"},
    )


@ui_router.get("/card-attachments/{attachment_id}/{filename}")
def card_attachment(
    request: Request,
    attachment_id: str,
    filename: str,
) -> FileResponse:
    if not ATTACHMENT_ID_RE.fullmatch(attachment_id) or Path(filename).name != filename:
        raise HTTPException(status_code=404, detail="Attachment not found")
    root = _attachment_root(request).resolve()
    path = (root / attachment_id / filename).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise HTTPException(status_code=404, detail="Attachment not found")
    media_type = _attachment_media_type(path.name, None)
    inline = media_type in SAFE_IMAGE_TYPES or media_type.startswith(
        ("video/", "audio/")
    )
    return FileResponse(
        path,
        filename=path.name,
        media_type=media_type,
        content_disposition_type="inline" if inline else "attachment",
        headers={
            "Content-Security-Policy": (
                "sandbox; default-src 'none'; img-src 'self'; media-src 'self'"
            ),
            "X-Content-Type-Options": "nosniff",
        },
    )


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
    limit: int = 10,
) -> HTMLResponse:
    context = _cards_context(request, lane=lane)
    done_total = 0
    done_visible = 0
    done_show_more_count = 0
    done_show_more_query = ""
    if lane == CardLane.DONE:
        done_total = len(context["cards"])
        done_limit = max(10, limit)
        context["cards"] = context["cards"][:done_limit]
        done_visible = len(context["cards"])
        done_show_more_count = min(10, done_total - done_visible)
        if done_show_more_count:
            show_more_params = dict(request.query_params)
            show_more_params["lane"] = CardLane.DONE.value
            show_more_params["limit"] = str(done_visible + done_show_more_count)
            done_show_more_query = urlencode(show_more_params)
    return _templates(request).TemplateResponse(
        request,
        "partials/cards.html",
        {
            **context,
            "lane": lane,
            "done_total": done_total,
            "done_visible": done_visible,
            "done_show_more_count": done_show_more_count,
            "done_show_more_query": done_show_more_query,
        },
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
        _card_summary_context(request, card),
    )


@ui_router.get("/partials/cards/{card_id}/agent", response_class=HTMLResponse)
def card_detail_agent_partial(
    request: Request, card_id: str, realm: str | None = None
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request,
        "partials/card-detail-agent.html",
        _card_agent_context(request, card),
    )


@ui_router.get("/partials/cards/{card_id}/activity", response_class=HTMLResponse)
def card_detail_activity_partial(
    request: Request, card_id: str, realm: str | None = None
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request,
        "partials/card-detail-activity.html",
        _card_activity_context(request, card),
    )


@ui_router.post("/partials/cards/{card_id}", response_model=None)
def card_detail_update(
    request: Request,
    card_id: str,
    title: str | None = Form(None),
    body: str | None = Form(None),
    summary: str | None = Form(None),
    lane: CardLane | None = Form(None),
    realm: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    settings = request.app.state.ctx.settings
    store = get_store()
    existing = store.get_card(card_id, realm_id=realm_id)
    if not existing:
        raise HTTPException(status_code=404, detail="Card not found")
    changes = {}
    if title is not None and title != existing.title:
        changes["title"] = title
    if body is not None and body != existing.body:
        changes["body"] = body
    if summary is not None and summary.strip() != existing.summary:
        changes["summary"] = summary
    if lane is not None and lane != existing.lane:
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
        request, "partials/card-detail.html", _card_summary_context(request, card)
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
    context = _knowledge_context(request)
    return _templates(request).TemplateResponse(
        request,
        "partials/knowledge.html",
        context,
    )


@ui_router.post("/partials/knowledge", response_class=HTMLResponse)
def create_knowledge_ui(
    request: Request,
    summary: str = Form(...),
    kind: KnowledgeKind = Form(KnowledgeKind.MEMORY),
    scope: str = Form("realm"),
    source_url: str = Form(""),
    owner: str = Form(""),
    confidence: str = Form(""),
    card_id: str = Form(""),
    session_id: str = Form(""),
    supersedes_id: str = Form(""),
    review_at: datetime | None = Form(None),
    expires_at: datetime | None = Form(None),
    tags: str = Form(""),
) -> HTMLResponse:
    get_store().add_knowledge(
        KnowledgeEntry(
            summary=summary.strip(),
            kind=kind,
            scope=scope.strip() or "realm",
            source="promoted" if session_id else "manual",
            source_url=source_url.strip() or None,
            owner=owner.strip() or get_principal_id(request),
            confidence=float(confidence) if confidence.strip() else None,
            card_id=card_id or None,
            item_id=card_id or None,
            session_id=session_id or None,
            supersedes_id=supersedes_id or None,
            review_at=review_at,
            expires_at=expires_at,
            tags=[tag.strip() for tag in tags.split(",") if tag.strip()],
        )
    )
    return knowledge_partial(request)


@ui_router.post("/partials/knowledge/{entry_id}/status", response_class=HTMLResponse)
def update_knowledge_status_ui(
    request: Request,
    entry_id: str,
    status: KnowledgeStatus = Form(...),
) -> HTMLResponse:
    if not get_store().update_knowledge(entry_id, KnowledgeUpdate(status=status)):
        raise HTTPException(status_code=404, detail="Memory not found")
    return knowledge_partial(request)


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
                label="Memory",
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
            kind: ItemKind | None = None, status: ItemStatus | None = None
        ) -> list[dict]:
            """List goals, tasks, projects, and concerns."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/items",
                params={"kind": kind, "status": status},
            )

        @mcp.tool()
        def list_cards(
            realm: str | None = None,
            lane: CardLane | None = None,
            kind: CardKind | None = None,
        ) -> list[dict]:
            """List canonical cards, optionally filtered by lane and kind."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/cards",
                params={"realm": realm, "lane": lane, "kind": kind},
            )

        @mcp.tool()
        def create_card(
            title: str,
            kind: CardKind = CardKind.TASK,
            body: str = "",
            lane: CardLane = CardLane.INBOX,
            realm: str = "default",
            parent_id: str | None = None,
            project_id: str | None = None,
            tags: list[str] | None = None,
        ) -> dict:
            """Create a canonical card. Use lane: inbox, active, waiting, or done."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/cards",
                json={
                    "realm_id": realm,
                    "kind": kind,
                    "title": title,
                    "body": body,
                    "lane": lane,
                    "parent_id": parent_id,
                    "project_id": project_id,
                    "tags": tags or [],
                },
            )

        @mcp.tool()
        def update_card(
            card_id: str,
            title: str | None = None,
            body: str | None = None,
            lane: CardLane | None = None,
            realm: str = "default",
            tags: list[str] | None = None,
        ) -> dict | None:
            """Update a canonical card. Omitted fields remain unchanged."""
            changes = {
                key: value
                for key, value in {
                    "title": title,
                    "body": body,
                    "lane": lane,
                    "tags": tags,
                }.items()
                if value is not None
            }
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/cards/{card_id}",
                params={"realm": realm},
                json=changes,
                allow_not_found=True,
            )

        @mcp.tool()
        def update_card_preferred_instance(
            card_id: str,
            instance_id: str,
            realm: str = "default",
        ) -> dict | None:
            """Set a card's preferred fleet instance and return its new authority version."""
            instance_id = instance_id.strip()
            if not instance_id:
                raise ValueError("instance_id cannot be empty")
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/cards/{card_id}",
                params={"realm": realm},
                json={"preferred_instance": instance_id},
                allow_not_found=True,
            )

        @mcp.tool()
        def create_item(
            kind: ItemKind,
            title: str,
            body: str = "",
            status: ItemStatus = ItemStatus.OPEN,
            parent_id: str | None = None,
        ) -> dict:
            """Deprecated: create an item. Prefer create_card with canonical lane."""
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
        def update_item(
            item_id: str,
            title: str | None = None,
            body: str | None = None,
            status: ItemStatus | None = None,
            parent_id: str | None = None,
        ) -> dict | None:
            """Update an item's mutable fields."""
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/items/{item_id}",
                json={
                    key: value
                    for key, value in {
                        "title": title,
                        "body": body,
                        "status": status,
                        "parent_id": parent_id,
                    }.items()
                    if value is not None
                },
                allow_not_found=True,
            )

        @mcp.tool()
        def create_card(
            title: str,
            kind: CardKind = CardKind.TASK,
            body: str = "",
            lane: CardLane = CardLane.INBOX,
            realm: str = "default",
            parent_id: str | None = None,
            project_id: str | None = None,
        ) -> dict:
            """Create a card in a realm."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/cards",
                json={
                    "realm_id": realm,
                    "kind": kind,
                    "title": title,
                    "body": body,
                    "lane": lane,
                    "parent_id": parent_id,
                    "project_id": project_id,
                },
            )

        @mcp.tool()
        def update_card(
            card_id: str,
            title: str | None = None,
            body: str | None = None,
            lane: CardLane | None = None,
            parent_id: str | None = None,
            project_id: str | None = None,
            realm: str = "default",
        ) -> dict | None:
            """Update a card's mutable fields."""
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/cards/{card_id}",
                params={"realm": realm},
                json={
                    key: value
                    for key, value in {
                        "title": title,
                        "body": body,
                        "lane": lane,
                        "parent_id": parent_id,
                        "project_id": project_id,
                    }.items()
                    if value is not None
                },
                allow_not_found=True,
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
            """List curated durable memories and decisions."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/knowledge",
                params={"item_id": item_id, "limit": limit},
            )
