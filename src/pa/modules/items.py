from __future__ import annotations

import asyncio
import hashlib
import json
import mimetypes
import os
import re
import shutil
from datetime import UTC, datetime, timedelta
from itertools import zip_longest
from pathlib import Path
from typing import Annotated, Literal
from urllib.parse import quote, urlencode, urlsplit
from uuid import uuid4

import httpx
from fastapi import (
    APIRouter,
    BackgroundTasks,
    File,
    Form,
    Header,
    HTTPException,
    Request,
    Response,
    UploadFile,
)
from fastapi.responses import (
    FileResponse,
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    StreamingResponse,
)
from pydantic import BaseModel, Field

from pa.attachments import AttachmentError, AttachmentStore, safe_filename
from pa.auth.csrf import token_for_request
from pa.auth.middleware import get_principal_id
from pa.config import get_settings
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.instance_identity import (
    canonical_instance_identities,
    canonicalize_dispatch_public,
    current_instance_name,
    present_instance_references,
    resolve_instance_identity,
)
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.core.ui.work_presentation import present_work_item
from pa.domain.card_enrichment import enrich_card, explicit_enrichment_fields
from pa.domain.models import (
    CardAttachment,
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
    KnowledgeAuditEvent,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeStatus,
    KnowledgeUpdate,
)
from pa.domain.projection import (
    CardVersionConflict,
    MutationOperationConflict,
    MutationOperationInProgress,
)
from pa.domain.session_selection import preferred_sessions_by_card
from pa.domain.store import get_store
from pa.knowledge.capture import (
    audit_knowledge_records,
    promote_from_transcript,
    record_lifecycle_change,
    regenerate_knowledge,
)
from pa.sync.event_log import HISTORY_PAGE_LIMIT, EventHistoryError

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

HOME_ATTENTION_LIMIT = 6
HOME_MOTION_LIMIT = 8
HOME_OUTCOME_LIMIT = 6
HOME_FLEET_LIMIT = 50
HOME_ROUTE_LIMIT = 200
WORK_PRESENTATION_PAGE_LIMIT = 100


class KnowledgePromotionRequest(BaseModel):
    session_id: str
    summary: str | None = None
    start_seq: int | None = Field(default=None, ge=1)
    end_seq: int | None = Field(default=None, ge=1)
    kind: KnowledgeKind = KnowledgeKind.MEMORY
    scope: str = "realm"
    source_url: str | None = None
    confidence: float | None = Field(default=None, ge=0, le=1)
    card_id: str | None = None
    tags: list[str] = Field(default_factory=list)
    supersedes_id: str | None = None
    review_at: datetime | None = None
    expires_at: datetime | None = None


class KnowledgeBulkRequest(BaseModel):
    ids: list[str] = Field(min_length=1, max_length=500)
    action: Literal["archive", "supersede"]


class CardProjectChangeRequest(BaseModel):
    project_id: str | None = None
    decision: Literal["preserve", "migrate", "cancel"] | None = None


class CardRepairRequest(BaseModel):
    card_ids: list[str] = Field(min_length=1, max_length=100)
    realm_id: str = "default"


def _require_memory_editor(request: Request) -> str:
    user = getattr(request.state, "user", None)
    if not user or getattr(user, "role", "viewer") not in {"editor", "admin"}:
        raise HTTPException(
            status_code=403,
            detail="Memory promotion and lifecycle changes require editor access",
        )
    return get_principal_id(request)


@router.get("/cards/events")
async def card_events(request: Request, realm: str | None = None) -> StreamingResponse:
    from pa.server.shutdown import is_shutting_down, wait_for_shutdown_or

    realm_id = realm or request.app.state.ctx.settings.primary_realm
    broker = request.app.state.ctx.require_service("live_updates")

    async def stream():
        queue = broker.subscribe(realm_id)
        try:
            yield ": connected\n\n"
            while not is_shutting_down() and not await request.is_disconnected():
                try:
                    stopping, event = await wait_for_shutdown_or(
                        queue.get(), timeout=20.0
                    )
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if stopping or event is None:
                    return
                yield (
                    "event: cards-changed\n"
                    f"data: {json.dumps(event, separators=(',', ':'))}\n\n"
                )
        finally:
            broker.unsubscribe(realm_id, queue)

    return StreamingResponse(
        stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


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


def _latest_card_progress(request: Request, card_id: str) -> dict | None:
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    if not dispatch_store:
        return None
    records = [
        record
        for record in dispatch_store.list(limit=1000)
        if record.card_id == card_id
    ]
    if not records:
        return None
    active = [
        record
        for record in records
        if record.state
        in {
            "queued",
            "checking_sync",
            "materializing",
            "provisioning",
            "starting_session",
            "delivering_prompt",
            "running",
        }
    ]
    record = max(active or records, key=lambda item: item.updated_at)
    public = canonicalize_dispatch_public(request.app.state.ctx, record)
    progress = dict(public.get("progress") or {})
    progress.update(
        {
            "dispatch_id": record.dispatch_id,
            "session_id": record.session_id,
            "dispatch_state": record.state,
            "target_instance_id": record.target_instance_id,
            "target_instance_name": public["target_instance_name"],
            "target_instance_name_at_dispatch": public[
                "target_instance_name_at_dispatch"
            ],
            "updated_at": record.updated_at.isoformat(),
            "evaluated_outcome": public.get("evaluated_outcome"),
            "post_turn_evaluation": public.get("post_turn_evaluation"),
            "turn_end": public.get("turn_end"),
            "followup_state": public.get("followup_state"),
        }
    )
    return progress


def _progress_from_dispatch(ctx: AppContext, record) -> dict:
    public = canonicalize_dispatch_public(ctx, record)
    progress = dict(public.get("progress") or {})
    progress.update(
        {
            "dispatch_id": record.dispatch_id,
            "session_id": record.session_id,
            "dispatch_state": record.state,
            "target_instance_id": record.target_instance_id,
            "target_instance_name": public["target_instance_name"],
            "target_instance_name_at_dispatch": public[
                "target_instance_name_at_dispatch"
            ],
            "updated_at": record.updated_at.isoformat(),
            "evaluated_outcome": public.get("evaluated_outcome"),
            "post_turn_evaluation": public.get("post_turn_evaluation"),
            "turn_end": public.get("turn_end"),
            "followup_state": public.get("followup_state"),
        }
    )
    return progress


def _session_presentation_signal(ctx: AppContext, session) -> dict | None:
    if session is None:
        return None
    agent = ctx.services.get("instance_agent")
    runtime = agent.get(session.id) if agent and hasattr(agent, "get") else None
    if runtime and not getattr(runtime, "_closed", False):
        active = bool(
            getattr(runtime, "prompting", False) or getattr(runtime, "_in_flight", None)
        )
        return {
            "id": session.id,
            "session_state": "busy" if active else "connected",
            "state": "working" if active else "connected",
            "connected": bool(getattr(runtime, "connected", False)),
            "turn": {"state": "running"} if active else None,
            "liveness": {
                "classification": "live" if active else "completed_idle",
            },
        }
    status = str(session.status or "")
    failed = status in {"failed", "error", "recovery_blocked"}
    terminal = status in {"closed", "quiesced"} or failed
    return {
        "id": session.id,
        "session_state": "failed" if failed else "closed" if terminal else "stale",
        "state": "failed" if failed else "completed" if terminal else "stale",
        "connected": False,
        "turn": None,
        "liveness": {
            "classification": "failed_closed" if failed else "stale",
        },
    }


def _watches_for_cards(request: Request, card_ids: set[str]) -> dict[str, list]:
    if not card_ids:
        return {}
    store = request.app.state.ctx.services.get("pr_supervisor_store")
    if not store:
        return {}
    if hasattr(store, "list_watches_for_cards"):
        watches = store.list_watches_for_cards(
            card_ids,
            realm_id=_active_realm(request),
            include_retired=False,
            per_card_limit=3,
        )
    else:
        watches = [
            watch
            for watch in store.list_watches(include_retired=False)
            if watch.card_id in card_ids
        ][: len(card_ids) * 3]
    result: dict[str, list] = {}
    for watch in watches:
        if watch.card_id:
            result.setdefault(watch.card_id, []).append(watch)
    return result


def _presentation_context_for_cards(
    request: Request,
    cards: list,
) -> tuple[dict, dict, dict, dict]:
    card_ids = {card.id for card in cards}
    store = get_store()
    sessions = preferred_sessions_by_card(store.list_sessions_for_cards(card_ids))
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    dispatches = dispatch_store.latest_by_card(card_ids) if dispatch_store else {}
    watches = _watches_for_cards(request, card_ids)
    progress: dict[str, dict] = {}
    presentations: dict[str, dict] = {}
    ctx = request.app.state.ctx
    for card in cards:
        record = dispatches.get(card.id)
        public = canonicalize_dispatch_public(ctx, record) if record else None
        if record:
            progress[card.id] = _progress_from_dispatch(ctx, record)
        presentations[card.id] = present_work_item(
            card,
            dispatch=public,
            session=_session_presentation_signal(ctx, sessions.get(card.id)),
            watches=watches.get(card.id, ()),
            target_instance_name=(
                public.get("target_instance_name") if public else None
            ),
        )
    return sessions, progress, presentations, dispatches


def _work_presentation_for_card(request: Request, card) -> dict:
    _sessions, _progress, presentations, _dispatches = _presentation_context_for_cards(
        request, [card]
    )
    return presentations[card.id]


def _card_project_impact(request: Request, card, project_id: str | None) -> dict:
    """Describe project-sensitive relationships without changing their provenance."""
    store = get_store()
    source_links = (
        store.list_project_repositories(card.project_id, realm_id=card.realm_id)
        if card.project_id
        else []
    )
    target_repository_ids = {
        repository.id
        for repository, _link in (
            store.list_project_repositories(project_id, realm_id=card.realm_id)
            if project_id
            else []
        )
    }
    repositories = []
    checkout_count = 0
    for repository, link in source_links:
        checkouts = store.list_repository_checkouts(repository.id)
        checkout_count += len(checkouts)
        repositories.append(
            {
                "id": repository.id,
                "name": repository.name or repository.url,
                "branch": link.branch,
                "checkout_count": len(checkouts),
                "available_in_target": repository.id in target_repository_ids,
            }
        )
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    dispatches = (
        [
            record
            for record in dispatch_store.list(limit=1000)
            if record.card_id == card.id
        ]
        if dispatch_store
        else []
    )
    watches = _pr_watch_context(request, card.id, include_events=False)["pr_watches"]
    agent = request.app.state.ctx.services.get("instance_agent")
    workspace_manager = getattr(agent, "workspace_manager", None)
    leases = workspace_manager.list(card_id=card.id) if workspace_manager else []
    incompatible_repositories = [
        repository
        for repository in repositories
        if not repository["available_in_target"]
    ]
    dependent = bool(repositories or dispatches or watches or leases)
    return {
        "repositories": repositories,
        "repository_count": len(repositories),
        "checkout_count": checkout_count,
        "dispatch_count": len(dispatches),
        "session_count": len(
            {record.session_id for record in dispatches if record.session_id}
        ),
        "pr_count": len(watches),
        "workspace_count": len(leases),
        "dependent": dependent,
        "migration_compatible": dependent
        and not incompatible_repositories
        and bool(project_id),
        "incompatible_repositories": incompatible_repositories,
    }


def _card_summary_context(request: Request, card) -> dict:
    store = get_store()
    summary_service = request.app.state.ctx.require_service("card_summary_service")
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
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
        "summary_diagnostics": summary_service.diagnostics(),
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
        "current_progress": _latest_card_progress(request, card.id),
        "work_presentation": _work_presentation_for_card(request, card),
        "card_reconciliations": (
            [
                record.public_dict()["card_reconciliation"]
                | {
                    "dispatch_id": record.dispatch_id,
                    "session_id": record.session_id,
                }
                for record in dispatch_store.list(limit=1000)
                if record.card_id == card.id
                and record.reconciliation_state != "not_requested"
            ]
            if dispatch_store
            else []
        ),
        "lanes": list(CardLane),
        "projects": store.list_projects(realm_id=realm_id),
        "csrf_token": token_for_request(request),
        **watch_context,
    }


def _schedule_card_summary(
    request: Request,
    background_tasks: BackgroundTasks,
    card,
    *,
    force: bool = False,
):
    service = request.app.state.ctx.require_service("card_summary_service")
    card = service.disable_if_unconfigured(card, force=force)
    if card.summary_status.value != "disabled":
        background_tasks.add_task(
            service.schedule,
            card.id,
            card.realm_id,
            force=force,
        )
    return card


def _card_session_view(session, local_instance_id: str) -> dict:
    owner_id = session.origin_instance_id or local_instance_id
    local = owner_id == local_instance_id
    failed = session.status in {"failed", "error", "recovery_blocked"}
    if failed:
        state, detail = (
            "failed",
            "Recovery failed; start a new session or retry after resolving the error.",
        )
    elif not local:
        state, detail = (
            "unavailable",
            "Owned by another instance; history remains available here.",
        )
    elif (
        session.status not in {"idle", "connected", "prompting"}
        and session.external_session_id
    ):
        state, detail = (
            "resumable",
            "Closed locally; the provider thread can be resumed or loaded.",
        )
    elif session.status not in {"idle", "connected", "prompting"}:
        state, detail = (
            "unavailable",
            "Not active and has no resumable provider identity.",
        )
    else:
        state, detail = "active", "Active on this instance and ready to select."
    return {
        "session": session,
        "owner_id": owner_id,
        "state": state,
        "detail": detail,
        "selectable": state in {"active", "resumable"},
        "resumable": state == "resumable",
    }


def _card_agent_context(request: Request, card) -> dict:
    store = get_store()
    ctx = request.app.state.ctx
    related_sessions = store.list_sessions_for_cards({card.id})
    session_views = [
        _card_session_view(session, ctx.settings.instance_id)
        for session in related_sessions
    ]
    # Store order is updated_at DESC. Prefer the most recently updated active
    # local session, then the most recently updated closed-but-resumable one.
    current_view = next(
        (view for view in session_views if view["state"] == "active"),
        next((view for view in session_views if view["resumable"]), None),
    )
    current_session = current_view["session"] if current_view else None
    dispatch_store = ctx.services.get("dispatch_store")
    dispatches = (
        [
            canonicalize_dispatch_public(ctx, record)
            for record in dispatch_store.list(limit=100)
            if record.card_id == card.id and record.realm_id == card.realm_id
        ]
        if dispatch_store
        else []
    )
    fleet = ctx.services.get("fleet_registry")
    instances = (
        sorted(
            fleet.list_instances(),
            key=lambda item: (item.name.lower(), item.instance_id),
        )
        if fleet
        else []
    )
    from pa.fleet.capacity import effective_capacity
    from pa.fleet.overview import build_overview

    overview = build_overview(ctx, instances, []) if instances else {"nodes": []}
    policy_service = ctx.services.get("fleet_policy")
    worker_groups = (
        [
            {
                **group.model_dump(mode="json"),
                "summary": (
                    f"{len(instances)} canonical member"
                    f"{'' if len(instances) == 1 else 's'} before policy filters"
                    if group.system
                    else (
                        f"{len(set(group.included_instance_ids) - set(group.excluded_instance_ids))} "
                        "explicit members before selectors and policy filters"
                    )
                ),
            }
            for group in policy_service.list_groups(card.realm_id)
        ]
        if policy_service
        else []
    )
    participation_summaries = {}
    if policy_service:
        for instance in instances:
            policy, explicit = policy_service.effective_policy(
                card.realm_id, instance.instance_id
            )
            participation_summaries[instance.instance_id] = {
                "summary": policy.summary(),
                "reason": policy.reason,
                "explicit": explicit,
            }
    fleet_capacity: dict[str, dict] = {}
    dispatch_inventory: dict[str, dict] = {}
    for node in overview.get("nodes", []):
        activity = (node.get("dimensions") or {}).get("activity") or {}
        value = activity.get("value") or {}
        capacity = value.get("capacity") or effective_capacity(
            configured=node.get("dispatch_capacity"),
            provider_capacities=node.get("dispatch_provider_capacities") or {},
            capabilities=node.get("capabilities") or [],
        ).model_dump(mode="json")
        consumed = int(capacity.get("consumed") or 0)
        limit = int(capacity.get("limit") or 0)
        queued_prompts = int(value.get("queued_prompts") or 0)
        queued_prompt_label = "prompt" if queued_prompts == 1 else "prompts"
        freshness = activity.get("state") or "unavailable"
        source = str(capacity.get("source") or "unknown").replace("_", " ")
        fleet_capacity[node["id"]] = {
            "consumed": consumed,
            "limit": limit,
            "queued_prompts": queued_prompts,
            "source": source,
            "freshness": freshness,
            "eligible": freshness == "fresh" and limit > consumed,
            "summary": (
                f"{consumed}/{limit} slots used · "
                f"{queued_prompts} {queued_prompt_label} queued · "
                f"{source} · {freshness}"
                if limit
                else f"capacity unavailable · {freshness}"
            ),
        }
        providers = (node.get("dimensions") or {}).get("providers") or {}
        dispatch_inventory[node["id"]] = {
            "instance_name": node.get("name") or node["id"],
            "state": providers.get("state") or "unavailable",
            "observed_at": providers.get("observed_at"),
            "providers": providers.get("value") or [],
        }
    return {
        "card": card,
        "related_sessions": related_sessions,
        "session_views": session_views,
        "current_session_view": current_view,
        "current_session": current_session,
        "dispatches": dispatches,
        "latest_dispatch": dispatches[0] if dispatches else None,
        "fleet_instances": instances,
        "fleet_capacity": fleet_capacity,
        "dispatch_inventory": dispatch_inventory,
        "local_instance_id": ctx.settings.instance_id,
        "local_instance_name": ctx.settings.instance_name,
        "agent_enabled": ctx.settings.agent_enabled,
        "worker_groups": worker_groups,
        "participation_summaries": participation_summaries,
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
                        "instance_id": event.author_instance,
                        "detail": "",
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
                "instance_id": card.created_by_instance or "",
                "detail": "",
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
                "instance_id": instance.get("id") or session.origin_instance_id or "",
                "detail": session.title or session.label or "",
                "timestamp": session.updated_at,
            }
        )

    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    if dispatch_store:
        for dispatch in dispatch_store.list(limit=500):
            if dispatch.card_id != card.id:
                continue
            for event in dispatch.events:
                message = present_instance_references(
                    request.app.state.ctx,
                    event.message,
                    dispatch.target_instance_id,
                    dispatch.target_instance_name,
                )
                entries.append(
                    {
                        "id": f"dispatch-{dispatch.dispatch_id}-{event.seq}",
                        "kind": "dispatch",
                        "label": message,
                        "actor": "Dispatch target",
                        "instance_id": dispatch.target_instance_id,
                        "detail": event.state.replace("_", " "),
                        "timestamp": event.created_at,
                    }
                )
            for snapshot in dispatch.turn_end_snapshots:
                entries.append(
                    {
                        "id": f"turn-end-{snapshot.snapshot_id}",
                        "kind": "progress",
                        "label": "Agent turn ended",
                        "actor": current_instance_name(
                            request.app.state.ctx,
                            dispatch.target_instance_id,
                            dispatch.target_instance_name,
                        ),
                        "detail": (
                            f"stop reason {snapshot.stop_reason or 'unknown'}; "
                            f"dispatch {snapshot.dispatch_state}"
                        ),
                        "timestamp": snapshot.captured_at,
                        "session_url": (
                            f"/agent?session={snapshot.session_id}"
                            f"&instance={snapshot.originating_instance_id}"
                            if snapshot.session_id
                            else None
                        ),
                        "details": [
                            {
                                "label": "Card lane",
                                "value": (
                                    f"{snapshot.card_lane_before or 'unknown'} → "
                                    f"{snapshot.card_lane_after or 'unchanged'}"
                                ),
                            },
                            {
                                "label": "Completion delivery",
                                "value": snapshot.completion_delivery.get(
                                    "classification"
                                ),
                            },
                            {
                                "label": "Disposition",
                                "value": snapshot.disposition_status,
                            },
                            *(
                                [
                                    {
                                        "label": "Disposition extraction error",
                                        "value": snapshot.disposition_parse_error,
                                    }
                                ]
                                if snapshot.disposition_parse_error
                                else []
                            ),
                        ],
                    }
                )
            for evaluation in dispatch.post_turn_evaluations:
                entries.append(
                    {
                        "id": f"evaluation-{evaluation.evaluation_id}",
                        "kind": "progress",
                        "label": evaluation.operator_status_text,
                        "actor": "PA post-turn evaluator",
                        "detail": evaluation.decision.value.replace("_", " "),
                        "timestamp": evaluation.created_at,
                        "details": [
                            {
                                "label": "Rationale",
                                "value": evaluation.rationale,
                            },
                            {
                                "label": "Confidence",
                                "value": f"{evaluation.confidence:.0%}",
                            },
                            *[
                                {
                                    "label": f"Action · {action.name.value}",
                                    "value": action.status.value,
                                }
                                for action in evaluation.recommended_actions
                            ],
                        ],
                    }
                )
            for progress in dispatch.progress_events:
                details = []
                details.extend(
                    {
                        "label": validation.command,
                        "value": validation.status
                        + (f" · {validation.summary}" if validation.summary else ""),
                    }
                    for validation in progress.validations
                )
                details.extend(
                    {
                        "label": tool.title,
                        "value": " · ".join(
                            value
                            for value in (tool.kind, tool.status, tool.result)
                            if value
                        ),
                    }
                    for tool in progress.tool_details
                )
                details.extend(
                    {"label": "Blocker", "value": blocker}
                    for blocker in progress.blockers
                )
                if progress.operator_input:
                    details.append(
                        {
                            "label": "Operator input requested",
                            "value": progress.operator_input,
                        }
                    )
                entries.append(
                    {
                        "id": (
                            f"progress-{dispatch.dispatch_id}-"
                            f"{progress.idempotency_key}"
                        ),
                        "kind": "progress",
                        "label": progress.summary,
                        "actor": current_instance_name(
                            request.app.state.ctx,
                            dispatch.target_instance_id,
                            dispatch.target_instance_name,
                        ),
                        "detail": progress.phase.value.replace("_", " "),
                        "timestamp": progress.occurred_at,
                        "session_url": (
                            f"/agent?session={progress.acp_session_id}"
                            f"&instance={progress.originating_instance_id}"
                        ),
                        "details": details,
                    }
                )
            if dispatch.final_report and not any(
                event.kind.value == "final" for event in dispatch.progress_events
            ):
                entries.append(
                    {
                        "id": f"progress-report-{dispatch.dispatch_id}",
                        "kind": "progress",
                        "label": dispatch.final_report.outcome,
                        "actor": current_instance_name(
                            request.app.state.ctx,
                            dispatch.target_instance_id,
                            dispatch.target_instance_name,
                        ),
                        "detail": "completion report",
                        "timestamp": dispatch.final_report.created_at,
                        "session_url": (
                            f"/agent?session={dispatch.session_id}"
                            f"&instance={dispatch.target_instance_id}"
                            if dispatch.session_id
                            else None
                        ),
                        "details": [
                            {
                                "label": validation.command,
                                "value": validation.status,
                            }
                            for validation in dispatch.final_report.validations
                        ],
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
        "activity": sorted(entries, key=lambda entry: entry["timestamp"], reverse=True),
    }


def _canonical_presentation_counts(request: Request) -> dict[str, int]:
    """Count every lifecycle group from fixed-size, body-free cached pages."""
    store = get_store()
    realm = _active_realm(request)
    counts = {group: 0 for group in ("attention", "motion", "outcome", "quiet")}
    offset = 0
    while True:
        page = store.list_card_work_projections(
            realm_id=realm,
            limit=WORK_PRESENTATION_PAGE_LIMIT,
            offset=offset,
        )
        if not page:
            break
        _, _, presentations, _ = _presentation_context_for_cards(request, page)
        for card in page:
            group = presentations[card.id]["group"]
            counts[group] = counts.get(group, 0) + 1
        offset += len(page)
        if len(page) < WORK_PRESENTATION_PAGE_LIMIT:
            break
    return counts


def _bounded_attention_cards_context(
    request: Request,
    *,
    kind: CardKind | None,
    lane: CardLane | None,
    result_limit: int,
    result_offset: int = 0,
) -> dict:
    """Filter lifecycle groups in fixed-size body-free pages before hydration."""
    store = get_store()
    realm = _active_realm(request)
    project_id = _active_project(request)
    query = request.query_params.get("q", "").strip()
    owner = request.query_params.get("owner", "").strip()
    instance = request.query_params.get("instance", "").strip()
    blocked = request.query_params.get("blocked", "").strip()
    tag = request.query_params.get("tag", "").strip()
    updated = request.query_params.get("updated", "").strip()
    try:
        updated_days = int(updated) if updated else None
    except ValueError:
        updated = ""
        updated_days = None
    attention = request.query_params.get("attention", "").strip()
    expected_group = {
        "actionable": "attention",
        "motion": "motion",
        "outcome": "outcome",
    }[attention]
    matching_ids: list[str] = []
    total_cards = 0
    offset = 0
    while True:
        projection_page = store.list_card_work_projections(
            realm_id=realm,
            lane=lane,
            kind=kind,
            project_id=project_id,
            query=query,
            owner=owner,
            instance=instance,
            blocked=blocked,
            tag=tag,
            updated_days=updated_days,
            limit=WORK_PRESENTATION_PAGE_LIMIT,
            offset=offset,
        )
        if not projection_page:
            break
        _, _, page_presentations, _ = _presentation_context_for_cards(
            request, projection_page
        )
        for projection in projection_page:
            if page_presentations[projection.id]["group"] != expected_group:
                continue
            total_cards += 1
            match_index = total_cards - 1
            if (
                match_index >= result_offset
                and len(matching_ids) < result_limit
            ):
                matching_ids.append(projection.id)
        offset += len(projection_page)
        if len(projection_page) < WORK_PRESENTATION_PAGE_LIMIT:
            break

    cards_by_id = {
        card.id: card
        for card in store.list_cards_by_ids(matching_ids, realm_id=realm)
    }
    cards = [
        cards_by_id[card_id]
        for card_id in matching_ids
        if card_id in cards_by_id
    ]
    projects = store.list_projects(realm_id=realm)
    project_by_id = {project.id: project for project in projects}
    card_sessions, card_progress, card_presentations, _ = (
        _presentation_context_for_cards(request, cards)
    )
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
        "attention": attention,
    }
    return {
        "cards": cards,
        "total_cards": total_cards,
        "page_offset": result_offset,
        "items": [Item.from_card(card) for card in cards],
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": projects,
        "card_projects": {
            card.id: project_by_id.get(card.project_id) for card in cards
        },
        "card_sessions": card_sessions,
        "card_progress": card_progress,
        "card_presentations": card_presentations,
        "owners": [],
        "instances": [],
        "tags": [],
        "filters": {
            "q": query,
            "project": project_id or "",
            "kind": kind.value if kind else "",
            "owner": owner,
            "instance": instance,
            "blocked": blocked,
            "tag": tag,
            "updated": updated,
            "attention": attention,
        },
        "filter_query": urlencode(
            {key: value for key, value in filter_params.items() if value}
        ),
        "realms": request.app.state.ctx.settings.subscribed_realms,
        "active_realm": realm,
        "active_project": project_id,
    }


def _cards_context(
    request: Request,
    *,
    kind: CardKind | None = None,
    lane: CardLane | None = None,
    apply_filters: bool = True,
    result_limit: int | None = None,
    result_offset: int = 0,
) -> dict:
    store = get_store()
    realm = _active_realm(request)
    project_id = _active_project(request) if apply_filters else None
    if apply_filters and kind is None and request.query_params.get("kind"):
        try:
            kind = CardKind(request.query_params["kind"])
        except ValueError:
            kind = None
    attention_filter = (
        request.query_params.get("attention", "").strip() if apply_filters else ""
    )
    if (
        result_limit is not None
        and attention_filter in {"actionable", "motion", "outcome"}
        and hasattr(store, "list_card_work_projections")
    ):
        return _bounded_attention_cards_context(
            request,
            kind=kind,
            lane=lane,
            result_limit=result_limit,
            result_offset=result_offset,
        )
    query = request.query_params.get("q", "").strip() if apply_filters else ""
    owner = request.query_params.get("owner", "").strip() if apply_filters else ""
    instance = request.query_params.get("instance", "").strip() if apply_filters else ""
    blocked = request.query_params.get("blocked", "").strip() if apply_filters else ""
    tag = request.query_params.get("tag", "").strip() if apply_filters else ""
    updated = request.query_params.get("updated", "").strip() if apply_filters else ""
    try:
        updated_days = int(updated) if updated else None
    except ValueError:
        updated = ""
        updated_days = None
    page_limit = 100 if result_limit is None else max(1, min(int(result_limit), 100))
    cards = store.list_card_work_projections(
        realm_id=realm,
        lane=lane,
        kind=kind,
        project_id=project_id,
        query=query,
        owner=owner,
        instance=instance,
        blocked=blocked,
        tag=tag,
        updated_days=updated_days,
        limit=page_limit,
        offset=max(0, result_offset),
    )
    total_cards = store.count_card_work_projections(
        realm_id=realm,
        lane=lane,
        kind=kind,
        project_id=project_id,
        query=query,
        owner=owner,
        instance=instance,
        blocked=blocked,
        tag=tag,
        updated_days=updated_days,
    )
    projects = store.list_projects(realm_id=realm)
    project_by_id = {project.id: project for project in projects}
    card_sessions, card_progress, card_presentations, _ = (
        _presentation_context_for_cards(request, cards)
    )
    facets = store.list_card_filter_facets(realm_id=realm)
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
        "attention": attention_filter,
    }
    return {
        "cards": cards,
        "total_cards": total_cards,
        "items": [Item.from_card(c) for c in cards],
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": projects,
        "card_projects": {
            card.id: project_by_id.get(card.project_id) for card in cards
        },
        "card_sessions": card_sessions,
        "card_progress": card_progress,
        "card_presentations": card_presentations,
        "owners": facets["owners"],
        "instances": [
            {
                "id": instance_id,
                "display_name": resolve_instance_identity(
                    request.app.state.ctx, instance_id
                )["display_name"],
            }
            for instance_id in facets["instances"]
        ],
        "tags": facets["tags"],
        "filters": {
            "q": query,
            "project": project_id or "",
            "kind": kind.value if kind else "",
            "owner": owner,
            "instance": instance,
            "blocked": blocked,
            "tag": tag,
            "updated": updated,
            "attention": attention_filter,
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


def _work_context(request: Request) -> dict:
    """Build only filter metadata; lane rows are fetched as bounded partials."""
    realm = _active_realm(request)
    store = get_store()
    facets = store.list_card_filter_facets(realm_id=realm)
    projects = store.list_projects(realm_id=realm)
    project_id = _active_project(request)
    selected_lane = request.query_params.get("lane", CardLane.ACTIVE.value)
    if selected_lane not in {lane.value for lane in CardLane}:
        selected_lane = CardLane.ACTIVE.value
    filters = {
        key: request.query_params.get(key, "").strip()
        for key in (
            "q",
            "owner",
            "instance",
            "blocked",
            "tag",
            "updated",
            "attention",
        )
    }
    filters["project"] = project_id or ""
    filters["kind"] = request.query_params.get("kind", "").strip()
    filter_params = {"realm": realm, **filters}
    return {
        "kinds": list(CardKind),
        "lanes": list(CardLane),
        "projects": projects,
        "owners": facets["owners"],
        "instances": [
            {
                "id": instance_id,
                "display_name": resolve_instance_identity(
                    request.app.state.ctx, instance_id
                )["display_name"],
            }
            for instance_id in facets["instances"]
        ],
        "tags": facets["tags"],
        "filters": filters,
        "filter_query": urlencode(
            {key: value for key, value in filter_params.items() if value}
        ),
        "selected_lane": selected_lane,
        "active_realm": realm,
    }


def _home_context(request: Request) -> dict:
    """Build Home from one bounded, cached-first operational projection."""
    from pa.fleet.overview import build_overview
    from pa.fleet.workshop import build_workshop_snapshot

    realm = _active_realm(request)
    ctx = request.app.state.ctx
    agent = ctx.services.get("instance_agent")
    fleet = ctx.services.get("fleet_registry")
    peer_table = ctx.services.get("peer_table")
    fleet_instances = list(fleet.list_instances())[:HOME_FLEET_LIMIT] if fleet else []
    routes = list(peer_table.all_routes())[:HOME_ROUTE_LIMIT] if peer_table else []
    overview = (
        build_overview(ctx, fleet_instances, routes)
        if fleet_instances
        else {"nodes": [], "edges": []}
    )
    snapshot = build_workshop_snapshot(ctx, overview, realm_id=realm)
    canonical_counts = _canonical_presentation_counts(request)
    snapshot_total = snapshot["counts"].get(
        "total", snapshot.get("inventory", {}).get("total", 0)
    )
    if sum(canonical_counts.values()) != snapshot_total:
        projected_counts = snapshot["counts"].get("presentations", {})
        rendered_counts = {
            group: sum(
                1
                for item in snapshot.get("work_orders", ())
                if item.get("presentation", {}).get("group") == group
            )
            for group in ("attention", "motion", "outcome", "quiet")
        }
        canonical_counts = {
            "attention": projected_counts.get(
                "attention", rendered_counts["attention"]
            ),
            "motion": projected_counts.get("motion", rendered_counts["motion"]),
            "outcome": snapshot["counts"].get("lanes", {}).get(
                CardLane.DONE.value,
                projected_counts.get("outcome", rendered_counts["outcome"]),
            ),
            "quiet": projected_counts.get("quiet", rendered_counts["quiet"]),
        }
    work_orders = list(snapshot["work_orders"])

    def sort_key(item: dict) -> tuple[int, str, str]:
        presentation = item["presentation"]
        return (
            int(presentation["priority"]),
            str(presentation.get("occurred_at") or item.get("updated_at") or ""),
            str(item["id"]),
        )

    attention = sorted(
        (item for item in work_orders if item["presentation"]["group"] == "attention"),
        key=sort_key,
        reverse=True,
    )
    in_motion = sorted(
        (item for item in work_orders if item["presentation"]["group"] == "motion"),
        key=sort_key,
        reverse=True,
    )
    outcomes = sorted(
        (item for item in work_orders if item["presentation"]["group"] == "outcome"),
        key=sort_key,
        reverse=True,
    )
    return {
        "needs_attention": attention[:HOME_ATTENTION_LIMIT],
        "needs_attention_total": canonical_counts["attention"],
        "active_work": in_motion[:HOME_MOTION_LIMIT],
        "active_work_total": canonical_counts["motion"],
        "recent_outcomes": outcomes[:HOME_OUTCOME_LIMIT],
        "recent_outcomes_total": canonical_counts["outcome"],
        "home_inventory": snapshot["inventory"],
        "agent_connected": bool(agent and agent.connected),
        "fleet_instances": fleet_instances,
        "instance_name": ctx.settings.instance_name,
        "realms": ctx.settings.subscribed_realms,
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
    instances = canonical_instance_identities(ctx)
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
    source = request.query_params.get("source", "").strip() or None
    return {
        "knowledge": store.list_knowledge(
            limit=100,
            search=query or None,
            kind=kind,
            status=status,
            scope=scope,
            source=source,
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
            "source": source or "",
        },
        "knowledge_sources": ["promoted", "manual", "generated", "imported"],
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


@router.get("/cards/summary/diagnostics")
def card_summary_diagnostics_api(request: Request) -> dict:
    """Return redaction-safe effective summary configuration and failure state."""
    return request.app.state.ctx.require_service("card_summary_service").diagnostics()


def _operation_key(request: Request) -> str:
    supplied = request.headers.get("Idempotency-Key", "")
    key = supplied.strip() if isinstance(supplied, str) else ""
    if not key:
        key = f"server-generated:{uuid4()}"
    if len(key) > 300:
        raise HTTPException(
            status_code=422,
            detail={"code": "idempotency_key_too_long", "max_length": 300},
        )
    return key


def _operation_fingerprint(request: Request, operation: str, payload: dict) -> str:
    canonical = {
        "operation": operation,
        "principal": get_principal_id(request),
        "payload": payload,
    }
    return hashlib.sha256(
        json.dumps(
            canonical, sort_keys=True, separators=(",", ":"), default=str
        ).encode()
    ).hexdigest()


def _replay_operation(
    request: Request,
    *,
    operation: str,
    realm_id: str,
    payload: dict,
    store=None,
) -> tuple[str, str, dict | None]:
    """Replay a known receipt before validating state that may have advanced."""
    key = _operation_key(request)
    fingerprint = _operation_fingerprint(request, operation, payload)
    operation_store = store or get_store()
    try:
        replay = operation_store.replay_operation(
            idempotency_key=key,
            operation=operation,
            request_fingerprint=fingerprint,
            realm_id=realm_id,
        )
    except MutationOperationConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": str(exc),
                "idempotency_key": key,
                "recovery_state": "new_key_required",
            },
        ) from exc
    except MutationOperationInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "operation_in_progress",
                "message": str(exc),
                "idempotency_key": key,
                "correlation_id": exc.correlation_id,
                "recoverable": True,
                "recovery_state": "lookup_required",
                "recovery_action": "get_operation_outcome",
            },
        ) from exc
    return key, fingerprint, replay


def _begin_operation(
    request: Request,
    *,
    operation: str,
    realm_id: str,
    payload: dict,
    store=None,
) -> tuple[str, str, dict | None]:
    key = _operation_key(request)
    fingerprint = _operation_fingerprint(request, operation, payload)
    operation_store = store or get_store()
    supplied_correlation = request.headers.get("X-Request-ID")
    correlation_id = (
        supplied_correlation
        if isinstance(supplied_correlation, str)
        else None
    )
    try:
        replay = operation_store.begin_operation(
            idempotency_key=key,
            operation=operation,
            request_fingerprint=fingerprint,
            realm_id=realm_id,
            correlation_id=correlation_id,
        )
    except MutationOperationConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "idempotency_conflict",
                "message": str(exc),
                "idempotency_key": key,
                "recovery_state": "new_key_required",
            },
        ) from exc
    except MutationOperationInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "operation_in_progress",
                "message": str(exc),
                "idempotency_key": key,
                "correlation_id": exc.correlation_id,
                "recoverable": True,
                "recovery_state": "lookup_required",
                "recovery_action": "get_operation_outcome",
            },
        ) from exc
    return key, fingerprint, replay


@router.post("/cards", status_code=201)
def create_card_api(
    request: Request,
    response: Response,
    data: CardCreate,
    background_tasks: BackgroundTasks,
    _idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=300),
    ],
) -> dict:
    store = get_store()
    settings = request.app.state.ctx.settings
    payload = data.model_dump(mode="json") | {"auto_enrich": data.auto_enrich}
    key, fingerprint, replay = _begin_operation(
        request, operation="card.create", realm_id=data.realm_id, payload=payload
    )
    response.headers["X-PA-Operation-ID"] = key
    if replay is not None:
        response.headers["X-PA-Operation-Replayed"] = "true"
        return replay
    try:
        card = store.create_card(
            data,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
        if data.auto_enrich:
            background_tasks.add_task(
                enrich_card,
                request.app.state.ctx,
                card.id,
                card.realm_id,
                explicit_enrichment_fields(data),
            )
        if not data.summary.strip():
            card = _schedule_card_summary(request, background_tasks, card)
        result = card.model_dump(mode="json")
        store.complete_operation(key, result)
        return result
    except Exception as exc:
        store.fail_operation(key, type(exc).__name__)
        raise


@router.get("/operations/{idempotency_key}")
def operation_outcome_api(
    request: Request, idempotency_key: str, realm: str | None = None
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    outcome = get_store().get_operation_outcome(
        idempotency_key, realm_id=realm_id
    )
    if outcome["status"] != "not_found":
        return outcome
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    if dispatch_store is None:
        return outcome
    dispatch = dispatch_store.find_operation_by_idempotency(
        idempotency_key, realm_id=realm_id
    )
    if dispatch is None:
        return outcome
    operation, record = dispatch
    if record.realm_id != realm_id:
        return outcome
    return {
        "idempotency_key": idempotency_key,
        "operation": operation,
        "status": "succeeded",
        "durable": True,
        "recovery_state": "durable_dispatch_record_found",
        "result": {
            "dispatch_id": record.dispatch_id,
            "state": record.state,
            "card_id": record.card_id,
            "session_id": record.session_id,
        },
    }


@router.get("/cards/{card_id}")
def get_card_api(request: Request, card_id: str, realm: str | None = None) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return card.model_dump(mode="json")


@router.get("/cards/{card_id}/history")
def card_history_api(
    request: Request,
    card_id: str,
    realm: str | None = None,
    limit: int = 100,
    cursor: str | None = None,
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    log = request.app.state.ctx.require_service("event_log")
    if limit < 1 or limit > HISTORY_PAGE_LIMIT:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "invalid_history_limit",
                "message": f"limit must be between 1 and {HISTORY_PAGE_LIMIT}",
            },
        )
    try:
        page = log.entity_history_page(
            realm_id, "card", card_id, limit=limit, cursor=cursor
        )
    except EventHistoryError as exc:
        raise HTTPException(status_code=422, detail=exc.as_detail()) from exc
    return {"card_id": card_id, "realm_id": realm_id, **page}


@router.post("/cards/repair-legacy-history")
def repair_legacy_card_history_api(request: Request, body: CardRepairRequest) -> dict:
    principal_id = (
        "instance:fleet"
        if getattr(request.state, "instance_authenticated", False)
        else get_principal_id(request)
    )
    results = get_store().repair_legacy_card_history(
        body.card_ids,
        realm_id=body.realm_id,
        principal_id=principal_id,
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return {
        "realm_id": body.realm_id,
        "results": results,
        "head": request.app.state.ctx.require_service("event_log").get_head(
            body.realm_id
        ),
    }


@router.patch("/cards/{card_id}")
def update_card_api(
    request: Request,
    response: Response,
    card_id: str,
    data: CardUpdate,
    background_tasks: BackgroundTasks,
    _idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=300),
    ],
    realm: str | None = None,
) -> dict:
    settings = request.app.state.ctx.settings
    realm_id = realm or settings.primary_realm
    payload = {
        "card_id": card_id,
        **data.model_dump(mode="json", exclude_unset=True),
        "expected_version": (
            data.expected_version.isoformat() if data.expected_version else None
        ),
        "field_intent": data.field_intent,
    }
    key, fingerprint, replay = _begin_operation(
        request, operation="card.update", realm_id=realm_id, payload=payload
    )
    response.headers["X-PA-Operation-ID"] = key
    if replay is not None:
        response.headers["X-PA-Operation-Replayed"] = "true"
        return replay
    store = get_store()
    try:
        card = store.update_card(
            card_id,
            data,
            realm_id=realm_id,
            principal_id=get_principal_id(request),
            instance_id=settings.instance_id,
            idempotency_key=key,
            request_fingerprint=fingerprint,
        )
    except CardVersionConflict as exc:
        store.fail_operation(key, "stale_card_version")
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_card_version",
                "card_id": exc.card_id,
                "expected_version": exc.expected.isoformat(),
                "actual_version": exc.actual.isoformat(),
                "message": "The card changed after this snapshot was read. Retry with field_intent or the current updated_at.",
            },
        ) from exc
    except Exception as exc:
        store.fail_operation(key, type(exc).__name__)
        raise
    if not card:
        store.fail_operation(key, "card_not_found")
        raise HTTPException(status_code=404, detail="Card not found")
    if {"title", "body"} & data.model_fields_set:
        card = _schedule_card_summary(request, background_tasks, card)
    result = card.model_dump(mode="json")
    store.complete_operation(key, result)
    return result


@router.post("/cards/{card_id}/summary/regenerate", status_code=202)
def regenerate_card_summary_api(
    request: Request,
    card_id: str,
    background_tasks: BackgroundTasks,
    realm: str | None = None,
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    service = request.app.state.ctx.require_service("card_summary_service")
    diagnostics = service.diagnostics()
    if not service.is_authority:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "summary_authority_remote",
                "message": "Regenerate the summary on the fleet-owner instance.",
                "authority": "fleet_owner",
                "recoverable": True,
            },
        )
    card = _schedule_card_summary(request, background_tasks, card, force=True)
    return {
        "card_id": card.id,
        "summary_status": (
            "disabled" if diagnostics["state"] == "disabled" else "pending"
        ),
        "summary_configuration": diagnostics,
    }


@router.get("/items")
def list_items(
    response: Response,
    kind: ItemKind | None = None,
    status: ItemStatus | None = None,
) -> list[dict]:
    _mark_legacy_item_api(response)
    items = get_store().list_items(kind=kind, status=status)
    return [item.model_dump(mode="json") for item in items]


def _attachment_store(request: Request) -> AttachmentStore:
    service = request.app.state.ctx.services.get("attachment_store")
    if not isinstance(service, AttachmentStore):
        service = AttachmentStore(request.app.state.ctx.settings.data_dir)
        request.app.state.ctx.register_service("attachment_store", service)
    return service


def _card_attachment_or_404(
    request: Request, card_id: str, attachment_id: str, realm: str | None = None
):
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    attachment = next(
        (item for item in card.attachments if item.attachment_id == attachment_id), None
    )
    if not attachment:
        raise HTTPException(status_code=404, detail="Attachment not found")
    return card, attachment


@router.post("/cards/{card_id}/attachments", status_code=201)
async def create_card_attachment_api(
    request: Request,
    card_id: str,
    file: UploadFile = File(...),
    realm: str | None = None,
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    runtime = request.app.state.ctx.require_service("async_runtime")
    try:
        sha256, size = await runtime.run_blocking(
            "cards.attachments.ingest",
            _attachment_store(request).ingest,
            file.file,
            timeout=120.0,
        )
        attachment = CardAttachment(
            card_id=card.id,
            realm_id=realm_id,
            filename=safe_filename(file.filename or "attachment"),
            media_type=_attachment_media_type(
                file.filename or "attachment", file.content_type
            ),
            size=size,
            sha256=sha256,
            blob_ref=f"sha256:{sha256}",
            created_by_principal=get_principal_id(request),
            created_by_instance=request.app.state.ctx.settings.instance_id,
            visibility=card.visibility,
        )
        get_store().add_attachment(
            attachment,
            principal_id=get_principal_id(request),
            instance_id=request.app.state.ctx.settings.instance_id,
        )
        return attachment.model_dump(mode="json")
    except AttachmentError as exc:
        raise HTTPException(
            status_code=413 if not exc.recoverable else 409, detail=exc.detail()
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc
    finally:
        await file.close()


@router.get("/cards/{card_id}/attachments/{attachment_id}")
def download_card_attachment_api(
    request: Request,
    card_id: str,
    attachment_id: str,
    realm: str | None = None,
    download: bool = False,
) -> FileResponse:
    _card, attachment = _card_attachment_or_404(request, card_id, attachment_id, realm)
    blobs = _ensure_attachment_available(request, attachment)
    inline = (
        attachment.media_type in SAFE_IMAGE_TYPES
        or attachment.media_type.startswith(("video/", "audio/"))
    )
    return FileResponse(
        blobs.blob_path(attachment.sha256),
        filename=attachment.filename,
        media_type=attachment.media_type,
        content_disposition_type="attachment" if download or not inline else "inline",
        headers={
            "Content-Security-Policy": "sandbox; default-src 'none'; img-src 'self'; media-src 'self'",
            "X-Content-Type-Options": "nosniff",
            "Accept-Ranges": "bytes",
            "Cache-Control": "private, no-store",
            "Digest": f"sha-256={attachment.sha256}",
        },
    )


@router.delete("/cards/{card_id}/attachments/{attachment_id}")
def remove_card_attachment_api(
    request: Request, card_id: str, attachment_id: str, realm: str | None = None
) -> dict:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    _card_attachment_or_404(request, card_id, attachment_id, realm_id)
    card = get_store().remove_attachment(
        card_id,
        attachment_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return card.model_dump(mode="json")


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
    source: str | None = None,
) -> list[dict]:
    entries = get_store().list_knowledge(
        item_id=item_id,
        limit=limit,
        search=q,
        kind=kind.value if kind else None,
        status=status.value if status else None,
        scope=scope,
        source=source,
    )
    return [entry.model_dump(mode="json") for entry in entries]


@router.post("/knowledge", status_code=201)
def create_knowledge(request: Request, data: KnowledgeEntry) -> dict:
    actor = _require_memory_editor(request)
    if not data.owner:
        data.owner = actor
    if data.source in {"acp_session", "promoted"} and not data.provenance:
        raise HTTPException(
            status_code=422,
            detail="Transcript-derived Memory must use the promotion API with provenance",
        )
    entry = get_store().add_knowledge(data)
    get_store().add_knowledge_audit(
        KnowledgeAuditEvent(
            knowledge_id=entry.id,
            action="created",
            actor=actor,
            payload={"source": entry.source, "scope": entry.scope},
        )
    )
    return entry.model_dump(mode="json")


@router.post("/knowledge/promote", status_code=201)
def promote_knowledge(request: Request, data: KnowledgePromotionRequest) -> dict:
    actor = _require_memory_editor(request)
    try:
        entry = promote_from_transcript(
            get_store(),
            session_id=data.session_id,
            actor=actor,
            summary=data.summary,
            start_seq=data.start_seq,
            end_seq=data.end_seq,
            kind=data.kind,
            scope=data.scope,
            source_url=data.source_url,
            confidence=data.confidence,
            card_id=data.card_id,
            tags=data.tags,
            supersedes_id=data.supersedes_id,
            review_at=data.review_at,
            expires_at=data.expires_at,
        )
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return entry.model_dump(mode="json")


@router.get("/knowledge/audit")
def audit_knowledge() -> dict:
    records = audit_knowledge_records(get_store())
    return {"records": records, "count": len(records), "mutated": False}


@router.get("/knowledge/{entry_id}/audit")
def knowledge_audit_events(entry_id: str) -> list[dict]:
    if not get_store().get_knowledge(entry_id):
        raise HTTPException(status_code=404, detail="Memory not found")
    return [
        event.model_dump(mode="json")
        for event in get_store().list_knowledge_audit(entry_id)
    ]


@router.post("/knowledge/{entry_id}/regenerate", status_code=201)
def regenerate_knowledge_api(request: Request, entry_id: str) -> dict:
    actor = _require_memory_editor(request)
    try:
        entry = regenerate_knowledge(get_store(), entry_id=entry_id, actor=actor)
    except ValueError as exc:
        status = 404 if str(exc).startswith("Memory not found:") else 409
        raise HTTPException(status_code=status, detail=str(exc)) from exc
    return entry.model_dump(mode="json")


@router.post("/knowledge/bulk")
def bulk_update_knowledge(request: Request, data: KnowledgeBulkRequest) -> dict:
    actor = _require_memory_editor(request)
    status = (
        KnowledgeStatus.ARCHIVED
        if data.action == "archive"
        else KnowledgeStatus.SUPERSEDED
    )
    updated: list[str] = []
    missing: list[str] = []
    for entry_id in dict.fromkeys(data.ids):
        try:
            record_lifecycle_change(
                get_store(),
                entry_id=entry_id,
                status=status,
                actor=actor,
                action=f"bulk_{data.action}",
            )
            updated.append(entry_id)
        except ValueError:
            missing.append(entry_id)
    return {"updated": updated, "missing": missing, "status": status.value}


@router.patch("/knowledge/{entry_id}")
def update_knowledge(request: Request, entry_id: str, data: KnowledgeUpdate) -> dict:
    actor = _require_memory_editor(request)
    entry = get_store().update_knowledge(entry_id, data)
    if not entry:
        raise HTTPException(status_code=404, detail="Memory not found")
    get_store().add_knowledge_audit(
        KnowledgeAuditEvent(
            knowledge_id=entry.id,
            action="updated",
            actor=actor,
            payload={"fields": sorted(data.model_fields_set)},
        )
    )
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
    background_tasks: BackgroundTasks,
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
    auto_enrich: bool = Form(True),
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
    settings = request.app.state.ctx.settings
    principal = get_principal_id(request)
    create_data = CardCreate(
        realm_id=realm,
        kind=kind,
        title=cleaned_title,
        body=_compose_card_body(
            body,
            link_urls=link_urls or [],
            link_labels=link_labels or [],
            attachments=[],
            file_tokens=file_tokens or [],
        ),
        summary=summary.strip(),
        summary_source=(CardSummarySource.MANUAL if summary.strip() else None),
        lane=lane,
        parent_id=selected_parent,
        project_id=selected_project,
        tags=_comma_separated(tags),
        preferred_instance=preferred_instance.strip() or None,
        preferred_capabilities=_comma_separated(preferred_capabilities),
        auto_enrich=auto_enrich,
    )
    card = store.create_card(
        create_data,
        principal_id=principal,
        instance_id=settings.instance_id,
    )
    if not summary.strip():
        card = _schedule_card_summary(request, background_tasks, card)
    if auto_enrich:
        background_tasks.add_task(
            enrich_card,
            request.app.state.ctx,
            card.id,
            card.realm_id,
            explicit_enrichment_fields(create_data),
        )
    try:
        for index, upload in enumerate(uploads, 1):
            sha256, size = await runtime.run_blocking(
                "cards.attachments.ingest",
                _attachment_store(request).ingest,
                upload.file,
                timeout=120.0,
            )
            filename = safe_filename(upload.filename or "", index)
            attachment = CardAttachment(
                card_id=card.id,
                realm_id=realm,
                filename=filename,
                media_type=_attachment_media_type(filename, upload.content_type),
                size=size,
                sha256=sha256,
                blob_ref=f"sha256:{sha256}",
                created_by_principal=principal,
                created_by_instance=settings.instance_id,
                visibility=card.visibility,
            )
            card = store.add_attachment(
                attachment, principal_id=principal, instance_id=settings.instance_id
            )
            attachments.append(
                {
                    **attachment.model_dump(mode="json"),
                    "url": f"/card-attachments/{attachment.attachment_id}/{quote(attachment.filename, safe='')}",
                }
            )
        if attachments:
            card = store.update_card(
                card.id,
                CardUpdate(
                    body=_compose_card_body(
                        body,
                        link_urls=link_urls or [],
                        link_labels=link_labels or [],
                        attachments=attachments,
                        file_tokens=file_tokens or [],
                    )
                ),
                realm_id=realm,
                principal_id=principal,
                instance_id=settings.instance_id,
            )
    finally:
        for upload in uploads:
            await upload.close()

    return JSONResponse(
        card.model_dump(mode="json"),
        status_code=201,
        headers={"Location": f"/cards/{card.id}"},
    )


def _ensure_attachment_available(
    request: Request, attachment: CardAttachment
) -> AttachmentStore:
    blobs = _attachment_store(request)
    if blobs.has_verified_blob(attachment.sha256, attachment.size):
        return blobs
    fleet = request.app.state.ctx.services.get("fleet_registry")
    sources = [item for item in (fleet.list_instances() if fleet else []) if item.url]
    sources.sort(key=lambda item: item.instance_id != attachment.created_by_instance)
    settings = request.app.state.ctx.settings
    sources = [item for item in sources if item.instance_id != settings.instance_id]
    headers = {"X-PA-Origin-Instance-ID": settings.instance_id}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    failures: list[dict[str, object]] = []
    for source in sources:
        try:
            with httpx.stream(
                "GET",
                f"{source.url.rstrip('/')}/api/fleet/attachments/{attachment.card_id}/{attachment.attachment_id}",
                params={"realm_id": attachment.realm_id},
                headers=headers,
                timeout=120.0,
            ) as response:
                if response.status_code >= 400:
                    failures.append(
                        {
                            "instance_id": source.instance_id,
                            "status": response.status_code,
                        }
                    )
                    continue
                if response.headers.get("X-PA-Attachment-SHA256") != attachment.sha256:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "attachment_source_substitution",
                            "source_instance_id": source.instance_id,
                            "recoverable": False,
                        },
                    )
                blobs.ingest_chunks(
                    response.iter_bytes(1024 * 1024),
                    expected_sha256=attachment.sha256,
                    expected_size=attachment.size,
                )
                return blobs
        except httpx.HTTPError as exc:
            failures.append(
                {"instance_id": source.instance_id, "error": str(exc)[:300]}
            )
        except AttachmentError as exc:
            raise HTTPException(status_code=409, detail=exc.detail()) from exc
    raise HTTPException(
        status_code=503,
        detail={
            "code": "attachment_source_unavailable",
            "recoverable": True,
            "source_instance_id": attachment.created_by_instance,
            "attempts": failures,
        },
    )


@ui_router.get("/card-attachments/{attachment_id}/{filename}")
def card_attachment(
    request: Request,
    attachment_id: str,
    filename: str,
) -> FileResponse:
    if not ATTACHMENT_ID_RE.fullmatch(attachment_id) or Path(filename).name != filename:
        raise HTTPException(status_code=404, detail="Attachment not found")
    for manifest_card in get_store().list_cards():
        manifest_item = next(
            (
                item
                for item in manifest_card.attachments
                if item.attachment_id == attachment_id and item.filename == filename
            ),
            None,
        )
        if not manifest_item:
            continue
        blobs = _ensure_attachment_available(request, manifest_item)
        inline = (
            manifest_item.media_type in SAFE_IMAGE_TYPES
            or manifest_item.media_type.startswith(("video/", "audio/"))
        )
        return FileResponse(
            blobs.blob_path(manifest_item.sha256),
            filename=manifest_item.filename,
            media_type=manifest_item.media_type,
            content_disposition_type="inline" if inline else "attachment",
            headers={
                "Content-Security-Policy": "sandbox; default-src 'none'; img-src 'self'; media-src 'self'",
                "X-Content-Type-Options": "nosniff",
                "Accept-Ranges": "bytes",
                "Cache-Control": "private, no-store",
            },
        )
    root = _attachment_root(request).resolve()
    path = (root / attachment_id / filename).resolve()
    if not path.is_relative_to(root) or not path.is_file():
        raise HTTPException(status_code=404, detail="Attachment not found")
    legacy_url = f"/card-attachments/{attachment_id}/{quote(filename, safe='')}"
    realm_id = _active_realm(request)
    store = get_store()
    for legacy_card in store.list_cards(realm_id=realm_id):
        if legacy_url not in legacy_card.body:
            continue
        with path.open("rb") as legacy_source:
            sha256, size = _attachment_store(request).ingest(legacy_source)
        migrated = CardAttachment(
            card_id=legacy_card.id,
            realm_id=realm_id,
            filename=safe_filename(filename),
            media_type=_attachment_media_type(path.name, None),
            size=size,
            sha256=sha256,
            blob_ref=f"sha256:{sha256}",
            created_by_principal=legacy_card.created_by_principal or "migration:legacy",
            created_by_instance=legacy_card.created_by_instance
            or request.app.state.ctx.settings.instance_id,
            visibility=legacy_card.visibility,
        )
        store.add_attachment(
            migrated,
            principal_id="migration:legacy",
            instance_id=request.app.state.ctx.settings.instance_id,
        )
        store.update_card(
            legacy_card.id,
            CardUpdate(
                body=legacy_card.body.replace(
                    legacy_url,
                    f"/api/cards/{legacy_card.id}/attachments/{migrated.attachment_id}",
                )
            ),
            realm_id=realm_id,
            principal_id="migration:legacy",
            instance_id=request.app.state.ctx.settings.instance_id,
        )
        break
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
    background_tasks: BackgroundTasks,
    kind: CardKind = Form(CardKind.TASK),
    title: str = Form(...),
    body: str = Form(""),
    lane: CardLane = Form(CardLane.INBOX),
    auto_enrich: bool = Form(True),
) -> RedirectResponse:
    realm = _active_realm(request)
    data = CardCreate(
        realm_id=realm,
        kind=kind,
        title=title,
        body=body,
        lane=lane,
        auto_enrich=auto_enrich,
    )
    card = get_store().create_card(
        data,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    if auto_enrich:
        background_tasks.add_task(
            enrich_card,
            request.app.state.ctx,
            card.id,
            card.realm_id,
            explicit_enrichment_fields(data),
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
    offset: int = 0,
) -> HTMLResponse:
    page_limit = min(100, max(10, limit))
    attention_filter = request.query_params.get("attention", "").strip()
    filtered_pagination = attention_filter in {
        "actionable",
        "motion",
        "outcome",
    }
    page_offset = max(0, offset) if filtered_pagination else 0
    context = _cards_context(
        request,
        lane=lane,
        result_limit=page_limit,
        result_offset=page_offset,
    )
    filtered_total = context["total_cards"] if filtered_pagination else 0
    filtered_visible = min(
        filtered_total, page_offset + len(context["cards"])
    )
    filtered_show_more_count = min(
        page_limit, max(0, filtered_total - filtered_visible)
    )
    filtered_show_more_query = ""
    if filtered_show_more_count:
        continuation_params = dict(request.query_params)
        if lane:
            continuation_params["lane"] = lane.value
        continuation_params["limit"] = str(page_limit)
        continuation_params["offset"] = str(filtered_visible)
        filtered_show_more_query = urlencode(continuation_params)

    done_total = 0
    done_visible = 0
    done_show_more_count = 0
    done_show_more_query = ""
    if lane == CardLane.DONE and not filtered_pagination:
        done_total = context["total_cards"]
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
            "filtered_pagination": filtered_pagination and filtered_total > 0,
            "filtered_total": filtered_total,
            "filtered_visible": filtered_visible,
            "filtered_show_more_count": filtered_show_more_count,
            "filtered_show_more_query": filtered_show_more_query,
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


@router.post("/cards/{card_id}/project-change")
def change_card_project_api(
    request: Request,
    card_id: str,
    body: CardProjectChangeRequest,
    realm: str | None = None,
) -> dict:
    realm_id = realm or _active_realm(request)
    store = get_store()
    card = store.get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    if body.project_id and not store.get_project(body.project_id, realm_id=realm_id):
        raise HTTPException(status_code=422, detail="Selected project was not found")
    impact = _card_project_impact(request, card, body.project_id)
    if body.project_id == card.project_id:
        return {
            "status": "unchanged",
            "card": card.model_dump(mode="json"),
            "impact": impact,
        }
    if body.decision == "cancel":
        return {
            "status": "cancelled",
            "card": card.model_dump(mode="json"),
            "impact": impact,
        }
    if impact["dependent"] and body.decision is None:
        return {"status": "review_required", "impact": impact}
    if body.decision == "migrate" and not impact["migration_compatible"]:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "The linked repositories are not all available in the destination project.",
                "impact": impact,
            },
        )
    settings = request.app.state.ctx.settings
    changed = store.assign_card_to_project(
        card_id,
        body.project_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    if not changed:
        raise HTTPException(status_code=404, detail="Card not found")
    return {
        "status": "changed",
        "card": changed.model_dump(mode="json"),
        "impact": impact,
    }


@ui_router.post("/partials/cards/{card_id}/project-change", response_class=HTMLResponse)
def change_card_project_ui(
    request: Request,
    card_id: str,
    project_id: str = Form(""),
    decision: str = Form(""),
    realm: str | None = None,
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    store = get_store()
    card = store.get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    selected_project_id = project_id.strip() or None
    if selected_project_id and not store.get_project(
        selected_project_id, realm_id=realm_id
    ):
        raise HTTPException(status_code=422, detail="Selected project was not found")
    impact = _card_project_impact(request, card, selected_project_id)
    if decision == "cancel":
        return _templates(request).TemplateResponse(
            request, "partials/card-detail.html", _card_summary_context(request, card)
        )
    if impact["dependent"] and not decision:
        return _templates(request).TemplateResponse(
            request,
            "partials/card-project-impact.html",
            {
                "card": card,
                "target_project": store.get_project(
                    selected_project_id, realm_id=realm_id
                )
                if selected_project_id
                else None,
                "selected_project_id": selected_project_id or "",
                "impact": impact,
                "csrf_token": token_for_request(request),
            },
        )
    if decision == "migrate" and not impact["migration_compatible"]:
        raise HTTPException(
            status_code=409,
            detail="Linked repositories are not available in the destination project",
        )
    settings = request.app.state.ctx.settings
    changed = store.assign_card_to_project(
        card_id,
        selected_project_id,
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=settings.instance_id,
    )
    if not changed:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request, "partials/card-detail.html", _card_summary_context(request, changed)
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


@ui_router.get("/partials/cards/{card_id}/dispatch", response_class=HTMLResponse)
def card_dispatch_partial(
    request: Request, card_id: str, realm: str | None = None
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request, "partials/card-dispatch.html", _card_agent_context(request, card)
    )


@ui_router.get("/partials/cards/{card_id}/progress", response_class=HTMLResponse)
def card_detail_progress_partial(
    request: Request, card_id: str, realm: str | None = None
) -> HTMLResponse:
    realm_id = realm or _active_realm(request)
    card = get_store().get_card(card_id, realm_id=realm_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")
    return _templates(request).TemplateResponse(
        request,
        "partials/card-progress.html",
        {
            "card": card,
            "current_progress": _latest_card_progress(request, card.id),
            "work_presentation": _work_presentation_for_card(request, card),
        },
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
    background_tasks: BackgroundTasks,
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
    if {"title", "body"} & changes.keys():
        card = _schedule_card_summary(request, background_tasks, card)
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
    start_seq: int | None = Form(None),
    end_seq: int | None = Form(None),
    supersedes_id: str = Form(""),
    review_at: datetime | None = Form(None),
    expires_at: datetime | None = Form(None),
    tags: str = Form(""),
) -> HTMLResponse:
    actor = _require_memory_editor(request)
    parsed_tags = [tag.strip() for tag in tags.split(",") if tag.strip()]
    if session_id:
        try:
            promote_from_transcript(
                get_store(),
                session_id=session_id,
                actor=actor,
                summary=summary,
                start_seq=start_seq,
                end_seq=end_seq,
                kind=kind,
                scope=scope.strip() or "realm",
                source_url=source_url.strip() or None,
                owner=owner.strip() or actor,
                confidence=float(confidence) if confidence.strip() else None,
                card_id=card_id or None,
                tags=parsed_tags,
                supersedes_id=supersedes_id or None,
                review_at=review_at,
                expires_at=expires_at,
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    else:
        entry = get_store().add_knowledge(
            KnowledgeEntry(
                summary=summary,
                kind=kind,
                scope=scope.strip() or "realm",
                source="manual",
                source_url=source_url.strip() or None,
                owner=owner.strip() or actor,
                confidence=float(confidence) if confidence.strip() else None,
                card_id=card_id or None,
                item_id=card_id or None,
                supersedes_id=supersedes_id or None,
                review_at=review_at,
                expires_at=expires_at,
                tags=parsed_tags,
            )
        )
        get_store().add_knowledge_audit(
            KnowledgeAuditEvent(
                knowledge_id=entry.id,
                action="created",
                actor=actor,
                payload={"source": "manual", "scope": entry.scope},
            )
        )
    return knowledge_partial(request)


@ui_router.post("/partials/knowledge/{entry_id}/status", response_class=HTMLResponse)
def update_knowledge_status_ui(
    request: Request,
    entry_id: str,
    status: KnowledgeStatus = Form(...),
) -> HTMLResponse:
    actor = _require_memory_editor(request)
    try:
        record_lifecycle_change(
            get_store(), entry_id=entry_id, status=status, actor=actor
        )
    except ValueError:
        raise HTTPException(status_code=404, detail="Memory not found")
    return knowledge_partial(request)


@ui_router.post(
    "/partials/knowledge/{entry_id}/regenerate", response_class=HTMLResponse
)
def regenerate_knowledge_ui(request: Request, entry_id: str) -> HTMLResponse:
    actor = _require_memory_editor(request)
    try:
        regenerate_knowledge(get_store(), entry_id=entry_id, actor=actor)
    except ValueError as exc:
        status = 404 if str(exc).startswith("Memory not found:") else 409
        raise HTTPException(status_code=status, detail=str(exc)) from exc
    return knowledge_partial(request)


@ui_router.post("/partials/knowledge/bulk", response_class=HTMLResponse)
def bulk_update_knowledge_ui(
    request: Request,
    ids: list[str] = Form(default=[]),
    action: str = Form(...),
) -> HTMLResponse:
    actor = _require_memory_editor(request)
    if action not in {"archive", "supersede"}:
        raise HTTPException(status_code=422, detail="Unsupported bulk action")
    status = (
        KnowledgeStatus.ARCHIVED if action == "archive" else KnowledgeStatus.SUPERSEDED
    )
    for entry_id in dict.fromkeys(ids):
        try:
            record_lifecycle_change(
                get_store(),
                entry_id=entry_id,
                status=status,
                actor=actor,
                action=f"bulk_{action}",
            )
        except ValueError:
            continue
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
        from pa.domain.card_summary_service import CardSummaryService

        ctx.register_service("card_summary_service", CardSummaryService(ctx))
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
                context_builder=_work_context,
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

    async def on_startup(self, app, ctx: AppContext) -> None:
        # Starting the sleeper is constant-time. Its first bounded migration /
        # retry scan happens after startup has completed.
        ctx.require_service("card_summary_service").start()

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        await ctx.require_service("card_summary_service").close()

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
            idempotency_key: str,
            kind: CardKind | None = None,
            body: str = "",
            lane: CardLane = CardLane.INBOX,
            realm: str = "default",
            parent_id: str | None = None,
            project_id: str | None = None,
            tags: list[str] | None = None,
            auto_enrich: bool = True,
        ) -> dict:
            """Create a canonical card. Use lane: inbox, active, waiting, or done."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/cards",
                json={
                    "realm_id": realm,
                    "title": title,
                    "body": body,
                    "lane": lane,
                    "parent_id": parent_id,
                    "project_id": project_id,
                    "tags": tags or [],
                    "auto_enrich": auto_enrich,
                    **({"kind": kind} if kind is not None else {}),
                },
                headers={"Idempotency-Key": key},
            )

        @mcp.tool()
        def update_card(
            card_id: str,
            idempotency_key: str,
            title: str | None = None,
            body: str | None = None,
            lane: CardLane | None = None,
            parent_id: str | None = None,
            project_id: str | None = None,
            realm: str = "default",
            tags: list[str] | None = None,
            expected_version: str | None = None,
            field_intent: list[str] | None = None,
        ) -> dict | None:
            """Update a canonical card. Omitted fields remain unchanged."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            changes = {
                key: value
                for key, value in {
                    "title": title,
                    "body": body,
                    "lane": lane,
                    "parent_id": parent_id,
                    "project_id": project_id,
                    "tags": tags,
                }.items()
                if value is not None
            }
            if expected_version is not None:
                changes["updated_at"] = expected_version
            if field_intent is not None:
                changes["field_intent"] = field_intent
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/cards/{card_id}",
                params={"realm": realm},
                json=changes,
                allow_not_found=True,
                headers={"Idempotency-Key": key},
            )

        @mcp.tool()
        def get_card_history(
            card_id: str,
            realm: str = "default",
            limit: int = 100,
            cursor: str | None = None,
        ) -> dict:
            """Inspect one stable cursor page of immutable card mutations."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/cards/{card_id}/history",
                params={
                    "realm": realm,
                    "limit": limit,
                    "cursor": cursor,
                },
            )

        @mcp.tool()
        def repair_legacy_card_history(
            card_ids: list[str], realm: str = "default"
        ) -> dict:
            """Idempotently append canonical bases for projection-only legacy cards."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/cards/repair-legacy-history",
                json={"card_ids": card_ids, "realm_id": realm},
            )

        @mcp.tool()
        def create_card_attachment(
            card_id: str,
            source_path: str,
            realm: str = "default",
            filename: str | None = None,
            media_type: str = "application/octet-stream",
        ) -> dict:
            """Attach a local file through PA's authenticated API; bytes become a durable fleet blob."""
            path = Path(source_path).expanduser().resolve()
            if not path.is_file():
                raise ValueError("source_path must be an existing regular file")
            with path.open("rb") as source:
                return request_local_pa(
                    ctx.settings,
                    "POST",
                    f"/api/cards/{card_id}/attachments",
                    params={"realm": realm},
                    files={
                        "file": (
                            safe_filename(filename or path.name),
                            source,
                            media_type,
                        )
                    },
                )

        @mcp.tool()
        def remove_card_attachment(
            card_id: str, attachment_id: str, realm: str = "default"
        ) -> dict:
            """Remove an attachment reference through a durable realm event."""
            return request_local_pa(
                ctx.settings,
                "DELETE",
                f"/api/cards/{card_id}/attachments/{attachment_id}",
                params={"realm": realm},
            )

        @mcp.tool()
        def update_card_preferred_instance(
            card_id: str,
            instance_id: str,
            idempotency_key: str,
            realm: str = "default",
        ) -> dict | None:
            """Set a card's preferred fleet instance and return its new authority version."""
            instance_id = instance_id.strip()
            key = idempotency_key.strip()
            if not instance_id:
                raise ValueError("instance_id cannot be empty")
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/cards/{card_id}",
                params={"realm": realm},
                json={"preferred_instance": instance_id},
                headers={"Idempotency-Key": key},
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

        @mcp.tool()
        def promote_session_memory(
            session_id: str,
            summary: str,
            kind: str = "memory",
            scope: str = "realm",
            start_seq: int | None = None,
            end_seq: int | None = None,
        ) -> dict:
            """Explicitly promote one curated conclusion with transcript provenance."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/knowledge/promote",
                json={
                    "session_id": session_id,
                    "summary": summary,
                    "kind": kind,
                    "scope": scope,
                    "start_seq": start_seq,
                    "end_seq": end_seq,
                },
            )

        @mcp.tool()
        def audit_knowledge_capture() -> dict:
            """Report likely corrupt or unintended Memory without mutating it."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/knowledge/audit",
            )

        @mcp.tool()
        def regenerate_memory(entry_id: str) -> dict:
            """Supersede Memory from its canonical source transcript."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/knowledge/{entry_id}/regenerate",
                json={},
            )

        @mcp.tool()
        def get_operation_outcome(
            idempotency_key: str, realm: str = "default"
        ) -> dict:
            """Look up the authoritative durable outcome of a mutation."""
            key = idempotency_key.strip()
            if not key:
                raise ValueError("idempotency_key cannot be empty")
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/operations/{quote(key, safe='')}",
                params={"realm": realm},
            )
