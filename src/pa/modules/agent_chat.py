"""REST + SSE APIs for multi-session agent chat."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.preferences import get_preferences_store
from pa.instance.agent_session import TRANSCRIPT_WINDOW_LIMIT
from pa.instance.quiesce import ImageAttachment, MAX_TOTAL_IMAGE_BYTES

router = APIRouter(prefix="/agent")
logger = logging.getLogger(__name__)


def _user_id(request: Request) -> str | None:
    principal = get_principal_id(request)
    if principal.startswith("user:"):
        return principal[5:]
    return None


def _manager(request: Request):
    return request.app.state.ctx.require_service("instance_agent")


def _session_pr_watches(request: Request, session) -> list[dict[str, Any]]:
    store = request.app.state.ctx.services.get("pr_supervisor_store")
    if not store:
        return []
    return [
        watch.model_dump(mode="json")
        for watch in store.list_watches(include_retired=True)
        if watch.originating_session_id == session.id
        or (watch.card_id and watch.card_id == session.card_id)
    ]


def _runtime_or_404(request: Request, session_id: str):
    mgr = _manager(request)
    runtime = mgr.get(session_id)
    if not runtime or runtime._closed:
        raise HTTPException(status_code=404, detail="Session not found")
    return runtime


class CreateSessionBody(BaseModel):
    label: str | None = None
    title: str | None = None
    cwd: str | None = None
    card_id: str | None = None
    project_id: str | None = None
    attach_default: bool = False
    provider: str | None = None
    surface: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    effort: str | None = None
    config: dict[str, str | bool] = Field(default_factory=dict)
    dispatch_id: str | None = None


def _config_option_id(runtime, requested: str) -> str:
    """Resolve friendly new-session fields to provider config option ids."""
    aliases = {
        "effort": {"effort", "reasoningeffort", "reasoninglevel", "thinkinglevel"},
    }
    wanted = aliases.get(
        requested, {requested.lower().replace("_", "").replace("-", "")}
    )
    connection = getattr(runtime, "connection", None)
    options = getattr(connection, "config_options", None) or []
    for option in options:
        if not isinstance(option, dict):
            continue
        option_id = (
            option.get("id") or option.get("configId") or option.get("config_id")
        )
        name = option.get("name")
        normalized = {
            str(value).lower().replace("_", "").replace("-", "").replace(" ", "")
            for value in (option_id, name)
            if value
        }
        if normalized & wanted and option_id:
            return str(option_id)
    return "reasoning_effort" if requested == "effort" else requested


async def _apply_initial_options(runtime, body: CreateSessionBody) -> None:
    if body.model_id:
        await runtime.set_model(body.model_id)
    if body.mode_id:
        await runtime.set_mode(body.mode_id)
    config = dict(body.config)
    if body.effort:
        config[_config_option_id(runtime, "effort")] = body.effort
    for config_id, value in config.items():
        await runtime.set_config(config_id, value)


class PromptBody(BaseModel):
    message: str = ""
    images: list[ImageAttachment] = Field(default_factory=list, max_length=4)
    action: Literal["append", "prepend", "interrupt"] = "append"
    card_id: str | None = None
    project_id: str | None = None

    @model_validator(mode="after")
    def validate_total_image_size(self) -> PromptBody:
        if sum(image.decoded_size for image in self.images) > MAX_TOTAL_IMAGE_BYTES:
            raise ValueError("images exceed 20 MB combined limit")
        return self


class PermissionBody(BaseModel):
    allow: bool = True
    option_id: str | None = None
    remember: bool = False
    scope: Literal["user", "global"] = "user"


class ModelBody(BaseModel):
    model_id: str


class ModeBody(BaseModel):
    mode_id: str


class ConfigBody(BaseModel):
    config_id: str
    value: str | bool


class ReorderBody(BaseModel):
    prompt_ids: list[str] = Field(default_factory=list)


class PreferencesBody(BaseModel):
    agent_auto_approve_permissions: bool | None = None
    agent_provider: str | None = None
    agent_surfaces: dict[str, Any] | None = None
    scope: Literal["user", "global"] = "user"


@router.post("/sessions")
async def create_session(request: Request, body: CreateSessionBody) -> dict:
    mgr = _manager(request)
    principal_id = get_principal_id(request)
    created_runtime = False
    from pa.acp.surfaces import surface_for_label

    surface = body.surface or surface_for_label(body.label, project_id=body.project_id)
    project_tool_config = None
    if body.project_id:
        project = mgr.store.get_project(body.project_id)
        if project and getattr(project, "tool_config", None):
            project_tool_config = dict(project.tool_config)
    try:
        if body.attach_default or body.label == "default":
            runtime = await mgr.attach_default(
                principal_id=principal_id,
                cwd=body.cwd,
                provider_override=body.provider,
            )
        elif body.label:
            # Reuse a live/persisted session with the same label (e.g. card:{id}).
            existing = None
            for rt in mgr.list_runtimes():
                if rt.session.label == body.label and not getattr(rt, "_closed", False):
                    existing = rt
                    break
            if existing is None:
                stored = mgr.store.get_session_by_label(body.label)
                if stored and stored.status not in {"closed", "quiesced"}:
                    runtime = await mgr.create_session(
                        label=body.label,
                        title=body.title or stored.title,
                        cwd=body.cwd or stored.cwd,
                        principal_id=principal_id or stored.principal_id,
                        card_id=body.card_id or stored.card_id,
                        project_id=body.project_id or stored.project_id,
                        existing=stored,
                        resume_external_id=stored.external_session_id,
                        surface=surface,
                        provider_override=body.provider,
                        project_tool_config=project_tool_config,
                    )
                    created_runtime = True
                else:
                    runtime = await mgr.create_session(
                        label=body.label,
                        title=body.title,
                        cwd=body.cwd,
                        principal_id=principal_id,
                        card_id=body.card_id,
                        project_id=body.project_id,
                        surface=surface,
                        provider_override=body.provider,
                        project_tool_config=project_tool_config,
                    )
                    created_runtime = True
            else:
                runtime = existing
        else:
            runtime = await mgr.create_session(
                label=body.label,
                title=body.title,
                cwd=body.cwd,
                principal_id=principal_id,
                card_id=body.card_id,
                project_id=body.project_id,
                surface=surface,
                provider_override=body.provider,
                project_tool_config=project_tool_config,
            )
            created_runtime = True
        try:
            await _apply_initial_options(runtime, body)
        except Exception:
            if created_runtime:
                try:
                    await runtime.close()
                except Exception:
                    logger.exception(
                        "Failed to close session %s after initial option failure",
                        runtime.session_id,
                    )
                finally:
                    mgr._runtimes.pop(runtime.session_id, None)
            raise
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if body.dispatch_id:
        dispatch_store = request.app.state.ctx.services.get("dispatch_store")
        record = dispatch_store.get(body.dispatch_id) if dispatch_store else None
        if not record:
            if created_runtime:
                await runtime.close()
                mgr._runtimes.pop(runtime.session_id, None)
            raise HTTPException(
                status_code=409,
                detail={"code": "dispatch_not_materialized", "recoverable": True},
            )
        record.session_id = runtime.session_id
        record.state = "dispatched"
        dispatch_store.put(record)
    return runtime.snapshot()


@router.get("/sessions")
def list_agent_sessions(request: Request) -> list[dict]:
    mgr = _manager(request)
    return [
        {
            "id": rt.session.id,
            "title": rt.session.title,
            "label": rt.session.label,
            "agent_name": rt.session.agent_name,
            "status": rt.session.status,
            "connected": rt.connected,
            "prompting": rt.prompting,
            "model_id": rt.session.model_id,
            "mode_id": rt.session.mode_id,
            "config_json": rt.session.config_json,
            "queue_length": len(rt._queue),
            "last_seq": rt._seq,
            "updated_at": rt.session.updated_at.isoformat(),
        }
        for rt in mgr.list_runtimes()
        if not rt._closed
    ]


@router.get("/history")
def list_agent_session_history(
    request: Request,
    card_id: str | None = None,
    project_id: str | None = None,
    limit: int = 100,
) -> list[dict]:
    """List persisted sessions, including sessions that are no longer live."""
    mgr = _manager(request)
    settings = request.app.state.ctx.settings
    sessions = mgr.store.list_sessions()
    if card_id:
        sessions = [session for session in sessions if session.card_id == card_id]
    if project_id:
        sessions = [session for session in sessions if session.project_id == project_id]
    return [
        {
            **session.model_dump(mode="json"),
            "instance_id": settings.instance_id,
            "instance_name": settings.instance_name,
            "pr_watches": _session_pr_watches(request, session),
            "live": bool(
                (runtime := mgr.get(session.id))
                and not getattr(runtime, "_closed", False)
            ),
        }
        for session in sessions[: max(1, min(limit, 500))]
    ]


@router.get("/history/{session_id}")
def get_agent_session_history(
    request: Request,
    session_id: str,
    after_seq: int | None = None,
    before_seq: int | None = None,
    limit: int = TRANSCRIPT_WINDOW_LIMIT,
) -> dict:
    """Return durable metadata and transcript events for a live or closed session."""
    if after_seq is not None and before_seq is not None:
        raise HTTPException(
            status_code=400,
            detail="Use either after_seq or before_seq, not both",
        )
    mgr = _manager(request)
    session = mgr.store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    runtime = mgr.get(session_id)
    if runtime and not getattr(runtime, "_closed", False):
        runtime._flush_transcript()
    page_limit = max(1, min(limit, 5000))
    if after_seq is not None:
        events = mgr.store.list_transcript_events(
            session_id,
            after_seq=max(0, after_seq),
            limit=page_limit + 1,
        )
        has_more = len(events) > page_limit
        events = events[:page_limit]
        page = {
            "oldest_seq": events[0].seq if events else None,
            "newest_seq": events[-1].seq if events else None,
            "has_older": False,
            "has_newer": has_more,
            "limit": page_limit,
        }
    else:
        cursor = max(1, before_seq) if before_seq is not None else None
        events = mgr.store.list_transcript_events_before(
            session_id,
            before_seq=cursor,
            limit=page_limit + 1,
        )
        has_older = len(events) > page_limit
        events = events[-page_limit:]
        page = {
            "oldest_seq": events[0].seq if events else None,
            "newest_seq": events[-1].seq if events else None,
            "has_older": has_older,
            "has_newer": before_seq is not None,
            "limit": page_limit,
        }
    settings = request.app.state.ctx.settings
    return {
        "session": session.model_dump(mode="json"),
        "instance": {
            "id": settings.instance_id,
            "name": settings.instance_name,
        },
        "live": bool(runtime and not getattr(runtime, "_closed", False)),
        "pr_watches": _session_pr_watches(request, session),
        "events": [event.model_dump(mode="json") for event in events],
        "page": page,
    }


@router.get("/sessions/{session_id}")
def get_session_snapshot(request: Request, session_id: str) -> dict:
    return _runtime_or_404(request, session_id).snapshot()


@router.get("/sessions/{session_id}/events")
async def session_events(request: Request, session_id: str) -> StreamingResponse:
    runtime = _runtime_or_404(request, session_id)
    last_event_id = request.headers.get("Last-Event-ID")
    after_seq = 0
    if last_event_id:
        try:
            after_seq = int(last_event_id)
        except ValueError:
            after_seq = 0
    query_after = request.query_params.get("after")
    if query_after:
        try:
            after_seq = max(after_seq, int(query_after))
        except ValueError:
            pass

    async def event_stream():
        # Local cursor — do not reassign outer after_seq (UnboundLocalError).
        cursor = after_seq
        # Subscribe first so events created while durable catch-up is paging are
        # queued. Durable replay remains authoritative; queued overlap is skipped.
        queue = runtime.subscribe()
        try:
            runtime._flush_transcript()
            while True:
                page = runtime.store.list_transcript_events(
                    session_id,
                    after_seq=cursor,
                    limit=TRANSCRIPT_WINDOW_LIMIT,
                )
                if not page:
                    break
                for te in page:
                    if te.seq <= cursor:
                        continue
                    payload = {
                        "id": te.id,
                        "seq": te.seq,
                        "type": te.event_type,
                        "session_id": te.session_id,
                        "payload": te.payload,
                        "created_at": te.created_at.isoformat(),
                    }
                    yield _sse(te.seq, payload)
                    cursor = te.seq
                if len(page) < TRANSCRIPT_WINDOW_LIMIT:
                    break

            while True:
                if await request.is_disconnected():
                    break
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                seq = int(event.get("seq") or 0)
                if seq and seq <= cursor:
                    continue
                if seq and seq > cursor + 1:
                    # A busy catch-up can overflow the bounded subscriber queue.
                    # Flush and fill any sequence gap from durable storage before
                    # emitting the retained live event.
                    runtime._flush_transcript()
                    while cursor < seq - 1:
                        gap_page = runtime.store.list_transcript_events(
                            session_id,
                            after_seq=cursor,
                            limit=TRANSCRIPT_WINDOW_LIMIT,
                        )
                        if not gap_page:
                            break
                        previous_cursor = cursor
                        for te in gap_page:
                            if te.seq <= cursor:
                                continue
                            payload = {
                                "id": te.id,
                                "seq": te.seq,
                                "type": te.event_type,
                                "session_id": te.session_id,
                                "payload": te.payload,
                                "created_at": te.created_at.isoformat(),
                            }
                            yield _sse(te.seq, payload)
                            cursor = te.seq
                        if cursor == previous_cursor:
                            break
                    if seq <= cursor:
                        continue
                cursor = max(cursor, seq)
                yield _sse(seq or None, event)
        finally:
            runtime.unsubscribe(queue)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _sse(event_id: int | None, data: dict[str, Any]) -> str:
    lines = []
    if event_id is not None:
        lines.append(f"id: {event_id}")
    lines.append(f"event: {data.get('type') or 'message'}")
    lines.append(f"data: {json.dumps(data, default=str)}")
    return "\n".join(lines) + "\n\n"


@router.post("/sessions/{session_id}/prompt")
async def session_prompt(request: Request, session_id: str, body: PromptBody) -> dict:
    message = body.message.strip()
    if not message and not body.images:
        raise HTTPException(status_code=400, detail="message or image required")
    runtime = _runtime_or_404(request, session_id)
    principal_id = get_principal_id(request)
    # Return immediately; transcript/SSE streams the turn. Blocking here made the
    # old HTMX UI look like it only ever received "Turn completed".
    stop_reason = await runtime.prompt(
        message,
        images=body.images,
        item_id=body.card_id,
        principal_id=principal_id,
        project_id=body.project_id,
        action=body.action,
        wait=False,
    )
    return {
        "stop_reason": stop_reason,
        "queued": stop_reason == "queued",
        "started": stop_reason == "started",
        "session_id": session_id,
        "queue": [q.public_dict() for q in runtime._queue],
    }


@router.post("/sessions/{session_id}/cancel")
async def session_cancel(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    await runtime.cancel(pause_queue=True)
    return {"ok": True, "queue_paused": True}


@router.post("/sessions/{session_id}/close")
async def session_close(request: Request, session_id: str) -> dict:
    mgr = _manager(request)
    runtime = _runtime_or_404(request, session_id)
    await runtime.close()
    mgr._runtimes.pop(session_id, None)
    return {"ok": True}


@router.post("/sessions/{session_id}/permissions/{request_id}")
async def session_permission(
    request: Request,
    session_id: str,
    request_id: str,
    body: PermissionBody,
) -> dict:
    runtime = _runtime_or_404(request, session_id)
    ok = await runtime.respond_permission(
        request_id,
        allow=body.allow,
        option_id=body.option_id,
        remember=body.remember,
        scope=body.scope,
        principal_id=get_principal_id(request),
    )
    if not ok:
        raise HTTPException(status_code=404, detail="Permission request not found")
    return {"ok": True}


@router.put("/sessions/{session_id}/model")
async def session_model(request: Request, session_id: str, body: ModelBody) -> dict:
    runtime = _runtime_or_404(request, session_id)
    try:
        await runtime.set_model(body.model_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"model_id": runtime.session.model_id}


@router.put("/sessions/{session_id}/mode")
async def session_mode(request: Request, session_id: str, body: ModeBody) -> dict:
    runtime = _runtime_or_404(request, session_id)
    try:
        await runtime.set_mode(body.mode_id)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"mode_id": runtime.session.mode_id}


@router.put("/sessions/{session_id}/config")
async def session_config(request: Request, session_id: str, body: ConfigBody) -> dict:
    runtime = _runtime_or_404(request, session_id)
    try:
        await runtime.set_config(body.config_id, body.value)
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"config_id": body.config_id, "value": body.value}


@router.post("/sessions/{session_id}/queue/pause")
async def queue_pause(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    runtime.pause_queue()
    return {"queue_paused": True}


@router.post("/sessions/{session_id}/queue/resume")
async def queue_resume(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    runtime.resume_queue()
    return {"queue_paused": False}


@router.post("/sessions/{session_id}/queue/reorder")
async def queue_reorder(request: Request, session_id: str, body: ReorderBody) -> dict:
    runtime = _runtime_or_404(request, session_id)
    queue = runtime.reorder_queue(body.prompt_ids)
    return {"queue": [q.public_dict() for q in queue]}


@router.delete("/sessions/{session_id}/queue/{prompt_id}")
async def queue_remove(request: Request, session_id: str, prompt_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    removed = runtime.remove_queued(prompt_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Queued prompt not found")
    return {"ok": True}


@router.get("/preferences")
def get_agent_preferences(request: Request) -> dict:
    settings = request.app.state.ctx.settings
    user_id = _user_id(request)
    global_prefs = get_preferences_store(settings.data_dir).load()
    user_prefs = (
        get_preferences_store(settings.data_dir, user_id=user_id).load()
        if user_id
        else None
    )
    effective = False
    if user_id and user_prefs is not None:
        user_store = get_preferences_store(settings.data_dir, user_id=user_id)
        if user_store.path.exists():
            effective = bool(user_prefs.agent_auto_approve_permissions)
        else:
            effective = bool(global_prefs.agent_auto_approve_permissions)
    else:
        effective = bool(global_prefs.agent_auto_approve_permissions)

    def _provider_blob(prefs) -> dict:
        return {
            "agent_auto_approve_permissions": prefs.agent_auto_approve_permissions,
            "agent_provider": prefs.agent_provider,
            "agent_surfaces": {
                k: v.model_dump() if hasattr(v, "model_dump") else v
                for k, v in (prefs.agent_surfaces or {}).items()
            },
        }

    effective_provider = settings.agent_provider
    if global_prefs.agent_provider:
        effective_provider = global_prefs.agent_provider
    if user_id and user_prefs and user_prefs.agent_provider:
        effective_provider = user_prefs.agent_provider

    return {
        "agent_auto_approve_permissions": effective,
        "agent_provider": effective_provider,
        "instance_provider": settings.agent_provider,
        "user": _provider_blob(user_prefs) if user_prefs else None,
        "global": _provider_blob(global_prefs),
    }


@router.put("/preferences")
def put_agent_preferences(request: Request, body: PreferencesBody) -> dict:
    from pa.core.preferences import SurfaceAgentPrefs

    settings = request.app.state.ctx.settings
    updates: dict[str, Any] = {}
    if body.agent_auto_approve_permissions is not None:
        updates["agent_auto_approve_permissions"] = body.agent_auto_approve_permissions
    if "agent_provider" in body.model_fields_set:
        updates["agent_provider"] = body.agent_provider
    if body.agent_surfaces is not None:
        surfaces = {}
        for key, raw in body.agent_surfaces.items():
            if isinstance(raw, SurfaceAgentPrefs):
                surfaces[key] = raw
            elif isinstance(raw, dict):
                surfaces[key] = SurfaceAgentPrefs.model_validate(raw)
            else:
                surfaces[key] = SurfaceAgentPrefs(provider=str(raw) if raw else None)
        updates["agent_surfaces"] = surfaces
    if not updates:
        return get_agent_preferences(request)
    if body.scope == "global":
        get_preferences_store(settings.data_dir).update(**updates)
    else:
        user_id = _user_id(request)
        get_preferences_store(settings.data_dir, user_id=user_id).update(**updates)
    return get_agent_preferences(request)


class AgentChatModule(Module):
    @property
    def name(self) -> str:
        return "agent_chat"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Multi-session agent chat REST and SSE APIs"

    def api_routers(self):
        return [("/api", router, ["agent"])]
