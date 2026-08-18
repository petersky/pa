"""REST + SSE APIs for multi-session agent chat."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field, model_validator

from pa.acp.configuration import ACPConfigurationError, SessionConfigurationRequest
from pa.acp.sandbox_health import sandbox_health_registry
from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.preferences import get_preferences_store
from pa.core.ui.instance_identity import current_instance_name
from pa.domain.models import AgentSession, TranscriptEvent
from pa.execution.dispatch import GoalDispatchProvenance
from pa.execution.observability import (
    SESSION_OBSERVABILITY_CAPABILITY,
    SESSION_OBSERVABILITY_VERSION,
    build_session_observability,
    diagnostic_timeline,
)
from pa.instance.agent_session import (
    RECOVERY_BLOCKED_STATUS,
    TRANSCRIPT_WINDOW_LIMIT,
    AgentSessionManager,
    AgentSessionRecoveryError,
    AgentSessionRuntime,
    AgentStartupNotReady,
)
from pa.instance.quiesce import MAX_TOTAL_IMAGE_BYTES, ImageAttachment
from pa.intake.models import IntakeMutationContext
from pa.modules.agent_lifecycle import require_startup_ready

router = APIRouter(prefix="/agent")
logger = logging.getLogger(__name__)


def _user_id(request: Request) -> str | None:
    principal = get_principal_id(request)
    if principal.startswith("user:"):
        return principal[5:]
    return None


def _manager(request: Request):
    return request.app.state.ctx.require_service("instance_agent")


async def _offload(
    manager,
    operation: str,
    call,
    /,
    *args,
    timeout: float | None = None,
    **kwargs,
):
    if isinstance(manager, AgentSessionManager):
        return await manager._offload(operation, call, *args, timeout=timeout, **kwargs)
    return await asyncio.to_thread(call, *args, **kwargs)


async def _runtime_offload(
    runtime,
    operation: str,
    call,
    /,
    *args,
    timeout: float | None = None,
    **kwargs,
):
    if isinstance(runtime, AgentSessionRuntime):
        return await runtime._offload(operation, call, *args, timeout=timeout, **kwargs)
    return await asyncio.to_thread(call, *args, **kwargs)


async def _drain_runtime_transcripts(runtime) -> None:
    if isinstance(runtime, AgentSessionRuntime):
        await runtime._drain_transcripts()


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


def _session_project_payload(request: Request, session: AgentSession) -> dict | None:
    if not session.project_id:
        return None
    project = request.app.state.ctx.store.get_project(
        session.project_id,
        realm_id=session.realm_id,
    )
    if not project:
        return None
    return {"id": project.id, "title": project.title}


def _session_reconciliation(request: Request, session_id: str) -> dict[str, Any]:
    store = request.app.state.ctx.services.get("dispatch_store")
    record = store.by_session(session_id) if store else None
    if not record:
        return {
            "state": "not_requested",
            "reason": None,
            "recoverable": False,
        }
    return record.public_dict()["card_reconciliation"]


def _observability(
    request: Request,
    session: AgentSession,
    *,
    events: list[TranscriptEvent] | None = None,
) -> dict[str, Any]:
    mgr = _manager(request)
    runtime = mgr.get(session.id)
    if runtime and getattr(runtime, "_closed", False):
        runtime = None
    if events is None:
        events = mgr.store.list_transcript_events_before(session.id, limit=5000)
    settings = request.app.state.ctx.settings
    result = build_session_observability(
        session,
        runtime=runtime,
        events=events,
        instance_id=settings.instance_id,
        instance_name=settings.instance_name,
        reconciliation=_session_reconciliation(request, session.id),
    )
    snapshot = session.origin_instance_name
    current = current_instance_name(
        request.app.state.ctx,
        session.origin_instance_id or settings.instance_id,
        snapshot or settings.instance_name,
    )
    result["instance"]["name"] = current
    result["instance"]["name_snapshot"] = snapshot
    result["instance"]["name_at_session_start"] = (
        snapshot if snapshot and snapshot != current else None
    )
    return result


def _require_session_traffic_ready(request: Request):
    manager = _manager(request)
    require_startup_ready(manager)
    return manager


def _session_actions(session_id: str, *, recoverable: bool) -> dict[str, Any]:
    return {
        "history_url": f"/api/agent/history/{session_id}",
        "recover_url": (
            f"/api/agent/sessions/{session_id}/recover" if recoverable else None
        ),
    }


def _durable_session_state(manager, session) -> dict[str, Any]:
    runtime = manager.get(session.id)
    live = bool(runtime and not getattr(runtime, "_closed", False))
    recoverable = (
        session.status
        not in {
            "closed",
            RECOVERY_BLOCKED_STATUS,
        }
        and not live
    )
    durable = dict((session.config_json or {}).get("durable_runtime") or {})
    return {
        "exists": True,
        "live": live,
        "recoverable": recoverable,
        "reason": (
            "live"
            if live
            else "session_closed"
            if session.status == "closed"
            else "recovery_blocked"
            if session.status == RECOVERY_BLOCKED_STATUS
            else "provider_thread_lost"
        ),
        "status": session.status,
        "provider": session.agent_name,
        "recovery_error": durable.get("recovery_error"),
        "actions": _session_actions(session.id, recoverable=recoverable),
    }


def _runtime_or_404(request: Request, session_id: str):
    mgr = _require_session_traffic_ready(request)
    runtime = mgr.get(session_id)
    if not runtime or runtime._closed:
        session = mgr.store.get_session(session_id)
        if session:
            state = _durable_session_state(mgr, session)
            detail = {
                "code": "session_not_live",
                "message": (
                    "This PA session is closed. Durable history remains available."
                    if state["reason"] == "session_closed"
                    else "The PA session exists, but its provider thread is not live."
                ),
                "recoverable": state["recoverable"],
                "durable_session": state,
                **state["actions"],
            }
        else:
            detail = {
                "code": "session_deleted",
                "message": "This PA session no longer exists.",
                "recoverable": False,
                "durable_session": {"exists": False, "reason": "pa_session_deleted"},
                **_session_actions(session_id, recoverable=False),
            }
        logger.info(
            "Agent session request rejected because runtime is not live",
            extra={
                "session_id": session_id,
                "runtime_present": runtime is not None,
                "runtime_closed": bool(runtime and runtime._closed),
            },
        )
        raise HTTPException(status_code=404, detail=detail)
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
    model_provider: str | None = None
    model_id: str | None = None
    mode_id: str | None = None
    effort: str | None = None
    config: dict[str, str | bool] = Field(default_factory=dict)
    dispatch_id: str | None = None
    resume: bool = False
    resume_session_id: str | None = None
    fresh: bool = False
    goal_provenance: GoalDispatchProvenance | None = None


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


_UNSET_EFFORT = {"", "default", "none", "auto"}


def _requested_effort(value: str | None) -> str | None:
    """Treat provider-default aliases as unset so PA does not force a no-op apply."""
    raw = str(value or "").strip()
    if not raw or raw.lower() in _UNSET_EFFORT:
        return None
    return raw


def _configuration_request(
    body: CreateSessionBody, defaults=None
) -> SessionConfigurationRequest:
    config = dict(defaults.config if defaults else {})
    config.update(body.config)
    return SessionConfigurationRequest.from_values(
        model_id=body.model_id or (defaults.model_id if defaults else None),
        mode_id=body.mode_id or (defaults.mode_id if defaults else None),
        reasoning=_requested_effort(body.effort)
        or _requested_effort(defaults.effort if defaults else None),
        model_provider=body.model_provider
        or (defaults.model_provider if defaults else None),
        config=config,
    )


async def _apply_initial_options(
    runtime, body: CreateSessionBody, defaults=None
) -> None:
    requested = _configuration_request(body, defaults)
    if requested.empty:
        return
    session = getattr(runtime, "session", None)
    diagnostic = dict(
        (getattr(session, "config_json", None) or {}).get("configuration") or {}
    )
    if (
        diagnostic.get("state") == "ready"
        and diagnostic.get("requested") == requested.as_dict()
    ):
        return
    configure = getattr(type(runtime), "configure", None)
    if callable(configure):
        await runtime.configure(requested)
        return

    # Compatibility for embedders/tests providing the older runtime surface.
    if requested.model_id:
        await runtime.set_model(requested.model_id)
    if requested.mode_id:
        await runtime.set_mode(requested.mode_id)
    legacy_config = dict(requested.config)
    if requested.reasoning:
        legacy_config[_config_option_id(runtime, "effort")] = requested.reasoning
    for config_id, value in legacy_config.items():
        await runtime.set_config(config_id, value)


class PromptBody(BaseModel):
    message: str = ""
    images: list[ImageAttachment] = Field(default_factory=list, max_length=4)
    action: Literal["append", "prepend", "interrupt"] = "append"
    card_id: str | None = None
    project_id: str | None = None
    dispatch_id: str | None = None
    client_prompt_id: str | None = Field(
        default=None,
        min_length=8,
        max_length=128,
        pattern=r"^[A-Za-z0-9._:-]+$",
    )
    idempotency_key: str | None = None
    goal_provenance: GoalDispatchProvenance | None = None

    @model_validator(mode="after")
    def validate_total_image_size(self) -> PromptBody:
        if sum(image.decoded_size for image in self.images) > MAX_TOTAL_IMAGE_BYTES:
            raise ValueError("images exceed 20 MB combined limit")
        if self.client_prompt_id and self.dispatch_id:
            raise ValueError("client_prompt_id and dispatch_id are mutually exclusive")
        return self


def _goal_followup_provenance_matches(
    base: GoalDispatchProvenance,
    candidate: GoalDispatchProvenance | None,
) -> bool:
    """Accept only a fresh action fenced to the same governed dispatch lineage."""

    return bool(
        candidate is not None
        and candidate.released_at is None
        and candidate.action_reservation_id != base.action_reservation_id
        and candidate.goal_id == base.goal_id
        and candidate.authority_instance_id == base.authority_instance_id
        and candidate.actor_principal == base.actor_principal
        and candidate.action_class == base.action_class
        and candidate.fencing_token == base.fencing_token
        and str(candidate.provider_id or "").strip().lower()
        == str(base.provider_id or "").strip().lower()
    )


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


class RecoverSessionBody(BaseModel):
    provider: str | None = None


class SessionCardLinkBody(BaseModel):
    make_primary: bool = True


class PreferencesBody(BaseModel):
    agent_auto_approve_permissions: bool | None = None
    agent_provider: str | None = None
    agent_surfaces: dict[str, Any] | None = None
    telemetry_session_header: Literal["hidden", "compact", "expanded"] | None = None
    scope: Literal["user", "global"] = "user"


@router.post("/sessions")
async def create_session(request: Request, body: CreateSessionBody) -> dict:
    mgr = _require_session_traffic_ready(request)
    principal_id = get_principal_id(request)
    created_runtime = False
    from pa.acp.startup_trace import SessionStartupTrace

    startup_trace = SessionStartupTrace()
    from pa.acp.providers.resolve import (
        resolve_provider_id,
        resolve_surface_preferences,
    )
    from pa.acp.surfaces import AgentInvocationContext, surface_for_label
    from pa.core.preferences import SurfaceAgentPrefs

    surface = body.surface or surface_for_label(body.label, project_id=body.project_id)
    settings = request.app.state.ctx.settings
    surface_defaults = SurfaceAgentPrefs()
    with startup_trace.phase("preference_resolution"):
        if isinstance(settings.data_dir, (str, Path)):
            surface_defaults = await _offload(
                mgr,
                "preferences.agent_surface_read",
                resolve_surface_preferences,
                settings,
                AgentInvocationContext(
                    surface=surface,
                    principal_id=principal_id,
                    card_id=body.card_id,
                    project_id=body.project_id,
                ),
                timeout=10.0,
            )
    project_tool_config = None
    new_logical_session = True
    dispatch_record = None
    dispatch_store = None
    if body.dispatch_id:
        dispatch_store = request.app.state.ctx.services.get("dispatch_store")
        dispatch_record = (
            await _offload(
                mgr, "dispatch.record_read", dispatch_store.get, body.dispatch_id
            )
            if dispatch_store
            else None
        )
        if not dispatch_record:
            raise HTTPException(
                status_code=409,
                detail={"code": "dispatch_not_materialized", "recoverable": True},
            )
        if body.resume != dispatch_record.resume_requested:
            raise HTTPException(
                status_code=409,
                detail={"code": "dispatch_resume_mismatch", "recoverable": False},
            )
        if body.resume and body.resume_session_id != dispatch_record.resume_session_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "resume_session_mismatch",
                    "expected": dispatch_record.resume_session_id,
                    "actual": body.resume_session_id,
                    "recoverable": False,
                },
            )
        mismatches = {
            field: {"expected": expected, "actual": actual}
            for field, expected, actual in (
                ("card_id", dispatch_record.card_id, body.card_id),
                ("project_id", dispatch_record.project_id, body.project_id),
                (
                    "target_instance_id",
                    dispatch_record.target_instance_id,
                    settings.instance_id,
                ),
            )
            if expected != actual
        }
        if mismatches:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "dispatch_provenance_mismatch",
                    "message": "Session provenance does not match the materialized dispatch.",
                    "mismatches": mismatches,
                    "recoverable": False,
                },
            )
        if dispatch_record.goal_provenance != body.goal_provenance:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_dispatch_provenance_mismatch",
                    "recoverable": False,
                },
            )
        if body.goal_provenance is not None and (
            str(body.provider or "").strip().lower()
            != str(body.goal_provenance.provider_id or "").strip().lower()
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_dispatch_provider_mismatch",
                    "recoverable": False,
                },
            )
        if not body.resume and not dispatch_record.session_id:

            def reserve_dispatch_session() -> None:
                dispatch_record.session_id = str(uuid4())
                dispatch_store.transition(
                    dispatch_record,
                    "starting_session",
                    "Reserved stable session identity before provider startup.",
                    detail={"session_id": dispatch_record.session_id},
                )

            await _offload(mgr, "dispatch.session_reserve", reserve_dispatch_session)
    if body.project_id:
        project = await _offload(
            mgr, "sqlite.project_read", mgr.store.get_project, body.project_id
        )
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        if project and getattr(project, "tool_config", None):
            project_tool_config = dict(project.tool_config)
    try:
        explicit_configuration = _configuration_request(body)
    except ACPConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    new_session_defaults = None
    if isinstance(settings.data_dir, (str, Path)):

        def resolve_requested_providers():
            inherited, _ = resolve_provider_id(
                settings,
                AgentInvocationContext(
                    surface=surface,
                    principal_id=principal_id,
                    card_id=body.card_id,
                    project_id=body.project_id,
                ),
                project_tool_config=project_tool_config,
            )
            requested, _ = resolve_provider_id(
                settings,
                AgentInvocationContext(
                    surface=surface,
                    principal_id=principal_id,
                    card_id=body.card_id,
                    project_id=body.project_id,
                    provider_override=body.provider,
                ),
                project_tool_config=project_tool_config,
            )
            return inherited, requested

        inherited_provider, requested_provider = await _offload(
            mgr, "agent.provider_resolve", resolve_requested_providers, timeout=30.0
        )
        defaults_provider = surface_defaults.provider or inherited_provider
        if defaults_provider.strip().lower() == requested_provider.strip().lower():
            new_session_defaults = surface_defaults
    try:
        new_session_configuration = _configuration_request(body, new_session_defaults)
    except ACPConfigurationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    dispatch_session_kwargs: dict[str, Any] = {}
    if dispatch_record:
        dispatch_session_kwargs = {
            "principal_id": dispatch_record.principal_id,
            "authority_instance_id": dispatch_record.authority_instance_id,
            "dispatch_id": dispatch_record.dispatch_id,
            "realm_id": dispatch_record.realm_id,
            "execution_context_seed": {
                "authority_instance": {
                    "id": dispatch_record.authority_instance_id,
                    "name": dispatch_record.authority_instance_name
                    or dispatch_record.authority_instance_id,
                },
                "dispatch_id": dispatch_record.dispatch_id,
                "attachments": dispatch_record.attachment_evidence
                or {"verified": True, "attachments": []},
                "materialization_plan": dispatch_record.materialization_plan,
            },
        }
    try:
        if dispatch_record:
            linked_session_id = dispatch_record.session_id
            linked_runtime = mgr.get(linked_session_id) if linked_session_id else None
            if linked_runtime and not getattr(linked_runtime, "_closed", False):
                new_logical_session = False
                runtime = linked_runtime
            elif linked_session_id:
                stored = await _offload(
                    mgr,
                    "sqlite.agent_session_read",
                    mgr.store.get_session,
                    linked_session_id,
                )
                if not stored and not body.resume:
                    runtime = await mgr.create_session(
                        session_id=linked_session_id,
                        label=body.label,
                        title=body.title,
                        cwd=body.cwd,
                        **dispatch_session_kwargs,
                        card_id=body.card_id,
                        project_id=body.project_id,
                        surface=surface,
                        provider_override=body.provider,
                        project_tool_config=project_tool_config,
                        initial_configuration=new_session_configuration,
                        startup_trace=startup_trace,
                    )
                    created_runtime = True
                elif not stored or stored.status in {"closed", "quiesced"}:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "linked_session_unavailable",
                            "session_id": linked_session_id,
                            "recoverable": False,
                        },
                    )
                else:
                    new_logical_session = False
                    runtime = await mgr.create_session(
                        label=stored.label,
                        title=body.title or stored.title,
                        cwd=body.cwd or stored.cwd,
                        **dispatch_session_kwargs,
                        card_id=body.card_id or stored.card_id,
                        project_id=body.project_id or stored.project_id,
                        existing=stored,
                        resume_external_id=stored.external_session_id,
                        surface=surface,
                        provider_override=body.provider,
                        project_tool_config=project_tool_config,
                        initial_configuration=(
                            explicit_configuration
                            if not explicit_configuration.empty
                            else None
                        ),
                        startup_trace=startup_trace,
                    )
                    created_runtime = True
            elif body.resume:
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "resume_session_missing",
                        "session_id": body.resume_session_id,
                        "recoverable": False,
                    },
                )
            else:
                # A materialized fresh dispatch never consults label lookup.
                runtime = await mgr.create_session(
                    label=body.label,
                    title=body.title,
                    cwd=body.cwd,
                    **dispatch_session_kwargs,
                    card_id=body.card_id,
                    project_id=body.project_id,
                    surface=surface,
                    provider_override=body.provider,
                    project_tool_config=project_tool_config,
                    initial_configuration=new_session_configuration,
                    startup_trace=startup_trace,
                )
                created_runtime = True
        elif body.attach_default or body.label == "default":
            new_logical_session = (
                await _offload(
                    mgr,
                    "sqlite.agent_session_read",
                    mgr.store.get_session_by_label,
                    "default",
                )
                is None
            )
            runtime = await mgr.attach_default(
                principal_id=principal_id,
                cwd=body.cwd,
                provider_override=body.provider,
                initial_configuration=(
                    new_session_configuration
                    if new_logical_session
                    else explicit_configuration
                    if not explicit_configuration.empty
                    else None
                ),
                startup_trace=startup_trace,
            )
        elif body.label and not body.fresh:
            # Reuse a live/persisted session with the same label (e.g. card:{id}).
            async with mgr.label_lock(body.label):
                existing = None
                for rt in mgr.list_runtimes():
                    if rt.session.label == body.label and not getattr(
                        rt, "_closed", False
                    ):
                        existing = rt
                        break
                if existing is None:
                    stored = await _offload(
                        mgr,
                        "sqlite.agent_session_read",
                        mgr.store.get_session_by_label,
                        body.label,
                    )
                    if stored and stored.status not in {"closed", "quiesced"}:
                        new_logical_session = False
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
                            initial_configuration=(
                                explicit_configuration
                                if not explicit_configuration.empty
                                else None
                            ),
                            startup_trace=startup_trace,
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
                            initial_configuration=new_session_configuration,
                            startup_trace=startup_trace,
                        )
                        created_runtime = True
                else:
                    new_logical_session = False
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
                initial_configuration=new_session_configuration,
                startup_trace=startup_trace,
            )
            created_runtime = True
        created_runtime = created_runtime or startup_trace.attached
        actual_provider = str(
            getattr(getattr(runtime, "session", None), "agent_name", "") or ""
        )
        actual_provider = actual_provider.strip().lower()
        defaults_provider = str(surface_defaults.provider or "").strip().lower()
        if (
            new_logical_session
            and not defaults_provider
            and isinstance(settings.data_dir, (str, Path))
        ):
            # Resolve what "inherit" means without the request override. Saved
            # option ids are safe only when that provider is the one we started.
            defaults_provider, _ = await _offload(
                mgr,
                "agent.provider_resolve",
                resolve_provider_id,
                settings,
                AgentInvocationContext(
                    surface=surface,
                    principal_id=principal_id,
                    card_id=body.card_id,
                    project_id=body.project_id,
                ),
                project_tool_config=project_tool_config,
                timeout=30.0,
            )
            defaults_provider = defaults_provider.strip().lower()
        initial_defaults = (
            surface_defaults
            if new_logical_session
            and defaults_provider
            and defaults_provider == actual_provider
            else None
        )
        try:
            await _apply_initial_options(runtime, body, initial_defaults)
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
    except HTTPException:
        raise
    except Exception as exc:
        from pa.acp.errors import classify_acp_failure

        provider_id = str(body.provider or settings.agent_provider).strip().lower()
        classified = classify_acp_failure(
            exc, provider_id=provider_id, stage="session_admission"
        )
        health = sandbox_health_registry.failure(
            provider_id,
            "workspace-write",
            exc,
            metadata={
                "stage": "session_admission",
                "session_level": True,
                "dispatch_id": dispatch_record.dispatch_id if dispatch_record else None,
                "failure": classified,
            },
        )
        if dispatch_record and dispatch_store:
            await _offload(
                mgr,
                "dispatch.sandbox_admission_failure",
                dispatch_store.fail,
                dispatch_record,
                classified.get("message")
                or "Provider sandbox/session admission failed before prompt delivery.",
                code=classified.get("code") or health["classification"],
                recoverable=bool(classified.get("recoverable", True)),
                detail={"sandbox_health": health, "failure": classified},
            )
        raise HTTPException(status_code=503, detail=classified) from exc
    if dispatch_record:
        if (
            dispatch_record.session_id
            and dispatch_record.session_id != runtime.session_id
        ):
            if created_runtime:
                await runtime.close()
                mgr._runtimes.pop(runtime.session_id, None)
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "dispatch_session_mismatch",
                    "expected": dispatch_record.session_id,
                    "actual": runtime.session_id,
                    "recoverable": False,
                },
            )
        dispatch_record.session_id = runtime.session_id
        config = dict(runtime.session.config_json or {})
        execution = dict(config.get("execution_context") or {})
        execution["authority_instance"] = {
            "id": dispatch_record.authority_instance_id,
            "name": dispatch_record.authority_instance_name
            or dispatch_record.authority_instance_id,
        }
        execution["dispatch_id"] = dispatch_record.dispatch_id
        execution["attachments"] = dispatch_record.attachment_evidence or {
            "verified": True,
            "attachments": [],
        }
        config["execution_context"] = execution
        runtime.session.config_json = config

        def persist_dispatch_link() -> None:
            mgr.store.save_session(runtime.session)
            dispatch_store.transition(
                dispatch_record,
                "starting_session",
                "Remote session linked to dispatch.",
                detail={
                    "session_id": runtime.session_id,
                    "configuration": dict(
                        (runtime.session.config_json or {}).get("configuration") or {}
                    ),
                },
            )

        await _offload(mgr, "dispatch.session_link", persist_dispatch_link)
    if startup_trace.attached:
        with startup_trace.phase("response_readiness"):
            await _offload(mgr, "agent.session_snapshot", runtime.snapshot)
        await _offload(
            mgr,
            "sqlite.agent_session_save",
            mgr.store.save_session,
            runtime.session,
        )
    return await _offload(mgr, "agent.session_snapshot", runtime.snapshot)


@router.get("/observability/v1/capabilities")
def session_observability_capabilities() -> dict[str, Any]:
    return {
        "schema_version": SESSION_OBSERVABILITY_VERSION,
        "capabilities": [SESSION_OBSERVABILITY_CAPABILITY],
        "legacy_unknown_fields": True,
    }


@router.get("/observability/v1/sessions")
def list_session_observability(request: Request, limit: int = 100) -> dict[str, Any]:
    mgr = _manager(request)
    sessions = sorted(
        mgr.store.list_sessions(), key=lambda item: item.updated_at, reverse=True
    )
    bounded = max(1, min(limit, 500))
    return {
        "schema_version": SESSION_OBSERVABILITY_VERSION,
        "sessions": [
            _observability(request, session) for session in sessions[:bounded]
        ],
    }


@router.get("/observability/v1/sessions/{session_id}")
def get_session_observability(request: Request, session_id: str) -> dict[str, Any]:
    session = _manager(request).store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return _observability(request, session)


@router.get("/observability/v1/sessions/{session_id}/turns")
def list_session_turns(request: Request, session_id: str) -> dict[str, Any]:
    projection = get_session_observability(request, session_id)
    return {
        "schema_version": SESSION_OBSERVABILITY_VERSION,
        "session_id": session_id,
        "turns": projection["turns"],
    }


@router.post("/observability/v1/sessions/{session_id}/diagnostics")
def request_session_diagnostics(
    request: Request, session_id: str, limit: int = 50
) -> dict[str, Any]:
    mgr = _manager(request)
    session = mgr.store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    events = mgr.store.list_transcript_events_before(session_id, limit=5000)
    observed = _observability(request, session)
    return {
        "schema_version": SESSION_OBSERVABILITY_VERSION,
        "snapshot_id": str(uuid4()),
        "session_id": session_id,
        "created_at": datetime.now(UTC).isoformat(),
        "timeline": diagnostic_timeline(events, limit=limit),
        "runtime": {
            "session_state": observed["session_state"],
            "turn": observed["turn"],
            "liveness": observed["liveness"],
            "transport": observed["transport"],
            "provider_process": observed["provider_process"],
        },
        "redaction": "allowlisted_metadata_only",
    }


@router.get("/sessions")
def list_agent_sessions(request: Request) -> list[dict]:
    mgr = _require_session_traffic_ready(request)
    result: list[dict] = []
    live_ids: set[str] = set()
    for runtime in mgr.list_runtimes():
        if runtime._closed:
            continue
        live_ids.add(runtime.session.id)
        result.append(_session_list_item(request, runtime.session, runtime=runtime))
    for session in mgr.store.list_sessions():
        if session.id in live_ids or session.status == "closed":
            continue
        result.append(_session_list_item(request, session))
    return result


@router.get("/session-events/capabilities")
def multiplexed_session_event_capabilities() -> dict[str, Any]:
    """Advertise the bounded fleet activity transport to rolling-upgrade peers."""
    return {
        "schema_version": 1,
        "transport": "sse",
        "scope": "all_live_sessions",
        "max_browser_connections_per_instance": 1,
        "resume": "per_session_sequence",
        "dynamic_membership": True,
    }


def _multiplex_after_cursors(request: Request) -> dict[str, int]:
    cursors: dict[str, int] = {}
    raw = request.query_params.get("after")
    if raw:
        try:
            value = json.loads(raw)
        except TypeError, ValueError:
            value = {}
        if isinstance(value, dict):
            for session_id, seq in value.items():
                try:
                    cursors[str(session_id)] = max(0, int(seq))
                except TypeError, ValueError:
                    continue
    last_event_id = request.headers.get("Last-Event-ID") or ""
    if ":" in last_event_id:
        session_id, raw_seq = last_event_id.rsplit(":", 1)
        try:
            cursors[session_id] = max(cursors.get(session_id, 0), int(raw_seq))
        except ValueError:
            pass
    return cursors


@router.get("/session-events")
async def multiplexed_session_events(request: Request) -> StreamingResponse:
    """Fan every live ACP runtime into one resumable SSE connection.

    Membership is reconciled continuously, so starting or closing a session does
    not replace the browser transport. Durable transcript sequences remain the
    per-session ordering and replay boundary.
    """
    from pa.core.sse_observability import sse_connections
    from pa.server.shutdown import is_shutting_down, wait_for_shutdown, wait_for_shutdown_or

    manager = _require_session_traffic_ready(request)
    initial_cursors = _multiplex_after_cursors(request)
    client_id = request.query_params.get("client_id")

    async def event_stream():
        subscriptions: dict[
            str, tuple[AgentSessionRuntime, asyncio.Queue[dict[str, Any]]]
        ] = {}
        cursors = dict(initial_cursors)
        outcome = "closed"
        connection_id = sse_connections.open(
            endpoint="/api/agent/session-events",
            direction="downstream",
            client_id=client_id,
            session_scope="all_live",
        )
        try:
            reconnect_attempt = int(request.query_params.get("reconnect_attempt") or 0)
        except TypeError, ValueError:
            reconnect_attempt = 0
        if reconnect_attempt > 0:
            sse_connections.increment("reconnecting")

        async def durable_events(
            runtime: AgentSessionRuntime, after_seq: int
        ) -> list[dict[str, Any]]:
            runtime._flush_transcript()
            await _drain_runtime_transcripts(runtime)
            events: list[dict[str, Any]] = []
            cursor = after_seq
            while True:
                page = await _runtime_offload(
                    runtime,
                    "sqlite.transcript_read",
                    runtime.store.list_transcript_events,
                    runtime.session_id,
                    after_seq=cursor,
                    limit=TRANSCRIPT_WINDOW_LIMIT,
                )
                if not page:
                    break
                for item in page:
                    if item.seq <= cursor:
                        continue
                    events.append(
                        {
                            "id": item.id,
                            "seq": item.seq,
                            "type": item.event_type,
                            "session_id": item.session_id,
                            "payload": item.payload,
                            "created_at": item.created_at.isoformat(),
                        }
                    )
                    cursor = item.seq
                if len(page) < TRANSCRIPT_WINDOW_LIMIT:
                    break
            return events

        try:
            yield (
                "retry: 5000\nevent: ready\ndata: "
                + json.dumps(
                    {
                        "schema_version": 1,
                        "scope": "all_live_sessions",
                        "session_count": 0,
                        "timestamp": datetime.now(UTC).isoformat(),
                    }
                )
                + "\n\n"
            )
            while not is_shutting_down():
                if await request.is_disconnected():
                    outcome = "cancelled"
                    break

                live = {
                    runtime.session_id: runtime
                    for runtime in manager.list_runtimes()
                    if not getattr(runtime, "_closed", False)
                }
                for session_id in list(subscriptions):
                    runtime, queue = subscriptions[session_id]
                    if live.get(session_id) is runtime:
                        continue
                    runtime.unsubscribe(queue)
                    del subscriptions[session_id]
                for session_id, runtime in live.items():
                    if session_id in subscriptions:
                        continue
                    queue = runtime.subscribe()
                    subscriptions[session_id] = (runtime, queue)
                    if session_id not in cursors:
                        # A runtime added after this browser transport opened has
                        # no client replay contract yet. Begin at its current
                        # authoritative cursor; the queued subscription preserves
                        # anything emitted after this point.
                        cursors[session_id] = int(getattr(runtime, "_seq", 0) or 0)
                    for event in await durable_events(
                        runtime, cursors.get(session_id, 0)
                    ):
                        seq = int(event.get("seq") or 0)
                        cursors[session_id] = max(cursors.get(session_id, 0), seq)
                        yield _multiplex_sse(event)

                pending = {
                    asyncio.create_task(queue.get()): (session_id, runtime)
                    for session_id, (runtime, queue) in subscriptions.items()
                }
                if not pending:
                    if await wait_for_shutdown(1.0):
                        break
                    continue
                stopping, waited = await wait_for_shutdown_or(
                    asyncio.wait(
                        pending, timeout=1.0, return_when=asyncio.FIRST_COMPLETED
                    )
                )
                if stopping or waited is None:
                    for task in pending:
                        if not task.done():
                            task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)
                    break
                done, waiting = waited
                for task in waiting:
                    task.cancel()
                if waiting:
                    await asyncio.gather(*waiting, return_exceptions=True)
                if not done:
                    continue
                ready: list[tuple[str, AgentSessionRuntime, dict[str, Any]]] = []
                for task in done:
                    session_id, runtime = pending[task]
                    ready.append((session_id, runtime, task.result()))
                ready.sort(
                    key=lambda item: (
                        str(item[2].get("created_at") or ""),
                        item[0],
                        int(item[2].get("seq") or 0),
                    )
                )
                for session_id, runtime, event in ready:
                    seq = int(event.get("seq") or 0)
                    cursor = cursors.get(session_id, 0)
                    if seq and seq <= cursor:
                        continue
                    if seq and seq > cursor + 1:
                        for retained in await durable_events(runtime, cursor):
                            retained_seq = int(retained.get("seq") or 0)
                            if retained_seq >= seq:
                                break
                            cursors[session_id] = retained_seq
                            yield _multiplex_sse(retained)
                    cursors[session_id] = max(cursors.get(session_id, 0), seq)
                    yield _multiplex_sse(event)
        except asyncio.CancelledError:
            outcome = "cancelled"
            raise
        except Exception:
            outcome = "errored"
            logger.exception("Multiplexed agent activity stream failed")
            raise
        finally:
            for runtime, queue in subscriptions.values():
                runtime.unsubscribe(queue)
            sse_connections.close(connection_id, outcome)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _multiplex_sse(data: dict[str, Any]) -> str:
    session_id = str(data.get("session_id") or "")
    seq = int(data.get("seq") or 0)
    lines = [f"id: {session_id}:{seq}"]
    lines.append(f"event: {data.get('type') or 'message'}")
    lines.append(f"data: {json.dumps(data, default=str)}")
    return "\n".join(lines) + "\n\n"


def _session_list_item(
    request: Request,
    session: AgentSession,
    *,
    runtime: AgentSessionRuntime | None = None,
) -> dict:
    config = session.config_json or {}
    configuration = config.get("configuration", {})
    current_origin_name = current_instance_name(
        request.app.state.ctx,
        session.origin_instance_id,
        session.origin_instance_name,
    )
    durable = config.get("durable_runtime", {})
    queued = durable.get("queued_prompts") or []
    associated_cards = request.app.state.ctx.store.list_cards_for_session(session.id)
    return {
        "id": session.id,
        "title": session.title,
        "label": session.label,
        "agent_name": session.agent_name,
        "origin_instance_id": session.origin_instance_id,
        "origin_instance_name": current_origin_name,
        "origin_instance_name_snapshot": session.origin_instance_name,
        "origin_instance_name_at_session_start": (
            session.origin_instance_name
            if session.origin_instance_name
            and session.origin_instance_name != current_origin_name
            else None
        ),
        "status": session.status,
        "connected": bool(runtime and runtime.connected),
        "prompting": bool(runtime and runtime.prompting),
        "live": runtime is not None,
        "orphan": runtime is None,
        "model_id": session.model_id,
        "mode_id": session.mode_id,
        "created_at": session.created_at.isoformat(),
        "metrics_json": session.metrics_json or {},
        "card_id": session.card_id,
        "project_id": session.project_id,
        "project": _session_project_payload(request, session),
        "cards": [
            {
                "id": card.id,
                "title": card.title,
                "project_id": card.project_id,
                "primary": card.id == session.card_id,
            }
            for card in associated_cards
        ],
        "card_ids": [card.id for card in associated_cards],
        "requested_model_id": configuration.get("requested", {}).get("model_id"),
        "requested_reasoning": configuration.get("requested", {}).get("reasoning"),
        "effective_reasoning": configuration.get("effective", {}).get("reasoning"),
        "configuration_state": configuration.get("state"),
        "pa_mcp": runtime.connection.pa_mcp_health
        if runtime and runtime.connection
        else None,
        "config_json": config,
        "queue_length": len(runtime._queue) if runtime else len(queued),
        "last_seq": runtime._seq if runtime else durable.get("last_event_cursor", 0),
        "updated_at": session.updated_at.isoformat(),
        "pr_watches": _session_pr_watches(request, session),
        "card_reconciliation": _session_reconciliation(request, session.id),
        "observability": _observability(request, session),
    }


@router.get("/provider-options/{provider_id}")
def get_provider_options(
    request: Request,
    provider_id: str,
    model_provider: str | None = None,
) -> dict:
    """Return live or durably cached session options for a provider."""
    from pa.acp.providers.registry import get_provider

    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    mgr = _manager(request)
    principal_id = get_principal_id(request)
    auth_required = bool(request.app.state.ctx.settings.auth_required)

    def session_is_visible(session) -> bool:
        return (
            not auth_required or getattr(session, "principal_id", None) == principal_id
        )

    for runtime in mgr.list_runtimes():
        if (
            runtime.session.agent_name == provider_id
            and session_is_visible(runtime.session)
            and not getattr(runtime, "_closed", False)
            and runtime.connection
        ):
            payload = {
                "provider": provider_id,
                "models": runtime.connection.models,
                "modes": runtime.connection.modes,
                "config_options": runtime.connection.config_options,
                "cached": False,
                "source": "live_session",
            }
            if provider_id == "openinterpreter":
                from pa.acp.providers.openinterpreter import provider_options_snapshot

                catalog = provider_options_snapshot(
                    request.app.state.ctx.settings.data_dir,
                    model_provider=model_provider,
                )
                payload["model_providers"] = catalog.get("model_providers")
                payload["supports_model_provider"] = True
                payload["model_provider"] = catalog.get("model_provider")
                if model_provider:
                    # Selecting a different backend refreshes models before start.
                    payload["models"] = catalog.get("models")
                    payload["config_options"] = catalog.get("config_options")
                    payload["source"] = "openinterpreter_catalog"
            return payload
    for session in mgr.store.list_sessions():
        if session.agent_name != provider_id or not session_is_visible(session):
            continue
        config = dict(session.config_json or {})
        if any(key in config for key in ("models", "modes", "options")):
            payload = {
                "provider": provider_id,
                "models": config.get("models"),
                "modes": config.get("modes"),
                "config_options": config.get("options"),
                "cached": True,
                "source": "session_cache",
            }
            if provider_id == "openinterpreter":
                from pa.acp.providers.openinterpreter import provider_options_snapshot

                catalog = provider_options_snapshot(
                    request.app.state.ctx.settings.data_dir,
                    model_provider=model_provider
                    or (config.get("configuration") or {})
                    .get("requested", {})
                    .get("model_provider")
                    or (config.get("configuration") or {})
                    .get("effective", {})
                    .get("model_provider"),
                )
                payload["model_providers"] = catalog.get("model_providers")
                payload["supports_model_provider"] = True
                payload["model_provider"] = catalog.get("model_provider")
                if model_provider or not payload["models"]:
                    payload["models"] = catalog.get("models")
                    payload["modes"] = payload["modes"] or catalog.get("modes")
                    payload["config_options"] = (
                        payload["config_options"] or catalog.get("config_options")
                    )
            return payload
    if provider_id == "openinterpreter":
        from pa.acp.providers.openinterpreter import provider_options_snapshot

        return provider_options_snapshot(
            request.app.state.ctx.settings.data_dir,
            model_provider=model_provider,
        )
    # Prefer provider status metadata options when a host cached them.
    status_meta = {}
    try:
        status = provider.status(request.app.state.ctx.settings.data_dir)
        status_meta = dict(getattr(status, "meta", None) or {})
    except Exception:
        status_meta = {}
    options = status_meta.get("options") if isinstance(status_meta, dict) else None
    if isinstance(options, dict) and any(
        options.get(key) for key in ("models", "modes", "config_options")
    ):
        return {
            "provider": provider_id,
            "models": options.get("models"),
            "modes": options.get("modes"),
            "config_options": options.get("config_options"),
            "model_providers": status_meta.get("model_providers"),
            "cached": True,
            "source": "provider_status",
        }
    return {
        "provider": provider_id,
        "models": None,
        "modes": None,
        "config_options": None,
        "cached": True,
        "source": "empty",
    }


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
        sessions = [
            session
            for session in sessions
            if session.card_id == card_id
            or card_id in mgr.store.list_card_ids_for_session(session.id)
        ]
    if project_id:
        sessions = [session for session in sessions if session.project_id == project_id]
    return [
        {
            **session.model_dump(mode="json"),
            "cards": _session_cards_payload(request, session),
            "card_ids": mgr.store.list_card_ids_for_session(session.id),
            "project": _session_project_payload(request, session),
            "instance_id": settings.instance_id,
            "instance_name": settings.instance_name,
            "pr_watches": _session_pr_watches(request, session),
            "card_reconciliation": _session_reconciliation(request, session.id),
            "live": bool(
                (runtime := mgr.get(session.id))
                and not getattr(runtime, "_closed", False)
            ),
            "recovery": _durable_session_state(mgr, session),
        }
        for session in sessions[: max(1, min(limit, 500))]
    ]


@router.get("/history/{session_id}")
async def get_agent_session_history(
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
    session = await _offload(
        mgr,
        "sqlite.agent_session_read",
        mgr.store.get_session,
        session_id,
        timeout=3.0,
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    runtime = mgr.get(session_id)
    page_limit = max(1, min(limit, 5000))
    if after_seq is not None:
        events = await _offload(
            mgr,
            "sqlite.transcript_read",
            mgr.store.list_transcript_events,
            session_id,
            after_seq=max(0, after_seq),
            limit=page_limit + 1,
            timeout=3.0,
        )
        has_more = len(events) > page_limit
        events = events[:page_limit]
        page = {
            "oldest_seq": events[0].seq if events else None,
            "newest_seq": events[-1].seq if events else None,
            "next_before_seq": None,
            "has_older": False,
            "has_newer": has_more,
            "limit": page_limit,
        }
    else:
        cursor = max(1, before_seq) if before_seq is not None else None
        events = await _offload(
            mgr,
            "sqlite.transcript_read",
            mgr.store.list_transcript_events_before,
            session_id,
            before_seq=cursor,
            limit=page_limit + 1,
            timeout=3.0,
        )
        has_older = len(events) > page_limit
        events = events[-page_limit:]
        page = {
            "oldest_seq": events[0].seq if events else None,
            "newest_seq": events[-1].seq if events else None,
            "next_before_seq": events[0].seq if has_older and events else None,
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
        "card_reconciliation": _session_reconciliation(request, session.id),
        "recovery": _durable_session_state(mgr, session),
        "events": [event.model_dump(mode="json") for event in events],
        "page": page,
    }


@router.post("/sessions/{session_id}/recover")
async def recover_session(
    request: Request,
    session_id: str,
    body: RecoverSessionBody | None = None,
) -> dict:
    mgr = _require_session_traffic_ready(request)
    try:
        runtime = await mgr.recover_session(
            session_id, provider_override=body.provider if body else None
        )
    except AgentStartupNotReady:
        _require_session_traffic_ready(request)
        raise RuntimeError("unreachable startup gate")
    except AgentSessionRecoveryError as exc:
        message = str(exc)
        lowered = message.lower()
        deleted = "deleted" in lowered
        blocked = "blocked" in lowered
        raise HTTPException(
            status_code=404 if deleted else 409,
            detail={
                "code": (
                    "session_deleted"
                    if deleted
                    else "session_recovery_blocked"
                    if blocked
                    else "session_closed"
                ),
                "message": message,
                "recoverable": False,
                "durable_session": {
                    "exists": not deleted,
                    "reason": (
                        "pa_session_deleted"
                        if deleted
                        else "recovery_blocked"
                        if blocked
                        else "session_closed"
                    ),
                },
                **_session_actions(session_id, recoverable=False),
            },
        ) from exc
    except Exception as exc:
        session = await _offload(
            mgr,
            "sqlite.agent_session_read",
            mgr.store.get_session,
            session_id,
        )
        state = (
            _durable_session_state(mgr, session)
            if session
            else {"exists": False, "recoverable": False}
        )
        missing_provider = "Unknown ACP provider" in str(exc)
        raise HTTPException(
            status_code=503,
            detail={
                "code": "provider_unavailable"
                if missing_provider
                else "session_recovery_failed",
                "message": str(exc),
                "recoverable": bool(state.get("recoverable")),
                "durable_session": state,
                **_session_actions(
                    session_id, recoverable=bool(state.get("recoverable"))
                ),
            },
        ) from exc
    return await _offload(mgr, "agent.session_snapshot", runtime.snapshot)


@router.get("/sessions/{session_id}")
async def get_session_snapshot(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    snapshot = runtime.snapshot(include_transcript=False)
    snapshot["card_reconciliation"] = _session_reconciliation(request, session_id)
    snapshot["observability"] = _observability(
        request,
        runtime.session,
        events=[],
    )
    snapshot["cards"] = _session_cards_payload(request, runtime.session)
    return snapshot


def _session_cards_payload(request: Request, session: AgentSession) -> list[dict]:
    return [
        {
            "id": card.id,
            "title": card.title,
            "project_id": card.project_id,
            "lane": card.lane.value,
            "primary": card.id == session.card_id,
        }
        for card in request.app.state.ctx.store.list_cards_for_session(session.id)
    ]


@router.get("/sessions/{session_id}/cards")
def list_session_cards(request: Request, session_id: str) -> dict:
    mgr = _manager(request)
    session = mgr.store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    return {
        "session_id": session_id,
        "primary_card_id": session.card_id,
        "cards": _session_cards_payload(request, session),
    }


@router.post("/sessions/{session_id}/cards/{card_id}")
def link_session_card(
    request: Request,
    session_id: str,
    card_id: str,
    body: SessionCardLinkBody,
) -> dict:
    mgr = _manager(request)
    try:
        session = mgr.store.link_session_card(
            session_id,
            card_id,
            principal_id=get_principal_id(request),
            make_primary=body.make_primary,
        )
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    runtime = mgr.get(session_id)
    if runtime and body.make_primary:
        runtime.session.card_id = session.card_id
        runtime.session.item_id = session.item_id
        runtime.session.project_id = session.project_id
    return {
        "session_id": session_id,
        "primary_card_id": session.card_id,
        "cards": _session_cards_payload(request, session),
    }


@router.delete("/sessions/{session_id}/cards/{card_id}")
def unlink_session_card(request: Request, session_id: str, card_id: str) -> dict:
    mgr = _manager(request)
    try:
        session = mgr.store.unlink_session_card(session_id, card_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    runtime = mgr.get(session_id)
    if runtime:
        runtime.session.card_id = session.card_id
        runtime.session.item_id = session.item_id
        runtime.session.project_id = session.project_id
    return {
        "session_id": session_id,
        "primary_card_id": session.card_id,
        "cards": _session_cards_payload(request, session),
    }


@router.get("/sessions/{session_id}/events")
async def session_events(request: Request, session_id: str) -> StreamingResponse:
    from pa.server.shutdown import is_shutting_down, wait_for_shutdown_or

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
            await _drain_runtime_transcripts(runtime)
            while True:
                if is_shutting_down():
                    return
                page = await _runtime_offload(
                    runtime,
                    "sqlite.transcript_read",
                    runtime.store.list_transcript_events,
                    session_id,
                    after_seq=cursor,
                    limit=TRANSCRIPT_WINDOW_LIMIT,
                )
                if not page:
                    break
                for te in page:
                    if is_shutting_down():
                        return
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
                    yield await _runtime_offload(
                        runtime, "agent.sse_serialize", _sse, te.seq, payload
                    )
                    cursor = te.seq
                if len(page) < TRANSCRIPT_WINDOW_LIMIT:
                    break

            while True:
                if is_shutting_down() or await request.is_disconnected():
                    break
                try:
                    stopping, event = await wait_for_shutdown_or(
                        queue.get(), timeout=15.0
                    )
                except TimeoutError:
                    yield ": keepalive\n\n"
                    continue
                if stopping:
                    break
                assert event is not None
                seq = int(event.get("seq") or 0)
                if seq and seq <= cursor:
                    continue
                if seq and seq > cursor + 1:
                    # A busy catch-up can overflow the bounded subscriber queue.
                    # Flush and fill any sequence gap from durable storage before
                    # emitting the retained live event.
                    runtime._flush_transcript()
                    await _drain_runtime_transcripts(runtime)
                    while cursor < seq - 1:
                        if is_shutting_down():
                            return
                        gap_page = await _runtime_offload(
                            runtime,
                            "sqlite.transcript_read",
                            runtime.store.list_transcript_events,
                            session_id,
                            after_seq=cursor,
                            limit=TRANSCRIPT_WINDOW_LIMIT,
                        )
                        if not gap_page:
                            break
                        previous_cursor = cursor
                        for te in gap_page:
                            if is_shutting_down():
                                return
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
                            yield await _runtime_offload(
                                runtime, "agent.sse_serialize", _sse, te.seq, payload
                            )
                            cursor = te.seq
                        if cursor == previous_cursor:
                            break
                    if seq <= cursor:
                        continue
                cursor = max(cursor, seq)
                yield await _runtime_offload(
                    runtime, "agent.sse_serialize", _sse, seq or None, event
                )
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


def _public_prompt_images(images: list[ImageAttachment]) -> list[dict[str, str]]:
    return [image.public_dict() for image in images]


def _prompt_acceptance_matches(
    event: TranscriptEvent, message: str, images: list[ImageAttachment]
) -> bool:
    payload = event.payload or {}
    return payload.get("message") == message and list(
        payload.get("images") or []
    ) == _public_prompt_images(images)


async def _record_web_intake(
    request: Request,
    session_id: str,
    body: PromptBody,
    runtime,
    message: str,
) -> dict[str, str] | None:
    ctx = getattr(request.app.state, "ctx", None)
    services = getattr(ctx, "services", None)
    if not isinstance(services, dict):
        return None
    service = services.get("intake_service")
    if service is None:
        return None
    principal_id = get_principal_id(request)
    message_id = (
        body.client_prompt_id
        or body.idempotency_key
        or (f"dispatch:{body.dispatch_id}" if body.dispatch_id else str(uuid4()))
    )
    realm_id = getattr(runtime.session, "realm_id", None) or ctx.settings.primary_realm
    project_id = body.project_id or getattr(runtime.session, "project_id", None)
    item = await _runtime_offload(
        runtime,
        "intake.web_prompt",
        service.ingest_web_prompt,
        principal_id=principal_id,
        session_id=session_id,
        message=message,
        images=body.images,
        realm_id=realm_id,
        project_id=project_id,
        goal_ids=[],
        channel_message_id=message_id,
        context=IntakeMutationContext(
            actor_principal=principal_id,
            authority_instance_id=ctx.settings.instance_id,
            idempotency_key=f"web:{session_id}:{message_id}",
        ),
    )
    return {
        "id": item.id,
        "correlation_id": item.correlation_id,
        "disposition": item.security.disposition.value,
    }


async def _submit_client_prompt(
    request: Request,
    session_id: str,
    body: PromptBody,
    runtime,
    message: str,
) -> dict:
    """Idempotently admit a browser prompt and prove transcript durability."""
    assert body.client_prompt_id
    prompt_id = body.client_prompt_id
    async with runtime._prompt_admission_lock:
        existing = await _runtime_offload(
            runtime,
            "sqlite.prompt_acceptance_read",
            runtime.store.get_prompt_acceptance,
            session_id,
            prompt_id,
        )
        if existing:
            if not _prompt_acceptance_matches(existing, message, body.images):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "client_prompt_id_conflict",
                        "message": "This prompt id was already accepted for different content.",
                        "recoverable": False,
                    },
                )
            queued = (
                existing.event_type == "queue_enqueued"
                and existing.payload.get("action") != "run"
            )
            return {
                "stop_reason": "accepted",
                "queued": queued,
                "started": not queued,
                "accepted": True,
                "accepted_event": existing.event_type,
                "prompt_id": prompt_id,
                "dispatch_id": None,
                "session_id": session_id,
                "duplicate": True,
                "queue": [item.public_dict() for item in runtime._queue],
            }

        stop_reason = await runtime.prompt(
            message,
            images=body.images,
            item_id=body.card_id,
            principal_id=get_principal_id(request),
            project_id=body.project_id,
            action=body.action,
            prompt_id=prompt_id,
            wait=False,
        )
        runtime._flush_transcript()
        await _drain_runtime_transcripts(runtime)
        accepted = await _runtime_offload(
            runtime,
            "sqlite.prompt_acceptance_read",
            runtime.store.get_prompt_acceptance,
            session_id,
            prompt_id,
        )
        if not accepted or not _prompt_acceptance_matches(
            accepted, message, body.images
        ):
            raise HTTPException(
                status_code=503,
                detail={
                    "code": "prompt_not_persisted",
                    "message": "Prompt acceptance was not present in the durable transcript.",
                    "recoverable": True,
                },
            )
        return {
            "stop_reason": stop_reason,
            "queued": stop_reason == "queued",
            "started": stop_reason == "started",
            "accepted": True,
            "accepted_event": accepted.event_type,
            "prompt_id": prompt_id,
            "dispatch_id": None,
            "session_id": session_id,
            "duplicate": False,
            "queue": [item.public_dict() for item in runtime._queue],
        }


@router.post("/sessions/{session_id}/prompt")
async def session_prompt(request: Request, session_id: str, body: PromptBody) -> dict:
    message = body.message.strip()
    if not message and not body.images:
        raise HTTPException(status_code=400, detail="message or image required")
    runtime = None
    durable_session = None
    needs_recovery = False
    try:
        runtime = _runtime_or_404(request, session_id)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        if (
            exc.status_code != 409
            or detail.get("code") != "session_not_live"
            or not detail.get("recoverable")
        ):
            raise
        mgr = _require_session_traffic_ready(request)
        durable_session = await _offload(
            mgr, "sqlite.agent_session_read", mgr.store.get_session, session_id
        )
        if durable_session is None:
            raise
        needs_recovery = True
    settings = request.app.state.ctx.settings
    principal_id = get_principal_id(request)
    user = getattr(request.state, "user", None)
    instance_authenticated = (
        getattr(request.state, "instance_authenticated", False) is True
    )
    session_record = runtime.session if runtime is not None else durable_session
    linked_dispatch_value = getattr(session_record, "dispatch_id", None)
    linked_dispatch_id = (
        linked_dispatch_value.strip()
        if isinstance(linked_dispatch_value, str) and linked_dispatch_value.strip()
        else None
    )
    dispatch_record = None
    dispatch_store = request.app.state.ctx.services.get("dispatch_store")
    if linked_dispatch_id and dispatch_store:
        dispatch_record = await (
            _runtime_offload(
                runtime,
                "dispatch.record_read",
                dispatch_store.get,
                linked_dispatch_id,
            )
            if runtime is not None
            else _offload(
                mgr,
                "dispatch.record_read",
                dispatch_store.get,
                linked_dispatch_id,
            )
        )
    if dispatch_record and dispatch_record.goal_provenance is not None:
        if not instance_authenticated:
            raise HTTPException(
                status_code=403,
                detail={
                    "code": "governed_dispatch_prompt_requires_authority",
                    "message": "Goal-linked sessions accept prompts only through their dispatch authority.",
                    "recoverable": False,
                },
            )
        if body.dispatch_id != linked_dispatch_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "governed_dispatch_prompt_missing_contract",
                    "expected_dispatch_id": linked_dispatch_id,
                    "recoverable": False,
                },
            )
        if body.idempotency_key:
            if not _goal_followup_provenance_matches(
                dispatch_record.goal_provenance,
                body.goal_provenance,
            ):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "code": "goal_followup_provenance_mismatch",
                        "recoverable": False,
                    },
                )
        elif dispatch_record.goal_provenance != body.goal_provenance:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_dispatch_provenance_mismatch",
                    "recoverable": False,
                },
            )
    if instance_authenticated and not body.dispatch_id:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "insufficient_authorization",
                "message": "Fleet credentials may prompt only a dispatch-linked session.",
            },
        )
    if (
        settings.auth_required is True
        and not instance_authenticated
        and getattr(
            runtime.session if runtime is not None else durable_session,
            "principal_id",
            None,
        )
        != principal_id
        and getattr(user, "role", None) != "admin"
    ):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "insufficient_authorization",
                "message": "This principal does not own the linked agent session.",
            },
        )
    if needs_recovery:
        try:
            runtime = await mgr.recover_session(session_id)
        except (AgentSessionRecoveryError, RuntimeError) as exc:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_recovery_failed",
                    "message": str(exc),
                    "recoverable": True,
                    **_session_actions(session_id, recoverable=True),
                },
            ) from exc
    assert runtime is not None
    intake = await _record_web_intake(request, session_id, body, runtime, message)
    if body.client_prompt_id:
        response = await _submit_client_prompt(
            request, session_id, body, runtime, message
        )
        if intake:
            response["intake"] = intake
        return response
    if body.dispatch_id:
        if dispatch_record is None:
            dispatch_record = (
                await _runtime_offload(
                    runtime,
                    "dispatch.record_read",
                    dispatch_store.get,
                    body.dispatch_id,
                )
                if dispatch_store
                else None
            )
        if not dispatch_record:
            raise HTTPException(
                status_code=409,
                detail={"code": "dispatch_not_materialized", "recoverable": True},
            )
        if dispatch_record.session_id != session_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "dispatch_session_mismatch",
                    "expected": dispatch_record.session_id,
                    "actual": session_id,
                    "recoverable": False,
                },
            )
        if (
            instance_authenticated
            and dispatch_record.goal_provenance is None
            and body.goal_provenance is not None
        ):
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "goal_dispatch_provenance_mismatch",
                    "recoverable": False,
                },
            )
        followup_fingerprint = hashlib.sha256(
            json.dumps(
                {"message": message, "action": body.action},
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()
        if body.idempotency_key:
            prior = dispatch_record.followup_operations.get(body.idempotency_key)
            if prior:
                if prior.get("fingerprint") != followup_fingerprint:
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "idempotency_conflict",
                            "message": "This follow-up key was used for a different prompt.",
                        },
                    )
                if dispatch_record.goal_provenance is not None and (
                    prior.get("goal_provenance")
                    != body.goal_provenance.model_dump(mode="json")
                ):
                    raise HTTPException(
                        status_code=409,
                        detail={
                            "code": "goal_followup_provenance_mismatch",
                            "recoverable": False,
                        },
                    )
                return {
                    **dict(prior.get("response") or {}),
                    "duplicate": True,
                    "intake": intake,
                }
        elif dispatch_record.prompt_ack:
            ack = dispatch_record.prompt_ack
            return {
                "stop_reason": "accepted",
                "queued": True,
                "started": False,
                "accepted": True,
                "accepted_event": ack.get("event_type"),
                "prompt_id": ack.get("prompt_id"),
                "dispatch_id": body.dispatch_id,
                "session_id": session_id,
                "duplicate": True,
                "queue": [item.public_dict() for item in runtime._queue],
                "intake": intake,
            }
    # Return immediately; transcript/SSE streams the turn. Blocking here made the
    # old HTMX UI look like it only ever received "Turn completed".
    before_seq = runtime._seq
    logger.info(
        "Agent prompt submission received",
        extra={
            "session_id": session_id,
            "principal_id": principal_id,
            "action": body.action,
            "message_length": len(message),
            "image_count": len(body.images),
            "prompting": runtime.prompting,
            "queue_length": len(runtime._queue),
            "before_seq": before_seq,
            "dispatch_id": body.dispatch_id,
        },
    )
    stop_reason = await runtime.prompt(
        message,
        images=body.images,
        item_id=body.card_id,
        principal_id=principal_id,
        project_id=body.project_id,
        action=body.action,
        wait=False,
    )
    runtime._flush_transcript()
    await _drain_runtime_transcripts(runtime)
    accepted = [
        event
        for event in await _runtime_offload(
            runtime,
            "sqlite.transcript_read",
            runtime.store.list_transcript_events,
            session_id,
            after_seq=before_seq,
            limit=20,
        )
        if event.event_type in {"queue_enqueued", "user_message"}
        and event.payload.get("message") == message
    ]
    if body.dispatch_id and not accepted:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "prompt_not_persisted",
                "message": "Prompt acceptance was not present in the durable transcript.",
                "recoverable": True,
            },
        )
    accepted_event = accepted[0] if accepted else None
    logger.info(
        "Agent prompt submission processed",
        extra={
            "session_id": session_id,
            "stop_reason": stop_reason,
            "accepted": accepted_event is not None,
            "accepted_event": (
                accepted_event.event_type if accepted_event is not None else None
            ),
            "accepted_seq": accepted_event.seq if accepted_event is not None else None,
            "queue_length": len(runtime._queue),
            "dispatch_id": body.dispatch_id,
        },
    )
    response = {
        "stop_reason": stop_reason,
        "queued": stop_reason == "queued",
        "started": stop_reason == "started",
        "accepted": bool(accepted_event),
        "accepted_event": accepted_event.event_type if accepted_event else None,
        "prompt_id": accepted_event.payload.get("id") if accepted_event else None,
        "dispatch_id": body.dispatch_id,
        "session_id": session_id,
        "duplicate": False,
        "queue": [q.public_dict() for q in runtime._queue],
    }
    if dispatch_record and dispatch_store:
        assert accepted_event is not None
        ack = {
            "event_id": accepted_event.id,
            "event_seq": accepted_event.seq,
            "event_type": accepted_event.event_type,
            "prompt_id": accepted_event.payload.get("id"),
        }
        if body.idempotency_key:
            dispatch_record.followup_operations[body.idempotency_key] = {
                "fingerprint": followup_fingerprint,
                "goal_provenance": (
                    body.goal_provenance.model_dump(mode="json")
                    if body.goal_provenance
                    else None
                ),
                "state": "accepted_target",
                "response": {
                    key: response.get(key)
                    for key in (
                        "stop_reason",
                        "queued",
                        "started",
                        "accepted",
                        "accepted_event",
                        "prompt_id",
                        "dispatch_id",
                        "session_id",
                        "duplicate",
                    )
                },
            }
            message_text = "Follow-up durably accepted by linked remote session."
        else:
            dispatch_record.prompt_acknowledged_at = accepted_event.created_at
            dispatch_record.prompt_ack = ack
            message_text = "Prompt durably accepted by linked remote session."
        if body.idempotency_key:
            await _runtime_offload(
                runtime,
                "dispatch.followup_ack",
                dispatch_store.record_followup_started,
                dispatch_record,
                idempotency_key=body.idempotency_key,
                prompt_id=accepted_event.payload.get("id"),
                event_id=accepted_event.id,
                event_seq=accepted_event.seq,
            )
        else:
            await _runtime_offload(
                runtime,
                "dispatch.prompt_ack",
                dispatch_store.transition,
                dispatch_record,
                "running",
                message_text,
                detail={
                    "session_id": session_id,
                    "event_id": accepted_event.id,
                    "event_seq": accepted_event.seq,
                    "event_type": accepted_event.event_type,
                    "followup": False,
                },
            )
    if intake:
        response["intake"] = intake
    return response


@router.post("/sessions/{session_id}/cancel")
async def session_cancel(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    await runtime.cancel(pause_queue=True)
    return {"ok": True, "queue_paused": True}


@router.post("/sessions/{session_id}/retry")
async def session_retry(request: Request, session_id: str) -> dict:
    """Explicitly retry a durable session that is not currently live."""
    mgr = _require_session_traffic_ready(request)
    try:
        runtime = await mgr.retry_session(session_id)
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except Exception as exc:
        session = await _offload(
            mgr,
            "sqlite.agent_session_read",
            mgr.store.get_session,
            session_id,
        )
        if session and session.status == RECOVERY_BLOCKED_STATUS:
            provisioning = dict((session.config_json or {}).get("provisioning") or {})
            raise HTTPException(
                status_code=409,
                detail={
                    "code": provisioning.get("error_code")
                    or "session_recovery_blocked",
                    "message": provisioning.get("error") or str(exc),
                    "blocked": True,
                    "retryable": False,
                    "manual_retry": True,
                    "action": provisioning.get("action"),
                },
            ) from exc
        if session and session.status == "closed":
            raise HTTPException(
                status_code=409,
                detail={
                    "code": "session_closed",
                    "message": "Closed sessions cannot be retried",
                    "recoverable": False,
                },
            ) from exc
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return await _offload(mgr, "agent.session_snapshot", runtime.snapshot)


async def _close_orphan_session(
    mgr,
    session: AgentSession,
    *,
    reason: str,
) -> str | None:
    """Close one durable store-only session and return its prior status."""
    if session.status == "closed":
        return None
    closed_at = datetime.now(UTC)
    close_session_method = getattr(type(mgr.store), "close_session", None)
    if close_session_method is not None:
        closed, prior_status = await _offload(
            mgr,
            "sqlite.agent_session_close",
            mgr.store.close_session,
            session.id,
            reason=reason,
            closed_at=closed_at,
        )
        if closed is None:
            return None
        session.status = closed.status
        session.updated_at = closed.updated_at
        if prior_status is None:
            return None
    else:
        prior_status = session.status
        session.status = "closed"
        session.updated_at = closed_at

        def persist_close() -> None:
            mgr.store.save_session(session)
            next_seq = mgr.store.next_transcript_seq(session.id)
            mgr.store.append_transcript_events(
                [
                    TranscriptEvent(
                        session_id=session.id,
                        seq=next_seq,
                        event_type="session_closed",
                        payload={"reason": reason, "prior_status": prior_status},
                    )
                ]
            )

        await _offload(mgr, "sqlite.agent_session_close", persist_close)
    logger.info(
        "Orphaned agent session closed",
        extra={
            "session_id": session.id,
            "prior_status": prior_status,
            "close_reason": reason,
        },
    )
    return prior_status


async def _reconcile_closed_session_workspaces(
    mgr,
    session_ids: list[str],
) -> None:
    if not session_ids:
        return
    if isinstance(mgr, AgentSessionManager):
        await mgr.reconcile_closed_sessions(session_ids)
        return
    for session_id in dict.fromkeys(session_ids):
        try:
            await _offload(
                mgr,
                "workspace.expire_session",
                mgr.workspace_manager.expire_session,
                session_id,
                timeout=30.0,
            )
        except Exception:
            logger.exception(
                "Workspace expiration after session close failed for %s",
                session_id,
            )
    try:
        await _offload(
            mgr,
            "workspace.reconcile_terminal_state",
            mgr.workspace_manager.reconcile_terminal_state,
            timeout=30.0,
        )
        active_session_ids = {
            item.session_id
            for item in mgr.list_runtimes()
            if not getattr(item, "_closed", False)
        }
        await _offload(
            mgr,
            "workspace.collect_garbage",
            mgr.workspace_manager.collect_garbage,
            active_session_ids=active_session_ids,
            timeout=120.0,
        )
    except Exception:
        logger.exception(
            "Workspace reconciliation after closing sessions failed",
            extra={"session_ids": list(dict.fromkeys(session_ids))},
        )


@router.post("/sessions/close-all")
async def session_close_all(request: Request) -> dict:
    """Close every live or durable nonterminal session on this instance."""
    mgr = _require_session_traffic_ready(request)
    runtimes = [
        runtime
        for runtime in mgr.list_runtimes()
        if not getattr(runtime, "_closed", False)
    ]
    persisted = await _offload(
        mgr,
        "sqlite.agent_sessions_list",
        mgr.store.list_sessions,
    )
    live_ids = {runtime.session_id for runtime in runtimes}
    closed_ids: list[str] = []
    live_closed = 0
    orphan_closed = 0

    for runtime in runtimes:
        changed = await runtime.close(
            reason="bulk_user_close",
            reconcile_workspace=False,
        )
        mgr._runtimes.pop(runtime.session_id, None)
        if changed:
            closed_ids.append(runtime.session_id)
            live_closed += 1

    for session in persisted:
        if session.id in live_ids or session.status == "closed":
            continue
        prior_status = await _close_orphan_session(
            mgr,
            session,
            reason="bulk_user_close",
        )
        if prior_status is not None:
            closed_ids.append(session.id)
            orphan_closed += 1

    await _reconcile_closed_session_workspaces(mgr, closed_ids)
    logger.info(
        "All nonterminal agent sessions closed",
        extra={
            "closed_count": len(closed_ids),
            "live_closed": live_closed,
            "orphan_closed": orphan_closed,
            "session_ids": closed_ids,
        },
    )
    return {
        "ok": True,
        "closed": len(closed_ids),
        "live_closed": live_closed,
        "orphan_closed": orphan_closed,
        "session_ids": closed_ids,
    }


@router.post("/sessions/{session_id}/close")
async def session_close(request: Request, session_id: str) -> dict:
    """Close a live runtime, or mark a store-only orphan session closed.

    After abrupt restarts, sessions can remain `prompting`/`connected` in the
    durable store with no live ACP runtime. Operators still need `/close` to
    clear those so card labels can be reused.
    """
    mgr = _require_session_traffic_ready(request)
    runtime = mgr.get(session_id)
    if runtime and not getattr(runtime, "_closed", False):
        await runtime.close(reason="user_close")
        mgr._runtimes.pop(session_id, None)
        return {"ok": True, "live": False}

    session = await _offload(
        mgr, "sqlite.agent_session_read", mgr.store.get_session, session_id
    )
    if not session:
        raise HTTPException(status_code=404, detail="Session not found")
    prior_status = await _close_orphan_session(
        mgr,
        session,
        reason="orphan_user_close",
    )
    if prior_status is not None:
        await _reconcile_closed_session_workspaces(mgr, [session_id])
    return {"ok": True, "live": False, "orphan": True}


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
    await _drain_runtime_transcripts(runtime)
    return {"queue_paused": True}


@router.post("/sessions/{session_id}/queue/resume")
async def queue_resume(request: Request, session_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    runtime.resume_queue()
    await _drain_runtime_transcripts(runtime)
    return {"queue_paused": False}


@router.post("/sessions/{session_id}/queue/reorder")
async def queue_reorder(request: Request, session_id: str, body: ReorderBody) -> dict:
    runtime = _runtime_or_404(request, session_id)
    queue = runtime.reorder_queue(body.prompt_ids)
    await _drain_runtime_transcripts(runtime)
    return {"queue": [q.public_dict() for q in queue]}


@router.delete("/sessions/{session_id}/queue/{prompt_id}")
async def queue_remove(request: Request, session_id: str, prompt_id: str) -> dict:
    runtime = _runtime_or_404(request, session_id)
    removed = runtime.remove_queued(prompt_id)
    if not removed:
        raise HTTPException(status_code=404, detail="Queued prompt not found")
    await _drain_runtime_transcripts(runtime)
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
            "telemetry_session_header": prefs.telemetry_session_header,
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
        # Merge keys so partial clients (e.g. Settings saving only chat.default)
        # do not wipe other surface defaults.
        if body.scope == "global":
            existing = get_preferences_store(settings.data_dir).load().agent_surfaces
        else:
            user_id = _user_id(request)
            existing = (
                get_preferences_store(settings.data_dir, user_id=user_id)
                .load()
                .agent_surfaces
            )
        updates["agent_surfaces"] = {**(existing or {}), **surfaces}
    if body.telemetry_session_header is not None:
        updates["telemetry_session_header"] = body.telemetry_session_header
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

    def register_mcp(self, mcp, ctx) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def list_agent_session_liveness(limit: int = 100) -> dict:
            """List normalized authoritative liveness for recent ACP sessions."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/agent/observability/v1/sessions",
                params={"limit": limit},
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def get_agent_session_liveness(session_id: str) -> dict | None:
            """Get one ACP session with turns, queue, progress, freshness, and recovery state."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/agent/observability/v1/sessions/{session_id}",
                allow_not_found=True,
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def list_agent_session_turns(session_id: str) -> dict | None:
            """List independent prompt/turn lifecycles, including post-dispatch follow-ups."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/agent/observability/v1/sessions/{session_id}/turns",
                allow_not_found=True,
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def request_agent_session_diagnostics(
            session_id: str, limit: int = 50
        ) -> dict | None:
            """Create a bounded privacy-safe diagnostic snapshot for an ACP session."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/agent/observability/v1/sessions/{session_id}/diagnostics",
                params={"limit": limit},
                allow_not_found=True,
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def list_agent_session_cards(session_id: str) -> dict | None:
            """List every card associated with one local ACP session."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/agent/sessions/{session_id}/cards",
                allow_not_found=True,
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def associate_agent_session_card(
            session_id: str,
            card_id: str,
            make_primary: bool = True,
        ) -> dict | None:
            """Associate a canonical card with a local ACP session without replacing older links."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/agent/sessions/{session_id}/cards/{card_id}",
                json={"make_primary": make_primary},
                allow_not_found=True,
                timeout_seconds=15.0,
            )

        @mcp.tool()
        def dissociate_agent_session_card(
            session_id: str,
            card_id: str,
        ) -> dict | None:
            """Remove one card association while preserving the session and its other cards."""
            return request_local_pa(
                ctx.settings,
                "DELETE",
                f"/api/agent/sessions/{session_id}/cards/{card_id}",
                allow_not_found=True,
                timeout_seconds=15.0,
            )
