"""Provider-neutral selection APIs. Mutation remains owned by the running PA."""

from __future__ import annotations

import asyncio
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.execution.selection import (
    ExecutionPreferences,
    OutcomeEvidence,
    SelectionError,
    SelectionPolicy,
    TaskAssessment,
)
from pa.execution.selection_service import service_for

router = APIRouter(prefix="/execution")


def _context(request):
    require_user(request)
    ctx = request.app.state.ctx
    return (
        ctx,
        service_for(ctx),
        get_principal_id(request),
        request.query_params.get("realm") or ctx.settings.primary_realm,
    )


def _error(exc):
    return HTTPException(
        status_code=409,
        detail={
            "code": exc.code,
            "message": str(exc),
            "execution_selection": exc.receipt,
        },
    )


@router.get("/catalog")
async def catalog(request: Request):
    ctx, service, _, _ = _context(request)
    candidates = await service.local_catalog(refresh=True)
    return _catalog_response(ctx, candidates)


def _catalog_response(ctx, candidates):
    return {
        "contract": "pa.execution-catalog/v1",
        "instance_id": ctx.settings.instance_id,
        "candidates": [c.model_dump(mode="json") for c in candidates],
        "empty_reason": None
        if candidates
        else "Discovery returned no available choices. Check provider status and refresh.",
        "refresh_interval_seconds": 60,
    }


@router.post("/catalog/refresh")
async def refresh_catalog(request: Request):
    ctx, service, _, _ = _context(request)
    generation = service.catalog_generation
    candidates = await service.local_catalog(force=True)
    response = _catalog_response(ctx, candidates)
    response["refresh"] = {
        "state": "refreshed"
        if service.catalog_generation != generation
        else "rate_limited",
        "retry_after_seconds": 3,
    }
    return response


class PreviewBody(BaseModel):
    card_id: str | None = None
    project_id: str | None = None
    surface: str = "execution"
    execution_preferences: ExecutionPreferences = Field(
        default_factory=ExecutionPreferences
    )
    task_assessment: TaskAssessment | None = None
    expected_card_version: datetime | None = None
    replace_card_preferences: bool = False


@router.get("/connections")
async def connections(request: Request):
    _, service, _, _ = _context(request)
    return {
        "profiles": [
            p.model_dump(mode="json", exclude={"credential_reference"})
            for p in service.store.connections()
        ],
        "native_contracts": {
            "codex": "Responses endpoints; native MODEL_PROVIDER/CODEX_CONFIG",
            "cursor": "account API key; account-exposed models only",
            "openinterpreter": "native responses/chat/messages endpoints",
        },
    }


from pa.execution.selection_connections import ConnectionProfile


class ConnectionBody(BaseModel):
    profile: ConnectionProfile
    expected_revision: int = 0


@router.put("/connections/{connection_id}")
async def configure_connection(
    request: Request, connection_id: str, body: ConnectionBody
):
    user = require_user(request)
    if user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Administrator permission is required to configure account/endpoint routing",
        )
    if body.profile.id != connection_id:
        raise HTTPException(status_code=422, detail="Connection ID mismatch")
    _, service, _, _ = _context(request)
    try:
        saved = await asyncio.to_thread(
            service.store.save_connection, body.profile, body.expected_revision
        )
        return saved.model_dump(mode="json", exclude={"credential_reference"})
    except SelectionError as exc:
        raise _error(exc) from exc


@router.post("/connections/{connection_id}/refresh")
async def refresh_connection(request: Request, connection_id: str):
    _, service, _, _ = _context(request)
    try:
        return {"candidates": await service.refresh_connection(connection_id)}
    except SelectionError as exc:
        raise _error(exc) from exc


@router.post("/preview")
async def preview(request: Request, body: PreviewBody):
    ctx, service, principal, realm = _context(request)
    card = (
        await asyncio.to_thread(ctx.store.get_card, body.card_id, realm_id=realm)
        if body.card_id
        else None
    )
    if body.card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    if card and body.project_id and body.project_id != card.project_id:
        raise HTTPException(
            status_code=409,
            detail="Project does not match the durable card; update the card explicitly first",
        )
    if body.expected_card_version and (
        not card or card.updated_at != body.expected_card_version
    ):
        raise HTTPException(
            status_code=409,
            detail={
                "code": "stale_card_preferences",
                "message": "Card changed; refresh before dispatch.",
            },
        )
    if card and body.replace_card_preferences:
        card = card.model_copy(
            update={"execution_preferences": body.execution_preferences}
        )
    project_id = body.project_id or (card.project_id if card else None)
    project = (
        await asyncio.to_thread(ctx.store.get_project, project_id, realm_id=realm)
        if project_id
        else None
    )
    candidates = await service.local_catalog(refresh=True)
    try:
        return await asyncio.to_thread(
            service.resolve,
            candidates=candidates,
            principal=principal,
            realm=realm,
            surface=body.surface,
            card=card,
            project_config=project.tool_config if project else None,
            overrides=body.execution_preferences,
            assessment=body.task_assessment,
        )
    except SelectionError as exc:
        raise _error(exc) from exc


@router.get("/defaults")
async def defaults(
    request: Request,
    card_id: str | None = None,
    project_id: str | None = None,
    surface: str = "execution",
):
    ctx, service, principal, realm = _context(request)
    card = (
        await asyncio.to_thread(ctx.store.get_card, card_id, realm_id=realm)
        if card_id
        else None
    )
    if card_id and not card:
        raise HTTPException(status_code=404, detail="Card not found")
    if card and project_id and project_id != card.project_id:
        raise HTTPException(
            status_code=409,
            detail="Project does not match the durable card; update the card explicitly first",
        )
    project_id = project_id or (card.project_id if card else None)
    project = (
        await asyncio.to_thread(ctx.store.get_project, project_id, realm_id=realm)
        if project_id
        else None
    )
    try:
        layers = await asyncio.to_thread(
            service.layers,
            principal=principal,
            surface=surface,
            card=card,
            project_config=project.tool_config if project else None,
        )
    except SelectionError as exc:
        raise _error(exc) from exc
    from pa.execution.selection import merge_preferences

    values, sources = merge_preferences(layers)
    return {
        "values": values,
        "provenance": sources,
        "card_version": card.updated_at.isoformat() if card else None,
        "layers": [
            {"source": s, "preferences": p.model_dump(mode="json")} for s, p in layers
        ],
    }


@router.get("/policy")
async def policy(request: Request):
    _, service, _, realm = _context(request)
    return await asyncio.to_thread(service.store.policy, realm)


class PolicyBody(BaseModel):
    expected_revision: int
    policy: SelectionPolicy


@router.put("/policy")
async def edit_policy(request: Request, body: PolicyBody):
    user = require_user(request)
    if user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Administrator permission is required to edit execution routing and budget policy.",
        )
    _, service, _, realm = _context(request)
    try:
        return await asyncio.to_thread(
            service.store.save_policy, realm, body.policy, body.expected_revision
        )
    except SelectionError as exc:
        raise _error(exc) from exc


@router.get("/decisions/{decision_id}")
async def decision(
    request: Request,
    decision_id: str,
    offset: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
):
    _, service, principal, realm = _context(request)
    receipt = await asyncio.to_thread(
        service.store.decision, decision_id, realm, principal
    )
    if not receipt:
        raise HTTPException(status_code=404, detail="Decision not found")
    attempts, total = await asyncio.gather(
        asyncio.to_thread(
            service.store.attempts, decision_id, offset=offset, limit=limit
        ),
        asyncio.to_thread(service.store.attempt_count, decision_id),
    )
    return {
        "decision": receipt,
        "attempts": attempts,
        "attempts_page": {
            "offset": offset,
            "limit": limit,
            "total": total,
            "next_offset": offset + len(attempts)
            if offset + len(attempts) < total
            else None,
        },
    }


@router.post("/evidence")
async def evidence(request: Request, body: OutcomeEvidence):
    user = require_user(request)
    # Workers may append observations but cannot promote their own assertions
    # into validated competence. Operator validation is explicit and auditable.
    if body.validated and user.role != "admin":
        raise HTTPException(
            status_code=403,
            detail="Validated outcome evidence requires an operator with administrator permission.",
        )
    if body.validated and not body.references:
        raise HTTPException(
            status_code=422,
            detail="Validated evidence needs test/review/completion references.",
        )
    _, service, principal, realm = _context(request)
    try:
        return await asyncio.to_thread(
            service.store.record_evidence, body, realm, principal
        )
    except SelectionError as exc:
        raise _error(exc) from exc


class SettingsBody(BaseModel):
    execution_preferences: ExecutionPreferences
    expected_version: datetime
    idempotency_key: str = Field(min_length=1, max_length=200)
    defer: bool = False


class CancelSettingsBody(BaseModel):
    idempotency_key: str
    expected_version: datetime


class BoundaryBody(BaseModel):
    execution_preferences: ExecutionPreferences
    idempotency_key: str = Field(min_length=1, max_length=200)


@router.post("/sessions/{session_id}/boundary")
async def session_boundary(request: Request, session_id: str, body: BoundaryBody):
    from pa.modules.agent_chat import _require_session_traffic_ready

    require_user(request)
    manager = _require_session_traffic_ready(request)
    source = await manager._offload(
        "selection.boundary_source", manager.store.get_session, session_id
    )
    if not source:
        raise HTTPException(status_code=404, detail="Source session not found")
    try:
        runtime = await manager.create_session(
            context_source_session_id=session_id,
            label="execution-boundary:" + session_id + ":" + body.idempotency_key,
            principal_id=get_principal_id(request),
            card_id=source.card_id,
            project_id=source.project_id,
            realm_id=source.realm_id,
            title="Linked: " + (source.title or "conversation"),
            execution_preferences=body.execution_preferences,
            purpose="chat",
            control_mode="human",
        )
        return await manager._offload("selection.boundary_snapshot", runtime.snapshot)
    except SelectionError as exc:
        raise _error(exc) from exc


@router.post("/sessions/{session_id}/settings/cancel")
async def cancel_session_settings(
    request: Request, session_id: str, body: CancelSettingsBody
):
    from pa.execution.selection_settings import cancel_settings
    from pa.modules.agent_chat import _runtime_or_404

    runtime = _runtime_or_404(request, session_id)
    user = require_user(request)
    principal = get_principal_id(request)
    if runtime.session.principal_id not in {None, principal} and user.role != "admin":
        raise HTTPException(
            status_code=403, detail="Session owner or administrator required"
        )
    try:
        return await cancel_settings(
            runtime,
            key=body.idempotency_key,
            expected_version=body.expected_version,
            principal=principal,
        )
    except SelectionError as exc:
        raise _error(exc) from exc


@router.post("/sessions/{session_id}/settings")
async def session_settings(request: Request, session_id: str, body: SettingsBody):
    from pa.execution.selection_settings import request_settings
    from pa.modules.agent_chat import _runtime_or_404

    runtime = _runtime_or_404(request, session_id)
    user = require_user(request)
    principal = get_principal_id(request)
    if runtime.session.principal_id not in {None, principal} and user.role != "admin":
        raise HTTPException(
            status_code=403, detail="Session owner or administrator required"
        )
    try:
        return await request_settings(
            runtime,
            body.execution_preferences,
            principal=principal,
            key=body.idempotency_key,
            expected_version=body.expected_version,
            defer=body.defer,
        )
    except SelectionError as exc:
        raise _error(exc) from exc


@router.get("/sessions/{session_id}/catalog")
async def session_catalog(request: Request, session_id: str):
    from pa.execution.selection_settings import live_candidates
    from pa.modules.agent_chat import _runtime_or_404

    runtime = _runtime_or_404(request, session_id)
    user = require_user(request)
    if (
        runtime.session.principal_id not in {None, get_principal_id(request)}
        and user.role != "admin"
    ):
        raise HTTPException(
            status_code=403, detail="Session owner or administrator required"
        )
    return {
        "candidates": [c.model_dump(mode="json") for c in live_candidates(runtime)],
        "session_version": runtime.session.updated_at.isoformat(),
    }


def register_mcp(mcp, ctx):
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def start_execution(
        execution_preferences: dict,
        idempotency_key: str,
        title: str | None = None,
        project_id: str | None = None,
    ) -> dict:
        """Start an owned standalone session using explicit selection and a stable label. Card work must use durable fleet dispatch."""
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise ValueError(
                "A nonempty stable idempotency key of at most 200 characters is required"
            )
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/agent/sessions",
            json={
                "label": "execution-mcp:" + idempotency_key,
                "title": title,
                "project_id": project_id,
                "execution_preferences": ExecutionPreferences.model_validate(
                    execution_preferences
                ).model_dump(mode="json"),
            },
        )

    @mcp.tool()
    def start_linked_execution(
        session_id: str, execution_preferences: dict, idempotency_key: str
    ) -> dict:
        """Explicitly replace an idle standalone context with a linked, separately leased attempt. Dispatch-owned sources require a new terminal-successor dispatch."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/boundary",
            json={
                "execution_preferences": execution_preferences,
                "idempotency_key": idempotency_key,
            },
        )

    @mcp.tool()
    def execution_catalog(refresh: bool = False) -> dict:
        """Read cached, scoped model/native-option capability evidence; explicit refresh is bounded/coalesced."""
        return request_local_pa(
            ctx.settings,
            "POST" if refresh else "GET",
            "/api/execution/catalog/refresh" if refresh else "/api/execution/catalog",
        )

    @mcp.tool()
    def execution_connections(
        connection_id: str | None = None,
        profile: dict | None = None,
        expected_revision: int = 0,
        refresh: bool = False,
    ) -> dict:
        """List configured connections, explicitly refresh one, or configure an administrator-owned native profile. Credentials are references, never secret values."""
        from urllib.parse import quote

        if not connection_id:
            return request_local_pa(ctx.settings, "GET", "/api/execution/connections")
        path = "/api/execution/connections/" + quote(connection_id, safe="")
        if profile is not None:
            return request_local_pa(
                ctx.settings,
                "PUT",
                path,
                json={"profile": profile, "expected_revision": expected_revision},
            )
        if refresh:
            return request_local_pa(ctx.settings, "POST", path + "/refresh", json={})
        return request_local_pa(ctx.settings, "GET", "/api/execution/connections")

    @mcp.tool()
    def cancel_execution_settings(
        session_id: str, idempotency_key: str, expected_version: str
    ) -> dict:
        """Cancel this exact pending setting change; never claim rollback after partial native application."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/settings/cancel",
            json={
                "idempotency_key": idempotency_key,
                "expected_version": expected_version,
            },
        )

    @mcp.tool()
    def preview_execution(
        card_id: str | None = None,
        project_id: str | None = None,
        execution_preferences: dict | None = None,
        task_assessment: dict | None = None,
    ) -> dict:
        """Preview local selection without applying it. Fleet dispatch preview evaluates remote instances jointly."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/execution/preview",
            json={
                "card_id": card_id,
                "project_id": project_id,
                "execution_preferences": execution_preferences or {},
                "task_assessment": task_assessment,
            },
        )

    @mcp.tool()
    def get_execution_decision(
        decision_id: str, offset: int = 0, limit: int = 100
    ) -> dict:
        """Read an owned immutable execution-selection receipt; runtime confirmation remains separate."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/execution/decisions/{decision_id}?offset={offset}&limit={limit}",
        )

    @mcp.tool()
    def execution_policy(
        policy: dict | None = None, expected_revision: int | None = None
    ) -> dict:
        """Read policy, or explicitly edit its versioned rules/constraints (administrator and revision required)."""
        if policy is None:
            return request_local_pa(ctx.settings, "GET", "/api/execution/policy")
        return request_local_pa(
            ctx.settings,
            "PUT",
            "/api/execution/policy",
            json={"policy": policy, "expected_revision": expected_revision},
        )

    @mcp.tool()
    def request_execution_settings(
        session_id: str,
        execution_preferences: dict,
        expected_version: str,
        idempotency_key: str,
        defer: bool = False,
    ) -> dict:
        """Request supported native settings now or durably defer to a safe turn boundary. Does not change card defaults."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/settings",
            json={
                "execution_preferences": execution_preferences,
                "expected_version": expected_version,
                "idempotency_key": idempotency_key,
                "defer": defer,
            },
        )
