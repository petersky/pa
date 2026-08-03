"""HTTP and MCP surfaces for policy-controlled collaboration modes."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pa.auth.middleware import get_principal_id, require_user
from pa.collaboration.models import (
    ExecuteCommandRequest,
    ModeTransitionRequest,
    PolicyInput,
    PolicyWrite,
)
from pa.collaboration.service import CollaborationService
from pa.collaboration.store import IdempotencyConflict
from pa.core.contracts import Module

router = APIRouter(prefix="/agent")


class CommandExecutionBody(BaseModel):
    name: str
    arguments: str | dict[str, Any] | None = None
    catalog_generation: int | None = None
    dispatch_id: str | None = None
    card_id: str | None = None
    authority_instance_id: str | None = None
    authority_version: str | None = None
    idempotency_key: str


def _service(request: Request) -> CollaborationService:
    return request.app.state.ctx.require_service("collaboration")


def _manager(request: Request):
    return request.app.state.ctx.require_service("instance_agent")


def _session_or_404(request: Request, session_id: str):
    manager = _manager(request)
    runtime = manager.get(session_id)
    session = runtime.session if runtime else manager.store.get_session(session_id)
    if not session:
        raise HTTPException(status_code=404, detail={"code": "session_not_found"})
    return session


def _require_session_access(request: Request, session: Any) -> str:
    user = require_user(request)
    principal = get_principal_id(request)
    if (
        request.app.state.ctx.settings.auth_required
        and session.principal_id
        and session.principal_id != principal
        and getattr(user, "role", None) != "admin"
        and getattr(request.state, "instance_authenticated", False) is not True
    ):
        raise HTTPException(
            status_code=403,
            detail={
                "code": "insufficient_authorization",
                "message": "This principal does not own the collaboration session.",
            },
        )
    return principal


@router.get("/collaboration/policies")
def list_collaboration_policies(request: Request) -> list[dict[str, Any]]:
    require_user(request)
    return [
        policy.model_dump(mode="json")
        for policy in _service(request).store.list_policies()
    ]


@router.put("/collaboration/policies/{policy_id}")
def put_collaboration_policy(
    request: Request, policy_id: str, body: PolicyWrite
) -> dict[str, Any]:
    user = require_user(request)
    if (
        request.app.state.ctx.settings.auth_required
        and getattr(user, "role", None) != "admin"
    ):
        raise HTTPException(status_code=403, detail={"code": "admin_required"})
    if body.policy.id != policy_id:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "policy_id_mismatch",
                "message": "Path and body policy IDs differ.",
            },
        )
    try:
        policy = _service(request).save_policy(
            body.policy, expected_version=body.expected_version
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_policy_version", "message": str(exc)},
        ) from exc
    return policy.model_dump(mode="json")


@router.post("/collaboration/policy/resolve")
def resolve_collaboration_policy(request: Request, body: PolicyInput) -> dict[str, Any]:
    require_user(request)
    decision = _service(request).resolve_dispatch_policy(body, card_id=body.card_id)
    return decision.model_dump(mode="json")


@router.get("/sessions/{session_id}/collaboration")
def get_collaboration_state(request: Request, session_id: str) -> dict[str, Any]:
    session = _session_or_404(request, session_id)
    _require_session_access(request, session)
    return _service(request).state(session).model_dump(mode="json")


@router.post("/sessions/{session_id}/collaboration/requests")
async def request_collaboration_transition(
    request: Request, session_id: str, body: ModeTransitionRequest
) -> dict[str, Any]:
    session = _session_or_404(request, session_id)
    principal = _require_session_access(request, session)
    if body.session_id != session_id:
        raise HTTPException(status_code=422, detail={"code": "session_id_mismatch"})
    if (
        body.actor == "agent"
        and principal != session.principal_id
        and getattr(request.state, "instance_authenticated", False) is not True
    ):
        raise HTTPException(
            status_code=403, detail={"code": "agent_provenance_mismatch"}
        )
    try:
        result = await _service(request).request_transition(session, body)
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_conflict", "message": str(exc)},
        ) from exc
    return result.model_dump(mode="json")


@router.get("/sessions/{session_id}/commands")
def get_session_commands(request: Request, session_id: str) -> dict[str, Any]:
    session = _session_or_404(request, session_id)
    _require_session_access(request, session)
    service = _service(request)
    catalog = service.ensure_catalog(session)
    card = (
        service.domain_store.get_card(session.card_id, realm_id=session.realm_id)
        if session.card_id
        else None
    )
    return {
        **catalog.model_dump(mode="json"),
        "state": "ready",
        "dispatch_id": session.dispatch_id,
        "card_id": session.card_id,
        "authority_instance_id": (
            session.authority_instance_id or request.app.state.ctx.settings.instance_id
        ),
        "authority_version": card.updated_at.isoformat() if card else None,
    }


@router.post("/sessions/{session_id}/commands/execute")
async def execute_session_command(
    request: Request, session_id: str, body: CommandExecutionBody
) -> dict[str, Any]:
    session = _session_or_404(request, session_id)
    principal = _require_session_access(request, session)
    command_request = ExecuteCommandRequest(
        session_id=session_id,
        name=body.name,
        arguments=body.arguments,
        catalog_generation=body.catalog_generation,
        dispatch_id=body.dispatch_id or session.dispatch_id,
        card_id=body.card_id or session.card_id,
        authority_instance_id=body.authority_instance_id,
        authority_version=body.authority_version,
        idempotency_key=body.idempotency_key,
        actor=principal,
    )
    try:
        result = await _service(request).execute_command(session, command_request)
    except IdempotencyConflict as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "idempotency_conflict", "message": str(exc)},
        ) from exc
    return result.model_dump(mode="json")


class CollaborationModule(Module):
    @property
    def name(self) -> str:
        return "collaboration"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Policy-controlled collaboration modes and durable slash commands"

    def on_load(self, ctx) -> None:
        ctx.register_service(
            "collaboration", CollaborationService(ctx.settings, ctx.store)
        )

    async def on_startup(self, app, ctx) -> None:
        service: CollaborationService = ctx.require_service("collaboration")
        manager = ctx.require_service("instance_agent")
        notifier = (
            ctx.services.get("notifications")
            or ctx.services.get("notification_service")
            or ctx.services.get("interactions")
        )
        service.bind_runtime(manager, notifier=notifier)
        manager.collaboration_service = service

    def api_routers(self):
        return [("/api", router, ["collaboration"])]

    def register_mcp(self, mcp, ctx) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def get_collaboration_mode_state(session_id: str) -> dict:
            """Inspect supported/current collaboration mode and pending policy request."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/agent/sessions/{session_id}/collaboration",
            )

        @mcp.tool()
        def request_collaboration_mode(
            requested_mode: str,
            purpose: str,
            intended_next_action: str,
            session_id: str,
            dispatch_id: str | None,
            card_id: str | None,
            authority_instance_id: str | None,
            authority_version: str | None,
            idempotency_key: str,
        ) -> dict:
            """Ask PA to evaluate and durably apply a collaboration-mode transition."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/agent/sessions/{session_id}/collaboration/requests",
                json={
                    "requested_mode": requested_mode,
                    "purpose": purpose,
                    "intended_next_action": intended_next_action,
                    "session_id": session_id,
                    "dispatch_id": dispatch_id,
                    "card_id": card_id,
                    "authority_instance_id": authority_instance_id,
                    "authority_version": authority_version,
                    "idempotency_key": idempotency_key,
                    "actor": "agent",
                },
            )

        @mcp.tool()
        def list_session_commands(session_id: str) -> dict:
            """List the normalized provider and PA slash-command catalog."""
            return request_local_pa(
                ctx.settings, "GET", f"/api/agent/sessions/{session_id}/commands"
            )

        @mcp.tool()
        def execute_agent_session_command(
            session_id: str,
            name: str,
            idempotency_key: str,
            arguments: str | None = None,
            catalog_generation: int | None = None,
            dispatch_id: str | None = None,
            card_id: str | None = None,
            authority_version: str | None = None,
            authority_instance_id: str | None = None,
        ) -> dict:
            """Execute a recognized command through PA; failures are never prompt fallbacks."""
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/agent/sessions/{session_id}/commands/execute",
                json={
                    "name": name,
                    "arguments": arguments,
                    "catalog_generation": catalog_generation,
                    "dispatch_id": dispatch_id,
                    "card_id": card_id,
                    "authority_instance_id": authority_instance_id,
                    "authority_version": authority_version,
                    "idempotency_key": idempotency_key,
                },
            )
