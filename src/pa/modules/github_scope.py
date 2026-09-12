"""Authenticated repository-supervision settings (never a credential API)."""
from __future__ import annotations

import hashlib
import json

import httpx
from fastapi import APIRouter, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.routing import APIRoute
from pydantic import BaseModel, ConfigDict, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.pr_supervisor import scope
from pa.pr_supervisor.github import GitHubAPIError, GitHubClient, GitHubCredentials

class ScopeRoute(APIRoute):
    def get_route_handler(self):
        handler = super().get_route_handler()

        async def redacted_handler(request: Request):
            # auth_required=False permits PA's local UI user, but must never
            # turn an explicitly supplied fleet/invalid bearer into an operator.
            if (request.headers.get("authorization") or getattr(request.state, "instance_authenticated", False)) and not getattr(request.state, "user_authenticated", False):
                raise HTTPException(403, {"code": "scope_user_required", "message": "Use a PA user session or user API credential for repository scope settings."})
            try:
                return await handler(request)
            except RequestValidationError:
                # FastAPI normally echoes invalid input, including unexpected
                # credential fields. This settings API must never do that.
                raise HTTPException(422, {"code": "invalid_scope_request",
                    "message": "Supply only repository scope, revision, idempotency and confirmation fields."}) from None
        return redacted_handler


router = APIRouter(prefix="/supervision-scope", route_class=ScopeRoute)


class ScopePreview(BaseModel):
    model_config = ConfigDict(extra="forbid")
    allowed_repositories: list[str] = Field(min_length=1, max_length=200)
    expected_revision: str = Field(min_length=1, max_length=100)


class ScopeUpdate(ScopePreview):
    idempotency_key: str = Field(min_length=1, max_length=200)
    confirmed_additions: list[str] = Field(default_factory=list, max_length=200)
    confirmation_id: str = Field(default="", max_length=200)


async def _run(request: Request, call, *args, **kwargs):
    try:
        return await request.app.state.ctx.require_service("async_runtime").run_blocking(
            "github.supervision_scope", call, *args, **kwargs)
    except scope.ScopeError as exc:
        raise HTTPException(exc.status, {"code": exc.code, "message": str(exc)}) from None


async def _validate(request: Request, repositories: list[str], credentials: GitHubCredentials) -> list[dict]:
    if not credentials.token:
        raise HTTPException(401, {"code": "github_unauthenticated", "message": "Configure GitHub credentials on this instance first."})
    ctx = request.app.state.ctx
    client = GitHubClient(credentials, client=ctx.require_service("pr_supervisor").http_client,
                          async_runtime=ctx.require_service("async_runtime"))
    rows = []
    try:
        for repository in repositories:
            _, row = await client._request("GET", f"/repos/{repository}", operation="scope repository access")
            if scope.normalize_repositories([row.get("full_name", "")]) != [repository]:
                raise HTTPException(409, {"code": "repository_identity_changed", "message": "Repository identity changed; use its current GitHub owner/name and preview again."})
            rows.append({"repository": repository, "private": bool(row.get("private", True))})
    except (GitHubAPIError, httpx.HTTPError, ValueError, TypeError, AttributeError):
        # Provider errors may contain credentials, URLs or private response text.
        raise HTTPException(422, {"code": "repository_access_failed", "message": "The existing GitHub credential could not validate every candidate repository. Check repository access and retry."}) from None
    return rows


@router.get("")
async def read_scope(request: Request) -> dict:
    require_user(request)
    return {**await _run(request, scope.snapshot, request.app.state.ctx.settings.data_dir),
            "instance_id": request.app.state.ctx.settings.instance_id,
            "instance_name": request.app.state.ctx.settings.instance_name,
            "published_capability": _publication(request)}


def _publication(request: Request) -> dict:
    service = request.app.state.ctx.require_service("pr_supervisor")
    capability = service.capability
    return {"policy_revision": capability.policy_revision,
            "observed_at": capability.checked_at.isoformat(),
            "state": "publication_pending" if service._authority_last_error else capability.state}


@router.get("/comparison")
async def comparison(request: Request) -> dict:
    require_user(request)
    ctx = request.app.state.ctx
    report = await ctx.require_service("pr_supervisor")._eligible_capabilities(None)
    return {**report.model_dump(mode="json"), "current_instance_id": ctx.settings.instance_id,
            "current_instance_name": ctx.settings.instance_name,
            "inventory_kind": "authority_received_advertisements",
            "limit": 200, "message": report.summary() if report.evaluation_state == "unavailable" else None}


@router.get("/audit")
async def read_audit(request: Request) -> dict:
    require_user(request)
    return {"events": await _run(request, scope.audit, request.app.state.ctx.settings.data_dir)}


@router.post("/preview")
async def preview_scope(request: Request, body: ScopePreview) -> dict:
    require_user(request)
    ctx = request.app.state.ctx
    current = await read_scope(request)
    if body.expected_revision != current["revision"]:
        raise HTTPException(409, {"code": "scope_revision_conflict", "message": "GitHub scope changed; refresh and preview again."})
    repositories = await _run(request, scope.normalize_repositories, body.allowed_repositories)
    credentials = await _run(request, GitHubCredentials.load, ctx.settings.data_dir)
    validated = await _validate(request, repositories, credentials)
    additions = sorted(set(repositories) - set(current["allowed_repositories"]))
    removals = sorted(set(current["allowed_repositories"]) - set(repositories))
    change = {"instance_id": ctx.settings.instance_id, "expected_revision": current["revision"],
              "allowed_repositories": repositories, "confirmed_additions": additions}
    request_id = "github-scope-" + hashlib.sha256(json.dumps(change, sort_keys=True).encode()).hexdigest()[:32]
    change["confirmation_id"] = request_id
    return {"current": current, "candidate": repositories, "additions": additions,
            "removals": removals, "validated_repositories": validated,
            "operator_input": {"schema_version": 1, "request_id": request_id,
                "prompt": f"Set GitHub supervision on {ctx.settings.instance_name} to exactly {', '.join(repositories)}?",
                "details": "Private repositories may be included. Existing credentials are preserved. This changes only this instance's explicit supervision allowlist.",
                "choices": [
                    {"id": "apply_scope", "label": "Apply exact scope", "description": "Save the reviewed repository list and refresh capability.", "value": change},
                    {"id": "keep_scope", "label": "Keep current scope", "description": "Leave supervision scope unchanged.", "value": "keep_scope"}],
                "allow_freeform": False, "allow_cancel": True}}


@router.put("")
async def update_scope(request: Request, body: ScopeUpdate) -> dict:
    require_user(request)
    ctx = request.app.state.ctx
    actor = get_principal_id(request)
    prepared, original, credentials, fingerprint = await _run(
        request, scope.prepare, ctx.settings.data_dir,
        repositories=body.allowed_repositories, expected_revision=body.expected_revision,
        actor=actor, idempotency_key=body.idempotency_key,
        confirmed_additions=body.confirmed_additions, confirmation_id=body.confirmation_id)
    if prepared.get("duplicate"):
        result = prepared
    else:
        await _validate(request, prepared["candidate"], credentials)
        result = await _run(request, scope.commit, ctx.settings.data_dir,
            original=original, credentials=credentials, repositories=prepared["candidate"],
            actor=actor, instance_id=ctx.settings.instance_id, idempotency_key=body.idempotency_key,
            fingerprint=fingerprint, confirmation_id=body.confirmation_id)
    # Replay also repairs a crash/failure between the durable save and refresh.
    try:
        capability = await ctx.require_service("pr_supervisor").refresh_capability(force=True)
        refresh = {"state": ("refresh_pending" if ctx.require_service("pr_supervisor")._authority_last_error
                             else capability.state), "authenticated": capability.authenticated,
                   "policy_revision": capability.policy_revision}
    except Exception:
        refresh = {"state": "refresh_pending", "authenticated": False}
    return {**result, "instance_id": ctx.settings.instance_id, "capability_refresh": refresh}
