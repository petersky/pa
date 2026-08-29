from __future__ import annotations

import json
import math
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.auth.middleware import get_principal_id
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import Project, ProjectUpdate
from pa.pr_supervisor.github import (
    GitHubAPIError,
    GitHubClient,
    GitHubCredentials,
    verify_webhook_signature,
)
from pa.pr_supervisor.models import (
    GITHUB_TERMINAL_PR_WATCH_STATUSES,
    PR_WATCH_PROTOCOL_VERSION,
    GitHubCapability,
    LeaseGrant,
    PRPolicy,
    PRWatch,
    PRWatchEvent,
    PRWatchStatus,
)
from pa.pr_supervisor.service import (
    ProvenanceValidationError,
    PRSupervisor,
    RemoteDispatchError,
)
from pa.pr_supervisor.store import PRSupervisorStore, StaleFenceError

router = APIRouter()
ui_router = APIRouter()
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024


def _service(request: Request) -> PRSupervisor:
    return request.app.state.ctx.require_service("pr_supervisor")


def _store(request: Request) -> PRSupervisorStore:
    return request.app.state.ctx.require_service("pr_supervisor_store")


def _provenance_http_error(exc: ProvenanceValidationError) -> HTTPException:
    status = 404 if exc.code in {"watch_not_found"} else 422
    return HTTPException(status_code=status, detail=exc.http_detail())


async def _offload(request: Request, operation: str, call, *args, **kwargs):
    runtime = request.app.state.ctx.require_service("async_runtime")
    return await runtime.run_blocking(operation, call, *args, **kwargs)


def resolve_policy(
    domain_store,
    *,
    project_id: str | None,
    realm_id: str,
    repository: str,
) -> PRPolicy:
    if not project_id:
        return PRPolicy()
    project = domain_store.get_project(project_id, realm_id=realm_id)
    if not project:
        return PRPolicy()
    config = project.tool_config or {}
    base = dict(config.get("pr_policy") or {})
    per_repo = config.get("pr_repository_policies") or {}
    base.update(per_repo.get(repository) or {})
    return PRPolicy.model_validate(base)


def _record_policy_audit(
    config: dict[str, Any],
    *,
    old: dict[str, Any],
    new: dict[str, Any],
    scope: str,
    actor: str,
    instance_id: str,
) -> None:
    history = list(config.get("pr_policy_audit") or [])
    timestamp = datetime.now(UTC).isoformat()
    for field in sorted(set(old) | set(new)):
        if old.get(field) != new.get(field):
            history.append(
                {
                    "field": field,
                    "old_value": old.get(field),
                    "new_value": new.get(field),
                    "scope": scope,
                    "actor": actor,
                    "instance_id": instance_id,
                    "timestamp": timestamp,
                }
            )
    config["pr_policy_audit"] = history[-100:]


def _page_context(request: Request) -> dict[str, Any]:
    service = _service(request)
    realm = (
        request.query_params.get("realm")
        or request.app.state.ctx.settings.primary_realm
    )
    view = request.query_params.get("view", "attention")
    if view not in {"attention", "history", "all", "errors"}:
        view = "attention"
    search = request.query_params.get("q", "").strip()[:100]
    try:
        page_number = max(1, int(request.query_params.get("page", "1")))
        audit_page = max(1, int(request.query_params.get("audit_page", "1")))
    except ValueError:
        page_number = audit_page = 1
    page_size = 25
    watches, watch_total = service.store.list_watch_page(
        realm_id=realm,
        view=view,
        query_text=search,
        limit=page_size,
        offset=(page_number - 1) * page_size,
    )
    selected_id = request.query_params.get("watch")
    selected = service.store.get_watch(selected_id) if selected_id else None
    if selected and selected.realm_id != realm:
        selected = None
    current_policy = (
        resolve_policy(
            request.app.state.ctx.store,
            project_id=selected.project_id,
            realm_id=selected.realm_id,
            repository=selected.repository,
        )
        if selected
        else None
    )
    domain_store = request.app.state.ctx.store
    identities: dict[str, dict[str, str | None]] = {}
    for item in [*watches, *([selected] if selected else [])]:
        if item.id in identities:
            continue
        card = (
            domain_store.get_card(item.card_id, realm_id=item.realm_id)
            if item.card_id
            else None
        )
        project = (
            domain_store.get_project(item.project_id, realm_id=item.realm_id)
            if item.project_id
            else None
        )
        identities[item.id] = {
            "card_title": card.title if card else None,
            "project_title": project.title if project else None,
        }
    raw_events, event_total = (
        service.store.list_event_page(
            selected.id, limit=50, offset=(audit_page - 1) * 50
        )
        if selected
        else ([], 0)
    )
    event_groups: list[dict[str, Any]] = []
    for event in raw_events:
        signature = (
            event.event_type,
            event.head_sha,
            tuple(event.payload.get("reasons") or []),
        )
        if (
            event_groups
            and event.event_type == "observation"
            and event_groups[-1]["signature"] == signature
        ):
            event_groups[-1]["count"] += 1
            event_groups[-1]["oldest_at"] = event.created_at
        else:
            event_groups.append(
                {
                    "event": event,
                    "signature": signature,
                    "count": 1,
                    "newest_at": event.created_at,
                    "oldest_at": event.created_at,
                }
            )
    metrics = service.store.metrics()
    errors = metrics.get("poll_errors", 0) + metrics.get("dispatch_errors", 0)
    polls = metrics.get("polls", 0)
    operations = polls + metrics.get("executor_prompts", 0)
    health = service.authority_health()
    degradation = None
    if capability := service.capability:
        if capability.state != "ready" or not capability.authenticated:
            degradation = (
                capability.detail
                or "GitHub data fetch is unavailable on this instance."
            )
    if health.get("stopped_renewers"):
        degradation = f"{len(health['stopped_renewers'])} watch renewer(s) are stopped."
    if health.get("state") == "worker_stale":
        degradation = "The PR supervisor worker is dead or has stopped making progress."
    return {
        "watches": watches,
        "watch": selected,
        "watch_event_groups": event_groups,
        "watch_event_total": event_total,
        "audit_page": audit_page,
        "audit_pages": max(1, math.ceil(event_total / 50)),
        "watch_deliveries": service.store.list_dispatches(selected.id) if selected else [],
        "watch_policy_differs": bool(
            selected and current_policy and selected.policy != current_policy
        ),
        "watch_current_policy": current_policy,
        "capability": service.capability,
        "capabilities": service.store.list_capabilities(),
        "metrics": metrics,
        "operations": {
            "window": "since this supervisor data store was initialized",
            "errors": errors,
            "polls": polls,
            "denominator": operations,
            "error_rate": (100 * errors / operations) if operations else 0,
            "severity": "degraded" if degradation else "healthy",
        },
        "degradation": degradation,
        "supervisor_health": health,
        "watch_identities": identities,
        "view": view,
        "search": search,
        "page_number": page_number,
        "page_count": max(1, math.ceil(watch_total / page_size)),
        "watch_total": watch_total,
        "active_realm": realm,
        "realms": request.app.state.ctx.settings.subscribed_realms,
    }


@router.get("/pr-supervisor/watches")
def list_watches(
    request: Request,
    realm: str | None = None,
    card_id: str | None = None,
    include_retired: bool = False,
) -> list[dict[str, Any]]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return [
        watch.model_dump(mode="json")
        for watch in _store(request).list_watches(
            realm_id=realm_id,
            card_id=card_id,
            include_retired=include_retired,
        )
    ]


@router.post("/pr-supervisor/watches", status_code=201)
async def create_watch(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    settings = request.app.state.ctx.settings
    realm_id = str(body.get("realm_id") or settings.primary_realm)
    repository = str(body.get("repository") or "")
    if not repository:
        raise HTTPException(status_code=400, detail="repository required")
    session_id = body.get("originating_session_id")
    inferred_fields = {
        field: body.get(field)
        for field in (
            "card_id",
            "project_id",
            "repository_id",
            "dispatch_id",
            "originating_instance_id",
            "authority_instance_id",
            "originating_principal_id",
        )
        if body.get(field) is not None
    }
    if inferred_fields and not session_id:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "originating_session_required",
                "message": "Linked provenance must be resolved from a canonical session.",
                "fields": sorted(inferred_fields),
            },
        )
    policy = body.get("policy")
    if not policy and not session_id:
        resolved_policy = await _offload(
            request,
            "sqlite.pr_policy_read",
            resolve_policy,
            request.app.state.ctx.store,
            project_id=body.get("project_id"),
            realm_id=realm_id,
            repository=repository,
        )
        policy = resolved_policy.model_dump(mode="json")
    if session_id:
        # Session-backed registration freezes policy after canonical provenance.
        policy = PRPolicy().model_dump(mode="json")
    try:
        watch = PRWatch(
            realm_id=realm_id,
            project_id=body.get("project_id"),
            card_id=body.get("card_id"),
            repository_id=body.get("repository_id"),
            dispatch_id=body.get("dispatch_id"),
            repository=repository,
            pr_number=int(body.get("pr_number") or 0),
            pr_url=str(
                body.get("pr_url")
                or f"https://github.com/{repository}/pull/{body.get('pr_number')}"
            ),
            base_branch=body.get("base_branch"),
            head_sha=body.get("head_sha"),
            originating_instance_id=body.get("originating_instance_id"),
            authority_instance_id=body.get("authority_instance_id"),
            originating_session_id=session_id,
            originating_principal_id=body.get("originating_principal_id"),
            creation_reason=str(
                body.get("creation_reason") or "api_explicit_watch_operation"
            ),
            qualifying_evidence=str(
                body.get("qualifying_evidence")
                or f"POST /api/pr-supervisor/watches for {repository}#{body.get('pr_number')}"
            ),
            policy=policy,
            required_capabilities=body.get("required_capabilities") or [],
        )
        service = _service(request)
        stored = (
            await service.register_watch_from_session(
                watch, source=f"api:{get_principal_id(request)}"
            )
            if session_id
            else await service.register_watch(
                watch, source=f"api:{get_principal_id(request)}"
            )
        )
    except ProvenanceValidationError as exc:
        raise _provenance_http_error(exc) from exc
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    return stored.model_dump(mode="json")


@router.get("/pr-supervisor/watches/{watch_id}")
def get_watch(request: Request, watch_id: str) -> dict[str, Any]:
    watch = _store(request).get_watch(watch_id)
    if not watch:
        raise HTTPException(status_code=404, detail="PR watch not found")
    return {
        "watch": watch.model_dump(mode="json"),
        "events": [
            event.model_dump(mode="json")
            for event in _store(request).list_events(watch_id)
        ],
        "notifications": _store(request).list_dispatches(watch_id),
    }


@router.get("/pr-supervisor/provenance/issues")
async def provenance_issues(
    request: Request,
    realm: str | None = None,
    include_retired: bool = True,
) -> dict[str, Any]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    issues = await _service(request).provenance_diagnostics(
        realm_id=realm_id, include_retired=include_retired
    )
    return {"realm_id": realm_id, "count": len(issues), "issues": issues}


@router.post("/pr-supervisor/watches/{watch_id}/provenance/repair")
async def repair_provenance(
    request: Request, watch_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    try:
        repaired = await _service(request).repair_watch_provenance(
            watch_id,
            originating_session_id=str(body.get("originating_session_id") or ""),
            idempotency_key=str(body.get("idempotency_key") or ""),
            actor=get_principal_id(request),
        )
    except ProvenanceValidationError as exc:
        raise _provenance_http_error(exc) from exc
    return repaired.model_dump(mode="json")


@router.post("/pr-supervisor/watches/{watch_id}/refresh")
async def refresh_watch(request: Request, watch_id: str) -> dict[str, Any]:
    watch = await _service(request).refresh_watch(watch_id)
    if not watch:
        raise HTTPException(status_code=404, detail="Active PR watch not found")
    return {"scheduled": True, "watch_id": watch_id}


@router.delete("/pr-supervisor/watches/{watch_id}")
async def retire_watch(request: Request, watch_id: str) -> dict[str, Any]:
    watch = await _service(request).retire_watch(watch_id)
    if not watch:
        raise HTTPException(status_code=404, detail="PR watch not found")
    return watch.model_dump(mode="json")


@router.post("/pr-supervisor/migrations/terminal-retirements")
async def backfill_terminal_retirements(
    request: Request, body: dict[str, Any]
) -> dict[str, Any]:
    realm_id = str(body.get("realm_id") or request.app.state.ctx.settings.primary_realm)
    return await _service(request).backfill_terminal_retirements(
        realm_id=realm_id,
        dry_run=bool(body.get("dry_run", False)),
    )


@router.post("/pr-supervisor/pull-requests", status_code=201)
async def create_pull_request(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    service = _service(request)
    settings = request.app.state.ctx.settings
    repository = str(body.get("repository") or "")
    realm_id = str(body.get("realm_id") or settings.primary_realm)
    session_id = body.get("originating_session_id")
    if not repository or not body.get("title") or not body.get("head"):
        raise HTTPException(
            status_code=400, detail="repository, title, and head are required"
        )
    linked_fields = {
        field: body.get(field)
        for field in (
            "card_id",
            "project_id",
            "repository_id",
            "dispatch_id",
            "authority_instance_id",
        )
        if body.get(field) is not None
    }
    if linked_fields and not session_id:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "originating_session_required",
                "message": "Linked provenance must be resolved before creating the pull request.",
                "fields": sorted(linked_fields),
            },
        )
    policy = PRPolicy()
    provisional = PRWatch(
        realm_id=realm_id,
        project_id=body.get("project_id"),
        card_id=body.get("card_id"),
        repository_id=body.get("repository_id"),
        dispatch_id=body.get("dispatch_id"),
        authority_instance_id=body.get("authority_instance_id"),
        repository=repository,
        pr_number=1,
        pr_url=f"https://github.com/{repository}/pull/pending",
        originating_session_id=session_id,
        creation_reason="pull_request_created_for_integration",
        qualifying_evidence=(
            f"POST /api/pr-supervisor/pull-requests head={body.get('head')} "
            f"base={body.get('base') or policy.integration_branch or 'main'}"
        ),
        policy=policy,
    )
    if session_id:
        try:
            provisional = await service.resolve_session_provenance(provisional)
            provisional = await service.freeze_canonical_policy(provisional)
            policy = provisional.policy
        except ProvenanceValidationError as exc:
            raise _provenance_http_error(exc) from exc
    else:
        policy = await _offload(
            request,
            "sqlite.pr_policy_read",
            resolve_policy,
            request.app.state.ctx.store,
            project_id=None,
            realm_id=realm_id,
            repository=repository,
        )
        provisional.policy = policy
    try:
        pr = await service.github.create_pull_request(
            repository,
            title=str(body["title"]),
            head=str(body["head"]),
            base=str(body.get("base") or policy.integration_branch or "main"),
            body=str(body.get("body") or ""),
            draft=body.get("draft"),
            policy=policy,
        )
    except GitHubAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    provisional.pr_number = int(pr["number"])
    provisional.pr_url = str(pr.get("html_url") or "")
    provisional.base_branch = str(
        (pr.get("base") or {}).get("ref") or body.get("base") or "main"
    )
    provisional.head_sha = str((pr.get("head") or {}).get("sha") or "") or None
    watch = await service.register_watch(
        provisional, source=f"pull_request_create:{get_principal_id(request)}"
    )
    return {
        "pull_request": {
            "number": pr["number"],
            "url": pr.get("html_url"),
            "draft": bool(pr.get("draft")),
        },
        "watch": watch.model_dump(mode="json"),
    }


@router.get("/pr-supervisor/capabilities")
def capabilities(request: Request) -> dict[str, Any]:
    service = _service(request)
    instances = _store(request).list_capabilities()
    if not any(
        item.instance_id == service.capability.instance_id for item in instances
    ):
        instances.insert(0, service.capability)
    return {
        "local": service.capability.model_dump(mode="json"),
        "instances": [item.model_dump(mode="json") for item in instances],
    }


@router.get("/pr-supervisor/metrics")
def metrics(request: Request) -> dict[str, int]:
    return _store(request).metrics()


@router.get("/pr-supervisor/health")
def supervisor_health(request: Request) -> dict[str, Any]:
    return _service(request).authority_health()


@router.put("/pr-supervisor/policies/projects/{project_id}")
def update_policy(
    request: Request,
    project_id: str,
    body: dict[str, Any],
    realm: str | None = None,
) -> dict[str, Any]:
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    project = request.app.state.ctx.store.get_project(project_id, realm_id=realm_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    policy = PRPolicy.model_validate(body.get("policy") or body)
    config = dict(project.tool_config or {})
    repository = body.get("repository")
    if repository:
        policies = dict(config.get("pr_repository_policies") or {})
        old_policy = dict(policies.get(str(repository)) or {})
        policies[str(repository)] = policy.model_dump(mode="json")
        config["pr_repository_policies"] = policies
    else:
        old_policy = dict(config.get("pr_policy") or {})
        config["pr_policy"] = policy.model_dump(mode="json")
    _record_policy_audit(
        config,
        old=old_policy,
        new=policy.model_dump(mode="json"),
        scope=f"repository:{repository}" if repository else "project",
        actor=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    updated = request.app.state.ctx.store.update_project(
        project_id,
        ProjectUpdate(tool_config=config),
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    return {
        "project_id": project_id,
        "repository": repository,
        "policy": policy.model_dump(mode="json"),
        "tool_config": updated.tool_config if updated else config,
    }


# Fleet-internal replica, authority, and dispatch routes accept the PA sync token
# through AuthMiddleware's instance-route allowlist.
@router.post("/pr-supervisor/replicas")
async def ingest_replica(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    watch = PRWatch.model_validate(body.get("watch") or body)
    try:
        watch = await _service(request).validate_replica_provenance(watch)
    except ProvenanceValidationError as exc:
        raise _provenance_http_error(exc) from exc
    stored = _store(request).upsert_watch(watch, preserve_lease=True)
    _service(request).watch_state_changed(stored)
    return stored.model_dump(mode="json")


@router.post("/pr-supervisor/retirements")
async def ingest_retirement(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    incoming = PRWatch.model_validate(body.get("watch") or {})
    try:
        incoming = await _service(request).validate_replica_provenance(incoming)
    except ProvenanceValidationError as exc:
        raise _provenance_http_error(exc) from exc
    store = _store(request)
    existing = store.find_watch(
        incoming.realm_id, incoming.repository, incoming.pr_number
    )
    if existing:
        desired_status = (
            incoming.status
            if incoming.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            else PRWatchStatus.RETIRED
        )
        state = (
            existing.state
            if existing.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            else incoming.state or existing.state
        )
        retirement = incoming.state.get("retirement")
        reason = (
            str(retirement.get("reason"))
            if isinstance(retirement, dict) and retirement.get("reason")
            else "fleet_retirement_transition"
        )
        retired = store.set_terminal(
            existing.id,
            desired_status,
            state=state,
            retirement_reason=reason,
            retired_at=incoming.retired_at,
        )
    else:
        desired_status = (
            incoming.status
            if incoming.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
            else PRWatchStatus.RETIRED
        )
        stored = store.upsert_watch(incoming, preserve_lease=False)
        retired = store.set_terminal(
            stored.id,
            desired_status,
            state=stored.state,
            retirement_reason="fleet_retirement_transition",
            retired_at=incoming.retired_at,
        )
    event_type = (
        "watch_archived"
        if retired.status in GITHUB_TERMINAL_PR_WATCH_STATUSES
        else "watch_retired"
    )
    event_key = str(body.get("event_key") or f"{retired.id}:retired")
    store.append_event(
        PRWatchEvent(
            watch_id=retired.id,
            event_key=event_key,
            event_type=event_type,
            source="fleet_transition",
        )
    )
    _service(request).watch_state_changed(retired)
    return retired.model_dump(mode="json")


@router.post("/pr-supervisor/watches/{watch_id}/lease")
async def acquire_lease(
    request: Request, watch_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    instance_id = str(body.get("instance_id") or "")
    capability = GitHubCapability.model_validate(body.get("capability") or {})
    caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
    if caller != instance_id or capability.instance_id != instance_id:
        _store(request).save_capability(capability)
        return LeaseGrant(
            acquired=False,
            reason="capability_identity_mismatch",
            protocol_version=PR_WATCH_PROTOCOL_VERSION,
        ).model_dump(mode="json")
    if capability.pr_watch_protocol_version < PR_WATCH_PROTOCOL_VERSION:
        _store(request).save_capability(capability)
        return LeaseGrant(
            acquired=False,
            reason="protocol_upgrade_required",
            protocol_version=PR_WATCH_PROTOCOL_VERSION,
        ).model_dump(mode="json")
    if not capability.authenticated:
        _store(request).save_capability(capability)
        return LeaseGrant(
            acquired=False,
            reason="capability_ineligible",
            protocol_version=PR_WATCH_PROTOCOL_VERSION,
        ).model_dump(mode="json")
    canonical_id = watch_id
    if body.get("watch"):
        incoming = PRWatch.model_validate(body["watch"])
        try:
            incoming = await _service(request).validate_replica_provenance(incoming)
        except ProvenanceValidationError as exc:
            raise _provenance_http_error(exc) from exc
        if not capability.supports(incoming.repository):
            _store(request).save_capability(capability)
            return LeaseGrant(
                acquired=False,
                reason="capability_ineligible",
                protocol_version=PR_WATCH_PROTOCOL_VERSION,
            ).model_dump(mode="json")
        store = _store(request)
        existing = store.find_watch(
            incoming.realm_id, incoming.repository, incoming.pr_number
        )
        if (
            existing is None
            or (incoming.fence_token, incoming.lease_version)
            > (existing.fence_token, existing.lease_version)
            or (incoming.terminal and not existing.terminal)
        ):
            stored = store.upsert_watch(incoming, preserve_lease=True)
        else:
            stored = existing
        _service(request).watch_state_changed(stored)
        canonical_id = stored.id
    grant = _store(request).try_acquire_lease(
        canonical_id,
        instance_id,
        ttl_seconds=min(max(int(body.get("ttl_seconds") or 45), 10), 300),
        renewal_window_seconds=min(
            max(int(body.get("renewal_window_seconds") or 12), 1), 60
        ),
        capability=capability,
    )
    if grant.reason != "lease_valid":
        _store(request).save_capability(capability)
    return grant.model_dump(mode="json")


@router.post("/pr-supervisor/instances/heartbeat")
def heartbeat(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    capability = GitHubCapability.model_validate(body)
    _store(request).save_capability(capability)
    return {"accepted": True}


@router.post("/pr-supervisor/effects/dispatch")
async def dispatch_authorized_effect(
    request: Request, body: dict[str, Any]
) -> dict[str, Any]:
    caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
    if not caller:
        raise HTTPException(
            status_code=401,
            detail={"code": "effect_origin_required"},
        )
    try:
        state = await _service(request).authorize_and_dispatch_effect(
            body, caller_instance_id=caller
        )
    except RemoteDispatchError as exc:
        headers = {}
        if exc.retry_after_seconds is not None:
            headers["Retry-After"] = str(exc.retry_after_seconds)
        raise HTTPException(
            status_code=exc.status_code or 503,
            detail=exc.audit_detail(),
            headers=headers,
        ) from exc
    except StaleFenceError as exc:
        raise HTTPException(
            status_code=409,
            detail={"code": "stale_effect_authorization", "message": str(exc)},
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=503,
            detail={"code": str(exc)},
        ) from exc
    return {"state": state}


@router.post("/pr-supervisor/dispatch")
async def dispatch_executor(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    authorization = body.get("authorization")
    if not isinstance(authorization, dict):
        raise HTTPException(
            status_code=428,
            detail={"code": "pr_watch_effect_authorization_required"},
        )
    if authorization.get("protocol_version") != PR_WATCH_PROTOCOL_VERSION:
        raise HTTPException(
            status_code=426,
            detail={"code": "pr_watch_effect_upgrade_required"},
        )
    caller = request.headers.get("X-PA-Origin-Instance-ID", "").strip()
    if caller != str(authorization.get("issuer_instance_id") or ""):
        raise HTTPException(
            status_code=401,
            detail={"code": "effect_issuer_mismatch"},
        )
    service = _service(request)
    watch = PRWatch.model_validate(body.get("watch") or {})
    try:
        watch = await service.validate_replica_provenance(watch)
    except ProvenanceValidationError as exc:
        raise _provenance_http_error(exc) from exc
    await service._offload(
        "sqlite.pr_supervisor_watch_write", service.store.upsert_watch, watch
    )
    event_key = str(body.get("event_key") or "")
    prompt = str(body.get("prompt") or "")
    if not event_key or not prompt:
        raise HTTPException(status_code=400, detail="event_key and prompt required")
    prompt_audit = body.get("prompt_audit") or []
    if not isinstance(prompt_audit, list):
        raise HTTPException(status_code=400, detail="prompt_audit must be a list")
    state = await service.dispatcher.dispatch_local(
        watch,
        event_key,
        prompt,
        prompt_audit=prompt_audit,
        authorization=authorization,
    )
    return {"state": state}


@router.post("/pr-supervisor/webhook/github")
async def github_webhook(request: Request) -> dict[str, Any]:
    body = await request.body()
    if len(body) > MAX_WEBHOOK_BYTES:
        raise HTTPException(status_code=413, detail="Webhook payload too large")
    service = _service(request)
    signature = request.headers.get("x-hub-signature-256")
    verified = await _offload(
        request,
        "pr_supervisor.webhook_verify",
        verify_webhook_signature,
        body,
        service.credentials.webhook_secret,
        signature,
    )
    if not verified:
        raise HTTPException(status_code=401, detail="Invalid webhook signature")
    try:
        payload = await _offload(
            request, "pr_supervisor.webhook_json", json.loads, body
        )
    except json.JSONDecodeError as exc:
        raise HTTPException(status_code=400, detail="Invalid JSON") from exc
    count = await service.handle_webhook(
        request.headers.get("x-github-event", ""),
        request.headers.get("x-github-delivery", str(uuid4())),
        payload,
    )
    return {"accepted": True, "scheduled_watches": count}


@ui_router.get("/pull-requests", response_class=HTMLResponse)
def pull_requests_page(request: Request):
    from pa.modules.ui_shell import render_page

    page = request.app.state.ctx.require_service("pages").get_by_path("/pull-requests")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


@ui_router.post("/partials/projects/{project_id}/pr-policy", response_model=None)
def update_project_policy_ui(
    request: Request,
    project_id: str,
    ready_by_default: str | None = Form(None),
    auto_notify: str | None = Form(None),
    agent_merge_on_green: str | None = Form(None),
    repair_failed_checks: str | None = Form(None),
    required_checks: str = Form(""),
    realm: str | None = Form(None),
) -> HTMLResponse:
    from pa.modules.ui_shell import render_page

    realm_id = realm or request.app.state.ctx.settings.primary_realm
    project = request.app.state.ctx.store.get_project(project_id, realm_id=realm_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    config = dict(project.tool_config or {})
    policy_data = dict(config.get("pr_policy") or {})
    policy_data.update(
        {
            "ready_by_default": ready_by_default is not None,
            "auto_notify": auto_notify is not None,
            "agent_merge_on_green": agent_merge_on_green is not None,
            "repair_failed_checks": repair_failed_checks is not None,
            "required_checks": [
                item.strip() for item in required_checks.split(",") if item.strip()
            ],
        }
    )
    policy = PRPolicy.model_validate(policy_data)
    old_policy = dict(config.get("pr_policy") or {})
    config["pr_policy"] = policy.model_dump(mode="json")
    _record_policy_audit(
        config,
        old=old_policy,
        new=policy.model_dump(mode="json"),
        scope="project",
        actor=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    request.app.state.ctx.store.update_project(
        project_id,
        ProjectUpdate(tool_config=config),
        realm_id=realm_id,
        principal_id=get_principal_id(request),
        instance_id=request.app.state.ctx.settings.instance_id,
    )
    page = request.app.state.ctx.require_service("pages").get_by_path("/projects")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


class PRSupervisorModule(Module):
    @property
    def name(self) -> str:
        return "pr-supervisor"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Durable fleet-wide GitHub pull-request lifecycle supervision"

    def on_load(self, ctx: AppContext) -> None:
        store = PRSupervisorStore(ctx.settings.data_dir / "pr_supervisor.db")
        ctx.register_service("pr_supervisor_store", store)
        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="pull-requests",
                path="/pull-requests",
                label="Pull requests",
                icon="pull-requests",
                template="pages/pr-supervisor.html",
                nav_order=18,
                context_builder=_page_context,
            )
        )

    async def on_startup(self, app, ctx: AppContext) -> None:
        async_runtime = ctx.require_service("async_runtime")
        credentials = await async_runtime.run_blocking(
            "filesystem.github_credentials_read",
            GitHubCredentials.load,
            ctx.settings.data_dir,
        )
        service = PRSupervisor(
            ctx.settings,
            ctx.store,
            supervisor_store=ctx.require_service("pr_supervisor_store"),
            github_client=GitHubClient(
                credentials,
                async_runtime=async_runtime,
            ),
            agent_manager=ctx.services.get("instance_agent"),
            workspace_manager=getattr(
                ctx.services.get("instance_agent"), "workspace_manager", None
            ),
            dispatch_store=ctx.services.get("dispatch_store"),
            fleet_registry=ctx.services.get("fleet_registry"),
            peer_table=ctx.services.get("peer_table"),
            async_runtime=async_runtime,
        )
        ctx.register_service("pr_supervisor", service)
        await service.start()

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        service = ctx.services.get("pr_supervisor")
        if service:
            await service.stop()

    def api_routers(self):
        return [("/api", router, ["pr-supervisor"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        async_runtime = ctx.require_service("async_runtime")

        @mcp.tool()
        def list_pr_watches(
            realm: str = "default",
            card_id: str | None = None,
            include_retired: bool = False,
        ) -> list[dict[str, Any]]:
            """List durable PR watches and their current lifecycle state."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/pr-supervisor/watches",
                params={
                    "realm": realm,
                    "card_id": card_id,
                    "include_retired": include_retired,
                },
            )

        @mcp.tool()
        def get_pr_watch(watch_id: str) -> dict[str, Any] | None:
            """Get a PR watch and its audit history."""
            return request_local_pa(
                ctx.settings,
                "GET",
                f"/api/pr-supervisor/watches/{watch_id}",
                allow_not_found=True,
            )

        @mcp.tool()
        async def create_pr_watch(
            repository: str,
            pr_number: int,
            pr_url: str,
            realm: str = "default",
            project_id: str | None = None,
            card_id: str | None = None,
            originating_session_id: str | None = None,
            originating_agent: str | None = None,
            executor_cwd: str | None = None,
        ) -> dict[str, Any]:
            """Create a durable, fleet-supervised PR watch."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_create_http",
                request_local_pa,
                ctx.settings,
                "POST",
                "/api/pr-supervisor/watches",
                json={
                    "realm_id": realm,
                    "project_id": project_id,
                    "card_id": card_id,
                    "repository": repository,
                    "pr_number": pr_number,
                    "pr_url": pr_url,
                    "originating_session_id": originating_session_id,
                    "originating_agent": originating_agent,
                    "executor_cwd": executor_cwd,
                },
            )

        @mcp.tool()
        async def refresh_pr_watch(watch_id: str) -> dict[str, Any]:
            """Schedule an immediate refresh for an active PR watch."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_refresh_http",
                request_local_pa,
                ctx.settings,
                "POST",
                f"/api/pr-supervisor/watches/{watch_id}/refresh",
            )

        @mcp.tool()
        async def retire_pr_watch(watch_id: str) -> dict[str, Any] | None:
            """Retire a PR watch without deleting its audit history."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_retire_http",
                request_local_pa,
                ctx.settings,
                "DELETE",
                f"/api/pr-supervisor/watches/{watch_id}",
                allow_not_found=True,
            )

        @mcp.tool()
        async def backfill_terminal_pr_watches(
            realm: str = "default",
            dry_run: bool = False,
        ) -> dict[str, Any]:
            """Revalidate and archive legacy merged/closed watches idempotently."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_terminal_backfill_http",
                request_local_pa,
                ctx.settings,
                "POST",
                "/api/pr-supervisor/migrations/terminal-retirements",
                json={"realm_id": realm, "dry_run": dry_run},
            )

        @mcp.tool()
        async def create_supervised_pull_request(
            repository: str,
            title: str,
            head: str,
            base: str = "main",
            body: str = "",
            realm: str = "default",
            project_id: str | None = None,
            card_id: str | None = None,
            originating_session_id: str | None = None,
            executor_cwd: str | None = None,
            draft: bool | None = None,
        ) -> dict[str, Any]:
            """Open a PR ready for review by policy and immediately supervise it."""
            return await async_runtime.run_blocking(
                "mcp.pr_create_http",
                request_local_pa,
                ctx.settings,
                "POST",
                "/api/pr-supervisor/pull-requests",
                json={
                    "repository": repository,
                    "title": title,
                    "head": head,
                    "base": base,
                    "body": body,
                    "realm_id": realm,
                    "project_id": project_id,
                    "card_id": card_id,
                    "originating_session_id": originating_session_id,
                    "executor_cwd": executor_cwd,
                    "draft": draft,
                },
            )

        @mcp.tool()
        def set_project_pr_policy(
            project_id: str,
            realm: str = "default",
            repository: str | None = None,
            ready_by_default: bool = True,
            auto_notify: bool = True,
            agent_merge_on_green: bool = True,
            repair_failed_checks: bool = True,
            required_checks: list[str] | None = None,
        ) -> dict[str, Any] | None:
            """Set project-wide or repository-specific PR supervision policy."""
            project_data = request_local_pa(
                ctx.settings,
                "GET",
                f"/api/projects/{project_id}",
                params={"realm": realm},
                allow_not_found=True,
            )
            project = Project.model_validate(project_data) if project_data else None
            if not project:
                return None
            config = dict(project.tool_config or {})
            if repository:
                policies = dict(config.get("pr_repository_policies") or {})
                policy_data = dict(
                    policies.get(repository) or config.get("pr_policy") or {}
                )
                policy_data.update(
                    {
                        "ready_by_default": ready_by_default,
                        "auto_notify": auto_notify,
                        "agent_merge_on_green": agent_merge_on_green,
                        "repair_failed_checks": repair_failed_checks,
                        "required_checks": (
                            required_checks
                            if required_checks is not None
                            else policy_data.get("required_checks", [])
                        ),
                    }
                )
                policy = PRPolicy.model_validate(policy_data)
                policies[repository] = policy.model_dump(mode="json")
                config["pr_repository_policies"] = policies
            else:
                policy_data = dict(config.get("pr_policy") or {})
                policy_data.update(
                    {
                        "ready_by_default": ready_by_default,
                        "auto_notify": auto_notify,
                        "agent_merge_on_green": agent_merge_on_green,
                        "repair_failed_checks": repair_failed_checks,
                        "required_checks": (
                            required_checks
                            if required_checks is not None
                            else policy_data.get("required_checks", [])
                        ),
                    }
                )
                policy = PRPolicy.model_validate(policy_data)
                config["pr_policy"] = policy.model_dump(mode="json")
            updated = request_local_pa(
                ctx.settings,
                "PUT",
                f"/api/pr-supervisor/policies/projects/{project_id}",
                params={"realm": realm},
                json={
                    "repository": repository,
                    "policy": policy.model_dump(mode="json"),
                },
            )
            return {
                "project_id": project_id,
                "repository": repository,
                "policy": policy.model_dump(mode="json"),
                "tool_config": updated.get("tool_config", config)
                if updated
                else config,
            }

        @mcp.tool()
        async def diagnose_pr_watch_provenance(
            realm: str = "default", include_retired: bool = True
        ) -> dict[str, Any]:
            """Detect malformed, shortened, missing, or mismatched watch provenance."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_provenance_diagnostics_http",
                request_local_pa,
                ctx.settings,
                "GET",
                "/api/pr-supervisor/provenance/issues",
                params={"realm": realm, "include_retired": include_retired},
            )

        @mcp.tool()
        async def repair_pr_watch_provenance(
            watch_id: str,
            originating_session_id: str,
            idempotency_key: str,
        ) -> dict[str, Any]:
            """Audited relink of a corrupt watch to one explicit canonical session."""
            return await async_runtime.run_blocking(
                "mcp.pr_watch_provenance_repair_http",
                request_local_pa,
                ctx.settings,
                "POST",
                f"/api/pr-supervisor/watches/{watch_id}/provenance/repair",
                json={
                    "originating_session_id": originating_session_id,
                    "idempotency_key": idempotency_key,
                },
            )

        @mcp.tool()
        def github_integration_capability() -> dict[str, Any]:
            """Report local GitHub authentication/webhook capability without secrets."""
            capabilities = request_local_pa(
                ctx.settings,
                "GET",
                "/api/pr-supervisor/capabilities",
            )
            return capabilities["local"]
