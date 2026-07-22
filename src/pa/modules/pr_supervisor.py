from __future__ import annotations

import json
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.models import Project, ProjectUpdate
from pa.pr_supervisor.github import (
    GitHubAPIError,
    GitHubClient,
    GitHubCredentials,
    verify_webhook_signature,
)
from pa.pr_supervisor.models import (
    GitHubCapability,
    PRPolicy,
    PRWatch,
    PRWatchEvent,
    PRWatchStatus,
)
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore

router = APIRouter()
ui_router = APIRouter()
MAX_WEBHOOK_BYTES = 2 * 1024 * 1024


def _service(request: Request) -> PRSupervisor:
    return request.app.state.ctx.require_service("pr_supervisor")


def _store(request: Request) -> PRSupervisorStore:
    return request.app.state.ctx.require_service("pr_supervisor_store")


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


def _page_context(request: Request) -> dict[str, Any]:
    service = _service(request)
    realm = (
        request.query_params.get("realm")
        or request.app.state.ctx.settings.primary_realm
    )
    watches = service.store.list_watches(realm_id=realm, include_retired=True)
    selected_id = request.query_params.get("watch")
    selected = service.store.get_watch(selected_id) if selected_id else None
    return {
        "watches": watches,
        "watch": selected,
        "watch_events": service.store.list_events(selected.id) if selected else [],
        "capability": service.capability,
        "capabilities": service.store.list_capabilities(),
        "metrics": service.store.metrics(),
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
    policy = body.get("policy")
    if not policy:
        resolved = await _offload(
            request,
            "sqlite.pr_policy_read",
            resolve_policy,
            request.app.state.ctx.store,
            project_id=body.get("project_id"),
            realm_id=realm_id,
            repository=repository,
        )
        policy = resolved.model_dump(mode="json")
    try:
        watch = PRWatch(
            realm_id=realm_id,
            project_id=body.get("project_id"),
            card_id=body.get("card_id"),
            repository=repository,
            pr_number=int(body.get("pr_number") or 0),
            pr_url=str(
                body.get("pr_url")
                or f"https://github.com/{repository}/pull/{body.get('pr_number')}"
            ),
            base_branch=body.get("base_branch"),
            head_sha=body.get("head_sha"),
            originating_instance_id=body.get("originating_instance_id")
            or settings.instance_id,
            originating_session_id=body.get("originating_session_id"),
            originating_agent=body.get("originating_agent"),
            executor_cwd=body.get("executor_cwd"),
            policy=policy,
            required_capabilities=body.get("required_capabilities") or [],
        )
    except (ValueError, TypeError) as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    stored = await _service(request).register_watch(
        watch, source=f"api:{get_principal_id(request)}"
    )
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
    }


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


@router.post("/pr-supervisor/pull-requests", status_code=201)
async def create_pull_request(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    service = _service(request)
    settings = request.app.state.ctx.settings
    repository = str(body.get("repository") or "")
    realm_id = str(body.get("realm_id") or settings.primary_realm)
    project_id = body.get("project_id")
    if not repository or not body.get("title") or not body.get("head"):
        raise HTTPException(
            status_code=400, detail="repository, title, and head are required"
        )
    policy = await _offload(
        request,
        "sqlite.pr_policy_read",
        resolve_policy,
        request.app.state.ctx.store,
        project_id=project_id,
        realm_id=realm_id,
        repository=repository,
    )
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
    watch = await service.register_watch(
        PRWatch(
            realm_id=realm_id,
            project_id=project_id,
            card_id=body.get("card_id"),
            repository=repository,
            pr_number=int(pr["number"]),
            pr_url=str(pr.get("html_url") or ""),
            base_branch=str(
                (pr.get("base") or {}).get("ref") or body.get("base") or "main"
            ),
            head_sha=str((pr.get("head") or {}).get("sha") or "") or None,
            originating_instance_id=settings.instance_id,
            originating_session_id=body.get("originating_session_id"),
            originating_agent=body.get("originating_agent"),
            executor_cwd=body.get("executor_cwd"),
            policy=policy,
        ),
        source=f"pull_request_create:{get_principal_id(request)}",
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
        policies[str(repository)] = policy.model_dump(mode="json")
        config["pr_repository_policies"] = policies
    else:
        config["pr_policy"] = policy.model_dump(mode="json")
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
def ingest_replica(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    watch = PRWatch.model_validate(body.get("watch") or body)
    stored = _store(request).upsert_watch(watch, preserve_lease=True)
    return stored.model_dump(mode="json")


@router.post("/pr-supervisor/retirements")
def ingest_retirement(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    incoming = PRWatch.model_validate(body.get("watch") or {})
    store = _store(request)
    existing = store.find_watch(
        incoming.realm_id, incoming.repository, incoming.pr_number
    )
    if existing:
        if existing.status in {PRWatchStatus.MERGED, PRWatchStatus.CLOSED}:
            retired = existing
            event_type = "retirement_ignored_stronger_terminal"
        else:
            retired = store.set_terminal(
                existing.id,
                PRWatchStatus.RETIRED,
                state=existing.state,
            )
            event_type = "watch_retired"
    else:
        incoming.status = PRWatchStatus.RETIRED
        retired = store.upsert_watch(incoming, preserve_lease=False)
        event_type = "watch_retired"
    event_key = str(body.get("event_key") or f"{retired.id}:retired")
    store.append_event(
        PRWatchEvent(
            watch_id=retired.id,
            event_key=event_key,
            event_type=event_type,
            source="fleet_transition",
        )
    )
    return retired.model_dump(mode="json")


@router.post("/pr-supervisor/watches/{watch_id}/lease")
def acquire_lease(
    request: Request, watch_id: str, body: dict[str, Any]
) -> dict[str, Any]:
    canonical_id = watch_id
    if body.get("watch"):
        stored = _store(request).upsert_watch(
            PRWatch.model_validate(body["watch"]), preserve_lease=True
        )
        canonical_id = stored.id
    capability = GitHubCapability.model_validate(body.get("capability") or {})
    _store(request).save_capability(capability)
    grant = _store(request).try_acquire_lease(
        canonical_id,
        str(body.get("instance_id") or ""),
        ttl_seconds=min(max(int(body.get("ttl_seconds") or 45), 10), 300),
        capability=capability,
    )
    return grant.model_dump(mode="json")


@router.post("/pr-supervisor/instances/heartbeat")
def heartbeat(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    capability = GitHubCapability.model_validate(body)
    _store(request).save_capability(capability)
    return {"accepted": True}


@router.post("/pr-supervisor/dispatch")
async def dispatch_executor(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    service = _service(request)
    watch = PRWatch.model_validate(body.get("watch") or {})
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
        watch, event_key, prompt, prompt_audit=prompt_audit
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
        }
    )
    policy = PRPolicy.model_validate(policy_data)
    config["pr_policy"] = policy.model_dump(mode="json")
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
                icon="work",
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

        store: PRSupervisorStore = ctx.require_service("pr_supervisor_store")
        async_runtime = ctx.require_service("async_runtime")

        @mcp.tool()
        def list_pr_watches(
            realm: str = "default",
            card_id: str | None = None,
            include_retired: bool = False,
        ) -> list[dict[str, Any]]:
            """List durable PR watches and their current lifecycle state."""
            return [
                watch.model_dump(mode="json")
                for watch in store.list_watches(
                    realm_id=realm,
                    card_id=card_id,
                    include_retired=include_retired,
                )
            ]

        @mcp.tool()
        def get_pr_watch(watch_id: str) -> dict[str, Any] | None:
            """Get a PR watch and its audit history."""
            watch = store.get_watch(watch_id)
            if not watch:
                return None
            return {
                "watch": watch.model_dump(mode="json"),
                "events": [
                    event.model_dump(mode="json")
                    for event in store.list_events(watch_id)
                ],
            }

        @mcp.tool()
        async def create_pr_watch(
            repository: str,
            pr_number: int,
            pr_url: str,
            realm: str = "default",
            project_id: str | None = None,
            card_id: str | None = None,
            originating_session_id: str | None = None,
            executor_cwd: str | None = None,
        ) -> dict[str, Any]:
            """Create a durable, fleet-supervised PR watch."""
            service: PRSupervisor = ctx.require_service("pr_supervisor")
            policy = await async_runtime.run_blocking(
                "pr_supervisor.resolve_policy",
                resolve_policy,
                ctx.store,
                project_id=project_id,
                realm_id=realm,
                repository=repository,
            )
            watch = await service.register_watch(
                PRWatch(
                    realm_id=realm,
                    project_id=project_id,
                    card_id=card_id,
                    repository=repository,
                    pr_number=pr_number,
                    pr_url=pr_url,
                    originating_instance_id=ctx.settings.instance_id,
                    originating_session_id=originating_session_id,
                    executor_cwd=executor_cwd,
                    policy=policy,
                ),
                source="mcp",
            )
            return watch.model_dump(mode="json")

        @mcp.tool()
        async def refresh_pr_watch(watch_id: str) -> dict[str, Any]:
            """Schedule an immediate refresh for an active PR watch."""
            service: PRSupervisor = ctx.require_service("pr_supervisor")
            return {
                "watch_id": watch_id,
                "scheduled": bool(await service.refresh_watch(watch_id)),
            }

        @mcp.tool()
        async def retire_pr_watch(watch_id: str) -> dict[str, Any] | None:
            """Retire a PR watch without deleting its audit history."""
            service: PRSupervisor = ctx.require_service("pr_supervisor")
            watch = await service.retire_watch(watch_id)
            return watch.model_dump(mode="json") if watch else None

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
            service: PRSupervisor = ctx.require_service("pr_supervisor")
            policy = await async_runtime.run_blocking(
                "pr_supervisor.resolve_policy",
                resolve_policy,
                ctx.store,
                project_id=project_id,
                realm_id=realm,
                repository=repository,
            )
            pr = await service.github.create_pull_request(
                repository,
                title=title,
                head=head,
                base=base or policy.integration_branch or "main",
                body=body,
                draft=draft,
                policy=policy,
            )
            watched = await service.register_watch(
                PRWatch(
                    realm_id=realm,
                    project_id=project_id,
                    card_id=card_id,
                    repository=repository,
                    pr_number=int(pr["number"]),
                    pr_url=str(pr.get("html_url") or ""),
                    base_branch=str((pr.get("base") or {}).get("ref") or base),
                    head_sha=str((pr.get("head") or {}).get("sha") or "") or None,
                    originating_instance_id=ctx.settings.instance_id,
                    originating_session_id=originating_session_id,
                    executor_cwd=executor_cwd,
                    policy=policy,
                ),
                source="mcp:pull_request_create",
            )
            return {
                "pull_request": {
                    "number": pr["number"],
                    "url": pr.get("html_url"),
                    "draft": bool(pr.get("draft")),
                },
                "watch": watched.model_dump(mode="json"),
            }

        @mcp.tool()
        def set_project_pr_policy(
            project_id: str,
            realm: str = "default",
            repository: str | None = None,
            ready_by_default: bool = True,
            auto_notify: bool = True,
            agent_merge_on_green: bool = True,
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
                    }
                )
                policy = PRPolicy.model_validate(policy_data)
                config["pr_policy"] = policy.model_dump(mode="json")
            updated = request_local_pa(
                ctx.settings,
                "PATCH",
                f"/api/projects/{project_id}",
                params={"realm": realm},
                json={"tool_config": config},
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
        def github_integration_capability() -> dict[str, Any]:
            """Report local GitHub authentication/webhook capability without secrets."""
            service = ctx.services.get("pr_supervisor")
            capability = (
                service.capability
                if service
                else GitHubCapability(
                    instance_id=ctx.settings.instance_id,
                    state="service_not_running",
                )
            )
            return capability.model_dump(mode="json")
