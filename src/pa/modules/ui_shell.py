from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from time import perf_counter

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.acp.configuration import normalized_session_config_json
from pa.auth.csrf import token_for_request
from pa.auth.middleware import get_principal_id
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.preferences import get_preferences_store
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.modules.agent_lifecycle import startup_state
from pa.modules.theme import get_theme_catalog
from pa.prompts import PROMPTS

router = APIRouter()
logger = logging.getLogger(__name__)
_SETTINGS_SECTIONS = {
    "appearance",
    "agent",
    "mcp-servers",
    "prompts",
    "telemetry",
    "configuration",
    "backups",
    "instance",
}


class _Timings:
    def __init__(self) -> None:
        self.started = perf_counter()
        self.values: list[tuple[str, float]] = []

    def measure(self, name: str, call):
        started = perf_counter()
        value = call()
        self.values.append((name, (perf_counter() - started) * 1000))
        return value

    def header(self) -> str:
        values = [*self.values, ("total", (perf_counter() - self.started) * 1000)]
        return ", ".join(f"{name};dur={duration:.1f}" for name, duration in values)


def _templates(request: Request):
    return request.app.state.templates


def _user_id_from_request(request: Request) -> str | None:
    principal = get_principal_id(request)
    if principal.startswith("user:"):
        return principal[5:]
    return None


def _shell_context(request: Request) -> dict:
    from pa.acp.providers.registry import provider_catalog

    ctx: AppContext = request.app.state.ctx
    settings = ctx.settings
    prefs = get_preferences_store(
        settings.data_dir, user_id=_user_id_from_request(request)
    ).load()
    agent = ctx.require_service("instance_agent")
    pages: PageRegistry = ctx.require_service("pages")
    assets = ctx.require_service("assets")
    agent_startup = startup_state(agent)

    return {
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "principal_id": get_principal_id(request),
        "agent_connected": agent.connected,
        "agent_startup": agent_startup,
        "debug": settings.debug,
        "dev_tools": settings.dev_tools,
        "theme_id": prefs.theme_id,
        "appearance": prefs.appearance.value,
        "themes": get_theme_catalog(),
        "nav_pages": pages.nav_pages(),
        "asset_version": assets.version,
        "static_url": assets.url,
        "csrf_token": token_for_request(request),
        "telemetry_enabled": settings.telemetry_enabled,
        "telemetry_ui_refresh_seconds": settings.telemetry_ui_refresh_seconds,
        "telemetry_session_header": prefs.telemetry_session_header,
        "pa_version": __import__("pa").__version__,
        "build_id": f"{__import__('pa').__version__}+{assets.version}",
        "agent_providers": provider_catalog(),
    }


def render_page(request: Request, page: PageDefinition) -> HTMLResponse:
    timings = _Timings()
    templates = _templates(request)
    context = timings.measure("shell", lambda: _shell_context(request))
    context["active_path"] = page.path
    context["page"] = page
    timing_name = "settings-section" if page.id == "settings" else "page_context"
    context.update(timings.measure(timing_name, lambda: page.build_context(request)))
    context["request"] = request
    template_name = page.template
    if not request.headers.get("HX-Request"):
        context["include_template"] = page.template
        template_name = "shell.html"
    html = timings.measure(
        "template", lambda: templates.get_template(template_name).render(context)
    )
    response = HTMLResponse(html)
    response.headers["Server-Timing"] = timings.header()
    correlation_id = getattr(request.state, "correlation_id", None) or request.headers.get(
        "X-Request-ID", ""
    )
    response_bytes = len(html.encode("utf-8"))
    diagnostics = {
        "event": "page_shell",
        "page": page.id,
        "correlation_id": correlation_id,
        "timings_ms": {name: round(value, 1) for name, value in timings.values},
        "total_ms": round((perf_counter() - timings.started) * 1000, 1),
        "response_bytes": response_bytes,
        "htmx": bool(request.headers.get("HX-Request")),
    }
    logger.info("page_shell %s", json.dumps(diagnostics, sort_keys=True))
    if page.id in {"work", "home"}:
        diagnostics = {
            "event": f"{page.id}.render",
            "timings_ms": {name: round(value, 1) for name, value in timings.values},
            "total_ms": round((perf_counter() - timings.started) * 1000, 1),
            "response_bytes": response_bytes,
            "htmx": bool(request.headers.get("HX-Request")),
            "correlation_id": correlation_id,
        }
        header = "X-PA-Work-Bytes" if page.id == "work" else "X-PA-Home-Bytes"
        response.headers[header] = str(response_bytes)
        logger.info("%s_render %s", page.id, json.dumps(diagnostics, sort_keys=True))
    if page.id == "settings":
        section = str(context.get("active_settings_section") or "agent")
        diagnostics = {
            "event": "settings.render",
            "section": section,
            "timings_ms": {name: round(value, 1) for name, value in timings.values},
            "total_ms": round((perf_counter() - timings.started) * 1000, 1),
            "response_bytes": response_bytes,
            "correlation_id": correlation_id,
        }
        response.headers["X-PA-Settings-Section"] = section
        response.headers["X-PA-Settings-Bytes"] = str(response_bytes)
        logger.info("settings_render %s", json.dumps(diagnostics, sort_keys=True))
    return response


def _settings_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    settings = ctx.settings
    principal = get_principal_id(request)
    user_id = principal[5:] if principal.startswith("user:") else None
    prefs = get_preferences_store(settings.data_dir, user_id=user_id).load()
    global_prefs = get_preferences_store(settings.data_dir).load()
    requested = request.query_params.get("section", "agent")
    section = requested if requested in _SETTINGS_SECTIONS else "agent"
    from pa.acp.providers.registry import provider_catalog

    result = {
        "active_settings_section": section,
        "prefs": prefs,
        "global_prefs": global_prefs,
        "settings": settings,
        "status": {
            "service": {"state": "deferred", "backend": "none", "installed": False}
        },
        "themes": get_theme_catalog(),
        "agent_providers": provider_catalog(),
        "prompt_catalog": [],
        "prompt_adapters": [],
        "telemetry_health": {"state": "deferred"},
        "configuration": {
            "settings": [],
            "precedence": [],
            "deprecated": [],
            "unknown": [],
            "revision": "",
        },
        "backup_status": None,
        "backup_records": [],
    }
    if section == "prompts":
        result["prompt_catalog"] = PROMPTS.catalog(provider=settings.agent_provider)
        result["prompt_adapters"] = [
            item.model_dump(mode="json") for item in PROMPTS.adapters()
        ]
    elif section == "telemetry":
        result["telemetry_health"] = (
            ctx.services["telemetry"].health()
            if "telemetry" in ctx.services
            else {"state": "starting"}
        )
    elif section == "configuration":
        from pa.configuration.service import configuration_snapshot

        result["configuration"] = configuration_snapshot(settings)
    elif section == "backups":
        backup_service = ctx.services.get("backup_service")
        result["backup_status"] = backup_service.status() if backup_service else None
        result["backup_records"] = (
            [item.public_dict() for item in backup_service.list_backups()]
            if backup_service
            else []
        )
    elif section == "instance":
        from pa.status.info import build_status_snapshot

        kernel = request.app.state.kernel
        result["status"] = build_status_snapshot(
            ctx, module_count=len(kernel.registry.modules)
        )
    return result


def _agent_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    agent = ctx.require_service("instance_agent")
    runtimes = agent.list_runtimes() if hasattr(agent, "list_runtimes") else []
    active_runtimes = [rt for rt in runtimes if not getattr(rt, "_closed", False)]
    live = [rt.session for rt in active_runtimes]
    runtimes_by_session = {rt.session.id: rt for rt in active_runtimes}
    live_ids = {session.id for session in live}
    persisted = ctx.store.list_sessions()
    by_id = {session.id: session for session in persisted}
    by_id.update({session.id: session for session in live})
    all_sessions = list(by_id.values())
    selected_id = request.query_params.get("session")
    default = next((s for s in all_sessions if s.id == selected_id), None)
    if not default and not selected_id:
        default = next(
            (
                s
                for s in all_sessions
                if s.purpose == "chat" and s.archived_at is None
            ),
            live[0] if live else None,
        )
    chat_sessions = [
        session
        for session in all_sessions
        if session.purpose == "chat" and session.archived_at is None
    ]
    chat_sessions.sort(
        key=lambda session: (
            session.pinned_at is not None,
            session.pinned_at
            or session.human_activity_at
            or session.updated_at,
        ),
        reverse=True,
    )
    archived_chats = [
        session
        for session in all_sessions
        if session.purpose == "chat" and session.archived_at is not None
    ]
    archived_chats.sort(key=lambda session: session.archived_at, reverse=True)
    activity_sessions = [
        session
        for session in all_sessions
        if session.purpose in {"automated_run", "one_shot_job"}
        and not (
            session.purpose == "one_shot_job"
            and session.workflow_state == "succeeded"
        )
    ]
    activity_sessions.sort(key=lambda session: session.updated_at, reverse=True)
    all_sessions.sort(key=lambda session: session.updated_at, reverse=True)
    selected_view = request.query_params.get("view", "chats")
    activity_filter = request.query_params.get("filter", "all")
    if activity_filter not in {"all", "needs_you", "running", "completed"}:
        activity_filter = "all"
    if selected_view == "activity":
        sessions = activity_sessions
    elif selected_view == "all":
        sessions = all_sessions
    elif selected_view == "archived":
        sessions = archived_chats
    else:
        selected_view = "chats"
        sessions = chat_sessions
    realm_id = ctx.settings.primary_realm
    cards = {card.id: card for card in ctx.store.list_cards(realm_id=realm_id)}
    projects = {
        project.id: project for project in ctx.store.list_projects(realm_id=realm_id)
    }
    now = datetime.now(UTC)
    session_details = {}
    from pa.execution.session_presentation import build_session_presentation

    dispatch_store = ctx.services.get("dispatch_store")
    for session in all_sessions:
        elapsed = max(0, int((now - session.created_at).total_seconds()))
        if elapsed >= 3600:
            elapsed_label = f"{elapsed // 3600}h {(elapsed % 3600) // 60}m"
        elif elapsed >= 60:
            elapsed_label = f"{elapsed // 60}m"
        else:
            elapsed_label = f"{elapsed}s"
        config = session.config_json or {}
        normalized_config, confirmed_configuration = normalized_session_config_json(
            config,
            model_id=session.model_id,
            mode_id=session.mode_id,
        )
        runtime = runtimes_by_session.get(session.id)
        durable = dict(config.get("durable_runtime") or {})
        queued = list(runtime._queue) if runtime else durable.get("queued_prompts") or []
        pending_approval = bool(
            (session.metrics_json or {}).get("pending_approval")
            or config.get("pending_approval")
        )
        presentation = build_session_presentation(
            session,
            runtime=runtime,
            dispatch=dispatch_store.by_session(session.id) if dispatch_store else None,
            quiescing=agent.quiescing,
            startup_complete=agent.startup_complete,
        )
        state = presentation["display_status"].casefold().replace(" ", "_")
        execution = dict(config.get("execution_context") or {})
        repositories = list(execution.get("repositories") or [])
        repository = dict(repositories[0]) if repositories else {}
        repository_url = str(repository.get("repository_url") or "")
        repository_name = repository_url.rstrip("/").rsplit("/", 1)[-1]
        if repository_name.endswith(".git"):
            repository_name = repository_name[:-4]
        metrics = dict(session.metrics_json or {})
        usage = dict(metrics.get("last_usage") or metrics.get("usage") or {})
        associated_cards = ctx.store.list_cards_for_session(session.id)
        recent_events = ctx.store.list_transcript_events_before(session.id, limit=100)
        closure = next(
            (
                event
                for event in reversed(recent_events)
                if event.event_type == "session_closed"
            ),
            None,
        )
        session_details[session.id] = {
            "card": cards.get(session.card_id),
            "cards": associated_cards,
            "project": projects.get(session.project_id),
            "host": config.get("instance_name") or ctx.settings.instance_name,
            "elapsed": elapsed_label,
            "pending_approval": pending_approval,
            "state": state,
            "repository_name": repository_name,
            "repository_url": repository_url,
            "branch": repository.get("branch"),
            "turns": metrics.get("turns"),
            "total_tokens": usage.get("total_tokens") or usage.get("totalTokens"),
            "presentation": presentation,
            "provider_attempts": sum(
                event.event_type in {"session_started", "session_admission_failed"}
                for event in recent_events
            ),
            "closure_reason": (closure.payload or {}).get("reason") if closure else None,
            "activity_group": (
                cards[session.card_id].title
                if session.card_id in cards
                else str((session.initiating_workflow or {}).get("kind") or "Other activity")
                .replace("_", " ")
                .title()
            ),
            "model_id": confirmed_configuration.get("model_id"),
            "mode_id": confirmed_configuration.get("mode_id"),
            "config_json": normalized_config,
        }
    if selected_view == "activity":
        if activity_filter != "all":
            activity_sessions = [
                session
                for session in activity_sessions
                if (
                    activity_filter == "needs_you"
                    and session_details[session.id]["presentation"]["display_status"]
                    == "Needs you"
                )
                or (
                    activity_filter == "completed"
                    and session_details[session.id]["presentation"]["workflow"]["state"]
                    in {"succeeded", "failed", "cancelled", "validation_failed"}
                )
                or (
                    activity_filter == "running"
                    and session_details[session.id]["presentation"]["display_status"]
                    in {"Running", "Queued", "Restoring your work", "Waiting"}
                )
            ]
        activity_sessions.sort(
            key=lambda session: (
                session_details[session.id]["activity_group"].casefold(),
                -session.updated_at.timestamp(),
            )
        )
        sessions = activity_sessions
    watches_by_session: dict[str, list] = {session.id: [] for session in all_sessions}
    supervisor_store = ctx.services.get("pr_supervisor_store")
    if supervisor_store:
        for watch in supervisor_store.list_watches(include_retired=True):
            for session in all_sessions:
                if watch.originating_session_id == session.id or (
                    watch.card_id and watch.card_id == session.card_id
                ):
                    watches_by_session[session.id].append(watch)
    from pa.acp.providers.registry import provider_catalog

    selected_dispatch = None
    if selected_id and dispatch_store:
        selected_dispatch = dispatch_store.by_session(selected_id)
    attribution = None
    if selected_dispatch:
        attribution = {
            "source": "durable dispatch envelope",
            "dispatch_id": selected_dispatch.dispatch_id,
            "session_id": selected_dispatch.session_id,
            "initiator": getattr(selected_dispatch, "initiating_principal", None)
            or getattr(selected_dispatch, "principal_id", None)
            or (selected_dispatch.request_payload or {}).get("principal_id")
            or "Initiator not recorded by this peer version",
            "authority_instance_id": selected_dispatch.authority_instance_id,
            "target_instance_id": selected_dispatch.target_instance_id,
            "provider": (selected_dispatch.request_payload or {}).get("provider") or "Target default",
            "stale": selected_dispatch.state not in {"running", "delivering_prompt", "starting_session"},
        }
    return {
        "agent_connected": agent.connected,
        "agent_startup": startup_state(agent),
        "agent_enabled": ctx.settings.agent_enabled,
        "sessions": sessions,
        "chat_sessions": chat_sessions,
        "activity_sessions": activity_sessions,
        "archived_chat_sessions": archived_chats,
        "all_sessions": all_sessions,
        "session_view": selected_view,
        "activity_filter": activity_filter,
        "live_session_ids": live_ids,
        "session_id": selected_id or (default.id if default else ""),
        "session_instance_id": request.query_params.get("instance")
        or (default.origin_instance_id if default else None)
        or ctx.settings.instance_id,
        "session_details": session_details,
        "agent_realm_id": realm_id,
        "available_cards": sorted(
            cards.values(), key=lambda card: (card.title.casefold(), card.id)
        ),
        "available_projects": sorted(
            projects.values(),
            key=lambda project: (project.title.casefold(), project.id),
        ),
        "pr_watches_by_session": watches_by_session,
        "agent_providers": provider_catalog(),
        "session_attribution": attribution,
    }


@router.get("/", response_class=HTMLResponse)
def page_home(request: Request) -> HTMLResponse:
    page = request.app.state.ctx.require_service("pages").get_by_path("/")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


@router.get("/{page_path:path}", response_class=HTMLResponse)
def page_route(request: Request, page_path: str) -> HTMLResponse:
    reserved = ("partials", "static", "api", "items", "login")
    first = page_path.split("/", 1)[0]
    if first in reserved:
        raise HTTPException(status_code=404)

    path = f"/{page_path}" if page_path else "/"
    pages: PageRegistry = request.app.state.ctx.require_service("pages")
    page = pages.get_by_path(path)
    if not page:
        raise HTTPException(status_code=404, detail=f"Unknown page: {path}")
    return render_page(request, page)


class UiShellModule(Module):
    @property
    def name(self) -> str:
        return "ui_shell"

    @property
    def version(self) -> str:
        return "0.0.1"

    @property
    def description(self) -> str:
        return "SPA shell, routing, settings, and agent chat page"

    def on_load(self, ctx: AppContext) -> None:
        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="settings",
                path="/settings",
                label="Settings",
                icon="gear",
                template="pages/settings.html",
                nav=False,
                nav_order=900,
                context_builder=_settings_context,
            )
        )
        pages.register(
            PageDefinition(
                id="agent",
                path="/agent",
                label="Sessions",
                icon="agent",
                template="pages/agent.html",
                nav=False,
                nav_order=800,
                context_builder=_agent_context,
            )
        )

    def ui_routers(self):
        return [router]
