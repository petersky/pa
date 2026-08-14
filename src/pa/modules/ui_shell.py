from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from time import perf_counter

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

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
    response_bytes = len(html.encode("utf-8"))
    if page.id == "work":
        diagnostics = {
            "event": "work.render",
            "timings_ms": {name: round(value, 1) for name, value in timings.values},
            "total_ms": round((perf_counter() - timings.started) * 1000, 1),
            "response_bytes": response_bytes,
            "htmx": bool(request.headers.get("HX-Request")),
        }
        response.headers["X-PA-Work-Bytes"] = str(response_bytes)
        logger.info("work_render %s", json.dumps(diagnostics, sort_keys=True))
    if page.id == "settings":
        section = str(context.get("active_settings_section") or "agent")
        diagnostics = {
            "event": "settings.render",
            "section": section,
            "timings_ms": {name: round(value, 1) for name, value in timings.values},
            "total_ms": round((perf_counter() - timings.started) * 1000, 1),
            "response_bytes": response_bytes,
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
    from pa.acp.providers.registry import list_providers

    result = {
        "active_settings_section": section,
        "prefs": prefs,
        "global_prefs": global_prefs,
        "settings": settings,
        "status": {
            "service": {"state": "deferred", "backend": "none", "installed": False}
        },
        "themes": get_theme_catalog(),
        "agent_providers": [
            {"id": provider.id, "display_name": provider.display_name}
            for provider in list_providers()
        ],
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
    live = [rt.session for rt in runtimes if not getattr(rt, "_closed", False)]
    live_ids = {session.id for session in live}
    orphans = [
        session
        for session in ctx.store.list_sessions()
        if session.status != "closed" and session.id not in live_ids
    ]
    selected_id = request.query_params.get("session")
    default = next((s for s in live if s.id == selected_id), None)
    if not default and not selected_id:
        default = next(
            (s for s in live if s.label == "default"), live[0] if live else None
        )
    # Durable nonterminal sessions remain actionable even when their ACP
    # runtime was lost. Closed sessions are still opt-in history.
    sessions = live + orphans
    realm_id = ctx.settings.primary_realm
    cards = {card.id: card for card in ctx.store.list_cards(realm_id=realm_id)}
    projects = {
        project.id: project for project in ctx.store.list_projects(realm_id=realm_id)
    }
    now = datetime.now(UTC)
    session_details = {}
    for session in sessions:
        elapsed = max(0, int((now - session.created_at).total_seconds()))
        if elapsed >= 3600:
            elapsed_label = f"{elapsed // 3600}h {(elapsed % 3600) // 60}m"
        elif elapsed >= 60:
            elapsed_label = f"{elapsed // 60}m"
        else:
            elapsed_label = f"{elapsed}s"
        config = session.config_json or {}
        associated_cards = ctx.store.list_cards_for_session(session.id)
        session_details[session.id] = {
            "card": cards.get(session.card_id),
            "cards": associated_cards,
            "project": projects.get(session.project_id),
            "host": config.get("instance_name") or ctx.settings.instance_name,
            "elapsed": elapsed_label,
            "pending_approval": bool(
                (session.metrics_json or {}).get("pending_approval")
                or config.get("pending_approval")
            ),
        }
    watches_by_session: dict[str, list] = {session.id: [] for session in sessions}
    supervisor_store = ctx.services.get("pr_supervisor_store")
    if supervisor_store:
        for watch in supervisor_store.list_watches(include_retired=True):
            for session in sessions:
                if watch.originating_session_id == session.id or (
                    watch.card_id and watch.card_id == session.card_id
                ):
                    watches_by_session[session.id].append(watch)
    return {
        "agent_connected": agent.connected,
        "agent_startup": startup_state(agent),
        "agent_enabled": ctx.settings.agent_enabled,
        "sessions": sessions,
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
                label="Agent",
                icon="agent",
                template="pages/agent.html",
                nav=False,
                nav_order=800,
                context_builder=_agent_context,
            )
        )

    def ui_routers(self):
        return [router]
