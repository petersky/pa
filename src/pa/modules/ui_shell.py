from __future__ import annotations

from datetime import UTC, datetime

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.auth.csrf import token_for_request
from pa.auth.middleware import get_principal_id
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.preferences import get_preferences_store
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.modules.theme import get_theme_catalog
from pa.prompts import PROMPTS

router = APIRouter()


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

    return {
        "instance_name": settings.instance_name,
        "agent_connected": agent.connected,
        "debug": settings.debug,
        "dev_tools": settings.dev_tools,
        "theme_id": prefs.theme_id,
        "appearance": prefs.appearance.value,
        "themes": get_theme_catalog(),
        "nav_pages": pages.nav_pages(),
        "asset_version": assets.version,
        "static_url": assets.url,
        "csrf_token": token_for_request(request),
        "pa_version": __import__("pa").__version__,
        "build_id": f"{__import__('pa').__version__}+{assets.version}",
    }


def render_page(request: Request, page: PageDefinition) -> HTMLResponse:
    templates = _templates(request)
    context = _shell_context(request)
    context["active_path"] = page.path
    context["page"] = page
    context.update(page.build_context(request))

    if request.headers.get("HX-Request"):
        return templates.TemplateResponse(request, page.template, context)

    context["include_template"] = page.template
    return templates.TemplateResponse(request, "shell.html", context)


def _settings_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    settings = ctx.settings
    principal = get_principal_id(request)
    user_id = principal[5:] if principal.startswith("user:") else None
    prefs = get_preferences_store(settings.data_dir, user_id=user_id).load()
    global_prefs = get_preferences_store(settings.data_dir).load()
    kernel = request.app.state.kernel
    from pa.status.info import build_status_snapshot
    from pa.acp.providers.registry import list_providers

    status = build_status_snapshot(ctx, module_count=len(kernel.registry.modules))
    return {
        "prefs": prefs,
        "global_prefs": global_prefs,
        "settings": settings,
        "status": status,
        "themes": get_theme_catalog(),
        "agent_providers": [
            {"id": provider.id, "display_name": provider.display_name}
            for provider in list_providers()
        ],
        "prompt_catalog": PROMPTS.catalog(provider=settings.agent_provider),
        "prompt_adapters": [
            item.model_dump(mode="json") for item in PROMPTS.adapters()
        ],
    }


def _agent_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    agent = ctx.require_service("instance_agent")
    runtimes = agent.list_runtimes() if hasattr(agent, "list_runtimes") else []
    live = [rt.session for rt in runtimes if not getattr(rt, "_closed", False)]
    selected_id = request.query_params.get("session")
    default = next((s for s in live if s.id == selected_id), None)
    if not default:
        default = next(
            (s for s in live if s.label == "default"), live[0] if live else None
        )
    # The Agent sidebar starts with live runtimes only. Closed sessions are
    # loaded explicitly from the durable history API when the user opts in.
    sessions = live
    cards = {card.id: card for card in ctx.store.list_cards()}
    projects = {project.id: project for project in ctx.store.list_projects()}
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
        session_details[session.id] = {
            "card": cards.get(session.card_id),
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
        "agent_enabled": ctx.settings.agent_enabled,
        "sessions": sessions,
        "session_id": default.id if default else "",
        "session_details": session_details,
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
