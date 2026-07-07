from __future__ import annotations

from fastapi import APIRouter, Form, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.preferences import get_preferences_store
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.modules.theme import get_theme_catalog

router = APIRouter()


def _templates(request: Request):
    return request.app.state.templates


def _shell_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    settings = ctx.settings
    prefs = get_preferences_store(settings.data_dir).load()
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
        "csrf_token": request.cookies.get("pa_csrf", ""),
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
    prefs = get_preferences_store(settings.data_dir).load()
    return {
        "prefs": prefs,
        "settings": settings,
    }


def _agent_context(request: Request) -> dict:
    ctx: AppContext = request.app.state.ctx
    agent = ctx.require_service("instance_agent")
    sessions = ctx.store.list_sessions()
    return {
        "agent_connected": agent.connected,
        "sessions": sessions[:5],
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


@router.post("/partials/agent/prompt", response_class=HTMLResponse)
async def agent_prompt_partial(
    request: Request,
    message: str = Form(...),
) -> HTMLResponse:
    text = message.strip()
    if not text:
        return _templates(request).TemplateResponse(
            request,
            "partials/agent-message.html",
            {"role": "system", "content": "Message is required."},
        )

    agent = request.app.state.ctx.require_service("instance_agent")
    if not agent.connected:
        return _templates(request).TemplateResponse(
            request,
            "partials/agent-message.html",
            {"role": "system", "content": "Agent is offline."},
        )

    try:
        stop_reason = await agent.prompt(text)
        content = f"Turn completed ({stop_reason})."
    except Exception:
        content = "Something went wrong. Try again or check the server logs."

    return _templates(request).TemplateResponse(
        request,
        "partials/agent-message.html",
        {"role": "user", "content": text, "reply": content},
    )


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
