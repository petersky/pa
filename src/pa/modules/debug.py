from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import HTMLResponse

from pa.core.contracts import Module
from pa.core.context import AppContext

router = APIRouter(prefix="/debug", tags=["debug"])
ui_router = APIRouter()


@router.get("/modules")
def debug_modules(request: Request) -> list[dict]:
    _require_debug(request)
    return request.app.state.kernel.registry.describe()


@router.get("/hooks")
def debug_hooks(request: Request, limit: int = 50, name: str | None = None) -> dict:
    _require_debug(request)
    ctx: AppContext = request.app.state.ctx
    events = ctx.hooks.history(limit=limit, name=name)
    return {
        "registered": ctx.hooks.list_hooks(),
        "events": [
            {
                "name": event.name,
                "timestamp": event.timestamp,
                "payload": event.payload,
                "errors": event.errors,
            }
            for event in events
        ],
    }


@router.get("/config")
def debug_config(request: Request) -> dict:
    _require_debug(request)
    settings = request.app.state.ctx.settings
    return {
        "instance_id": settings.instance_id,
        "instance_name": settings.instance_name,
        "debug": settings.debug,
        "dev_tools": settings.dev_tools,
        "log_level": settings.log_level,
        "data_dir": str(settings.data_dir),
        "agent_enabled": settings.agent_enabled,
    }


@ui_router.get("/partials/debug-panel", response_class=HTMLResponse)
def debug_panel_partial(request: Request) -> HTMLResponse:
    _require_debug(request)
    kernel = request.app.state.kernel
    ctx: AppContext = request.app.state.ctx
    return request.app.state.templates.TemplateResponse(
        request,
        "partials/debug-panel.html",
        {
            "modules": kernel.registry.describe(),
            "hooks": ctx.hooks.list_hooks(),
            "recent_events": ctx.hooks.history(limit=10),
        },
    )


def _require_debug(request: Request) -> None:
    if not request.app.state.ctx.settings.debug:
        raise HTTPException(status_code=404, detail="Not found")


class DebugModule(Module):
    @property
    def name(self) -> str:
        return "debug"

    @property
    def version(self) -> str:
        return "0.0.1"

    @property
    def description(self) -> str:
        return "Developer visibility: hooks, modules, and request tracing"

    def on_load(self, ctx: AppContext) -> None:
        if ctx.settings.debug:
            ctx.hooks.enable_history(True)

            async def _log_startup(**payload: object) -> None:
                import logging

                logging.getLogger("pa.debug").debug(
                    "app.startup: %s", payload.get("modules")
                )

            ctx.hooks.on("app.startup", _log_startup, priority=100)

    def api_routers(self):
        return [("/api", router, ["debug"])]

    def ui_routers(self):
        return [ui_router]
