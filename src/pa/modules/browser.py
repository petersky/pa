"""Browser surface REST API and model-facing MCP tools."""

from __future__ import annotations

import base64
import json
import os
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id
from pa.browser.cdp import CdpError, CdpPage
from pa.browser.session import (
    MAX_ACTIONS,
    MAX_COORDINATE,
    MAX_PAUSE_SECONDS,
    MAX_TOTAL_PAUSE_SECONDS,
    NAMED_KEYS,
    BrowserScope,
    BrowserSessionError,
    BrowserSessionManager,
)
from pa.core.context import AppContext
from pa.core.contracts import Module

router = APIRouter(prefix="/agent/sessions/{session_id}/browser")
automation_router = APIRouter(prefix="/browser")


def _runtime(request: Request, session_id: str):
    manager = request.app.state.ctx.require_service("instance_agent")
    runtime = manager.get(session_id)
    if not runtime:
        raise HTTPException(status_code=404, detail="Agent session not found")
    return runtime


class AttachBody(BaseModel):
    url: str = "about:blank"
    width: int | None = Field(default=None, ge=320, le=7680)
    height: int | None = Field(default=None, ge=240, le=4320)
    device_scale_factor: float = Field(default=1, ge=0.25, le=4)


class ResizeBody(BaseModel):
    width: int = Field(ge=320, le=7680)
    height: int = Field(ge=240, le=4320)
    device_scale_factor: float = Field(default=1, ge=0.25, le=4)


class NavigateBody(BaseModel):
    url: str


class ClickBody(BaseModel):
    x: float
    y: float


class TypeBody(BaseModel):
    text: str = Field(max_length=100_000)


class AutomationBody(BaseModel):
    agent_session_id: str
    browser_handle: str | None = None
    operation_id: str | None = None
    share_handle: str | None = None
    authorized_session_id: str | None = None
    ttl_seconds: int = Field(default=300, ge=30, le=900)
    url: str | None = None
    width: int = Field(default=1440, ge=320, le=7680)
    height: int = Field(default=900, ge=240, le=4320)
    device_scale_factor: float = Field(default=1, ge=0.25, le=4)
    selector: str | None = None
    ref: str | None = None
    x: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    y: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    button: str | int | None = "left"
    click_count: int = Field(default=1, ge=1, le=3)
    modifiers: list[str] = Field(default_factory=list, max_length=4)
    key: str | None = None
    text: str | None = Field(default=None, max_length=100_000)
    clear: bool = True
    submit: bool = False
    delay_ms: int = Field(default=0, ge=0, le=1000)
    delta_x: float = 0
    delta_y: float = 0
    source_selector: str | None = None
    source_ref: str | None = None
    source_x: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    source_y: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    target_selector: str | None = None
    target_ref: str | None = None
    target_x: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    target_y: float | None = Field(default=None, ge=-MAX_COORDINATE, le=MAX_COORDINATE)
    steps: int = Field(default=10, ge=1, le=50)
    actions: list[dict[str, Any]] | None = Field(
        default=None, min_length=1, max_length=MAX_ACTIONS
    )


@router.post("/attach")
async def attach_browser(request: Request, session_id: str, body: AttachBody) -> dict:
    try:
        return await _runtime(request, session_id).set_browser_attached(
            True,
            url=body.url,
            width=body.width,
            height=body.height,
            device_scale_factor=body.device_scale_factor,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.post("/detach")
async def detach_browser(request: Request, session_id: str) -> dict:
    try:
        return await _runtime(request, session_id).set_browser_attached(False)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@router.get("")
async def browser_state(request: Request, session_id: str) -> dict:
    return await _runtime(request, session_id).browser_state()


def _page(request: Request, session_id: str) -> CdpPage:
    attachment = _runtime(request, session_id).manager.browser.get(session_id)
    if not attachment:
        raise HTTPException(status_code=409, detail="No browser is attached")
    return attachment.page


@router.get("/screenshot")
async def browser_screenshot(request: Request, session_id: str) -> Response:
    try:
        image = await _page(request, session_id).screenshot()
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=503, detail="Attached browser is unavailable"
        ) from exc
    return Response(image, media_type="image/png")


@router.post("/navigate")
async def browser_navigate(
    request: Request, session_id: str, body: NavigateBody
) -> dict:
    await _page(request, session_id).navigate(body.url)
    return await _runtime(request, session_id).browser_state()


@router.post("/resize")
async def browser_resize(request: Request, session_id: str, body: ResizeBody) -> dict:
    try:
        return await _runtime(request, session_id).resize_browser(
            body.width,
            body.height,
            device_scale_factor=body.device_scale_factor,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/click")
async def browser_click(request: Request, session_id: str, body: ClickBody) -> dict:
    await _page(request, session_id).command(
        "Input.dispatchMouseEvent",
        {
            "type": "mousePressed",
            "x": body.x,
            "y": body.y,
            "button": "left",
            "clickCount": 1,
        },
    )
    await _page(request, session_id).command(
        "Input.dispatchMouseEvent",
        {
            "type": "mouseReleased",
            "x": body.x,
            "y": body.y,
            "button": "left",
            "clickCount": 1,
        },
    )
    return {"ok": True}


@router.post("/type")
async def browser_type(request: Request, session_id: str, body: TypeBody) -> dict:
    await _page(request, session_id).command("Input.insertText", {"text": body.text})
    return {"ok": True}


def _scope(request: Request, agent_session_id: str) -> BrowserScope:
    try:
        UUID(agent_session_id)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "ownership_failure",
                "message": "agent_session_id must be the full canonical session UUID.",
            },
        ) from exc
    ctx = request.app.state.ctx
    stored = ctx.store.get_session(agent_session_id)
    principal = get_principal_id(request)
    if not stored or getattr(stored, "principal_id", None) != principal:
        raise HTTPException(
            status_code=403,
            detail={
                "code": "ownership_failure",
                "message": "The authenticated principal does not own this agent session.",
            },
        )
    return BrowserScope(principal, agent_session_id, ctx.settings.instance_id)


def _browser_sessions(request: Request) -> BrowserSessionManager:
    return request.app.state.ctx.require_service("browser_sessions")


async def _ensure_attached(
    manager: BrowserSessionManager, scope: BrowserScope, body: AutomationBody
) -> None:
    if body.browser_handle:
        return
    try:
        manager.resolve(scope)
    except BrowserSessionError as exc:
        if exc.code != "browser_not_attached":
            raise
        await manager.attach(scope)


def _browser_http_error(exc: BrowserSessionError) -> HTTPException:
    status = 403 if exc.code == "ownership_failure" else 409
    if exc.code.startswith("invalid_") or exc.code in {
        "unsupported_button",
        "unsupported_key",
        "quota_exceeded",
    }:
        status = 422
    if exc.code in {"browser_unavailable", "browser_protocol_error"}:
        status = 503
    if exc.code == "timeout":
        status = 504
    return HTTPException(status_code=status, detail=exc.as_dict())


@automation_router.get("/capabilities")
async def browser_capabilities() -> dict[str, Any]:
    return {
        "schema": "pa.browser-capabilities/v1",
        "common_actions": [
            "attach",
            "state",
            "open",
            "snapshot",
            "click",
            "hover",
            "type",
            "press",
            "scroll",
            "drag",
            "resize",
            "back",
            "screenshot",
            "detach",
        ],
        "advanced_actions": [
            "pointer_move",
            "pointer_down",
            "pointer_up",
            "key_down",
            "key_press",
            "key_up",
            "wheel",
            "pause",
        ],
        "buttons": {"names": ["left", "middle", "right"], "numbers": [0, 1, 2]},
        "modifiers": ["Alt", "Control", "Meta", "Shift"],
        "named_keys": sorted(NAMED_KEYS),
        "limits": {
            "max_actions": MAX_ACTIONS,
            "max_pause_ms": MAX_PAUSE_SECONDS * 1000,
            "max_total_pause_ms": MAX_TOTAL_PAUSE_SECONDS * 1000,
            "max_coordinate": MAX_COORDINATE,
            "coordinate_unit": "viewport_css_pixel",
            "wheel_unit": "css_pixel",
        },
        "cli": "pa browser --help",
        "http": {
            "capabilities": "GET /api/browser/capabilities",
            "operations": "POST /api/browser/{operation}",
            "authentication": "PA bearer token required",
        },
    }


@automation_router.post("/{operation}")
async def browser_automation(
    request: Request, operation: str, body: AutomationBody
) -> dict[str, Any]:
    manager = _browser_sessions(request)
    scope = _scope(request, body.agent_session_id)
    try:
        if operation == "attach":
            return await manager.attach(
                scope,
                url=body.url or "about:blank",
                width=body.width,
                height=body.height,
                device_scale_factor=body.device_scale_factor,
                share_handle=body.share_handle,
            )
        if operation == "state":
            return await manager.state(scope, handle=body.browser_handle)
        if operation == "share":
            if not body.authorized_session_id:
                raise BrowserSessionError(
                    "invalid_share_target", "authorized_session_id is required."
                )
            try:
                UUID(body.authorized_session_id)
            except ValueError as exc:
                raise BrowserSessionError(
                    "invalid_share_target",
                    "authorized_session_id must be a full canonical session UUID.",
                ) from exc
            target_session = request.app.state.ctx.store.get_session(
                body.authorized_session_id
            )
            if (
                not target_session
                or getattr(target_session, "principal_id", None) != scope.principal_id
            ):
                raise BrowserSessionError(
                    "ownership_failure",
                    "The authenticated principal does not own the authorized share target session.",
                )
            return await manager.share(
                scope,
                authorized_session_id=body.authorized_session_id,
                handle=body.browser_handle,
                ttl_seconds=body.ttl_seconds,
            )
        if operation == "detach":
            return await manager.detach(scope, handle=body.browser_handle)
        if operation not in {
            "open",
            "snapshot",
            "click",
            "hover",
            "type",
            "press",
            "scroll",
            "drag",
            "actions",
            "back",
            "resize",
            "screenshot",
        }:
            raise BrowserSessionError(
                "invalid_operation", f"Unsupported browser operation {operation!r}."
            )
        await _ensure_attached(manager, scope, body)
        common = {"handle": body.browser_handle, "operation_id": body.operation_id}
        if operation == "open":
            if not body.url:
                raise BrowserSessionError("invalid_url", "url is required.")
            return await manager.open(scope, url=body.url, **common)
        if operation == "snapshot":
            return await manager.snapshot(scope, handle=body.browser_handle)
        if operation == "click":
            return await manager.click(
                scope,
                selector=body.selector,
                ref=body.ref,
                x=body.x,
                y=body.y,
                button=body.button,
                click_count=body.click_count,
                modifiers=body.modifiers,
                **common,
            )
        if operation == "hover":
            return await manager.hover(
                scope,
                selector=body.selector,
                ref=body.ref,
                x=body.x,
                y=body.y,
                modifiers=body.modifiers,
                **common,
            )
        if operation == "type":
            if body.text is None:
                raise BrowserSessionError("invalid_text", "text is required.")
            return await manager.type_text(
                scope,
                selector=body.selector,
                ref=body.ref,
                text=body.text,
                clear=body.clear,
                submit=body.submit,
                delay_ms=body.delay_ms,
                modifiers=body.modifiers,
                **common,
            )
        if operation == "press":
            if body.key is None:
                raise BrowserSessionError("unsupported_key", "key is required.")
            return await manager.press(
                scope, key=body.key, modifiers=body.modifiers, **common
            )
        if operation == "scroll":
            return await manager.scroll(
                scope,
                delta_x=body.delta_x,
                delta_y=body.delta_y,
                selector=body.selector,
                ref=body.ref,
                x=body.x,
                y=body.y,
                **common,
            )
        if operation == "drag":
            return await manager.drag(
                scope,
                source_selector=body.source_selector,
                source_ref=body.source_ref,
                source_x=body.source_x,
                source_y=body.source_y,
                target_selector=body.target_selector,
                target_ref=body.target_ref,
                target_x=body.target_x,
                target_y=body.target_y,
                button=body.button,
                steps=body.steps,
                **common,
            )
        if operation == "actions":
            return await manager.actions(scope, actions=body.actions or [], **common)
        if operation == "back":
            return await manager.back(scope, **common)
        if operation == "resize":
            return await manager.resize(
                scope,
                width=body.width,
                height=body.height,
                device_scale_factor=body.device_scale_factor,
                **common,
            )
        if operation == "screenshot":
            data = await manager.screenshot(scope, handle=body.browser_handle)
            return {
                "ok": True,
                "browser_handle": manager.resolve(scope, body.browser_handle).handle,
                "media_type": "image/png",
                "data_base64": base64.b64encode(data).decode("ascii"),
            }
        raise BrowserSessionError(
            "invalid_operation", f"Unsupported browser operation {operation!r}."
        )
    except BrowserSessionError as exc:
        raise _browser_http_error(exc) from exc

    except CdpError as exc:
        error = BrowserSessionError("browser_protocol_error", str(exc), retryable=True)
        raise _browser_http_error(error) from exc
    except RuntimeError as exc:
        error = BrowserSessionError(
            "browser_unavailable",
            "The managed browser is unavailable; attach again after checking the server browser installation.",
            retryable=True,
        )
        raise _browser_http_error(error) from exc


def _mcp_execution_identity(ctx: AppContext) -> tuple[str, str]:
    raw = os.environ.get("PA_EXECUTION_CONTEXT", "")
    try:
        execution = json.loads(raw) if raw else {}
    except ValueError:
        execution = {}
    session_id = str(
        execution.get("session_id") or os.environ.get("PA_BROWSER_SESSION_ID") or ""
    )
    instance = execution.get("instance") or {}
    instance_id = str(instance.get("id") or ctx.settings.instance_id)
    try:
        UUID(session_id)
    except ValueError as exc:
        raise RuntimeError(
            "Browser MCP requires a full canonical PA agent session ID in PA_EXECUTION_CONTEXT."
        ) from exc
    if instance_id != ctx.settings.instance_id:
        raise RuntimeError(
            "Browser MCP execution instance does not match the owning PA server."
        )
    return session_id, instance_id


class BrowserModule(Module):
    @property
    def name(self) -> str:
        return "browser"

    @property
    def version(self) -> str:
        return "0.2.0"

    @property
    def description(self) -> str:
        return "Isolated browser sessions and Playwright-style automation"

    async def on_startup(self, app, ctx: AppContext) -> None:
        agent = ctx.require_service("instance_agent")
        manager = BrowserSessionManager(
            agent.browser,
            instance_id=ctx.settings.instance_id,
            attached_lookup=agent.browser.get,
        )
        await manager.start()
        ctx.register_service("browser_sessions", manager)

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        manager = ctx.services.get("browser_sessions")
        if manager:
            await manager.close()

    def api_routers(self):
        return [
            ("/api", router, ["browser"]),
            ("/api", automation_router, ["browser-automation"]),
        ]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        def call(operation: str, **payload: Any) -> dict[str, Any]:
            session_id, _ = _mcp_execution_identity(ctx)
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/browser/{operation}",
                json={"agent_session_id": session_id, **payload},
            )

        @mcp.tool()
        def browser_capabilities() -> dict[str, Any]:
            """Return compact Browser action names, key/button semantics, and safety limits."""
            return request_local_pa(ctx.settings, "GET", "/api/browser/capabilities")

        @mcp.tool()
        def browser_attach(
            url: str = "about:blank",
            width: int = 1440,
            height: int = 900,
            device_scale_factor: float = 1,
            share_handle: str | None = None,
        ) -> str:
            """Attach this PA agent session to its isolated browser, or redeem an authorized share handle."""
            return json.dumps(
                call(
                    "attach",
                    url=url,
                    width=width,
                    height=height,
                    device_scale_factor=device_scale_factor,
                    share_handle=share_handle,
                )
            )

        @mcp.tool()
        def browser_state(browser_handle: str | None = None) -> str:
            """Return this session's browser handle, ownership, target, viewport, and expiry."""
            return json.dumps(call("state", browser_handle=browser_handle))

        @mcp.tool()
        def browser_open(
            url: str, browser_handle: str | None = None, operation_id: str | None = None
        ) -> str:
            """Navigate the isolated browser; operation_id prevents duplicate transport retries."""
            return json.dumps(
                call(
                    "open",
                    url=url,
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_resize(
            width: int,
            height: int,
            device_scale_factor: float = 1,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Resize the browser viewport in CSS pixels."""
            return json.dumps(
                call(
                    "resize",
                    width=width,
                    height=height,
                    device_scale_factor=device_scale_factor,
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_detach(browser_handle: str | None = None) -> str:
            """Detach; user-owned browsers are preserved and shared callers only detach themselves."""
            return json.dumps(call("detach", browser_handle=browser_handle))

        @mcp.tool()
        def browser_share(
            authorized_session_id: str,
            browser_handle: str | None = None,
            ttl_seconds: int = 300,
        ) -> str:
            """Mint a single-use share handle for one explicit canonical agent session."""
            return json.dumps(
                call(
                    "share",
                    authorized_session_id=authorized_session_id,
                    browser_handle=browser_handle,
                    ttl_seconds=ttl_seconds,
                )
            )

        @mcp.tool()
        def browser_snapshot(browser_handle: str | None = None) -> str:
            """Return visible content with target- and document-revision-bound element refs."""
            return json.dumps(
                call("snapshot", browser_handle=browser_handle), ensure_ascii=False
            )

        @mcp.tool()
        def browser_click(
            selector: str | None = None,
            ref: str | None = None,
            x: float | None = None,
            y: float | None = None,
            button: str | int = "left",
            click_count: int = 1,
            modifiers: list[str] | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Click a selector/ref (preferred) or viewport coordinates with button, count, and modifiers."""
            return json.dumps(
                call(
                    "click",
                    selector=selector,
                    ref=ref,
                    x=x,
                    y=y,
                    button=button,
                    click_count=click_count,
                    modifiers=modifiers or [],
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_hover(
            selector: str | None = None,
            ref: str | None = None,
            x: float | None = None,
            y: float | None = None,
            modifiers: list[str] | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Move the pointer to a selector/ref or explicit viewport coordinates."""
            return json.dumps(
                call(
                    "hover",
                    selector=selector,
                    ref=ref,
                    x=x,
                    y=y,
                    modifiers=modifiers or [],
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_type(
            selector: str | None,
            text: str,
            clear: bool = True,
            submit: bool = False,
            delay_ms: int = 0,
            modifiers: list[str] | None = None,
            ref: str | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Focus and type; delay emits key events and submit submits its form or presses Enter."""
            return json.dumps(
                call(
                    "type",
                    selector=selector,
                    ref=ref,
                    text=text,
                    clear=clear,
                    submit=submit,
                    delay_ms=delay_ms,
                    modifiers=modifiers or [],
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_press(
            key: str,
            modifiers: list[str] | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Press one documented named key or Unicode character with an optional modifier chord."""
            return json.dumps(
                call(
                    "press",
                    key=key,
                    modifiers=modifiers or [],
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_press_key(
            key: str,
            modifiers: list[str] | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Compatibility alias for browser_press."""
            return browser_press(key, modifiers, browser_handle, operation_id)

        @mcp.tool()
        def browser_scroll(
            delta_y: float,
            delta_x: float = 0,
            selector: str | None = None,
            ref: str | None = None,
            x: float | None = None,
            y: float | None = None,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Dispatch CSS-pixel wheel deltas at an element, coordinate, or viewport center."""
            return json.dumps(
                call(
                    "scroll",
                    delta_x=delta_x,
                    delta_y=delta_y,
                    selector=selector,
                    ref=ref,
                    x=x,
                    y=y,
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_drag(
            source_selector: str | None = None,
            target_selector: str | None = None,
            source_ref: str | None = None,
            target_ref: str | None = None,
            source_x: float | None = None,
            source_y: float | None = None,
            target_x: float | None = None,
            target_y: float | None = None,
            button: str | int = "left",
            steps: int = 10,
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Drag between selector/ref endpoints (preferred) or bounded viewport coordinates."""
            return json.dumps(
                call(
                    "drag",
                    source_selector=source_selector,
                    target_selector=target_selector,
                    source_ref=source_ref,
                    target_ref=target_ref,
                    source_x=source_x,
                    source_y=source_y,
                    target_x=target_x,
                    target_y=target_y,
                    button=button,
                    steps=steps,
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_actions(
            actions: list[dict[str, Any]],
            browser_handle: str | None = None,
            operation_id: str | None = None,
        ) -> str:
            """Atomically execute a bounded sequence of pointer/key/wheel/pause input primitives."""
            return json.dumps(
                call(
                    "actions",
                    actions=actions,
                    browser_handle=browser_handle,
                    operation_id=operation_id,
                )
            )

        @mcp.tool()
        def browser_back(
            browser_handle: str | None = None, operation_id: str | None = None
        ) -> str:
            """Navigate the isolated browser back one history entry."""
            return json.dumps(
                call("back", browser_handle=browser_handle, operation_id=operation_id)
            )

        @mcp.tool()
        def browser_screenshot(browser_handle: str | None = None):
            """Capture the current browser viewport as PNG."""
            from mcp.server.fastmcp import Image

            result = call("screenshot", browser_handle=browser_handle)
            return Image(data=base64.b64decode(result["data_base64"]), format="png")
