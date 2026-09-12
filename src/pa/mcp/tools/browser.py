"""browser tools: authenticated owner API proxies."""

from __future__ import annotations

import base64
import json
import os
from typing import Any
from uuid import UUID
from pa.core.context import AppContext


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

def register_mcp(mcp, ctx: AppContext) -> None:
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
        operation_id: str | None = None,
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
                operation_id=operation_id,
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
    def browser_snapshot(
        browser_handle: str | None = None,
        operation_id: str | None = None,
    ) -> str:
        """Return visible content with target- and document-revision-bound element refs."""
        return json.dumps(
            call(
                "snapshot",
                browser_handle=browser_handle,
                operation_id=operation_id,
            ),
            ensure_ascii=False,
        )

    @mcp.tool()
    def browser_operation_outcome(
        operation_id: str, browser_handle: str | None = None
    ) -> str:
        """Return completed, running, or not_started for a retry-safe browser operation."""
        return json.dumps(
            call(
                "operation_outcome",
                operation_id=operation_id,
                browser_handle=browser_handle,
            )
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
        from mcp.server.mcpserver import Image

        result = call("screenshot", browser_handle=browser_handle)
        return Image(data=base64.b64decode(result["data_base64"]), format="png")
