"""Browser surface REST API and model-facing MCP tools."""

from __future__ import annotations

import atexit
import json
import os
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from pa.browser.cdp import CdpPage
from pa.browser.manager import BrowserAttachment, BrowserManager
from pa.core.context import AppContext
from pa.core.contracts import Module

router = APIRouter(prefix="/agent/sessions/{session_id}/browser")


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
        raise HTTPException(status_code=503, detail="Attached browser is unavailable") from exc
    return Response(image, media_type="image/png")


@router.post("/navigate")
async def browser_navigate(request: Request, session_id: str, body: NavigateBody) -> dict:
    await _page(request, session_id).navigate(body.url)
    return await _runtime(request, session_id).browser_state()


@router.post("/resize")
async def browser_resize(request: Request, session_id: str, body: ResizeBody) -> dict:
    attachment = _runtime(request, session_id).manager.browser.get(session_id)
    if not attachment:
        raise HTTPException(status_code=409, detail="No browser is attached")
    await attachment.resize(
        body.width,
        body.height,
        device_scale_factor=body.device_scale_factor,
    )
    return await attachment.state()


@router.post("/click")
async def browser_click(request: Request, session_id: str, body: ClickBody) -> dict:
    await _page(request, session_id).command("Input.dispatchMouseEvent", {"type": "mousePressed", "x": body.x, "y": body.y, "button": "left", "clickCount": 1})
    await _page(request, session_id).command("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": body.x, "y": body.y, "button": "left", "clickCount": 1})
    return {"ok": True}


@router.post("/type")
async def browser_type(request: Request, session_id: str, body: TypeBody) -> dict:
    await _page(request, session_id).command("Input.insertText", {"text": body.text})
    return {"ok": True}


class McpBrowserController:
    """Use a session-attached browser or own an agent-started headless browser."""

    def __init__(self, data_dir: Path) -> None:
        self.manager = BrowserManager(data_dir / "mcp-browser")
        self.attachment: BrowserAttachment | None = None
        self.session_key = str(uuid4())
        self.attributes: dict[str, int | float] = {}
        atexit.register(self.close)

    def close(self) -> None:
        if self.attachment and self.attachment.process.returncode is None:
            self.attachment.process.terminate()

    def page(self) -> CdpPage | None:
        endpoint = os.environ.get("PA_BROWSER_CDP_URL")
        if endpoint:
            return CdpPage(endpoint, os.environ.get("PA_BROWSER_TARGET_ID"))
        if self.attachment and self.attachment.process.returncode is None:
            return self.attachment.page
        return None

    async def ensure_page(
        self,
        *,
        url: str = "about:blank",
        width: int = 1440,
        height: int = 900,
        device_scale_factor: float = 1,
    ) -> CdpPage:
        page = self.page()
        if page:
            return page
        self.attachment = await self.manager.attach(
            self.session_key,
            url=url,
            width=width,
            height=height,
            device_scale_factor=device_scale_factor,
        )
        self.attributes = {
            "width": width,
            "height": height,
            "device_scale_factor": device_scale_factor,
        }
        return self.attachment.page

    async def state(self) -> dict:
        page = self.page()
        if not page:
            return {"attached": False}
        state = {"attached": True, **await page.metadata()}
        if self.attachment:
            state.update(
                width=self.attachment.width,
                height=self.attachment.height,
                device_scale_factor=self.attachment.device_scale_factor,
                owner="agent",
            )
        else:
            state["owner"] = "session"
            state.update(self.attributes)
        return state


_SNAPSHOT_JS = """(() => {
  const visible = el => { const r = el.getBoundingClientRect(); const s = getComputedStyle(el); return r.width > 0 && r.height > 0 && s.visibility !== 'hidden' && s.display !== 'none'; };
  return Array.from(document.querySelectorAll('a,button,input,textarea,select,[role],h1,h2,h3,p'))
    .filter(visible).slice(0, 300).map((el, index) => ({
      index, tag: el.tagName.toLowerCase(), role: el.getAttribute('role') || '',
      text: (el.innerText || el.value || el.getAttribute('aria-label') || '').trim().slice(0, 500),
      href: el.href || '', disabled: !!el.disabled
    }));
})()"""


class BrowserModule(Module):
    @property
    def name(self) -> str:
        return "browser"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Session-scoped browser surface and automation tools"

    def api_routers(self):
        return [("/api", router, ["browser"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        controller = McpBrowserController(ctx.settings.data_dir)

        @mcp.tool()
        async def browser_attach(
            url: str = "about:blank",
            width: int = 1440,
            height: int = 900,
            device_scale_factor: float = 1,
        ) -> str:
            """Attach or start PA's headless browser. The agent may call this without user action."""
            page = await controller.ensure_page(
                url=url,
                width=width,
                height=height,
                device_scale_factor=device_scale_factor,
            )
            await page.resize(width, height, device_scale_factor=device_scale_factor)
            if url != "about:blank":
                await page.navigate(url)
            controller.attributes = {
                "width": width,
                "height": height,
                "device_scale_factor": device_scale_factor,
            }
            return json.dumps(await controller.state())

        @mcp.tool()
        async def browser_state() -> str:
            """Return whether a PA browser is available and its current attributes."""
            return json.dumps(await controller.state())

        @mcp.tool()
        async def browser_open(url: str) -> str:
            """Open a URL in PA's browser, starting a default headless browser if needed."""
            page = await controller.ensure_page()
            await page.navigate(url)
            return json.dumps(await page.metadata())

        @mcp.tool()
        async def browser_resize(
            width: int,
            height: int,
            device_scale_factor: float = 1,
        ) -> str:
            """Set PA's browser viewport width, height, and device scale factor."""
            page = await controller.ensure_page(
                width=width,
                height=height,
                device_scale_factor=device_scale_factor,
            )
            await page.resize(width, height, device_scale_factor=device_scale_factor)
            if controller.attachment:
                controller.attachment.width = width
                controller.attachment.height = height
                controller.attachment.device_scale_factor = device_scale_factor
            controller.attributes = {
                "width": width,
                "height": height,
                "device_scale_factor": device_scale_factor,
            }
            return json.dumps(await controller.state())

        @mcp.tool()
        async def browser_detach() -> str:
            """Stop a browser started by the agent. Session-attached browsers remain user-owned."""
            if not controller.attachment:
                return json.dumps({"attached": bool(controller.page()), "detached": False, "owner": "session"})
            await controller.manager.detach(controller.session_key)
            controller.attachment = None
            controller.attributes = {}
            return json.dumps({"attached": False, "detached": True})

        @mcp.tool()
        async def browser_snapshot() -> str:
            """Return a compact snapshot of visible, interactive page content."""
            page = await controller.ensure_page()
            return json.dumps({"page": await page.metadata(), "elements": await page.evaluate(_SNAPSHOT_JS)}, ensure_ascii=False)

        @mcp.tool()
        async def browser_click(selector: str) -> str:
            """Click the first element matching a CSS selector."""
            expression = f"""(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('Element not found'); el.click(); return true; }})()"""
            await (await controller.ensure_page()).evaluate(expression)
            return "clicked"

        @mcp.tool()
        async def browser_type(selector: str, text: str, clear: bool = True) -> str:
            """Focus an input matched by CSS selector and enter text."""
            expression = f"""(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('Element not found'); el.focus(); if ({json.dumps(clear)}) el.value = ''; el.value += {json.dumps(text)}; el.dispatchEvent(new Event('input', {{bubbles:true}})); el.dispatchEvent(new Event('change', {{bubbles:true}})); return true; }})()"""
            await (await controller.ensure_page()).evaluate(expression)
            return "typed"

        @mcp.tool()
        async def browser_back() -> str:
            """Navigate the attached browser back one history entry."""
            await (await controller.ensure_page()).evaluate("history.back(); true")
            return "navigating back"

        @mcp.tool()
        async def browser_screenshot():
            """Capture the current attached browser viewport as a PNG image."""
            from mcp.server.fastmcp import Image

            return Image(data=await (await controller.ensure_page()).screenshot(), format="png")
