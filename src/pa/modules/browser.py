"""Browser surface REST API and model-facing MCP tools."""

from __future__ import annotations

import json
import os
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import Response
from pydantic import BaseModel, Field

from pa.browser.cdp import CdpPage
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
        return await _runtime(request, session_id).set_browser_attached(True, url=body.url)
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
    return Response(await _page(request, session_id).screenshot(), media_type="image/png")


@router.post("/navigate")
async def browser_navigate(request: Request, session_id: str, body: NavigateBody) -> dict:
    await _page(request, session_id).navigate(body.url)
    return await _runtime(request, session_id).browser_state()


@router.post("/click")
async def browser_click(request: Request, session_id: str, body: ClickBody) -> dict:
    await _page(request, session_id).command("Input.dispatchMouseEvent", {"type": "mousePressed", "x": body.x, "y": body.y, "button": "left", "clickCount": 1})
    await _page(request, session_id).command("Input.dispatchMouseEvent", {"type": "mouseReleased", "x": body.x, "y": body.y, "button": "left", "clickCount": 1})
    return {"ok": True}


@router.post("/type")
async def browser_type(request: Request, session_id: str, body: TypeBody) -> dict:
    await _page(request, session_id).command("Input.insertText", {"text": body.text})
    return {"ok": True}


def _mcp_page() -> CdpPage:
    endpoint = os.environ.get("PA_BROWSER_CDP_URL")
    if not endpoint:
        raise RuntimeError("No PA browser is attached to this agent session")
    return CdpPage(endpoint, os.environ.get("PA_BROWSER_TARGET_ID"))


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
        @mcp.tool()
        async def browser_open(url: str) -> str:
            """Navigate the attached browser to an absolute URL."""
            await _mcp_page().navigate(url)
            return json.dumps(await _mcp_page().metadata())

        @mcp.tool()
        async def browser_snapshot() -> str:
            """Return a compact snapshot of visible, interactive page content."""
            page = _mcp_page()
            return json.dumps({"page": await page.metadata(), "elements": await page.evaluate(_SNAPSHOT_JS)}, ensure_ascii=False)

        @mcp.tool()
        async def browser_click(selector: str) -> str:
            """Click the first element matching a CSS selector."""
            expression = f"""(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('Element not found'); el.click(); return true; }})()"""
            await _mcp_page().evaluate(expression)
            return "clicked"

        @mcp.tool()
        async def browser_type(selector: str, text: str, clear: bool = True) -> str:
            """Focus an input matched by CSS selector and enter text."""
            expression = f"""(() => {{ const el = document.querySelector({json.dumps(selector)}); if (!el) throw new Error('Element not found'); el.focus(); if ({json.dumps(clear)}) el.value = ''; el.value += {json.dumps(text)}; el.dispatchEvent(new Event('input', {{bubbles:true}})); el.dispatchEvent(new Event('change', {{bubbles:true}})); return true; }})()"""
            await _mcp_page().evaluate(expression)
            return "typed"

        @mcp.tool()
        async def browser_back() -> str:
            """Navigate the attached browser back one history entry."""
            await _mcp_page().evaluate("history.back(); true")
            return "navigating back"

        @mcp.tool()
        async def browser_screenshot():
            """Capture the current attached browser viewport as a PNG image."""
            from mcp.server.fastmcp import Image

            return Image(data=await _mcp_page().screenshot(), format="png")
