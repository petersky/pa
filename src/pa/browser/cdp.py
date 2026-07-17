"""Small Chrome DevTools Protocol client used by PA and its MCP subprocess."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
import websockets
from urllib.parse import urlparse


class CdpError(RuntimeError):
    pass


def validate_browser_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https", "about", "data"}:
        raise CdpError("Browser URLs must use http, https, about, or data")
    return url


async def list_targets(endpoint: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.get(f"{endpoint.rstrip('/')}/json/list")
        response.raise_for_status()
        return response.json()


async def page_target(endpoint: str, target_id: str | None = None) -> dict[str, Any]:
    targets = await list_targets(endpoint)
    pages = [target for target in targets if target.get("type") == "page"]
    if target_id:
        for target in pages:
            if target.get("id") == target_id:
                return target
    if not pages:
        raise CdpError("Browser has no page target")
    return pages[0]


class CdpPage:
    def __init__(self, endpoint: str, target_id: str | None = None) -> None:
        self.endpoint = endpoint.rstrip("/")
        self.target_id = target_id
        self._next_id = 0

    async def command(self, method: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        target = await page_target(self.endpoint, self.target_id)
        self.target_id = str(target["id"])
        websocket_url = str(target["webSocketDebuggerUrl"])
        self._next_id += 1
        command_id = self._next_id
        async with websockets.connect(websocket_url, open_timeout=5, close_timeout=2) as ws:
            await ws.send(json.dumps({"id": command_id, "method": method, "params": params or {}}))
            while True:
                message = json.loads(await asyncio.wait_for(ws.recv(), timeout=15))
                if message.get("id") != command_id:
                    continue
                if "error" in message:
                    raise CdpError(str(message["error"].get("message") or message["error"]))
                return dict(message.get("result") or {})

    async def evaluate(self, expression: str, *, await_promise: bool = True) -> Any:
        result = await self.command(
            "Runtime.evaluate",
            {
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": await_promise,
                "userGesture": True,
            },
        )
        remote = result.get("result") or {}
        if remote.get("subtype") == "error":
            raise CdpError(str(remote.get("description") or "JavaScript evaluation failed"))
        return remote.get("value")

    async def navigate(self, url: str) -> None:
        result = await self.command("Page.navigate", {"url": validate_browser_url(url)})
        if result.get("errorText"):
            raise CdpError(str(result["errorText"]))

    async def screenshot(self) -> bytes:
        result = await self.command("Page.captureScreenshot", {"format": "png", "fromSurface": True})
        import base64

        return base64.b64decode(result["data"])

    async def resize(self, width: int, height: int, *, device_scale_factor: float = 1) -> None:
        if not 320 <= width <= 7680 or not 240 <= height <= 4320:
            raise CdpError("Browser dimensions must be between 320x240 and 7680x4320")
        if not 0.25 <= device_scale_factor <= 4:
            raise CdpError("Browser device scale factor must be between 0.25 and 4")
        await self.command(
            "Emulation.setDeviceMetricsOverride",
            {
                "width": width,
                "height": height,
                "deviceScaleFactor": device_scale_factor,
                "mobile": False,
            },
        )

    async def viewport(self) -> dict[str, int | float]:
        viewport = await self.evaluate(
            "({width: window.innerWidth, height: window.innerHeight, "
            "device_scale_factor: window.devicePixelRatio})"
        )
        return {
            "width": int(viewport["width"]),
            "height": int(viewport["height"]),
            "device_scale_factor": float(viewport["device_scale_factor"]),
        }

    async def metadata(self) -> dict[str, Any]:
        target = await page_target(self.endpoint, self.target_id)
        self.target_id = str(target["id"])
        return {"target_id": self.target_id, "title": target.get("title", ""), "url": target.get("url", "")}
