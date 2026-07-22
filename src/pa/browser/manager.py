"""Lifecycle manager for browser surfaces attached to agent sessions."""

from __future__ import annotations

import asyncio
import logging
import os
import signal
import shutil
import socket
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx

from pa.browser.cdp import CdpPage
from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)


def _browser_executable() -> str | None:
    override = os.environ.get("PA_BROWSER_EXECUTABLE")
    if override and Path(override).is_file():
        return override
    candidates = [
        shutil.which("google-chrome"),
        shutil.which("chromium"),
        shutil.which("chromium-browser"),
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge",
    ]
    return next((str(path) for path in candidates if path and Path(path).is_file()), None)


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


@dataclass
class BrowserAttachment:
    id: str
    session_id: str
    endpoint: str
    target_id: str
    process: asyncio.subprocess.Process
    profile_dir: Path
    width: int = 1440
    height: int = 900
    device_scale_factor: float = 1

    @property
    def page(self) -> CdpPage:
        return CdpPage(self.endpoint, self.target_id)

    def environment(self) -> dict[str, str]:
        return {
            "PA_BROWSER_CDP_URL": self.endpoint,
            "PA_BROWSER_TARGET_ID": self.target_id,
            "PA_BROWSER_ATTACHMENT_ID": self.id,
            "PA_BROWSER_SESSION_ID": self.session_id,
        }

    async def state(self) -> dict:
        page = self.page
        metadata = await page.metadata()
        viewport = await page.viewport()
        self.width = int(viewport["width"])
        self.height = int(viewport["height"])
        self.device_scale_factor = float(viewport["device_scale_factor"])
        return {
            "attached": True,
            "id": self.id,
            "width": self.width,
            "height": self.height,
            "device_scale_factor": self.device_scale_factor,
            **metadata,
        }

    async def resize(self, width: int, height: int, *, device_scale_factor: float = 1) -> None:
        await self.page.resize(width, height, device_scale_factor=device_scale_factor)
        self.width = width
        self.height = height
        self.device_scale_factor = device_scale_factor


class BrowserManager:
    def __init__(
        self, data_dir: Path, *, async_runtime: AsyncRuntime | None = None
    ) -> None:
        self.data_dir = data_dir
        self.async_runtime = async_runtime
        self._attachments: dict[str, BrowserAttachment] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._startup_slots = asyncio.Semaphore(2)
        self._client: httpx.AsyncClient | None = None

    def _lock_for(self, session_id: str) -> asyncio.Lock:
        return self._locks.setdefault(session_id, asyncio.Lock())

    async def _offload(self, operation: str, call, *args):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=10.0
            )
        return await asyncio.to_thread(call, *args)

    def _http_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(
                timeout=httpx.Timeout(connect=0.5, read=0.5, write=0.5, pool=0.5),
                limits=httpx.Limits(max_connections=4, max_keepalive_connections=2),
            )
        return self._client

    def get(self, session_id: str) -> BrowserAttachment | None:
        attachment = self._attachments.get(session_id)
        if attachment and attachment.process.returncode is None:
            return attachment
        return None

    async def attach(
        self,
        session_id: str,
        *,
        url: str = "about:blank",
        width: int | None = None,
        height: int | None = None,
        device_scale_factor: float = 1,
    ) -> BrowserAttachment:
        async with self._lock_for(session_id):
            width = width or int(os.environ.get("PA_BROWSER_WIDTH", "1440"))
            height = height or int(os.environ.get("PA_BROWSER_HEIGHT", "900"))
            if not 320 <= width <= 7680 or not 240 <= height <= 4320:
                raise ValueError("Browser dimensions must be between 320x240 and 7680x4320")
            if not 0.25 <= device_scale_factor <= 4:
                raise ValueError("Browser device scale factor must be between 0.25 and 4")
            existing = self.get(session_id)
            if existing:
                if (width, height, device_scale_factor) != (
                    existing.width,
                    existing.height,
                    existing.device_scale_factor,
                ):
                    await existing.resize(width, height, device_scale_factor=device_scale_factor)
                if url and url != "about:blank":
                    await existing.page.navigate(url)
                return existing
            executable = await self._offload(
                "browser.executable_lookup", _browser_executable
            )
            if not executable:
                raise RuntimeError(
                    "No Chromium browser found. Install Google Chrome/Chromium or set PA_BROWSER_EXECUTABLE."
                )
            attachment_id = str(uuid4())
            port = await self._offload("browser.free_port", _free_port)
            profile_dir = self.data_dir / "browser" / session_id
            await self._offload(
                "browser.profile_mkdir",
                lambda: profile_dir.mkdir(parents=True, exist_ok=True),
            )
            process: asyncio.subprocess.Process | None = None
            async with self._startup_slots:
                try:
                    process = await asyncio.create_subprocess_exec(
                        executable,
                        "--headless=new",
                        "--disable-background-networking",
                        "--disable-component-update",
                        "--disable-default-apps",
                        "--disable-sync",
                        "--no-first-run",
                        f"--window-size={width},{height}",
                        "--remote-debugging-address=127.0.0.1",
                        f"--remote-debugging-port={port}",
                        f"--user-data-dir={profile_dir}",
                        url,
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                        start_new_session=os.name == "posix",
                    )
                    endpoint = f"http://127.0.0.1:{port}"
                    target = None
                    client = self._http_client()
                    async with asyncio.timeout(6.0):
                        for _ in range(50):
                            if process.returncode is not None:
                                break
                            try:
                                response = await client.get(f"{endpoint}/json/list")
                                pages = [
                                    item
                                    for item in response.json()
                                    if item.get("type") == "page"
                                ]
                                if pages:
                                    target = pages[0]
                                    break
                            except (httpx.HTTPError, ValueError):
                                pass
                            await asyncio.sleep(0.1)
                    if not target:
                        raise RuntimeError("Chromium did not expose a browser page")
                    attachment = BrowserAttachment(
                        id=attachment_id,
                        session_id=session_id,
                        endpoint=endpoint,
                        target_id=str(target["id"]),
                        process=process,
                        profile_dir=profile_dir,
                        width=width,
                        height=height,
                        device_scale_factor=device_scale_factor,
                    )
                    await attachment.resize(
                        width,
                        height,
                        device_scale_factor=device_scale_factor,
                    )
                    self._attachments[session_id] = attachment
                    return attachment
                except BaseException:
                    if process is not None:
                        await self._terminate_process(process)
                    raise

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        if process.returncode is not None:
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGTERM)
            else:  # pragma: no cover - Windows fallback
                process.terminate()
        except ProcessLookupError:
            return
        try:
            await asyncio.wait_for(process.wait(), timeout=3.0)
            return
        except asyncio.TimeoutError:
            pass
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:  # pragma: no cover - Windows fallback
                process.kill()
        except ProcessLookupError:
            return
        await asyncio.wait_for(process.wait(), timeout=3.0)

    async def detach(self, session_id: str) -> None:
        async with self._lock_for(session_id):
            attachment = self._attachments.pop(session_id, None)
            if not attachment:
                return
            await self._terminate_process(attachment.process)

    async def close(self) -> None:
        await asyncio.gather(
            *(self.detach(session_id) for session_id in list(self._attachments)),
            return_exceptions=True,
        )
        if self._client is not None:
            await self._client.aclose()
            self._client = None
