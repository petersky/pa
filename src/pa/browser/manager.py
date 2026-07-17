"""Lifecycle manager for browser surfaces attached to agent sessions."""

from __future__ import annotations

import asyncio
import logging
import os
import shutil
import socket
from dataclasses import dataclass
from pathlib import Path
from uuid import uuid4

import httpx

from pa.browser.cdp import CdpPage

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

    @property
    def page(self) -> CdpPage:
        return CdpPage(self.endpoint, self.target_id)

    def environment(self) -> dict[str, str]:
        return {
            "PA_BROWSER_CDP_URL": self.endpoint,
            "PA_BROWSER_TARGET_ID": self.target_id,
            "PA_BROWSER_ATTACHMENT_ID": self.id,
        }

    async def state(self) -> dict:
        metadata = await self.page.metadata()
        return {"attached": True, "id": self.id, **metadata}


class BrowserManager:
    def __init__(self, data_dir: Path) -> None:
        self.data_dir = data_dir
        self._attachments: dict[str, BrowserAttachment] = {}
        self._lock = asyncio.Lock()

    def get(self, session_id: str) -> BrowserAttachment | None:
        attachment = self._attachments.get(session_id)
        if attachment and attachment.process.returncode is None:
            return attachment
        return None

    async def attach(self, session_id: str, *, url: str = "about:blank") -> BrowserAttachment:
        async with self._lock:
            existing = self.get(session_id)
            if existing:
                if url and url != "about:blank":
                    await existing.page.navigate(url)
                return existing
            executable = _browser_executable()
            if not executable:
                raise RuntimeError(
                    "No Chromium browser found. Install Google Chrome/Chromium or set PA_BROWSER_EXECUTABLE."
                )
            attachment_id = str(uuid4())
            port = _free_port()
            profile_dir = self.data_dir / "browser" / session_id
            profile_dir.mkdir(parents=True, exist_ok=True)
            process = await asyncio.create_subprocess_exec(
                executable,
                "--headless=new",
                "--disable-background-networking",
                "--disable-component-update",
                "--disable-default-apps",
                "--disable-sync",
                "--no-first-run",
                "--remote-debugging-address=127.0.0.1",
                f"--remote-debugging-port={port}",
                f"--user-data-dir={profile_dir}",
                url,
                stdout=asyncio.subprocess.DEVNULL,
                stderr=asyncio.subprocess.DEVNULL,
            )
            endpoint = f"http://127.0.0.1:{port}"
            target = None
            async with httpx.AsyncClient(timeout=1) as client:
                for _ in range(40):
                    if process.returncode is not None:
                        break
                    try:
                        response = await client.get(f"{endpoint}/json/list")
                        pages = [item for item in response.json() if item.get("type") == "page"]
                        if pages:
                            target = pages[0]
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.1)
            if not target:
                process.terminate()
                await process.wait()
                raise RuntimeError("Chromium did not expose a browser page")
            attachment = BrowserAttachment(
                id=attachment_id,
                session_id=session_id,
                endpoint=endpoint,
                target_id=str(target["id"]),
                process=process,
                profile_dir=profile_dir,
            )
            self._attachments[session_id] = attachment
            return attachment

    async def detach(self, session_id: str) -> None:
        async with self._lock:
            attachment = self._attachments.pop(session_id, None)
            if not attachment:
                return
            if attachment.process.returncode is None:
                attachment.process.terminate()
                try:
                    await asyncio.wait_for(attachment.process.wait(), timeout=3)
                except asyncio.TimeoutError:
                    attachment.process.kill()
                    await attachment.process.wait()

    async def close(self) -> None:
        for session_id in list(self._attachments):
            await self.detach(session_id)
