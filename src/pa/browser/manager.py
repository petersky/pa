"""Lifecycle manager for browser surfaces attached to agent sessions."""

from __future__ import annotations

import asyncio
import atexit
import json
import logging
import os
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from dataclasses import dataclass, field
from pathlib import Path
from uuid import uuid4

import httpx

from pa.browser.cdp import CdpPage, validate_browser_url
from pa.core.async_runtime import AsyncRuntime

logger = logging.getLogger(__name__)

BROWSER_STARTUP_TIMEOUT_SECONDS = 12.0
BROWSER_STARTUP_MAX_ATTEMPTS = 100


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
    return next(
        (str(path) for path in candidates if path and Path(path).is_file()), None
    )


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


# Browsers are launched in their own session so that terminal signals aimed at
# the owner do not reach them. The cost is that a browser outlives an owner that
# is killed without running its cleanup (a timed-out or SIGKILLed test run, a
# crashed worker). On macOS such a leftover headless Chrome also registers as the
# running "Google Chrome" application, so the user's next normal launch only
# re-activates the windowless process and "quit" never completes. To bound that,
# every launch is recorded in a small per-user registry. Later launches reap
# browsers whose owner is gone, and a normal interpreter exit terminates the
# browsers it still owns.
_REGISTRY_ENV = "PA_BROWSER_REGISTRY_DIR"
_REAP_GRACE_SECONDS = 3.0

_owned_browsers: dict[int, str] = {}
_reaped_this_process = False


def _registry_dir() -> Path:
    override = os.environ.get(_REGISTRY_ENV)
    if override:
        return Path(override)
    uid = os.getuid() if hasattr(os, "getuid") else 0
    return Path(tempfile.gettempdir()) / f"pa-browser-registry-{uid}"


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _ps_field(pid: int, field_name: str) -> str:
    try:
        result = subprocess.run(
            ["ps", "-o", f"{field_name}=", "-p", str(pid)],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return result.stdout.strip() if result.returncode == 0 else ""


def _profile_marker(profile_dir: str | Path) -> str:
    return f"--user-data-dir={profile_dir}"


def _is_registered_browser(pid: int, profile_dir: str) -> bool:
    """True when ``pid`` is still the browser launched for ``profile_dir``.

    Guards against signalling an unrelated process that reused the pid.
    """
    return _profile_marker(profile_dir) in _ps_field(pid, "command")


def _terminate_browsers(
    browsers: dict[int, str], grace: float = _REAP_GRACE_SECONDS
) -> list[int]:
    """SIGTERM then SIGKILL the verified browser process groups in ``browsers``."""
    targets = [
        pid
        for pid, profile_dir in browsers.items()
        if _pid_alive(pid) and _is_registered_browser(pid, profile_dir)
    ]

    def signal_all(sig: int) -> None:
        for pid in targets:
            try:
                if os.getpgid(pid) == pid:
                    os.killpg(pid, sig)
                else:
                    os.kill(pid, sig)
            except (ProcessLookupError, PermissionError):
                continue

    signal_all(signal.SIGTERM)
    deadline = time.monotonic() + grace
    while time.monotonic() < deadline and any(_pid_alive(pid) for pid in targets):
        time.sleep(0.05)
    if any(_pid_alive(pid) for pid in targets):
        signal_all(signal.SIGKILL)
    return targets


def _register_browser(pid: int, profile_dir: Path) -> None:
    _owned_browsers[pid] = str(profile_dir)
    if os.name != "posix":  # pragma: no cover - registry is POSIX-only
        return
    try:
        registry = _registry_dir()
        registry.mkdir(parents=True, exist_ok=True)
        record = {
            "pid": pid,
            "profile_dir": str(profile_dir),
            "owner_pid": os.getpid(),
            "owner_started": _ps_field(os.getpid(), "lstart"),
        }
        temporary = registry / f".{pid}.tmp"
        temporary.write_text(json.dumps(record))
        os.replace(temporary, registry / f"{pid}.json")
    except OSError:
        logger.debug("Could not record browser %s in the registry", pid, exc_info=True)


def _unregister_browser(pid: int) -> None:
    _owned_browsers.pop(pid, None)
    try:
        (_registry_dir() / f"{pid}.json").unlink(missing_ok=True)
    except OSError:
        pass


def reap_orphaned_browsers() -> list[int]:
    """Terminate registered browsers whose owning process no longer exists."""
    if os.name != "posix":  # pragma: no cover - registry is POSIX-only
        return []
    registry = _registry_dir()
    try:
        entries = sorted(registry.glob("*.json"))
    except OSError:
        return []
    orphans: dict[int, str] = {}
    stale: list[Path] = []
    for entry in entries:
        try:
            record = json.loads(entry.read_text())
            pid = int(record["pid"])
            profile_dir = str(record["profile_dir"])
            owner_pid = int(record["owner_pid"])
            owner_started = str(record.get("owner_started", ""))
        except (OSError, ValueError, KeyError, TypeError):
            stale.append(entry)
            continue
        if owner_pid == os.getpid():
            continue
        owner_alive = (
            _pid_alive(owner_pid)
            and bool(owner_started)
            and _ps_field(owner_pid, "lstart") == owner_started
        )
        if owner_alive:
            continue
        stale.append(entry)
        orphans[pid] = profile_dir
    reaped = _terminate_browsers(orphans) if orphans else []
    for entry in stale:
        try:
            entry.unlink(missing_ok=True)
        except OSError:
            pass
    if reaped:
        logger.warning(
            "Terminated %d orphaned browser process(es) left by a previous run: %s",
            len(reaped),
            reaped,
        )
    return reaped


def _terminate_owned_browsers_at_exit() -> None:
    if not _owned_browsers:
        return
    owned = dict(_owned_browsers)
    try:
        _terminate_browsers(owned, grace=1.5)
    finally:
        for pid in owned:
            _unregister_browser(pid)


atexit.register(_terminate_owned_browsers_at_exit)


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
    _page: CdpPage = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._page = CdpPage(self.endpoint, self.target_id)

    @property
    def page(self) -> CdpPage:
        return self._page

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

    async def resize(
        self, width: int, height: int, *, device_scale_factor: float = 1
    ) -> None:
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
        url = validate_browser_url(url)
        async with self._lock_for(session_id):
            width = width or int(os.environ.get("PA_BROWSER_WIDTH", "1440"))
            height = height or int(os.environ.get("PA_BROWSER_HEIGHT", "900"))
            if not 320 <= width <= 7680 or not 240 <= height <= 4320:
                raise ValueError(
                    "Browser dimensions must be between 320x240 and 7680x4320"
                )
            if not 0.25 <= device_scale_factor <= 4:
                raise ValueError(
                    "Browser device scale factor must be between 0.25 and 4"
                )
            existing = self.get(session_id)
            if existing:
                if (width, height, device_scale_factor) != (
                    existing.width,
                    existing.height,
                    existing.device_scale_factor,
                ):
                    await existing.resize(
                        width, height, device_scale_factor=device_scale_factor
                    )
                if url and url != "about:blank":
                    await existing.page.navigate(url)
                return existing
            global _reaped_this_process
            if not _reaped_this_process:
                _reaped_this_process = True
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(reap_orphaned_browsers),
                        timeout=_REAP_GRACE_SECONDS * 3,
                    )
                except Exception:
                    logger.debug("Orphaned browser reaping failed", exc_info=True)
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
            attachment: BrowserAttachment | None = None
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
                        "about:blank",
                        stdout=asyncio.subprocess.DEVNULL,
                        stderr=asyncio.subprocess.DEVNULL,
                        start_new_session=os.name == "posix",
                    )
                    if isinstance(getattr(process, "pid", None), int):
                        _register_browser(process.pid, profile_dir)
                    endpoint = f"http://127.0.0.1:{port}"
                    target = None
                    client = self._http_client()
                    try:
                        async with asyncio.timeout(BROWSER_STARTUP_TIMEOUT_SECONDS):
                            for _ in range(BROWSER_STARTUP_MAX_ATTEMPTS):
                                if process.returncode is not None:
                                    break
                                try:
                                    response = await client.get(
                                        f"{endpoint}/json/list"
                                    )
                                    pages = [
                                        item
                                        for item in response.json()
                                        if item.get("type") == "page"
                                    ]
                                    if pages:
                                        target = pages[0]
                                        break
                                except httpx.HTTPError, ValueError:
                                    pass
                                await asyncio.sleep(0.1)
                    except TimeoutError:
                        # A busy host can accept the debugging connection before
                        # Chromium finishes serving targets. Preserve the fallback
                        # page creation and the actionable diagnostic below.
                        pass
                    if not target and process.returncode is None:
                        try:
                            response = await client.put(
                                f"{endpoint}/json/new?about:blank"
                            )
                            created = response.json()
                            if created.get("type") == "page":
                                target = created
                        except httpx.HTTPError, ValueError:
                            pass
                    if not target:
                        state = (
                            f"exited with code {process.returncode}"
                            if process.returncode is not None
                            else "kept running without a page target"
                        )
                        raise RuntimeError(
                            "Chromium started but did not expose a usable page within "
                            f"{BROWSER_STARTUP_TIMEOUT_SECONDS:g} seconds ({state}, "
                            f"endpoint={endpoint}). Check that the "
                            "configured executable supports --headless=new and remote "
                            "debugging, and that its profile directory is writable."
                        )
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
                    # Launch targets can still expose the completed blank
                    # document while their requested navigation is pending.
                    # Send the requested navigation once over the established
                    # CDP connection before publishing this attachment.
                    if url != "about:blank":
                        await attachment.page.navigate(url)
                    self._attachments[session_id] = attachment
                    return attachment
                except BaseException:
                    try:
                        if attachment is not None:
                            await attachment.page.close()
                    finally:
                        if process is not None:
                            await self._terminate_process(process)
                    raise

    async def _terminate_process(self, process: asyncio.subprocess.Process) -> None:
        try:
            await self._terminate_process_group(process)
        finally:
            if isinstance(getattr(process, "pid", None), int):
                _unregister_browser(process.pid)

    async def _terminate_process_group(
        self, process: asyncio.subprocess.Process
    ) -> None:
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
            try:
                await attachment.page.close()
            finally:
                await self._terminate_process(attachment.process)

    async def close(self) -> None:
        await asyncio.gather(
            *(self.detach(session_id) for session_id in list(self._attachments)),
            return_exceptions=True,
        )
        if self._client is not None:
            await self._client.aclose()
            self._client = None
