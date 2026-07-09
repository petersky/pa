"""CLI helpers for ACP quiesce/resume around service lifecycle."""

from __future__ import annotations

import sys
import time
from typing import Any

import httpx

from pa.config import Settings


def _base_url(settings: Settings) -> str:
    return f"http://{settings.host}:{settings.port}"


def _supports_carriage_return() -> bool:
    return bool(sys.stdout.isatty())


class _StatusLine:
    def __init__(self) -> None:
        self._use_cr = _supports_carriage_return()
        self._last = ""

    def update(self, text: str) -> None:
        text = text.replace("\n", " ").strip()
        if self._use_cr:
            pad = max(0, len(self._last) - len(text))
            sys.stdout.write("\r" + text + (" " * pad))
            sys.stdout.flush()
            self._last = text
        else:
            if text != self._last:
                print(text)
                self._last = text

    def finish(self, text: str | None = None) -> None:
        if text:
            if self._use_cr and self._last:
                sys.stdout.write("\r" + text + (" " * max(0, len(self._last) - len(text))) + "\n")
                sys.stdout.flush()
            else:
                print(text)
        elif self._use_cr and self._last:
            sys.stdout.write("\n")
            sys.stdout.flush()
        self._last = ""


def agent_runtime_status(settings: Settings) -> dict[str, Any] | None:
    try:
        with httpx.Client(timeout=3.0) as client:
            resp = client.get(f"{_base_url(settings)}/api/agent/status")
            if resp.status_code != 200:
                return None
            return resp.json()
    except httpx.HTTPError:
        return None


def quiesce_running_agent(
    settings: Settings,
    *,
    reason: str = "restart",
    timeout: float = 300.0,
) -> dict[str, Any] | None:
    """Ask the running server to quiesce ACP sessions. Returns final status or None."""
    status = agent_runtime_status(settings)
    if status is None:
        print("PA server not reachable; skipping ACP quiesce.")
        return None

    active = int(status.get("active_sessions") or 0)
    prompting = bool(status.get("prompting"))
    queued = int(status.get("queued_prompts") or 0)
    if active == 0 and not prompting and queued == 0:
        print("No active ACP sessions.")
        # Still start quiesce so accepting_prompts flips and snapshot is clean.
    else:
        parts = [f"{active} ACP session{'s' if active != 1 else ''}"]
        if prompting:
            parts.append("1 actively working")
        if queued:
            parts.append(f"{queued} queued prompt{'s' if queued != 1 else ''}")
        print("Quiescing " + ", ".join(parts) + ".")

    line = _StatusLine()
    try:
        with httpx.Client(timeout=timeout + 30.0) as client:
            start = client.post(
                f"{_base_url(settings)}/api/agent/quiesce",
                json={"reason": reason, "timeout": timeout, "wait": False},
            )
            if start.status_code >= 400:
                detail = start.json().get("detail") if start.headers.get("content-type", "").startswith("application/json") else start.text
                print(f"Failed to start ACP quiesce: {detail}", file=sys.stderr)
                return None

            deadline = time.monotonic() + timeout
            final: dict[str, Any] | None = None
            while time.monotonic() < deadline:
                resp = client.get(f"{_base_url(settings)}/api/agent/quiesce")
                if resp.status_code != 200:
                    time.sleep(0.4)
                    continue
                data = resp.json()
                final = data
                msg = data.get("message") or "Quiescing ACP sessions…"
                active_n = data.get("active_sessions", 0)
                queued_n = data.get("queued_prompts", 0)
                line.update(
                    f"{msg}  (sessions={active_n}, queued={queued_n})"
                )
                if data.get("done"):
                    break
                time.sleep(0.4)
            else:
                line.finish("Timed out waiting for ACP quiesce.")
                return final

            if final and final.get("error"):
                line.finish(f"ACP quiesce failed: {final['error']}")
            else:
                line.finish(final.get("message") if final else "ACP quiesce complete.")
            return final
    except httpx.HTTPError as exc:
        line.finish(f"ACP quiesce request failed: {exc}")
        return None


def mark_no_resume(settings: Settings) -> None:
    from pa.instance.quiesce import mark_snapshot_no_resume

    mark_snapshot_no_resume(settings.data_dir)
