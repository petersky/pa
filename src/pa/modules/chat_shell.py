"""`!command` in the chat composer: run an OS command in the conversation's working directory.

The composer posts the command here, shows the result in the transcript, and (by setting, or
per message with `!!`) hands the command and its output to the agent as an ordinary prompt.

Guard rails, since this runs as the PA server user:
- access is a setting (`chat_shell_access`: anyone / signed_in / disabled) and is enforced here,
  not only in the UI; the caller must also own the conversation (or be an admin);
- the command runs in the session's own working directory, one at a time per conversation,
  with no stdin, a hard timeout that kills the whole process group, and bounded output;
- PA's own secrets (`PA_*` variables that look like credentials) are not passed down.
"""
from __future__ import annotations

import asyncio
import codecs
import contextlib
import os
import re
import shutil
import signal
import time
from collections import OrderedDict, deque
from pathlib import Path
from typing import Any
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id, require_user

router = APIRouter(prefix="/agent/sessions")

MAX_COMMAND_CHARS = 20_000
KILL_GRACE_SECONDS = 2.0
READ_CHUNK = 65_536
_SECRET_NAME = re.compile(r"(SECRET|TOKEN|PASSWORD|PASSWD|API_?KEY|CREDENTIAL|PRIVATE_KEY)", re.I)
_ANSI = re.compile(r"\x1b(?:\[[0-?]*[ -/]*[@-~]|\][^\x07\x1b]*(?:\x07|\x1b\\)|[@-Z\\-_])")

_running: dict[str, asyncio.subprocess.Process] = {}
_stopped: set[str] = set()
_recent: "OrderedDict[tuple[str, str], dict[str, Any]]" = OrderedDict()
RECENT_RESULTS = 64


class ShellBody(BaseModel):
    command: str = Field(min_length=1, max_length=MAX_COMMAND_CHARS)
    # None follows the instance default; the composer sends the explicit choice for `!!`.
    send_to_agent: bool | None = None
    # Lets a retried request return the first result instead of running the command twice.
    client_id: str | None = Field(default=None, max_length=80)


# --------------------------------------------------------------------------- policy


def shell_available(request: Request) -> bool:
    """Whether the composer should treat a leading `!` as a command for this caller."""
    if os.name != "posix":
        return False
    settings = request.app.state.ctx.settings
    access = settings.chat_shell_access
    if access == "disabled":
        return False
    if access == "signed_in":
        return bool(getattr(request.state, "user_authenticated", False))
    return True


def shell_to_agent_default(request: Request) -> bool:
    return bool(request.app.state.ctx.settings.chat_shell_output_to_agent)


def shell_template_context(request: Request) -> dict[str, Any]:
    return {
        "shell_enabled": shell_available(request),
        "shell_to_agent": shell_to_agent_default(request),
    }


def _require_access(request: Request) -> None:
    if os.name != "posix":
        raise HTTPException(501, "Shell commands are not supported on this platform")
    access = request.app.state.ctx.settings.chat_shell_access
    if access == "disabled":
        raise HTTPException(403, {"code": "shell_disabled", "message": "Shell commands are turned off on this instance."})
    if access == "signed_in" and not getattr(request.state, "user_authenticated", False):
        raise HTTPException(401, {"code": "shell_requires_login", "message": "Sign in to run shell commands."})


# --------------------------------------------------------------------------- running


def _shell_program() -> str:
    for candidate in ("/bin/bash", "/usr/bin/bash", shutil.which("bash") or "", "/bin/sh"):
        if candidate and os.access(candidate, os.X_OK):
            return candidate
    return "/bin/sh"


def child_environment(base: dict[str, str] | None = None) -> dict[str, str]:
    """The server's environment minus PA's own credentials, tuned for non-interactive output."""
    env = {
        key: value
        for key, value in (os.environ if base is None else base).items()
        if not (key.upper().startswith("PA_") and _SECRET_NAME.search(key))
    }
    env.update({"TERM": "dumb", "NO_COLOR": "1", "PAGER": "cat", "GIT_PAGER": "cat", "GIT_TERMINAL_PROMPT": "0"})
    return env


class _Capture:
    """Keeps the start and the end of the output; the middle is what gets dropped."""

    def __init__(self, limit: int) -> None:
        self.half = max(limit // 2, 1)
        self.head = ""
        self.tail: deque[str] = deque()
        self.tail_size = 0
        self.total = 0
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")

    def feed(self, data: bytes, *, final: bool = False) -> None:
        text = _ANSI.sub("", self._decoder.decode(data, final=final)).replace("\r\n", "\n")
        if not text:
            return
        self.total += len(text)
        room = self.half - len(self.head)
        if room > 0:
            self.head += text[:room]
            text = text[room:]
        if not text:
            return
        self.tail.append(text)
        self.tail_size += len(text)
        while self.tail_size - len(self.tail[0]) >= self.half:
            self.tail_size -= len(self.tail.popleft())

    def result(self) -> tuple[str, bool, int]:
        tail = "".join(self.tail)[-self.half:]
        kept = len(self.head) + len(tail)
        omitted = max(self.total - kept, 0)
        if not omitted:
            return self.head + tail, False, 0
        return f"{self.head}\n… {omitted} characters omitted …\n{tail}", True, omitted


async def _terminate(proc: asyncio.subprocess.Process) -> None:
    for sig, grace in ((signal.SIGTERM, KILL_GRACE_SECONDS), (signal.SIGKILL, 5.0)):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(proc.pid, sig)
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(proc.wait(), grace)
            return


async def run_command(
    session_id: str, command: str, cwd: str, *, timeout: float, limit: int,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    capture = _Capture(limit)
    started = time.monotonic()
    proc = await asyncio.create_subprocess_exec(
        _shell_program(), "-c", command,
        cwd=cwd, env=child_environment(env),
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    _running[session_id] = proc
    _stopped.discard(session_id)
    timed_out = False

    async def pump() -> None:
        assert proc.stdout is not None
        while True:
            chunk = await proc.stdout.read(READ_CHUNK)
            if not chunk:
                break
            capture.feed(chunk)
        await proc.wait()

    try:
        await asyncio.wait_for(pump(), timeout)
    except asyncio.TimeoutError:
        timed_out = True
        await _terminate(proc)
    except asyncio.CancelledError:
        await _terminate(proc)
        raise
    finally:
        if _running.get(session_id) is proc:
            del _running[session_id]
        stopped = session_id in _stopped
        _stopped.discard(session_id)
    capture.feed(b"", final=True)
    output, truncated, omitted = capture.result()
    code = proc.returncode
    killed_by = -code if code is not None and code < 0 else None
    return {
        "exit_code": None if killed_by else code,
        "signal": signal.Signals(killed_by).name if killed_by else None,
        "timed_out": timed_out,
        "cancelled": bool(killed_by and not timed_out and stopped),
        "output": output,
        "truncated": truncated,
        "omitted_chars": omitted,
        "duration_ms": int((time.monotonic() - started) * 1000),
    }


# --------------------------------------------------------------------------- prompt text


def _fence(text: str) -> str:
    longest = max((len(run) for run in re.findall(r"`+", text)), default=0)
    return "`" * max(3, longest + 1)


def _status_line(result: dict[str, Any]) -> str:
    seconds = result["duration_ms"] / 1000
    if result["timed_out"]:
        return f"timed out after {seconds:.0f}s and was stopped"
    if result["cancelled"]:
        return f"stopped by the user after {seconds:.1f}s"
    if result["signal"]:
        return f"killed by {result['signal']} after {seconds:.1f}s"
    return f"exit {result['exit_code']} in {seconds:.1f}s"


def format_for_agent(result: dict[str, Any]) -> str:
    """The prompt the agent receives (and the user sees as their own message)."""
    output = result["output"].rstrip("\n")
    body = output if output else "(no output)"
    fence = _fence(body + result["command"])
    return (
        f"I ran this in the terminal (working directory `{result['cwd']}`):\n\n"
        f"{fence}sh\n$ {result['command']}\n{fence}\n\n"
        f"Result: {_status_line(result)}"
        f"{' · output shortened' if result['truncated'] else ''}\n\n"
        f"{fence}\n{body}\n{fence}"
    )


# --------------------------------------------------------------------------- routes


async def _load_session(request: Request, session_id: str):
    from pa.modules.agent_chat import _manager, _offload

    user = require_user(request)
    manager = _manager(request)
    session = await _offload(manager, "shell.session", manager.store.get_session, session_id)
    if not session:
        raise HTTPException(404, "Conversation not found")
    if session.principal_id not in {None, get_principal_id(request)} and getattr(user, "role", None) != "admin":
        raise HTTPException(403, "Conversation belongs to another principal")
    return session


@router.post("/{session_id}/shell")
async def run_shell(request: Request, session_id: str, body: ShellBody) -> dict[str, Any]:
    from pa.modules.agent_chat import _runtime_or_404_async

    _require_access(request)
    session = await _load_session(request, session_id)
    command = body.command.strip()
    if not command:
        raise HTTPException(400, "A command is required")
    if body.client_id:
        earlier = _recent.get((session_id, body.client_id))
        if earlier:
            return {**earlier, "duplicate": True}
    if session_id in _running:
        raise HTTPException(409, {"code": "shell_busy", "message": "A command is already running in this conversation."})
    cwd = session.cwd
    if not cwd or not Path(cwd).is_dir():
        raise HTTPException(409, {"code": "shell_no_workspace", "message": "This conversation has no working directory to run a command in."})
    runtime = await _runtime_or_404_async(request, session_id)
    settings = request.app.state.ctx.settings
    to_agent = shell_to_agent_default(request) if body.send_to_agent is None else body.send_to_agent

    result = await run_command(
        session_id, command, cwd,
        timeout=float(settings.chat_shell_timeout_seconds),
        limit=int(settings.chat_shell_output_char_limit),
    )
    result.update({"id": str(uuid4()), "command": command, "cwd": cwd, "sent_to_agent": to_agent})
    if to_agent:
        # The agent's prompt carries the output, so it is not also recorded as a shell event.
        result["prompt"] = format_for_agent(result)
    else:
        runtime._append_transcript("shell_result", {k: v for k, v in result.items() if k != "prompt"})
        await runtime._drain_transcripts()
    result["duplicate"] = False
    if body.client_id:
        _recent[(session_id, body.client_id)] = result
        while len(_recent) > RECENT_RESULTS:
            _recent.popitem(last=False)
    return result


@router.delete("/{session_id}/shell")
async def stop_shell(request: Request, session_id: str) -> dict[str, Any]:
    _require_access(request)
    await _load_session(request, session_id)
    proc = _running.get(session_id)
    if not proc:
        return {"stopped": False}
    _stopped.add(session_id)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(proc.pid, signal.SIGTERM)
    return {"stopped": True}
