"""Bounded, redacted Codex CLI device-auth jobs.

Credentials are written by Codex itself to the target OS user's credential store.
PA persists only public device-flow instructions and lifecycle events.
"""

from __future__ import annotations

import os
import queue
import re
import selectors
import shutil
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json
from pa.packaging.paths import resolve_executable

DEFAULT_LOGIN_TIMEOUT_S = 600
MIN_LOGIN_TIMEOUT_S = 60
MAX_LOGIN_TIMEOUT_S = 1800
NO_OUTPUT_TIMEOUT_S = 30
NO_INSTRUCTIONS_TIMEOUT_S = 90
MAX_CAPTURE_CHARS = 16_384
MAX_EVENT_CHARS = 500
MAX_EVENTS = 200

_URL_RE = re.compile(r"https://[^\s<>\]\[\"']{1,2048}", re.IGNORECASE)
_CODE_RE = re.compile(
    r"(?i)(?:code(?:\s+is)?|enter|use)\s*[:\-]?\s*"
    r"([A-Z0-9]{4}(?:[ -][A-Z0-9]{4}){1,4})|"
    r"(?<![A-Z0-9])([A-Z0-9]{4}(?:-[A-Z0-9]{4}){1,4})(?![A-Z0-9])"
)
_ANSI_CSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
_ANSI_OSC_RE = re.compile(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)")
_ANSI_SINGLE_RE = re.compile(r"\x1b[@-_]")
_SECRET_VALUE_RE = re.compile(
    r"(?i)\b(access[_ -]?token|refresh[_ -]?token|id[_ -]?token|api[_ -]?key|authorization)\b(\s*[:=]\s*)(?:bearer\s+)?\S+"
)
_BEARER_RE = re.compile(r"(?i)bearer\s+[A-Za-z0-9._~+/=-]+")


class LoginState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    WAITING_FOR_USER = "waiting_for_user"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed_out"
    INTERRUPTED = "interrupted"


TERMINAL_STATES = {
    LoginState.SUCCEEDED,
    LoginState.FAILED,
    LoginState.CANCELLED,
    LoginState.TIMED_OUT,
    LoginState.INTERRUPTED,
}


class LoginEvent(BaseModel):
    sequence: int
    timestamp: str
    type: str
    message: str


class CodexLoginJob(BaseModel):
    job_id: str
    provider_id: str = "codex"
    state: LoginState = LoginState.PENDING
    created_at: str
    updated_at: str
    expires_at: str
    timeout_seconds: int
    owner_pid: int = 0
    process_pid: int = 0
    verification_url: str | None = None
    user_code: str | None = None
    error: str | None = None
    events: list[LoginEvent] = Field(default_factory=list)

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def public_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class CodexLoginJobStore:
    """Thread-safe job registry backed by redacted JSON snapshots."""

    def __init__(self, data_dir: Path) -> None:
        self.directory = data_dir / "agent_provider_jobs" / "codex"
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._jobs: dict[str, CodexLoginJob] = {}
        self._processes: dict[str, subprocess.Popen[Any]] = {}
        self._lease_path = self.directory / ".login.lock"
        self._load()

    def _load(self) -> None:
        for path in self.directory.glob("*.json"):
            try:
                job = CodexLoginJob.model_validate_json(path.read_text())
            except OSError, ValueError:
                continue
            if not job.terminal and (_is_expired(job) or not _pid_alive(job.owner_pid)):
                _terminate_orphan_group(job.process_pid)
                job.state = LoginState.INTERRUPTED
                job.error = (
                    "PA restarted while this login was active; start a new login."
                )
                self._event(job, "interrupted", job.error)
                self._persist(job)
            self._jobs[job.job_id] = job

    def create(
        self, *, timeout_seconds: int = DEFAULT_LOGIN_TIMEOUT_S
    ) -> CodexLoginJob:
        timeout_seconds = max(
            MIN_LOGIN_TIMEOUT_S, min(MAX_LOGIN_TIMEOUT_S, timeout_seconds)
        )
        now = datetime.now(UTC)
        job = CodexLoginJob(
            job_id=str(uuid4()),
            created_at=now.isoformat(),
            updated_at=now.isoformat(),
            expires_at=datetime.fromtimestamp(
                now.timestamp() + timeout_seconds, UTC
            ).isoformat(),
            timeout_seconds=timeout_seconds,
            owner_pid=os.getpid(),
        )
        with self._lock:
            with _file_lock(self._lease_path):
                self._reload_from_disk()
                if any(not existing.terminal for existing in self._jobs.values()):
                    raise ValueError("A Codex login is already active")
                self._jobs[job.job_id] = job
                self._event(
                    job, "created", "Device authentication requested by the user."
                )
                self._persist(job)
        return job

    def get(self, job_id: str) -> CodexLoginJob | None:
        with self._lock:
            self._reload_from_disk()
            return self._jobs.get(job_id)

    def latest_active(self) -> CodexLoginJob | None:
        with self._lock:
            self._reload_from_disk()
            active = [job for job in self._jobs.values() if not job.terminal]
            return max(active, key=lambda item: item.created_at) if active else None

    def start(self, job: CodexLoginJob, codex: str) -> None:
        thread = threading.Thread(
            target=self._run,
            args=(job.job_id, codex),
            daemon=True,
            name=f"codex-login-{job.job_id}",
        )
        thread.start()

    def cancel(self, job_id: str) -> CodexLoginJob | None:
        with self._lock:
            self._reload_from_disk()
            job = self._jobs.get(job_id)
            if not job or job.terminal:
                return job
            job.state = LoginState.CANCELLED
            job.error = "Login cancelled by the user."
            self._event(job, "cancelled", job.error)
            proc = self._processes.get(job_id)
            self._persist(job)
        if proc and proc.poll() is None:
            _terminate_process(proc)
        return job

    def _run(self, job_id: str, codex: str) -> None:
        job = self.get(job_id)
        if not job:
            return
        with self._lock:
            if job.state == LoginState.CANCELLED:
                return
            job.state = LoginState.RUNNING
            self._event(
                job,
                "started",
                "Codex device authentication started on the target instance.",
            )
            self._persist(job)
        master_fd: int | None = None
        slave_fd: int | None = None
        try:
            process_options: dict[str, Any] = {"start_new_session": True}
            stdio: dict[str, Any]
            if os.name == "nt":
                process_options = {
                    "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP,
                }
                stdio = {
                    "stdin": subprocess.DEVNULL,
                    "stdout": subprocess.PIPE,
                    "stderr": subprocess.STDOUT,
                }
            else:
                master_fd, slave_fd = os.openpty()
                _configure_pty(slave_fd)
                stdio = {"stdin": slave_fd, "stdout": slave_fd, "stderr": slave_fd}
            proc = subprocess.Popen(
                [codex, "login", "--device-auth"],
                env=os.environ.copy(),
                **stdio,
                **process_options,
            )
        except OSError as exc:
            _close_fd(master_fd)
            _close_fd(slave_fd)
            self._finish(job, LoginState.FAILED, f"Unable to start Codex CLI: {exc}")
            return
        if slave_fd is not None:
            _close_fd(slave_fd)
            slave_fd = None
        started_at = time.monotonic()
        deadline = started_at + job.timeout_seconds
        with self._lock:
            self._processes[job_id] = proc
            job.process_pid = proc.pid
            job.expires_at = (
                datetime.now(UTC) + timedelta(seconds=job.timeout_seconds)
            ).isoformat()
            self._persist(job)
            cancelled_before_registration = job.state == LoginState.CANCELLED
        if cancelled_before_registration:
            _terminate_process(proc)

        last_output_at: float | None = None
        capture = ""
        selector: selectors.BaseSelector | None = None
        output_queue: queue.Queue[str] | None = None
        output_thread: threading.Thread | None = None
        try:
            if master_fd is not None:
                os.set_blocking(master_fd, False)
                selector = selectors.DefaultSelector()
                selector.register(master_fd, selectors.EVENT_READ)
            elif proc.stdout is not None:
                output_queue = queue.Queue()
                output_thread = threading.Thread(
                    target=_read_stream,
                    args=(proc.stdout, output_queue),
                    daemon=True,
                    name=f"codex-login-output-{job_id}",
                )
                output_thread.start()
            while proc.poll() is None:
                self._refresh_cancelled(job)
                if job.state == LoginState.CANCELLED:
                    _terminate_process(proc)
                    break
                if time.monotonic() >= deadline:
                    _terminate_process(proc)
                    self._finish(
                        job,
                        LoginState.TIMED_OUT,
                        "Codex login timed out; start a new login to retry.",
                    )
                    return
                now = time.monotonic()
                if last_output_at is None and now - started_at >= NO_OUTPUT_TIMEOUT_S:
                    _terminate_process(proc)
                    self._finish(
                        job,
                        LoginState.FAILED,
                        "Codex produced no device-login instructions. Verify the target Codex CLI and retry.",
                    )
                    return
                if (
                    not (job.verification_url and job.user_code)
                    and now - started_at >= NO_INSTRUCTIONS_TIMEOUT_S
                ):
                    _terminate_process(proc)
                    self._finish(
                        job,
                        LoginState.FAILED,
                        "Codex did not provide a verification URL and code. Update the target Codex CLI or retry.",
                    )
                    return
                chunk = self._read_ready(
                    proc, master_fd, selector, output_queue, timeout=0.25
                )
                if chunk:
                    last_output_at = time.monotonic()
                    capture = (capture + chunk)[-MAX_CAPTURE_CHARS:]
                    self._consume_output(job, capture, chunk)
            if output_thread is not None:
                output_thread.join(timeout=1)
            while True:
                chunk = self._read_ready(
                    proc, master_fd, selector, output_queue, timeout=0
                )
                if not chunk:
                    break
                capture = (capture + chunk)[-MAX_CAPTURE_CHARS:]
                self._consume_output(job, capture, chunk)
            if job.state == LoginState.CANCELLED:
                _terminate_process(proc)
                proc.wait(timeout=5)
                return
            returncode = proc.wait(timeout=5)
            self._refresh_cancelled(job)
            if job.state == LoginState.CANCELLED:
                return
            if returncode == 0:
                self._finish(
                    job,
                    LoginState.SUCCEEDED,
                    "Signed in with ChatGPT on the target instance.",
                )
            else:
                self._finish(
                    job,
                    LoginState.FAILED,
                    "Codex device authentication failed; retry or inspect target Codex login logs.",
                )
        except Exception as exc:
            _terminate_process(proc)
            self._finish(
                job,
                LoginState.FAILED,
                f"Codex login process error: {type(exc).__name__}",
            )
        finally:
            if selector is not None:
                selector.close()
            _close_fd(master_fd)
            with self._lock:
                self._processes.pop(job_id, None)

    def _read_ready(
        self,
        proc: subprocess.Popen[Any],
        master_fd: int | None,
        selector: selectors.BaseSelector | None,
        output_queue: queue.Queue[str] | None,
        *,
        timeout: float,
    ) -> str:
        if master_fd is not None:
            if selector is not None and not selector.select(timeout=timeout):
                return ""
            try:
                return os.read(master_fd, 4096).decode("utf-8", errors="replace")
            except (BlockingIOError, OSError):
                return ""
        if output_queue is None:
            return ""
        try:
            if timeout:
                return output_queue.get(timeout=timeout)
            return output_queue.get_nowait()
        except queue.Empty:
            return ""

    def _consume_output(self, job: CodexLoginJob, capture: str, _chunk: str) -> None:
        clean_capture = normalize_terminal_output(capture)
        if not clean_capture:
            return
        url = _URL_RE.search(clean_capture)
        code = _CODE_RE.search(clean_capture)
        with self._lock:
            had_url = bool(job.verification_url)
            had_code = bool(job.user_code)
            if url:
                job.verification_url = url.group(0).rstrip(".,):;")
            if code:
                job.user_code = (code.group(1) or code.group(2)).replace(" ", "-").upper()
            actionable = job.verification_url or job.user_code
            if actionable and job.state == LoginState.RUNNING:
                job.state = LoginState.WAITING_FOR_USER
            if bool(job.verification_url) != had_url or bool(job.user_code) != had_code:
                self._event(
                    job,
                    "instruction",
                    "Device-login instructions are ready for the user.",
                )
            self._persist(job)

    def _finish(self, job: CodexLoginJob, state: LoginState, message: str) -> None:
        self._refresh_cancelled(job)
        with self._lock:
            if job.state == LoginState.CANCELLED:
                return
            job.state = state
            job.error = message if state != LoginState.SUCCEEDED else None
            self._event(job, state.value, message)
            self._persist(job)

    def _event(self, job: CodexLoginJob, event_type: str, message: str) -> None:
        job.updated_at = datetime.now(UTC).isoformat()
        job.events.append(
            LoginEvent(
                sequence=(job.events[-1].sequence + 1 if job.events else 1),
                timestamp=job.updated_at,
                type=event_type,
                message=redact_login_output(message)[:MAX_EVENT_CHARS],
            )
        )
        job.events = job.events[-MAX_EVENTS:]

    def _persist(self, job: CodexLoginJob) -> None:
        atomic_write_json(self.directory / f"{job.job_id}.json", job.public_dict())

    def _reload_from_disk(self) -> None:
        for path in self.directory.glob("*.json"):
            try:
                disk_job = CodexLoginJob.model_validate_json(path.read_text())
            except OSError, ValueError:
                continue
            if not disk_job.terminal and (
                _is_expired(disk_job) or not _pid_alive(disk_job.owner_pid)
            ):
                _terminate_orphan_group(disk_job.process_pid)
                disk_job.state = LoginState.INTERRUPTED
                disk_job.error = "Login lease expired; start a new login."
                self._event(disk_job, "interrupted", disk_job.error)
                self._persist(disk_job)
            current = self._jobs.get(disk_job.job_id)
            if current is None or disk_job.updated_at > current.updated_at:
                self._jobs[disk_job.job_id] = disk_job

    def _refresh_cancelled(self, job: CodexLoginJob) -> None:
        path = self.directory / f"{job.job_id}.json"
        try:
            disk_job = CodexLoginJob.model_validate_json(path.read_text())
        except OSError, ValueError:
            return
        if disk_job.state == LoginState.CANCELLED:
            job.state = LoginState.CANCELLED
            job.error = disk_job.error


_stores: dict[str, CodexLoginJobStore] = {}
_stores_lock = threading.Lock()


def get_codex_login_store(data_dir: Path) -> CodexLoginJobStore:
    key = str(data_dir.resolve())
    with _stores_lock:
        if key not in _stores:
            _stores[key] = CodexLoginJobStore(data_dir)
        return _stores[key]


def resolve_codex_cli(configured_path: str | None = None) -> str | None:
    if configured_path:
        resolved = resolve_executable(configured_path)
        return str(resolved) if resolved else None
    resolved = resolve_executable("codex") or shutil.which("codex")
    return str(resolved) if resolved else None


def redact_login_output(text: str) -> str:
    """Allow device URL/code, but suppress anything resembling credential output."""
    clean = normalize_terminal_output(text)
    if not clean:
        return ""
    clean = _SECRET_VALUE_RE.sub(
        lambda match: f"{match.group(1)}{match.group(2)}[redacted]", clean
    )
    clean = _BEARER_RE.sub("Bearer [redacted]", clean)
    # Codex device codes are short and hyphenated. Hide long opaque blobs.
    clean = re.sub(
        r"(?<![A-Za-z0-9])[A-Za-z0-9._~+/=-]{40,}(?![A-Za-z0-9])", "[redacted]", clean
    )
    return clean


def normalize_terminal_output(text: str) -> str:
    """Render terminal output as stable plain text before parsing or persistence."""
    clean = _ANSI_OSC_RE.sub("", text)
    clean = _ANSI_CSI_RE.sub("", clean)
    clean = _ANSI_SINGLE_RE.sub("", clean)
    clean = clean.replace("\r\n", "\n").replace("\r", "\n")
    # Apply backspaces so rewritten terminal text cannot confuse the parser.
    while "\b" in clean:
        clean = re.sub(r"[^\n]\b", "", clean).replace("\b", "")
    clean = "".join(char for char in clean if char in "\n\t" or ord(char) >= 32)
    return "\n".join(line.rstrip() for line in clean.splitlines()).strip()


def _configure_pty(fd: int) -> None:
    """Use a wide terminal to keep the public URL and code from wrapping."""
    try:
        import fcntl
        import struct
        import termios

        fcntl.ioctl(fd, termios.TIOCSWINSZ, struct.pack("HHHH", 24, 240, 0, 0))
    except (ImportError, OSError):
        pass


def _close_fd(fd: int | None) -> None:
    if fd is None:
        return
    try:
        os.close(fd)
    except OSError:
        pass


def _read_stream(stream: Any, output: queue.Queue[str]) -> None:
    """Read a blocking pipe off-thread so lifecycle checks remain responsive."""
    read = getattr(stream, "read1", stream.read)
    while True:
        chunk = read(4096)
        if not chunk:
            return
        if isinstance(chunk, bytes):
            chunk = chunk.decode("utf-8", errors="replace")
        output.put(chunk)


def _terminate_orphan_group(pid: int) -> None:
    """Best-effort cleanup for a device-login process left by a dead PA owner."""
    if os.name == "nt" or pid <= 0 or not _pid_alive(pid):
        return
    try:
        # Login children are session leaders; never signal PA's own process group.
        if os.getpgid(pid) == pid and pid != os.getpgrp():
            os.killpg(pid, signal.SIGTERM)
    except OSError:
        pass


def _terminate_process(proc: subprocess.Popen[Any]) -> None:
    if proc.poll() is not None:
        return
    if os.name == "nt":
        try:
            proc.terminate()
            proc.wait(timeout=3)
        except OSError, subprocess.TimeoutExpired:
            try:
                proc.kill()
                proc.wait(timeout=3)
            except OSError, subprocess.TimeoutExpired:
                pass
        return
    try:
        os.killpg(proc.pid, signal.SIGTERM)
        proc.wait(timeout=3)
    except OSError, subprocess.TimeoutExpired:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=3)
        except OSError:
            pass
        except subprocess.TimeoutExpired:
            pass


def _is_expired(job: CodexLoginJob) -> bool:
    try:
        return datetime.fromisoformat(job.expires_at) <= datetime.now(UTC)
    except ValueError:
        return True


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except PermissionError:
        return True
    except OSError:
        return False


@contextmanager
def _file_lock(path: Path):
    """Small cross-platform exclusive lock for atomic active-job creation."""
    path.touch(exist_ok=True)
    with path.open("r+b") as handle:
        if os.name == "nt":
            import msvcrt

            if path.stat().st_size == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl

            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
