"""Bounded, redacted Codex CLI device-auth jobs.

Credentials are written by Codex itself to the target OS user's credential store.
PA persists only public device-flow instructions and lifecycle events.
"""

from __future__ import annotations

import os
import re
import selectors
import shutil
import signal
import subprocess
import threading
import time
from contextlib import contextmanager
from datetime import UTC, datetime
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

_URL_RE = re.compile(r"https://[^\s<>]+", re.IGNORECASE)
_CODE_RE = re.compile(r"(?<![A-Z0-9])([A-Z0-9]{4}(?:-[A-Z0-9]{4})+)(?![A-Z0-9])")
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
        self._processes: dict[str, subprocess.Popen[str]] = {}
        self._lease_path = self.directory / ".login.lock"
        self._load()

    def _load(self) -> None:
        for path in self.directory.glob("*.json"):
            try:
                job = CodexLoginJob.model_validate_json(path.read_text())
            except OSError, ValueError:
                continue
            if not job.terminal and (_is_expired(job) or not _pid_alive(job.owner_pid)):
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
        try:
            process_options: dict[str, Any] = {"start_new_session": True}
            if os.name == "nt":
                process_options = {
                    "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP,
                }
            proc = subprocess.Popen(
                [codex, "login", "--device-auth"],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=os.environ.copy(),
                **process_options,
            )
        except OSError as exc:
            self._finish(job, LoginState.FAILED, f"Unable to start Codex CLI: {exc}")
            return
        with self._lock:
            self._processes[job_id] = proc
            cancelled_before_registration = job.state == LoginState.CANCELLED
        if cancelled_before_registration:
            _terminate_process(proc)

        deadline = time.monotonic() + job.timeout_seconds
        selector: selectors.BaseSelector | None = None
        try:
            assert proc.stdout is not None
            selector = selectors.DefaultSelector()
            selector.register(proc.stdout, selectors.EVENT_READ)
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
                ready = selector.select(timeout=0.25)
                if ready:
                    line = proc.stdout.readline()
                    if line:
                        self._consume_line(job, line)
            for line in proc.stdout:
                self._consume_line(job, line)
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
            with self._lock:
                self._processes.pop(job_id, None)

    def _consume_line(self, job: CodexLoginJob, line: str) -> None:
        clean = redact_login_output(line)
        if not clean:
            return
        url = _URL_RE.search(clean)
        code = _CODE_RE.search(clean)
        with self._lock:
            if url:
                job.verification_url = url.group(0).rstrip(".,)")
            if code:
                job.user_code = code.group(1)
            if (url or code) and job.state == LoginState.RUNNING:
                job.state = LoginState.WAITING_FOR_USER
            self._event(
                job, "instruction" if (url or code) else "progress", clean[:500]
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
                message=redact_login_output(message)[:500],
            )
        )
        job.events = job.events[-200:]

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
    clean = text.strip().replace("\x1b", "")
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


def _terminate_process(proc: subprocess.Popen[str]) -> None:
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
