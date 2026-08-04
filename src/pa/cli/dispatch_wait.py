"""Read-only waiting and host sleep inhibition for durable dispatches."""

from __future__ import annotations

import atexit
import os
import shutil
import signal
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager, nullcontext
from dataclasses import asdict, dataclass, field
from typing import Any, Self

DEFAULT_WAIT_TIMEOUT_SECONDS = 24 * 60 * 60
DEFAULT_POLL_INTERVAL_SECONDS = 2.0
WAIT_FAILED_EXIT = 1
WAIT_TIMEOUT_EXIT = 124
WAIT_INTERRUPTED_EXIT = 130

SUCCESS_STATES = {"completed", "acknowledged"}


class DispatchFetchError(RuntimeError):
    """A classified read failure from PA's public dispatch API."""

    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code

    @property
    def terminal(self) -> bool:
        return self.status_code is not None and self.status_code < 500


class KeepAwakeError(RuntimeError):
    pass


class _WaitSignal(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


@dataclass
class WatchedDispatch:
    dispatch_id: str
    state: str = "pending"
    outcome: str = "waiting"
    target: str | None = None
    message: str | None = None
    terminal: bool = False
    transitions: list[dict[str, str]] = field(default_factory=list)


class MacOSKeepAwake:
    """Own a crash-safe macOS caffeinate assertion for this CLI process."""

    def __init__(self) -> None:
        self._process: subprocess.Popen[bytes] | None = None
        self._registered = False

    def __enter__(self) -> Self:
        if sys.platform != "darwin":
            raise KeepAwakeError(
                "--keep-awake is currently supported on macOS only; rerun without it "
                "on this platform."
            )
        executable = shutil.which("caffeinate")
        if not executable:
            raise KeepAwakeError(
                "macOS caffeinate is unavailable; cannot create a sleep assertion."
            )
        try:
            self._process = subprocess.Popen(
                [executable, "-dimsu", "-w", str(os.getpid())],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                start_new_session=True,
            )
        except OSError as exc:
            raise KeepAwakeError(f"Could not start macOS caffeinate: {exc}") from exc
        atexit.register(self.close)
        self._registered = True
        return self

    def close(self) -> None:
        process, self._process = self._process, None
        if self._registered:
            atexit.unregister(self.close)
            self._registered = False
        if process is None or process.poll() is not None:
            return
        process.terminate()
        try:
            process.wait(timeout=5.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5.0)

    def __exit__(self, *_args: object) -> None:
        self.close()


@contextmanager
def wait_signal_guard() -> Iterator[None]:
    """Turn SIGTERM into a cleanup-aware interruption; SIGINT is KeyboardInterrupt."""

    installed = threading.current_thread() is threading.main_thread()
    previous = signal.getsignal(signal.SIGTERM) if installed else None

    def interrupt(signum: int, _frame: object) -> None:
        raise _WaitSignal(signum)

    if installed:
        signal.signal(signal.SIGTERM, interrupt)
    try:
        yield
    finally:
        if installed and previous is not None:
            signal.signal(signal.SIGTERM, previous)


def _transition(
    watched: WatchedDispatch,
    state: str,
    *,
    message: str | None,
    emit: Callable[[str], None] | None,
) -> None:
    if watched.state == state:
        watched.message = message or watched.message
        return
    watched.state = state
    watched.message = message
    transition = {"state": state}
    if message:
        transition["message"] = message
    watched.transitions.append(transition)
    if emit:
        suffix = f" — {message}" if message else ""
        emit(f"{watched.dispatch_id}: {state}{suffix}")


def _result(
    watched: list[WatchedDispatch],
    *,
    started_at: float,
    keep_awake: bool,
    timeout_seconds: float,
    exit_code: int | None = None,
) -> dict[str, Any]:
    if exit_code is None:
        exit_code = (
            0
            if all(item.outcome == "succeeded" for item in watched)
            else WAIT_FAILED_EXIT
        )
    status = (
        "succeeded"
        if exit_code == 0
        else "timed_out"
        if exit_code == WAIT_TIMEOUT_EXIT
        else "interrupted"
        if exit_code >= 128
        else "failed"
    )
    return {
        "status": status,
        "exit_code": exit_code,
        "elapsed_seconds": round(max(0.0, time.monotonic() - started_at), 3),
        "timeout_seconds": timeout_seconds,
        "keep_awake": keep_awake,
        "dispatches": [
            {
                key: value
                for key, value in asdict(item).items()
                if key != "terminal"
            }
            for item in watched
        ],
    }


def wait_for_dispatches(
    dispatch_ids: list[str],
    *,
    fetch: Callable[[str], dict[str, Any]],
    timeout_seconds: float,
    poll_interval_seconds: float,
    keep_awake: bool,
    emit: Callable[[str], None] | None,
) -> dict[str, Any]:
    """Observe dispatches until all are terminal, without mutating their lifecycle."""

    watched = [WatchedDispatch(dispatch_id=value) for value in dispatch_ids]
    started_at = time.monotonic()
    deadline = started_at + timeout_seconds
    assertion = MacOSKeepAwake() if keep_awake else nullcontext()
    try:
        with assertion, wait_signal_guard():
            while True:
                for item in watched:
                    if item.terminal:
                        continue
                    try:
                        payload = fetch(item.dispatch_id)
                    except DispatchFetchError as exc:
                        if exc.terminal:
                            item.outcome = "unavailable"
                            item.terminal = True
                            _transition(
                                item, "unavailable", message=str(exc), emit=emit
                            )
                        else:
                            _transition(
                                item, "api-unavailable", message=str(exc), emit=emit
                            )
                        continue

                    state = str(
                        payload.get("effective_state")
                        or payload.get("state")
                        or "unknown"
                    )
                    item.target = str(
                        payload.get("target_instance_name")
                        or payload.get("target_instance_id")
                        or "unknown"
                    )
                    message = None
                    events = payload.get("events")
                    if isinstance(events, list) and events:
                        latest = events[-1]
                        if isinstance(latest, dict) and latest.get("message"):
                            message = str(latest["message"])
                    if state in SUCCESS_STATES:
                        item.outcome = "succeeded"
                        item.terminal = True
                    elif state == "failed":
                        item.outcome = "failed"
                        item.terminal = True
                        message = str(payload.get("last_error") or message or "") or None
                    elif state == "cancelled":
                        item.outcome = "cancelled"
                        item.terminal = True
                    _transition(item, state, message=message, emit=emit)

                if all(item.terminal for item in watched):
                    return _result(
                        watched,
                        started_at=started_at,
                        keep_awake=keep_awake,
                        timeout_seconds=timeout_seconds,
                    )
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    for item in watched:
                        if not item.terminal:
                            item.outcome = "timed_out"
                            item.terminal = True
                            _transition(
                                item,
                                "timed-out",
                                message="Wait deadline elapsed.",
                                emit=emit,
                            )
                    return _result(
                        watched,
                        started_at=started_at,
                        keep_awake=keep_awake,
                        timeout_seconds=timeout_seconds,
                        exit_code=WAIT_TIMEOUT_EXIT,
                    )
                time.sleep(min(poll_interval_seconds, remaining))
    except KeyboardInterrupt:
        exit_code = WAIT_INTERRUPTED_EXIT
    except _WaitSignal as exc:
        exit_code = 128 + exc.signum

    for item in watched:
        if not item.terminal:
            item.outcome = "interrupted"
            item.terminal = True
            _transition(
                item,
                "interrupted",
                message="Wait interrupted by operator signal.",
                emit=emit,
            )
    return _result(
        watched,
        started_at=started_at,
        keep_awake=keep_awake,
        timeout_seconds=timeout_seconds,
        exit_code=exit_code,
    )
