from __future__ import annotations

from typing import Any

from fastapi import HTTPException


def startup_state(manager: Any) -> dict[str, Any]:
    state = getattr(manager, "startup_state", None)
    if callable(state):
        return dict(state())
    return {"phase": "ready", "complete": True, "error": None}


def startup_recovery_error(manager: Any) -> HTTPException | None:
    """Return the shared bounded response for ACP admission during startup."""
    state = startup_state(manager)
    if state.get("complete", True):
        return None
    failed = state.get("phase") == "failed"
    return HTTPException(
        status_code=503,
        detail={
            "code": (
                "agent_recovery_failed" if failed else "agent_recovery_in_progress"
            ),
            "message": (
                "Durable agent session recovery failed. Session history remains available."
                if failed
                else "PA is restoring durable agent sessions. Try again shortly."
            ),
            "recoverable": not failed,
            "retry_after_ms": 250,
            "startup": state,
            "history_url": "/api/agent/history",
        },
        headers={"Retry-After": "1"},
    )


def require_startup_ready(manager: Any) -> None:
    if getattr(manager, "quiescing", False) is True or getattr(
        manager, "_accepting", True
    ) is False:
        raise HTTPException(
            status_code=503,
            detail={
                "code": "agent_draining",
                "message": "PA is draining agent sessions for shutdown.",
                "recoverable": True,
                "retry_after_ms": 1000,
            },
            headers={"Retry-After": "1"},
        )
    error = startup_recovery_error(manager)
    if error:
        raise error
