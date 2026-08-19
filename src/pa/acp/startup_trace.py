"""Durable, phase-level observability for ACP session admission."""

from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Iterator


logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


class SessionStartupTrace:
    """Record one new-session request without retaining sensitive inputs."""

    schema_version = 1

    def __init__(self) -> None:
        self.started_at = _now()
        self._started_ns = time.perf_counter_ns()
        self._phases: list[dict[str, Any]] = []
        self._session: Any | None = None

    def attach(self, session: Any) -> None:
        self._session = session
        self._publish()

    @property
    def attached(self) -> bool:
        return self._session is not None

    @contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started_at = _now()
        started_ns = time.perf_counter_ns()
        try:
            yield
        except BaseException as exc:
            self._finish(
                name,
                started_at=started_at,
                started_ns=started_ns,
                status="failed",
                error_type=type(exc).__name__,
            )
            raise
        else:
            self._finish(
                name,
                started_at=started_at,
                started_ns=started_ns,
                status="ok",
            )

    def mark(self, name: str, *, status: str = "ok") -> None:
        started_ns = time.perf_counter_ns()
        self._finish(
            name,
            started_at=_now(),
            started_ns=started_ns,
            status=status,
        )

    def _finish(
        self,
        name: str,
        *,
        started_at: str,
        started_ns: int,
        status: str,
        error_type: str | None = None,
    ) -> None:
        completed_at = _now()
        duration_ms = round((time.perf_counter_ns() - started_ns) / 1_000_000, 3)
        phase: dict[str, Any] = {
            "name": name,
            "started_at": started_at,
            "completed_at": completed_at,
            "duration_ms": duration_ms,
            "status": status,
        }
        if error_type:
            phase["error_type"] = error_type
        self._phases.append(phase)
        self._publish()
        logger.info(
            "ACP session startup phase completed",
            extra={
                "acp_startup_phase": name,
                "acp_startup_status": status,
                "acp_startup_duration_ms": duration_ms,
                "acp_session_id": getattr(self._session, "id", None),
            },
        )

    def snapshot(self) -> dict[str, Any]:
        updated_at = (
            self._phases[-1]["completed_at"] if self._phases else self.started_at
        )
        return {
            "schema_version": self.schema_version,
            "started_at": self.started_at,
            "updated_at": updated_at,
            "total_duration_ms": round(
                (time.perf_counter_ns() - self._started_ns) / 1_000_000, 3
            ),
            "complete": bool(
                self._phases
                and self._phases[-1]["name"] == "response_readiness"
                and self._phases[-1]["status"] == "ok"
            ),
            "phases": [dict(item) for item in self._phases],
        }

    def _publish(self) -> None:
        if self._session is None:
            return
        config = dict(getattr(self._session, "config_json", None) or {})
        config["startup_trace"] = self.snapshot()
        self._session.config_json = config
