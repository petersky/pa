"""Sanitized provider-sandbox failure classification and bounded health state."""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from threading import RLock
from typing import Any

PATTERNS = (
    (
        "sandbox_loopback_init_failed",
        re.compile(r"(RTM_NEWADDR|loopback: Failed).*Operation not permitted", re.IGNORECASE),
    ),
    (
        "sandbox_apparmor_denied",
        re.compile(r"(apparmor=.*DENIED|apparmor denial|profile=.*bwrap)", re.IGNORECASE),
    ),
    (
        "sandbox_userns_denied",
        re.compile(r"(userns|user namespace).*(denied|not permitted)", re.IGNORECASE),
    ),
    (
        "sandbox_nested_proxy_netns_failed",
        re.compile(r"(proxy|nested).*(netns|network namespace).*(failed|denied)", re.IGNORECASE),
    ),
    (
        "sandbox_binary_missing_or_incompatible",
        re.compile(r"(bwrap|bubblewrap).*(not found|no such file|incompatible)", re.IGNORECASE),
    ),
    (
        "sandbox_writable_root_mismatch",
        re.compile(
            r"(read-only file system|outside writable roots).*(worktree|workspace)",
            re.IGNORECASE,
        ),
    ),
    (
        "sandbox_namespace_unavailable",
        re.compile(
            r"(namespace|unshare|clone3).*(not permitted|unavailable|failed)", re.IGNORECASE
        ),
    ),
)
_PATH = re.compile(r"(?<![\w.-])/(?:[^\s:'\"]+/?)+")
_SECRET = re.compile(
    r"(?i)\b(token|authorization|password|secret|api[_-]?key)\b\s*[:=]\s*\S+"
)


def sanitize_provider_error(value: object, *, limit: int = 2048) -> str:
    text = str(value or "").replace("\x00", "")
    text = _SECRET.sub(r"\1=<redacted>", text)
    return _PATH.sub("<path>", text)[:limit]


def classify_sandbox_failure(value: object) -> str:
    text = sanitize_provider_error(value)
    return next(
        (name for name, pattern in PATTERNS if pattern.search(text)),
        "unknown_provider_tool_failure",
    )


@dataclass
class SandboxHealth:
    provider_id: str
    sandbox_profile: str
    state: str = "unknown"
    classification: str | None = None
    consecutive_failures: int = 0
    last_probe_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    unhealthy_until: datetime | None = None
    sanitized_signature: str | None = None
    recovery: str = "probe"
    metadata: dict[str, Any] = field(default_factory=dict)

    def public_dict(self, now: datetime | None = None) -> dict[str, Any]:
        now = now or datetime.now(UTC)
        expired = (
            self.state == "unhealthy"
            and self.unhealthy_until
            and now >= self.unhealthy_until
        )
        return {
            "provider_id": self.provider_id,
            "sandbox_profile": self.sandbox_profile,
            "state": "stale" if expired else self.state,
            "classification": self.classification,
            "consecutive_failures": self.consecutive_failures,
            "last_probe_at": self.last_probe_at.isoformat()
            if self.last_probe_at
            else None,
            "last_success_at": self.last_success_at.isoformat()
            if self.last_success_at
            else None,
            "last_failure_at": self.last_failure_at.isoformat()
            if self.last_failure_at
            else None,
            "unhealthy_until": self.unhealthy_until.isoformat()
            if self.unhealthy_until
            else None,
            "sanitized_signature": self.sanitized_signature,
            "recovery": "probe" if expired else self.recovery,
            "metadata": dict(self.metadata),
        }


class SandboxHealthRegistry:
    def __init__(self, *, failure_threshold: int = 2, unhealthy_seconds: int = 300):
        self.failure_threshold = max(1, failure_threshold)
        self.unhealthy_seconds = max(1, unhealthy_seconds)
        self._records: dict[tuple[str, str], SandboxHealth] = {}
        self._lock = RLock()

    def _record(self, provider_id: str, profile: str) -> SandboxHealth:
        return self._records.setdefault(
            (provider_id, profile), SandboxHealth(provider_id, profile)
        )

    def success(
        self, provider_id: str, profile: str, *, metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        with self._lock:
            record = self._record(provider_id, profile)
            record.state, record.classification, record.consecutive_failures = (
                "healthy",
                None,
                0,
            )
            record.last_probe_at = record.last_success_at = now
            record.unhealthy_until = record.sanitized_signature = None
            record.recovery, record.metadata = "none", dict(metadata or {})
            return record.public_dict(now)

    def failure(
        self,
        provider_id: str,
        profile: str,
        error: object,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = datetime.now(UTC)
        with self._lock:
            record = self._record(provider_id, profile)
            record.classification = classify_sandbox_failure(error)
            record.consecutive_failures += 1
            record.last_probe_at = record.last_failure_at = now
            record.sanitized_signature = sanitize_provider_error(error)
            record.metadata = dict(metadata or {})
            if record.consecutive_failures >= self.failure_threshold:
                record.state, record.recovery = "unhealthy", "reroute_or_operator_probe"
                record.unhealthy_until = now + timedelta(seconds=self.unhealthy_seconds)
            else:
                record.state, record.recovery = "degraded", "retry_session_once"
            return record.public_dict(now)

    def get(self, provider_id: str, profile: str) -> dict[str, Any]:
        with self._lock:
            return self._record(provider_id, profile).public_dict()

    def clear(self, provider_id: str, profile: str) -> dict[str, Any]:
        with self._lock:
            self._records.pop((provider_id, profile), None)
            return self._record(provider_id, profile).public_dict()

    def snapshot(self) -> list[dict[str, Any]]:
        with self._lock:
            return [record.public_dict() for record in self._records.values()]


sandbox_health_registry = SandboxHealthRegistry()
