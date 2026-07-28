"""Canonical, authenticated channel from PA-launched children to their owner."""

from __future__ import annotations

import logging
import threading
import time
from dataclasses import asdict, dataclass
from ipaddress import ip_address
from typing import Any

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class OwnerEndpoint:
    url: str
    endpoint_type: str


@dataclass
class OwnerChannelHealth:
    state: str = "unknown"
    endpoint_type: str = "unknown"
    failure_classification: str | None = None
    last_success_at: float | None = None
    last_failure_at: float | None = None
    consecutive_failures: int = 0
    retry_at: float | None = None
    recovery_action: str | None = None

    def snapshot(self) -> dict[str, Any]:
        result = asdict(self)
        if self.retry_at is not None:
            result["retry_in_seconds"] = max(0.0, self.retry_at - time.time())
        return result


_lock = threading.Lock()
_health: dict[str, OwnerChannelHealth] = {}


def owner_endpoint(settings: Settings) -> OwnerEndpoint:
    """Resolve the listener address reachable in the child's namespace."""
    host = settings.host.strip()
    if host == "0.0.0.0":
        return OwnerEndpoint(f"http://127.0.0.1:{settings.port}", "wildcard_ipv4")
    if host == "::":
        return OwnerEndpoint(f"http://[::1]:{settings.port}", "wildcard_ipv6")
    if host.lower() == "localhost":
        return OwnerEndpoint(f"http://localhost:{settings.port}", "loopback_hostname")
    try:
        address = ip_address(host)
    except ValueError:
        kind = "concrete_hostname"
    else:
        kind = (
            f"loopback_ipv{address.version}"
            if address.is_loopback
            else f"concrete_ipv{address.version}"
        )
        if address.version == 6:
            host = f"[{host}]"
    return OwnerEndpoint(f"http://{host}:{settings.port}", kind)


def owner_channel_snapshot(instance_id: str) -> dict[str, Any]:
    with _lock:
        health = _health.get(instance_id, OwnerChannelHealth())
        return health.snapshot()


def _record(settings: Settings, health: OwnerChannelHealth) -> None:
    with _lock:
        _health[settings.instance_id] = health
    logger.info(
        "PA MCP owner channel state=%s endpoint_type=%s failure=%s retry_at=%s",
        health.state,
        health.endpoint_type,
        health.failure_classification,
        health.retry_at,
    )


def probe_owner_channel(
    settings: Settings, *, attempts: int = 5, initial_delay: float = 0.1
) -> dict[str, Any]:
    """Verify auth, readiness, API compatibility, and instance identity."""
    endpoint = owner_endpoint(settings)
    token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    delay = initial_delay
    last: OwnerChannelHealth | None = None
    for attempt in range(attempts):
        now = time.time()
        try:
            response = httpx.get(
                f"{endpoint.url}/api/ready",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-PA-MCP-Instance-ID": settings.instance_id,
                },
                timeout=1.0,
                trust_env=False,
            )
        except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
            classification = "connection_refused_or_unreachable"
        except httpx.HTTPError as exc:
            classification = "api_incompatible"
        else:
            actual = response.headers.get("X-PA-Instance-ID", "").strip()
            if response.status_code in {401, 403}:
                classification = "authentication_rejected"
            elif actual != settings.instance_id:
                classification = "instance_mismatch"
            elif response.status_code == 503:
                classification = "api_not_ready"
            elif response.status_code != 200:
                classification = "api_incompatible"
            else:
                try:
                    body = response.json()
                except ValueError:
                    classification = "api_incompatible"
                else:
                    if body.get("status") != "ready":
                        classification = "api_incompatible"
                    else:
                        health = OwnerChannelHealth(
                            state="connected_identity_verified",
                            endpoint_type=endpoint.endpoint_type,
                            last_success_at=now,
                        )
                        _record(settings, health)
                        return health.snapshot()
        last = OwnerChannelHealth(
            state="disconnected",
            endpoint_type=endpoint.endpoint_type,
            failure_classification=classification,
            last_failure_at=now,
            consecutive_failures=attempt + 1,
            retry_at=now + delay,
            recovery_action=(
                "Verify the configured PA bind is available in the ACP process "
                "network namespace, then retry or reconnect the session."
            ),
        )
        _record(settings, last)
        if attempt + 1 < attempts:
            time.sleep(delay)
            delay = min(delay * 2, 1.0)
    assert last is not None
    raise RuntimeError(
        "PA MCP owner channel startup probe failed "
        f"(endpoint_type={last.endpoint_type}, "
        f"classification={last.failure_classification}). "
        f"{last.recovery_action}"
    )
