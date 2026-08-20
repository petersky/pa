"""Shared fleet endpoint selection, health persistence, and circuit breaking."""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from threading import RLock
from typing import Any

import httpx

from pa.core.io import atomic_write_json
from pa.domain.models import FleetInstance
from pa.status.serving import loopback_base_url

BASE_BACKOFF = 5.0
MAX_BACKOFF = 300.0
SUCCESS_FRESHNESS = 60.0


class EndpointUnavailable(TimeoutError):
    """All canonical endpoints are in an open circuit or failed transport."""

    def __init__(self, instance_id: str, endpoints: list[str], retry_after: float):
        self.instance_id = instance_id
        self.endpoints = endpoints
        self.selected_endpoint = endpoints[0] if endpoints else None
        self.retry_after = max(0.0, retry_after)
        super().__init__(
            f"No healthy endpoint for {instance_id}; selected endpoint "
            f"{self.selected_endpoint or 'none'}; retry after "
            f"{self.retry_after:.1f}s"
        )


@dataclass(frozen=True)
class EndpointChoice:
    url: str
    circuit_open: bool
    next_probe_at: float


class EndpointHealthRegistry:
    """Persistent health keyed by endpoint rather than fleet dimension."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "fleet_endpoint_health.json"
        self._lock = RLock()
        self._states: dict[str, dict[str, Any]] = {}
        try:
            payload = json.loads(self.path.read_text())
            if isinstance(payload, dict):
                self._states = dict(payload.get("endpoints") or {})
        except (OSError, ValueError, TypeError):
            pass

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {key: dict(value) for key, value in self._states.items()}

    def _save(self) -> None:
        atomic_write_json(
            self.path,
            {
                "version": 1,
                "updated_at": datetime.now(UTC).isoformat(),
                "endpoints": self._states,
            },
        )

    def success(self, endpoint: str, latency_ms: float) -> None:
        endpoint = endpoint.rstrip("/")
        with self._lock:
            previous = self._states.get(endpoint) or {}
            old_latency = previous.get("latency_ms")
            latency = (
                latency_ms
                if not isinstance(old_latency, (int, float))
                else old_latency * 0.7 + latency_ms * 0.3
            )
            self._states[endpoint] = {
                "state": "healthy",
                "last_success_at": time.time(),
                "last_failure_at": previous.get("last_failure_at"),
                "latency_ms": round(latency, 1),
                "failures": 0,
                "next_probe_at": 0.0,
                "error_class": None,
                "error": None,
            }
            self._save()

    def failure(self, endpoint: str, exc: BaseException) -> float:
        endpoint = endpoint.rstrip("/")
        with self._lock:
            previous = self._states.get(endpoint) or {}
            failures = min(10, int(previous.get("failures") or 0) + 1)
            delay = min(MAX_BACKOFF, BASE_BACKOFF * (2 ** (failures - 1)))
            delay *= random.uniform(0.8, 1.2)
            next_probe = time.time() + delay
            self._states[endpoint] = {
                **previous,
                "state": "down",
                "last_failure_at": time.time(),
                "failures": failures,
                "next_probe_at": next_probe,
                "error_class": type(exc).__name__,
                "error": str(exc)[:240],
            }
            self._save()
            return next_probe

    def choices(self, settings: Any, inst: FleetInstance) -> list[EndpointChoice]:
        local = inst.instance_id == settings.instance_id
        canonical = list(
            dict.fromkeys(
                [
                    *([loopback_base_url(settings)] if local else []),
                    *(endpoint.rstrip("/") for endpoint in inst.endpoints),
                    inst.url.rstrip("/"),
                ]
            )
        )
        now = time.time()
        with self._lock:
            states = {key: dict(self._states.get(key) or {}) for key in canonical}

        def rank(endpoint: str) -> tuple[int, int, float, int]:
            state = states[endpoint]
            next_probe = float(state.get("next_probe_at") or 0.0)
            open_circuit = next_probe > now
            successful = float(state.get("last_success_at") or 0.0)
            fresh = now - successful <= SUCCESS_FRESHNESS
            latency = float(state.get("latency_ms") or float("inf"))
            return (
                0 if local and endpoint == loopback_base_url(settings) else 1,
                0 if fresh else 1,
                latency,
                1 if open_circuit else 0,
            )

        canonical.sort(key=rank)
        return [
            EndpointChoice(
                endpoint,
                float(states[endpoint].get("next_probe_at") or 0.0) > now,
                float(states[endpoint].get("next_probe_at") or 0.0),
            )
            for endpoint in canonical
        ]


async def request_peer(
    ctx: Any,
    client: httpx.AsyncClient,
    inst: FleetInstance,
    method: str,
    path: str,
    *,
    timeout: float,
    **kwargs: Any,
) -> tuple[httpx.Response, str]:
    """Request a peer through the best canonical endpoint with failover."""
    registry = ctx.services.get("endpoint_health_registry")
    if not isinstance(registry, EndpointHealthRegistry):
        registry = EndpointHealthRegistry(ctx.settings.data_dir)
        ctx.services["endpoint_health_registry"] = registry
    choices = registry.choices(ctx.settings, inst)
    available = [choice for choice in choices if not choice.circuit_open]
    if not available:
        retry = min((choice.next_probe_at for choice in choices), default=time.time())
        raise EndpointUnavailable(inst.instance_id, [c.url for c in choices], retry - time.time())
    failures: list[tuple[str, BaseException]] = []
    for choice in available:
        started = time.perf_counter()
        try:
            response = await client.request(
                method,
                f"{choice.url.rstrip('/')}/{path.lstrip('/')}",
                timeout=timeout,
                **kwargs,
            )
        except (httpx.TransportError, TimeoutError) as exc:
            registry.failure(choice.url, exc)
            failures.append((choice.url, exc))
            continue
        registry.success(choice.url, (time.perf_counter() - started) * 1000)
        response.extensions["pa_selected_endpoint"] = choice.url
        return response, choice.url
    last_endpoint, last_exc = failures[-1]
    choices = registry.choices(ctx.settings, inst)
    retry = min((choice.next_probe_at for choice in choices), default=time.time())
    unavailable = EndpointUnavailable(
        inst.instance_id, [last_endpoint, *[c.url for c in choices if c.url != last_endpoint]], retry - time.time()
    )
    unavailable.__cause__ = last_exc
    raise unavailable
