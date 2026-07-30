"""Normalized, cache-first fleet overview state and bounded dimension probes."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock, RLock
from typing import Any

import httpx

from pa.core.async_runtime import AsyncRuntime
from pa.core.io import atomic_write_json
from pa.domain.models import FleetInstance
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES
from pa.fleet.capacity import effective_capacity
from pa.fleet.update import TERMINAL_PHASES
from pa.pr_supervisor.models import (
    PRWatchStatus,
    canonical_repository_name,
)

logger = logging.getLogger(__name__)

DIMENSIONS = (
    "reachability",
    "status",
    "providers",
    "update",
    "activity",
    "sync",
    "repositories",
    "supervisor",
)
DETAIL_TIMEOUT = 4.0
REACHABILITY_TIMEOUT = 2.5
GOOD_STATES = {"fresh", "stale"}
EDGE_STATUS_SEVERITY = {
    "healthy": 0,
    "stale": 1,
    "degraded": 2,
    "unavailable": 3,
}


def _runtime(ctx: Any) -> AsyncRuntime | None:
    services = getattr(ctx, "services", None)
    candidate = services.get("async_runtime") if isinstance(services, dict) else None
    return candidate if isinstance(candidate, AsyncRuntime) else None


async def _offload(ctx: Any, operation: str, call, *args, timeout=None, **kwargs):
    runtime = _runtime(ctx)
    if runtime:
        return await runtime.run_blocking(
            operation, call, *args, timeout=timeout, **kwargs
        )
    return await asyncio.to_thread(call, *args, **kwargs)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _worst_edge_status(statuses: list[str]) -> str:
    return max(
        statuses or ["unavailable"],
        key=lambda status: EDGE_STATUS_SEVERITY.get(status, 3),
    )


def _group_edge_id(key: tuple[str, str | None, str | None, str]) -> str:
    digest = hashlib.sha256(
        "\0".join("" if value is None else value for value in key).encode()
    ).hexdigest()[:16]
    return f"edge-{key[0]}-{digest}"


def aggregate_topology_edges(edges: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group coincident activity edges while retaining stable child identities."""
    groups: dict[
        tuple[str, str | None, str | None, str], list[dict[str, Any]]
    ] = {}
    for edge in edges:
        key = (
            str(edge.get("kind") or "activity"),
            edge.get("source"),
            edge.get("target"),
            str(edge.get("direction") or ""),
        )
        groups.setdefault(key, []).append(edge)

    result = []
    for key in sorted(
        groups,
        key=lambda item: tuple("" if value is None else value for value in item),
    ):
        members = sorted(groups[key], key=lambda edge: str(edge.get("id") or ""))
        statuses = [str(edge.get("status") or "unavailable") for edge in members]
        items = [
            {
                "id": str(edge.get("id") or ""),
                "status": status,
                "label": str(edge.get("label") or edge.get("id") or ""),
                "details": edge.get("details") or {},
            }
            for edge, status in zip(members, statuses, strict=True)
        ]
        kind, source, target, direction = key
        details: dict[str, Any] = {"items": items}
        label = items[0]["label"] if len(items) == 1 else f"{len(items)} {kind} activities"
        distinct_count = len(items)

        if kind == "supervisor":
            pull_requests: dict[tuple[str, int], list[dict[str, Any]]] = {}
            for item in items:
                watch = item["details"]
                try:
                    repository = canonical_repository_name(
                        str(watch.get("repository") or "")
                    )
                    number = int(watch.get("pr_number") or 0)
                except (TypeError, ValueError):
                    repository = str(watch.get("repository") or "unknown").casefold()
                    number = int(watch.get("pr_number") or 0)
                pull_requests.setdefault((repository, number), []).append(item)
            pr_rows = []
            for (repository, number), watches in sorted(pull_requests.items()):
                watch_statuses = [str(watch["status"]) for watch in watches]
                pr_rows.append(
                    {
                        "id": f"{repository}#{number}",
                        "repository": repository,
                        "pr_number": number,
                        "count": len(watches),
                        "status": _worst_edge_status(watch_statuses),
                        "watch_ids": [watch["id"] for watch in watches],
                    }
                )
            details["pull_requests"] = pr_rows
            distinct_count = len(pr_rows)
            if len(pr_rows) == 1:
                pr = pr_rows[0]
                label = f"PR {pr['repository']}#{pr['pr_number']}"
                if len(items) > 1:
                    label += f" · {len(items)} watches"
            else:
                label = f"{len(items)} watches · {len(pr_rows)} pull requests"

        result.append(
            {
                "id": _group_edge_id(key),
                "kind": kind,
                "source": source,
                "target": target,
                "direction": direction,
                "status": _worst_edge_status(statuses),
                "status_counts": dict(sorted(Counter(statuses).items())),
                "label": label,
                "count": len(items),
                "distinct_count": distinct_count,
                "details": details,
            }
        )
    return result


def field(
    state: str,
    value: Any = None,
    *,
    observed_at: str | None = None,
    duration_ms: float | None = None,
    error: str | None = None,
    failure_code: str | None = None,
    attempted_at: str | None = None,
) -> dict[str, Any]:
    """Build a last-success value plus independently observable attempt contract."""
    normalized = (
        state
        if state in {"fresh", "stale", "timeout", "error", "unavailable"}
        else "error"
    )
    safe_error = (error or "")[:240] or None
    attempt_time = attempted_at or (observed_at if normalized in GOOD_STATES else _now())
    successful_at = observed_at if normalized in GOOD_STATES and value is not None else None
    return {
        "state": normalized,
        "value": value,
        "observed_at": observed_at,
        "duration_ms": round(duration_ms, 1) if duration_ms is not None else None,
        "error": safe_error,
        "last_attempted_at": attempt_time,
        "last_attempt_state": normalized,
        "last_attempt_duration_ms": round(duration_ms, 1)
        if duration_ms is not None
        else None,
        "last_successful_at": successful_at,
        "failure": {
            "code": failure_code or normalized,
            "message": safe_error,
            "retryable": normalized in {"timeout", "error"},
        }
        if safe_error
        else None,
    }


class FleetOverviewCache:
    """Small persistent last-good cache; the PA server remains its only writer."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "fleet_overview_cache.json"
        self._lock = RLock()
        self._data: dict[str, dict[str, Any]] = {}
        self._revision = 0
        try:
            payload = json.loads(self.path.read_text())
            if isinstance(payload, dict):
                self._data = dict(payload.get("instances") or {})
                self._revision = int(payload.get("revision") or 0)
        except OSError, ValueError, TypeError:
            pass

    def get(self, instance_id: str, dimension: str) -> dict[str, Any] | None:
        with self._lock:
            value = (self._data.get(instance_id) or {}).get(dimension)
            return dict(value) if isinstance(value, dict) else None

    @property
    def revision(self) -> int:
        with self._lock:
            return self._revision

    def put(
        self,
        instance_id: str,
        dimension: str,
        value: dict[str, Any],
        *,
        attempt_id: int | None = None,
    ) -> bool:
        with self._lock:
            current = self._data.setdefault(instance_id, {})
            previous = current.get(dimension)
            candidate_id = attempt_id or time.time_ns()
            previous_id = int((previous or {}).get("attempt_id") or 0)
            if candidate_id < previous_id:
                return False
            value = {**value, "attempt_id": candidate_id}
            # A failed refresh is diagnostic metadata, never a replacement for
            # the last successful value/timestamp.
            if (
                value.get("state") not in GOOD_STATES
                and previous
                and previous.get("value") is not None
            ):
                value = {
                    **value,
                    "state": "stale",
                    "value": previous.get("value"),
                    "observed_at": previous.get("observed_at"),
                    "duration_ms": previous.get("duration_ms"),
                    "last_successful_at": previous.get("last_successful_at")
                    or previous.get("observed_at"),
                }
            current[dimension] = value
            self._revision += 1
            atomic_write_json(
                self.path,
                {
                    "version": 2,
                    "revision": self._revision,
                    "updated_at": _now(),
                    "instances": self._data,
                },
            )
            return True

    def invalidate_all(self, *dimensions: str) -> None:
        """Invalidate a dimension after a local mutation without guessing instance IDs."""
        with self._lock:
            changed = False
            for instance_id in list(self._data):
                current = self._data[instance_id]
                for dimension in dimensions:
                    changed = current.pop(dimension, None) is not None or changed
                if not current:
                    self._data.pop(instance_id, None)
            if changed:
                self._revision += 1
                atomic_write_json(
                    self.path,
                    {
                        "version": 2,
                        "revision": self._revision,
                        "updated_at": _now(),
                        "instances": self._data,
                    },
                )

    def invalidate(self, instance_id: str, *dimensions: str) -> None:
        """Drop fields made obsolete by a successful fleet mutation."""
        with self._lock:
            current = self._data.get(instance_id)
            if not current:
                return
            changed = False
            for dimension in dimensions:
                changed = current.pop(dimension, None) is not None or changed
            if not current:
                self._data.pop(instance_id, None)
            if changed:
                atomic_write_json(
                    self.path,
                    {
                        "version": 2,
                        "revision": self._revision + 1,
                        "updated_at": _now(),
                        "instances": self._data,
                    },
                )
                self._revision += 1


_caches: dict[str, FleetOverviewCache] = {}
_caches_lock = Lock()
_probe_tasks: dict[
    tuple[str, str, str], tuple[int, asyncio.Task[dict[str, Any]]]
] = {}
_probe_lock = asyncio.Lock()


def cache_for(data_dir: Path) -> FleetOverviewCache:
    key = str(data_dir)
    with _caches_lock:
        cache = _caches.get(key)
        if cache is None:
            cache = FleetOverviewCache(data_dir)
            _caches[key] = cache
        return cache


def _cached_or_default(
    cache: FleetOverviewCache, inst: FleetInstance, dimension: str
) -> dict[str, Any]:
    cached = cache.get(inst.instance_id, dimension)
    if cached:
        return {**cached, "state": "stale"}
    if dimension == "reachability":
        value = {"health": "up" if inst.healthy else "unknown"}
        return field(
            "stale" if inst.last_seen else "unavailable",
            value,
            observed_at=inst.last_seen.isoformat() if inst.last_seen else None,
        )
    return field("unavailable", None)


def _local_activity(ctx: Any) -> dict[str, Any]:
    from pa.server.shutdown import is_shutting_down

    manager = ctx.services.get("instance_agent")
    progress = (
        manager.progress().model_dump(mode="json")
        if manager
        else {
            "phase": "unavailable",
            "active_sessions": 0,
            "queued_prompts": 0,
            "quiescing": False,
            "prompting": False,
            "message": "Agent service unavailable",
        }
    )
    runtime_by_id = {
        runtime.session.id: runtime
        for runtime in (manager.list_runtimes() if manager else [])
        if not getattr(runtime, "_closed", False)
    }
    prompting_session_ids = {
        runtime.session.id for runtime in runtime_by_id.values() if runtime.prompting
    }
    sessions = []
    deferred_sessions = 0
    for session in ctx.store.list_sessions():
        runtime = runtime_by_id.get(session.id)
        active = bool(runtime) or session.status in {
            "active",
            "working",
            "prompting",
            "queued",
            "idle",
            "connected",
        }
        if not active:
            continue
        if runtime and runtime.prompting:
            semantic_state = "working"
        elif runtime and runtime._queue:
            semantic_state = "queued"
        elif runtime:
            semantic_state = "idle"
        else:
            semantic_state = "deferred"
            deferred_sessions += 1
        sessions.append(
            {
                "id": session.id,
                "title": session.title or session.label or session.id,
                "card_id": session.card_id or session.item_id,
                "project_id": session.project_id,
                "status": semantic_state,
                "durable_status": session.status,
                "connected": bool(runtime and runtime.connected),
                "capacity_consuming": semantic_state in {"working", "queued"},
                "provider": session.agent_name,
                "queued": len(runtime._queue) if runtime else 0,
                "cwd": session.cwd,
                "updated_at": session.updated_at.isoformat(),
            }
        )
    dispatches = []
    dispatch_store = ctx.services.get("dispatch_store")
    if dispatch_store:
        dispatch_store.expire_capacity_reservations()
        dispatches = [
            item.public_dict()
            for item in dispatch_store.list(limit=100)
            if item.state not in TERMINAL_DISPATCH_STATES
            and (
                item.target_instance_id == ctx.settings.instance_id
                or item.authority_instance_id == ctx.settings.instance_id
            )
        ]
    reservation_states = {
        "queued",
        "checking_sync",
        "materializing",
        "starting_session",
        "delivering_prompt",
    }
    reservations = [
        item
        for item in dispatches
        if item.get("target_instance_id") == ctx.settings.instance_id
        and item.get("state") in reservation_states
        and item.get("session_id") not in prompting_session_ids
    ]
    completion_work = [
        item
        for item in dispatches
        if item.get("state") == "completion_pending"
        or (item.get("card_reconciliation") or {}).get("state")
        in {"pending", "running", "retrying", "blocked"}
    ]
    provider_concurrency = {
        provider: dict(counts)
        for provider, counts in (progress.get("provider_concurrency") or {}).items()
    }
    for item in reservations:
        provider = str(item.get("capacity_provider") or "unknown").lower()
        counts = provider_concurrency.setdefault(
            provider,
            {
                "connected_runtimes": 0,
                "idle_sessions": 0,
                "prompting_turns": 0,
                "active_capacity_consumers": 0,
                "queued_prompts": 0,
            },
        )
        counts["dispatch_reservations"] = counts.get("dispatch_reservations", 0) + 1
    effective = effective_capacity(
        configured=ctx.settings.dispatch_capacity,
        provider_capacities=ctx.settings.dispatch_provider_capacities,
        capabilities=list(ctx.settings.capabilities),
    )
    capacity_links = [
        {
            "kind": "session",
            "session_id": item["id"],
            "href": f"/agent?session={item['id']}",
            "state": item["status"],
            "slots": 1 + int(item.get("queued") or 0),
        }
        for item in sessions
        if item["capacity_consuming"]
    ] + [
        {
            "kind": "dispatch",
            "dispatch_id": item.get("dispatch_id"),
            "card_id": item.get("card_id"),
            "href": (
                f"/?card={item.get('card_id')}" if item.get("card_id") else "/fleet"
            ),
            "state": item.get("state"),
            "slots": 1,
        }
        for item in reservations
    ]
    logger.debug(
        "fleet capacity utilization instance=%s configured=%s effective=%s "
        "source=%s active=%s queued=%s reservations=%s connected=%s idle=%s",
        ctx.settings.instance_id,
        ctx.settings.dispatch_capacity,
        effective.limit,
        effective.source,
        progress.get("active_capacity_consumers", 0),
        progress.get("queued_prompts", 0),
        len(reservations),
        progress.get("connected_runtimes", 0),
        progress.get("idle_sessions", 0),
    )
    state = "idle"
    if is_shutting_down():
        state = "shutting_down"
    elif progress.get("quiescing"):
        state = "quiescing"
    elif progress.get("prompting"):
        state = "working"
    elif progress.get("queued_prompts"):
        state = "queued"
    elif sessions:
        state = "active"
    current_dispatch = None
    active_dispatch_states = {
        "queued",
        "checking_sync",
        "materializing",
        "starting_session",
        "delivering_prompt",
        "running",
    }
    structured = [
        item
        for item in dispatches
        if (item.get("progress") or {}).get("latest")
        and item.get("state") in active_dispatch_states
        and not (item.get("dispatch_completion") or {}).get("completed")
    ]
    if structured:
        current_dispatch = max(
            structured,
            key=lambda item: (
                (item.get("progress") or {})
                .get("freshness", {})
                .get("last_activity_at")
                or item.get("updated_at")
                or ""
            ),
        )
        current_progress = current_dispatch["progress"]
        latest = current_progress["latest"]
        phase = latest.get("phase") or state
        state = "blocked" if phase == "blocked" else "working"
        progress["message"] = latest.get("summary") or progress.get("message")
    return {
        "state": state,
        "summary": progress.get("message") or state,
        "active_sessions": progress.get("active_sessions", len(sessions)),
        "connected_runtimes": progress.get("connected_runtimes", 0),
        "idle_sessions": progress.get("idle_sessions", 0),
        "deferred_sessions": deferred_sessions,
        "prompting_turns": progress.get("prompting_turns", 0),
        "active_capacity_consumers": progress.get("active_capacity_consumers", 0),
        "queued_prompts": progress.get("queued_prompts", 0),
        "dispatch_reservations": len(reservations),
        "durable_dispatches_starting": len(reservations),
        "completion_work": len(completion_work),
        "provider_concurrency": provider_concurrency,
        "capacity": {
            **effective.model_dump(mode="json"),
            "configured": ctx.settings.dispatch_capacity,
            "provider_limits": dict(ctx.settings.dispatch_provider_capacities),
            "consumed": progress.get("active_capacity_consumers", 0)
            + progress.get("queued_prompts", 0)
            + len(reservations),
        },
        "capacity_consumer_links": capacity_links,
        "capacity_policy": {
            "consumes": ["prompting_turns", "queued_prompts", "dispatch_reservations"],
            "does_not_consume": [
                "idle_sessions",
                "deferred_sessions",
                "completion_reconciliation",
                "provider_login_jobs",
                "control_plane_operations",
            ],
        },
        "sessions": sessions,
        "dispatches": dispatches,
        "current_dispatch": (
            {
                "dispatch_id": current_dispatch.get("dispatch_id"),
                "card_id": current_dispatch.get("card_id"),
                "session_id": current_dispatch.get("session_id"),
                "phase": current_dispatch["progress"]["latest"].get("phase"),
                "summary": current_dispatch["progress"]["latest"].get("summary"),
                "freshness": current_dispatch["progress"].get("freshness"),
            }
            if current_dispatch
            else None
        ),
    }


def _local_sync(ctx: Any) -> dict[str, Any]:
    realm = ctx.settings.primary_realm
    log = ctx.services.get("event_log")
    engine = ctx.services.get("sync_engine")
    durable = log.get_head(realm) if log else None
    projection = ctx.store.get_projection_head(realm)
    result = engine.status(realm) if engine else {"realm_id": realm}
    result.update(
        {
            "head": durable,
            "projection_head": projection,
            "consistent": durable == projection,
        }
    )
    return result


def _local_repositories(ctx: Any) -> dict[str, Any]:
    service = ctx.services.get("repository_state")
    observations = (
        [item.model_dump(mode="json") for item in service.list()] if service else []
    )
    manager = ctx.services.get("instance_agent")
    workspace_manager = getattr(manager, "workspace_manager", None)
    leases = (
        [item.model_dump(mode="json") for item in workspace_manager.list()]
        if workspace_manager
        else []
    )
    return {"observations": observations, "workspaces": leases}


def _local_supervisor(ctx: Any) -> dict[str, Any]:
    service = ctx.services.get("pr_supervisor")
    store = ctx.services.get("pr_supervisor_store")
    health = (
        service.authority_health()
        if service
        else {"state": "unavailable", "role": "unavailable"}
    )
    watches = (
        [
            item.model_dump(mode="json")
            for item in store.list_watches(include_retired=False)
            if item.actionable
        ]
        if store
        else []
    )
    return {**health, "watches": watches}


def local_dimension(ctx: Any, dimension: str) -> Any:
    if dimension == "reachability":
        return {"health": "up"}
    if dimension == "status":
        return {
            "version": __import__("pa").__version__,
            "release_track": ctx.settings.release_track,
            "lifecycle": "running",
        }
    if dimension == "activity":
        return _local_activity(ctx)
    if dimension == "sync":
        return _local_sync(ctx)
    if dimension == "repositories":
        return _local_repositories(ctx)
    if dimension == "supervisor":
        return _local_supervisor(ctx)
    raise KeyError(dimension)


async def _json_get(
    ctx: Any, client: httpx.AsyncClient, url: str, headers: dict[str, str]
) -> Any:
    runtime = _runtime(ctx)
    request = client.get(url, headers=headers)
    response = (
        await runtime.observe(
            "http.fleet_overview", request, timeout=DETAIL_TIMEOUT
        )
        if runtime
        else await request
    )
    response.raise_for_status()
    return await _offload(
        ctx, "fleet.overview_response_json", response.json, timeout=2.0
    )


async def _probe(ctx: Any, inst: FleetInstance, dimension: str) -> dict[str, Any]:
    started = time.perf_counter()
    is_local = inst.instance_id == ctx.settings.instance_id
    timeout = REACHABILITY_TIMEOUT if dimension == "reachability" else DETAIL_TIMEOUT
    try:
        if is_local and dimension not in {"providers", "update"}:
            value = await _offload(
                ctx,
                f"fleet.overview.{dimension}",
                local_dimension,
                ctx,
                dimension,
                timeout=timeout,
            )
        elif is_local and dimension == "providers":
            from pa.acp.providers.resolve import list_provider_summaries_bounded

            value = await list_provider_summaries_bounded(
                ctx.settings.data_dir,
                manager=ctx.services.get("instance_agent"),
                async_runtime=_runtime(ctx),
                timeout=max(0.5, timeout - 0.5),
            )
        elif is_local and dimension == "update":
            from pa.update.runner import check_update

            result = await _offload(
                ctx,
                "fleet.overview.update",
                check_update,
                ctx.settings,
                timeout=timeout,
            )
            value = {
                "current_version": result.current,
                "available_version": result.latest,
                "upgrade_available": result.upgrade_available,
                "channel": ctx.settings.release_track,
            }
        else:
            headers = {}
            if ctx.settings.sync_token:
                headers["Authorization"] = f"Bearer {ctx.settings.sync_token}"
            base = inst.url.rstrip("/")
            client = ctx.services.get("fleet_http_client")
            owns_client = client is None
            client = client or httpx.AsyncClient(timeout=timeout)
            try:
                if dimension == "reachability":
                    await _json_get(ctx, client, f"{base}/api/health", {})
                    value = {"health": "up"}
                elif dimension == "status":
                    value = await _json_get(
                        ctx, client, f"{base}/api/status", headers
                    )
                elif dimension == "providers":
                    value = await _json_get(
                        ctx, client, f"{base}/api/agent/providers", headers
                    )
                elif dimension == "update":
                    value = await _json_get(
                        ctx,
                        client,
                        f"{base}/api/fleet/peer-update-check",
                        headers,
                    )
                else:
                    payload = await _json_get(
                        ctx,
                        client,
                        f"{base}/api/fleet/overview/local?dimension={dimension}",
                        headers,
                    )
                    value = (
                        payload.get("value") if isinstance(payload, dict) else payload
                    )
            finally:
                if owns_client:
                    await client.aclose()
        elapsed = (time.perf_counter() - started) * 1000
        return field("fresh", value, observed_at=_now(), duration_ms=elapsed)
    except TimeoutError, asyncio.TimeoutError, httpx.TimeoutException:
        elapsed = (time.perf_counter() - started) * 1000
        return field(
            "timeout",
            None,
            duration_ms=elapsed,
            error=f"{dimension} exceeded {timeout:g}s deadline",
            failure_code="deadline_exceeded",
        )
    except (
        httpx.HTTPError,
        OSError,
        RuntimeError,
        ValueError,
        TypeError,
        AttributeError,
        KeyError,
    ) as exc:
        elapsed = (time.perf_counter() - started) * 1000
        return field(
            "error",
            None,
            duration_ms=elapsed,
            error=f"{dimension} probe failed ({exc.__class__.__name__})",
            failure_code="probe_failed",
        )


def _merge_provider_snapshots(previous: Any, current: Any) -> list[dict[str, Any]]:
    """Retain affirmative auth across inconclusive provider attempts only."""
    previous_by_id = {
        str(item.get("id")): item
        for item in (previous if isinstance(previous, list) else [])
        if isinstance(item, dict) and item.get("id")
    }
    merged: list[dict[str, Any]] = []
    for raw in current if isinstance(current, list) else []:
        if not isinstance(raw, dict):
            continue
        item = dict(raw)
        prior = previous_by_id.get(str(item.get("id")))
        state = str(item.get("auth_state") or "unknown")
        attempt_state = str(item.get("direct_auth_state") or state)
        if (
            prior
            and prior.get("auth_state") == "authenticated"
            and attempt_state in {"timed_out", "probe_failed", "unknown"}
        ):
            for key in (
                "installed",
                "available",
                "command",
                "resolved_path",
                "version",
                "codex_cli_installed",
                "codex_cli_path",
                "codex_cli_version",
                "install_method",
            ):
                if key in prior:
                    item[key] = prior[key]
            item.update(
                {
                    "auth_state": "authenticated",
                    "auth_configured": True,
                    "auth_method": prior.get("auth_method") or "authenticated",
                    "auth_status": prior.get("auth_status"),
                    "last_successful_at": prior.get("last_successful_at")
                    or prior.get("last_attempted_at"),
                    "stale": True,
                    "last_attempt": {
                        "state": attempt_state,
                        "at": item.get("last_attempted_at"),
                        "duration_ms": item.get("probe_duration_ms"),
                        "error": item.get("auth_error") or item.get("error"),
                    },
                }
            )
            item["auth_evidence"] = list(
                dict.fromkeys(
                    list(prior.get("auth_evidence") or [])
                    + list(item.get("auth_evidence") or [])
                )
            )
        merged.append(item)
    return merged


async def probe_dimension(
    ctx: Any,
    inst: FleetInstance,
    dimension: str,
    *,
    force: bool = False,
) -> dict[str, Any]:
    """Single-flight a bounded probe and preserve the previous good value."""
    cache = await _offload(
        ctx, "fleet.overview_cache_read", cache_for, ctx.settings.data_dir
    )
    cached = await _offload(
        ctx, "fleet.overview_cache_read", cache.get, inst.instance_id, dimension
    )
    if cached and not force and cached.get("state") == "fresh":
        try:
            observed = datetime.fromisoformat(str(cached.get("observed_at")))
            age = (datetime.now(UTC) - observed).total_seconds()
            ttl = 30.0 if dimension in {"providers", "update"} else 3.0
            if age < ttl:
                return {**cached, "cache_hit": True}
        except TypeError, ValueError:
            pass
    key = (str(ctx.settings.data_dir), inst.instance_id, dimension)
    async with _probe_lock:
        active = _probe_tasks.get(key)
        if active is None or active[1].done() or force:
            attempt_id = time.time_ns()
            task = asyncio.create_task(_probe(ctx, inst, dimension))
            _probe_tasks[key] = (attempt_id, task)
        else:
            attempt_id, task = active
    try:
        result = await asyncio.shield(task)
    finally:
        if task.done():
            async with _probe_lock:
                if _probe_tasks.get(key) == (attempt_id, task):
                    _probe_tasks.pop(key, None)
    if dimension == "providers" and result.get("state") in GOOD_STATES:
        result = {
            **result,
            "value": _merge_provider_snapshots(
                (cached or {}).get("value"), result.get("value")
            ),
        }
    await _offload(
        ctx,
        "fleet.overview_cache_write",
        cache.put,
        inst.instance_id,
        dimension,
        result,
        attempt_id=attempt_id,
    )
    merged = (
        await _offload(
            ctx,
            "fleet.overview_cache_read",
            cache.get,
            inst.instance_id,
            dimension,
        )
        or result
    )
    logger.info(
        "fleet overview probe instance=%s dimension=%s state=%s duration_ms=%s",
        inst.instance_id,
        dimension,
        merged.get("state"),
        result.get("duration_ms"),
    )
    return merged


def build_overview(
    ctx: Any, instances: list[FleetInstance], peer_routes: list[Any]
) -> dict[str, Any]:
    """Compose one normalized source for the initial table and topology."""
    cache = cache_for(ctx.settings.data_dir)
    nodes = []
    by_url = {item.url.rstrip("/"): item.instance_id for item in instances}
    by_id: dict[str, dict[str, Any]] = {}
    for inst in instances:
        dimensions = {
            dimension: _cached_or_default(cache, inst, dimension)
            for dimension in DIMENSIONS
        }
        if inst.instance_id == ctx.settings.instance_id:
            for dimension in (
                "reachability",
                "status",
                "activity",
                "sync",
                "repositories",
                "supervisor",
            ):
                try:
                    dimensions[dimension] = field(
                        "fresh",
                        local_dimension(ctx, dimension),
                        observed_at=_now(),
                        duration_ms=0,
                    )
                except (
                    OSError,
                    RuntimeError,
                    ValueError,
                    TypeError,
                    AttributeError,
                    KeyError,
                ) as exc:
                    dimensions[dimension] = field("error", None, error=str(exc))
        node = {
            "id": inst.instance_id,
            "name": inst.name,
            "url": inst.url,
            "zone": inst.zone,
            "capabilities": list(inst.capabilities),
            "dispatch_capacity": inst.dispatch_capacity,
            "dispatch_provider_capacities": dict(inst.dispatch_provider_capacities),
            "lifecycle_state": inst.lifecycle_state,
            "membership_generation": inst.membership_generation,
            "local": inst.instance_id == ctx.settings.instance_id,
            "last_seen": inst.last_seen.isoformat() if inst.last_seen else None,
            "dimensions": dimensions,
        }
        nodes.append(node)
        by_id[inst.instance_id] = node

    edges: list[dict[str, Any]] = []
    local_id = ctx.settings.instance_id
    for index, route in enumerate(peer_routes):
        target = route.target_instance_id or by_url.get(route.target_url.rstrip("/"))
        if target not in by_id:
            continue
        edges.append(
            {
                "id": f"route-{index}",
                "kind": "sync",
                "source": local_id,
                "target": target,
                "direction": "outbound",
                "status": "healthy" if target in by_id else "unavailable",
                "label": f"{route.realm_id} · {route.mode.value}",
                "details": route.model_dump(mode="json"),
            }
        )

    dispatch_store = ctx.services.get("dispatch_store")
    if dispatch_store:
        for item in dispatch_store.list(limit=100):
            if item.state in TERMINAL_DISPATCH_STATES:
                continue
            edges.append(
                {
                    "id": f"dispatch-{item.dispatch_id}",
                    "kind": "dispatch",
                    "source": item.authority_instance_id,
                    "target": item.target_instance_id,
                    "direction": "authority-to-target",
                    "status": "degraded" if item.last_error else "healthy",
                    "label": (
                        f"{item.latest_progress.phase.value} · "
                        f"{item.card_id or item.dispatch_id}"
                        if item.latest_progress
                        else f"{item.state} · {item.card_id or item.dispatch_id}"
                    ),
                    "details": item.public_dict(),
                }
            )

    update_store = ctx.services.get("fleet_update_job_store")
    if update_store:
        for job in update_store.list():
            if job.phase in TERMINAL_PHASES:
                continue
            node = by_id.get(job.instance_id)
            if node:
                activity = node["dimensions"]["activity"]
                value = dict(activity.get("value") or {})
                phase = job.phase.value
                lifecycle = (
                    "quiescing"
                    if phase == "quiescing"
                    else (
                        "starting"
                        if phase in {"restarting", "waiting_install", "verifying"}
                        else "updating"
                    )
                )
                value.update({"state": lifecycle, "update_job": job.public_dict()})
                node["dimensions"]["activity"] = {**activity, "value": value}

    supervisor_store = ctx.services.get("pr_supervisor_store")
    if supervisor_store:
        for watch in supervisor_store.list_watches(include_retired=False):
            if not watch.actionable:
                continue
            owner = watch.owner_instance_id or local_id
            target = watch.originating_instance_id or owner
            watch_status = getattr(watch.status, "value", watch.status)
            status = (
                "degraded"
                if watch.last_error or watch_status == PRWatchStatus.BLOCKED.value
                else "healthy"
            )
            if owner not in by_id or target not in by_id:
                status = "unavailable"
            edges.append(
                {
                    "id": f"watch-{watch.id}",
                    "kind": "supervisor",
                    "source": owner,
                    "target": target,
                    "direction": "owner-to-origin",
                    "status": status,
                    "label": f"PR {watch.repository}#{watch.pr_number}",
                    "details": watch.model_dump(mode="json"),
                }
            )

    try:
        for repository in ctx.store.list_repositories(ctx.settings.primary_realm):
            for checkout in ctx.store.list_repository_checkouts(repository.id):
                edges.append(
                    {
                        "id": f"repository-{repository.id}-{checkout.instance_id}",
                        "kind": "repository",
                        "source": checkout.instance_id,
                        "target": checkout.instance_id,
                        "direction": "placement",
                        "status": "healthy"
                        if checkout.instance_id in by_id
                        else "unavailable",
                        "label": f"{repository.name or repository.url} · {checkout.branch or repository.default_branch or 'default'}",
                        "details": {
                            "repository": repository.model_dump(mode="json"),
                            "checkout": checkout.model_dump(mode="json"),
                        },
                    }
                )
    except OSError, RuntimeError, ValueError, TypeError, AttributeError:
        pass

    return {
        "version": 2,
        "snapshot_version": cache.revision,
        "membership_version": getattr(
            ctx.services.get("fleet_registry"), "generation", 0
        ),
        "generated_at": _now(),
        "local_instance_id": local_id,
        "dimensions": list(DIMENSIONS),
        "nodes": nodes,
        "edges": aggregate_topology_edges(edges),
    }
