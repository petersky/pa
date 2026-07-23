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
from pa.fleet.update import TERMINAL_PHASES
from pa.pr_supervisor.models import PRWatchStatus, canonical_repository_name

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
        key=EDGE_STATUS_SEVERITY.__getitem__,
    )


def _edge_status(value: Any) -> str:
    status = str(value or "unavailable")
    return status if status in EDGE_STATUS_SEVERITY else "unavailable"


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
        statuses = [_edge_status(edge.get("status")) for edge in members]
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
                raw_repository = str(watch.get("repository") or "unknown/unknown")
                try:
                    repository = canonical_repository_name(raw_repository)
                except ValueError:
                    repository = raw_repository.casefold()
                try:
                    number = int(watch.get("pr_number") or 0)
                except (TypeError, ValueError):
                    number = 0
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
) -> dict[str, Any]:
    """Build the stable browser/server field contract."""
    return {
        "state": state
        if state in {"fresh", "stale", "timeout", "error", "unavailable"}
        else "error",
        "value": value,
        "observed_at": observed_at,
        "duration_ms": round(duration_ms, 1) if duration_ms is not None else None,
        "error": (error or "")[:240] or None,
    }


class FleetOverviewCache:
    """Small persistent last-good cache; the PA server remains its only writer."""

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "fleet_overview_cache.json"
        self._lock = RLock()
        self._data: dict[str, dict[str, Any]] = {}
        try:
            payload = json.loads(self.path.read_text())
            if isinstance(payload, dict):
                self._data = dict(payload.get("instances") or {})
        except OSError, ValueError, TypeError:
            pass

    def get(self, instance_id: str, dimension: str) -> dict[str, Any] | None:
        with self._lock:
            value = (self._data.get(instance_id) or {}).get(dimension)
            return dict(value) if isinstance(value, dict) else None

    def put(self, instance_id: str, dimension: str, value: dict[str, Any]) -> None:
        with self._lock:
            current = self._data.setdefault(instance_id, {})
            previous = current.get(dimension)
            # A failed refresh never erases the last useful value.
            if (
                value.get("state") not in GOOD_STATES
                and previous
                and previous.get("value") is not None
            ):
                value = {
                    **value,
                    "value": previous.get("value"),
                    "observed_at": previous.get("observed_at"),
                }
            current[dimension] = value
            atomic_write_json(
                self.path,
                {"version": 1, "updated_at": _now(), "instances": self._data},
            )


_caches: dict[str, FleetOverviewCache] = {}
_caches_lock = Lock()
_probe_tasks: dict[tuple[str, str, str], asyncio.Task[dict[str, Any]]] = {}
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
    sessions = []
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
        sessions.append(
            {
                "id": session.id,
                "title": session.title or session.label or session.id,
                "card_id": session.card_id or session.item_id,
                "project_id": session.project_id,
                "status": "working"
                if runtime and runtime.prompting
                else session.status,
                "queued": len(runtime._queue) if runtime else 0,
                "cwd": session.cwd,
                "updated_at": session.updated_at.isoformat(),
            }
        )
    dispatches = []
    dispatch_store = ctx.services.get("dispatch_store")
    if dispatch_store:
        dispatches = [
            item.public_dict()
            for item in dispatch_store.list(limit=100)
            if item.state not in TERMINAL_DISPATCH_STATES
            and (
                item.target_instance_id == ctx.settings.instance_id
                or item.authority_instance_id == ctx.settings.instance_id
            )
        ]
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
    return {
        "state": state,
        "summary": progress.get("message") or state,
        "active_sessions": progress.get("active_sessions", len(sessions)),
        "queued_prompts": progress.get("queued_prompts", 0),
        "sessions": sessions,
        "dispatches": dispatches,
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
            from pa.acp.providers.resolve import list_provider_summaries

            value = await _offload(
                ctx,
                "fleet.overview.providers",
                list_provider_summaries,
                ctx.settings.data_dir,
                timeout=timeout,
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
            "error", None, duration_ms=elapsed, error=str(exc) or exc.__class__.__name__
        )


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
        task = _probe_tasks.get(key)
        if task is None or task.done():
            task = asyncio.create_task(_probe(ctx, inst, dimension))
            _probe_tasks[key] = task
    try:
        result = await asyncio.shield(task)
    finally:
        if task.done():
            async with _probe_lock:
                if _probe_tasks.get(key) is task:
                    _probe_tasks.pop(key, None)
    await _offload(
        ctx,
        "fleet.overview_cache_write",
        cache.put,
        inst.instance_id,
        dimension,
        result,
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
                    "label": f"{item.state} · {item.card_id or item.dispatch_id}",
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
        "version": 1,
        "generated_at": _now(),
        "local_instance_id": local_id,
        "dimensions": list(DIMENSIONS),
        "nodes": nodes,
        "edges": aggregate_topology_edges(edges),
    }
