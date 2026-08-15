"""Admission gate for local PA startup.

``/api/health`` is process liveness. ``/api/ready`` is the operator and owner-channel
admission gate: required services, warmed OpenAPI paths, finished ACP startup, and
completed local sync projection repair. Peer/network convergence stays background.
"""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI

from pa.config import Settings
from pa.server.listeners import owner_channel_health

REQUIRED_READY_SERVICES = frozenset(
    {
        "instance_agent",
        "async_runtime",
        "event_log",
        "fleet_registry",
    }
)
REQUIRED_READY_PATHS = frozenset(
    {
        "/api/cards",
        "/api/items",
        "/api/projects",
        "/api/sync/status",
    }
)
READY_LIFECYCLE_PHASES = frozenset({"ready", "idle", "error"})
OWNER_NOT_READY_STATES = frozenset({"disconnected", "degraded"})


def warm_ready_contract(app: FastAPI) -> None:
    """Generate OpenAPI once so ``/api/ready`` never pays schema cost."""
    schema = app.openapi()
    app.state.ready_paths = frozenset(schema.get("paths", {}))
    app.state.required_ready_paths = REQUIRED_READY_PATHS
    app.state.ready_openapi_warmed = True


def evaluate_ready(app: FastAPI, ctx: Any, settings: Settings) -> dict[str, Any] | None:
    """Return a 503 detail payload when admission is not yet complete."""
    services = ctx.services
    missing_services = sorted(REQUIRED_READY_SERVICES - services.keys())
    if missing_services:
        return {"status": "starting", "missing_services": missing_services}

    warmed_paths = getattr(app.state, "ready_paths", None)
    if not getattr(app.state, "ready_openapi_warmed", False) or warmed_paths is None:
        return {"status": "starting", "missing_routes": ["openapi"]}
    required_paths = getattr(app.state, "required_ready_paths", REQUIRED_READY_PATHS)
    missing_routes = sorted(required_paths - set(warmed_paths))
    if missing_routes:
        return {"status": "starting", "missing_routes": missing_routes}

    lifecycle = dict(services.get("agent_lifecycle") or {})
    phase = str(lifecycle.get("phase") or "")
    if phase not in READY_LIFECYCLE_PHASES:
        return {"status": "starting", "lifecycle": phase or "unknown"}

    if "event_log" in services and not services.get("sync_startup_repaired"):
        return {"status": "starting", "sync": "repair_pending"}

    owner = owner_channel_health(settings)
    owner_state = str(owner.get("state") or "")
    if owner_state in OWNER_NOT_READY_STATES:
        return {
            "status": "starting",
            "owner_channel": owner_state,
            "owner_classification": owner.get("failure_classification"),
        }
    return None
