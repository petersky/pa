from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Any, Literal

import httpx
from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id
from pa.config import Settings
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.instance_config import update_instance_config
from pa.telemetry.models import TelemetryQuery
from pa.telemetry.service import TelemetryService
from pa.telemetry.storage import TelemetryStorage

router = APIRouter()
ui_router = APIRouter()

RANGES = {
    "15m": timedelta(minutes=15),
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
    "30d": timedelta(days=30),
}
DEFAULT_METRICS = [
    "cpu.utilization",
    "memory.utilization",
    "swap.utilization",
    "disk.read_throughput",
    "disk.write_throughput",
    "disk.read_iops",
    "disk.write_iops",
    "disk.latency",
    "network.ingress",
    "network.egress",
    "network.connections",
    "network.errors",
    "pa.cpu",
    "pa.memory_rss",
    "pa.threads",
    "session.cpu",
    "session.memory_rss",
    "session.disk_read",
    "session.disk_write",
    "session.network_ingress",
    "session.network_egress",
    "session.processes",
    "session.tasks",
    "agents.concurrent",
]


class QueryBody(BaseModel):
    range: str = "1h"
    start: datetime | None = None
    end: datetime | None = None
    scope_type: Literal["instance", "session"] | None = None
    scope_ids: list[str] = Field(default_factory=list, max_length=50)
    instance_ids: list[str] = Field(default_factory=list, max_length=50)
    provider_ids: list[str] = Field(default_factory=list, max_length=50)
    card_ids: list[str] = Field(default_factory=list, max_length=50)
    metrics: list[str] = Field(default_factory=list, max_length=100)
    bucket_seconds: int | None = Field(default=None, ge=1, le=86400)


class ConfigPatch(BaseModel):
    enabled: bool | None = None
    live_interval_seconds: float | None = Field(default=None, ge=1, le=300)
    persistence_interval_seconds: float | None = Field(default=None, ge=5, le=3600)
    raw_retention_hours: float | None = Field(default=None, ge=1, le=8760)
    rollup_retention_hours: float | None = Field(default=None, ge=1, le=43800)
    max_database_bytes: int | None = Field(
        default=None, ge=16 * 1024 * 1024, le=64 * 1024 * 1024 * 1024
    )
    database_path: Path | None = None
    per_session_enabled: bool | None = None
    ui_refresh_seconds: float | None = Field(default=None, ge=2, le=300)
    default_report_range: str | None = None


class MaintenanceBody(BaseModel):
    action: Literal["prune", "compact"] = "prune"


def _service(request: Request) -> TelemetryService:
    return request.app.state.ctx.require_service("telemetry")


def _principal(request: Request) -> str | None:
    return (
        get_principal_id(request)
        if request.app.state.ctx.settings.auth_required
        else None
    )


def _admin(request: Request) -> bool:
    user = getattr(request.state, "user", None)
    return bool(user and user.role == "admin")


def _instance_caller(request: Request) -> bool:
    return bool(getattr(request.state, "instance_authenticated", False)) and not bool(
        getattr(request.state, "user_authenticated", False)
    )


def _require_admin(request: Request) -> None:
    if not _admin(request):
        raise HTTPException(status_code=403, detail="Administrator access required")


def _make_query(
    request: Request, body: QueryBody, *, force_instance: bool = False
) -> TelemetryQuery:
    end = body.end or datetime.now(UTC)
    if body.start:
        start = body.start
    else:
        try:
            start = end - RANGES[body.range]
        except KeyError as exc:
            raise HTTPException(
                status_code=422, detail="Unsupported report range"
            ) from exc
    if start.tzinfo is None or end.tzinfo is None:
        raise HTTPException(
            status_code=422, detail="Timestamps must include a timezone"
        )
    duration = end - start
    if duration <= timedelta(0) or duration > timedelta(days=31):
        raise HTTPException(
            status_code=422, detail="Range must be positive and at most 31 days"
        )
    bucket = body.bucket_seconds or max(1, int(duration.total_seconds() / 360))
    scope_type = "instance" if force_instance else body.scope_type
    scope_ids = body.scope_ids
    if force_instance:
        scope_ids = []
    return TelemetryQuery(
        start=start,
        end=end,
        scope_type=scope_type,
        scope_ids=scope_ids,
        instance_ids=body.instance_ids,
        provider_ids=[] if force_instance else body.provider_ids,
        card_ids=[] if force_instance else body.card_ids,
        metrics=body.metrics or DEFAULT_METRICS,
        bucket_seconds=bucket,
        visible_principal_id=(
            None if force_instance or _admin(request) else _principal(request)
        ),
    )


@router.get("/telemetry/live")
async def live(
    request: Request,
    scope_type: Literal["instance", "session"] | None = None,
    scope_id: str | None = None,
) -> dict:
    if _instance_caller(request):
        scope_type, scope_id = "instance", None
    return _service(request).live(
        scope_type=scope_type,
        scope_id=scope_id,
        principal_id=_principal(request),
        auth_required=(
            request.app.state.ctx.settings.auth_required and not _admin(request)
        ),
    )


@router.get("/telemetry/health")
def health(request: Request) -> dict:
    return _service(request).health()


@router.get("/telemetry/storage")
def storage_status(request: Request) -> dict:
    service = _service(request)
    return {
        **service.storage.status(),
        "dropped_samples": service.dropped_samples,
        "storage_failures": service.storage_failures,
    }


@router.get("/telemetry/config")
def telemetry_config(request: Request) -> dict:
    return {
        "effective": _service(request).effective_config(),
        "constraints": {
            "persistence_interval_gte_live_interval": True,
            "rollup_retention_gte_raw_retention": True,
            "maximum_custom_range_days": 31,
            "database_must_be_outside_metadata_and_sync_authority": True,
        },
    }


@router.get("/telemetry/dimensions")
def dimensions(request: Request) -> dict:
    if _instance_caller(request):
        raise HTTPException(status_code=403, detail="User authorization required")
    return _service(request).storage.dimensions(
        visible_principal_id=None if _admin(request) else _principal(request)
    )


@router.get("/telemetry/series")
def series(
    request: Request,
    range: str = "1h",
    start: datetime | None = None,
    end: datetime | None = None,
    scope_type: Literal["instance", "session"] | None = None,
    scope_id: Annotated[list[str] | None, Query()] = None,
    instance_id: Annotated[list[str] | None, Query()] = None,
    provider_id: Annotated[list[str] | None, Query()] = None,
    card_id: Annotated[list[str] | None, Query()] = None,
    metric: Annotated[list[str] | None, Query()] = None,
    bucket_seconds: int | None = None,
) -> dict:
    body = QueryBody(
        range=range,
        start=start,
        end=end,
        scope_type=scope_type,
        scope_ids=scope_id or [],
        instance_ids=instance_id or [],
        provider_ids=provider_id or [],
        card_ids=card_id or [],
        metrics=metric or [],
        bucket_seconds=bucket_seconds,
    )
    query = _make_query(request, body, force_instance=_instance_caller(request))
    try:
        return _service(request).storage.query(query)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.post("/telemetry/query")
def query_series(request: Request, body: QueryBody) -> dict:
    query = _make_query(request, body, force_instance=_instance_caller(request))
    try:
        return _service(request).storage.query(query)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@router.get("/telemetry/export")
def export(
    request: Request,
    range: str = "15m",
    scope_type: Literal["instance", "session"] | None = None,
    scope_id: Annotated[list[str] | None, Query()] = None,
) -> JSONResponse:
    if _instance_caller(request):
        raise HTTPException(status_code=403, detail="User authorization required")
    query = _make_query(
        request,
        QueryBody(range=range, scope_type=scope_type, scope_ids=scope_id or []),
    )
    try:
        payload = _service(request).storage.export(query)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return JSONResponse(
        payload,
        headers={
            "Content-Disposition": f'attachment; filename="pa-telemetry-{stamp}.json"'
        },
    )


@router.patch("/telemetry/config")
async def update_config(request: Request, body: ConfigPatch) -> dict:
    _require_admin(request)
    settings = request.app.state.ctx.settings
    updates = {
        f"telemetry_{name}": value
        for name, value in body.model_dump(exclude_unset=True).items()
    }
    if not updates:
        return telemetry_config(request)
    candidate_data = settings.model_dump()
    candidate_data.update(updates)
    try:
        candidate = Settings(**candidate_data)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    persisted = {
        key: str(value) if isinstance(value, Path) else value
        for key, value in updates.items()
    }
    update_instance_config(settings.data_dir, **persisted)
    path_changed = candidate.telemetry_database_path != settings.telemetry_database_path
    service = _service(request)
    was_enabled = settings.telemetry_enabled
    for key in updates:
        if key != "telemetry_database_path":
            setattr(settings, key, getattr(candidate, key))
    if was_enabled and not settings.telemetry_enabled:
        await service.stop()
    elif not was_enabled and settings.telemetry_enabled:
        await service.start()
    return {
        **telemetry_config(request),
        "persisted": True,
        "restart_required": path_changed,
        "restart_reason": (
            "database_path changes take effect after a bounded PA restart"
            if path_changed
            else None
        ),
    }


@router.post("/telemetry/maintenance")
async def maintenance(request: Request, body: MaintenanceBody) -> dict:
    _require_admin(request)
    service = _service(request)
    if body.action == "compact":
        return await service._run(service.storage.compact)
    return await service._run(
        service.storage.prune,
        raw_retention_hours=service.settings.telemetry_raw_retention_hours,
        rollup_retention_hours=service.settings.telemetry_rollup_retention_hours,
        max_database_bytes=service.settings.telemetry_max_database_bytes,
    )


async def _peer_json(
    client: httpx.AsyncClient,
    url: str,
    path: str,
    *,
    token: str,
    instance_id: str,
    body: dict | None = None,
) -> dict:
    headers = {"X-PA-Origin-Instance-ID": instance_id}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    response = (
        await client.post(url.rstrip("/") + path, headers=headers, json=body)
        if body is not None
        else await client.get(url.rstrip("/") + path, headers=headers)
    )
    response.raise_for_status()
    return response.json()


@router.get("/telemetry/fleet/live")
async def fleet_live(request: Request) -> dict:
    if _instance_caller(request):
        raise HTTPException(status_code=403, detail="User authorization required")
    ctx = request.app.state.ctx
    local = _service(request).live(scope_type="instance")
    results = list(local["samples"])
    failures = []
    client = ctx.services.get("fleet_http_client")
    registry = ctx.services.get("fleet_registry")
    if client and registry:
        peers = [
            item
            for item in registry.list_instances()
            if item.instance_id != ctx.settings.instance_id and item.url
        ][:32]
        calls = [
            _peer_json(
                client,
                item.url,
                "/api/telemetry/live?scope_type=instance",
                token=ctx.settings.sync_token,
                instance_id=ctx.settings.instance_id,
            )
            for item in peers
        ]
        responses = await asyncio.gather(*calls, return_exceptions=True)
        for item, response in zip(peers, responses, strict=True):
            if isinstance(response, Exception):
                failures.append(
                    {"instance_id": item.instance_id, "state": "unavailable"}
                )
            else:
                results.extend(response.get("samples") or [])
    return {"samples": results, "failures": failures}


@router.post("/telemetry/fleet/query")
async def fleet_query(request: Request, body: QueryBody) -> dict:
    if _instance_caller(request):
        raise HTTPException(status_code=403, detail="User authorization required")
    body.scope_type = "instance"
    body.scope_ids = []
    body.provider_ids = []
    body.card_ids = []
    local_query = _make_query(request, body, force_instance=True)
    combined = _service(request).storage.query(local_query)
    for series in combined.get("series") or []:
        series.setdefault("bucket_seconds", combined["bucket_seconds"])
    failures = []
    ctx = request.app.state.ctx
    client = ctx.services.get("fleet_http_client")
    registry = ctx.services.get("fleet_registry")
    selected = set(body.instance_ids)
    if client and registry:
        peers = [
            item
            for item in registry.list_instances()
            if item.instance_id != ctx.settings.instance_id
            and item.url
            and (not selected or item.instance_id in selected)
        ][:32]
        remote_body = body.model_dump(mode="json")
        remote_body["instance_ids"] = []
        remote_body["start"] = local_query.start.isoformat()
        remote_body["end"] = local_query.end.isoformat()
        responses = await asyncio.gather(
            *[
                _peer_json(
                    client,
                    item.url,
                    "/api/telemetry/query",
                    token=ctx.settings.sync_token,
                    instance_id=ctx.settings.instance_id,
                    body=remote_body,
                )
                for item in peers
            ],
            return_exceptions=True,
        )
        for item, response in zip(peers, responses, strict=True):
            if isinstance(response, Exception):
                failures.append(
                    {
                        "instance_id": item.instance_id,
                        "state": "unavailable",
                        "reason": "peer_failure",
                        "start": local_query.start.isoformat(),
                        "end": local_query.end.isoformat(),
                    }
                )
            else:
                peer_bucket = (
                    response.get("bucket_seconds") or local_query.bucket_seconds
                )
                for series in response.get("series") or []:
                    series.setdefault("bucket_seconds", peer_bucket)
                    combined["series"].append(series)
    bucket_seconds_values = sorted(
        {
            int(series.get("bucket_seconds") or combined["bucket_seconds"])
            for series in combined["series"]
        }
        or {combined["bucket_seconds"]}
    )
    combined["bucket_seconds_values"] = bucket_seconds_values
    combined["mixed_bucket_seconds"] = len(bucket_seconds_values) > 1
    combined["failures"] = failures
    return combined


@ui_router.get("/reports", response_class=HTMLResponse)
def reports_page(request: Request) -> HTMLResponse:
    from pa.modules.ui_shell import render_page

    page = request.app.state.ctx.require_service("pages").get_by_path("/reports")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


def _page_context(request: Request) -> dict[str, Any]:
    settings = request.app.state.ctx.settings
    return {
        "telemetry_default_range": settings.telemetry_default_report_range,
        "telemetry_ui_refresh_seconds": settings.telemetry_ui_refresh_seconds,
        "telemetry_dimensions": _service(request).storage.dimensions(
            visible_principal_id=None if _admin(request) else _principal(request)
        ),
    }


class TelemetryModule(Module):
    @property
    def name(self) -> str:
        return "telemetry"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Bounded instance and PA-owned agent-session resource telemetry"

    def on_load(self, ctx: AppContext) -> None:
        storage = TelemetryStorage(Path(ctx.settings.telemetry_database_path))
        ctx.register_service("telemetry_storage", storage)
        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="reports",
                path="/reports",
                label="Reports",
                icon="fleet",
                template="pages/reports.html",
                nav_order=19,
                context_builder=_page_context,
            )
        )

    async def on_startup(self, app, ctx: AppContext) -> None:
        service = TelemetryService(
            ctx.settings,
            storage=ctx.require_service("telemetry_storage"),
            agent_manager=ctx.services.get("instance_agent"),
        )
        ctx.register_service("telemetry", service)
        await service.start()

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        service = ctx.services.get("telemetry")
        if service:
            await service.stop(close=True)

    def api_routers(self):
        return [("/api", router, ["telemetry"])]

    def ui_routers(self):
        return [ui_router]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        from pa.mcp.local_api import request_local_pa

        @mcp.tool()
        def telemetry_live(
            scope_type: str | None = None, scope_id: str | None = None
        ) -> dict:
            """Read fresh normalized instance or PA-owned session telemetry."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/telemetry/live",
                params={"scope_type": scope_type, "scope_id": scope_id},
            )

        @mcp.tool()
        def telemetry_health() -> dict:
            """Inspect collection, backpressure, failure, and storage health."""
            return request_local_pa(ctx.settings, "GET", "/api/telemetry/health")

        @mcp.tool()
        def telemetry_query(
            range: str = "1h",
            scope_type: str | None = None,
            scope_ids: list[str] | None = None,
            metrics: list[str] | None = None,
        ) -> dict:
            """Query a bounded historical series with server-side aggregation."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/telemetry/query",
                json={
                    "range": range,
                    "scope_type": scope_type,
                    "scope_ids": scope_ids or [],
                    "metrics": metrics or [],
                },
            )

        @mcp.tool()
        def telemetry_storage_status() -> dict:
            """Read telemetry database size, interval, drops, and prune status."""
            return request_local_pa(ctx.settings, "GET", "/api/telemetry/storage")

        @mcp.tool()
        def telemetry_configure(config: dict[str, Any]) -> dict:
            """Validate and persist collection and retention configuration."""
            return request_local_pa(
                ctx.settings, "PATCH", "/api/telemetry/config", json=config
            )

        @mcp.tool()
        def telemetry_maintenance(action: str = "prune") -> dict:
            """Safely prune or compact the independent telemetry database."""
            return request_local_pa(
                ctx.settings,
                "POST",
                "/api/telemetry/maintenance",
                json={"action": action},
                timeout=300,
            )

        @mcp.tool()
        def telemetry_export(
            range: str = "15m",
            scope_type: str | None = None,
            scope_id: str | None = None,
        ) -> dict:
            """Export a bounded, redacted diagnostic telemetry slice."""
            return request_local_pa(
                ctx.settings,
                "GET",
                "/api/telemetry/export",
                params={
                    "range": range,
                    "scope_type": scope_type,
                    "scope_id": scope_id,
                },
            )
