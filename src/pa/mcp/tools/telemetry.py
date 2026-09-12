"""telemetry tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any
from pa.core.context import AppContext


def register_mcp(mcp, ctx: AppContext) -> None:
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
