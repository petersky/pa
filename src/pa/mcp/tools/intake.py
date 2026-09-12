"""intake tools: authenticated owner API proxies."""

from __future__ import annotations

from uuid import uuid4
from pa.core.context import AppContext
from pa.intake.models import Channel


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def intake_capability() -> dict:
        """Report configured canonical channel capabilities without secrets."""
        return request_local_pa(ctx.settings, "GET", "/api/intake/capabilities")

    @mcp.tool()
    def list_intake(
        realm: str | None = None,
        channel: Channel | None = None,
        correlation_id: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """List bounded canonical intake envelopes."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/intake",
            params={
                "realm": realm,
                "channel": channel.value if channel else None,
                "correlation_id": correlation_id,
                "limit": limit,
            },
        )

    @mcp.tool()
    def get_intake(envelope_id: str) -> dict:
        """Get one canonical intake envelope and delivery history."""
        return request_local_pa(ctx.settings, "GET", f"/api/intake/{envelope_id}")

    @mcp.tool()
    def create_intake_link(
        channel: Channel,
        realm_id: str = "default",
        expires_in_seconds: int = 600,
    ) -> dict:
        """Create a short-lived one-time channel identity link code."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/intake/links",
            json={
                "channel": channel.value,
                "realm_id": realm_id,
                "expires_in_seconds": expires_in_seconds,
            },
            idempotency_key=f"intake-link:{uuid4()}",
        )
