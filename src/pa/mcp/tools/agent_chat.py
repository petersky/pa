"""agent_chat tools: authenticated owner API proxies."""

from __future__ import annotations




def register_mcp(mcp, ctx) -> None:
    from pa.mcp.local_api import request_local_pa
    from pa.mcp.tools.execution_selection import (
        register_mcp as register_selection_mcp,
    )

    register_selection_mcp(mcp, ctx)

    @mcp.tool()
    def list_agent_session_liveness(limit: int = 100) -> dict:
        """List normalized authoritative liveness for recent ACP sessions."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/agent/observability/v1/sessions",
            params={"limit": limit},
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def get_agent_session_liveness(session_id: str) -> dict | None:
        """Get one ACP session with turns, queue, progress, freshness, and recovery state."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/agent/observability/v1/sessions/{session_id}",
            # Unknown IDs retain the compatibility null. Dispatch-known IDs
            # are routed above and therefore return data or an explicit 5xx.
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def list_agent_session_turns(session_id: str) -> dict | None:
        """List independent prompt/turn lifecycles, including post-dispatch follow-ups."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/agent/observability/v1/sessions/{session_id}/turns",
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def request_agent_session_diagnostics(
        session_id: str, limit: int = 50
    ) -> dict | None:
        """Create a bounded privacy-safe diagnostic snapshot for an ACP session."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/agent/observability/v1/sessions/{session_id}/diagnostics",
            params={"limit": limit},
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def list_agent_session_cards(session_id: str) -> dict | None:
        """List every card associated with one local ACP session."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/agent/sessions/{session_id}/cards",
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def associate_agent_session_card(
        session_id: str,
        card_id: str,
        make_primary: bool = True,
    ) -> dict | None:
        """Associate a canonical card with a local ACP session without replacing older links."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/agent/sessions/{session_id}/cards/{card_id}",
            json={"make_primary": make_primary},
            allow_not_found=True,
            timeout_seconds=15.0,
        )

    @mcp.tool()
    def dissociate_agent_session_card(
        session_id: str,
        card_id: str,
    ) -> dict | None:
        """Remove one card association while preserving the session and its other cards."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/agent/sessions/{session_id}/cards/{card_id}",
            allow_not_found=True,
            timeout_seconds=15.0,
        )
