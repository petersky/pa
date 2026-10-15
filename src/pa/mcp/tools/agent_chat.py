"""agent_chat tools: authenticated owner API proxies."""

from __future__ import annotations




def register_mcp(mcp, ctx) -> None:
    from pa.mcp.local_api import request_local_pa
    from pa.mcp.tools.execution_selection import (
        register_mcp as register_selection_mcp,
    )

    register_selection_mcp(mcp, ctx)

    @mcp.tool()
    def get_conversation_capabilities(session_id: str) -> dict:
        """Inspect negotiated native fork and steering safety for this conversation."""
        return request_local_pa(ctx.settings, "GET", f"/api/agent/sessions/{session_id}/conversation-capabilities", timeout_seconds=15.0)

    @mcp.tool()
    def branch_conversation(session_id: str, request_id: str, strategy: str) -> dict:
        """Create an explicitly requested independent conversation, never a second source workspace writer.

        Choose native only when supported (latest provider history, not an exact
        message checkpoint). Choose copy explicitly for bounded saved text.
        Reuse request_id and strategy after timeout; do not silently fall back.
        Creating a branch does not authorize executing new work.
        """
        return request_local_pa(ctx.settings, "POST", f"/api/agent/sessions/{session_id}/branch",
            json={"request_id": request_id, "strategy": strategy}, headers={"Idempotency-Key": request_id}, timeout_seconds=60.0)

    @mcp.tool()
    def get_conversation_issues(session_id: str) -> dict:
        """Read a compact running discussion list and durable work receipts."""
        return request_local_pa(ctx.settings, "GET", f"/api/agent/sessions/{session_id}/issues", timeout_seconds=15.0)

    @mcp.tool()
    def update_conversation_issues(session_id: str, revision: int, issues: list[dict]) -> dict:
        """Edit discussion notes without creating cards or executing work.

        Supply the current revision and stable issue IDs; retain started issues.
        Each issue has id, title, optional detail/group and deferred boolean.
        """
        return request_local_pa(ctx.settings, "PUT", f"/api/agent/sessions/{session_id}/issues",
                                json={"revision": revision, "issues": issues}, timeout_seconds=15.0)

    @mcp.tool()
    def start_conversation_issues(session_id: str, revision: int, issue_ids: list[str], request_id: str) -> dict:
        """Start selected, bounded work only after an explicit user fix/start request.

        Discussion, speculation and listing issues are not execution permission.
        Reuse the exact request_id/selection/revision after timeout or reload.
        PA creates/links cards and admits workers using existing permission and
        workspace fencing. Accepted/admitted is not task completion.
        """
        return request_local_pa(ctx.settings, "POST", f"/api/agent/sessions/{session_id}/issues/start",
            json={"revision": revision, "issue_ids": issue_ids, "request_id": request_id},
            headers={"Idempotency-Key": request_id}, timeout_seconds=15.0)

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
