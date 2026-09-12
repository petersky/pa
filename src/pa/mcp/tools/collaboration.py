"""collaboration tools: authenticated owner API proxies."""

from __future__ import annotations




def register_mcp(mcp, ctx) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def get_collaboration_mode_state(session_id: str) -> dict:
        """Inspect supported/current collaboration mode and pending policy request."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/agent/sessions/{session_id}/collaboration",
        )

    @mcp.tool()
    def request_collaboration_mode(
        requested_mode: str,
        purpose: str,
        intended_next_action: str,
        session_id: str,
        dispatch_id: str | None,
        card_id: str | None,
        authority_instance_id: str | None,
        authority_version: str | None,
        idempotency_key: str,
    ) -> dict:
        """Ask PA to evaluate and durably apply a collaboration-mode transition."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/agent/sessions/{session_id}/collaboration/requests",
            json={
                "requested_mode": requested_mode,
                "purpose": purpose,
                "intended_next_action": intended_next_action,
                "session_id": session_id,
                "dispatch_id": dispatch_id,
                "card_id": card_id,
                "authority_instance_id": authority_instance_id,
                "authority_version": authority_version,
                "idempotency_key": idempotency_key,
                "actor": "agent",
            },
        )

    @mcp.tool()
    def list_session_commands(session_id: str) -> dict:
        """List the normalized provider and PA slash-command catalog."""
        return request_local_pa(
            ctx.settings, "GET", f"/api/agent/sessions/{session_id}/commands"
        )

    @mcp.tool()
    def execute_agent_session_command(
        session_id: str,
        name: str,
        idempotency_key: str,
        arguments: str | None = None,
        catalog_generation: int | None = None,
        dispatch_id: str | None = None,
        card_id: str | None = None,
        authority_version: str | None = None,
        authority_instance_id: str | None = None,
    ) -> dict:
        """Execute a recognized command through PA; failures are never prompt fallbacks."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/agent/sessions/{session_id}/commands/execute",
            json={
                "name": name,
                "arguments": arguments,
                "catalog_generation": catalog_generation,
                "dispatch_id": dispatch_id,
                "card_id": card_id,
                "authority_instance_id": authority_instance_id,
                "authority_version": authority_version,
                "idempotency_key": idempotency_key,
            },
        )
