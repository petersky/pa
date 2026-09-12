"""execution_selection tools: authenticated owner API proxies."""

from __future__ import annotations

from pa.execution.selection import ExecutionPreferences


def register_mcp(mcp, ctx):
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def start_execution(
        execution_preferences: dict,
        idempotency_key: str,
        title: str | None = None,
        project_id: str | None = None,
    ) -> dict:
        """Start an owned standalone session using explicit selection and a stable label. Card work must use durable fleet dispatch."""
        if not idempotency_key.strip() or len(idempotency_key) > 200:
            raise ValueError(
                "A nonempty stable idempotency key of at most 200 characters is required"
            )
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/agent/sessions",
            json={
                "label": "execution-mcp:" + idempotency_key,
                "title": title,
                "project_id": project_id,
                "execution_preferences": ExecutionPreferences.model_validate(
                    execution_preferences
                ).model_dump(mode="json"),
            },
        )

    @mcp.tool()
    def start_linked_execution(
        session_id: str, execution_preferences: dict, idempotency_key: str
    ) -> dict:
        """Explicitly replace an idle standalone context with a linked, separately leased attempt. Dispatch-owned sources require a new terminal-successor dispatch."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/boundary",
            json={
                "execution_preferences": execution_preferences,
                "idempotency_key": idempotency_key,
            },
        )

    @mcp.tool()
    def execution_catalog(refresh: bool = False) -> dict:
        """Read cached, scoped model/native-option capability evidence; explicit refresh is bounded/coalesced."""
        return request_local_pa(
            ctx.settings,
            "POST" if refresh else "GET",
            "/api/execution/catalog/refresh" if refresh else "/api/execution/catalog",
        )

    @mcp.tool()
    def execution_connections(
        connection_id: str | None = None,
        profile: dict | None = None,
        expected_revision: int = 0,
        refresh: bool = False,
    ) -> dict:
        """List configured connections, explicitly refresh one, or configure an administrator-owned native profile. Credentials are references, never secret values."""
        from urllib.parse import quote

        if not connection_id:
            return request_local_pa(ctx.settings, "GET", "/api/execution/connections")
        path = "/api/execution/connections/" + quote(connection_id, safe="")
        if profile is not None:
            return request_local_pa(
                ctx.settings,
                "PUT",
                path,
                json={"profile": profile, "expected_revision": expected_revision},
            )
        if refresh:
            return request_local_pa(ctx.settings, "POST", path + "/refresh", json={})
        return request_local_pa(ctx.settings, "GET", "/api/execution/connections")

    @mcp.tool()
    def cancel_execution_settings(
        session_id: str, idempotency_key: str, expected_version: str
    ) -> dict:
        """Cancel this exact pending setting change; never claim rollback after partial native application."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/settings/cancel",
            json={
                "idempotency_key": idempotency_key,
                "expected_version": expected_version,
            },
        )

    @mcp.tool()
    def preview_execution(
        card_id: str | None = None,
        project_id: str | None = None,
        execution_preferences: dict | None = None,
        task_assessment: dict | None = None,
    ) -> dict:
        """Preview local selection without applying it. Fleet dispatch preview evaluates remote instances jointly."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/execution/preview",
            json={
                "card_id": card_id,
                "project_id": project_id,
                "execution_preferences": execution_preferences or {},
                "task_assessment": task_assessment,
            },
        )

    @mcp.tool()
    def get_execution_decision(
        decision_id: str, offset: int = 0, limit: int = 100
    ) -> dict:
        """Read an owned immutable execution-selection receipt; runtime confirmation remains separate."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/execution/decisions/{decision_id}?offset={offset}&limit={limit}",
        )

    @mcp.tool()
    def execution_policy(
        policy: dict | None = None, expected_revision: int | None = None
    ) -> dict:
        """Read policy, or explicitly edit its versioned rules/constraints (administrator and revision required)."""
        if policy is None:
            return request_local_pa(ctx.settings, "GET", "/api/execution/policy")
        return request_local_pa(
            ctx.settings,
            "PUT",
            "/api/execution/policy",
            json={"policy": policy, "expected_revision": expected_revision},
        )

    @mcp.tool()
    def request_execution_settings(
        session_id: str,
        execution_preferences: dict,
        expected_version: str,
        idempotency_key: str,
        defer: bool = False,
    ) -> dict:
        """Request supported native settings now or durably defer to a safe turn boundary. Does not change card defaults."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/execution/sessions/{session_id}/settings",
            json={
                "execution_preferences": execution_preferences,
                "expected_version": expected_version,
                "idempotency_key": idempotency_key,
                "defer": defer,
            },
        )
