"""notifications tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any
from pa.core.context import AppContext


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def list_notifications(
        realm: str | None = None,
        type: str | None = None,
        priority: str | None = None,
        unread: bool | None = None,
        outstanding: bool | None = None,
        resolved: bool | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict:
        """List authorized fleet notifications with filters and pagination."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/notifications",
            params={
                "realm": realm,
                "type": type,
                "priority": priority,
                "unread": unread,
                "outstanding": outstanding,
                "resolved": resolved,
                "limit": limit,
                "offset": offset,
            },
        )

    @mcp.tool()
    def get_notification(notification_id: str) -> dict | None:
        """View one authorized notification, routing metadata, and audit trail."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/notifications/{notification_id}",
            allow_not_found=True,
        )

    @mcp.tool()
    def acknowledge_notification(
        notification_id: str, idempotency_key: str
    ) -> dict:
        """Idempotently acknowledge a notification."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/notifications/{notification_id}/acknowledge",
            json={"idempotency_key": idempotency_key},
        )

    @mcp.tool()
    def resolve_notification(notification_id: str, idempotency_key: str) -> dict:
        """Idempotently resolve a notification when authorized."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/notifications/{notification_id}/resolve",
            json={"idempotency_key": idempotency_key},
        )

    @mcp.tool()
    def transfer_notification_continuation(
        notification_id: str, idempotency_key: str, expected_version: int,
        expected_session_id: str, expected_dispatch_id: str,
        successor_session_id: str, successor_dispatch_id: str, reason: str,
    ) -> dict:
        """Audit a one-hop MCP operator-input continuation repair; never answer it.

        Call on the owner with exact current version/origin and an authorized
        recoverable successor on the same card. Native ACP requests cannot move.
        A previously recorded response still requires explicit delivery retry.
        """
        return request_local_pa(
            ctx.settings, "POST",
            f"/api/notifications/{notification_id}/transfer-continuation",
            json={
                "idempotency_key": idempotency_key, "expected_version": expected_version,
                "expected_session_id": expected_session_id, "expected_dispatch_id": expected_dispatch_id,
                "successor_session_id": successor_session_id, "successor_dispatch_id": successor_dispatch_id,
                "reason": reason,
            },
        )

    @mcp.tool()
    def respond_notification(
        notification_id: str,
        idempotency_key: str,
        choice_id: str | None = None,
        choice_ids: list[str] | None = None,
        value: str | None = None,
        fields: dict[str, Any] | None = None,
        cancel: bool = False,
        retry: bool = False,
    ) -> dict:
        """Answer an interaction or retry delivery of its recorded response."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/notifications/{notification_id}/respond",
            json={
                "idempotency_key": idempotency_key,
                "choice_id": choice_id,
                "choice_ids": choice_ids,
                "value": value,
                "fields": fields,
                "cancel": cancel,
                "retry": retry,
            },
        )
