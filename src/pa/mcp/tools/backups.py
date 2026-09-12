"""backups tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Any
from pa.core.context import AppContext


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def backup_status() -> dict[str, Any]:
        """View this instance's backup schedule, health, storage, and history."""
        return request_local_pa(ctx.settings, "GET", "/api/backups/status")

    @mcp.tool()
    def backup_list(verify: bool = False) -> list[dict[str, Any]]:
        """List retained local metadata backups and verification state."""
        return request_local_pa(
            ctx.settings, "GET", "/api/backups", params={"verify": verify}
        )

    @mcp.tool()
    def backup_run(idempotency_key: str) -> dict[str, Any]:
        """Trigger one idempotent online metadata backup."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/backups",
            json={"idempotency_key": idempotency_key},
        )

    @mcp.tool()
    def backup_inspect(backup_id: str) -> dict[str, Any]:
        """Inspect and verify a backup manifest before restore."""
        return request_local_pa(ctx.settings, "GET", f"/api/backups/{backup_id}")

    @mcp.tool()
    def backup_verify(backup_id: str) -> dict[str, Any]:
        """Re-run archive, checksum, schema, and SQLite integrity verification."""
        return request_local_pa(
            ctx.settings, "POST", f"/api/backups/{backup_id}/verify"
        )

    @mcp.tool()
    def backup_delete(backup_id: str) -> dict[str, bool]:
        """Delete an explicit verified backup without deleting the last good copy."""
        request_local_pa(ctx.settings, "DELETE", f"/api/backups/{backup_id}")
        return {"deleted": True}

    @mcp.tool()
    def backup_export(backup_id: str) -> dict[str, Any]:
        """Authorize export and return a verified archive checksum and download URL."""
        return request_local_pa(
            ctx.settings, "GET", f"/api/backups/{backup_id}/export-info"
        )

    @mcp.tool()
    def backup_update_config(config: dict[str, Any]) -> dict[str, Any]:
        """Validate and persist backup schedule, destination, and retention policy."""
        return request_local_pa(
            ctx.settings, "PATCH", "/api/backups/config", json=config
        )

    @mcp.tool()
    def backup_restore_initiate(
        backup_id: str, confirm_instance_id: str
    ) -> dict[str, Any]:
        """Validate a backup and create a guarded offline restore request."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/backups/restores",
            json={
                "backup_id": backup_id,
                "confirm_instance_id": confirm_instance_id,
            },
        )

    @mcp.tool()
    def backup_restore_status(restore_id: str) -> dict[str, Any]:
        """Monitor a guarded restore request."""
        return request_local_pa(
            ctx.settings, "GET", f"/api/backups/restores/{restore_id}"
        )
