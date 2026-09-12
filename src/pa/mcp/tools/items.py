"""items tools: authenticated owner API proxies."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import quote
from pa.attachments import safe_filename
from pa.core.context import AppContext
from pa.domain.models import CardKind, CardLane, ItemKind, ItemStatus


def register_mcp(mcp, ctx: AppContext) -> None:
    from pa.mcp.local_api import request_local_pa

    @mcp.tool()
    def list_items(
        kind: ItemKind | None = None, status: ItemStatus | None = None
    ) -> list[dict]:
        """List goals, tasks, projects, and concerns."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/items",
            params={"kind": kind, "status": status},
        )

    @mcp.tool()
    def list_cards(
        realm: str | None = None,
        lane: CardLane | None = None,
        kind: CardKind | None = None,
    ) -> list[dict]:
        """List canonical cards, optionally filtered by lane and kind."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/cards",
            params={"realm": realm, "lane": lane, "kind": kind},
        )

    @mcp.tool()
    def create_card(
        idempotency_key: str,
        title: str = "",
        kind: CardKind | None = None,
        body: str = "",
        lane: CardLane = CardLane.INBOX,
        realm: str = "default",
        parent_id: str | None = None,
        project_id: str | None = None,
        tags: list[str] | None = None,
        auto_enrich: bool = True,
        execution_preferences: dict | None = None,
    ) -> dict:
        """Create a canonical card. Use lane: inbox, active, waiting, or done."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/cards",
            json={
                "realm_id": realm,
                "title": title,
                "body": body,
                "lane": lane,
                "parent_id": parent_id,
                "project_id": project_id,
                "tags": tags or [],
                "auto_enrich": auto_enrich,
                **(
                    {"execution_preferences": execution_preferences}
                    if execution_preferences is not None
                    else {}
                ),
                **({"kind": kind} if kind is not None else {}),
            },
            headers={"Idempotency-Key": key},
        )

    @mcp.tool()
    def update_card(
        card_id: str,
        idempotency_key: str,
        title: str | None = None,
        body: str | None = None,
        lane: CardLane | None = None,
        parent_id: str | None = None,
        project_id: str | None = None,
        realm: str = "default",
        tags: list[str] | None = None,
        expected_version: str | None = None,
        field_intent: list[str] | None = None,
        execution_preferences: dict | None = None,
    ) -> dict | None:
        """Update a canonical card. Omitted fields remain unchanged."""
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        changes = {
            key: value
            for key, value in {
                "title": title,
                "body": body,
                "lane": lane,
                "parent_id": parent_id,
                "project_id": project_id,
                "tags": tags,
                "execution_preferences": execution_preferences,
            }.items()
            if value is not None
        }
        if expected_version is not None:
            changes["updated_at"] = expected_version
        if field_intent is not None:
            changes["field_intent"] = field_intent
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/cards/{card_id}",
            params={"realm": realm},
            json=changes,
            allow_not_found=True,
            headers={"Idempotency-Key": key},
        )

    @mcp.tool()
    def get_card_history(
        card_id: str,
        realm: str = "default",
        limit: int = 100,
        cursor: str | None = None,
    ) -> dict:
        """Inspect one stable cursor page of immutable card mutations."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/cards/{card_id}/history",
            params={
                "realm": realm,
                "limit": limit,
                "cursor": cursor,
            },
        )

    @mcp.tool()
    def repair_legacy_card_history(
        card_ids: list[str], realm: str = "default", diagnose_only: bool = False
    ) -> dict:
        """Diagnose reachability and re-anchor orphaned/projection-only cards."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/cards/repair-legacy-history",
            json={
                "card_ids": card_ids,
                "realm_id": realm,
                "diagnose_only": diagnose_only,
            },
        )

    @mcp.tool()
    def create_card_attachment(
        card_id: str,
        source_path: str,
        realm: str = "default",
        filename: str | None = None,
        media_type: str = "application/octet-stream",
    ) -> dict:
        """Attach a local file through PA's authenticated API; bytes become a durable fleet blob."""
        path = Path(source_path).expanduser().resolve()
        if not path.is_file():
            raise ValueError("source_path must be an existing regular file")
        with path.open("rb") as source:
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/cards/{card_id}/attachments",
                params={"realm": realm},
                files={
                    "file": (
                        safe_filename(filename or path.name),
                        source,
                        media_type,
                    )
                },
            )

    @mcp.tool()
    def remove_card_attachment(
        card_id: str, attachment_id: str, realm: str = "default"
    ) -> dict:
        """Remove an attachment reference through a durable realm event."""
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/cards/{card_id}/attachments/{attachment_id}",
            params={"realm": realm},
        )

    @mcp.tool()
    def update_card_preferred_instance(
        card_id: str,
        instance_id: str,
        idempotency_key: str,
        realm: str = "default",
    ) -> dict | None:
        """Set a card's preferred fleet instance and return its new authority version."""
        instance_id = instance_id.strip()
        key = idempotency_key.strip()
        if not instance_id:
            raise ValueError("instance_id cannot be empty")
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/cards/{card_id}",
            params={"realm": realm},
            json={"preferred_instance": instance_id},
            headers={"Idempotency-Key": key},
            allow_not_found=True,
        )

    @mcp.tool()
    def create_item(
        kind: ItemKind,
        title: str,
        body: str = "",
        status: ItemStatus = ItemStatus.OPEN,
        parent_id: str | None = None,
    ) -> dict:
        """Deprecated: create an item. Prefer create_card with canonical lane."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/items",
            json={
                "kind": kind,
                "title": title,
                "body": body,
                "status": status,
                "parent_id": parent_id,
            },
        )

    @mcp.tool()
    def update_item(
        item_id: str,
        title: str | None = None,
        body: str | None = None,
        status: ItemStatus | None = None,
        parent_id: str | None = None,
    ) -> dict | None:
        """Update an item's mutable fields."""
        return request_local_pa(
            ctx.settings,
            "PATCH",
            f"/api/items/{item_id}",
            json={
                key: value
                for key, value in {
                    "title": title,
                    "body": body,
                    "status": status,
                    "parent_id": parent_id,
                }.items()
                if value is not None
            },
            allow_not_found=True,
        )

    @mcp.tool()
    def get_item(item_id: str) -> dict | None:
        """Get a single item by ID."""
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/items/{item_id}",
            allow_not_found=True,
        )

    @mcp.tool()
    def list_knowledge(item_id: str | None = None, limit: int = 20) -> dict:
        """List curated durable memories and decisions."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/knowledge",
            params={"item_id": item_id, "limit": limit},
        )

    @mcp.tool()
    def promote_session_memory(
        session_id: str,
        summary: str,
        kind: str = "memory",
        scope: str = "realm",
        start_seq: int | None = None,
        end_seq: int | None = None,
    ) -> dict:
        """Explicitly promote one curated conclusion with transcript provenance."""
        return request_local_pa(
            ctx.settings,
            "POST",
            "/api/knowledge/promote",
            json={
                "session_id": session_id,
                "summary": summary,
                "kind": kind,
                "scope": scope,
                "start_seq": start_seq,
                "end_seq": end_seq,
            },
        )

    @mcp.tool()
    def audit_knowledge_capture() -> dict:
        """Report likely corrupt or unintended Memory without mutating it."""
        return request_local_pa(
            ctx.settings,
            "GET",
            "/api/knowledge/audit",
        )

    @mcp.tool()
    def regenerate_memory(entry_id: str) -> dict:
        """Supersede Memory from its canonical source transcript."""
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/knowledge/{entry_id}/regenerate",
            json={},
        )

    @mcp.tool()
    def recover_operation_outcome(
        idempotency_key: str, realm: str = "default", owner: Literal["canonical", "restart", "dispatch"] | None = None,
        operation: str | None = None, request_fingerprint: str | None = None,
    ) -> dict:
        """Explicitly admit owned receipt reconciliation without resending a mutation.

        Poll get_operation_outcome for passive observations of this stable job.
        """
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "POST",
            f"/api/operation-recovery/{quote(key, safe='')}",
            params={k: v for k, v in {"realm": realm, "owner": owner,
                    "operation": operation, "request_fingerprint": request_fingerprint}.items()
                    if v is not None},
        )

    @mcp.tool()
    def get_operation_outcome(
        idempotency_key: str, realm: str = "default", owner: Literal["canonical", "restart", "dispatch"] | None = None,
        operation: str | None = None, request_fingerprint: str | None = None,
    ) -> dict:
        """Passively observe an owner/realm receipt; pending is not non-commit.

        owner is canonical, restart, or dispatch. Legacy ambiguous keys conflict.
        operation/fingerprint, when supplied, must match the original request.
        """
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "GET",
            f"/api/operations/{quote(key, safe='')}",
            params={k: v for k, v in {"realm": realm, "owner": owner,
                    "operation": operation, "request_fingerprint": request_fingerprint}.items()
                    if v is not None},
        )
