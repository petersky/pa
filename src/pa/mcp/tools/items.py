"""items tools: authenticated owner API proxies."""

from __future__ import annotations

from pathlib import Path
from typing import Literal
from urllib.parse import quote
from pa.attachments import safe_filename
from pa.core.context import AppContext
from pa.domain.models import CardKind, CardLane, ItemKind, ItemStatus
from pa.domain.completion_records import CompletionRecordInput


def _attachment_key(value: str) -> dict:
    if not value.strip():
        raise ValueError("idempotency_key cannot be empty")
    return {"Idempotency-Key": value.strip()}


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
    def submit_completion_record(card_id: str, envelope: CompletionRecordInput, idempotency_key: str, realm: str = "default") -> dict:
        """Save a substantive result; does not accept, deploy, or change a lane.

        v1: title (200), summary (2000), 1–32 Markdown sections, up to 64 typed
        references, total 64 KiB. Publish larger evidence with create_card_attachment;
        reference its ID and SHA-256. Supersession appends and preserves history.
        Retry identical arguments and key; conflicting key reuse is rejected.
        PA derives session/dispatch provenance from this ordinary dispatch binding.
        """
        key = idempotency_key.strip()
        if not key:
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(ctx.settings, "POST", f"/api/cards/{quote(card_id, safe='')}/completion-records",
                                params={"realm": realm}, json=envelope.model_dump(mode="json"),
                                headers={"Idempotency-Key": key}, bound_result=True)

    @mcp.tool()
    def list_completion_records(card_id: str, realm: str = "default", limit: int = 20, offset: int = 0) -> dict:
        """List compact substantive results, newest first; follow next_offset.

        Limit 1–50; details and references are retrieved by get_completion_record.
        No records means no structured results, not verified acceptance.
        """
        return request_local_pa(ctx.settings, "GET", f"/api/cards/{quote(card_id, safe='')}/completion-records",
                                params={"realm": realm, "limit": limit, "offset": offset}, bound_result=True)

    @mcp.tool()
    def get_completion_record(card_id: str, record_id: str, realm: str = "default") -> dict:
        """Read an immutable substantive result, its references and PA provenance."""
        return request_local_pa(ctx.settings, "GET", f"/api/cards/{quote(card_id, safe='')}/completion-records/{quote(record_id, safe='')}",
                                params={"realm": realm}, bound_result=True)

    @mcp.tool()
    def record_card_acceptance(
        card_id: str, realm: str, expected_version: str,
        requirement_revision: str, subject_revision: str,
        milestones: list[str], references: list[str], idempotency_key: str,
    ) -> dict:
        """Record acceptance as this live ordinary dispatch/session for its exact card.

        Requires a declared eligible principal and independence from the repair
        origin. PA stamps actor identity; this tool does not change the card lane.
        Replay the same arguments and key to recover an existing receipt.
        """
        if not idempotency_key.strip():
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings, "PATCH", f"/api/cards/{quote(card_id, safe='')}",
            params={"realm": realm}, headers={"Idempotency-Key": idempotency_key},
            bound_completion=True,
            json={"expected_version": expected_version, "completion_acceptance": {
                "requirement_revision": requirement_revision,
                "subject_revision": subject_revision,
                "milestones": milestones, "references": references,
            }},
        )

    @mcp.tool()
    def list_card_summaries(realm: str | None = None, project_id: str | None = None,
                            q: str = "", limit: int = 30, offset: int = 0) -> dict:
        """Discover at most100 compact summaries; prefer this over full card lists.

        Follow next_offset for another page. Fetch a card or its immutable history
        only when needed; this read excludes bodies, transcripts and full evidence.
        """
        return request_local_pa(ctx.settings, "GET", "/api/cards/summaries",
            params={"realm": realm, "project_id": project_id, "q": q, "limit": limit, "offset": offset})

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
    def get_card(card_id: str, realm: str = "default") -> dict:
        """Read the current full description, attachments, updated_at and execution_version.

        Prefer list_card_summaries for discovery and get_card_materials for a compact
        manifest. Use updated_at as expected_version when editing; history retains
        prior descriptions and provenance. Reading or editing does not start work.
        """
        return request_local_pa(ctx.settings, "GET", f"/api/cards/{quote(card_id, safe='')}", params={"realm": realm})

    @mcp.tool()
    def get_card_materials(card_id: str, realm: str = "default") -> dict:
        """Read compact supporting-material links, hashes, versions and exact limits.

        No description or artifact bytes. Fetch the current card/history or the
        authorized attachment URL only when needed; dispatch retains its admitted
        material snapshot. Changed material does not authorize expanding active work.
        """
        return request_local_pa(ctx.settings, "GET", f"/api/cards/{quote(card_id, safe='')}/materials", params={"realm": realm})

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
        completion_requirement: dict | None = None,
        completion_acceptance: dict | None = None,
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
                "completion_requirement": completion_requirement,
                "completion_acceptance": completion_acceptance,
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
            headers={"Idempotency-Key": key, "X-PA-Completion-Producer": "automation"},
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
        idempotency_key: str | None = None,
    ) -> dict:
        """Attach a durable fleet blob: at most 64 files/card, 25 MiB/file, 100 MiB/card.

        Supply a stable key for recoverable retries. Overflow: link a separate
        specification card or remove unused references. Limits reject, never truncate.
        Adding material does not authorize execution or expand active work.
        """
        if idempotency_key is not None and not idempotency_key.strip():
            raise ValueError("idempotency_key cannot be empty")
        path = Path(source_path).expanduser().resolve()
        if not path.is_file():
            raise ValueError("source_path must be an existing regular file")
        with path.open("rb") as source:
            return request_local_pa(
                ctx.settings,
                "POST",
                f"/api/cards/{card_id}/attachments",
                params={"realm": realm},
                headers={"Idempotency-Key": idempotency_key.strip()} if idempotency_key else None,
                files={
                    "file": (
                        safe_filename(filename or path.name),
                        source,
                        media_type,
                    )
                },
            )

    @mcp.tool()
    def create_card_resource_link(card_id: str, name: str, resource_url: str,
                                  idempotency_key: str, description: str = "", realm: str = "default") -> dict:
        """Attach an http(s) resource URL; no target fetching or execution authorization.

        Uses the same durable manifest and 64-attachment quota as files. Name up
        to 200 characters, description 4000, URL 4096. Use a stable retry key.
        """
        return request_local_pa(ctx.settings, "POST", f"/api/cards/{quote(card_id, safe='')}/attachment-links",
            params={"realm": realm}, headers=_attachment_key(idempotency_key),
            json={"name": name, "description": description, "resource_url": resource_url})

    @mcp.tool()
    def update_card_attachment(card_id: str, attachment_id: str, expected_revision: int,
                               idempotency_key: str, name: str | None = None,
                               description: str | None = None, resource_url: str | None = None,
                               expected_version: str | None = None, realm: str = "default") -> dict:
        """Edit metadata/URL only. Echo revision and optional card updated_at from materials.

        Name up to 200, description 4000, http(s) URL 4096 characters. Omitted
        fields stay unchanged. File content editing and replacement are separate.
        Saves an immutable revision with provenance, without starting execution.
        """
        return request_local_pa(ctx.settings, "PATCH", f"/api/cards/{quote(card_id, safe='')}/attachments/{quote(attachment_id, safe='')}",
            params={"realm": realm}, headers=_attachment_key(idempotency_key),
            json={k: v for k, v in {"expected_revision": expected_revision, "expected_version": expected_version,
                  "name": name, "description": description, "resource_url": resource_url}.items() if v is not None})

    @mcp.tool()
    def get_card_attachment_content(card_id: str, attachment_id: str, realm: str = "default") -> dict:
        """Read editable UTF-8 source up to 1 MiB and its CAS versions; discovery stays compact.

        Unsupported formats return an explicit replacement/download path.
        """
        return request_local_pa(ctx.settings, "GET", f"/api/cards/{quote(card_id, safe='')}/attachments/{quote(attachment_id, safe='')}/content", params={"realm": realm})

    @mcp.tool()
    def edit_card_attachment_content(card_id: str, attachment_id: str, content: str,
                                     expected_revision: int, idempotency_key: str,
                                     expected_version: str | None = None, realm: str = "default") -> dict:
        """Save UTF-8 text/HTML/JSON source up to 1 MiB as a revision; JSON must validate.

        Echo the versions from get_card_attachment_content. Use replacement for
        unsupported formats. Original revisions and completion evidence survive.
        """
        return request_local_pa(ctx.settings, "PUT", f"/api/cards/{quote(card_id, safe='')}/attachments/{quote(attachment_id, safe='')}/content",
            params={"realm": realm}, headers=_attachment_key(idempotency_key),
            json={"content": content, "expected_revision": expected_revision, "expected_version": expected_version})

    @mcp.tool()
    def replace_card_attachment(card_id: str, attachment_id: str, source_path: str,
                                expected_revision: int, idempotency_key: str,
                                filename: str | None = None, media_type: str = "application/octet-stream",
                                expected_version: str | None = None, realm: str = "default") -> dict:
        """Replace file bytes (25 MiB/file, 100 MiB current card total), keeping the ID.

        Metadata is preserved; old revisions/evidence keep original bytes. Echo
        revision and optional card updated_at from materials; use a stable key.
        """
        path = Path(source_path).expanduser().resolve()
        if not path.is_file():
            raise ValueError("source_path must be an existing regular file")
        with path.open("rb") as source:
            return request_local_pa(ctx.settings, "PUT", f"/api/cards/{quote(card_id, safe='')}/attachments/{quote(attachment_id, safe='')}/replace",
                params={"realm": realm, "expected_revision": expected_revision, "expected_version": expected_version},
                headers=_attachment_key(idempotency_key), files={"file": (safe_filename(filename or path.name), source, media_type)})

    @mcp.tool()
    def remove_card_attachment(
        card_id: str, attachment_id: str, realm: str = "default",
        expected_version: str | None = None, idempotency_key: str | None = None,
    ) -> dict:
        """Remove a current reference; completion-record evidence remains accessible.

        Echo updated_at as expected_version and use a stable idempotency_key for
        conflict protection and safe retries. Known description/evidence links block
        removal unless the original is retained in a completion record. The result
        states whether evidence is retained; other links cease to work on removal.
        """
        if idempotency_key is not None and not idempotency_key.strip():
            raise ValueError("idempotency_key cannot be empty")
        return request_local_pa(
            ctx.settings,
            "DELETE",
            f"/api/cards/{card_id}/attachments/{attachment_id}",
            params={"realm": realm, "expected_version": expected_version},
            headers={"Idempotency-Key": idempotency_key.strip()} if idempotency_key else None,
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
