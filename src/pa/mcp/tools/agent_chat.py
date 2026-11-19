"""agent_chat tools: authenticated owner API proxies."""

from __future__ import annotations

from typing import Annotated

from pydantic import Field

from pa.domain.conversation_issues import Issue, IssueEdit


def register_mcp(mcp, ctx) -> None:
    from pa.mcp.local_api import request_local_pa
    from pa.mcp.tools.execution_selection import (
        register_mcp as register_selection_mcp,
    )

    register_selection_mcp(mcp, ctx)

    @mcp.tool()
    def get_rendering_capabilities(session_id: str) -> dict:
        """Inspect versioned PA chat visualization syntax, runtime and ownership limits."""
        return request_local_pa(ctx.settings, "GET", f"/api/agent/sessions/{session_id}/rendering-capabilities")

    @mcp.tool()
    def get_conversation_capabilities(session_id: str) -> dict:
        """Inspect native fork, steering safety and versioned visualization renderer capabilities."""
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
    def update_conversation_issues(session_id: str, revision: int, issues: Annotated[list[Issue], Field(max_length=100)]) -> dict:
        """Edit discussion notes without creating cards or executing work.

        Supply the current revision and editable_issues from get; retain started issues.
        At most 100 issues; title 300, detail 16,000, group 80 characters.
        For overflow, create a card with the full body/attachments and set card_id.
        Linked cards cannot be grouped; Start selected uses the existing card.
        PA captures its execution_version and rejects stale material at start.
        Public request_id is read-only and ignored on input; other extras are rejected.
        """
        body = IssueEdit(revision=revision, issues=issues)
        return request_local_pa(ctx.settings, "PUT", f"/api/agent/sessions/{session_id}/issues",
                                json=body.model_dump(mode="json"), timeout_seconds=15.0)

    @mcp.tool()
    def start_conversation_issues(session_id: str, revision: int, issue_ids: list[str], request_id: str) -> dict:
        """Start work called for by user intent or standing authority.

        Honor discussion-only/create-only restrictions in conversational context;
        a mention alone does not start unrelated work. No fresh permission token
        is required for ordinary work, root causes or necessary related fixes.
        Reuse the exact request_id/selection/revision after timeout or reload.
        The receipt coordinates recovery and deduplication, not new authorization.
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
    def get_agent_session_workspace_binding(
        session_id: str, expected_authority_instance_id: str,
        expected_session_version: str, expected_native_id: str,
        expected_binding_hash: str, expected_lease_hash: str,
        history_limit: int = 16, history_offset: int = 0,
    ) -> dict:
        """Inspect bounded binding/history/lease metadata; restoration is unsupported.

        Empty expected_native_id explicitly asserts no saved native identity.
        All hashes/versions/fences are read preconditions, never mutation authority.
        """
        from urllib.parse import quote
        return request_local_pa(ctx.settings, 'GET',
            f'/api/agent/observability/v1/sessions/{quote(session_id, safe="")}/workspace-binding',
            params=dict(expected_authority_instance_id=expected_authority_instance_id,
                expected_session_version=expected_session_version,
                expected_native_id=expected_native_id,
                expected_binding_hash=expected_binding_hash,
                expected_lease_hash=expected_lease_hash,
                history_limit=history_limit, history_offset=history_offset),
            timeout_seconds=15.0, max_response_bytes=256 * 1024)

    @mcp.tool()
    def get_agent_session_recovery_custody(
        session_id: str, expected_authority_instance_id: str,
        expected_session_version: str, expected_native_id: str,
        expected_binding_hash: str, expected_lease_hash: str,
    ) -> dict:
        """Read bounded checkpoint identities/native registry; never restore or load.

        Missing, null and in-flight checkpoints remain distinct. Registry
        uniqueness is not exclusive native ownership or historical outcome proof.
        Empty expected_native_id explicitly asserts no saved native identity.
        """
        from urllib.parse import quote
        return request_local_pa(ctx.settings, 'GET',
            f'/api/agent/observability/v1/sessions/{quote(session_id, safe="")}/recovery-custody',
            params=dict(expected_authority_instance_id=expected_authority_instance_id,
                expected_session_version=expected_session_version, expected_native_id=expected_native_id,
                expected_binding_hash=expected_binding_hash, expected_lease_hash=expected_lease_hash),
            timeout_seconds=15.0, max_response_bytes=64 * 1024)

    @mcp.tool()
    def get_agent_session_wire_metadata(
        session_id: str, handoff_id: str, expected_native_id: str,
        expected_version: int, expected_attempt: int,
        paged: bool = False, cursor: str | None = None,
        selection: str = "all",
        assess_unparsed: bool = False,
    ) -> dict:
        """Read fixed-session metadata only; absent records never prove non-delivery."""
        from urllib.parse import quote
        return request_local_pa(ctx.settings, "GET",
            f"/api/agent/observability/v1/sessions/{quote(session_id, safe='')}/restart-handoffs/{quote(handoff_id, safe='')}/wire-metadata",
            params=dict(expected_native_id=expected_native_id,
                        expected_version=expected_version, expected_attempt=expected_attempt,
                        paged=paged, selection=selection, assess_unparsed=assess_unparsed,
                        **({"cursor": cursor} if cursor else {})),
            timeout_seconds=15.0, max_response_bytes=512 * 1024)

    @mcp.tool()
    def get_restart_canonical_evidence(
        session_id: str, handoff_id: str, expected_authority_instance_id: str,
        expected_version: int, expected_attempt: int, expected_session_version: str,
        expected_native_id: str, after_seq: int = -1, limit: int = 64,
        expected_snapshot: str | None = None,
    ) -> dict:
        """Read bounded canonical evidence; no audit or receipt/session mutation.

        Missing/cold/pruned evidence never establishes non-delivery. Exact prompt
        completion is separate from explicit receipt-attempt attribution.
        """
        from urllib.parse import quote
        return request_local_pa(ctx.settings, 'GET',
            f'/api/agent/observability/v1/sessions/{quote(session_id, safe="")}/restart-handoffs/{quote(handoff_id, safe="")}/canonical-evidence',
            params=dict(expected_authority_instance_id=expected_authority_instance_id,
                expected_version=expected_version, expected_attempt=expected_attempt,
                expected_session_version=expected_session_version, expected_native_id=expected_native_id,
                after_seq=after_seq, limit=limit,
                **({'expected_snapshot': expected_snapshot} if expected_snapshot is not None else {})),
            timeout_seconds=15.0, max_response_bytes=128 * 1024)

    @mcp.tool()
    def reconcile_agent_restart_handoff(
        session_id: str, handoff_id: str, expected_version: int,
    ) -> dict:
        """Audit exact completion evidence for an existing inactive restart receipt.

        This writes an audit event through the owner API. It never restarts,
        reopens, rearms or sends a continuation. Missing evidence leaves effect
        unknown. Read the original receipt first and retain its exact identity
        and phase version. After an unknown transport outcome, observe that same
        receipt; do not automatically repeat this audit or create a handoff.
        Server principal, instance-owner and version fences remain authoritative.
        """
        from urllib.parse import quote
        return request_local_pa(
            ctx.settings, "POST",
            f"/api/agent/sessions/{quote(session_id, safe='')}/restart-handoffs/{quote(handoff_id, safe='')}/reconcile",
            json={"expected_version": expected_version},
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
