"""One-time repair of sessions stranded by the old recovery rules.

Re-evaluates every recovery-blocked or exhausted session (and zombie sessions on
finished cards) under the current rules. Dry-run by default; ``apply`` mutates
through the audited paths only: ``close_session`` for closing and the workspace
manager's reclaim (rescue ref first) for leases. Never touches a live runtime,
so applying requires the server to be stopped.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pa.acp.errors import classify_acp_failure, deterministic_recovery_block
from pa.repository.workspace import workspace_blocks_admission

ZOMBIE_STATUSES = frozenset({"quiesced", "disconnected"})
# Blocks that clear once nothing else holds the card's worktree.
LEASE_CODES = frozenset({"workspace_lease_conflict"})
NEEDS_OPERATOR = {
    "workspace_binding_mismatch": "The recorded base commit differs from the leased worktree; restore that exact workspace or start a new linked attempt.",
    "dispatch_binding_mismatch": "The dispatch this session belonged to is terminal; close the session or re-dispatch the card.",
    "empty_index_checkout": "The retained checkout has an empty index; repair or preserve it explicitly.",
    "interrupted_checkout_retained": "An interrupted checkout holds uncommitted changes; repair it explicitly.",
}


def _effective_code(session: Any) -> tuple[str, str]:
    """Classify the stored failure with today's rules, not the code it was saved with."""
    recovery = dict(session.recovery_json or {})
    stored = str(recovery.get("code") or "recovery_blocked")
    error = str(recovery.get("last_error") or "")
    if stored == "workspace_binding_mismatch" or not error:
        return stored, error
    deterministic = deterministic_recovery_block(error)
    if deterministic:
        return deterministic["code"], error
    return classify_acp_failure(RuntimeError(error))["code"], error


def repair_recovery(
    settings: Any,
    store: Any,
    workspace_manager: Any,
    *,
    apply: bool = False,
    stale_days: int = 7,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Plan (and with ``apply`` perform) the repair; returns a structured report."""
    now = now or datetime.now(UTC)
    # Offline there is no runtime; a blocked session never had one to lose.
    workspace_manager.live_runtime = lambda _session_id: False
    lanes = store.list_card_lanes()
    sessions = store.list_sessions(
        exclude_statuses=("closed",), include_archived=False, include_metrics=False
    )
    actions: list[dict[str, Any]] = []
    closed_ids: list[str] = []
    reclaimed_lease_ids: set[str] = set()

    def add(session: Any, action: str, **detail: Any) -> dict[str, Any]:
        entry = {
            "session_id": session.id, "card_id": session.card_id,
            "status": session.status, "action": action, **detail,
        }
        actions.append(entry)
        return entry

    def close(session: Any, reason: str) -> None:
        entry = add(session, "close", reason=reason, applied=False)
        if not apply:
            return
        try:
            _, prior = store.close_session(session.id, reason=reason)
        except Exception as exc:  # restart continuation pending, lock timeout, ...
            entry.update(action="skip", reason=f"close refused: {exc}")
            return
        entry["applied"] = prior is not None
        closed_ids.append(session.id)

    stale_before = now - timedelta(days=stale_days)
    # Close first so a closed session's lease is not reclaimed as a "dead owner".
    remaining: list[Any] = []
    for session in sessions:
        if session.purpose == "chat" or session.archived_at:
            continue
        recovery = dict(session.recovery_json or {})
        blocked = session.status == "recovery_blocked" or bool(
            recovery.get("blocked") or recovery.get("exhausted")
        )
        card_done = bool(session.card_id) and lanes.get(session.card_id) == "done"
        zombie = session.status in ZOMBIE_STATUSES
        if store.pending_restart_continuation(session_id=session.id):
            if blocked or zombie:
                add(session, "skip", reason="a restart continuation is pending")
            continue
        if card_done and (blocked or zombie):
            close(session, "repair_card_done")
        elif zombie and not session.card_id and session.updated_at < stale_before:
            close(session, "repair_stale_unlinked")
        elif blocked:
            remaining.append(session)

    statuses = store.list_session_statuses()
    if apply:
        for session_id in closed_ids:
            workspace_manager.expire_session(session_id)
        if closed_ids:
            workspace_manager.reconcile_terminal_state()
        statuses = store.list_session_statuses()

    # Leases held by the blocked sessions: reclaim with a rescue ref.
    for session in remaining:
        for lease in workspace_manager.list(include_cleaned=False):
            if lease.session_id != session.id:
                continue
            item = workspace_manager._admission_item(lease, statuses)
            reason = item["reclaimable"]
            if not reason or not workspace_blocks_admission(item):
                continue
            entry = add(
                session, "reclaim_lease", lease_id=lease.id, reason=reason,
                worktree_path=lease.worktree_path, applied=False,
            )
            if apply:
                result = workspace_manager.reclaim_dead_owner_lease(
                    lease.id, requested_by="repair-recovery"
                )
                if result:
                    reclaimed_lease_ids.add(lease.id)
                    entry.update(
                        applied=True, rescue_ref=(result["rescue"] or {}).get("ref"),
                        rescue_error=(result["rescue"] or {}).get("error"),
                    )
                else:
                    entry.update(action="skip", reason="lease changed owner while repairing")
            else:
                reclaimed_lease_ids.add(lease.id)

    # Re-evaluate each block under the current rules.
    statuses = store.list_session_statuses()
    for session in remaining:
        code, error = _effective_code(session)
        stored = str((session.recovery_json or {}).get("code") or "")
        if code in NEEDS_OPERATOR:
            add(session, "needs_operator", code=code, remedy=NEEDS_OPERATOR[code])
            continue
        if code in LEASE_CODES:
            blockers = [
                w["session_id"]
                for w in workspace_manager.admission_workspaces(card_id=session.card_id)
                if w["session_id"] != session.id
                and w["id"] not in reclaimed_lease_ids
                and workspace_blocks_admission(w)
                and not w.get("reclaimable")
            ] if session.card_id else []
            if blockers:
                add(session, "needs_operator", code=code,
                    remedy="A live session still holds this card's worktree lease.",
                    held_by=blockers)
                continue
        elif stored == code or code == "auth_missing":
            # Not reclassified by the current rules: a genuine block or an
            # exhausted transient failure that needs a person to decide.
            add(session, "needs_operator", code=code,
                remedy=str((session.recovery_json or {}).get("remedy")
                           or "Retry after correcting the cause."))
            continue
        entry = add(session, "rearm", code=code, previous_code=stored, applied=False)
        if apply:
            current = store.get_session(session.id)
            if current is not None and current.status == "recovery_blocked":
                current.status = "recoverable_interrupted"
                current.recovery_json = {}
                current.updated_at = now
                store.save_session(current)
                entry["applied"] = True

    counts: dict[str, int] = {}
    for entry in actions:
        counts[entry["action"]] = counts.get(entry["action"], 0) + 1
    return {"applied": apply, "counts": counts, "actions": actions}
