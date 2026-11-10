"""One-time repair of dispatch records stranded before liveness and completion fixes.

Finds non-terminal dispatch records that nothing will ever move again and closes
them through the dispatch ledger's atomic ``compare_and_mutate`` (the primitive the
audited terminal-repair endpoint uses), recording the same
``lifecycle_inconsistencies`` audit entries. No outcome is inferred: a closed
record never claims the dispatch or card succeeded. Dry-run by default; applying
needs the PA server stopped (the ledger has a single writer).

Candidates:

* legacy ``acknowledged`` records, normalized to ``completed``;
* work whose card is already Done;
* work whose linked session is closed or permanently blocked;
* remote ``running`` work with no progress or heartbeat for ``stale_days``.

Governed (goal) dispatches hold a governance reservation that only the running
server can release, so they are reported for the API repair path instead.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from pa.execution.dispatch import (
    CAPACITY_RESERVATION_STATES,
    TERMINAL_DISPATCH_STATES,
    DispatchCompareConflict,
    DispatchEvent,
    DispatchRecord,
    DispatchStore,
)
from pa.execution.liveness import last_activity

DEAD_SESSION_STATUSES = frozenset(
    {"closed", "recovery_blocked", "configuration_failed", "provisioning_failed"}
)
ACTOR = "cli:dispatch-repair-stuck"


def _has_completion_evidence(record: DispatchRecord) -> bool:
    return bool(
        record.completion_payload is not None
        or record.completion_envelope is not None
        or record.completion_received_at is not None
    )


def _classify(
    record: DispatchRecord, *, lanes: dict[str, str], session_status, instance_id: str,
    stale_before: datetime,
) -> tuple[str, str] | None:
    """(action, reason) for a record that needs repair, or None."""
    if record.state == "acknowledged" or (
        record.acknowledged_at is not None and record.state not in TERMINAL_DISPATCH_STATES
    ):
        return "normalize_acknowledged", "Legacy acknowledged record; completion was acknowledged."
    if record.state in TERMINAL_DISPATCH_STATES:
        return None
    if record.goal_provenance is not None:
        return "needs_operator", "Governed dispatch: repair it through the running server's API."
    if record.card_id and lanes.get(record.card_id) == "done":
        return "retire_card_done", "The card is already Done; the dispatch can never finish."
    status = session_status(record.session_id) if record.session_id else None
    if status in DEAD_SESSION_STATUSES:
        return "retire_dead_session", f"The linked session is {status}."
    if (
        record.state == "running"
        and record.target_instance_id != instance_id
        and last_activity(record) < stale_before
    ):
        return "fail_unreachable", (
            "Remote running dispatch with no progress since "
            f"{last_activity(record).date().isoformat()}."
        )
    return None


def _release_capacity(record: DispatchRecord, now: datetime, reason: str) -> None:
    if (
        record.state in CAPACITY_RESERVATION_STATES
        and record.capacity_reserved_at
        and not record.capacity_released_at
    ):
        record.capacity_released_at = now
        record.capacity_release_reason = reason


def _mutator(action: str, reason: str, previous_state: str):
    key = f"repair-stuck:{action}"

    def mutate(current: DispatchRecord) -> bool:
        if current.control_operations.get(key):
            return False
        now = datetime.now(UTC)
        _release_capacity(current, now, action)
        current.control_operations[key] = "repair_terminal"
        if action == "normalize_acknowledged":
            current.completion_delivery_class = "acknowledged"
            current.completion_next_retry_at = None
            current.last_error = None
            current.lifecycle_inconsistencies.append({
                "kind": "legacy_terminal_record_repaired", "previous_state": current.state,
                "normalized_state": "completed", "observed_at": now.isoformat(),
                "idempotency_key": key, "actor": ACTOR,
            })
            current.state = "completed"
            message = "Acknowledged legacy completion normalized to terminal state."
        else:
            evidence = _has_completion_evidence(current)
            # Without preserved completion evidence the work was simply abandoned;
            # with it, keep the record failed so the evidence is never discarded.
            if action == "retire_card_done" and not evidence:
                new_state, code = "cancelled", "legacy_abandoned_dispatch_retired"
            elif action == "fail_unreachable":
                new_state, code = "failed", "remote_dispatch_unreachable"
            else:
                new_state = "failed"
                code = (
                    "stale_closed_session_recovered" if action == "retire_dead_session"
                    else "stale_card_done_dispatch_retired"
                )
            current.recoverable = action == "fail_unreachable"
            current.error_code = code
            current.last_error = (
                f"Stranded dispatch closed by repair ({reason}); no dispatch or card "
                "outcome was inferred."
            )
            current.completion_next_retry_at = None
            if current.reconciliation_state in {"pending", "prompted", "blocked"}:
                current.reconciliation_state = "failed"
                current.reconciliation_reason = (
                    "Reconciliation abandoned by dispatch repair: " + reason
                )[:1000]
                current.reconciliation_recoverable = False
                current.reconciliation_next_retry_at = None
                current.reconciliation_updated_at = now
            current.lifecycle_inconsistencies.append({
                "kind": "stranded_dispatch_repaired", "action": action,
                "previous_state": current.state, "normalized_state": new_state,
                "observed_at": now.isoformat(), "idempotency_key": key, "reason": reason,
                "actor": ACTOR, "outcome_inferred": False,
                "preserved_evidence": {"completion": evidence,
                                       "card_reconciliation": current.reconciliation_state},
            })
            current.state = new_state
            message = "Stranded dispatch closed without outcome inference."
        current.lifecycle_inconsistencies = current.lifecycle_inconsistencies[-50:]
        current.events.append(DispatchEvent(
            seq=current.events[-1].seq + 1 if current.events else 1,
            state=current.state, message=message,
            detail={"previous_state": previous_state, "repair": True, "action": action,
                    "outcome_inferred": False},
        ))
        return True

    return mutate


def repair_stuck_dispatches(
    ledger: DispatchStore,
    store: Any,
    *,
    instance_id: str,
    apply: bool = False,
    stale_days: int = 30,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Plan (and with ``apply`` perform) the repair; returns a structured report."""
    now = now or datetime.now(UTC)
    lanes = store.list_card_lanes()
    sessions: dict[str, str | None] = {}

    def session_status(session_id: str | None) -> str | None:
        if session_id not in sessions:
            session = store.get_session(session_id) if session_id else None
            sessions[session_id] = getattr(session, "status", None)
        return sessions[session_id]

    stale_before = now - timedelta(days=stale_days)
    actions: list[dict[str, Any]] = []
    for record in sorted(ledger.list(limit=1_000_000), key=lambda item: item.dispatch_id):
        verdict = _classify(
            record, lanes=lanes, session_status=session_status,
            instance_id=instance_id, stale_before=stale_before,
        )
        if verdict is None:
            continue
        action, reason = verdict
        entry = {
            "dispatch_id": record.dispatch_id, "card_id": record.card_id,
            "state": record.state, "target": record.target_instance_id,
            "action": action, "reason": reason, "applied": False,
        }
        actions.append(entry)
        if not apply or action == "needs_operator":
            continue
        try:
            ledger.compare_and_mutate(record, _mutator(action, reason, record.state))
            entry["applied"] = True
        except DispatchCompareConflict as exc:
            entry.update(action="skip", reason="changed while repairing: " + ", ".join(exc.changed_fields))
    counts: dict[str, int] = {}
    for entry in actions:
        counts[entry["action"]] = counts.get(entry["action"], 0) + 1
    return {"applied": apply, "stale_days": stale_days, "counts": counts, "actions": actions}
