"""Stage strip: a card's progress as a short, fixed-vocabulary diagram.

The strip is ``Work -> [declared checkpoints] -> Done``. Work and the final node
always exist; the middle exists only when the card declares completion
milestones, so plain tasks, documents and graphics show just two nodes.

Every node is one of four states (``done``, ``active``, ``todo``, ``stuck``) with
an optional owner (``agent``, ``pa``, ``you``). Nothing here reads storage: it
only restates facts ``present_work_item`` has already derived, so the strip can
never disagree with the state label beside it.

Parallel work (child cards, or several sessions) collapses into the single Work
node as a ``parallel`` tally rather than adding nodes.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable

STAGE_STATES = ("done", "active", "todo", "stuck")
CHILD_STATUSES = ("done", "running", "stuck", "todo")

MILESTONE_LABELS = {
    "integrated": "Merged",
    "published": "Published",
    "active": "Active",
    "verified": "Verified",
}
FAILURE_CODES = {
    "delivery_failed", "reconciliation_failed", "blocked", "retry_required",
    "failed", "start_blocked", "session_failed",
}
# Pulse thresholds in seconds: how recently the running agent produced anything.
PULSE_LIVE_SECONDS = 60
PULSE_RECENT_SECONDS = 300


def parallel_tally(children: Iterable[str]) -> dict[str, int] | None:
    """Count child work by status; None when there is nothing parallel to show."""
    counts = {name: 0 for name in CHILD_STATUSES}
    for status in children:
        counts[status if status in counts else "todo"] += 1
    total = sum(counts.values())
    if not total:
        return None
    return {"total": total, **counts}


def pulse(*, active: bool, at: Any, now: datetime) -> dict[str, Any] | None:
    """Liveness of the running agent. Only meaningful while Work is active."""
    if not active:
        return None
    moment = _as_datetime(at)
    if moment is None:
        return {"level": "unknown", "at": None, "age_seconds": None}
    age = max(0, int((now - moment).total_seconds()))
    level = "live" if age <= PULSE_LIVE_SECONDS else "recent" if age <= PULSE_RECENT_SECONDS else "stale"
    return {"level": level, "at": moment.isoformat(), "age_seconds": age}


def build_stage_strip(
    *,
    requirement: dict[str, Any] | None,
    completion: dict[str, Any],
    lane: str,
    state_code: str,
    group: str,
    session_active: bool,
    dispatch_active: bool = False,
    has_execution: bool,
    activity_at: Any = None,
    children: Iterable[str] = (),
    now: datetime | None = None,
) -> dict[str, Any]:
    now = now or datetime.now(UTC)
    requirement = requirement or {}
    tally = parallel_tally(children)
    finished = lane == "done" or state_code == "completed"

    # --- Work ---------------------------------------------------------------
    # A dispatch that is queued/starting/running is work in motion even before any
    # session evidence exists (e.g. while the runtime is still being provisioned).
    running = bool(session_active or dispatch_active or (tally and tally["running"]))
    if running:
        work_state = "active"
    elif state_code in FAILURE_CODES or (tally and tally["stuck"]):
        work_state = "stuck"
    elif state_code == "not_started":
        work_state = "todo"
    elif tally:
        # Idle children with none running: partial progress is shown by the tally.
        work_state = "done" if tally["done"] == tally["total"] else "todo"
    elif has_execution or finished:
        work_state = "done"
    else:
        work_state = "todo"
    work: dict[str, Any] = {
        "id": "work",
        "label": "Work",
        "state": work_state,
        "owner": "you" if state_code == "not_started" and not running else "agent",
    }
    if tally:
        work["parallel"] = tally
    live = pulse(active=work_state == "active", at=activity_at, now=now)
    if live:
        work["pulse"] = live
    stages = [work]

    # --- Declared checkpoints -----------------------------------------------
    satisfied = set(completion.get("satisfied") or [])
    missing = list(completion.get("missing") or [])
    declared = list(requirement.get("milestones") or [])
    if requirement.get("mode") == "integration_only" and "integrated" not in declared:
        declared.insert(0, "integrated")
    first_missing = next((name for name in declared if name not in satisfied), None)
    independent = requirement.get("verification_policy") == "independent_agent_v1"
    for name in declared:
        done = name in satisfied or bool(finished and completion.get("accepted"))
        if done:
            node_state = "done"
        elif name == first_missing and work_state != "active" and group != "attention":
            node_state = "active"
        else:
            node_state = "todo"
        # The PR supervisor records merges; with independent verification PA's verifier
        # records every other checkpoint, so none of them is the working agent's to claim.
        owner = "pa" if name in {"integrated", "published"} or independent else "agent"
        node: dict[str, Any] = {"id": name, "label": MILESTONE_LABELS.get(name, name.replace("_", " ").capitalize()),
                                "state": node_state, "owner": owner}
        if node_state == "todo" and work_state == "active":
            node["waiting_on"] = "work"
        stages.append(node)

    # --- Final node ---------------------------------------------------------
    needs_you = requirement.get("mode") == "explicit_acceptance" and not completion.get("accepted")
    if needs_you:
        ready = all(item["state"] == "done" for item in stages[1:]) and work_state in {"done", "todo"} and not running
        # Only the final node is yours; it becomes the active step once everything before it is done.
        stages.append({"id": "accept", "label": "Accept", "state": "active" if ready and has_execution else "todo", "owner": "you"})
    else:
        done = finished or bool(requirement and completion.get("accepted") and not missing and has_execution and work_state == "done")
        stages.append({"id": "done", "label": "Done", "state": "done" if done else "todo", "owner": None})

    current = next((item["id"] for item in stages if item["state"] in {"active", "stuck"}), None)
    return {"stages": stages, "current": current, "pulse": live}


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
