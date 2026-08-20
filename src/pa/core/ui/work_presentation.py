"""Canonical, evidence-based presentation for card and agent work state.

This module intentionally contains no store or network access.  Callers provide
their freshest bounded snapshot (or cached snapshot) and every UI surface gets
the same lifecycle label, attention decision, reason, and contextual action.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from enum import Enum
from typing import Any

ACTIVE_DISPATCH_STATES = {
    "waiting_capacity",
    "blocked",
    "queued",
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
    "dispatching",
    "dispatched",
    "materialized",
    "running",
    "completion_pending",
}
STARTING_DISPATCH_STATES = {
    "waiting_capacity",
    "queued",
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
    "dispatching",
    "dispatched",
    "materialized",
}
TERMINAL_DISPATCH_STATES = {"completed", "acknowledged", "failed", "cancelled"}
ACTIVE_SESSION_STATES = {"busy", "working", "prompting", "running", "starting"}
QUIET_SESSION_STATES = {"connected", "idle", "completed_idle"}
STALE_PROGRESS_STATES = {"stale", "stalled", "disconnected"}
FAILED_DELIVERY_CLASSES = {
    "transport_exhausted",
    "permanent_failure",
    "semantic_conflict",
    "failed",
}
FAILED_RECONCILIATION_STATES = {"blocked", "conflict_requires_resolution", "failed"}
CURRENT_WATCH_STATES = {"active", "blocked"}

DISPATCH_LABELS = {
    "waiting_capacity": "Waiting for capacity",
    "blocked": "Dispatch blocked",
    "queued": "Queued",
    "checking_sync": "Checking fleet state",
    "materializing": "Preparing workspace",
    "provisioning": "Preparing target",
    "starting_session": "Starting session",
    "delivering_prompt": "Delivering work",
    "dispatching": "Dispatching",
    "dispatched": "Dispatched",
    "materialized": "Workspace ready",
    "running": "Working",
    "completion_pending": "Finishing",
    "completed": "Completed",
    "acknowledged": "Completed",
    "failed": "Failed",
    "cancelled": "Cancelled",
}
FRESHNESS_LABELS = {
    "fresh": "Current",
    "live": "Current",
    "delayed": "Delayed",
    "stale": "Stale",
    "stalled": "Update overdue",
    "disconnected": "Disconnected",
    "completed": "Completed",
    "failed": "Failed",
    "unavailable": "Unavailable",
}


def _value(source: Any, key: str, default: Any = None) -> Any:
    if source is None:
        return default
    if isinstance(source, dict):
        return source.get(key, default)
    return getattr(source, key, default)


def _enum_value(value: Any) -> Any:
    return value.value if isinstance(value, Enum) else value


def _text(value: Any, *, limit: int = 240) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict):
        value = value.get("summary") or value.get("prompt") or value.get("message")
    normalized = " ".join(str(value or "").split())
    if not normalized:
        return None
    return normalized[:limit]


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return str(value)


def _parsed_time(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except TypeError, ValueError:
        return None


def relative_time(value: Any, *, now: datetime | None = None) -> str:
    """Return the shared compact time vocabulary used by work surfaces."""
    observed = _parsed_time(value)
    if observed is None:
        return "Time unavailable"
    current = now or datetime.now(UTC)
    seconds = max(0, int((current - observed).total_seconds()))
    if seconds < 10:
        return "Just now"
    if seconds < 60:
        return f"{seconds}s ago"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 7:
        return f"{days}d ago"
    return observed.astimezone(UTC).strftime("%b %-d, %Y")


def absolute_time(value: Any) -> str:
    observed = _parsed_time(value)
    if observed is None:
        return "Time unavailable"
    return observed.astimezone(UTC).strftime("%Y-%m-%d %H:%M UTC")


def _operator_prompt(latest: dict[str, Any]) -> str | None:
    request = latest.get("operator_input")
    if isinstance(request, dict):
        return _text(request.get("prompt") or request.get("summary"))
    return _text(request)


def _completion_delivery_error(dispatch: dict[str, Any]) -> str | None:
    """Return a completion-delivery error only when turn-end delivery was attempted."""
    progress = dispatch.get("progress") or {}
    progress_error = _text(progress.get("delivery_error"))
    if progress_error:
        return progress_error
    delivery = dispatch.get("completion_outbox") or {}
    delivery_class = str(delivery.get("classification") or "")
    last_error = _text(delivery.get("last_error"))
    if not last_error:
        return None
    if delivery_class in FAILED_DELIVERY_CLASSES:
        return last_error
    agent_turn = dispatch.get("agent_turn") or {}
    if agent_turn.get("ended"):
        return last_error
    if dispatch.get("completion_payload") or dispatch.get("completion_received_at"):
        return last_error
    state = str(dispatch.get("effective_state") or dispatch.get("state") or "")
    if state == "completion_pending":
        return last_error
    return None


def _watch_gate(watch: Any) -> tuple[bool, str | None, str | None]:
    status = str(_enum_value(_value(watch, "status", "")) or "")
    if _value(watch, "retired_at") is not None or status not in CURRENT_WATCH_STATES:
        return False, None, None
    state = _value(watch, "state", {}) or {}
    gate = state.get("gate") or {}
    reasons = gate.get("reasons") or []
    reason = _text(reasons[0] if reasons else None)
    last_error = _text(_value(watch, "last_error"))
    actionable = bool(gate.get("actionable")) or status == "blocked" or bool(last_error)
    return (
        actionable,
        last_error or reason,
        _value(watch, "pr_url") or _value(watch, "url"),
    )


def _card_summary(card: Any) -> str:
    """Return only summary text whose lifecycle state permits presentation."""
    status = str(_enum_value(_value(card, "summary_status", "")) or "")
    source = str(_enum_value(_value(card, "summary_source", "")) or "")
    stale = bool(_value(card, "summary_stale", False))
    summary = _text(_value(card, "summary"))
    if summary and source != "fallback" and status == "ready" and not stale:
        return summary
    if status == "disabled":
        return "Summary generation is disabled."
    if status in {"pending", "stale"} or stale:
        return "Summary pending."
    return "No current execution signal."


def _session_facts(session: dict[str, Any] | None) -> dict[str, Any]:
    session = session or {}
    liveness = session.get("liveness") or {}
    activity = session.get("activity") or {}
    turn = session.get("turn") or {}
    state = str(
        session.get("session_state")
        or session.get("state")
        or session.get("status")
        or ""
    )
    classification = str(liveness.get("classification") or "")
    turn_state = str(turn.get("state") or "")
    active_tool = activity.get("active_tool") or session.get("active_tool")
    active = bool(
        turn_state in {"starting", "running"}
        or active_tool
        or state in ACTIVE_SESSION_STATES
        or session.get("live")
        and state not in {"idle", "completed", "failed"}
    )
    quiet = bool(
        not active
        and (
            state in QUIET_SESSION_STATES
            or classification == "completed_idle"
            or session.get("connected")
        )
    )
    failed = classification == "failed_closed" or state in {"failed", "error"}
    return {
        "active": active,
        "quiet": quiet,
        "failed": failed,
        "state": state,
        "classification": classification,
        "tool": active_tool,
        "activity": activity,
    }


def _action(kind: str, label: str, **values: Any) -> dict[str, Any]:
    return {"kind": kind, "label": label, **values}


def present_work_item(
    card: Any,
    *,
    dispatch: dict[str, Any] | None = None,
    session: dict[str, Any] | None = None,
    watches: Iterable[Any] = (),
    target_instance_name: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Derive one truthful lifecycle summary and attention decision.

    Active turn/tool evidence always outranks an older completed checkpoint.
    A Waiting lane is metadata only and never creates attention by itself.
    """
    dispatch = dispatch or {}
    progress = dispatch.get("progress") or {}
    latest = progress.get("latest") or {}
    freshness = progress.get("freshness") or {}
    session_facts = _session_facts(session)
    card_id = str(_value(card, "id", ""))
    card_title = str(_value(card, "title", "Untitled work"))
    realm_id = str(_value(card, "realm_id", "default") or "default")
    lane = str(_enum_value(_value(card, "lane", "")) or "")
    dispatch_id = dispatch.get("dispatch_id")
    session_id = (
        dispatch.get("session_id")
        or (session or {}).get("id")
        or (session or {}).get("session_id")
    )
    target_id = dispatch.get("target_instance_id") or (session or {}).get("instance_id")
    state = str(dispatch.get("effective_state") or dispatch.get("state") or "")
    latest_phase = str(latest.get("phase") or "")
    latest_summary = _text(latest.get("summary"))
    timestamp = (
        freshness.get("last_activity_at")
        or latest.get("last_activity_at")
        or latest.get("occurred_at")
        or dispatch.get("updated_at")
        or _value(card, "updated_at")
    )
    freshness_state = str(freshness.get("state") or "unavailable")
    card_href = f"/?realm={realm_id}&card={card_id}" if card_id else "/work"
    agent_href = (
        f"/agent?session={session_id}" + (f"&instance={target_id}" if target_id else "")
        if session_id
        else card_href
    )

    group = "quiet"
    state_code = state or lane or "unknown"
    state_label = DISPATCH_LABELS.get(state, "Not in motion")
    summary = latest_summary or _card_summary(card)
    reason = "No operator-owned next step is recorded."
    tone = "muted"
    priority = 0
    action = _action("open_card", "Open card", href=card_href)
    action_explanation = "No operator action is available from current evidence."
    attention_code: str | None = None

    operator_prompt = _operator_prompt(latest)
    blockers = [text for item in latest.get("blockers") or [] if (text := _text(item))]
    delivery = dispatch.get("completion_outbox") or {}
    delivery_class = str(delivery.get("classification") or "")
    delivery_error = _completion_delivery_error(dispatch)
    reconciliation = dispatch.get("card_reconciliation") or {}
    reconciliation_state = str(reconciliation.get("state") or "")
    reconciliation_reason = _text(
        reconciliation.get("last_dependency_error")
        or reconciliation.get("disposition_error")
        or reconciliation.get("reason")
        or reconciliation.get("condition")
    )
    review = next(
        (
            (watch, watch_reason, watch_url)
            for watch in watches
            if (gate := _watch_gate(watch))[0]
            for watch_reason, watch_url in [(gate[1], gate[2])]
        ),
        None,
    )

    # Pending operator input is already an operator-owned gate, even while
    # the provider keeps its turn open. Otherwise, a current turn or tool is
    # authoritative over older completed or idle checkpoints.
    if operator_prompt:
        group = "attention"
        state_code = "input_required"
        state_label = "Input needed"
        summary = operator_prompt
        reason = "The agent requested operator input before it can continue."
        tone = "blocked"
        priority = 120
        attention_code = "operator_input"
        action = _action("respond", "Respond", href=agent_href)
        action_explanation = None
    elif session_facts["active"]:
        tool = session_facts["tool"] or {}
        tool_name = _text(tool.get("name") if isinstance(tool, dict) else tool)
        group = "motion"
        state_code = "working"
        state_label = "Working"
        summary = tool_name or latest_summary or "Agent turn is active."
        reason = "A current agent turn or tool is running."
        tone = "active"
        priority = 90
        action = _action("open_agent", "Open agent", href=agent_href)
        action_explanation = (
            "No operator action needed; autonomous work is progressing."
        )
    elif review:
        _watch, review_reason, review_url = review
        group = "attention"
        state_code = "review_gate"
        state_label = "Review needed"
        summary = review_reason or "A pull-request review gate needs a decision."
        reason = "Integration is waiting on an operator-owned review gate."
        tone = "blocked"
        priority = 110
        attention_code = "review_gate"
        action = _action(
            "review",
            "Review",
            href=review_url or card_href,
            external=bool(review_url),
        )
        action_explanation = None
    elif (
        lane == "done"
        and state in {"failed", "cancelled"}
        and not session_facts["active"]
    ):
        group = "outcome"
        state_code = "completed"
        state_label = "Completed"
        evaluation = dispatch.get("post_turn_evaluation") or {}
        summary = (
            _text(evaluation.get("operator_status_text"))
            or latest_summary
            or "Work completed."
        )
        reason = "The card is Done; the linked dispatch did not finish autonomously."
        tone = "success"
        priority = 50
        action = _action("open_card", "Open card", href=card_href)
        action_explanation = "No operator action is required for this outcome."
    elif delivery_error or delivery_class in FAILED_DELIVERY_CLASSES:
        group = "attention"
        state_code = "delivery_failed"
        state_label = "Delivery failed"
        summary = (
            delivery_error
            or f"Completion delivery is {delivery_class.replace('_', ' ')}."
        )
        reason = "The completed turn has not been delivered or reconciled safely."
        tone = "failed"
        priority = 108
        attention_code = "delivery_failure"
        action = _action("inspect", "Inspect delivery", href=card_href)
        action_explanation = None
    elif reconciliation_state in FAILED_RECONCILIATION_STATES:
        group = "attention"
        state_code = "reconciliation_failed"
        state_label = "Reconciliation blocked"
        summary = reconciliation_reason or "Card reconciliation needs a decision."
        reason = "The authoritative card state could not be reconciled automatically."
        tone = "failed"
        priority = 106
        attention_code = "reconciliation_failure"
        action = _action("inspect", "Inspect blocker", href=card_href)
        action_explanation = None
    elif blockers or latest_phase == "blocked" or state == "blocked":
        group = "attention"
        state_code = "blocked"
        state_label = "Blocked"
        summary = (
            blockers[0]
            if blockers
            else latest_summary
            or _text(dispatch.get("last_error"))
            or "Dispatch is blocked."
        )
        reason = "A concrete blocker requires inspection or an operator decision."
        tone = "blocked"
        priority = 104
        attention_code = "explicit_blocker"
        action = _action("inspect", "Inspect blocker", href=card_href)
        action_explanation = None
    elif state in {"failed", "cancelled"} and dispatch.get("can_retry"):
        group = "attention"
        state_code = "retry_required"
        state_label = "Retry decision needed"
        summary = _text(dispatch.get("last_error")) or f"Dispatch {state}."
        reason = "The dispatch stopped and is explicitly safe to retry."
        tone = "failed"
        priority = 102
        attention_code = "retry_decision"
        action = _action("retry", "Retry", dispatch_id=dispatch_id)
        action_explanation = None
    elif freshness_state in STALE_PROGRESS_STATES and state in ACTIVE_DISPATCH_STATES:
        group = "attention"
        state_code = "progress_stale"
        state_label = "Progress overdue"
        summary = latest_summary or "No current progress checkpoint is available."
        reason = "Active work has no current runtime signal; inspect before deciding whether to retry."
        tone = "warning"
        priority = 100
        attention_code = "stale_progress"
        action = _action("inspect", "Inspect progress", href=card_href)
        action_explanation = None
    elif state in STARTING_DISPATCH_STATES:
        group = "motion"
        state_code = state
        state_label = DISPATCH_LABELS.get(state, "Starting")
        summary = latest_summary or state_label
        reason = "The dispatch is progressing autonomously."
        tone = "active"
        priority = 80
        action = _action("open_card", "Open card", href=card_href)
        action_explanation = "No operator action needed; startup is in progress."
    elif state in ACTIVE_DISPATCH_STATES:
        group = "motion"
        state_code = "awaiting_turn" if session_facts["quiet"] else state
        state_label = (
            "Awaiting next turn"
            if session_facts["quiet"]
            else DISPATCH_LABELS.get(state, "In motion")
        )
        summary = latest_summary or "The agent runtime is connected and awaiting work."
        reason = "The execution remains current; no operator-owned blocker is recorded."
        tone = "active"
        priority = 70
        action = (
            _action("open_agent", "Open agent", href=agent_href)
            if session_id
            else _action("open_card", "Open card", href=card_href)
        )
        action_explanation = "No operator action needed; the runtime is available."
    elif state in {"completed", "acknowledged"} or lane == "done":
        group = "outcome"
        state_code = "completed"
        state_label = "Completed"
        evaluation = dispatch.get("post_turn_evaluation") or {}
        summary = (
            _text(evaluation.get("operator_status_text"))
            or latest_summary
            or "Work completed."
        )
        reason = "This is a terminal outcome, not active work."
        tone = "success"
        priority = 50
        action = _action("open_card", "Open card", href=card_href)
        action_explanation = "No operator action is required for this outcome."
    elif state in TERMINAL_DISPATCH_STATES:
        group = "outcome"
        state_code = state
        state_label = DISPATCH_LABELS.get(state, state.capitalize())
        summary = _text(dispatch.get("last_error")) or latest_summary or state_label
        reason = "This execution has ended and no safe retry is currently available."
        tone = "failed" if state == "failed" else "muted"
        priority = 45
        action = _action("open_card", "Open card", href=card_href)
        action_explanation = "Inspect the terminal record for context."
    elif session_facts["quiet"]:
        state_code = "agent_idle"
        state_label = "Agent idle"
        summary = "The agent runtime is connected with no active turn or tool."
        reason = "No operator-owned next step is recorded."
        action = _action("open_agent", "Open agent", href=agent_href)
        action_explanation = "No operator action is available from current evidence."
    elif session_facts["failed"]:
        group = "outcome"
        state_code = "session_failed"
        state_label = "Session failed"
        summary = "The agent session ended without a current retry signal."
        reason = "This is terminal session evidence, not active work."
        tone = "failed"
        priority = 44

    presentation = {
        "group": group,
        "attention": group == "attention",
        "in_motion": group == "motion",
        "terminal": group == "outcome",
        "attention_code": attention_code,
        "priority": priority,
        "state": state_code,
        "state_label": state_label,
        "summary": summary,
        "reason": reason,
        "tone": tone,
        "freshness": freshness_state,
        "freshness_label": FRESHNESS_LABELS.get(
            freshness_state,
            freshness_state.replace("_", " ").capitalize(),
        ),
        "occurred_at": _iso(timestamp),
        "relative_time": relative_time(timestamp, now=now),
        "absolute_time": absolute_time(timestamp),
        "target_instance_id": target_id,
        "target_instance_name": target_instance_name or target_id or "Unassigned",
        "dispatch_id": dispatch_id,
        "session_id": session_id,
        "action": action,
        "action_explanation": action_explanation,
    }
    presentation["accessible_label"] = "; ".join(
        part
        for part in (
            card_title,
            state_label,
            summary,
            reason,
            f"Target {presentation['target_instance_name']}",
            f"{presentation['freshness_label']}, {presentation['relative_time']}",
            action.get("label"),
        )
        if part
    )
    return presentation
