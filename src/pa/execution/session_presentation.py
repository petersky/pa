"""One authoritative product presentation for durable agent sessions."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

SESSION_PRESENTATION_SCHEMA = "pa.session-presentation"
SESSION_PRESENTATION_VERSION = 1
SESSION_PRESENTATION_CAPABILITY = "pa.session-presentation.v1"

_TERMINAL_WORKFLOW_STATES = frozenset(
    {"succeeded", "failed", "cancelled", "validation_failed"}
)
_BLOCKED_SESSION_STATES = frozenset(
    {"recovery_blocked", "configuration_failed", "provisioning_failed"}
)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _iso(value: Any) -> str | None:
    return value.isoformat() if isinstance(value, datetime) else value or None


def _pending_interaction(session: Any, runtime: Any | None) -> dict[str, Any] | None:
    if runtime:
        permissions = list(getattr(runtime, "_pending_permissions", {}) or {})
        if permissions:
            return {
                "kind": "permission",
                "count": len(permissions),
                "request_ids": permissions,
                "action": "Approve or deny the requested permission.",
            }
        elicitations = list(getattr(runtime, "_pending_elicitations", {}) or {})
        if elicitations:
            return {
                "kind": "input",
                "count": len(elicitations),
                "request_ids": elicitations,
                "action": "Respond to the agent's requested input.",
            }
    durable = dict((session.config_json or {}).get("durable_runtime") or {})
    pending = durable.get("pending_interaction")
    if isinstance(pending, dict):
        return dict(pending)
    permissions = list(durable.get("pending_permissions") or [])
    if permissions:
        return {
            "kind": "permission",
            "count": len(permissions),
            "request_ids": [str(item.get("id") or item) for item in permissions],
            "action": "Approve or deny the requested permission.",
        }
    return None


def build_session_presentation(
    session: Any,
    *,
    runtime: Any | None = None,
    dispatch: Any | None = None,
    quiescing: bool = False,
    startup_complete: bool = True,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Combine durable obligations with optional live evidence without guessing.

    This contract is intentionally product-facing.  Provider transport and raw
    lifecycle values remain available in details, but are never promoted to a
    global healthy/offline claim.
    """

    now = now or datetime.now(UTC)
    durable = dict((session.config_json or {}).get("durable_runtime") or {})
    recovery = dict(getattr(session, "recovery_json", None) or {})
    live = bool(runtime and not getattr(runtime, "_closed", False))
    connected = bool(live and getattr(runtime, "connected", False))
    prompting = bool(live and getattr(runtime, "prompting", False))
    queue = (
        list(getattr(runtime, "_queue", []) or [])
        if live
        else list(durable.get("queued_prompts") or [])
    )
    in_flight = getattr(runtime, "_in_flight", None) if live else durable.get("in_flight")
    obligations = bool(
        in_flight
        or queue
        or durable.get("pending_permissions")
        or durable.get("pending_interaction")
        or durable.get("lifecycle")
        in {
            "admitted",
            "prompting",
            "queued",
            "permission_pending",
            "completion_pending",
            "reconciliation_pending",
            "recoverable_interrupted",
        }
    )
    interaction = _pending_interaction(session, runtime)
    purpose = getattr(session, "purpose", "unknown") or "unknown"
    control = getattr(session, "control_mode", "automation") or "automation"
    workflow_state = getattr(session, "workflow_state", "unknown") or "unknown"
    workflow_outcome = dict(getattr(session, "workflow_outcome", None) or {})
    initiating_workflow = dict(getattr(session, "initiating_workflow", None) or {})
    dispatch_state = str(_field(dispatch, "state", "") or "")
    if purpose in {"automated_run", "one_shot_job"} and workflow_state not in _TERMINAL_WORKFLOW_STATES:
        if dispatch_state == "completed":
            workflow_state = "succeeded"
        elif dispatch_state in {"failed", "cancelled"}:
            workflow_state = dispatch_state
        evaluated = _field(dispatch, "evaluated_outcome")
        if isinstance(evaluated, dict) and evaluated:
            workflow_outcome = {**workflow_outcome, **evaluated}
    archived = getattr(session, "archived_at", None) is not None
    status = str(getattr(session, "status", "unknown") or "unknown")

    queue_reason = None
    if queue:
        if control == "human" and any(
            str(_field(item, "source", "") or "")
            .casefold()
            .startswith(("card-reconciliation:", "pr-supervisor", "post-turn", "evaluation", "reconciliation", "dispatch"))
            for item in queue
        ):
            queue_reason = "automation_paused_for_takeover"
        elif prompting or in_flight:
            queue_reason = "waiting_for_current_response"
        else:
            queue_reason = "waiting_for_provider_capacity"

    if archived:
        display_status, explanation, next_action = (
            "Archived",
            "This conversation is archived. Its durable history is preserved.",
            None,
        )
    elif interaction:
        display_status, explanation, next_action = (
            "Needs you",
            interaction.get("action") or "Your input is required before work can continue.",
            "wait_for_user",
        )
    elif status in _BLOCKED_SESSION_STATES or recovery.get("blocked"):
        display_status, explanation, next_action = (
            "Recovery blocked",
            recovery.get("remedy")
            or durable.get("recovery_action")
            or (session.config_json or {}).get("provisioning", {}).get("action")
            or "Correct the provider or workspace configuration, then retry.",
            None,
        )
    elif quiescing or status == "quiesced" or (not startup_complete and obligations):
        display_status, explanation, next_action = (
            "PA is restarting",
            "The durable session is preserved and intentional pauses will remain paused.",
            "restore_after_restart" if obligations else None,
        )
    elif purpose in {"automated_run", "one_shot_job"} and workflow_state in _TERMINAL_WORKFLOW_STATES:
        labels = {
            "succeeded": "Completed",
            "failed": "Failed",
            "cancelled": "Cancelled",
            "validation_failed": "Validation failed",
        }
        display_status = labels[workflow_state]
        explanation = str(
            workflow_outcome.get("summary")
            or workflow_outcome.get("reason")
            or f"Workflow {workflow_state.replace('_', ' ')}."
        )
        next_action = None
    elif control == "human" and purpose == "automated_run":
        display_status, explanation, next_action = (
            "Taken over",
            "Automatic prompts are held. Human prompts can continue in this conversation.",
            None,
        )
    elif prompting or in_flight:
        display_status = "Responding" if purpose == "chat" else "Running"
        explanation = "The provider is working on the current turn."
        next_action = "finish_current_turn"
    elif queue:
        display_status = "Queued" if purpose == "chat" else "Running"
        explanation = {
            "automation_paused_for_takeover": "Automatic prompts are held until control returns to automation.",
            "waiting_for_current_response": "Prompts are waiting for the current response.",
            "waiting_for_provider_capacity": "Prompts are durably queued for provider capacity.",
        }[queue_reason]
        next_action = (
            None if queue_reason == "automation_paused_for_takeover" else "start_next_prompt"
        )
    elif not live and obligations:
        retry_at = recovery.get("next_retry_at")
        display_status = "Restoring your work"
        explanation = (
            f"Durable unfinished work will retry at {retry_at}."
            if retry_at
            else "Durable unfinished work is waiting for provider recovery."
        )
        next_action = "retry_recovery"
    elif purpose == "chat":
        display_status, explanation, next_action = (
            "Ready",
            "Conversation history is available; a provider process starts on demand.",
            None,
        )
    elif purpose in {"automated_run", "one_shot_job"}:
        next_event = workflow_outcome.get("next_expected_event") or initiating_workflow.get(
            "next_expected_event"
        )
        display_status = "Running" if workflow_state == "active" else "Waiting"
        explanation = str(
            next_event
            or "The workflow remains active between provider turns."
        )
        next_action = "wait_for_workflow" if not live else None
    else:
        display_status, explanation, next_action = (
            "Limited information",
            "This legacy session has no reliable purpose classification.",
            None,
        )

    actions: list[str] = ["inspect"]
    if purpose == "chat":
        actions.extend(["open", "archive"] if not archived else ["unarchive"])
        if not archived:
            actions.extend(["pin" if not getattr(session, "pinned_at", None) else "unpin", "prompt"])
            if not live:
                actions.append("recover")
    elif purpose == "automated_run" and workflow_state not in _TERMINAL_WORKFLOW_STATES:
        actions.append("return_to_automation" if control == "human" else "take_over")
        if control == "human":
            actions.append("prompt")
    if obligations and not live and not recovery.get("blocked"):
        actions.append("recover")
    if recovery.get("context_lost"):
        actions.append("continue_in_new_chat")
    if prompting:
        actions.append("stop_turn")

    return {
        "schema": SESSION_PRESENTATION_SCHEMA,
        "version": SESSION_PRESENTATION_VERSION,
        "capabilities": [SESSION_PRESENTATION_CAPABILITY],
        "session_id": session.id,
        "purpose": purpose,
        "display_status": display_status,
        "explanation": explanation,
        "next_automatic_action": next_action,
        "connection": {
            "state": (
                "connected"
                if connected
                else "disconnected"
                if live or obligations or recovery
                else "not_started"
            ),
            "live_runtime": live,
        },
        "turn": {
            "state": "running" if prompting or in_flight else "queued" if queue else "idle",
        },
        "workflow": {
            "state": workflow_state,
            "outcome": workflow_outcome,
        },
        "activity": {
            "objective": initiating_workflow.get("objective")
            or initiating_workflow.get("kind")
            or getattr(session, "title", None),
            "latest_meaningful_progress": workflow_outcome.get("latest_progress")
            or workflow_outcome.get("summary"),
            "outcome": workflow_outcome or None,
            "next_expected_event": workflow_outcome.get("next_expected_event")
            or initiating_workflow.get("next_expected_event"),
        },
        "control": {"mode": control},
        "archive": {
            "archived": archived,
            "archived_at": _iso(getattr(session, "archived_at", None)),
            "reason": getattr(session, "archive_reason", None),
            "pinned": getattr(session, "pinned_at", None) is not None,
        },
        "pending_interaction": interaction,
        "queue": {"count": len(queue), "reason": queue_reason},
        "recovery": {
            "attempts": int(recovery.get("attempts") or 0),
            "next_retry_at": recovery.get("next_retry_at"),
            "last_error": recovery.get("last_error") or durable.get("recovery_error"),
            "blocked": bool(recovery.get("blocked") or status in _BLOCKED_SESSION_STATES),
            "remedy": recovery.get("remedy"),
            "context_lost": bool(recovery.get("context_lost")),
        },
        "permitted_actions": list(dict.fromkeys(actions)),
        "observed_at": now.isoformat(),
    }
