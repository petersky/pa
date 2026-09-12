"""Legal restart receipt transitions and passive observation fields.

The receipt and existing recovery coordinator remain authoritative. These
fields are an adapter for the shared operation observation, not another owner.
"""

from __future__ import annotations

TERMINAL = frozenset({"continuation_delivered", "restart_completed"})
LEGAL_TRANSITIONS = {
    "requested": {"waiting_for_turn_end", "failed"},
    "waiting_for_turn_end": {"quiescing", "failed"},
    "quiescing": {"restarting", "failed"},
    "restarting": {"resuming", "restart_completed", "continuation_delivered", "failed"},
    "resuming": {"continuation_queued", "continuation_delivered", "restart_completed", "failed"},
    "continuation_queued": {"continuation_delivered", "failed"},
    "failed": {"continuation_delivered"},
    "continuation_delivered": set(),
    "restart_completed": set(),
}


class RestartTransitionConflict(ValueError):
    code = "stale_restart_phase"


def validate_transition(current, *, status: str, expected_status: str, expected_version: int, owner_instance_id: str, retry: bool = False) -> None:
    if current.status != expected_status or current.phase_version != expected_version:
        raise RestartTransitionConflict("The restart receipt advanced; reread the same operation")
    if not owner_instance_id or (current.instance_id and current.instance_id != owner_instance_id):
        raise RestartTransitionConflict("Restart receipt owner does not match")
    if status == current.status:
        return
    allowed = LEGAL_TRANSITIONS.get(current.status, set())
    if retry and current.status == "failed":
        allowed = allowed | {"requested", "resuming"}
    if status not in allowed:
        raise RestartTransitionConflict(f"Illegal restart transition {current.status} -> {status}")


def restart_reason_code(error: BaseException) -> str:
    code = getattr(error, "code", None)
    if isinstance(code, str) and code:
        return code
    classification = getattr(error, "classification", None)
    if classification == "api_not_ready":
        return "owner_not_ready"
    if isinstance(classification, str) and classification:
        return "owner_channel_" + classification
    return "restart_operation_failed"


def retryable_owner_failure(receipt) -> bool:
    if receipt.reason_code:
        return receipt.failure_stage == "resuming" and receipt.reason_code == "owner_not_ready"
    # One conservative compatibility boundary for previously persisted errors.
    return receipt.failure_stage == "resuming" and "PA MCP owner channel api_not_ready (endpoint=" in (receipt.error or "")


def restart_observation_fields(receipt, *, session=None, runtime=None) -> dict:
    status = receipt.status
    phase, effect, worker, next_action = {
        "requested": ("accepted", "not_started", "unconfirmed", "wait_for_caller_turn"),
        "waiting_for_turn_end": ("waiting", "pending", "unconfirmed", "finish_caller_turn"),
        "quiescing": ("running", "pending", "unconfirmed", "wait_for_quiesce"),
        "restarting": ("reconciling", "unknown", "unconfirmed", "reconcile_service_start"),
        "resuming": ("reconciling", "pending", "unconfirmed", "recover_exact_session"),
        "continuation_queued": ("accepted", "pending", "unconfirmed", "finish_continuation_turn"),
        "continuation_delivered": ("succeeded", "confirmed", "absent", None),
        "restart_completed": ("succeeded", "confirmed", "absent", None),
        "failed": ("failed", "unknown", "absent", "inspect_failure"),
    }.get(status, ("reconciling", "unknown", "unconfirmed", "inspect_legacy_receipt"))
    reason = receipt.reason_code or status
    if status not in TERMINAL and session is not None:
        durable = (session.config_json or {}).get("durable_runtime") or {}
        paused = runtime._queue_paused if runtime is not None else durable.get("queue_paused")
        if session.status == "closed" or session.archived_at:
            phase, reason, next_action = "waiting", "exact_session_closed", "inspect_session"
        elif paused or (session.purpose == "automated_run" and session.control_mode == "human"):
            phase, reason, next_action = "waiting", "operator_paused", "resume_by_operator"
        elif (session.recovery_json or {}).get("blocked"):
            phase, reason, next_action = "waiting", "session_recovery_blocked", "repair_session"
        elif receipt.execution_binding != session.execution_binding:
            phase, reason, next_action = "waiting", "execution_binding_mismatch", "inspect_session"
        elif runtime is not None and status == "continuation_queued":
            running = getattr(runtime, "_in_flight", None) or getattr(runtime, "_draining_prompt", None)
            if running is not None and running.id == receipt.continuation_prompt_id:
                phase, worker = "running", "active"
            elif any(item.id == receipt.continuation_prompt_id for item in runtime._queue):
                worker = "queued"
    return {"phase": phase, "phase_version": receipt.phase_version, "attempt": receipt.attempts, "reason_code": reason, "next_action": next_action, "effect_state": effect, "worker_state": worker, "domain_stage": status}
