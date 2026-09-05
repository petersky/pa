"""Versioned, privacy-safe projections of authoritative ACP runtime state."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pa.execution.progress import sanitize_text
from pa.execution.session_presentation import build_session_presentation

SESSION_OBSERVABILITY_VERSION = 1
SESSION_OBSERVABILITY_CAPABILITY = "pa.session-observability.v1"
DEFAULT_QUIET_SECONDS = 120
DEFAULT_STALLED_SECONDS = 900
MAX_DIAGNOSTIC_EVENTS = 100

_SAFE_EVENT_FIELDS = {
    "queue_enqueued": {"id", "action", "position"},
    "queue_dequeued": {"id"},
    "user_message": {"id", "source"},
    "turn_completed": {"queued_prompt_id", "stop_reason", "usage"},
    "prompt_blocked": {"queued_prompt_id", "reason", "before_provider_delivery"},
    "tool_call": {"title", "kind", "status"},
    "tool_call_update": {"title", "kind", "status"},
    "error": {"queued_prompt_id"},
    "session_closed": {"reason", "prior_status"},
    "connection_lost": {"queued_prompt_id"},
    "recovery_started": {"reason"},
    "recovery_completed": {"reason"},
}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _age_ms(now: datetime, value: datetime | None) -> int | None:
    if value is None:
        return None
    return max(0, int((now - value).total_seconds() * 1000))


def _safe_event(event: Any) -> dict[str, Any]:
    event_type = str(event.event_type)
    allowed = _SAFE_EVENT_FIELDS.get(event_type, set())
    payload = {
        key: (sanitize_text(value, limit=160) if isinstance(value, str) else value)
        for key, value in dict(event.payload or {}).items()
        if key in allowed
    }
    return {
        "seq": event.seq,
        "type": event_type,
        "occurred_at": _iso(event.created_at),
        "payload": payload,
    }


def _turns(events: list[Any], runtime: Any | None) -> list[dict[str, Any]]:
    turns: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for event in events:
        if event.event_type not in {
            "queue_enqueued",
            "queue_dequeued",
            "user_message",
            "turn_completed",
            "prompt_blocked",
            "error",
            "connection_lost",
        }:
            # Command receipts and other protocol records may also carry an
            # ``id``.  They are not prompt admissions and must not manufacture
            # phantom turns.
            continue
        payload = event.payload or {}
        prompt_id = payload.get("id") or payload.get("queued_prompt_id")
        if not prompt_id:
            continue
        prompt_id = str(prompt_id)
        if prompt_id not in turns:
            order.append(prompt_id)
            turns[prompt_id] = {
                "id": prompt_id,
                "sequence": len(order),
                "state": "unknown",
                "prompt_source": "unknown",
                "queue_position": None,
                "accepted_at": None,
                "started_at": None,
                "completed_at": None,
                "stop_reason": None,
            }
        turn = turns[prompt_id]
        if event.event_type == "queue_enqueued":
            turn.update(
                state="queued",
                queue_position=payload.get("position"),
                accepted_at=_iso(event.created_at),
                started_at=None,
                completed_at=None,
                stop_reason=None,
            )
        elif event.event_type == "queue_dequeued":
            turn.update(
                state="starting",
                started_at=_iso(event.created_at),
                completed_at=None,
                stop_reason=None,
            )
        elif event.event_type == "user_message":
            turn.update(
                state="running",
                prompt_source=sanitize_text(payload.get("source"), limit=80)
                or "unknown",
                accepted_at=turn["accepted_at"] or _iso(event.created_at),
                started_at=turn["started_at"] or _iso(event.created_at),
            )
        elif event.event_type == "turn_completed":
            turn.update(
                state="completed",
                completed_at=_iso(event.created_at),
                stop_reason=sanitize_text(payload.get("stop_reason"), limit=80) or None,
            )
        elif event.event_type == "prompt_blocked":
            turn.update(
                state="blocked",
                completed_at=None,
                stop_reason=sanitize_text(payload.get("reason"), limit=160) or None,
            )
        elif event.event_type in {"error", "connection_lost"}:
            turn.update(state="failed", completed_at=_iso(event.created_at))

    in_flight = getattr(runtime, "_in_flight", None) if runtime else None
    if in_flight:
        prompt_id = str(in_flight.id)
        if prompt_id not in turns:
            order.append(prompt_id)
            turns[prompt_id] = {
                "id": prompt_id,
                "sequence": len(order),
                "state": "running",
                "prompt_source": sanitize_text(in_flight.source, limit=80) or "unknown",
                "queue_position": None,
                "accepted_at": None,
                "started_at": _iso(getattr(runtime, "_turn_started_at", None)),
                "completed_at": None,
                "stop_reason": None,
            }
        else:
            turns[prompt_id]["state"] = "running"
            turns[prompt_id]["completed_at"] = None
            turns[prompt_id]["stop_reason"] = None
            turns[prompt_id]["started_at"] = turns[prompt_id]["started_at"] or _iso(
                getattr(runtime, "_turn_started_at", None)
            )
    return [turns[prompt_id] for prompt_id in order]


def _durable_prompt_state(session: Any, runtime: Any | None) -> tuple[Any | None, list[Any], bool | None]:
    if runtime:
        return (
            getattr(runtime, "_in_flight", None),
            list(getattr(runtime, "_queue", []) or []),
            bool(getattr(runtime, "_queue_paused", False)),
        )
    durable = dict((session.config_json or {}).get("durable_runtime") or {})
    return durable.get("in_flight"), list(durable.get("queued_prompts") or []), (
        bool(durable.get("queue_paused"))
        if "queue_paused" in durable
        else None
    )


def build_session_observability(
    session: Any,
    *,
    runtime: Any | None,
    events: list[Any],
    instance_id: str,
    instance_name: str,
    reconciliation: dict[str, Any] | None = None,
    now: datetime | None = None,
    quiet_seconds: int = DEFAULT_QUIET_SECONDS,
    stalled_seconds: int = DEFAULT_STALLED_SECONDS,
) -> dict[str, Any]:
    """Normalize live and durable evidence without manufacturing legacy health."""
    now = now or datetime.now(UTC)
    turns = _turns(events, runtime)
    durable_in_flight, durable_queue, queue_paused = _durable_prompt_state(
        session, runtime
    )
    terminal_prompt_ids = {
        str(turn["id"])
        for turn in turns
        if turn.get("state") in {"completed", "failed"}
    }
    if not runtime and durable_in_flight:
        in_flight_id = (
            durable_in_flight.get("id")
            if isinstance(durable_in_flight, dict)
            else getattr(durable_in_flight, "id", None)
        )
        if in_flight_id and str(in_flight_id) in terminal_prompt_ids:
            durable_in_flight = None
    if not runtime and durable_queue:
        durable_queue = [
            item
            for item in durable_queue
            if str(
                item.get("id")
                if isinstance(item, dict)
                else getattr(item, "id", "")
            )
            not in terminal_prompt_ids
        ]
    if durable_in_flight and not runtime:
        raw_id = (
            durable_in_flight.get("id")
            if isinstance(durable_in_flight, dict)
            else getattr(durable_in_flight, "id", None)
        )
        if raw_id and not any(turn["id"] == str(raw_id) for turn in turns):
            turns.append(
                {
                    "id": str(raw_id),
                    "sequence": len(turns) + 1,
                    "state": "starting",
                    "prompt_source": "durable_admission",
                    "queue_position": None,
                    "accepted_at": None,
                    "started_at": None,
                    "completed_at": None,
                    "stop_reason": None,
                }
            )
    for position, queued in enumerate(durable_queue):
        raw_id = queued.get("id") if isinstance(queued, dict) else getattr(queued, "id", None)
        if not raw_id or any(turn["id"] == str(raw_id) for turn in turns):
            continue
        source = queued.get("source") if isinstance(queued, dict) else getattr(queued, "source", None)
        turns.append(
            {
                "id": str(raw_id),
                "sequence": len(turns) + 1,
                "state": "queued",
                "prompt_source": sanitize_text(source, limit=80) or "durable_admission",
                "queue_position": position,
                "accepted_at": None,
                "started_at": None,
                "completed_at": None,
                "stop_reason": None,
            }
        )
    current_turn = next(
        (
            turn
            for turn in reversed(turns)
            if turn["state"] in {"queued", "starting", "running", "blocked"}
        ),
        None,
    )
    last_event = events[-1] if events else None
    meaningful = next(
        (
            event
            for event in reversed(events)
            if event.event_type
            in {
                "agent_message_chunk",
                "tool_call",
                "tool_call_update",
                "turn_completed",
            }
        ),
        None,
    )
    heartbeat_at = getattr(runtime, "_runtime_observed_at", None) if runtime else None
    heartbeat_at = heartbeat_at or (session.updated_at if runtime else None)
    protocol_at = last_event.created_at if last_event else None
    progress_at = meaningful.created_at if meaningful else None
    heartbeat_age = _age_ms(now, heartbeat_at)
    protocol_age = _age_ms(now, protocol_at)
    progress_age = _age_ms(now, progress_at)
    connected = bool(runtime and runtime.connected)
    busy = bool(current_turn and current_turn["state"] in {"starting", "running"})

    durable_obligations = bool(durable_in_flight or durable_queue)
    if session.status == "closed":
        classification = "completed_idle"
    elif session.status == "quiesced":
        classification = "restarting"
    elif session.status in {"failed", "configuration_failed", "provisioning_failed"}:
        classification = "failed_closed"
    elif not runtime and durable_obligations:
        classification = "restoring"
    elif not runtime and getattr(session, "purpose", "unknown") == "chat":
        classification = "available"
    elif not runtime and getattr(session, "purpose", "unknown") in {
        "automated_run",
        "one_shot_job",
    }:
        classification = "workflow_waiting"
    elif not runtime:
        classification = "unknown"
    elif not connected:
        classification = "disconnected_recovering"
    elif current_turn and current_turn["state"] == "queued" and not busy:
        classification = "queued"
    elif not busy:
        classification = "completed_idle"
    elif progress_age is not None and progress_age >= stalled_seconds * 1000:
        classification = "potentially_stalled"
    elif progress_age is not None and progress_age >= quiet_seconds * 1000:
        classification = "quiet_active"
    else:
        classification = "live"

    conn = getattr(runtime, "connection", None) if runtime else None
    proc = getattr(conn, "_proc", None) if conn else None
    queue = durable_queue
    active_tool_event = next(
        (
            event
            for event in reversed(events)
            if event.event_type in {"tool_call", "tool_call_update"}
        ),
        None,
    )
    active_tool = None
    if active_tool_event and (active_tool_event.payload or {}).get("status") not in {
        "completed",
        "failed",
        "cancelled",
    }:
        active_tool = {
            "category": sanitize_text(
                (active_tool_event.payload or {}).get("kind") or "tool", limit=80
            ),
            "name": sanitize_text(
                (active_tool_event.payload or {}).get("title") or "tool", limit=120
            ),
            "started_at": _iso(active_tool_event.created_at),
            "elapsed_ms": _age_ms(now, active_tool_event.created_at),
        }

    lifecycle = (
        "busy"
        if busy
        else "connected"
        if connected
        else "recovering"
        if runtime
        else "closed"
        if session.status == "closed"
        else "restarting"
        if session.status == "quiesced"
        else "failed"
        if "failed" in session.status
        else "idle"
    )
    presentation = build_session_presentation(session, runtime=runtime, now=now)
    return {
        "schema_version": SESSION_OBSERVABILITY_VERSION,
        "capabilities": [SESSION_OBSERVABILITY_CAPABILITY],
        "session_id": session.id,
        "dispatch_id": session.dispatch_id,
        "card_id": session.card_id,
        "authority_instance_id": session.authority_instance_id,
        "instance": {
            "id": session.origin_instance_id or instance_id,
            "name": session.origin_instance_name or instance_name,
        },
        "provider": {
            "id": session.agent_name,
            "model": session.model_id,
            "mode": session.mode_id,
        },
        "session_state": lifecycle,
        "presentation": presentation,
        "turn": current_turn,
        "turns": turns,
        "queue": {
            "length": len(queue),
            "paused": queue_paused,
            "prompt_ids": [
                str(item.get("id"))
                if isinstance(item, dict)
                else str(item.id)
                for item in queue
            ],
        },
        "liveness": {
            "classification": classification,
            "heartbeat_age_ms": heartbeat_age,
            "last_protocol_event_age_ms": protocol_age,
            "last_progress_age_ms": progress_age,
            "observed_at": _iso(heartbeat_at),
            "authority_received_at": _iso(now),
            "evidence": [
                evidence
                for evidence in (
                    "provider_transport_connected" if connected else None,
                    "turn_in_flight" if busy else None,
                    "queued_prompt"
                    if current_turn and current_turn["state"] == "queued"
                    else None,
                    "recent_sanitized_progress" if progress_at else None,
                )
                if evidence
            ],
        },
        "activity": {
            "phase": (
                "running"
                if busy
                else "blocked"
                if current_turn and current_turn["state"] == "blocked"
                else "idle"
            ),
            "summary": (
                "Turn currently running"
                if busy
                else "Prompt blocked before provider delivery"
                if current_turn and current_turn["state"] == "blocked"
                else "Prompt queued"
                if current_turn
                else "No active turn"
            ),
            "last_meaningful_progress_at": _iso(progress_at),
            "active_tool": active_tool,
        },
        "transport": {
            "connected": connected,
            "connection_generation": getattr(runtime, "_connection_generation", 0)
            if runtime
            else None,
            "last_read_at": _iso(protocol_at),
            "last_write_at": _iso(
                getattr(runtime, "_turn_started_at", None) if runtime else None
            ),
        },
        "provider_process": {
            "state": (
                "unsupported"
                if proc is None
                else "running"
                if getattr(proc, "returncode", None) is None
                else "exited"
            ),
            "pid": getattr(proc, "pid", None) if proc else None,
            "restart_count": getattr(runtime, "_connection_generation", 0)
            if runtime
            else None,
            "exit_status": getattr(proc, "returncode", None) if proc else None,
            "last_health_probe_at": _iso(heartbeat_at),
        },
        "completion": {
            "delivery": "completed"
            if any(t["state"] == "completed" for t in turns)
            else "not_started",
            "card_reconciliation": (reconciliation or {}).get("state", "unknown"),
        },
        "recovery": {
            "recommended_action": (
                "wait_for_user"
                if presentation["display_status"] == "Needs you"
                else "observe"
                if classification in {"live", "quiet_active"}
                else "checkpoint"
                if classification == "potentially_stalled"
                else "reconnect"
                if classification in {"restoring", "disconnected_recovering"}
                else None
            ),
            "actions": presentation["permitted_actions"],
        },
        "last_state_transition": _iso(session.updated_at),
        "error": {
            "code": "unknown" if "failed" in session.status else None,
            "retryable": None if not runtime else True,
        },
    }


def diagnostic_timeline(events: list[Any], *, limit: int = 50) -> list[dict[str, Any]]:
    """Return a bounded allowlisted protocol timeline; never raw prompts/output."""
    bounded = max(1, min(limit, MAX_DIAGNOSTIC_EVENTS))
    return [_safe_event(event) for event in events[-bounded:]]
