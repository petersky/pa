"""Canonical card-scoped execution presentation.

This is deliberately a presentation projection: durable dispatch/session stores
remain the authorities for mutations.  Consumers must not reconstruct presence
from the local ACP table because a dispatch-owned session may live on a peer.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Iterable

from pa.core.ui.instance_identity import canonicalize_dispatch_public
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES


def _value(item: Any, name: str, default: Any = None) -> Any:
    return item.get(name, default) if isinstance(item, dict) else getattr(item, name, default)


def _session_payload(session: Any) -> dict[str, Any]:
    config = dict(_value(session, "config_json", {}) or {})
    execution = dict(config.get("execution_context") or {})
    repositories = list(execution.get("repositories") or [])
    repository = dict(repositories[0]) if repositories else {}
    instance = dict(execution.get("instance") or {})
    requested = dict((config.get("configuration") or {}).get("requested") or {})
    return {
        "id": str(_value(session, "id", "")),
        "title": _value(session, "title") or _value(session, "label") or "Card agent",
        "status": str(_value(session, "status", "unknown")),
        "provider": _value(session, "agent_name") or "Unknown provider",
        "model": _value(session, "model_id") or requested.get("model_id") or "Provider default",
        "mode": _value(session, "mode_id") or "Provider default",
        "owner_instance_id": _value(session, "origin_instance_id"),
        "host": instance.get("name") or _value(session, "origin_instance_name"),
        "dispatch_id": _value(session, "dispatch_id"),
        "worktree": _value(session, "cwd"),
        "branch": repository.get("branch"),
        "updated_at": _value(session, "updated_at"),
        "external_session_id": _value(session, "external_session_id"),
        "source": "local_session_projection",
    }


def _dispatch_payload(ctx: Any, record: Any) -> dict[str, Any]:
    public = canonicalize_dispatch_public(ctx, record)
    request = dict(_value(record, "request_payload", {}) or {})
    progress = dict(public.get("progress") or {})
    latest = dict(progress.get("latest") or {})
    freshness = dict(progress.get("freshness") or {})
    repositories = list(request.get("repositories") or [])
    repository = dict(repositories[0]) if repositories else {}
    return {
        "record": record,
        "public": public,
        "dispatch_id": str(_value(record, "dispatch_id", "")),
        "session_id": _value(record, "session_id"),
        "state": str(public.get("effective_state") or _value(record, "state", "unknown")),
        "owner_instance_id": _value(record, "target_instance_id"),
        "host": public.get("target_instance_name") or "Unknown target",
        "provider": request.get("provider") or "Target default",
        "model": request.get("model_id") or "Provider default",
        "mode": request.get("mode_id") or "Provider default",
        "worktree": request.get("cwd") or repository.get("worktree"),
        "branch": repository.get("branch") or request.get("branch"),
        "phase": latest.get("phase") or _value(record, "state", "unknown"),
        "summary": latest.get("summary") or _value(record, "last_error"),
        "freshness": freshness.get("state") or "unavailable",
        "last_activity_at": freshness.get("last_activity_at") or _value(record, "updated_at"),
        "queue": public.get("queue") or {},
        "followup_state": public.get("followup_state") or {},
        "turn_end": public.get("turn_end"),
        "evaluated_outcome": public.get("evaluated_outcome"),
        "can_retry": bool(public.get("can_retry")),
        "authority_instance_id": _value(record, "authority_instance_id"),
        "initiating_principal": _value(record, "initiating_principal")
        or _value(record, "principal_id")
        or request.get("principal_id"),
        "provenance": "durable_dispatch_envelope",
    }


def build_card_execution_index(
    ctx: Any,
    *,
    sessions: Iterable[Any],
    dispatches: Iterable[Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    """Join dispatches and ACP rows once by durable dispatch/session identity."""
    now = now or datetime.now(UTC)
    by_session = {
        payload["id"]: payload
        for session in sessions
        if (payload := _session_payload(session))["id"]
    }
    executions: list[dict[str, Any]] = []
    consumed_sessions: set[str] = set()

    for record in dispatches:
        dispatch = _dispatch_payload(ctx, record)
        session_id = str(dispatch["session_id"] or "")
        local = by_session.get(session_id)
        if session_id:
            consumed_sessions.add(session_id)
        active = str(_value(record, "state", "")) not in TERMINAL_DISPATCH_STATES
        exact_href = (
            f"/agent?session={session_id}&instance={dispatch['owner_instance_id']}"
            if session_id
            else None
        )
        if active and session_id:
            primary = {"kind": "view", "label": "View running work", "href": exact_href}
        elif dispatch["can_retry"]:
            primary = {"kind": "retry", "label": "Retry", "dispatch_id": dispatch["dispatch_id"]}
        elif session_id:
            primary = {"kind": "resume", "label": "Resume", "href": exact_href}
        else:
            primary = {"kind": "dispatch", "label": "View dispatch", "dispatch_id": dispatch["dispatch_id"]}
        executions.append(
            {
                "id": dispatch["dispatch_id"] or session_id,
                "session": local,
                "dispatch": dispatch,
                "session_id": session_id or None,
                "dispatch_id": dispatch["dispatch_id"],
                "host": dispatch["host"],
                "owner_instance_id": dispatch["owner_instance_id"],
                "provider": (local or {}).get("provider") or dispatch["provider"],
                "model": (local or {}).get("model") or dispatch["model"],
                "mode": (local or {}).get("mode") or dispatch["mode"],
                "branch": (local or {}).get("branch") or dispatch["branch"],
                "worktree": (local or {}).get("worktree") or dispatch["worktree"],
                "phase": dispatch["phase"],
                "freshness": dispatch["freshness"],
                "last_activity_at": dispatch["last_activity_at"],
                "queue": dispatch["queue"],
                "followup_state": dispatch["followup_state"],
                "state": dispatch["state"],
                "active": active,
                "remote_only": local is None,
                "exact_href": exact_href,
                "primary_action": primary,
                "diagnostic": (
                    "The durable execution is known, but its target-local session projection is unavailable. "
                    "Open the exact session or restore target connectivity."
                    if local is None
                    else None
                ),
                "updated_at": dispatch["last_activity_at"] or now,
            }
        )

    for session_id, session in by_session.items():
        if session_id in consumed_sessions:
            continue
        owner = session["owner_instance_id"] or ctx.settings.instance_id
        local = owner == ctx.settings.instance_id
        status = session["status"]
        active = status in {"idle", "connected", "prompting"}
        resumable = not active and bool(session["external_session_id"])
        href = f"/agent?session={session_id}&instance={owner}"
        executions.append(
            {
                "id": session_id,
                "session": session,
                "dispatch": None,
                "session_id": session_id,
                "dispatch_id": None,
                "host": session["host"] or owner,
                "owner_instance_id": owner,
                "provider": session["provider"],
                "model": session["model"],
                "mode": session["mode"],
                "branch": session["branch"],
                "worktree": session["worktree"],
                "phase": "active" if active else status,
                "freshness": "local" if local else "unavailable",
                "last_activity_at": session["updated_at"],
                "queue": {},
                "followup_state": {},
                "state": "active" if active else "resumable" if resumable else "unavailable",
                "active": active,
                "remote_only": not local,
                "exact_href": href,
                "primary_action": {
                    "kind": "view" if active else "resume" if resumable else "view",
                    "label": "View running work" if active else "Resume" if resumable else "View session",
                    "href": href,
                },
                "diagnostic": None if local else "This session is owned by another instance.",
                "updated_at": session["updated_at"] or now,
            }
        )

    executions.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    active = [item for item in executions if item["active"]]
    primary = active[0]["primary_action"] if active else (
        executions[0]["primary_action"] if executions else {"kind": "dispatch", "label": "Dispatch"}
    )
    return {
        "executions": executions,
        "active": active,
        "exclusive_active": bool(active),
        "primary_action": primary,
        "parallel_start": {
            "allowed": True,
            "requires_confirmation": bool(active),
            "requires_reason": bool(active),
            "explanation": (
                "Another execution is active. Parallel work requires an explicit reason and remains subject to policy, capacity, and workspace checks."
                if active
                else None
            ),
        },
        "provenance": "dispatch-ledger+agent-session-store:v1",
    }
