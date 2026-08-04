"""Truthful, presentation-ready projection for the Workshop floor view."""

from __future__ import annotations

from collections.abc import Iterator
from copy import deepcopy
from datetime import UTC, datetime
from heapq import nsmallest
from typing import Any

from pa.domain.models import CardLane
from pa.execution.dispatch import TERMINAL_DISPATCH_STATES as CANONICAL_TERMINAL_STATES
from pa.fleet.control_plane import build_control_plane_status

TERMINAL_DISPATCH_STATES = {*CANONICAL_TERMINAL_STATES, "acknowledged"}
STARTING_DISPATCH_STATES = {
    "queued",
    "checking_sync",
    "materializing",
    "provisioning",
    "starting_session",
    "delivering_prompt",
}
PRE_SESSION_DISPATCH_STATES = {
    "waiting_capacity",
    "blocked",
    *STARTING_DISPATCH_STATES,
}
WORKSHOP_CARD_READ_LIMIT = 120
WORKSHOP_PROJECTION_LIMIT = 80
WORKSHOP_AREA_LIMIT = 12
WORKSHOP_WORKER_LIMIT = 80
SUPPORTED_WORKER_STATES = {
    "queued",
    "starting",
    "working",
    "quiet-active",
    "stalled",
    "recovering",
    "completed",
    "failed",
    "unsupported",
}
TOOL_CATEGORIES = {"coding", "testing", "review", "browser", "research"}
CURRENT_DISPATCH_STATES = {
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

LANE_LABELS = {
    "inbox": "Inbox",
    "active": "Active",
    "waiting": "Waiting",
    "done": "Done",
}
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
    "running": "Running",
    "completion_pending": "Finishing",
    "completed": "Completed",
    "acknowledged": "Completed",
    "failed": "Failed",
    "cancelled": "Cancelled",
}
ACTIVITY_LABELS = {
    "queued": "Queued",
    "starting": "Starting",
    "working": "Working",
    "quiet-active": "Active, awaiting progress",
    "stalled": "Needs attention",
    "recovering": "Recovering",
    "completed": "Session ended",
    "failed": "Session failed",
    "unsupported": "Activity unavailable",
}
FRESHNESS_LABELS = {
    "fresh": "Current",
    "stale": "Last known",
    "stalled": "Update overdue",
    "unavailable": "Unavailable",
    "unknown": "Unknown",
}
PROGRESS_FRESHNESS_LABELS = {
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
OUTCOME_LABELS = {
    "success": "Successful",
    "successful": "Successful",
    "completed": "Completed",
    "needs_evaluation": "Not evaluated",
    "needs_followup": "Follow-up needed",
    "followup_needed": "Follow-up needed",
    "blocked": "Blocked",
    "failed": "Failed",
    "cancelled": "Cancelled",
    "none": "No outcome yet",
}


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else value


def _age_seconds(value: Any) -> int | None:
    try:
        observed = datetime.fromisoformat(str(value))
        return max(0, int((datetime.now(UTC) - observed).total_seconds()))
    except TypeError, ValueError:
        return None


def _time_score(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value)).timestamp()
    except TypeError, ValueError:
        return 0.0


def _label(value: Any, labels: dict[str, str], fallback: str) -> str:
    key = str(value or "")
    if not key:
        return fallback
    return labels.get(key, key.replace("_", " ").replace("-", " ").capitalize())


def _dispatch_is_current(dispatch: dict[str, Any] | None) -> bool:
    if not dispatch:
        return False
    state = str(dispatch.get("effective_state") or dispatch.get("state") or "")
    return bool(state) and (
        state in CURRENT_DISPATCH_STATES or state not in TERMINAL_DISPATCH_STATES
    )


def _dispatch_sort_key(dispatch: dict[str, Any]) -> tuple[int, str]:
    return (
        1 if _dispatch_is_current(dispatch) else 0,
        str(dispatch.get("updated_at") or dispatch.get("created_at") or ""),
    )


def _session_rank_key(session: dict[str, Any]) -> tuple[int, float, str]:
    status = str(session.get("status") or "")
    return (
        0
        if status in {"working", "prompting"}
        else 1
        if status in {"failed", "recoverable", "deferred"}
        else 2,
        -_time_score(session.get("updated_at")),
        str(session.get("id") or ""),
    )


def _reservation_rank_key(dispatch: dict[str, Any]) -> tuple[int, float, str]:
    state = str(dispatch.get("state") or "")
    return (
        0 if state == "blocked" else 1,
        -_time_score(dispatch.get("updated_at") or dispatch.get("created_at")),
        str(dispatch.get("dispatch_id") or ""),
    )


def _worker_rank_key(worker: dict[str, Any]) -> tuple[int, float, str]:
    state = str(worker.get("state") or "")
    return (
        0
        if state == "working"
        else 1
        if state in {"stalled", "recovering", "failed"}
        else 2
        if state in {"queued", "starting"}
        else 3
        if state == "quiet-active"
        else 4,
        -_time_score(worker.get("elapsed_from")),
        str(worker.get("id") or ""),
    )


def _evidence_text(value: Any) -> str | None:
    if isinstance(value, dict):
        value = next(
            (
                value.get(key)
                for key in ("summary", "message", "reason", "error", "code")
                if value.get(key)
            ),
            None,
        )
    text = str(value or "").strip()
    return text[:500] if text else None


def _append_evidence(
    details: list[dict[str, str]], axis: str, code: str, value: Any
) -> None:
    summary = _evidence_text(value)
    if not summary:
        return
    candidate = {"axis": axis, "code": code, "summary": summary}
    if candidate not in details:
        details.append(candidate)


def _sessions_for_realm(
    sessions: list[dict[str, Any]], realm_id: str, counts: dict[str, int]
) -> Iterator[dict[str, Any]]:
    for session in sessions:
        session_realm = session.get("realm_id")
        if not session_realm:
            counts["unknown"] += 1
            continue
        if session_realm != realm_id:
            counts["other"] += 1
            continue
        counts["eligible"] += 1
        yield session


def _reservations_for_realm(
    dispatches: list[dict[str, Any]], realm_id: str, counts: dict[str, int]
) -> Iterator[dict[str, Any]]:
    for dispatch in dispatches:
        dispatch_realm = dispatch.get("realm_id")
        if not dispatch_realm:
            counts["unknown"] += 1
            continue
        if dispatch_realm != realm_id:
            counts["other"] += 1
            continue
        if (
            dispatch.get("session_id")
            or dispatch.get("state") not in PRE_SESSION_DISPATCH_STATES
        ):
            continue
        counts["eligible"] += 1
        yield dispatch


def _worker_state(session: dict[str, Any], dispatch: dict[str, Any] | None) -> str:
    status = str(session.get("status") or "")
    if status == "queued":
        return "queued"
    if status in {"deferred", "recoverable"}:
        return "recovering"
    if status in {"working", "prompting"}:
        return "working"
    if status in {"active", "idle", "connected"}:
        return "quiet-active"
    if status in {"failed", "cancelled"}:
        return "failed"
    if status in {"completed", "closed", "ended"}:
        return "completed"
    if dispatch:
        state = str(dispatch.get("state") or "")
        freshness = ((dispatch.get("progress") or {}).get("freshness") or {}).get(
            "state"
        )
        latest = (dispatch.get("progress") or {}).get("latest") or {}
        phase = str(latest.get("phase") or "")
        if state in STARTING_DISPATCH_STATES:
            return "starting"
        if state == "waiting_capacity":
            return "queued"
        if state == "blocked":
            return "stalled"
        if state in {"failed", "cancelled"}:
            return "failed"
        if state in TERMINAL_DISPATCH_STATES:
            return "completed"
        if phase == "blocked":
            return "stalled"
        if freshness in {"stalled", "stale"}:
            return "stalled"
        if latest:
            return "working"
        if not (dispatch.get("progress") or {}).get("schema_version"):
            return "unsupported"
    return "quiet-active"


def _tool_category(dispatch: dict[str, Any] | None) -> str | None:
    latest = ((dispatch or {}).get("progress") or {}).get("latest") or {}
    category = latest.get("tool_category")
    if category in TOOL_CATEGORIES:
        return str(category)
    phase = str(latest.get("phase") or "")
    return phase if phase in TOOL_CATEGORIES else None


def build_workshop_snapshot(
    ctx: Any,
    overview: dict[str, Any],
    *,
    recent_done_limit: int = 12,
) -> dict[str, Any]:
    """Derive a bounded Workshop model from canonical server projections."""
    realm_id = ctx.settings.primary_realm
    try:
        cards = list(
            ctx.store.list_cards(realm_id=realm_id, limit=WORKSHOP_CARD_READ_LIMIT)
        )
    except TypeError:  # Compatibility for small in-memory test/read stores.
        cards = list(ctx.store.list_cards(realm_id=realm_id))[:WORKSHOP_CARD_READ_LIMIT]
    card_by_id = {card.id: card for card in cards}
    projects = {
        project.id: project for project in ctx.store.list_projects(realm_id=realm_id)
    }

    def enrich_card(card_id: Any) -> None:
        normalized = str(card_id or "")
        if not normalized or normalized in card_by_id:
            return
        getter = getattr(ctx.store, "get_card", None)
        if not callable(getter):
            return
        try:
            card = getter(normalized, realm_id=realm_id)
        except TypeError:  # Compatibility for small extension/read stores.
            card = getter(normalized)
        if card:
            card_by_id[card.id] = card

    dispatch_store = ctx.services.get("dispatch_store")
    if dispatch_store and hasattr(dispatch_store, "current_card_ids"):
        for card_id in dispatch_store.current_card_ids(
            realm_id=realm_id, limit=WORKSHOP_PROJECTION_LIMIT
        ):
            enrich_card(card_id)
    operational_sessions = [
        session
        for node in overview.get("nodes", [])
        for session in ((node.get("dimensions") or {}).get("activity") or {})
        .get("value", {})
        .get("sessions", [])
        if session.get("realm_id") == realm_id
    ]
    for session in operational_sessions:
        enrich_card(session.get("card_id"))
    cards = list(card_by_id.values())
    session_ids = {
        str(session.get("id")) for session in operational_sessions if session.get("id")
    }
    has_latest_by_card = bool(
        dispatch_store and callable(getattr(dispatch_store, "latest_by_card", None))
    )
    has_latest_by_session = bool(
        dispatch_store and callable(getattr(dispatch_store, "latest_by_session", None))
    )
    if has_latest_by_card:
        latest_records = dispatch_store.latest_by_card(
            set(card_by_id), realm_id=realm_id
        )
        latest_dispatch_by_card = {
            card_id: record.public_dict()
            for card_id, record in latest_records.items()
            if record.realm_id == realm_id
        }
    else:
        latest_dispatch_by_card = {}
    if has_latest_by_session:
        dispatch_by_session = {
            session_id: record.public_dict()
            for session_id, record in dispatch_store.latest_by_session(
                session_ids, realm_id=realm_id
            ).items()
        }
    else:
        dispatch_by_session = {}
    dispatches = []
    if dispatch_store and (
        not has_latest_by_card or (session_ids and not has_latest_by_session)
    ):
        try:
            records = dispatch_store.list(realm_id=realm_id, limit=1000)
        except TypeError:
            records = dispatch_store.list(limit=1000)
        dispatches = [
            record.public_dict()
            for record in records
            if getattr(record, "realm_id", None) == realm_id
        ]
        for item in dispatches:
            card_id = str(item.get("card_id") or "")
            current = latest_dispatch_by_card.get(card_id)
            if card_id and (
                current is None
                or _dispatch_sort_key(item) > _dispatch_sort_key(current)
            ):
                latest_dispatch_by_card[card_id] = item
            session_id = str(item.get("session_id") or "")
            current = dispatch_by_session.get(session_id)
            if session_id and (
                current is None
                or _dispatch_sort_key(item) > _dispatch_sort_key(current)
            ):
                dispatch_by_session[session_id] = item
    dispatches_by_card: dict[str, list[dict[str, Any]]] = {}
    for item in dispatches:
        card_id = item.get("card_id")
        if card_id:
            dispatches_by_card.setdefault(str(card_id), []).append(item)
    history_counts = (
        dispatch_store.history_counts(set(card_by_id), realm_id=realm_id)
        if dispatch_store and hasattr(dispatch_store, "history_counts")
        else {card_id: len(items) for card_id, items in dispatches_by_card.items()}
    )

    watches_by_card: dict[str, list[dict[str, Any]]] = {}
    supervisor = ctx.services.get("pr_supervisor_store")
    if supervisor:
        if hasattr(supervisor, "list_watches_for_cards"):
            watches = supervisor.list_watches_for_cards(
                set(card_by_id),
                realm_id=realm_id,
                include_retired=True,
                per_card_limit=5,
            )
        else:
            try:
                all_watches = supervisor.list_watches(
                    realm_id=realm_id, include_retired=True
                )
            except TypeError:
                all_watches = supervisor.list_watches(include_retired=True)
            watches = [watch for watch in all_watches if watch.card_id in card_by_id][
                : WORKSHOP_PROJECTION_LIMIT * 5
            ]
        for watch in watches:
            if not watch.card_id:
                continue
            watches_by_card.setdefault(watch.card_id, []).append(
                {
                    "id": watch.id,
                    "repository": watch.repository,
                    "pr_number": watch.pr_number,
                    "status": watch.status.value,
                    "head_sha": watch.head_sha,
                    "url": watch.pr_url,
                }
            )

    def card_payload(card: Any) -> dict[str, Any]:
        dispatch = latest_dispatch_by_card.get(card.id)
        project = projects.get(card.project_id)
        progress = (dispatch or {}).get("progress") or {}
        latest = progress.get("latest") or {}
        blockers = latest.get("blockers") or []
        materialization = (dispatch or {}).get("materialization_plan") or {}
        workspace = materialization.get("workspace") or {}
        repositories = materialization.get("repositories") or []
        branch = workspace.get("branch") or next(
            (
                repository.get("branch")
                for repository in repositories
                if isinstance(repository, dict) and repository.get("branch")
            ),
            None,
        )
        dispatch_state = (dispatch or {}).get("effective_state") or (
            dispatch or {}
        ).get("state")
        current_dispatch = _dispatch_is_current(dispatch)
        exclusive_dispatch = current_dispatch and not bool(
            (dispatch or {}).get("allow_concurrent")
        )
        can_dispatch = card.lane != CardLane.DONE and not exclusive_dispatch
        dispatch_reason = None
        if card.lane == CardLane.DONE:
            dispatch_reason = "Done cards are not dispatchable."
        elif exclusive_dispatch:
            dispatch_reason = (
                "A current exclusive dispatch already owns this work order."
            )
        evaluated_outcome = (dispatch or {}).get("evaluated_outcome")
        agent_turn = (dispatch or {}).get("agent_turn") or {
            "ended": False,
            "completed": False,
            "stop_reason": None,
        }
        dispatch_completion = (dispatch or {}).get("dispatch_completion") or {
            "completed": dispatch_state in {"completed", "acknowledged"},
            "acknowledged_at": None,
        }
        card_completion = (dispatch or {}).get("card_completion") or {
            "status": "not_requested",
            "lane_before": None,
            "lane_after": None,
            "reason": None,
            "extraction_error": None,
        }
        card_reconciliation = (dispatch or {}).get("card_reconciliation") or {
            "state": "not_requested",
            "reason": None,
        }
        completion_delivery = (dispatch or {}).get("completion_outbox") or {
            "pending": dispatch_state == "completion_pending",
            "last_error": None,
            "classification": None,
        }
        progress_freshness = progress.get("freshness") or {}
        attention_evidence: list[dict[str, str]] = []

        for blocker in blockers[:5]:
            _append_evidence(attention_evidence, "progress", "blocker", blocker)
        _append_evidence(
            attention_evidence,
            "dispatch",
            "last_error",
            (dispatch or {}).get("last_error"),
        )
        _append_evidence(
            attention_evidence,
            "dispatch",
            "error_code",
            (dispatch or {}).get("error_code"),
        )
        _append_evidence(
            attention_evidence,
            "completion_delivery",
            "last_error",
            completion_delivery.get("last_error"),
        )
        if completion_delivery.get("pending") and completion_delivery.get(
            "classification"
        ) not in {None, "pending", "acknowledged"}:
            _append_evidence(
                attention_evidence,
                "completion_delivery",
                "classification",
                completion_delivery.get("classification"),
            )
        _append_evidence(
            attention_evidence,
            "progress",
            "delivery_error",
            progress.get("delivery_error"),
        )
        progress_state = str(progress_freshness.get("state") or "")
        if progress_state in {"delayed", "stale", "stalled", "disconnected"}:
            _append_evidence(
                attention_evidence,
                "progress",
                f"freshness_{progress_state}",
                f"Structured progress is {_label(progress_state, {}, progress_state).lower()}",
            )
        completion_status = str(card_completion.get("status") or "")
        _append_evidence(
            attention_evidence,
            "card_disposition",
            "extraction_error",
            card_completion.get("extraction_error"),
        )
        if completion_status not in {"applied", "completed", "acknowledged", "valid"}:
            _append_evidence(
                attention_evidence,
                "card_disposition",
                "reason",
                card_completion.get("reason"),
            )
        reconciliation_state = str(card_reconciliation.get("state") or "")
        if reconciliation_state in {"pending", "running", "retrying", "blocked"}:
            for key in (
                "reason",
                "disposition_error",
                "last_dependency_error",
                "condition",
                "recovery_action",
            ):
                _append_evidence(
                    attention_evidence,
                    "card_reconciliation",
                    key,
                    card_reconciliation.get(key),
                )
        return {
            "id": card.id,
            "title": card.title,
            "lane": card.lane.value,
            "project": (
                {"id": project.id, "title": project.title} if project else None
            ),
            "preferred_instance": card.preferred_instance,
            "updated_at": card.updated_at.isoformat(),
            "dispatch_id": (dispatch or {}).get("dispatch_id"),
            "session_id": (dispatch or {}).get("session_id"),
            "dispatch_state": dispatch_state,
            "dispatch_label": _label(dispatch_state, DISPATCH_LABELS, "Not dispatched"),
            "dispatch_current": current_dispatch,
            "dispatch_exclusive": exclusive_dispatch,
            "can_dispatch": can_dispatch,
            "dispatch_unavailable_reason": dispatch_reason,
            "progress_freshness": progress_state or None,
            "progress_freshness_label": _label(
                progress_state, PROGRESS_FRESHNESS_LABELS, "No progress signal"
            ),
            "progress_last_activity_at": progress_freshness.get("last_activity_at"),
            "progress_age_seconds": progress_freshness.get("age_seconds"),
            "progress_delivery_error": progress.get("delivery_error"),
            "target_instance_id": (dispatch or {}).get("target_instance_id"),
            "blockers": [
                str(item.get("summary") if isinstance(item, dict) else item)[:160]
                for item in blockers[:5]
            ],
            "branch": branch,
            "pull_requests": watches_by_card.get(card.id, []),
            "evaluated_outcome": evaluated_outcome,
            "outcome_label": _label(
                evaluated_outcome, OUTCOME_LABELS, "No outcome yet"
            ),
            "historical_dispatch_count": history_counts.get(card.id, 0),
            "agent_turn": agent_turn,
            "dispatch_completion": dispatch_completion,
            "card_completion": card_completion,
            "card_reconciliation": card_reconciliation,
            "dispatch_error": {
                "message": (dispatch or {}).get("last_error"),
                "code": (dispatch or {}).get("error_code"),
            },
            "completion_delivery": completion_delivery,
            "attention_evidence": attention_evidence,
            "href": f"/?card={card.id}",
        }

    card_payloads = {card.id: card_payload(card) for card in cards}
    bays = []
    reported_session_total = 0
    reported_session_omitted = 0
    eligible_reservation_total = 0
    unknown_realm_sessions = 0
    unknown_realm_dispatches = 0
    other_realm_sessions = 0
    other_realm_dispatches = 0
    for node in overview.get("nodes", []):
        dimensions = node.get("dimensions") or {}
        reachability = dimensions.get("reachability") or {}
        activity_field = dimensions.get("activity") or {}
        activity = activity_field.get("value") or {}
        activity_state = activity_field.get("state") or "unavailable"
        capacity = activity.get("capacity") or {}
        providers = (dimensions.get("providers") or {}).get("value") or []
        workers = []
        raw_sessions = activity.get("sessions") or []
        reported_session_total += int(
            activity.get("session_total")
            if activity.get("session_total") is not None
            else len(raw_sessions)
        )
        reported_session_omitted += int(activity.get("session_omitted") or 0)
        session_counts = {"eligible": 0, "unknown": 0, "other": 0}
        ranked_sessions = nsmallest(
            WORKSHOP_WORKER_LIMIT,
            _sessions_for_realm(raw_sessions, realm_id, session_counts),
            key=_session_rank_key,
        )
        unknown_realm_sessions += session_counts["unknown"]
        other_realm_sessions += session_counts["other"]
        for session in ranked_sessions:
            session_id = str(session.get("id") or "")
            if not session_id:
                continue
            dispatch = dispatch_by_session.get(session_id)
            card_id = session.get("card_id") or (dispatch or {}).get("card_id")
            state = _worker_state(session, dispatch)
            if activity_state != "fresh" and state in {"working", "quiet-active"}:
                state = "stalled"
            dispatch_progress = (dispatch or {}).get("progress") or {}
            dispatch_progress_freshness = dispatch_progress.get("freshness") or {}
            progress_state = str(dispatch_progress_freshness.get("state") or "")
            workers.append(
                {
                    "id": session_id,
                    "title": session.get("title") or session_id,
                    "state": state
                    if state in SUPPORTED_WORKER_STATES
                    else "unsupported",
                    "state_label": _label(
                        state, ACTIVITY_LABELS, "Activity unavailable"
                    ),
                    "provider": session.get("provider"),
                    "connected": bool(session.get("connected")),
                    "card": card_payloads.get(card_id),
                    "dispatch_id": (dispatch or {}).get("dispatch_id"),
                    "elapsed_from": (dispatch or {}).get("created_at")
                    or session.get("updated_at"),
                    "latest_progress": (dispatch_progress.get("latest") or {}).get(
                        "summary"
                    ),
                    "progress_freshness": progress_state or None,
                    "progress_freshness_label": _label(
                        progress_state,
                        PROGRESS_FRESHNESS_LABELS,
                        "No progress signal",
                    ),
                    "progress_last_activity_at": dispatch_progress_freshness.get(
                        "last_activity_at"
                    ),
                    "progress_age_seconds": dispatch_progress_freshness.get(
                        "age_seconds"
                    ),
                    "tool_category": _tool_category(dispatch),
                    "href": f"/agent?session={session_id}&instance={node.get('id')}",
                    "relationship_kind": "session",
                    "live": activity_state == "fresh" and state == "working",
                    "freshness": activity_state,
                    "freshness_label": _label(
                        activity_state, FRESHNESS_LABELS, "Unknown"
                    ),
                }
            )
        # Durable dispatch admission is visible even before a session exists.
        dispatch_counts = {"eligible": 0, "unknown": 0, "other": 0}
        ranked_dispatches = nsmallest(
            WORKSHOP_WORKER_LIMIT,
            _reservations_for_realm(
                activity.get("dispatches") or [], realm_id, dispatch_counts
            ),
            key=_reservation_rank_key,
        )
        eligible_reservation_total += dispatch_counts["eligible"]
        unknown_realm_dispatches += dispatch_counts["unknown"]
        other_realm_dispatches += dispatch_counts["other"]
        for dispatch in ranked_dispatches:
            worker_id = f"dispatch:{dispatch.get('dispatch_id')}"
            card_id = dispatch.get("card_id")
            dispatch_state = str(dispatch.get("state") or "")
            queue = dispatch.get("queue") or {}
            reason = (
                queue.get("reason")
                or dispatch.get("queue_wait_reason")
                or dispatch.get("last_error")
                or queue.get("blocked_code")
                or dispatch.get("queue_blocked_code")
            )
            worker_state = (
                "queued"
                if dispatch_state == "waiting_capacity"
                else "stalled"
                if dispatch_state == "blocked"
                else "starting"
            )
            workers.append(
                {
                    "id": worker_id,
                    "title": "Reserved worker",
                    "state": worker_state,
                    "state_label": _label(
                        worker_state, ACTIVITY_LABELS, "Preparing work"
                    ),
                    "provider": dispatch.get("capacity_provider"),
                    "connected": False,
                    "card": card_payloads.get(card_id),
                    "dispatch_id": dispatch.get("dispatch_id"),
                    "elapsed_from": dispatch.get("created_at"),
                    "latest_progress": reason,
                    "queue_reason": reason,
                    "queue_position": queue.get("position")
                    or dispatch.get("queue_position"),
                    "tool_category": None,
                    "href": None,
                    "relationship_kind": "reservation",
                    "live": False,
                    "freshness": activity_state,
                    "freshness_label": _label(
                        activity_state, FRESHNESS_LABELS, "Unknown"
                    ),
                }
            )
        reach_value = reachability.get("value") or {}
        bays.append(
            {
                "id": node.get("id"),
                "name": node.get("name") or node.get("id"),
                "url": node.get("url"),
                "zone": node.get("zone"),
                "local": bool(node.get("local")),
                "health": reach_value.get("health") or "unknown",
                "freshness": reachability.get("state") or "unavailable",
                "freshness_label": _label(
                    reachability.get("state"), FRESHNESS_LABELS, "Unavailable"
                ),
                "observed_at": reachability.get("observed_at"),
                "activity_freshness": activity_state,
                "activity_freshness_label": _label(
                    activity_state, FRESHNESS_LABELS, "Unavailable"
                ),
                "activity_observed_at": activity_field.get("observed_at"),
                "activity_age_seconds": _age_seconds(activity_field.get("observed_at")),
                "connectivity": (
                    "connected"
                    if reach_value.get("health") == "up"
                    and reachability.get("state") in {"fresh", "stale"}
                    else "disconnected"
                ),
                "connectivity_label": (
                    "Connected"
                    if reach_value.get("health") == "up"
                    and reachability.get("state") in {"fresh", "stale"}
                    else "Disconnected"
                ),
                "capacity": {
                    "consumed": capacity.get("consumed"),
                    "limit": capacity.get("limit") or node.get("dispatch_capacity"),
                    "source": capacity.get("source"),
                    "queued_prompts": int(activity.get("queued_prompts") or 0),
                },
                "active": len(
                    [worker for worker in workers if worker["state"] == "working"]
                ),
                "queued": len(
                    [
                        worker
                        for worker in workers
                        if worker["state"] in {"queued", "starting"}
                    ]
                ),
                "providers": [
                    {
                        "id": provider.get("id"),
                        "name": provider.get("display_name") or provider.get("id"),
                        "auth_state": provider.get("auth_state") or "unknown",
                    }
                    for provider in providers
                    if isinstance(provider, dict)
                ],
                "workers": workers,
            }
        )

    worker_refs: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    for bay in bays:
        for worker in bay["workers"]:
            current = worker_refs.get(worker["id"])
            if current is None or _worker_rank_key(worker) < _worker_rank_key(
                current[1]
            ):
                worker_refs[worker["id"]] = (bay, worker)
    ranked_worker_refs = nsmallest(
        WORKSHOP_WORKER_LIMIT,
        worker_refs.values(),
        key=lambda pair: _worker_rank_key(pair[1]),
    )
    selected_workers = {id(worker) for _, worker in ranked_worker_refs}
    for bay in bays:
        bay["workers"] = [
            worker for worker in bay["workers"] if id(worker) in selected_workers
        ]
        bay["active"] = sum(worker["state"] == "working" for worker in bay["workers"])
        bay["queued"] = sum(
            worker["state"] in {"queued", "starting"} for worker in bay["workers"]
        )

    worker_by_card: dict[str, tuple[dict[str, Any], dict[str, Any]]] = {}
    orphan_workers: list[dict[str, Any]] = []
    for bay in bays:
        for worker in bay["workers"]:
            card_id = (worker.get("card") or {}).get("id")
            if card_id:
                current = worker_by_card.get(card_id)
                candidate_score = (
                    worker.get("state")
                    in {"working", "quiet-active", "starting", "queued"},
                    bool(worker.get("live")),
                    str(worker.get("elapsed_from") or ""),
                )
                current_score = (
                    (
                        (
                            current[1].get("state")
                            in {"working", "quiet-active", "starting", "queued"}
                        ),
                        bool(current[1].get("live")),
                        str(current[1].get("elapsed_from") or ""),
                    )
                    if current
                    else None
                )
                if current is None or candidate_score > current_score:
                    worker_by_card[card_id] = (bay, worker)
            else:
                orphan_workers.append({"bay": bay, "worker": worker})

    work_orders = []
    for card in sorted(cards, key=lambda item: item.updated_at, reverse=True):
        payload = card_payloads[card.id]
        linked = worker_by_card.get(card.id)
        bay = linked[0] if linked else None
        worker = linked[1] if linked else None
        session_worker = (
            worker if worker and worker.get("relationship_kind") == "session" else None
        )
        reservation_worker = (
            worker
            if worker and worker.get("relationship_kind") == "reservation"
            else None
        )
        activity_state = session_worker.get("state") if session_worker else None
        freshness = (
            session_worker.get("freshness")
            if session_worker
            else payload["progress_freshness"]
        )
        progress_freshness = (
            session_worker.get("progress_freshness")
            if session_worker and session_worker.get("progress_freshness")
            else payload["progress_freshness"]
        )
        progress_freshness_label = (
            session_worker.get("progress_freshness_label")
            if session_worker and session_worker.get("progress_freshness")
            else payload["progress_freshness_label"]
        )
        progress_last_activity_at = (
            session_worker.get("progress_last_activity_at")
            if session_worker and session_worker.get("progress_freshness")
            else payload["progress_last_activity_at"]
        )
        progress_age_seconds = (
            session_worker.get("progress_age_seconds")
            if session_worker and session_worker.get("progress_freshness")
            else payload["progress_age_seconds"]
        )
        live = bool(
            session_worker
            and session_worker.get("live")
            and activity_state not in {"completed", "failed"}
        )
        attention_details = list(payload["attention_evidence"])

        if payload["dispatch_state"] in {"blocked", "failed"}:
            _append_evidence(
                attention_details, "dispatch", "state", payload["dispatch_label"]
            )
        if not session_worker and payload["dispatch_state"] in {
            "waiting_capacity",
            "blocked",
            "completion_pending",
        }:
            _append_evidence(
                attention_details, "dispatch", "state", payload["dispatch_label"]
            )
        reconciliation_state = str(
            (payload.get("card_reconciliation") or {}).get("state") or ""
        )
        if reconciliation_state in {"pending", "running", "retrying", "blocked"}:
            _append_evidence(
                attention_details,
                "card_reconciliation",
                "state",
                _label(reconciliation_state, {}, "Card reconciliation needs attention"),
            )
        if activity_state in {"stalled", "failed", "recovering"}:
            _append_evidence(
                attention_details,
                "session",
                "activity_state",
                worker.get("state_label") or ACTIVITY_LABELS.get(activity_state),
            )
        if card.lane == CardLane.WAITING:
            _append_evidence(
                attention_details, "card", "lane_waiting", "Card is waiting"
            )
        if card.lane == CardLane.ACTIVE and not payload["dispatch_current"]:
            _append_evidence(
                attention_details,
                "dispatch",
                "missing_current",
                "Active card has no current dispatch",
            )
        if reservation_worker:
            _append_evidence(
                attention_details,
                "reservation",
                "state",
                reservation_worker.get("state_label") or "Dispatch is preparing",
            )
            _append_evidence(
                attention_details,
                "reservation",
                "reason",
                reservation_worker.get("queue_reason")
                or reservation_worker.get("latest_progress"),
            )
        attention_reasons = list(
            dict.fromkeys(detail["summary"] for detail in attention_details)
        )
        relationship_label = None
        if session_worker:
            relationship_label = (
                "Linked session"
                if str(session_worker.get("title") or "").strip().casefold()
                == str(card.title).strip().casefold()
                else f"Session: {session_worker.get('title') or session_worker.get('id')}"
            )
        work_orders.append(
            {
                "id": card.id,
                "title": card.title,
                "card": payload,
                "lane": card.lane.value,
                "lane_label": LANE_LABELS[card.lane.value],
                "dispatch_state": payload["dispatch_state"],
                "dispatch_label": payload["dispatch_label"],
                "dispatch_current": payload["dispatch_current"],
                "activity_state": activity_state,
                "activity_label": (
                    session_worker.get("state_label")
                    if session_worker
                    else "No current session"
                ),
                "freshness": freshness,
                "freshness_label": (
                    session_worker.get("freshness_label")
                    if session_worker
                    else _label(freshness, FRESHNESS_LABELS, "No session signal")
                ),
                "progress_freshness": progress_freshness,
                "progress_freshness_label": progress_freshness_label,
                "progress_last_activity_at": progress_last_activity_at,
                "progress_age_seconds": progress_age_seconds,
                "progress_delivery_error": payload["progress_delivery_error"],
                "evaluated_outcome": payload["evaluated_outcome"],
                "outcome_label": payload["outcome_label"],
                "agent_turn": payload["agent_turn"],
                "dispatch_completion": payload["dispatch_completion"],
                "card_completion": payload["card_completion"],
                "card_reconciliation": payload["card_reconciliation"],
                "dispatch_error": payload["dispatch_error"],
                "completion_delivery": payload["completion_delivery"],
                "session": (
                    {
                        "id": session_worker["id"],
                        "title": session_worker["title"],
                        "relationship_label": relationship_label,
                        "href": session_worker["href"],
                        "provider": session_worker.get("provider"),
                        "connected": session_worker.get("connected"),
                        "latest_progress": session_worker.get("latest_progress"),
                        "tool_category": session_worker.get("tool_category"),
                    }
                    if session_worker
                    else None
                ),
                "reservation": (
                    {
                        "id": reservation_worker["id"],
                        "dispatch_id": reservation_worker.get("dispatch_id"),
                        "relationship_kind": "reservation",
                        "label": "Dispatch reservation",
                        "state": reservation_worker.get("state"),
                        "state_label": reservation_worker.get("state_label"),
                        "reason": reservation_worker.get("queue_reason")
                        or reservation_worker.get("latest_progress"),
                        "queue_position": reservation_worker.get("queue_position"),
                    }
                    if reservation_worker
                    else None
                ),
                "location": (
                    {
                        "id": bay["id"],
                        "name": bay["name"],
                        "href": f"/fleet?instance={bay['id']}",
                    }
                    if bay
                    else None
                ),
                "live": live,
                "attention": bool(attention_details),
                "attention_reasons": attention_reasons,
                "attention_details": attention_details,
                "updated_at": payload["updated_at"],
            }
        )

    lane_cards: dict[str, list[dict[str, Any]]] = {}
    for lane in CardLane:
        lane_items = sorted(
            (card for card in cards if card.lane == lane),
            key=lambda card: card.updated_at,
            reverse=True,
        )
        if lane == CardLane.DONE:
            lane_items = lane_items[:recent_done_limit]
        lane_cards[lane.value] = [
            card_payloads[card.id] for card in lane_items[:WORKSHOP_AREA_LIMIT]
        ]

    sync_nodes = []
    sync_issues = []
    for node in overview.get("nodes", []):
        sync = (node.get("dimensions") or {}).get("sync") or {}
        value = sync.get("value") or {}
        freshness = sync.get("state") or "unavailable"
        convergence = value.get("convergence") or {}
        conflicts = value.get("conflicts") or convergence.get("conflicts") or []
        offline_peers = value.get("offline_peers") or [
            item
            for item in convergence.get("instances") or []
            if item.get("status") not in {"reachable", "converged"}
        ]
        durable_head = value.get("durable_head") or value.get("head")
        projection_head = value.get("projection_head")
        consistent = value.get("consistent")
        observed_at = sync.get("observed_at")
        phase = str(convergence.get("phase") or "")
        offline_names = [
            str(
                item.get("name")
                or item.get("instance_name")
                or item.get("instance_id")
                or item.get("id")
                or "Unknown peer"
            )
            if isinstance(item, dict)
            else str(item)
            for item in offline_peers
        ]
        reasons = []
        if freshness == "stale":
            reasons.append("Sync observation is out of date")
        elif freshness not in {"fresh"}:
            reasons.append("Sync status is unavailable")
        if consistent is False:
            reasons.append("Durable state and the local view differ")
        if conflicts:
            reasons.append(f"{len(conflicts)} conflict(s) need resolution")
        if offline_names:
            reasons.append("Unavailable peers: " + ", ".join(offline_names))
        if phase and phase not in {"converged", "idle"}:
            reasons.append(_label(phase, {}, "Recovery in progress"))
        if freshness == "stale":
            operational_state = "stale"
        elif freshness != "fresh":
            operational_state = "unavailable"
        elif conflicts:
            operational_state = "conflict"
        elif consistent is False:
            operational_state = "inconsistent"
        elif offline_names:
            operational_state = "offline"
        elif phase and phase not in {"converged", "idle"}:
            operational_state = "recovering"
        else:
            operational_state = "healthy"
        operational_labels = {
            "healthy": "Healthy",
            "stale": "Last known; needs attention",
            "unavailable": "Unavailable",
            "conflict": "Conflicts need attention",
            "inconsistent": "Views differ",
            "offline": "Peer unavailable",
            "recovering": "Recovery in progress",
        }
        node_payload = {
            "instance_id": node.get("id"),
            "name": node.get("name") or node.get("id"),
            "state": operational_state,
            "state_label": operational_labels[operational_state],
            "freshness": freshness,
            "freshness_label": _label(freshness, FRESHNESS_LABELS, "Unavailable"),
            "attention": bool(reasons),
            "reasons": list(dict.fromkeys(reasons)),
            "consistent": consistent,
            "durable_head": durable_head,
            "projection_head": projection_head,
            "conflicts": conflicts,
            "offline_peers": offline_peers,
            "offline_peer_names": offline_names,
            "observed_at": observed_at,
            "age_seconds": _age_seconds(observed_at),
            "recovery_phase": convergence.get("phase"),
            "recovery_attempt": convergence.get("attempt"),
            "recovery_started_at": convergence.get("started_at"),
            "recovery_completed_at": convergence.get("completed_at"),
            "href": f"/fleet?section=sync&instance={node.get('id')}",
        }
        sync_nodes.append(node_payload)
        if reasons:
            sync_issues.append(
                {
                    "instance_id": node_payload["instance_id"],
                    "peer_name": node_payload["name"],
                    "condition": ("historical" if freshness == "stale" else "current"),
                    "condition_label": (
                        "Historical observation"
                        if freshness == "stale"
                        else "Current condition"
                    ),
                    "summary": "; ".join(dict.fromkeys(reasons)),
                    "state": operational_state,
                    "state_label": operational_labels[operational_state],
                    "affected_peer_names": offline_names,
                    "observed_at": observed_at,
                    "age_seconds": node_payload["age_seconds"],
                    "recovery_phase": phase or None,
                    "recovery_label": (
                        "Recovered"
                        if phase == "converged"
                        else _label(phase, {}, "No recovery status")
                    ),
                    "recovery_attempt": convergence.get("attempt"),
                    "href": node_payload["href"],
                }
            )
    degraded = bool(sync_issues)
    control_plane = build_control_plane_status(ctx.settings)
    authority = (
        (control_plane.get("service_authorities") or {})
        .get("pr-supervisor", {})
        .get("authority_instance_id")
    )
    work_orders.sort(
        key=lambda item: (
            bool(item["attention"]),
            bool(item["live"]),
            str(item["updated_at"]),
        ),
        reverse=True,
    )
    work_orders = work_orders[:WORKSHOP_PROJECTION_LIMIT]
    try:
        lane_counts = {
            lane.value: ctx.store.count_cards(realm_id=realm_id, lane=lane)
            for lane in CardLane
        }
    except AttributeError:
        lane_counts = {
            lane.value: sum(1 for card in cards if card.lane == lane)
            for lane in CardLane
        }
    total_cards = sum(lane_counts.values())
    projected_sessions = sum(
        worker.get("relationship_kind") == "session"
        for bay in bays
        for worker in bay["workers"]
    )
    projected_reservations = sum(
        worker.get("relationship_kind") == "reservation"
        for bay in bays
        for worker in bay["workers"]
    )
    omitted_sessions = max(
        reported_session_omitted,
        max(0, reported_session_total - projected_sessions),
    )
    omitted_reservations = max(0, eligible_reservation_total - projected_reservations)
    counts = {
        "total": total_cards,
        "projected": len(work_orders),
        "live": sum(1 for item in work_orders if item["live"]),
        "attention": sum(1 for item in work_orders if item["attention"]),
        "lanes": lane_counts,
        "orphan_sessions": len(orphan_workers),
        "sessions": {
            "reported": reported_session_total,
            "projected": projected_sessions,
            "omitted": omitted_sessions,
        },
        "reservations": projected_reservations,
        "workers": {
            "reported": reported_session_total + eligible_reservation_total,
            "projected": projected_sessions + projected_reservations,
            "omitted": omitted_sessions + omitted_reservations,
        },
        "excluded_activity": {
            "unknown_realm_sessions": unknown_realm_sessions,
            "unknown_realm_dispatches": unknown_realm_dispatches,
            "other_realm_sessions": other_realm_sessions,
            "other_realm_dispatches": other_realm_dispatches,
        },
    }
    return {
        "schema": "pa.workshop/v2",
        "generated_at": datetime.now(UTC).isoformat(),
        "realm_id": ctx.settings.primary_realm,
        "authority": {
            "instance_id": authority,
            "current_instance_id": ctx.settings.instance_id,
            "mode": control_plane.get("mode"),
            "supported": bool(authority),
        },
        "bays": bays,
        "work_orders": work_orders,
        "orphan_sessions": orphan_workers,
        "counts": counts,
        "default_view": {
            "filter": "operational",
            "page_size": 20,
            "description": "Live work and work needing attention",
        },
        "inventory": {
            "loaded": len(work_orders),
            "total": total_cards,
            "omitted": max(0, total_cards - len(work_orders)),
            "overflow_href": f"/?realm={realm_id}",
            "description": (
                "Newest and operational cards are bounded in Workshop; "
                "open Cards for the full inventory."
            ),
        },
        "areas": lane_cards,
        "sync": {
            "state": "degraded" if degraded else "healthy",
            "state_label": "Needs attention" if degraded else "Healthy",
            "nodes": sync_nodes,
            "issues": sync_issues,
            "counts": {
                "total": len(sync_nodes),
                "attention": len(sync_issues),
                "historical": sum(
                    issue["condition"] == "historical" for issue in sync_issues
                ),
            },
            "edges": [
                edge for edge in overview.get("edges", []) if edge.get("kind") == "sync"
            ],
        },
    }


def workshop_semantic_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    """Remove observation clocks that must not turn an SSE heartbeat into a rebuild."""
    volatile = {
        "generated_at",
        "observed_at",
        "age_seconds",
        "activity_observed_at",
        "activity_age_seconds",
    }

    def normalize(value: Any) -> Any:
        if isinstance(value, dict):
            return {
                key: normalize(item)
                for key, item in value.items()
                if key not in volatile
            }
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return deepcopy(value)

    return normalize(snapshot)
