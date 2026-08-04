"""Truthful, presentation-ready projection for the Workshop floor view."""

from __future__ import annotations

from datetime import UTC, datetime
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


def _worker_state(session: dict[str, Any], dispatch: dict[str, Any] | None) -> str:
    if dispatch:
        state = str(dispatch.get("state") or "")
        freshness = ((dispatch.get("progress") or {}).get("freshness") or {}).get(
            "state"
        )
        latest = (dispatch.get("progress") or {}).get("latest") or {}
        phase = str(latest.get("phase") or "")
        if state in STARTING_DISPATCH_STATES:
            return "starting"
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
    status = str(session.get("status") or "")
    if status == "queued":
        return "queued"
    if status in {"deferred", "recoverable"}:
        return "recovering"
    if status == "working":
        return "working"
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
    """Derive the complete Workshop model from canonical server projections."""
    cards = list(ctx.store.list_cards(realm_id=ctx.settings.primary_realm))
    projects = {
        project.id: project
        for project in ctx.store.list_projects(realm_id=ctx.settings.primary_realm)
    }
    dispatch_store = ctx.services.get("dispatch_store")
    dispatches = (
        [record.public_dict() for record in dispatch_store.list(limit=500)]
        if dispatch_store
        else []
    )
    dispatch_by_session = {
        item["session_id"]: item for item in dispatches if item.get("session_id")
    }
    dispatches_by_card: dict[str, list[dict[str, Any]]] = {}
    for item in dispatches:
        card_id = item.get("card_id")
        if card_id:
            dispatches_by_card.setdefault(str(card_id), []).append(item)
    latest_dispatch_by_card = {
        card_id: max(items, key=_dispatch_sort_key)
        for card_id, items in dispatches_by_card.items()
    }

    watches_by_card: dict[str, list[dict[str, Any]]] = {}
    supervisor = ctx.services.get("pr_supervisor_store")
    if supervisor:
        for watch in supervisor.list_watches(include_retired=True):
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
        if not evaluated_outcome and dispatch_state in {
            "completed",
            "failed",
            "cancelled",
        }:
            evaluated_outcome = dispatch_state
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
            "progress_freshness": (progress.get("freshness") or {}).get("state"),
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
            "historical_dispatch_count": len(dispatches_by_card.get(card.id, [])),
            "href": f"/?card={card.id}",
        }

    card_payloads = {card.id: card_payload(card) for card in cards}
    bays = []
    seen_workers: set[str] = set()
    for node in overview.get("nodes", []):
        dimensions = node.get("dimensions") or {}
        reachability = dimensions.get("reachability") or {}
        activity_field = dimensions.get("activity") or {}
        activity = activity_field.get("value") or {}
        activity_state = activity_field.get("state") or "unavailable"
        capacity = activity.get("capacity") or {}
        providers = (dimensions.get("providers") or {}).get("value") or []
        workers = []
        for session in activity.get("sessions") or []:
            session_id = str(session.get("id") or "")
            if not session_id or session_id in seen_workers:
                continue
            seen_workers.add(session_id)
            dispatch = dispatch_by_session.get(session_id)
            card_id = session.get("card_id") or (dispatch or {}).get("card_id")
            state = _worker_state(session, dispatch)
            if activity_state != "fresh" and state in {"working", "quiet-active"}:
                state = "stalled"
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
                    "latest_progress": (
                        ((dispatch or {}).get("progress") or {}).get("latest") or {}
                    ).get("summary"),
                    "tool_category": _tool_category(dispatch),
                    "href": f"/agent?session={session_id}&instance={node.get('id')}",
                    "live": activity_state == "fresh",
                    "freshness": activity_state,
                    "freshness_label": _label(
                        activity_state, FRESHNESS_LABELS, "Unknown"
                    ),
                }
            )
        # Durable dispatch admission is visible even before a session exists.
        for dispatch in activity.get("dispatches") or []:
            if (
                dispatch.get("session_id")
                or dispatch.get("state") not in STARTING_DISPATCH_STATES
            ):
                continue
            worker_id = f"dispatch:{dispatch.get('dispatch_id')}"
            if worker_id in seen_workers:
                continue
            seen_workers.add(worker_id)
            card_id = dispatch.get("card_id")
            workers.append(
                {
                    "id": worker_id,
                    "title": "Reserved worker",
                    "state": "starting",
                    "state_label": ACTIVITY_LABELS["starting"],
                    "provider": dispatch.get("capacity_provider"),
                    "connected": False,
                    "card": card_payloads.get(card_id),
                    "dispatch_id": dispatch.get("dispatch_id"),
                    "elapsed_from": dispatch.get("created_at"),
                    "latest_progress": None,
                    "tool_category": None,
                    "href": card_payloads.get(card_id, {}).get("href") or "/fleet",
                    "live": activity_state == "fresh",
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
        activity_state = worker.get("state") if worker else None
        freshness = worker.get("freshness") if worker else payload["progress_freshness"]
        live = bool(
            payload["dispatch_current"]
            or (worker and worker.get("live") and activity_state != "completed")
        )
        attention_reasons = list(payload["blockers"])
        if payload["dispatch_state"] in {"blocked", "failed"}:
            attention_reasons.append(payload["dispatch_label"])
        if activity_state in {"stalled", "failed", "recovering"}:
            attention_reasons.append(
                worker.get("state_label") or ACTIVITY_LABELS.get(activity_state)
            )
        if card.lane == CardLane.WAITING:
            attention_reasons.append("Card is waiting")
        if card.lane == CardLane.ACTIVE and not payload["dispatch_current"]:
            attention_reasons.append("Active card has no current dispatch")
        relationship_label = None
        if worker:
            relationship_label = (
                "Linked session"
                if str(worker.get("title") or "").strip().casefold()
                == str(card.title).strip().casefold()
                else f"Session: {worker.get('title') or worker.get('id')}"
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
                "activity_state": activity_state,
                "activity_label": (
                    worker.get("state_label") if worker else "No current session"
                ),
                "freshness": freshness,
                "freshness_label": (
                    worker.get("freshness_label")
                    if worker
                    else _label(freshness, FRESHNESS_LABELS, "No session signal")
                ),
                "evaluated_outcome": payload["evaluated_outcome"],
                "outcome_label": payload["outcome_label"],
                "session": (
                    {
                        "id": worker["id"],
                        "title": worker["title"],
                        "relationship_label": relationship_label,
                        "href": worker["href"],
                        "provider": worker.get("provider"),
                        "connected": worker.get("connected"),
                        "latest_progress": worker.get("latest_progress"),
                        "tool_category": worker.get("tool_category"),
                    }
                    if worker
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
                "attention": bool(attention_reasons),
                "attention_reasons": list(dict.fromkeys(attention_reasons)),
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
        lane_cards[lane.value] = [card_payloads[card.id] for card in lane_items]

    sync_nodes = []
    sync_issues = []
    for node in overview.get("nodes", []):
        sync = (node.get("dimensions") or {}).get("sync") or {}
        value = sync.get("value") or {}
        state = sync.get("state") or "unavailable"
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
        node_payload = {
            "instance_id": node.get("id"),
            "name": node.get("name") or node.get("id"),
            "state": state,
            "state_label": _label(state, FRESHNESS_LABELS, "Unavailable"),
            "consistent": consistent,
            "durable_head": durable_head,
            "projection_head": projection_head,
            "conflicts": conflicts,
            "offline_peers": offline_peers,
            "observed_at": observed_at,
            "age_seconds": _age_seconds(observed_at),
            "recovery_phase": convergence.get("phase"),
            "recovery_attempt": convergence.get("attempt"),
            "recovery_started_at": convergence.get("started_at"),
            "recovery_completed_at": convergence.get("completed_at"),
            "href": f"/fleet?section=sync&instance={node.get('id')}",
        }
        sync_nodes.append(node_payload)
        reasons = []
        if consistent is False:
            reasons.append("Durable state and the local view differ")
        if conflicts:
            reasons.append(f"{len(conflicts)} conflict(s) need resolution")
        if offline_peers:
            reasons.append(f"{len(offline_peers)} peer connection(s) unavailable")
        phase = str(convergence.get("phase") or "")
        if phase and phase not in {"converged", "idle"}:
            reasons.append(_label(phase, {}, "Recovery in progress"))
        if state not in {"fresh", "stale"}:
            reasons.append("Sync status is unavailable")
        if reasons:
            sync_issues.append(
                {
                    "instance_id": node_payload["instance_id"],
                    "peer_name": node_payload["name"],
                    "condition": "historical" if state == "stale" else "current",
                    "condition_label": (
                        "Historical observation"
                        if state == "stale"
                        else "Current condition"
                    ),
                    "summary": "; ".join(dict.fromkeys(reasons)),
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
    degraded = bool(sync_issues) or any(
        item["state"] not in {"fresh", "stale"} or item["consistent"] is False
        for item in sync_nodes
    )
    control_plane = build_control_plane_status(ctx.settings)
    authority = (
        (control_plane.get("service_authorities") or {})
        .get("pr-supervisor", {})
        .get("authority_instance_id")
    )
    counts = {
        "total": len(work_orders),
        "live": sum(1 for item in work_orders if item["live"]),
        "attention": sum(1 for item in work_orders if item["attention"]),
        "lanes": {
            lane.value: sum(1 for card in cards if card.lane == lane)
            for lane in CardLane
        },
        "orphan_sessions": len(orphan_workers),
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
        "areas": lane_cards,
        "sync": {
            "state": "degraded" if degraded else "healthy",
            "state_label": "Needs attention" if degraded else "Healthy",
            "nodes": sync_nodes,
            "issues": sync_issues,
            "edges": [
                edge for edge in overview.get("edges", []) if edge.get("kind") == "sync"
            ],
        },
    }
