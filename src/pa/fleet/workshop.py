"""Truthful, presentation-ready projection for the Workshop floor view."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pa.domain.models import CardLane
from pa.fleet.control_plane import build_control_plane_status

TERMINAL_DISPATCH_STATES = {"completed", "acknowledged", "failed", "cancelled"}
STARTING_DISPATCH_STATES = {
    "queued",
    "checking_sync",
    "materializing",
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


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else value


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
    card_by_id = {card.id: card for card in cards}
    dispatch_store = ctx.services.get("dispatch_store")
    dispatches = (
        [record.public_dict() for record in dispatch_store.list(limit=500)]
        if dispatch_store
        else []
    )
    dispatch_by_session = {
        item["session_id"]: item for item in dispatches if item.get("session_id")
    }
    latest_dispatch_by_card: dict[str, dict[str, Any]] = {}
    for item in dispatches:
        card_id = item.get("card_id")
        if card_id and card_id not in latest_dispatch_by_card:
            latest_dispatch_by_card[card_id] = item

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
            "dispatch_state": (dispatch or {}).get("state"),
            "target_instance_id": (dispatch or {}).get("target_instance_id"),
            "blockers": [
                str(item.get("summary") if isinstance(item, dict) else item)[:160]
                for item in blockers[:5]
            ],
            "branch": branch,
            "pull_requests": watches_by_card.get(card.id, []),
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
            workers.append(
                {
                    "id": session_id,
                    "title": session.get("title") or session_id,
                    "state": state if state in SUPPORTED_WORKER_STATES else "unsupported",
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
                    "provider": dispatch.get("capacity_provider"),
                    "connected": False,
                    "card": card_payloads.get(card_id),
                    "dispatch_id": dispatch.get("dispatch_id"),
                    "elapsed_from": dispatch.get("created_at"),
                    "latest_progress": None,
                    "tool_category": None,
                    "href": card_payloads.get(card_id, {}).get("href") or "/fleet",
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
                "observed_at": reachability.get("observed_at"),
                "connectivity": (
                    "connected"
                    if reach_value.get("health") == "up"
                    and reachability.get("state") in {"fresh", "stale"}
                    else "disconnected"
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
    for node in overview.get("nodes", []):
        sync = (node.get("dimensions") or {}).get("sync") or {}
        value = sync.get("value") or {}
        sync_nodes.append(
            {
                "instance_id": node.get("id"),
                "name": node.get("name"),
                "state": sync.get("state") or "unavailable",
                "consistent": value.get("consistent"),
                "durable_head": value.get("durable_head"),
                "projection_head": value.get("projection_head"),
                "conflicts": value.get("conflicts") or [],
                "offline_peers": value.get("offline_peers") or [],
                "observed_at": sync.get("observed_at"),
            }
        )
    degraded = any(
        item["state"] not in {"fresh", "stale"} or item["consistent"] is False
        for item in sync_nodes
    )
    control_plane = build_control_plane_status(ctx.settings)
    authority = (
        (control_plane.get("service_authorities") or {})
        .get("pr-supervisor", {})
        .get("authority_instance_id")
    )
    return {
        "schema": "pa.workshop/v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "realm_id": ctx.settings.primary_realm,
        "authority": {
            "instance_id": authority,
            "current_instance_id": ctx.settings.instance_id,
            "mode": control_plane.get("mode"),
            "supported": bool(authority),
        },
        "bays": bays,
        "areas": lane_cards,
        "sync": {
            "state": "degraded" if degraded else "healthy",
            "nodes": sync_nodes,
            "edges": [
                edge
                for edge in overview.get("edges", [])
                if edge.get("kind") == "sync"
            ],
        },
    }
