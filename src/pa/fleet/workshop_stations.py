"""Station mapping for the Workshop floor.

The floor shows each agent and work order at the station that matches its real
state. This module is the single, pure place where server-side state is mapped
to a station; the browser applies the same table to live events
(``workshop-floor-model.js``) and both are tested against one shared fixture
(``tests/fixtures/workshop_floor_stations.json``), so they cannot drift apart.

Stations
--------
``intake``        card in the inbox with no dispatch
``queue``         dispatch admission_pending / waiting_capacity / blocked / queued /
                  checking_sync (reservation workers)
``tooling``       materializing / provisioning / starting_session / delivering_prompt
``workbench``     session prompting (inside the instance bay)
``bell``          pending permission / elicitation / operator-input interaction
``gantry``        pull-request watch active or blocked (CI, draft, behind, conflicts)
``verification``  card Waiting on verification_pending / acceptance_pending
``shipping``      card done recently
``bench``         session connected but not prompting (inside the instance bay)
``infirmary``     recovery blocked/restoring, connection lost, stale heartbeat
``offline``       the whole bay is unreachable (greyed; never forced to "stalled")
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

STATIONS = (
    "intake",
    "queue",
    "tooling",
    "workbench",
    "bell",
    "gantry",
    "verification",
    "shipping",
    "bench",
    "infirmary",
    "offline",
)
# Stations that live inside an instance bay; the rest are shared floor areas.
BAY_STATIONS = frozenset({"workbench", "bench", "offline"})
# Where an agent physically carries its work order with it.
CARRY_STATIONS = frozenset(
    {"queue", "tooling", "workbench", "bell", "gantry", "verification", "infirmary", "offline"}
)
QUEUE_DISPATCH_STATES = frozenset(
    {"admission_pending", "waiting_capacity", "blocked", "queued", "checking_sync"}
)
TOOLING_DISPATCH_STATES = frozenset(
    {"materializing", "provisioning", "starting_session", "delivering_prompt"}
)
VERIFICATION_STATES = frozenset({"verification_pending", "acceptance_pending"})
GANTRY_WATCH_STATUSES = frozenset({"active", "blocked"})
SHIPPING_RECENT_SECONDS = 30 * 60
_POSES = {
    "coding": "edit",
    "testing": "test",
    "browser": "browse",
    "research": "read",
    "review": "read",
}


def pose_for_tool_category(category: Any) -> str | None:
    """Pose an agent strikes at the workbench for a dispatch tool category."""
    return _POSES.get(str(category or ""))


def gate_reasons(gate: dict[str, Any] | None) -> list[str]:
    """Why a pull request is parked at the gantry, from the compact gate summary."""
    gate = gate or {}
    checks = gate.get("checks") or {}
    reasons: list[str] = []
    if checks.get("failing"):
        reasons.append("ci_failing")
    elif checks.get("pending"):
        reasons.append("ci_pending")
    if gate.get("draft"):
        reasons.append("draft")
    mergeable = str(gate.get("mergeable_state") or "").lower()
    if mergeable == "behind":
        reasons.append("behind")
    elif mergeable == "dirty":
        reasons.append("conflicts")
    if str(gate.get("review_decision") or "").upper() in {
        "REVIEW_REQUIRED",
        "CHANGES_REQUESTED",
    }:
        reasons.append("review_required")
    return reasons


def active_gate(pull_requests: list[dict[str, Any]] | None) -> dict[str, Any] | None:
    """The compact summary of the first pull request still being watched."""
    for watch in pull_requests or []:
        if (
            isinstance(watch, dict)
            and not watch.get("retired_at")
            and str(watch.get("status") or "") in GANTRY_WATCH_STATUSES
        ):
            return {
                "watch_id": watch.get("id"),
                "pr_number": watch.get("pr_number"),
                "repository": watch.get("repository"),
                "status": watch.get("status"),
                "reasons": gate_reasons(watch.get("gate")),
            }
    return None


def reservation_station(dispatch_state: Any) -> str | None:
    state = str(dispatch_state or "")
    if state in QUEUE_DISPATCH_STATES:
        return "queue"
    if state in TOOLING_DISPATCH_STATES:
        return "tooling"
    return None


def worker_station(
    *,
    relationship: str,
    bay_connected: bool,
    state: str | None = None,
    dispatch_state: str | None = None,
    interaction: dict[str, Any] | None = None,
    recovery: str | None = None,
    connection: str | None = None,
    verification: bool = False,
    gantry: bool = False,
) -> str | None:
    """Station of one agent (a live session or a pre-session reservation)."""
    if not bay_connected:
        return "offline"
    if relationship == "reservation":
        return reservation_station(dispatch_state)
    if state in {"completed", "unsupported"}:
        return None
    if interaction:
        return "bell"
    if (
        recovery
        or state in {"recovering", "failed", "stalled"}
        or connection == "disconnected"
    ):
        return "infirmary"
    if state == "working":
        return "workbench"
    if verification:
        return "verification"
    if gantry:
        return "gantry"
    return "bench"


def card_station(
    *,
    lane: str,
    has_dispatch: bool,
    has_session: bool,
    presentation_state: str | None,
    gantry: bool,
    updated_at: Any = None,
    now: datetime | None = None,
) -> str | None:
    """Station of a work order that is not carried by a visible agent."""
    if lane == "done":
        return "shipping" if _recent(updated_at, now) else None
    if presentation_state in VERIFICATION_STATES:
        return "verification"
    if gantry:
        return "gantry"
    if lane == "inbox" and not has_dispatch and not has_session:
        return "intake"
    return None


def _recent(value: Any, now: datetime | None) -> bool:
    try:
        moment = datetime.fromisoformat(str(value))
    except TypeError, ValueError:
        return False
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    reference = now or datetime.now(UTC)
    return 0 <= (reference - moment).total_seconds() <= SHIPPING_RECENT_SECONDS
