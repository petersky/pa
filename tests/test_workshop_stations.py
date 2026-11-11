"""Server-side mapping of real state to Workshop floor stations."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from pa.fleet.workshop import build_workshop_snapshot, workshop_floor_view
from pa.fleet.workshop_stations import (
    STATIONS,
    active_gate,
    card_station,
    gate_reasons,
    worker_station,
)
from tests.test_workshop import _Dispatch, _ctx, _heavy_ctx, _overview

FIXTURE = json.loads(
    (Path(__file__).parent / "fixtures" / "workshop_floor_stations.json").read_text()
)
NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)


def _case_id(case: dict) -> str:
    return case["name"]


@pytest.mark.parametrize("case", FIXTURE["cases"], ids=_case_id)
def test_state_maps_to_station(case: dict) -> None:
    data = dict(case["input"])
    if case["kind"] == "worker":
        station = worker_station(**data)
    else:
        age = data.pop("updated_age_seconds", None)
        station = card_station(
            **data,
            updated_at=(NOW - timedelta(seconds=age)).isoformat() if age is not None else None,
            now=NOW,
        )
    assert station == case["station"]
    assert station is None or station in STATIONS


def test_every_station_is_reachable_from_the_shared_fixture() -> None:
    reached = {case["station"] for case in FIXTURE["cases"]}
    assert set(STATIONS) <= reached


def test_gate_reasons_name_each_parking_reason() -> None:
    assert gate_reasons(
        {
            "draft": True,
            "mergeable_state": "behind",
            "review_decision": "REVIEW_REQUIRED",
            "checks": {"total": 3, "failing": 1, "pending": 1},
        }
    ) == ["ci_failing", "draft", "behind", "review_required"]
    assert gate_reasons({"mergeable_state": "dirty", "checks": {"pending": 2}}) == [
        "ci_pending",
        "conflicts",
    ]
    assert gate_reasons(None) == []


def test_only_active_or_blocked_unretired_watches_open_the_gantry() -> None:
    watch = {"id": "w", "pr_number": 3, "status": "merged", "gate": {}}
    assert active_gate([watch]) is None
    assert active_gate([{**watch, "status": "active", "retired_at": "x"}]) is None
    assert active_gate([{**watch, "status": "blocked"}])["status"] == "blocked"


def _floor(tmp_path, mutate=None):
    ctx = _heavy_ctx(tmp_path)
    overview = _overview()
    if mutate:
        mutate(ctx, overview)
    return workshop_floor_view(build_workshop_snapshot(ctx, overview))


def _worker(floor, worker_id):
    for bay in floor["bays"]:
        for worker in bay["workers"]:
            if worker["id"] == worker_id:
                return worker
    for worker in floor["orphan_sessions"]:
        if worker["id"] == worker_id:
            return worker
    raise AssertionError(worker_id)


def test_floor_projection_places_prompting_agent_at_the_workbench_with_pose(tmp_path):
    def mutate(ctx, overview):
        # The fixture card has a PR watch; a prompting agent still works.
        pass

    floor = _floor(tmp_path, mutate)
    worker = _worker(floor, "session")
    assert worker["station"] == "workbench"
    assert worker["pose"] == "test"  # progress phase "testing"
    assert "tool_category" not in worker
    assert worker["card_id"] == "active"
    order = next(item for item in floor["work_orders"] if item["id"] == "active")
    assert order["station"] is None  # carried by its agent
    assert order["gantry"]["reasons"] == [
        "ci_failing",
        "draft",
        "review_required",
    ]


def test_floor_projection_sends_permission_waits_to_the_bell(tmp_path):
    def mutate(ctx, overview):
        session = overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"][0]
        session["presentation"] = {
            "pending_interaction": {"kind": "permission", "count": 2},
            "connection": {"state": "connected"},
        }

    worker = _worker(_floor(tmp_path, mutate), "session")
    assert worker["station"] == "bell"
    assert worker["interaction"] == {"kind": "permission", "count": 2}


def test_floor_projection_sends_recovery_and_lost_connections_to_the_infirmary(tmp_path):
    def mutate(ctx, overview):
        sessions = overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"]
        sessions[0]["presentation"] = {
            "recovery": {"blocked": True},
            "connection": {"state": "disconnected"},
        }
        sessions.append(
            {
                "id": "lost",
                "realm_id": "default",
                "title": "Lost chat",
                "purpose": "chat",
                "status": "idle",
                "connected": False,
                "presentation": {"connection": {"state": "disconnected"}},
                "updated_at": "2026-07-28T20:01:00+00:00",
            }
        )

    floor = _floor(tmp_path, mutate)
    assert _worker(floor, "session")["station"] == "infirmary"
    assert _worker(floor, "session")["recovery"] == "blocked"
    assert _worker(floor, "lost")["station"] == "infirmary"


def test_floor_projection_greys_an_unreachable_bay_instead_of_stalling_workers(tmp_path):
    def mutate(ctx, overview):
        node = overview["nodes"][0]
        node["last_seen"] = "2026-07-28T19:00:00+00:00"
        node["dimensions"]["reachability"] = {
            "state": "unavailable",
            "value": {"health": "down"},
            "observed_at": "2026-07-28T20:01:00+00:00",
        }

    floor = _floor(tmp_path, mutate)
    bay = floor["bays"][0]
    assert bay["connectivity"] == "disconnected"
    assert bay["unreachable_since"] == "2026-07-28T19:00:00+00:00"
    assert _worker(floor, "session")["station"] == "offline"


def test_reachable_bay_has_no_unreachable_since(tmp_path):
    floor = _floor(tmp_path)
    assert floor["bays"][0]["unreachable_since"] is None


def test_floor_projection_stages_reservations_through_queue_and_tooling(tmp_path):
    def mutate(ctx, overview):
        activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
        activity["sessions"] = []
        activity["dispatches"] = [
            {
                "dispatch_id": f"d-{state}",
                "realm_id": "default",
                "card_id": "inbox",
                "state": state,
                "created_at": "2026-07-28T20:00:00+00:00",
            }
            for state in ("admission_pending", "queued", "provisioning")
        ]

    floor = _floor(tmp_path, mutate)
    stations = {
        worker["dispatch_state"]: worker["station"] for worker in floor["bays"][0]["workers"]
    }
    assert stations == {
        "admission_pending": "queue",
        "queued": "queue",
        "provisioning": "tooling",
    }


def test_floor_projection_places_cards_without_agents(tmp_path):
    ctx = _heavy_ctx(tmp_path)
    ctx.store.cards[1].lane = ctx.store.cards[1].lane  # keep the active card
    done = ctx.store.cards[3]
    done.updated_at = datetime.now(UTC)
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = []
    floor = workshop_floor_view(build_workshop_snapshot(ctx, overview))
    by_id = {order["id"]: order for order in floor["work_orders"]}
    assert by_id["inbox"]["station"] == "intake"
    assert by_id["done"]["station"] == "shipping"
    # An active card with a watched PR but no agent waits at the gantry.
    assert by_id["active"]["station"] == "gantry"


def test_floor_projection_verification_pending_card_waits_at_the_desk(tmp_path):
    snapshot = build_workshop_snapshot(_ctx(), _overview())
    waiting = deepcopy(next(item for item in snapshot["work_orders"] if item["id"] == "waiting"))
    waiting["presentation"]["state"] = "verification_pending"
    snapshot["work_orders"] = [
        waiting if item["id"] == "waiting" else item for item in snapshot["work_orders"]
    ]
    floor = workshop_floor_view(snapshot)
    assert next(i for i in floor["work_orders"] if i["id"] == "waiting")["station"] == "verification"


def test_dispatch_progress_helper_dispatch_fixture_is_importable():
    # Guard the shared test helpers this module reuses.
    assert _Dispatch().public_dict()["state"] == "running"
