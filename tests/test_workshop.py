from __future__ import annotations

import asyncio
import subprocess
import tempfile
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, Card, CardLane, FleetInstance, Project
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.fleet.workshop import (
    WORKSHOP_PROJECTION_LIMIT,
    WORKSHOP_WORKER_LIMIT,
    build_workshop_snapshot,
)
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import _refresh_workshop_dimensions, _workshop_stream_iteration
from pa.pr_supervisor.models import PRWatch
from pa.pr_supervisor.store import PRSupervisorStore


class _Store:
    def __init__(self):
        self.cards = [
            Card(id="inbox", title="Incoming", lane=CardLane.INBOX),
            Card(
                id="active",
                title="Build it",
                lane=CardLane.ACTIVE,
                project_id="project",
            ),
            Card(id="waiting", title="Needs input", lane=CardLane.WAITING),
            Card(id="done", title="Shipped", lane=CardLane.DONE),
        ]
        self.projects = [Project(id="project", title="PA Core")]
        self.card_read_limits = []
        self.project_read_limits = []

    def list_cards(self, *, realm_id, limit=None):
        self.card_read_limits.append(limit)
        return self.cards[:limit] if limit is not None else self.cards

    def count_cards(self, *, realm_id, lane):
        return sum(card.lane == lane for card in self.cards)

    def get_card(self, card_id, *, realm_id):
        return next((card for card in self.cards if card.id == card_id), None)

    def list_projects(self, *, realm_id, limit=None):
        self.project_read_limits.append(limit)
        return self.projects[:limit] if limit is not None else self.projects


class _Dispatch:
    def __init__(self, **changes):
        self.payload = {
            "dispatch_id": "dispatch",
            "realm_id": "default",
            "card_id": "active",
            "session_id": "session",
            "state": "running",
            "target_instance_id": "local",
            "created_at": "2026-07-28T20:00:00+00:00",
            "updated_at": "2026-07-28T20:01:00+00:00",
            "progress": {
                "schema_version": 1,
                "freshness": {"state": "fresh"},
                "latest": {
                    "phase": "testing",
                    "summary": "Running focused tests",
                    "blockers": [],
                },
            },
        }
        self.payload.update(changes)
        self.realm_id = self.payload["realm_id"]

    def public_dict(self):
        return self.payload


class _DispatchStore:
    def list(self, *, limit):
        return [_Dispatch()]


def _ctx():
    return SimpleNamespace(
        settings=SimpleNamespace(
            primary_realm="default",
            instance_id="local",
            pr_supervisor_authority_url="http://authority",
            fleet_owner_url="",
            instance_url="http://local",
            fleet_id="fleet",
        ),
        store=_Store(),
        services={"dispatch_store": _DispatchStore()},
    )


def _overview():
    field = lambda value, state="fresh": {
        "state": state,
        "value": value,
        "observed_at": "2026-07-28T20:01:00+00:00",
    }
    return {
        "nodes": [
            {
                "id": "local",
                "name": "Mac mini",
                "url": "http://local",
                "zone": "office",
                "local": True,
                "dispatch_capacity": 2,
                "dimensions": {
                    "reachability": field({"health": "up"}),
                    "providers": field(
                        [
                            {
                                "id": "codex",
                                "display_name": "Codex",
                                "auth_state": "authenticated",
                            }
                        ]
                    ),
                    "activity": field(
                        {
                            "capacity": {
                                "consumed": 1,
                                "limit": 2,
                                "source": "configured",
                            },
                            "queued_prompts": 9,
                            "capacity_consumer_links": [
                                {
                                    "kind": "session",
                                    "session_id": "session",
                                    "slots": 1,
                                    "consumer_id": "session:session",
                                }
                            ],
                            "capacity_consumer_links_omitted": 0,
                            "sessions": [
                                {
                                    "id": "session",
                                    "realm_id": "default",
                                    "title": "Workshop worker",
                                    "card_id": "active",
                                    "status": "working",
                                    "connected": True,
                                    "provider": "codex",
                                    "updated_at": "2026-07-28T20:01:00+00:00",
                                }
                            ],
                            "dispatches": [],
                        }
                    ),
                    "sync": field(
                        {
                            "consistent": True,
                            "durable_head": "abc",
                            "projection_head": "abc",
                        }
                    ),
                },
            }
        ],
        "edges": [{"id": "sync", "kind": "sync", "status": "healthy"}],
    }


def test_workshop_maps_each_session_and_card_to_canonical_state():
    snapshot = build_workshop_snapshot(_ctx(), _overview())

    worker = snapshot["bays"][0]["workers"][0]
    assert snapshot["schema"] == "pa.workshop/v2"
    assert worker["id"] == "session"
    assert worker["state"] == "working"
    assert worker["tool_category"] == "testing"
    assert worker["card"]["id"] == "active"
    assert snapshot["bays"][0]["capacity"] == {
        "consumed": 1,
        "limit": 2,
        "source": "configured",
        "queued_prompts": 9,
        "consumer_links": [
            {
                "kind": "session",
                "session_id": "session",
                "slots": 1,
                "consumer_id": "session:session",
            }
        ],
        "consumer_links_omitted": 0,
    }
    assert snapshot["areas"]["inbox"][0]["id"] == "inbox"
    assert snapshot["areas"]["active"][0]["id"] == "active"
    assert snapshot["areas"]["waiting"][0]["id"] == "waiting"
    assert snapshot["areas"]["done"][0]["id"] == "done"
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")
    assert row["lane_label"] == "Active"
    assert row["dispatch_label"] == "Running"
    assert row["activity_label"] == "Working"
    assert row["freshness_label"] == "Current"
    assert row["outcome_label"] == "No outcome yet"
    assert row["location"]["name"] == "Mac mini"
    assert row["card"]["can_dispatch"] is False
    assert "exclusive dispatch" in row["card"]["dispatch_unavailable_reason"]
    assert snapshot["authority"]["instance_id"] is None
    assert snapshot["authority"]["mode"] == "legacy_static"


def test_sync_degradation_does_not_mark_healthy_bay_unhealthy():
    overview = _overview()
    overview["nodes"][0]["dimensions"]["sync"]["value"]["consistent"] = False
    overview["nodes"][0]["dimensions"]["sync"]["value"]["conflicts"] = ["conflict"]

    snapshot = build_workshop_snapshot(_ctx(), overview)

    assert snapshot["sync"]["state"] == "degraded"
    assert snapshot["bays"][0]["health"] == "up"
    assert snapshot["bays"][0]["connectivity"] == "connected"


def test_stale_activity_is_preserved_but_never_presented_as_live():
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["state"] = "stale"

    snapshot = build_workshop_snapshot(_ctx(), overview)

    bay = snapshot["bays"][0]
    assert bay["activity_freshness"] == "stale"
    assert bay["workers"][0]["live"] is False
    assert bay["workers"][0]["state"] == "stalled"


def test_unavailable_activity_value_is_treated_as_empty():
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"] = {
        "state": "unavailable",
        "value": None,
    }
    context = _ctx()
    context.services = {}

    snapshot = build_workshop_snapshot(context, overview)

    bay = snapshot["bays"][0]
    assert bay["activity_freshness"] == "unavailable"
    assert bay["workers"] == []


def test_multi_instance_activity_can_begin_after_initial_snapshot():
    overview = _overview()
    remote = {
        **overview["nodes"][0],
        "id": "monica",
        "name": "Monica",
        "local": False,
        "dimensions": {
            **overview["nodes"][0]["dimensions"],
            "activity": {
                **overview["nodes"][0]["dimensions"]["activity"],
                "value": {
                    "capacity": {"consumed": 0, "limit": 2},
                    "sessions": [],
                    "dispatches": [],
                },
            },
        },
    }
    overview["nodes"].append(remote)
    initial = build_workshop_snapshot(_ctx(), overview)
    assert initial["bays"][1]["workers"] == []

    remote["dimensions"]["activity"]["value"] = {
        "capacity": {"consumed": 1, "limit": 2},
        "sessions": [
            {
                "id": "monica-session",
                "realm_id": "default",
                "title": "Monica worker",
                "status": "working",
                "connected": True,
                "provider": "codex",
                "updated_at": datetime.now(UTC).isoformat(),
            }
        ],
        "dispatches": [],
    }
    updated = build_workshop_snapshot(_ctx(), overview)
    assert updated["bays"][1]["workers"][0]["id"] == "monica-session"
    assert updated["bays"][1]["capacity"]["consumed"] == 1


def test_starting_dispatch_appears_only_after_durable_admission():
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = []
    overview["nodes"][0]["dimensions"]["activity"]["value"]["dispatches"] = [
        {
            "dispatch_id": "reserved",
            "realm_id": "default",
            "card_id": "active",
            "state": "starting_session",
            "created_at": datetime.now(UTC).isoformat(),
        }
    ]

    snapshot = build_workshop_snapshot(_ctx(), overview)

    worker = snapshot["bays"][0]["workers"][0]
    assert worker["id"] == "dispatch:reserved"
    assert worker["state"] == "starting"
    assert worker["card"]["lane"] == "active"


def test_session_activity_remains_truthful_when_dispatch_progress_is_unsupported():
    overview = _overview()
    dispatch = _Dispatch().public_dict()
    dispatch["progress"] = {"schema_version": None, "latest": None}

    class UnsupportedDispatchStore:
        def list(self, *, limit):
            return [SimpleNamespace(public_dict=lambda: dispatch)]

    ctx = _ctx()
    ctx.services["dispatch_store"] = UnsupportedDispatchStore()
    snapshot = build_workshop_snapshot(ctx, overview)

    assert snapshot["bays"][0]["workers"][0]["state"] == "working"


def test_current_followup_wins_over_completed_history_without_repeating_title():
    completed = _Dispatch(
        dispatch_id="completed-dispatch",
        session_id="historical-session",
        state="completed",
        effective_state="completed",
        updated_at="2026-07-28T20:05:00+00:00",
        evaluated_outcome="completed",
    )
    followup = _Dispatch(
        dispatch_id="followup-dispatch",
        session_id="current-session",
        state="running",
        updated_at="2026-07-28T20:04:00+00:00",
        evaluated_outcome="needs_evaluation",
    )

    class DispatchHistoryStore:
        def list(self, *, limit):
            return [completed, followup]

    ctx = _ctx()
    ctx.services["dispatch_store"] = DispatchHistoryStore()
    ctx.store.cards[1].title = "Same work and session title"
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = [
        {
            "id": "current-session",
            "realm_id": "default",
            "title": "Same work and session title",
            "card_id": "active",
            "status": "working",
            "connected": True,
            "provider": "codex",
            "updated_at": "2026-07-28T20:04:00+00:00",
        }
    ]

    snapshot = build_workshop_snapshot(ctx, overview)
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["card"]["dispatch_id"] == "followup-dispatch"
    assert row["session"]["id"] == "current-session"
    assert row["session"]["relationship_label"] == "Linked session"
    assert row["card"]["historical_dispatch_count"] == 2
    assert row["outcome_label"] == "Not evaluated"


def test_active_dispatch_wins_for_same_session_even_when_older_history_lists_last():
    running = _Dispatch(
        dispatch_id="running",
        session_id="session",
        state="running",
        updated_at="2026-07-28T20:02:00+00:00",
    )
    completed = _Dispatch(
        dispatch_id="completed",
        session_id="session",
        state="completed",
        effective_state="completed",
        updated_at="2026-07-28T20:01:00+00:00",
    )

    class SameSessionHistory:
        def list(self, *, limit):
            return [running, completed]

    ctx = _ctx()
    ctx.services["dispatch_store"] = SameSessionHistory()
    snapshot = build_workshop_snapshot(ctx, _overview())

    worker = snapshot["bays"][0]["workers"][0]
    assert worker["dispatch_id"] == "running"
    assert worker["state"] == "working"


def test_session_association_does_not_depend_on_truncated_dispatch_history():
    running = _Dispatch(
        dispatch_id="older-than-500-but-current",
        session_id="session",
        state="running",
    )

    class CanonicalAssociationStore:
        def latest_by_card(self, card_ids, *, realm_id):
            return {"active": running}

        def latest_by_session(self, session_ids, *, realm_id):
            return {"session": running}

        def history_counts(self, card_ids, *, realm_id):
            return {"active": 501}

        def current_card_ids(self, *, realm_id, limit):
            return []

        def list(self, **_kwargs):
            raise AssertionError(
                "Workshop must not join through a truncated history list"
            )

    running.realm_id = "default"
    ctx = _ctx()
    ctx.services["dispatch_store"] = CanonicalAssociationStore()
    snapshot = build_workshop_snapshot(ctx, _overview())
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["session"]["id"] == "session"
    assert row["card"]["dispatch_id"] == "older-than-500-but-current"
    assert row["card"]["historical_dispatch_count"] == 501


def test_empty_dispatch_indexes_are_authoritative_without_ledger_fallback():
    calls = {"card": 0, "session": 0}

    class EmptyIndexedDispatchStore:
        def current_card_ids(self, *, realm_id, limit):
            return []

        def latest_by_card(self, card_ids, *, realm_id):
            calls["card"] += 1
            assert "active" in card_ids
            return {}

        def latest_by_session(self, session_ids, *, realm_id):
            calls["session"] += 1
            assert session_ids == {"session"}
            return {}

        def history_counts(self, card_ids, *, realm_id):
            return {card_id: 0 for card_id in card_ids}

        def list(self, **_kwargs):
            raise AssertionError("legitimate empty indexes must not scan the ledger")

    ctx = _ctx()
    ctx.services["dispatch_store"] = EmptyIndexedDispatchStore()

    snapshot = build_workshop_snapshot(ctx, _overview())
    worker = snapshot["bays"][0]["workers"][0]
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert calls == {"card": 1, "session": 1}
    assert worker["id"] == "session"
    assert worker["dispatch_id"] is None
    assert row["card"]["dispatch_id"] is None
    assert row["card"]["historical_dispatch_count"] == 0


def test_waiting_capacity_and_blocked_pre_session_work_is_explicit():
    overview = _overview()
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = []
    activity["dispatches"] = [
        {
            "dispatch_id": "waiting",
            "realm_id": "default",
            "card_id": "active",
            "state": "waiting_capacity",
            "queue": {"position": 3, "reason": "All Codex slots are occupied"},
        },
        {
            "dispatch_id": "blocked",
            "realm_id": "default",
            "card_id": "waiting",
            "state": "blocked",
            "queue": {"blocked_code": "placement_unavailable"},
        },
    ]

    snapshot = build_workshop_snapshot(_ctx(), overview)
    workers = {
        worker["dispatch_id"]: worker for worker in snapshot["bays"][0]["workers"]
    }

    assert workers["waiting"]["state"] == "queued"
    assert workers["waiting"]["queue_position"] == 3
    assert workers["waiting"]["latest_progress"] == "All Codex slots are occupied"
    assert workers["blocked"]["state"] == "stalled"
    assert workers["blocked"]["latest_progress"] == "placement_unavailable"


def test_workshop_excludes_session_and_dispatch_joins_from_other_realms():
    overview = _overview()
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = [
        {
            "id": "foreign-session",
            "title": "Foreign work",
            "status": "working",
            "realm_id": "other",
        }
    ]
    activity["dispatches"] = [
        {
            "dispatch_id": "foreign-dispatch",
            "card_id": "active",
            "state": "waiting_capacity",
            "realm_id": "other",
        }
    ]

    snapshot = build_workshop_snapshot(_ctx(), overview)

    assert snapshot["bays"][0]["workers"] == []


def test_workshop_excludes_and_counts_missing_realm_activity():
    overview = _overview()
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = [
        {"id": "unknown-session", "status": "working"},
        {"id": "other-session", "realm_id": "other", "status": "working"},
    ]
    activity["dispatches"] = [
        {"dispatch_id": "unknown-dispatch", "state": "blocked"},
        {
            "dispatch_id": "other-dispatch",
            "realm_id": "other",
            "state": "blocked",
        },
    ]

    snapshot = build_workshop_snapshot(_ctx(), overview)

    assert snapshot["bays"][0]["workers"] == []
    assert snapshot["counts"]["excluded_activity"] == {
        "unknown_realm_sessions": 1,
        "unknown_realm_dispatches": 1,
        "other_realm_sessions": 1,
        "other_realm_dispatches": 1,
    }


def test_pre_session_reservation_is_in_motion_but_not_live_or_attention():
    waiting = _Dispatch(
        dispatch_id="waiting",
        session_id=None,
        state="waiting_capacity",
        effective_state="waiting_capacity",
    )

    class WaitingStore:
        def list(self, *, limit):
            return [waiting]

    ctx = _ctx()
    ctx.services["dispatch_store"] = WaitingStore()
    overview = _overview()
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = []
    activity["dispatches"] = [
        {
            "dispatch_id": "waiting",
            "realm_id": "default",
            "card_id": "active",
            "state": "waiting_capacity",
            "queue": {"position": 2, "reason": "All workers are occupied"},
        }
    ]

    snapshot = build_workshop_snapshot(ctx, overview)
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["dispatch_current"] is True
    assert row["live"] is False
    assert row["attention"] is False
    assert row["presentation"]["group"] == "motion"
    assert row["session"] is None
    assert row["reservation"] == {
        "id": "dispatch:waiting",
        "dispatch_id": "waiting",
        "relationship_kind": "reservation",
        "label": "Dispatch reservation",
        "state": "queued",
        "state_label": "Queued",
        "reason": "All workers are occupied",
        "queue_position": 2,
    }


def test_completion_pending_retry_is_in_motion_not_live_or_attention():
    pending = _Dispatch(
        session_id=None,
        state="completion_pending",
        effective_state="completion_pending",
        card_reconciliation={"state": "retrying", "reason": "Waiting for card"},
    )

    class CompletionStore:
        def list(self, *, limit):
            return [pending]

    ctx = _ctx()
    ctx.services["dispatch_store"] = CompletionStore()
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = []

    snapshot = build_workshop_snapshot(ctx, overview)
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["dispatch_current"] is True
    assert row["live"] is False
    assert row["attention"] is False
    assert row["presentation"]["group"] == "motion"
    assert row["attention_reasons"] == []


def test_stale_sync_is_attention_even_when_last_observation_was_consistent():
    overview = _overview()
    overview["nodes"][0]["dimensions"]["sync"]["state"] = "stale"

    snapshot = build_workshop_snapshot(_ctx(), overview)

    assert snapshot["sync"]["state"] == "degraded"
    assert snapshot["sync"]["state_label"] == "Needs attention"
    assert snapshot["sync"]["counts"] == {
        "total": 1,
        "attention": 1,
        "historical": 1,
    }
    issue = snapshot["sync"]["issues"][0]
    assert issue["peer_name"] == "Mac mini"
    assert issue["condition"] == "historical"
    assert "out of date" in issue["summary"]


def test_worker_projection_is_bounded_and_prioritizes_live_turns():
    overview = _overview()
    sessions = [
        {
            "id": f"quiet-{index}",
            "realm_id": "default",
            "status": "idle",
            "updated_at": f"2026-08-03T12:{index % 60:02d}:00+00:00",
        }
        for index in range(120)
    ]
    sessions.append(
        {
            "id": "important-live-turn",
            "realm_id": "default",
            "status": "working",
            "updated_at": "2026-07-01T00:00:00+00:00",
        }
    )
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = sessions
    activity["session_total"] = len(sessions)

    snapshot = build_workshop_snapshot(_ctx(), overview)
    workers = snapshot["bays"][0]["workers"]

    assert len(workers) == WORKSHOP_WORKER_LIMIT
    assert any(worker["id"] == "important-live-turn" for worker in workers)
    assert snapshot["counts"]["sessions"] == {
        "reported": 121,
        "projected": 80,
        "omitted": 41,
    }


def test_worker_projection_ranks_live_and_blocked_work_globally_across_nodes():
    overview = _overview()
    first = overview["nodes"][0]
    first_activity = first["dimensions"]["activity"]["value"]
    first_activity["sessions"] = [
        {
            "id": f"first-idle-{index:02d}",
            "realm_id": "default",
            "status": "idle",
            "connected": True,
            "updated_at": "2026-08-04T12:00:00+00:00",
        }
        for index in range(WORKSHOP_WORKER_LIMIT)
    ]
    first_activity["session_total"] = WORKSHOP_WORKER_LIMIT
    first_activity["dispatches"] = []

    second = deepcopy(first)
    second["id"] = "later-node"
    second["name"] = "Later node"
    second_activity = second["dimensions"]["activity"]["value"]
    second_activity["sessions"] = [
        {
            "id": "later-working",
            "realm_id": "default",
            "status": "working",
            "connected": True,
            "updated_at": "2026-07-01T00:00:00+00:00",
        }
    ]
    second_activity["session_total"] = 1
    second_activity["dispatches"] = [
        {
            "dispatch_id": "later-blocked",
            "realm_id": "default",
            "state": "blocked",
            "last_error": "Target workspace is unavailable",
            "updated_at": "2026-07-01T00:00:00+00:00",
        }
    ]
    overview["nodes"].append(second)

    snapshot = build_workshop_snapshot(_ctx(), overview)
    workers = [
        (bay["id"], worker) for bay in snapshot["bays"] for worker in bay["workers"]
    ]

    assert len(workers) == WORKSHOP_WORKER_LIMIT
    assert any(
        bay_id == "later-node" and worker["id"] == "later-working"
        for bay_id, worker in workers
    )
    assert any(
        bay_id == "later-node" and worker["id"] == "dispatch:later-blocked"
        for bay_id, worker in workers
    )
    assert all(
        not worker["live"]
        for _, worker in workers
        if worker["id"].startswith("first-idle-")
    )
    assert snapshot["counts"]["sessions"] == {
        "reported": 81,
        "projected": 79,
        "omitted": 2,
    }
    assert snapshot["counts"]["workers"] == {
        "reported": 82,
        "projected": 80,
        "omitted": 2,
    }
    assert snapshot["counts"]["reservations"] == 1


def test_two_thousand_unlinked_sessions_remain_bounded_with_truthful_counts():
    overview = _overview()
    activity = overview["nodes"][0]["dimensions"]["activity"]["value"]
    activity["sessions"] = [
        {
            "id": f"unlinked-{index:04d}",
            "realm_id": "default",
            "status": "idle",
            "updated_at": "2026-08-03T12:00:00+00:00",
        }
        for index in range(2_000)
    ]
    activity["session_total"] = 2_000

    snapshot = build_workshop_snapshot(_ctx(), overview)

    assert len(snapshot["bays"][0]["workers"]) == WORKSHOP_WORKER_LIMIT
    assert len(snapshot["orphan_sessions"]) == WORKSHOP_WORKER_LIMIT
    assert snapshot["counts"]["sessions"] == {
        "reported": 2_000,
        "projected": WORKSHOP_WORKER_LIMIT,
        "omitted": 2_000 - WORKSHOP_WORKER_LIMIT,
    }


def test_live_session_enriches_card_beyond_newest_card_read_window():
    ctx = _ctx()
    ctx.store.cards = [
        Card(
            id=f"card-{index}",
            title=f"Card {index}",
            lane=CardLane.ACTIVE,
        )
        for index in range(121)
    ]
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = [
        {
            "id": "live-on-121st-card",
            "realm_id": "default",
            "card_id": "card-120",
            "title": "Old card, live turn",
            "status": "working",
            "connected": True,
            "provider": "codex",
            "updated_at": "2026-08-04T12:00:00+00:00",
        }
    ]

    snapshot = build_workshop_snapshot(ctx, overview)
    row = next(item for item in snapshot["work_orders"] if item["id"] == "card-120")

    assert row["live"] is True
    assert row["session"]["id"] == "live-on-121st-card"
    assert snapshot["orphan_sessions"] == []
    assert snapshot["inventory"] == {
        "loaded": WORKSHOP_PROJECTION_LIMIT,
        "total": 121,
        "omitted": 121 - WORKSHOP_PROJECTION_LIMIT,
        "overflow_href": "/work?realm=default",
        "description": (
            "Newest and operational cards are bounded in Workshop; "
            "open the Work board for the full inventory."
        ),
    }


def test_work_order_preserves_exact_attention_axes_and_progress_freshness():
    dispatch = _Dispatch(
        last_error="Completion delivery timed out after three attempts",
        error_code="completion_delivery_timeout",
        completion_outbox={
            "pending": True,
            "last_error": "Authority endpoint closed the delivery stream",
            "classification": "transport_retry",
        },
        card_completion={
            "status": "invalid",
            "lane_before": "active",
            "lane_after": None,
            "reason": "Disposition requested done before integration merged",
            "extraction_error": "Disposition JSON omitted evidence.watched_head_sha",
        },
        card_reconciliation={
            "state": "blocked",
            "reason": "Reconciliation is blocked on the exact PR head",
            "disposition_error": "No valid card disposition was recorded",
            "last_dependency_error": "PR supervisor has not observed a stable head",
            "condition": "stable_green_head_required",
            "recovery_action": "Wait for the next supervisor observation",
        },
        progress={
            "schema_version": 1,
            "freshness": {
                "state": "stale",
                "last_activity_at": "2026-08-04T10:00:00+00:00",
                "age_seconds": 901,
            },
            "delivery_error": "Progress heartbeat delivery failed",
            "latest": {
                "phase": "testing",
                "summary": "Tests were running",
                "blockers": [],
            },
        },
    )

    class EvidenceStore:
        def list(self, *, limit):
            return [dispatch]

    ctx = _ctx()
    ctx.services["dispatch_store"] = EvidenceStore()
    overview = _overview()
    overview["nodes"][0]["dimensions"]["activity"]["value"]["sessions"] = []
    snapshot = build_workshop_snapshot(ctx, overview)
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["live"] is False
    assert row["freshness_label"] == "Last known"
    assert row["progress_freshness"] == "stale"
    assert row["progress_freshness_label"] == "Stale"
    assert row["progress_age_seconds"] == 901
    assert row["attention"] is True
    assert {detail["axis"] for detail in row["attention_details"]} >= {
        "dispatch",
        "completion_delivery",
        "progress",
        "card_disposition",
        "card_reconciliation",
    }
    for exact_reason in (
        "Completion delivery timed out after three attempts",
        "completion_delivery_timeout",
        "Authority endpoint closed the delivery stream",
        "Progress heartbeat delivery failed",
        "Disposition requested done before integration merged",
        "Disposition JSON omitted evidence.watched_head_sha",
        "Reconciliation is blocked on the exact PR head",
        "No valid card disposition was recorded",
        "PR supervisor has not observed a stable head",
        "stable_green_head_required",
        "Wait for the next supervisor observation",
        "Structured progress is stale",
    ):
        assert exact_reason in row["attention_reasons"]


def test_workshop_uses_card_scoped_pr_watch_projection():
    class WatchStore:
        called = None

        def list_watches_for_cards(
            self, card_ids, *, realm_id, include_retired, per_card_limit
        ):
            self.called = (card_ids, realm_id, include_retired, per_card_limit)
            return []

        def list_watches(self, **_kwargs):
            raise AssertionError("Workshop must not scan the full PR watch ledger")

    ctx = _ctx()
    store = WatchStore()
    ctx.services["pr_supervisor_store"] = store

    build_workshop_snapshot(ctx, _overview())

    assert store.called == (
        {"inbox", "active", "waiting", "done"},
        "default",
        True,
        5,
    )


def test_pr_watch_projection_is_card_scoped_and_bounded(tmp_path):
    store = PRSupervisorStore(tmp_path / "supervisor.db")
    for index in range(8):
        store.upsert_watch(
            PRWatch(
                id=f"active-{index}",
                realm_id="default",
                card_id="active",
                repository="petersky/pa",
                pr_number=index + 1,
                pr_url=f"https://github.com/petersky/pa/pull/{index + 1}",
                last_error="Current supervisor failure" if index == 0 else None,
            ),
            preserve_lease=False,
        )
    store.upsert_watch(
        PRWatch(
            id="unrelated",
            realm_id="default",
            card_id="other-card",
            repository="petersky/pa",
            pr_number=99,
            pr_url="https://github.com/petersky/pa/pull/99",
            state={"gate": {"actionable": True}},
        ),
        preserve_lease=False,
    )

    watches = store.list_watches_for_cards(
        {"active"}, realm_id="default", include_retired=True, per_card_limit=5
    )

    assert len(watches) == 5
    assert all(watch.card_id == "active" for watch in watches)
    assert all(watch.id != "unrelated" for watch in watches)
    actionable_card_ids = store.list_actionable_card_ids(realm_id="default", limit=1)
    assert actionable_card_ids == ["other-card"]
    assert set(
        store.list_actionable_card_ids(realm_id="default", limit=80)
    ) == {"active", "other-card"}


def test_session_projection_queries_only_bounded_active_realm_rows(tmp_path):
    projection = CardProjection(tmp_path / "projection.db")
    for index in range(105):
        projection.save_session(
            AgentSession(
                id=f"active-{index}",
                realm_id="default",
                agent_name="codex",
                status="idle",
                updated_at=datetime(2026, 8, 3, 12, index % 60, tzinfo=UTC),
            )
        )
    projection.save_session(
        AgentSession(
            id="important-working",
            realm_id="default",
            agent_name="codex",
            status="working",
            updated_at=datetime(2026, 7, 1, tzinfo=UTC),
        )
    )
    projection.save_session(
        AgentSession(
            id="closed-newer",
            realm_id="default",
            agent_name="codex",
            status="closed",
            updated_at=datetime(2026, 8, 4, tzinfo=UTC),
        )
    )
    projection.save_session(
        AgentSession(
            id="foreign-working",
            realm_id="other",
            agent_name="codex",
            status="working",
        )
    )

    sessions, total = projection.list_sessions_for_workshop(
        realm_id="default", limit=80
    )

    assert total == 106
    assert len(sessions) == 80
    assert sessions[0].id == "important-working"
    assert all(session.realm_id == "default" for session in sessions)
    assert all(session.status != "closed" for session in sessions)


def test_active_session_does_not_inherit_terminal_dispatch_activity_state():
    terminal = _Dispatch(
        state="completed",
        effective_state="completed",
        agent_turn={"ended": True, "completed": True, "stop_reason": "end_turn"},
        dispatch_completion={
            "completed": True,
            "acknowledged_at": "2026-07-28T20:02:00Z",
        },
        card_completion={
            "status": "pending",
            "lane_before": "active",
            "lane_after": None,
        },
        card_reconciliation={"state": "pending", "reason": "Disposition missing"},
        evaluated_outcome="needs_evaluation",
    )

    class TerminalDispatchStore:
        def list(self, *, limit):
            return [terminal]

    ctx = _ctx()
    ctx.services["dispatch_store"] = TerminalDispatchStore()
    snapshot = build_workshop_snapshot(ctx, _overview())
    row = next(item for item in snapshot["work_orders"] if item["id"] == "active")

    assert row["activity_state"] == "working"
    assert row["agent_turn"]["ended"] is True
    assert row["dispatch_completion"]["completed"] is True
    assert row["card_completion"]["status"] == "pending"
    assert row["card_reconciliation"]["state"] == "pending"
    assert row["outcome_label"] == "Not evaluated"


def test_no_session_card_keeps_lane_dispatch_activity_and_outcome_separate():
    snapshot = build_workshop_snapshot(_ctx(), _overview())
    row = next(item for item in snapshot["work_orders"] if item["id"] == "inbox")

    assert row["lane_label"] == "Inbox"
    assert row["dispatch_label"] == "Not dispatched"
    assert row["activity_label"] == "Not in motion"
    assert row["freshness_label"] == "No session signal"
    assert row["outcome_label"] == "No outcome yet"
    assert row["card"]["can_dispatch"] is True


def test_sync_rail_names_historical_peer_condition_and_recovery():
    overview = _overview()
    sync = overview["nodes"][0]["dimensions"]["sync"]
    sync["state"] = "stale"
    sync["value"].update(
        {
            "consistent": False,
            "convergence": {"phase": "retrying", "attempt": 2},
        }
    )

    snapshot = build_workshop_snapshot(_ctx(), overview)
    issue = snapshot["sync"]["issues"][0]

    assert issue["peer_name"] == "Mac mini"
    assert issue["condition"] == "historical"
    assert issue["condition_label"] == "Historical observation"
    assert issue["recovery_label"] == "Retrying"
    assert issue["recovery_attempt"] == 2
    assert issue["href"].endswith("instance=local")


def test_production_cardinality_snapshot_keeps_explicit_inventory_counts():
    ctx = _ctx()
    ctx.store.cards.extend(
        Card(id=f"bulk-{index}", title=f"Inventory card {index}", lane=CardLane.INBOX)
        for index in range(125)
    )

    snapshot = build_workshop_snapshot(ctx, _overview())

    assert snapshot["counts"]["total"] == 129
    assert snapshot["counts"]["lanes"]["inbox"] == 126
    assert len(snapshot["work_orders"]) == WORKSHOP_PROJECTION_LIMIT
    assert snapshot["inventory"]["omitted"] == 49
    assert all(len(cards) <= 12 for cards in snapshot["areas"].values())
    assert ctx.store.card_read_limits == [120]
    assert ctx.store.project_read_limits == [120]
    assert snapshot["default_view"] == {
        "filter": "operational",
        "page_size": 20,
        "description": "Live work and work needing attention",
    }


def test_workshop_ui_contract_is_accessible_and_excludes_replay_controls():
    root = Path(__file__).parents[1]
    template = (root / "src/pa/server/templates/pages/workshop.html").read_text()
    script = (root / "src/pa/server/static/js/workshop.js").read_text()
    style = (root / "src/pa/server/static/style.css").read_text()

    assert "Floor view" in template
    assert "Compact view" in template
    assert 'data-workshop-view-status aria-live="polite"' in template
    assert 'aria-live="polite"' in template
    assert "/api/cards/events" in script
    assert "/api/fleet/workshop/events" in script
    assert "/api/fleet/workshop" in script
    assert "acceptSnapshot" in script
    assert "Activity reconnecting" in script
    assert "refreshGeneration" in script
    assert "pa.workshop.view.v1" in script
    assert 'data-workshop-compact-row="work-order"' in script
    assert "PAGE_SIZE = 20" in script
    assert "Loaded inventory" in template
    assert "Open Work board" in template
    assert 'overflow.textContent = "Open Work board"' in script
    assert "data-workshop-search" in template
    assert 'data-label="Evaluated outcome"' in script
    assert "root === activeRoot" in script
    assert "prefers-reduced-motion" in style
    assert 'data-workshop-inspector tabindex="-1"' in template
    assert 'data-workshop-announcer aria-live="polite"' in template
    assert 'data-workshop-inspector aria-live="polite"' not in template
    assert 'class="workshop-live" role="status"' in template
    assert "data-workshop-refresh>Refresh</button>" in template
    assert "timeline" not in template.lower()
    assert "speed" not in template.lower()


def test_browser_transport_rejects_duplicate_and_out_of_order_snapshots():
    root = Path(__file__).parents[1]
    script = root / "src/pa/server/static/js/workshop.js"
    harness = """
const fs = require("fs");
global.window = { PA_TEST: true };
global.document = { addEventListener() {} };
eval(fs.readFileSync(process.argv[1], "utf8"));
const api = window.PAWorkshopTest;
api.reset();
if (!api.shouldAcceptSnapshot({generated_at: "2026-08-03T10:00:00Z"})) process.exit(2);
api.markSnapshot("2026-08-03T10:00:00Z");
if (api.shouldAcceptSnapshot({generated_at: "2026-08-03T10:00:00Z"})) process.exit(3);
if (api.shouldAcceptSnapshot({generated_at: "2026-08-03T09:59:59Z"})) process.exit(4);
if (!api.shouldAcceptSnapshot({generated_at: "2026-08-03T10:00:01Z"})) process.exit(5);
"""
    result = subprocess.run(
        ["node", "-e", harness, str(script)],
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr


def test_two_stream_iterations_emit_heartbeat_when_only_clocks_tick():
    first = build_workshop_snapshot(_ctx(), _overview())
    event, digest, sequence = _workshop_stream_iteration(first, "", 0)
    assert "event: snapshot" in event
    second = deepcopy(first)
    second["generated_at"] = "2026-07-28T20:02:00+00:00"
    second["bays"][0]["observed_at"] = "2026-07-28T20:02:00+00:00"
    second["bays"][0]["activity_observed_at"] = "2026-07-28T20:02:00+00:00"
    second["bays"][0]["activity_age_seconds"] = 0
    second["sync"]["nodes"][0]["age_seconds"] = 0

    event, next_digest, next_sequence = _workshop_stream_iteration(
        second, digest, sequence
    )

    assert event == ": workshop heartbeat\n\n"
    assert next_digest == digest
    assert next_sequence == sequence


def test_forced_workshop_refresh_never_coalesces_with_weaker_probe(
    monkeypatch, tmp_path
):
    async def exercise():
        nonforced_started = asyncio.Event()
        release_nonforced = asyncio.Event()
        calls = []

        async def probe(_ctx, instance, dimension, *, force=False):
            calls.append((dimension, force))
            if not force:
                nonforced_started.set()
                await release_nonforced.wait()
            return {"state": "fresh", "value": {}, "observed_at": "now"}

        monkeypatch.setattr("pa.modules.fleet.probe_dimension", probe)
        ctx = SimpleNamespace(settings=SimpleNamespace(data_dir=tmp_path))
        instance = FleetInstance(
            instance_id="remote", name="Remote", url="http://remote.test"
        )
        background = asyncio.create_task(
            _refresh_workshop_dimensions(ctx, [instance], force=False)
        )
        await nonforced_started.wait()
        forced = await _refresh_workshop_dimensions(ctx, [instance], force=True)
        release_nonforced.set()
        await background
        return calls, forced

    calls, forced = asyncio.run(exercise())

    assert {force for _dimension, force in calls} == {False, True}
    assert set(forced["remote"]) == {"reachability", "activity", "providers", "sync"}


def test_workshop_route_preserves_fresh_remote_probe_results(monkeypatch):
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Workshop host",
            instance_url="http://workshop.test",
            agent_enabled=False,
            peers=[],
        )
        calls = []

        async def probe(_ctx, instance, dimension, *, force=False):
            calls.append((instance.instance_id, dimension, force))
            values = {
                "reachability": {"health": "up"},
                "providers": [],
                "sync": {
                    "consistent": True,
                    "durable_head": "a",
                    "projection_head": "a",
                },
                "activity": {
                    "capacity": {"consumed": 1, "limit": 2},
                    "dispatches": [],
                    "sessions": [
                        {
                            "id": "remote-session",
                            "title": "Fresh remote work",
                            "status": "working",
                            "connected": True,
                            "provider": "codex",
                            "realm_id": "default",
                        }
                    ]
                    if instance.instance_id == "remote"
                    else [],
                },
            }
            return {
                "state": "fresh",
                "value": values[dimension],
                "observed_at": datetime.now(UTC).isoformat(),
            }

        monkeypatch.setattr("pa.modules.fleet.probe_dimension", probe)
        try:
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                app.state.ctx.require_service("fleet_registry").upsert_instance(
                    FleetInstance(
                        instance_id="remote",
                        name="Remote",
                        url="http://remote.test",
                    )
                )
                response = client.get("/api/fleet/workshop?refresh=true")
            assert response.status_code == 200
            remote = next(
                bay for bay in response.json()["bays"] if bay["id"] == "remote"
            )
            assert remote["activity_freshness"] == "fresh"
            assert remote["workers"][0]["state"] == "working"
            assert remote["workers"][0]["live"] is True
            assert ("remote", "sync", True) in calls
        finally:
            reset_instance_agent()
            reset_store()


def test_workshop_page_and_api_render_from_same_canonical_snapshot():
    reset_store()
    reset_instance_agent()
    with tempfile.TemporaryDirectory() as tmp:
        settings = Settings(
            data_dir=Path(tmp),
            instance_id="local",
            instance_name="Workshop host",
            instance_url="http://workshop.test",
            agent_enabled=False,
            peers=[],
        )
        try:
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                page = client.get("/workshop")
                assert page.status_code == 200
                assert "Workshop" in page.text
                assert "Floor view" in page.text
                assert "pa-workshop-data" in page.text
                assert 'href="/workshop"' in page.text

                snapshot = client.get("/api/fleet/workshop")
                assert snapshot.status_code == 200
                payload = snapshot.json()
                assert payload["schema"] == "pa.workshop/v2"
                assert payload["bays"][0]["id"] == "local"
                assert payload["work_orders"] == []
                assert payload["counts"]["total"] == 0
                assert payload["areas"] == {
                    "inbox": [],
                    "active": [],
                    "waiting": [],
                    "done": [],
                }
        finally:
            reset_instance_agent()

            reset_store()

def test_presentation_totals_are_computed_before_work_order_render_limit():
    ctx = _ctx()
    ctx.store.cards.extend(
        Card(
            id=f"historical-{index}",
            title=f"Historical outcome {index}",
            lane=CardLane.DONE,
        )
        for index in range(95)
    )

    snapshot = build_workshop_snapshot(ctx, _overview())

    assert len(snapshot["work_orders"]) == WORKSHOP_PROJECTION_LIMIT
    assert snapshot["counts"]["presentations"]["outcome"] == 96
    assert sum(snapshot["counts"]["presentations"].values()) == 99


def test_retired_watch_preserves_history_without_driving_attention(tmp_path):
    ctx = _ctx()
    supervisor = PRSupervisorStore(tmp_path / "supervisor.db")
    retired_at = datetime(2026, 8, 4, 12, 0, tzinfo=UTC)
    supervisor.upsert_watch(
        PRWatch(
            id="retired-watch",
            realm_id="default",
            card_id="waiting",
            repository="petersky/pa",
            pr_number=42,
            pr_url="https://github.com/petersky/pa/pull/42",
            status="merged",
            retired_at=retired_at,
            last_error="Preserved failure from before merge",
            state={
                "gate": {
                    "actionable": True,
                    "reasons": ["Historical review gate"],
                }
            },
        ),
        preserve_lease=False,
    )
    ctx.services["pr_supervisor_store"] = supervisor

    snapshot = build_workshop_snapshot(ctx, _overview())
    row = next(item for item in snapshot["work_orders"] if item["id"] == "waiting")

    assert row["presentation"]["group"] == "quiet"
    assert row["presentation"]["attention"] is False
    assert row["card"]["pull_requests"][0]["retired_at"] == retired_at.isoformat()
