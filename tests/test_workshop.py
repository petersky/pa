from __future__ import annotations

from datetime import UTC, datetime
import tempfile
from pathlib import Path
from types import SimpleNamespace

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import Card, CardLane, Project
from pa.domain.store import reset_store
from pa.fleet.workshop import build_workshop_snapshot
from pa.instance.agent_session import reset_instance_agent


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

    def list_cards(self, *, realm_id):
        return self.cards

    def list_projects(self, *, realm_id):
        return self.projects


class _Dispatch:
    def public_dict(self):
        return {
            "dispatch_id": "dispatch",
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
                        [{"id": "codex", "display_name": "Codex", "auth_state": "authenticated"}]
                    ),
                    "activity": field(
                        {
                            "capacity": {"consumed": 1, "limit": 2, "source": "configured"},
                            "sessions": [
                                {
                                    "id": "session",
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
    assert snapshot["schema"] == "pa.workshop/v1"
    assert worker["id"] == "session"
    assert worker["state"] == "working"
    assert worker["tool_category"] == "testing"
    assert worker["card"]["id"] == "active"
    assert snapshot["areas"]["inbox"][0]["id"] == "inbox"
    assert snapshot["areas"]["active"][0]["id"] == "active"
    assert snapshot["areas"]["waiting"][0]["id"] == "waiting"
    assert snapshot["areas"]["done"][0]["id"] == "done"
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


def test_unsupported_progress_is_explicit():
    overview = _overview()
    dispatch = _Dispatch().public_dict()
    dispatch["progress"] = {"schema_version": None, "latest": None}

    class UnsupportedDispatchStore:
        def list(self, *, limit):
            return [SimpleNamespace(public_dict=lambda: dispatch)]

    ctx = _ctx()
    ctx.services["dispatch_store"] = UnsupportedDispatchStore()
    snapshot = build_workshop_snapshot(ctx, overview)

    assert snapshot["bays"][0]["workers"][0]["state"] == "unsupported"


def test_workshop_ui_contract_is_accessible_and_excludes_replay_controls():
    root = Path(__file__).parents[1]
    template = (root / "src/pa/server/templates/pages/workshop.html").read_text()
    script = (root / "src/pa/server/static/js/workshop.js").read_text()
    style = (root / "src/pa/server/static/style.css").read_text()

    assert "Floor view" in template
    assert "Compact list" in template
    assert 'aria-live="polite"' in template
    assert "/api/cards/events" in script
    assert "/api/fleet/workshop/events" in script
    assert "/api/fleet/workshop" in script
    assert "acceptSnapshot" in script
    assert "Activity reconnecting" in script
    assert "refreshGeneration" in script
    assert "prefers-reduced-motion" in style
    assert "timeline" not in template.lower()
    assert "speed" not in template.lower()


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
                assert payload["schema"] == "pa.workshop/v1"
                assert payload["bays"][0]["id"] == "local"
                assert payload["areas"] == {
                    "inbox": [],
                    "active": [],
                    "waiting": [],
                    "done": [],
                }
        finally:
            reset_instance_agent()
            reset_store()
