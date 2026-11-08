"""The stage strip is derived from card facts, never from a code-workflow template."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pa.core.ui.stage_strip import build_stage_strip, parallel_tally, pulse
from pa.core.ui.work_presentation import child_work_status, present_work_item

NOW = datetime(2026, 11, 8, 20, 0, tzinfo=UTC)
PLAIN = {"schema_version": 1, "revision": "r1", "milestones": [], "mode": "integration_only"}


def _strip(**kw):
    base = dict(requirement=None, completion={"satisfied": [], "missing": [], "accepted": True}, lane="waiting",
                state_code="working", group="motion", session_active=True, has_execution=True,
                activity_at=NOW - timedelta(seconds=6), now=NOW)
    base.update(kw)
    return build_stage_strip(**base)


def _states(strip):
    return [(s["id"], s["state"]) for s in strip["stages"]]


def test_plain_task_is_just_work_and_done_with_a_live_pulse():
    strip = _strip()
    assert _states(strip) == [("work", "active"), ("done", "todo")]
    assert strip["pulse"] == {"level": "live", "at": (NOW - timedelta(seconds=6)).isoformat(), "age_seconds": 6}
    assert strip["current"] == "work"


def test_finished_plain_task_is_done_without_a_pulse():
    strip = _strip(lane="done", state_code="completed", group="outcome", session_active=False)
    assert _states(strip) == [("work", "done"), ("done", "done")]
    assert strip["pulse"] is None


def test_stale_agent_is_flagged_not_hidden():
    assert _strip(activity_at=NOW - timedelta(minutes=20))["pulse"]["level"] == "stale"
    assert _strip(activity_at=NOW - timedelta(seconds=120))["pulse"]["level"] == "recent"
    assert pulse(active=True, at=None, now=NOW)["level"] == "unknown"


def test_document_needing_review_makes_only_the_last_node_yours():
    requirement = {"schema_version": 1, "revision": "r", "mode": "explicit_acceptance", "milestones": []}
    working = _strip(requirement=requirement, completion={"satisfied": [], "missing": ["acceptance"], "accepted": False})
    assert _states(working) == [("work", "active"), ("accept", "todo")]
    waiting = _strip(requirement=requirement, completion={"satisfied": [], "missing": ["acceptance"], "accepted": False},
                     session_active=False, group="attention", state_code="acceptance_pending")
    assert _states(waiting) == [("work", "done"), ("accept", "active")]
    assert [s["owner"] for s in waiting["stages"]] == ["agent", "you"]


def test_declared_code_card_waits_on_work_while_merges_keep_landing():
    requirement = {"schema_version": 1, "revision": "r", "mode": "explicit_acceptance",
                   "milestones": ["integrated", "active", "verified"], "verification_policy": "independent_agent_v1"}
    strip = _strip(requirement=requirement, completion={"satisfied": ["integrated"], "missing": ["active", "verified", "acceptance"], "accepted": False})
    assert _states(strip) == [("work", "active"), ("integrated", "done"), ("active", "todo"), ("verified", "todo"), ("accept", "todo")]
    assert strip["stages"][2]["waiting_on"] == "work"
    assert [s["label"] for s in strip["stages"]] == ["Work", "Merged", "Active", "Verified", "Accept"]
    assert [s["owner"] for s in strip["stages"][1:4]] == ["pa", "pa", "pa"]


def test_next_checkpoint_activates_once_work_stops():
    requirement = {"schema_version": 1, "revision": "r", "mode": "explicit_acceptance", "milestones": ["integrated", "verified"]}
    strip = _strip(requirement=requirement, completion={"satisfied": ["integrated"], "missing": ["verified", "acceptance"], "accepted": False},
                   session_active=False, group="motion", state_code="verification_pending")
    assert _states(strip)[:3] == [("work", "done"), ("integrated", "done"), ("verified", "active")]


def test_failure_marks_work_stuck():
    assert _strip(state_code="delivery_failed", group="attention", session_active=False)["stages"][0]["state"] == "stuck"


def test_not_started_card_is_owned_by_you():
    strip = _strip(state_code="not_started", group="quiet", session_active=False, has_execution=False, activity_at=None)
    assert strip["stages"][0] == {"id": "work", "label": "Work", "state": "todo", "owner": "you"}


def test_parallel_children_collapse_into_the_work_node():
    tally = parallel_tally(["done", "running", "running", "todo", "stuck"])
    assert tally == {"total": 5, "done": 1, "running": 2, "stuck": 1, "todo": 1}
    strip = _strip(session_active=False, children=["done", "running", "todo"])
    work = strip["stages"][0]
    assert work["state"] == "active" and work["parallel"]["running"] == 1 and len(strip["stages"]) == 2
    assert _strip(session_active=False, children=["done", "done"], state_code="completed", group="outcome")["stages"][0]["state"] == "done"
    assert _strip(session_active=False, children=["done", "stuck"], group="attention")["stages"][0]["state"] == "stuck"
    idle = _strip(session_active=False, children=["done", "todo"], group="quiet")
    assert idle["stages"][0]["state"] == "todo" and idle["pulse"] is None
    assert parallel_tally([]) is None


def test_child_status_comes_from_lane_and_dispatch():
    assert child_work_status({"lane": "done"}, "running") == "done"
    assert child_work_status({"lane": "active"}, "running") == "running"
    assert child_work_status({"lane": "active"}, "failed") == "stuck"
    assert child_work_status({"lane": "inbox"}) == "todo"


def test_present_work_item_exposes_the_strip_and_names_it_for_screen_readers():
    card = {"id": "c", "title": "Doc", "lane": "inbox", "updated_at": NOW.isoformat()}
    result = present_work_item(card, now=NOW)
    assert [s["id"] for s in result["stage_strip"]["stages"]] == ["work", "done"]
    assert "Work not started" in result["accessible_label"]
    with_children = present_work_item(card, children=["running", "done"], now=NOW)
    assert with_children["stage_strip"]["stages"][0]["parallel"]["total"] == 2


def _render(strip):
    from pathlib import Path

    import jinja2

    import pa.server as server

    root = Path(server.__file__).parent / "templates"
    env = jinja2.Environment(loader=jinja2.FileSystemLoader(root), autoescape=True)
    from pa.core.ui.work_presentation import relative_time

    env.globals["relative_time"] = lambda value: relative_time(value, now=NOW)
    return env.from_string('{% from "macros/cards.html" import stage_strip %}{{ stage_strip(strip) }}').render(strip=strip)


def test_markup_carries_state_in_attributes_and_has_no_prose():
    requirement = {"schema_version": 1, "revision": "r", "mode": "explicit_acceptance", "milestones": ["integrated", "verified"]}
    html = _render(_strip(requirement=requirement, children=["done", "running", "todo"],
                          completion={"satisfied": ["integrated"], "missing": ["verified", "acceptance"], "accepted": False}))
    assert 'data-stage="work" data-stage-state="active"' in html and 'aria-current="step"' in html
    assert 'data-stage="integrated" data-stage-state="done"' in html
    assert 'data-stage="accept" data-stage-state="todo" data-stage-owner="you"' in html
    assert html.count("pip-") == 3 and "1/3" in html
    assert "pulse-live" in html and "Just now" in html
    assert 'owner-agent' not in html  # the agent owning Work is implied, not worth a chip
    assert "<ol" not in _render(None)
