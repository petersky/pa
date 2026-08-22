"""Deterministic regressions for the shared work-state presentation model."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from pa.core.ui.work_presentation import (
    absolute_time,
    present_work_item,
    presentation_state,
    relative_time,
)

NOW = datetime(2026, 8, 6, 18, 0, tzinfo=UTC)
CARD = {
    "id": "card-1",
    "title": "Unicode 🧭 work with a deliberately long title",
    "lane": "waiting",
    "updated_at": (NOW - timedelta(minutes=5)).isoformat(),
}


def dispatch(
    state: str,
    *,
    phase: str = "implementing",
    summary: str = "Current checkpoint",
    freshness: str = "fresh",
    **extra,
):
    return {
        "dispatch_id": "dispatch-1",
        "session_id": "session-1",
        "target_instance_id": "worker-1",
        "state": state,
        "effective_state": state,
        "updated_at": (NOW - timedelta(seconds=20)).isoformat(),
        "progress": {
            "latest": {
                "phase": phase,
                "summary": summary,
                "occurred_at": (NOW - timedelta(seconds=20)).isoformat(),
            },
            "freshness": {
                "state": freshness,
                "last_activity_at": (NOW - timedelta(seconds=20)).isoformat(),
            },
        },
        **extra,
    }


def present(*, card=CARD, dispatch_value=None, session=None, watches=()):
    return present_work_item(
        card,
        dispatch=dispatch_value,
        session=session,
        watches=watches,
        target_instance_name="Monica",
        now=NOW,
    )


@pytest.mark.parametrize(
    ("seconds", "expected"),
    [
        (0, "Just now"), (9, "Just now"), (10, "10s ago"), (59, "59s ago"),
        (60, "1m ago"), (3599, "59m ago"), (3600, "1h ago"),
        (86399, "23h ago"), (86400, "1d ago"), (604799, "6d ago"),
        (-5, "Just now"), (-6, "In 6s"), (-60, "In 1m"),
    ],
)
def test_relative_time_boundaries_and_clock_skew(seconds: int, expected: str) -> None:
    assert relative_time(NOW - timedelta(seconds=seconds), now=NOW) == expected


def test_relative_and_absolute_time_are_timezone_and_process_locale_stable() -> None:
    value = datetime.fromisoformat("2026-08-06T10:59:40-07:00")
    assert relative_time(value, now=NOW) == "20s ago"
    assert absolute_time(value) == "2026-08-06 17:59 UTC"
    assert relative_time("not-a-time", now=NOW) == "Time unavailable"


@pytest.mark.parametrize(
    ("domain", "state", "label"),
    [
        ("lane", "done", "Done"),
        ("dispatch", "acknowledged", "Completed"),
        ("session", "completed", "Session ended"),
        ("goal", "achieved", "Achieved"),
        ("freshness", "stale", "Stale"),
        ("dispatch", "failed", "Failed"),
    ],
)
def test_domain_states_have_distinct_stable_presentation_language(domain, state, label) -> None:
    result = presentation_state(domain, state)
    assert result["state"] == state
    assert result["label"] == label
    assert result["explanation"]


def test_active_turn_and_tool_override_idle_runtime_and_completed_checkpoint() -> None:
    value = dispatch(
        "completed", phase="completed", summary="Earlier checkpoint completed"
    )
    result = present(
        dispatch_value=value,
        session={
            "state": "idle",
            "turn": {"state": "running"},
            "activity": {"active_tool": {"name": "git apply …"}},
        },
    )

    assert result["group"] == "motion"
    assert result["state_label"] == "Working"
    assert result["summary"] == "git apply …"
    assert result["action"]["kind"] == "open_agent"


def test_idle_runtime_is_not_presented_as_active_and_terminal_dispatch_wins() -> None:
    idle = present(session={"state": "idle", "connected": True})
    terminal = present(
        dispatch_value=dispatch("completed", phase="completed"),
        session={"state": "idle", "connected": True},
    )

    assert idle["group"] == "quiet"
    assert idle["state_label"] == "Agent idle"
    assert terminal["group"] == "outcome"
    assert terminal["state_label"] == "Completed"


def test_stale_progress_on_current_dispatch_needs_inspection() -> None:
    result = present(dispatch_value=dispatch("running", freshness="stale"))

    assert result["attention"] is True
    assert result["attention_code"] == "stale_progress"
    assert result["action"]["label"] == "Inspect progress"


@pytest.mark.parametrize(
    ("state", "label"),
    [
        ("waiting_capacity", "Waiting for capacity"),
        ("queued", "Queued"),
        ("checking_sync", "Checking fleet state"),
    ],
)
def test_stale_starting_dispatch_remains_autonomous_motion(
    state: str, label: str
) -> None:
    result = present(
        dispatch_value=dispatch(state, freshness="disconnected", session_id=None)
    )

    assert result["group"] == "motion"
    assert result["attention"] is False
    assert result["state_label"] == label
    assert result["action"]["kind"] == "open_card"
    assert result["action_explanation"] == (
        "No operator action needed; startup is in progress."
    )


def test_waiting_lane_without_action_is_not_attention() -> None:
    result = present()

    assert result["group"] == "quiet"
    assert result["attention"] is False
    assert result["action_explanation"]


def test_structured_operator_input_exposes_respond_action() -> None:
    value = dispatch("running")
    value["progress"]["latest"]["operator_input"] = {
        "kind": "choice",
        "prompt": "Choose the release target",
        "choices": [{"id": "local", "label": "Local"}],
    }

    result = present(
        dispatch_value=value,
        session={"state": "working", "turn": {"state": "running"}},
    )

    assert result["attention_code"] == "operator_input"
    assert result["summary"] == "Choose the release target"
    assert result["action"]["kind"] == "respond"


@pytest.mark.parametrize(
    ("extra", "code", "label"),
    [
        (
            {
                "completion_outbox": {
                    "classification": "permanent_failure",
                    "last_error": "Authority delivery rejected",
                }
            },
            "delivery_failure",
            "Delivery failed",
        ),
        (
            {
                "card_reconciliation": {
                    "state": "blocked",
                    "last_dependency_error": "Destination branch diverged",
                }
            },
            "reconciliation_failure",
            "Reconciliation blocked",
        ),
    ],
)
def test_delivery_and_reconciliation_failures_are_actionable(
    extra, code, label
) -> None:
    result = present(dispatch_value=dispatch("completion_pending", **extra))

    assert result["attention_code"] == code
    assert result["state_label"] == label
    assert result["action"]["kind"] == "inspect"


@pytest.mark.parametrize(
    ("state", "can_retry", "group", "action"),
    [
        ("completed", False, "outcome", "open_card"),
        ("failed", False, "outcome", "open_card"),
        ("cancelled", True, "attention", "retry"),
    ],
)
def test_terminal_outcomes_and_explicit_retry_decisions(
    state: str, can_retry: bool, group: str, action: str
) -> None:
    result = present(
        dispatch_value=dispatch(state, can_retry=can_retry, last_error="Stopped")
    )

    assert result["group"] == group
    assert result["action"]["kind"] == action
    assert "Unicode 🧭" in result["accessible_label"]
    assert result["relative_time"] == "20s ago"
    assert result["absolute_time"] == "2026-08-06 17:59 UTC"


def test_actionable_review_gate_has_contextual_review_link() -> None:
    result = present(
        watches=[
            {
                "status": "active",
                "pr_url": "https://github.com/petersky/pa/pull/42",
                "state": {
                    "gate": {
                        "actionable": True,
                        "reasons": ["One requested change is unresolved"],
                    }
                },
            }
        ]
    )

    assert result["attention_code"] == "review_gate"
    assert result["action"] == {
        "kind": "review",
        "label": "Review",
        "href": "https://github.com/petersky/pa/pull/42",
        "external": True,
    }


@pytest.mark.parametrize(
    ("card", "expected"),
    [
        (
            {
                **CARD,
                "summary": "Legacy body-derived text",
                "summary_source": "fallback",
                "summary_status": "disabled",
                "summary_stale": True,
            },
            "Summary generation is disabled.",
        ),
        (
            {
                **CARD,
                "summary": "Stale legacy body-derived text",
                "summary_source": "fallback",
                "summary_status": "stale",
                "summary_stale": True,
            },
            "Summary pending.",
        ),
        (
            {
                **CARD,
                "summary": "Current authored summary",
                "summary_source": "manual",
                "summary_status": "ready",
                "summary_stale": False,
            },
            "Current authored summary",
        ),
    ],
)
def test_card_summary_lifecycle_outweighs_legacy_fallback_text(card, expected) -> None:
    result = present(card=card)

    assert result["summary"] == expected


@pytest.mark.parametrize(
    "watch",
    [
        {
            "status": "active",
            "retired_at": (NOW - timedelta(hours=1)).isoformat(),
            "last_error": "Preserved historical supervisor failure",
            "state": {"gate": {"actionable": True, "reasons": ["Old gate"]}},
        },
        {
            "status": "merged",
            "last_error": "Preserved pre-merge failure",
            "state": {"gate": {"actionable": True, "reasons": ["Old gate"]}},
        },
    ],
)
def test_retired_or_terminal_watch_history_never_drives_current_attention(watch) -> None:
    result = present(watches=[watch])

    assert result["group"] == "quiet"
    assert result["attention"] is False


def test_startup_dispatch_failure_on_done_card_shows_completed_outcome() -> None:
    done_card = {**CARD, "lane": "done"}
    value = dispatch(
        "failed",
        can_retry=True,
        last_error="blocking operation 'sqlite.card_write' exceeded 30.000s",
        completion_outbox={
            "pending": False,
            "last_error": None,
            "classification": None,
        },
        agent_turn={"ended": False, "completed": False, "stop_reason": None},
    )

    result = present(card=done_card, dispatch_value=value)

    assert result["group"] == "outcome"
    assert result["state_label"] == "Completed"
    assert result["attention"] is False


def test_startup_dispatch_failure_does_not_surface_as_delivery_failed() -> None:
    value = dispatch(
        "failed",
        can_retry=True,
        last_error="blocking operation 'sqlite.card_write' exceeded 30.000s",
        completion_outbox={
            "pending": False,
            "last_error": "blocking operation 'sqlite.card_write' exceeded 30.000s",
            "classification": None,
        },
        agent_turn={"ended": False, "completed": False, "stop_reason": None},
    )

    result = present(dispatch_value=value)

    assert result["attention_code"] != "delivery_failure"
    assert result["state_label"] != "Delivery failed"
