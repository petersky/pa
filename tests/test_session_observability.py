from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

from pa.domain.models import AgentSession, TranscriptEvent
from pa.execution.observability import (
    SESSION_OBSERVABILITY_VERSION,
    build_session_observability,
    diagnostic_timeline,
)

NOW = datetime(2026, 7, 28, 12, 0, tzinfo=UTC)


def event(seq: int, kind: str, payload: dict, age: int) -> TranscriptEvent:
    return TranscriptEvent(
        session_id="session-1",
        seq=seq,
        event_type=kind,
        payload=payload,
        created_at=NOW - timedelta(seconds=age),
    )


def runtime(*, connected: bool = True, observed_age: int = 2):
    prompt = SimpleNamespace(id="followup-1", source="dispatch_followup")
    return SimpleNamespace(
        connected=connected,
        _closed=False,
        _in_flight=prompt,
        _turn_started_at=NOW - timedelta(minutes=7),
        _runtime_observed_at=NOW - timedelta(seconds=observed_age),
        _connection_generation=2,
        _queue=[],
        _queue_paused=False,
        connection=SimpleNamespace(_proc=SimpleNamespace(pid=42, returncode=None)),
    )


def session(**updates):
    values = {
        "id": "session-1",
        "agent_name": "codex",
        "dispatch_id": "dispatch-1",
        "card_id": "card-1",
        "origin_instance_id": "monica",
        "origin_instance_name": "Monica",
        "authority_instance_id": "macbook",
        "status": "prompting",
        "updated_at": NOW - timedelta(seconds=3),
    }
    values.update(updates)
    return AgentSession(**values)


def test_followup_turn_is_independent_of_terminal_dispatch() -> None:
    events = [
        event(1, "queue_enqueued", {"id": "initial", "position": 0}, 900),
        event(2, "user_message", {"id": "initial", "source": "dispatch"}, 899),
        event(
            3,
            "turn_completed",
            {"queued_prompt_id": "initial", "stop_reason": "end_turn"},
            600,
        ),
        event(4, "queue_enqueued", {"id": "followup-1", "position": 0}, 430),
        event(
            5,
            "user_message",
            {"id": "followup-1", "source": "dispatch_followup"},
            420,
        ),
    ]
    result = build_session_observability(
        session(),
        runtime=runtime(),
        events=events,
        instance_id="monica",
        instance_name="Monica",
        reconciliation={"state": "completed"},
        now=NOW,
    )

    assert result["schema_version"] == SESSION_OBSERVABILITY_VERSION
    assert result["turn"]["id"] == "followup-1"
    assert result["turn"]["state"] == "running"
    assert result["turns"][0]["state"] == "completed"
    assert result["completion"]["card_reconciliation"] == "completed"
    assert result["liveness"]["classification"] == "quiet_active"


def test_missing_runtime_for_active_run_is_workflow_waiting_not_healthy_or_idle() -> None:
    result = build_session_observability(
        session(status="connected"),
        runtime=None,
        events=[],
        instance_id="monica",
        instance_name="Monica",
        now=NOW,
    )
    assert result["liveness"]["classification"] == "workflow_waiting"
    assert result["liveness"]["heartbeat_age_ms"] is None
    assert result["transport"]["connected"] is False


def test_retry_resets_completion_and_command_receipts_do_not_create_turns() -> None:
    events = [
        event(1, "queue_enqueued", {"id": "same", "position": 0}, 30),
        event(2, "error", {"queued_prompt_id": "same"}, 25),
        event(3, "command_result", {"id": "not-a-prompt"}, 20),
        event(4, "queue_enqueued", {"id": "same", "position": 0}, 15),
        event(5, "queue_dequeued", {"id": "same"}, 10),
        event(6, "prompt_blocked", {"queued_prompt_id": "same", "reason": "approval required"}, 5),
    ]
    result = build_session_observability(
        session(config_json={"durable_runtime": {"lifecycle": "admission_blocked", "queued_prompts": [{"id": "same", "source": "ui"}]}}),
        runtime=None,
        events=events,
        instance_id="monica",
        instance_name="Monica",
        now=NOW,
    )
    assert [turn["id"] for turn in result["turns"]] == ["same"]
    assert result["turns"][0]["state"] == "blocked"
    assert result["turns"][0]["completed_at"] is None
    assert result["turn"]["state"] == "blocked"
    assert result["presentation"]["turn"]["state"] == "blocked"


def test_diagnostics_redact_prompts_and_raw_tool_data() -> None:
    events = [
        event(
            1,
            "user_message",
            {
                "id": "prompt-1",
                "source": "api",
                "message": "password=secret private instructions",
            },
            2,
        ),
        event(
            2,
            "tool_call",
            {
                "title": "Run tests",
                "kind": "test",
                "status": "running",
                "raw_output": "token=secret",
                "command": "pytest --password secret",
            },
            1,
        ),
    ]
    timeline = diagnostic_timeline(events)
    assert timeline[0]["payload"] == {"id": "prompt-1", "source": "api"}
    assert timeline[1]["payload"] == {
        "title": "Run tests",
        "kind": "test",
        "status": "running",
    }
