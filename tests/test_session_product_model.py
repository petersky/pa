from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.execution.session_presentation import build_session_presentation
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.session_lifecycle import SessionLifecyclePolicy


NOW = datetime(2026, 9, 5, 12, 0, tzinfo=UTC)


def session(**updates) -> AgentSession:
    values = {
        "id": "session-1",
        "agent_name": "codex",
        "status": "available",
        "purpose": "chat",
        "control_mode": "human",
        "workflow_state": "not_applicable",
        "updated_at": NOW,
        "human_activity_at": NOW,
    }
    values.update(updates)
    return AgentSession(**values)


def test_presentation_contract_keeps_connection_turn_and_workflow_separate() -> None:
    chat = session()
    ready = build_session_presentation(chat, runtime=None, now=NOW)
    assert ready["version"] == 1
    assert ready["display_status"] == "Ready"
    assert ready["connection"]["state"] == "not_started"
    assert ready["turn"]["state"] == "idle"
    assert "recover" in ready["permitted_actions"]

    queued = chat.model_copy(
        update={
            "config_json": {
                "durable_runtime": {
                    "lifecycle": "queued",
                    "queued_prompts": [{"id": "prompt-1", "source": "ui"}],
                }
            }
        }
    )
    queued_view = build_session_presentation(queued, runtime=None, now=NOW)
    assert queued_view["display_status"] == "Restoring your work"
    assert queued_view["queue"] == {
        "count": 1,
        "reason": "waiting_for_recovery",
    }
    assert queued_view["connection"]["state"] == "disconnected"

    disconnected_runtime = SimpleNamespace(
        _closed=False,
        connected=False,
        prompting=True,
        _queue=[],
        _in_flight=SimpleNamespace(id="prompt-1"),
        _pending_permissions={},
        _pending_elicitations={},
    )
    disconnected_view = build_session_presentation(
        queued, runtime=disconnected_runtime, now=NOW
    )
    assert disconnected_view["display_status"] == "Restoring your work"
    assert disconnected_view["connection"]["state"] == "disconnected"
    assert disconnected_view["turn"]["state"] == "queued"


def test_presentation_reports_takeover_interactions_and_job_outcomes() -> None:
    run = session(
        purpose="automated_run",
        control_mode="human",
        workflow_state="active",
        config_json={
            "durable_runtime": {
                "queued_prompts": [
                    {"id": "automatic", "source": "pr-supervisor"}
                ]
            }
        },
    )
    taken_over = build_session_presentation(run, now=NOW)
    assert taken_over["display_status"] == "Taken over"
    assert taken_over["queue"]["reason"] == "automation_paused_for_takeover"
    assert "return_to_automation" in taken_over["permitted_actions"]

    waiting_job = session(
        purpose="one_shot_job",
        control_mode="automation",
        workflow_state="active",
        config_json={
            "durable_runtime": {
                "pending_interaction": {
                    "kind": "input",
                    "action": "Choose a destination.",
                }
            }
        },
    )
    assert build_session_presentation(waiting_job, now=NOW)["display_status"] == "Needs you"

    failed_job = waiting_job.model_copy(
        update={
            "workflow_state": "validation_failed",
            "workflow_outcome": {"summary": "The result did not validate."},
            "config_json": {},
        }
    )
    failed = build_session_presentation(failed_job, now=NOW)
    assert failed["display_status"] == "Validation failed"
    assert failed["explanation"] == "The result did not validate."


def test_dispatch_turn_completion_does_not_invent_workflow_success() -> None:
    run = session(
        purpose="automated_run",
        control_mode="automation",
        workflow_state="active",
    )
    unsettled = SimpleNamespace(
        state="completed",
        acknowledged_at=None,
        reconciliation_state="pending",
        followup_turns=[],
        evaluated_outcome={},
    )
    view = build_session_presentation(run, dispatch=unsettled, now=NOW)
    assert view["workflow"]["state"] == "active"
    assert view["display_status"] == "Running"

    settled = SimpleNamespace(
        state="completed",
        acknowledged_at=NOW,
        reconciliation_state="completed",
        followup_turns=[],
        evaluated_outcome="attempt_succeeded",
    )
    assert build_session_presentation(run, dispatch=settled, now=NOW)["workflow"]["state"] == "active"
    job = run.model_copy(update={"purpose": "one_shot_job"})
    assert build_session_presentation(job, dispatch=settled, now=NOW)["workflow"]["state"] == "succeeded"


@pytest.mark.asyncio
async def test_chat_lifecycle_never_closes_for_idle_card_or_duplicate() -> None:
    old = session(
        card_id="deleted-card",
        updated_at=NOW - timedelta(days=5),
        human_activity_at=NOW - timedelta(days=5),
    )
    newer = session(
        id="session-2",
        card_id="deleted-card",
        updated_at=NOW - timedelta(days=1),
    )
    manager = SimpleNamespace(
        get=lambda _session_id: None,
        store=SimpleNamespace(get_card=lambda *_args, **_kwargs: None),
        workspace_manager=SimpleNamespace(_status=lambda _path: (False, False)),
        settings=SimpleNamespace(agent_session_idle_retention_hours=24),
    )
    policy = SessionLifecyclePolicy(manager, {})
    decision = await policy._decision(
        old,
        sessions=[old, newer],
        dispatches=[],
        watches=[],
        leases=[],
        now=NOW,
    )
    assert decision == ("retained", "conversation_available")
    await policy.close()


@pytest.mark.asyncio
async def test_idle_chat_releases_only_its_provider_process() -> None:
    old = session(
        status="idle",
        updated_at=NOW - timedelta(days=2),
        human_activity_at=NOW - timedelta(days=2),
    )
    runtime = SimpleNamespace(
        prompting=False,
        _queue=[],
        _pending_permissions={},
        _pending_elicitations={},
        _transcript_buffer=[],
        _transcript_queue=SimpleNamespace(empty=lambda: True),
    )
    manager = SimpleNamespace(
        get=lambda _session_id: runtime,
        store=SimpleNamespace(get_card=lambda *_args, **_kwargs: None),
        workspace_manager=SimpleNamespace(_status=lambda _path: (False, False)),
        settings=SimpleNamespace(agent_session_idle_retention_hours=24),
    )
    policy = SessionLifecyclePolicy(manager, {})
    decision = await policy._decision(
        old,
        sessions=[old],
        dispatches=[],
        watches=[],
        leases=[],
        now=NOW,
    )
    assert decision == ("release", "idle_process_retention_expired")
    await policy.close()


def test_idle_expiry_migration_reopens_chat_but_explicit_close_archives(tmp_path) -> None:
    database = tmp_path / "pa.db"
    projection = CardProjection(database)
    idle = session(id="idle-chat", status="idle")
    explicit = session(id="explicit-chat", status="idle")
    later_explicit = session(id="later-explicit-chat", status="idle")
    projection.save_session(idle)
    projection.save_session(explicit)
    projection.save_session(later_explicit)
    projection.close_session(idle.id, reason="auto:idle_retention_expired")
    projection.close_session(explicit.id, reason="user_close")
    projection.close_session(later_explicit.id, reason="auto:idle_retention_expired")
    reopened = projection.get_session(later_explicit.id)
    reopened.status = "available"
    projection.save_session(reopened)
    projection.close_session(later_explicit.id, reason="user_close")
    incorrectly_reopened = projection.get_session(later_explicit.id)
    incorrectly_reopened.status = "available"
    incorrectly_reopened.archived_at = None
    incorrectly_reopened.archive_reason = None
    projection.save_session(incorrectly_reopened)

    migrated = CardProjection(database)
    restored_idle = migrated.get_session(idle.id)
    archived_explicit = migrated.get_session(explicit.id)
    archived_after_idle = migrated.get_session(later_explicit.id)
    assert restored_idle is not None
    assert restored_idle.status == "available"
    assert restored_idle.archived_at is None
    assert archived_explicit is not None
    assert archived_explicit.status == "closed"
    assert archived_explicit.archived_at is not None
    assert archived_after_idle is not None
    assert archived_after_idle.status == "closed"
    assert archived_after_idle.archived_at is not None


@pytest.mark.asyncio
async def test_takeover_holds_automatic_admission_until_return(tmp_path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    durable = session(
        purpose="automated_run",
        control_mode="human",
        workflow_state="active",
        status="idle",
    )
    store.save_session(durable)
    runtime = AgentSessionRuntime(manager, durable, initial_transcript_seq=0)
    runtime._start_drain = MagicMock()
    manager._runtimes[durable.id] = runtime

    automatic = runtime.enqueue("automated", source="pr-supervisor")
    assert automatic in runtime._queue
    runtime._start_drain.assert_not_called()

    human = runtime.enqueue("human", source="ui")
    assert human in runtime._queue
    runtime._start_drain.assert_called_once()
    runtime._start_drain.reset_mock()

    await runtime.set_control_mode("automation")
    assert store.get_session(durable.id).control_mode == "automation"
    runtime._start_drain.assert_called_once()


@pytest.mark.asyncio
async def test_recovery_coordinator_coalesces_same_session(tmp_path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    durable = session(
        status="recoverable_interrupted",
        purpose="automated_run",
        control_mode="automation",
        workflow_state="active",
        config_json={
            "durable_runtime": {
                "lifecycle": "queued",
                "queued_prompts": [{"id": "prompt-1", "source": "dispatch"}],
            }
        },
    )
    store.save_session(durable)
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def recover(_session_id: str):
        entered.set()
        await release.wait()
        current = store.get_session(durable.id)
        fake = SimpleNamespace(session=current)

        async def save() -> None:
            store.save_session(fake.session)

        fake._save_session_preserving_external_browser_async = save
        return fake

    manager.recover_session = recover
    await manager._recovery_once(now=NOW)
    await entered.wait()
    await manager._recovery_once(now=NOW)
    assert list(manager._recovery_tasks) == [durable.id]
    assert manager._recovery_metrics["coalesced"] == 1
    release.set()
    await asyncio.gather(*list(manager._recovery_tasks.values()))
    assert manager._recovery_metrics["succeeded"] == 1


@pytest.mark.asyncio
async def test_actionable_recovery_error_stops_retry_with_specific_remedy(tmp_path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    durable = session(
        status="recoverable_interrupted",
        purpose="automated_run",
        control_mode="automation",
        workflow_state="active",
    )
    store.save_session(durable)
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    state = await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(durable),
        RuntimeError("401 unauthorized: API key is missing"),
    )
    persisted = store.get_session(durable.id)
    assert state == "recovery_blocked"
    assert persisted is not None
    assert persisted.recovery_json["code"] == "auth_missing"
    assert persisted.recovery_json["next_retry_at"] is None
    assert "authentication" in persisted.recovery_json["remedy"].lower()


def test_completed_prompt_receipt_is_not_replayed_during_recovery(tmp_path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    durable = session(
        status="recoverable_interrupted",
        purpose="automated_run",
        control_mode="automation",
        workflow_state="active",
        config_json={
            "durable_runtime": {
                "lifecycle": "prompting",
                "in_flight": {
                    "id": "stable-prompt",
                    "message": "perform external work",
                    "source": "dispatch",
                },
                "queued_prompts": [
                    {
                        "id": "stable-prompt",
                        "message": "perform external work",
                        "source": "dispatch",
                    }
                ],
            }
        },
    )
    store.save_session(durable)
    store.append_transcript_events(
        [
            TranscriptEvent(
                session_id=durable.id,
                seq=1,
                event_type="turn_completed",
                payload={"queued_prompt_id": "stable-prompt"},
            )
        ]
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    snapshot = manager._snapshot_from_persisted(store.get_session(durable.id))
    assert snapshot.in_flight is None
    assert snapshot.queued_prompts == []
