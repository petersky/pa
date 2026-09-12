from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi import HTTPException
from typer.testing import CliRunner

from pa.cli.main import app
from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate, RestartHandoff
from pa.domain.projection import CardProjection
from pa.instance.agent_session import (
    AgentSessionManager,
    AgentSessionRuntime,
    AgentStartupNotReady,
)
from pa.instance.quiesce import QueuedPrompt
from pa.modules.agent_chat import (
    RestartHandoffBody,
    list_restart_handoffs,
    recover_session as recover_normal_session,
    request_restart_handoff as request_normal_restart_handoff,
    retry_restart_handoff as retry_normal_restart_handoff,
)
from pa.modules.fleet import (
    AssignedRestartHandoffBody,
    AssignedRestartHandoffEditBody,
    edit_assigned_restart_handoff,
    request_assigned_restart_handoff,
)
from pa.modules.items import operation_outcome_api



def _record_completed_turn(store, runtime, item):
    from pa.domain.models import TranscriptEvent
    store.append_transcript_events([TranscriptEvent(session_id=runtime.session_id, seq=store.next_transcript_seq(runtime.session_id), event_type="turn_completed", payload={"queued_prompt_id": item.id, "stop_reason": "end_turn"})])


def _advance_restart_fixture(store, handoff_id, *, status, **kwargs):
    """Seed interruption boundaries using legal observed-version transitions."""
    from pa.domain.models import TranscriptEvent
    route = ["requested", "waiting_for_turn_end", "quiescing", "restarting", "resuming", "continuation_queued", "continuation_delivered"]
    current = store.get_restart_handoff(handoff_id)
    if status == "continuation_delivered":
        store.append_transcript_events([TranscriptEvent(session_id=current.session_id, seq=store.next_transcript_seq(current.session_id), event_type="turn_completed", payload={"queued_prompt_id": current.continuation_prompt_id})])
    targets = [status] if status == "failed" else route[route.index(current.status) + 1:route.index(status) + 1]
    for target in targets:
        current = store.update_restart_handoff(handoff_id, status=target, expected_status=current.status, expected_version=current.phase_version, owner_instance_id=current.instance_id or "local", **(kwargs if target == status else {}))
    return current


def _queued_runtime(session):
    runtime = MagicMock(session=session, connected=True, _closed=False)
    runtime._queue = []
    runtime._queue_paused = False
    runtime._in_flight = None

    def enqueue(message, *, prompt_id, source, **kwargs):
        item = QueuedPrompt(id=prompt_id, message=message, source=source)
        runtime._queue.append(item)
        return item

    runtime.enqueue.side_effect = enqueue
    return runtime


def test_execution_binding_survives_primary_card_change(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    first = store.create_card(CardCreate(title="A"))
    second = store.create_card(CardCreate(title="B"))
    session = store.save_session(
        AgentSession(
            id="session-a",
            agent_name="codex",
            card_id=first.id,
            project_id="project-a",
            execution_binding={
                "version": 1,
                "execution_card_id": first.id,
                "execution_project_id": "project-a",
                "cwd": "/worktrees/a",
            },
        )
    )

    store.link_session_card(session.id, second.id, make_primary=True)
    changed = store.get_session(session.id)

    assert changed.card_id == second.id
    assert store.list_card_ids_for_session(session.id) == [first.id, second.id]
    assert changed.execution_binding["execution_card_id"] == first.id
    assert changed.execution_binding["execution_project_id"] == "project-a"
    assert changed.execution_binding["cwd"] == "/worktrees/a"


def test_execution_binding_can_be_finalized_after_provisioning(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = AgentSession(
        id="binding-finalize",
        agent_name="codex",
        card_id="card-a",
        project_id="project-a",
        execution_binding={
            "version": 1,
            "execution_card_id": "card-a",
            "execution_project_id": "project-a",
            "origin_instance_id": "instance-a",
        },
    )
    store.save_session(session)
    session.execution_binding.update(
        repository_ids=["repo-a"],
        worktree_paths=["/worktrees/a"],
        lease_ids=["lease-a"],
        branch="pa/card-a-session-a",
        base_sha="abc123",
        cwd="/worktrees/a",
    )

    store.save_session(session)
    minimal = {
        "version": 1,
        "execution_card_id": "card-a",
        "execution_project_id": "project-a",
        "origin_instance_id": "instance-a",
    }
    assert store.get_session(session.id).execution_binding == minimal

    finalized = store.set_session_execution_binding(
        session.id,
        session.execution_binding,
        reason="workspace_materialized",
        expected_binding=minimal,
    )

    assert finalized.execution_binding == session.execution_binding
    history = store.list_session_execution_binding_history(session.id)
    assert history == [
        {
            "id": history[0]["id"],
            "session_id": session.id,
            "reason": "workspace_materialized",
            "prior_binding": minimal,
            "binding": session.execution_binding,
            "changed_at": history[0]["changed_at"],
        }
    ]


def test_workspace_preparation_persists_complete_binding_not_minimal_seed(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(
            id="fresh-materialization",
            agent_name="codex",
            card_id="card-a",
            project_id=None,
            origin_instance_id="instance-a",
        )
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager.workspace_manager.list = MagicMock(return_value=[])
    workspace = MagicMock(cwd="/worktrees/a", repositories=[])
    workspace.execution_context.return_value = {
        "cwd": "/worktrees/a",
        "writable_roots": ["/worktrees/a"],
        "dependency_cache": "/deps",
        "repositories": [
            {
                "repository_id": "repo-a",
                "worktree_path": "/worktrees/a",
                "lease_id": "lease-a",
                "branch": "pa/card-a-session-a",
                "base_sha": "abc123",
            }
        ],
    }
    manager.workspace_manager.scratch_workspace = MagicMock(return_value=workspace)

    asyncio.run(
        manager._prepare_workspace(
            session, requested_cwd=None, provider_id="codex"
        )
    )

    binding = store.get_session(session.id).execution_binding
    assert binding["execution_card_id"] == "card-a"
    assert binding["repository_ids"] == ["repo-a"]
    assert binding["worktree_paths"] == ["/worktrees/a"]
    assert binding["lease_ids"] == ["lease-a"]
    assert binding["branch"] == "pa/card-a-session-a"
    assert binding["base_sha"] == "abc123"
    assert binding["cwd"] == "/worktrees/a"
    assert [
        item["reason"]
        for item in store.list_session_execution_binding_history(session.id)
    ] == ["workspace_binding_initialized"]


def test_execution_binding_materialization_cannot_retarget_or_drop_fence(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    binding = {
        "version": 1,
        "execution_card_id": "card-a",
        "execution_project_id": "project-a",
        "origin_instance_id": "instance-a",
        "cwd": "/worktrees/a",
    }
    store.save_session(
        AgentSession(
            id="binding-immutable",
            agent_name="codex",
            execution_binding=binding,
        )
    )

    with pytest.raises(ValueError, match="immutable provenance"):
        store.set_session_execution_binding(
            "binding-immutable",
            {**binding, "execution_card_id": "card-b"},
            reason="workspace_materialized",
            expected_binding=binding,
        )
    without_cwd = dict(binding)
    without_cwd.pop("cwd")
    with pytest.raises(ValueError, match="immutable provenance"):
        store.set_session_execution_binding(
            "binding-immutable",
            without_cwd,
            reason="workspace_materialized",
            expected_binding=binding,
        )

    assert store.get_session("binding-immutable").execution_binding == binding
    assert store.list_session_execution_binding_history("binding-immutable") == []


def test_restart_handoff_idempotency_is_content_fenced(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(AgentSession(id="s", agent_name="codex"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()

    first = asyncio.run(
        manager.request_restart_handoff(
            session_id="s", continuation_prompt="Continue safely", idempotency_key="stable"
        )
    )
    duplicate = asyncio.run(
        manager.request_restart_handoff(
            session_id="s", continuation_prompt="Continue safely", idempotency_key="stable"
        )
    )

    assert duplicate.id == first.id
    assert duplicate.continuation_prompt_id == first.continuation_prompt_id
    assert len(store.list_restart_handoffs(session_id="s")) == 1
    with pytest.raises(ValueError, match="nonterminal restart handoff"):
        asyncio.run(
            manager.request_restart_handoff(
                session_id="s", continuation_prompt="Also continue", idempotency_key="other"
            )
        )
    # Each asyncio.run closes its loop; a same-key receipt may safely re-arm
    # after the prior task has completed without creating a second receipt.
    assert manager._execute_restart_handoff.await_count == 2
    with pytest.raises(ValueError, match="different content"):
        asyncio.run(
            manager.request_restart_handoff(
                session_id="s", continuation_prompt="Different", idempotency_key="stable"
            )
        )


def test_restart_handoff_serializes_nonterminal_requests_per_session(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(AgentSession(id="s", agent_name="codex"))

    def create(key: str) -> RestartHandoff:
        return store.create_restart_handoff(
            RestartHandoff(
                session_id="s",
                idempotency_key=key,
                continuation_prompt=f"continue {key}",
                continuation_prompt_id=f"prompt-{key}",
            )
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda key: _capture_handoff(create, key), ("a", "b")))

    created = [value for value in outcomes if isinstance(value, RestartHandoff)]
    rejected = [value for value in outcomes if isinstance(value, ValueError)]
    assert len(created) == 1
    assert len(rejected) == 1
    assert "nonterminal restart handoff" in str(rejected[0])
    assert create(created[0].idempotency_key).id == created[0].id

    _advance_restart_fixture(store, created[0].id, status="continuation_delivered")
    assert create("later").idempotency_key == "later"
    _advance_restart_fixture(store,
        store.list_restart_handoffs(session_id="s")[-1].id,
        status="failed",
    )
    assert create("after-failure").idempotency_key == "after-failure"


def _capture_handoff(call, key: str) -> RestartHandoff | ValueError:
    try:
        return call(key)
    except ValueError as exc:
        return exc


def test_restart_handoff_listing_requires_session_owner_or_admin(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(
        AgentSession(id="private", agent_name="codex", principal_id="user:owner")
    )
    store.create_restart_handoff(
        RestartHandoff(
            session_id="private",
            idempotency_key="secret",
            continuation_prompt="agent-authored private continuation",
            continuation_prompt_id="private-prompt",
        )
    )
    manager = SimpleNamespace(store=store, get=lambda _: None)
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = True
    request.state.user.role = "member"

    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:other"),
        pytest.raises(HTTPException) as denied,
    ):
        list_restart_handoffs(request, "private")
    assert denied.value.status_code == 403

    request.state.user.role = "admin"
    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:other"),
    ):
        admin_result = list_restart_handoffs(request, "private")
    assert admin_result["handoffs"][0]["continuation_prompt"] == (
        "agent-authored private continuation"
    )

    request.state.user.role = "member"
    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:owner"),
    ):
        owner_result = list_restart_handoffs(request, "private")
    assert owner_result == admin_result


def test_managed_turn_cli_restart_requires_operator_emergency(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path)
    runner = CliRunner()
    managed_env = {"PA_BROWSER_SESSION_ID": "managed-session"}

    with (
        patch("pa.cli.main.get_settings", return_value=settings),
        patch("pa.cli.service.restart") as restart_service,
    ):
        ordinary = runner.invoke(app, ["restart"], env=managed_env)
        no_quiesce = runner.invoke(
            app, ["restart", "--no-acp-quiesce"], env=managed_env
        )
    assert ordinary.exit_code == 2
    assert no_quiesce.exit_code == 2
    assert "Refusing synchronous restart" in ordinary.output
    assert "operator emergency only" in no_quiesce.output
    restart_service.assert_not_called()

    with (
        patch("pa.cli.main.get_settings", return_value=settings),
        patch("pa.cli.service.restart") as restart_service,
        patch("pa.instance.quiesce.request_skip_quiesce"),
        patch("pa.cli.startup.print_service_ready"),
    ):
        override = runner.invoke(
            app,
            ["restart", "--no-acp-quiesce", "--operator-emergency"],
            env=managed_env,
        )
    assert override.exit_code == 0
    restart_service.assert_called_once()


def test_authenticated_normal_restart_handoff_post_and_get_ownership(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="owned", agent_name="codex", principal_id="user:owner")
    )
    manager = SimpleNamespace(store=store, get=lambda _: None, request_restart_handoff=AsyncMock())
    manager.request_restart_handoff.return_value = RestartHandoff(
        session_id=session.id,
        idempotency_key="owned-key",
        continuation_prompt="continue owned session",
        continuation_prompt_id="owned-prompt",
    )
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = True
    request.state.user.role = "member"
    body = RestartHandoffBody(
        continuation_prompt="continue owned session", idempotency_key="owned-key"
    )

    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:other"),
        pytest.raises(HTTPException) as denied,
    ):
        asyncio.run(request_normal_restart_handoff(request, session.id, body))
    assert denied.value.status_code == 403
    manager.request_restart_handoff.assert_not_awaited()

    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:owner"),
    ):
        posted = asyncio.run(request_normal_restart_handoff(request, session.id, body))
    assert posted["session_id"] == session.id
    manager.request_restart_handoff.assert_awaited_once_with(
        session_id=session.id,
        continuation_prompt="continue owned session",
        idempotency_key="owned-key",
    )

    store.create_restart_handoff(manager.request_restart_handoff.return_value)
    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:owner"),
    ):
        listed = list_restart_handoffs(request, session.id)
    assert listed["handoffs"][0]["continuation_prompt"] == "continue owned session"


def test_assigned_restart_handoff_derives_exact_durable_session() -> None:
    manager = SimpleNamespace(request_restart_handoff=AsyncMock())
    manager.request_restart_handoff.return_value = RestartHandoff(
        session_id="durable-session",
        idempotency_key="assigned-key",
        continuation_prompt="continue assigned work",
        continuation_prompt_id="assigned-prompt",
    )
    request = MagicMock()
    request.app.state.ctx.require_service.return_value = manager
    body = AssignedRestartHandoffBody(
        continuation_prompt="continue assigned work", idempotency_key="assigned-key"
    )
    record = SimpleNamespace(session_id="durable-session")

    with patch("pa.modules.fleet._assigned_local_dispatch", return_value=record):
        result = asyncio.run(request_assigned_restart_handoff(request, body))

    assert result["session_id"] == "durable-session"
    manager.request_restart_handoff.assert_awaited_once_with(
        session_id="durable-session",
        continuation_prompt="continue assigned work",
        idempotency_key="assigned-key",
    )


def test_assigned_restart_handoff_edit_derives_exact_durable_session() -> None:
    manager = SimpleNamespace(edit_restart_handoff=AsyncMock())
    manager.edit_restart_handoff.return_value = RestartHandoff(
        id="handoff-1",
        session_id="durable-session",
        idempotency_key="assigned-key",
        continuation_prompt="",
        continuation_prompt_id="assigned-prompt",
    )
    request = MagicMock()
    request.app.state.ctx.require_service.return_value = manager
    body = AssignedRestartHandoffEditBody(
        handoff_id="handoff-1", continuation_prompt=""
    )
    record = SimpleNamespace(session_id="durable-session")

    with patch("pa.modules.fleet._assigned_local_dispatch", return_value=record):
        result = asyncio.run(edit_assigned_restart_handoff(request, body))

    assert result["continuation_prompt"] == ""
    manager.edit_restart_handoff.assert_awaited_once_with(
        session_id="durable-session",
        handoff_id="handoff-1",
        continuation_prompt="",
    )


def test_startup_replays_continuation_once_into_exact_session(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="s", agent_name="codex", status="quiesced"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(
        manager.request_restart_handoff(
            session_id=session.id, continuation_prompt="Resume work", idempotency_key="once"
        )
    )
    _advance_restart_fixture(store, receipt.id, status="restarting")
    runtime = _queued_runtime(session)
    manager.recover_session = AsyncMock(return_value=runtime)

    asyncio.run(manager._resume_restart_handoffs())
    asyncio.run(manager._resume_restart_handoffs())

    runtime.enqueue.assert_called_once_with(
        "Resume work",
        prompt_id=receipt.continuation_prompt_id,
        source=f"restart-handoff:{receipt.id}",
        card_id=None,
        project_id=None,
        _defer_drain=True,
    )
    assert store.get_restart_handoff(receipt.id).status == "continuation_queued"


def test_restart_without_continuation_does_not_recover_or_enqueue(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="no-prompt", agent_name="codex", status="quiesced"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(
        manager.request_restart_handoff(
            session_id=session.id, continuation_prompt="", idempotency_key="no-prompt"
        )
    )
    _advance_restart_fixture(store, receipt.id, status="restarting")
    manager.recover_session = AsyncMock()

    asyncio.run(manager._resume_restart_handoffs())

    manager.recover_session.assert_not_awaited()
    persisted = store.get_restart_handoff(receipt.id)
    assert persisted.status == "restart_completed"
    assert persisted.continuation_prompt == ""


def test_pending_restart_continuation_can_be_edited_or_removed(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(AgentSession(id="edit", agent_name="codex"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(
        manager.request_restart_handoff(
            session_id="edit", continuation_prompt="first", idempotency_key="edit"
        )
    )
    edited = asyncio.run(
        manager.edit_restart_handoff(
            session_id="edit", handoff_id=receipt.id, continuation_prompt=""
        )
    )
    assert edited.continuation_prompt == ""
    _advance_restart_fixture(store, receipt.id, status="quiescing")
    with pytest.raises(ValueError, match="before PA begins quiescing"):
        asyncio.run(
            manager.edit_restart_handoff(
                session_id="edit", handoff_id=receipt.id, continuation_prompt="too late"
            )
        )


def test_long_turn_handoff_waits_for_turn_and_startup_fence_before_restart(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = CardProjection(tmp_path / "pa.db")
        session = store.save_session(AgentSession(id="long", agent_name="codex"))
        manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
        manager.begin_startup()
        runtime = _queued_runtime(session)
        runtime.prompting = True
        runtime._drain_transcripts = AsyncMock()
        manager.get = MagicMock(return_value=runtime)
        manager.quiesce = AsyncMock()

        with patch("pa.cli.service.request_restart") as restart:
            receipt = await manager.request_restart_handoff(
                session_id=session.id,
                continuation_prompt="continue after compacted long turn",
                idempotency_key="long-compacted",
            )
            task = manager._restart_handoff_tasks[receipt.id]
            await asyncio.sleep(0.15)
            restart.assert_not_called()

            runtime.prompting = False
            await asyncio.sleep(0.15)
            restart.assert_not_called()

            manager.complete_startup()
            await task

        runtime._flush_transcript.assert_called_once()
        runtime._drain_transcripts.assert_awaited_once()
        manager.quiesce.assert_awaited_once_with(
            reason=f"restart-handoff:{receipt.id}"
        )
        restart.assert_called_once()
        persisted = store.get_restart_handoff(receipt.id)
        assert persisted.status == "restarting"
        assert persisted.attempts == 1

    asyncio.run(scenario())


def test_startup_replay_runs_only_after_traffic_admission_is_ready(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="startup-fence", agent_name="codex", status="quiesced")
    )
    receipt = store.create_restart_handoff(
        RestartHandoff(
            session_id=session.id,
            idempotency_key="startup-fence",
            continuation_prompt="resume after ready",
            continuation_prompt_id="startup-fence-prompt",
            status="restarting",
        )
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager.begin_startup()
    runtime = _queued_runtime(session)
    manager.recover_session = AsyncMock(return_value=runtime)

    with pytest.raises(AgentStartupNotReady, match="recovery is still in progress"):
        asyncio.run(manager.resume_restart_handoffs_after_startup())
    runtime.enqueue.assert_not_called()

    manager.complete_startup()
    asyncio.run(manager.resume_restart_handoffs_after_startup())
    runtime.enqueue.assert_called_once()
    assert store.get_restart_handoff(receipt.id).status == "continuation_queued"


def test_same_key_retry_rearms_failed_restart_stage(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="retry-restart", agent_name="codex"))
    receipt = store.create_restart_handoff(
        RestartHandoff(
            session_id=session.id,
            idempotency_key="same-restart-key",
            continuation_prompt="same continuation",
            continuation_prompt_id="same-restart-prompt",
            status="failed",
            error="host scheduler unavailable",
            failure_stage="restarting",
            attempts=1,
        )
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()

    retried = asyncio.run(
        manager.request_restart_handoff(
            session_id=session.id,
            continuation_prompt=receipt.continuation_prompt,
            idempotency_key=receipt.idempotency_key,
        )
    )

    assert retried.id == receipt.id
    assert retried.status == "requested"
    assert retried.error is None
    assert retried.failure_stage is None
    manager._execute_restart_handoff.assert_awaited_once_with(receipt.id)


def test_operation_outcome_reports_failed_restart_receipt_truthfully(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="status-session", agent_name="codex", realm_id="default")
    )
    receipt = store.create_restart_handoff(
        RestartHandoff(
            session_id=session.id,
            idempotency_key="restart-status-key",
            continuation_prompt="private continuation",
            continuation_prompt_id="restart-status-prompt",
            status="failed",
            error="recovery unavailable",
            failure_stage="resuming",
            attempts=1,
        )
    )
    request = MagicMock()
    request.app.state.ctx.settings.primary_realm = "default"
    request.app.state.ctx.services.get.return_value = None

    with patch("pa.modules.items.get_store", return_value=store):
        outcome = operation_outcome_api(request, receipt.idempotency_key)

    assert outcome["operation"] == "agent_restart_handoff"
    assert outcome["status"] == "failed"
    assert outcome["recovery_state"] == "failed"
    assert outcome["recovery_action"] == "inspect_failure"
    assert outcome["worker_state"] == "unconfirmed"
    assert outcome["result"]["handoff_id"] == receipt.id
    assert outcome["result"]["failure_stage"] == "resuming"
    assert "continuation_prompt" not in outcome["result"]


def test_restart_replay_appends_continuation_after_durable_queue(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="ordered", agent_name="codex"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(
        manager.request_restart_handoff(
            session_id=session.id,
            continuation_prompt="restart continuation",
            idempotency_key="ordered-restart",
        )
    )
    _advance_restart_fixture(store, receipt.id, status="restarting")
    runtime = AgentSessionRuntime(manager, session)
    runtime.connection = MagicMock(connected=True)
    runtime._queue_paused = True
    runtime._queue = [
        QueuedPrompt(id="first", session_id=session.id, message="already queued first"),
        QueuedPrompt(id="second", session_id=session.id, message="already queued second"),
    ]
    runtime._checkpoint_runtime = MagicMock()
    runtime._append_transcript = MagicMock()
    runtime._flush_transcript = MagicMock()
    manager.get = MagicMock(return_value=runtime)

    asyncio.run(manager._resume_restart_handoffs())

    assert [item.id for item in runtime._queue] == [
        "first",
        "second",
        receipt.continuation_prompt_id,
    ]


def test_recovered_continuation_is_queued_then_delivered_exactly_once(
    tmp_path: Path,
) -> None:
    async def scenario() -> None:
        store = CardProjection(tmp_path / "pa.db")
        session = store.save_session(
            AgentSession(id="delivery-once", agent_name="codex", status="quiesced")
        )
        receipt = store.create_restart_handoff(
            RestartHandoff(
                session_id=session.id,
                idempotency_key="delivery-once",
                continuation_prompt="deliver this once",
                continuation_prompt_id="delivery-once-prompt",
                status="restarting",
            )
        )
        manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
        runtime = AgentSessionRuntime(manager, session)
        runtime.connection = MagicMock()
        runtime._checkpoint_runtime = MagicMock()
        runtime._append_transcript = MagicMock()
        runtime._flush_transcript = MagicMock()
        runtime._run_prompt = AsyncMock(side_effect=lambda item: _record_completed_turn(store, runtime, item))
        manager.get = MagicMock(return_value=runtime)

        await manager._resume_restart_handoffs()
        assert store.get_restart_handoff(receipt.id).status in {
            "continuation_queued", "continuation_delivered"
        }
        if runtime._drain_task:
            await runtime._drain_task
        assert store.get_restart_handoff(receipt.id).status == "continuation_delivered"

        await manager._resume_restart_handoffs()
        runtime._run_prompt.assert_awaited_once()
        delivered = store.get_restart_handoff(receipt.id)
        assert delivered.delivered_at is not None
        assert delivered.continuation_prompt_id == receipt.continuation_prompt_id

    asyncio.run(scenario())


def test_legacy_mismatch_recovers_using_existing_workspace_fence(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = AgentSession(
        id="legacy", agent_name="codex", card_id="new-card", project_id="new-project",
        cwd="/worktrees/old",
    )
    store.save_session(session)
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    lease = SimpleNamespace(
        session_id=session.id, state="ready", repository_id="repo", card_id="old-card",
        project_id="old-project", worktree_path="/worktrees/old", id="lease",
        branch="pa/old", base_sha="abc",
    )
    manager.workspace_manager.list = MagicMock(return_value=[lease])
    workspace = MagicMock(cwd="/worktrees/old", repositories=[lease])
    workspace.execution_context.return_value = {
        "cwd": "/worktrees/old", "writable_roots": ["/worktrees/old"],
        "dependency_cache": "/deps",
        "repositories": [{
            "repository_id": "repo", "worktree_path": "/worktrees/old",
            "lease_id": "lease", "branch": "pa/old", "base_sha": "abc",
        }]
    }
    manager.workspace_manager.provision_project = MagicMock(return_value=workspace)
    store.get_project = MagicMock(return_value=SimpleNamespace(realm_id="default"))

    asyncio.run(
        manager._prepare_workspace(
            session, requested_cwd=session.cwd, provider_id="codex"
        )
    )

    manager.workspace_manager.provision_project.assert_called_once_with(
        project_id="old-project", session_id="legacy", card_id="old-card",
        realm_id="default", provider_id="codex", allow_concurrent=True,
    )
    persisted = store.get_session("legacy")
    assert persisted.card_id == "new-card"
    assert persisted.project_id == "new-project"
    assert persisted.execution_binding["execution_card_id"] == "old-card"
    assert persisted.execution_binding["legacy_mismatch"] is True


def test_handoff_never_falls_back_to_new_session(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="s", agent_name="codex", status="quiesced"))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(manager.request_restart_handoff(
        session_id=session.id, continuation_prompt="Continue", idempotency_key="failure"
    ))
    _advance_restart_fixture(store, receipt.id, status="restarting")
    manager.recover_session = AsyncMock(side_effect=RuntimeError("workspace blocker"))
    manager.create_session = AsyncMock()

    asyncio.run(manager._resume_restart_handoffs())

    manager.create_session.assert_not_called()
    failed = store.get_restart_handoff(receipt.id)
    assert failed.status == "failed"
    assert failed.error == "workspace blocker"


def test_failed_handoff_retry_recovers_exact_session_and_queues_once(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="repaired", agent_name="codex", status="quiesced")
    )
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager._execute_restart_handoff = AsyncMock()
    receipt = asyncio.run(
        manager.request_restart_handoff(
            session_id=session.id,
            continuation_prompt="deterministic continuation",
            idempotency_key="repair-once",
        )
    )
    _advance_restart_fixture(store, receipt.id, status="restarting")
    manager.recover_session = AsyncMock(side_effect=RuntimeError("exact workspace blocker"))
    manager.create_session = AsyncMock()

    asyncio.run(manager._resume_restart_handoffs())
    failed = store.get_restart_handoff(receipt.id)
    assert failed.status == "failed"
    assert failed.error == "exact workspace blocker"
    manager.create_session.assert_not_called()

    runtime = _queued_runtime(session)
    async def recover(*args, **kwargs):
        manager._runtimes[session.id] = runtime
        return runtime

    manager.recover_session = AsyncMock(side_effect=recover)
    first = asyncio.run(
        manager.retry_restart_handoff(session_id=session.id, handoff_id=receipt.id)
    )
    repeated = asyncio.run(
        manager.retry_restart_handoff(session_id=session.id, handoff_id=receipt.id)
    )

    assert first.status == "continuation_queued"
    assert repeated.status == "continuation_queued"
    manager.recover_session.assert_awaited_once_with(
        session.id, _startup_recovery=True, _defer_drain=True
    )
    runtime.enqueue.assert_called_once_with(
        "deterministic continuation",
        prompt_id=receipt.continuation_prompt_id,
        source=f"restart-handoff:{receipt.id}",
        card_id=None,
        project_id=None,
        _defer_drain=True,
    )
    assert store.get_restart_handoff(receipt.id).error is None


def test_handoff_retry_route_is_owned_and_restart_session_rearms_latest_failure(
    tmp_path: Path,
) -> None:
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(
        AgentSession(id="ui-repair", agent_name="codex", principal_id="user:owner")
    )
    receipt = store.create_restart_handoff(
        RestartHandoff(
            session_id=session.id,
            idempotency_key="ui-retry",
            continuation_prompt="continue after UI repair",
            continuation_prompt_id="ui-retry-prompt",
            status="failed",
            error="repository unavailable",
        )
    )
    manager = SimpleNamespace(store=store, get=lambda _: None, retry_restart_handoff=AsyncMock())
    manager.retry_restart_handoff.return_value = receipt.model_copy(
        update={"status": "continuation_queued", "error": None}
    )
    runtime = MagicMock()
    runtime.snapshot.side_effect = [
        {"restart_handoffs": [receipt.model_dump(mode="json")]},
        {
            "restart_handoffs": [
                manager.retry_restart_handoff.return_value.model_dump(mode="json")
            ]
        },
    ]
    manager.recover_session = AsyncMock(return_value=runtime)
    request = MagicMock()
    request.app.state.ctx.settings.auth_required = True
    request.state.user.role = "member"

    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:other"),
        pytest.raises(HTTPException) as denied,
    ):
        asyncio.run(
            retry_normal_restart_handoff(request, session.id, receipt.id)
        )
    assert denied.value.status_code == 403

    with (
        patch("pa.modules.agent_chat._require_session_traffic_ready", return_value=manager),
        patch("pa.modules.agent_chat.get_principal_id", return_value="user:owner"),
    ):
        recovered = asyncio.run(recover_normal_session(request, session.id))

    assert recovered["restart_handoffs"][0]["status"] == "continuation_queued"
    manager.recover_session.assert_awaited_once_with(
        session.id, provider_override=None
    )
    manager.retry_restart_handoff.assert_awaited_once_with(
        session_id=session.id, handoff_id=receipt.id
    )


def test_explicit_recovery_passes_durable_work_to_provider_start(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = CardProjection(tmp_path / "pa.db")
        queued = QueuedPrompt(id="original-queue-id", message="retain me", source="card-reconciliation:d")
        session = store.save_session(AgentSession(
            id="queue-recovery", agent_name="codex", external_session_id="original-provider",
            status="quiesced", config_json={"durable_runtime": {
                "queued_prompts": [queued.model_dump(mode="json")], "queue_paused": True,
            }},
        ))
        manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
        manager._prepare_workspace = AsyncMock(return_value={})
        with patch.object(AgentSessionRuntime, "start", new=AsyncMock()) as start:
            runtime = await manager.recover_session(session.id)
        args = start.await_args.kwargs
        assert args["resume_external_id"] == "original-provider"
        assert args["require_restore"] is True
        assert args["queue_paused"] is True
        assert [(p.id, p.message, p.source) for p in args["queued_prompts"]] == [
            (queued.id, queued.message, queued.source)
        ]
        assert runtime.session.id == session.id
    asyncio.run(scenario())


def test_queued_handoff_recovers_missing_runtime_without_reenqueuing(tmp_path: Path) -> None:
    async def scenario() -> None:
        store = CardProjection(tmp_path / "pa.db")
        session = store.save_session(AgentSession(id="queued-recovery", agent_name="codex"))
        receipt = store.create_restart_handoff(RestartHandoff(
            session_id=session.id, idempotency_key="queued", continuation_prompt="continue",
            continuation_prompt_id="original-continuation", status="continuation_queued",
        ))
        manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
        runtime = _queued_runtime(session)
        runtime._queue = [QueuedPrompt(id=receipt.continuation_prompt_id, message="continue")]
        async def recover(*args, **kwargs):
            manager._runtimes[session.id] = runtime
            return runtime
        manager.recover_session = AsyncMock(side_effect=recover)
        await asyncio.gather(manager._resume_restart_handoffs(), manager._resume_restart_handoffs())
        manager.recover_session.assert_awaited_once_with(session.id, _startup_recovery=True, _defer_drain=True)
        runtime.enqueue.assert_not_called()
        assert runtime._start_drain.called
        assert store.get_restart_handoff(receipt.id).status == "continuation_queued"
    asyncio.run(scenario())


@pytest.mark.parametrize("receipt_status", ["resuming", "continuation_queued", "failed"])
def test_completed_continuation_repairs_receipt_without_provider_replay(tmp_path: Path, receipt_status: str) -> None:
    from pa.domain.models import TranscriptEvent

    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="completed-continuation", agent_name="codex"))
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=session.id, idempotency_key="completed", continuation_prompt="continue",
        continuation_prompt_id="already-done", status=receipt_status,
        failure_stage="resuming" if receipt_status == "failed" else None,
    ))
    store.append_transcript_events([TranscriptEvent(
        session_id=session.id, seq=1, event_type="turn_completed",
        payload={"queued_prompt_id": "already-done", "stop_reason": "end_turn"},
    )])
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager.recover_session = AsyncMock()
    asyncio.run(manager._resume_restart_handoffs())
    manager.recover_session.assert_not_awaited()
    delivered = store.get_restart_handoff(receipt.id)
    assert delivered.status == "continuation_delivered"
    assert delivered.delivered_at is not None


def test_interrupted_restart_continuation_preserves_receipt_correlation(tmp_path: Path) -> None:
    from pa.instance.quiesce import SessionSnapshot

    session = AgentSession(id="interrupted", agent_name="codex")
    prompt = QueuedPrompt(id="stable-id", message="continue", source="restart-handoff:receipt")
    queue = AgentSessionManager._recovery_queue(
        SessionSnapshot(session_id=session.id, in_flight=prompt), session, {},
    )
    assert [(p.id, p.source, p.message) for p in queue] == [(prompt.id, prompt.source, prompt.message)]


@pytest.mark.parametrize("present", [True, False])
def test_queued_receipt_requires_matching_work_not_just_connection(tmp_path, present):
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="queue-proof", agent_name="codex"))
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=session.id, idempotency_key="queue-proof", continuation_prompt="continue",
        continuation_prompt_id="original", status="continuation_queued",
    ))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    runtime = _queued_runtime(session)
    if present:
        runtime._queue = [QueuedPrompt(id="original", message="continue")]
    manager._runtimes[session.id] = runtime
    asyncio.run(manager._resume_restart_handoffs())
    runtime.enqueue.assert_not_called()
    result = store.get_restart_handoff(receipt.id)
    assert result.status == ("continuation_queued" if present else "failed")
    if not present:
        assert "no matching" in result.error
        assert result.continuation_prompt_id == "original"


def test_crash_between_queue_checkpoint_and_receipt_does_not_reenqueue(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    session = store.save_session(AgentSession(id="checkpoint-gap", agent_name="codex"))
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=session.id, idempotency_key="gap", continuation_prompt="continue",
        continuation_prompt_id="original", status="resuming",
    ))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    runtime = _queued_runtime(session)
    runtime._queue = [QueuedPrompt(id="original", message="continue")]
    manager.recover_session = AsyncMock(return_value=runtime)
    asyncio.run(manager._resume_restart_handoffs())
    runtime.enqueue.assert_not_called()
    runtime._start_drain.assert_called_once()
    manager.recover_session.assert_awaited_once_with(
        session.id, _startup_recovery=True, _defer_drain=True,
    )
    assert store.get_restart_handoff(receipt.id).status == "continuation_queued"


def _human_handoff(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    session = store.save_session(AgentSession(
        id='human-chat', agent_name='codex', purpose='chat', control_mode='human',
        status='quiesced', external_session_id='exact-provider-thread',
    ))
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=session.id, idempotency_key='authorized-restart',
        continuation_prompt='Continue the authorized task',
        continuation_prompt_id='stable-continuation', status='restarting',
    ))
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    runtime = AgentSessionRuntime(manager, session)
    runtime.connection = MagicMock(cancel=AsyncMock())
    runtime._checkpoint_runtime = MagicMock()
    runtime._checkpoint_runtime_async = AsyncMock()
    runtime._append_transcript = MagicMock()
    runtime._flush_transcript = MagicMock()
    runtime._drain_transcripts = AsyncMock()
    runtime._run_prompt = AsyncMock(side_effect=lambda item: _record_completed_turn(store, runtime, item))
    manager.get = MagicMock(return_value=runtime)
    return store, manager, runtime, receipt


def test_human_authorized_restart_drains_once_and_holds_other_automation(tmp_path):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        runtime.enqueue('unrelated automation', source='evaluation:other')
        await manager._resume_restart_handoffs()
        assert runtime._drain_task is not None
        await runtime._drain_task
        await manager._resume_restart_handoffs()
        runtime._run_prompt.assert_awaited_once()
        assert runtime._run_prompt.call_args.args[0].id == receipt.continuation_prompt_id
        assert [item.message for item in runtime._queue] == ['unrelated automation']
        assert runtime.session.control_mode == 'human'
        assert runtime.session.external_session_id == 'exact-provider-thread'
        assert store.get_restart_handoff(receipt.id).status == 'continuation_delivered'
    asyncio.run(scenario())


@pytest.mark.parametrize('mismatch', ['receipt', 'session', 'prompt', 'message', 'binding', 'status', 'images', 'cwd', 'environment', 'principal', 'interrupt'])
def test_restart_source_is_not_authorization(tmp_path, mismatch):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        _advance_restart_fixture(store, receipt.id, status='continuation_queued')
        item = QueuedPrompt(id=receipt.continuation_prompt_id,
                            message=receipt.continuation_prompt,
                            session_id=runtime.session_id,
                            source='restart-handoff:' + receipt.id)
        if mismatch == 'receipt': item.source = 'restart-handoff:invented'
        elif mismatch == 'session': runtime.session.id = 'another-session'
        elif mismatch == 'prompt': item.id = 'different-prompt'
        elif mismatch == 'message': item.message = 'different instructions'
        elif mismatch == 'binding': runtime.session.execution_binding = {'cwd': '/other'}
        elif mismatch == 'status': _advance_restart_fixture(store, receipt.id, status='failed')
        elif mismatch == 'cwd': item.cwd = '/other'
        elif mismatch == 'environment': item.agent_env = {'EXTRA': 'unauthorized'}
        elif mismatch == 'principal': item.principal_id = 'other-user'
        elif mismatch == 'interrupt': item.publication_fence = True
        elif mismatch == 'images':
            from pa.instance.quiesce import ImageAttachment
            item.images = [ImageAttachment(name='extra', mime_type='image/png', data='aGk=')]
        runtime._queue = [item]
        runtime._start_drain()
        if runtime._drain_task:
            await runtime._drain_task
        runtime._run_prompt.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize('pause', ['pause', 'cancel'])
def test_restart_continuation_respects_operator_pause_and_cancel(tmp_path, pause):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        if pause == 'cancel': await runtime.cancel()
        else: runtime.pause_queue()
        await manager._resume_restart_handoffs()
        runtime._run_prompt.assert_not_called()
        assert store.get_restart_handoff(receipt.id).status == 'continuation_queued'
        assert runtime._queue_paused
        runtime.resume_queue()
        await manager._resume_restart_handoffs()
        await runtime._drain_task
        runtime._run_prompt.assert_awaited_once()
        assert runtime.session.control_mode == 'human'
    asyncio.run(scenario())


@pytest.mark.parametrize('pause_after_queue', [False, True])
def test_restart_continuation_waits_for_existing_user_turn(tmp_path, pause_after_queue):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        entered, release = asyncio.Event(), asyncio.Event()
        delivered = []
        async def run(item):
            delivered.append(item.id)
            if item.id == 'user-turn':
                runtime._in_flight = item
                entered.set()
                await release.wait()
                runtime._in_flight = None
            _record_completed_turn(store, runtime, item)
        runtime._run_prompt.side_effect = run
        runtime.enqueue('user instructions', source='ui', prompt_id='user-turn')
        await entered.wait()
        await manager._resume_restart_handoffs()
        assert delivered == ['user-turn']
        assert runtime._queue[0].id == receipt.continuation_prompt_id
        if pause_after_queue:
            await runtime.cancel()
        release.set()
        await runtime._drain_task
        if pause_after_queue:
            assert delivered == ['user-turn']
            assert store.get_restart_handoff(receipt.id).status == 'continuation_queued'
            runtime.resume_queue()
            await runtime._drain_task
        assert delivered == ['user-turn', receipt.continuation_prompt_id]
        assert runtime.session.control_mode == 'human'
    asyncio.run(scenario())


def test_owner_not_ready_handoff_retries_same_receipt_with_bounded_backoff(tmp_path):
    async def scenario():
        from datetime import UTC, datetime, timedelta
        from pa.acp.mcp_config import OwnerChannelError
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        manager.get = MagicMock(return_value=None)
        manager.recover_session = AsyncMock(side_effect=OwnerChannelError(
            'api_not_ready', 'unix', 'Wait for startup'))
        await manager._resume_restart_handoffs()
        failed = store.get_restart_handoff(receipt.id)
        assert failed.status == 'failed' and failed.failure_stage == 'resuming'
        assert store.get_session(runtime.session_id).recovery_json['attempts'] == 1
        await manager._resume_restart_handoffs()
        assert manager.recover_session.await_count == 1
        session = store.get_session(runtime.session_id)
        session.recovery_json['next_retry_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
        store.save_session(session)
        manager.recover_session.side_effect = None
        manager.recover_session.return_value = runtime
        manager._startup_complete = True
        await manager._recover_unscheduled_restart_handoffs()
        await runtime._drain_task
        assert manager.recover_session.await_count == 2
        assert store.get_restart_handoff(receipt.id).status == 'continuation_delivered'
        runtime._run_prompt.assert_awaited_once()
        assert runtime._run_prompt.call_args.args[0].id == receipt.continuation_prompt_id
    asyncio.run(scenario())


def test_transient_handoff_recovery_exhausts_without_manual_rearm(tmp_path):
    async def scenario():
        from datetime import UTC, datetime, timedelta
        from pa.acp.mcp_config import OwnerChannelError
        from pa.instance.agent_session import _RECOVERY_MAX_ATTEMPTS
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        manager.get = MagicMock(return_value=None)
        manager.recover_session = AsyncMock(side_effect=OwnerChannelError('api_not_ready', 'unix', 'Wait'))
        for attempt in range(_RECOVERY_MAX_ATTEMPTS):
            await manager._resume_restart_handoffs()
            session = store.get_session(runtime.session_id)
            assert session.recovery_json['attempts'] == attempt + 1
            if not session.recovery_json['blocked']:
                session.recovery_json['next_retry_at'] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
                store.save_session(session)
        await manager._resume_restart_handoffs()
        assert manager.recover_session.await_count == _RECOVERY_MAX_ATTEMPTS
        assert store.get_session(runtime.session_id).recovery_json['exhausted']
        assert store.get_restart_handoff(receipt.id).status == 'failed'
    asyncio.run(scenario())


@pytest.mark.parametrize('reason', ['authentication_rejected', 'instance_mismatch', 'api_incompatible'])
def test_nontransient_owner_failure_does_not_retry_automatically(tmp_path, reason):
    async def scenario():
        from pa.acp.mcp_config import OwnerChannelError
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        manager.get = MagicMock(return_value=None)
        manager.recover_session = AsyncMock(side_effect=OwnerChannelError(reason, 'unix', 'Correct configuration'))
        await manager._resume_restart_handoffs()
        await manager._resume_restart_handoffs()
        manager.recover_session.assert_awaited_once()
        assert store.get_restart_handoff(receipt.id).status == 'failed'
    asyncio.run(scenario())


def test_concurrent_replay_coalesces_recovery_and_preserves_prompt_identity(tmp_path):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        manager.get = MagicMock(return_value=None)
        async def recover(*args, **kwargs):
            await asyncio.sleep(0)
            manager.get.return_value = runtime
            return runtime
        manager.recover_session = AsyncMock(side_effect=recover)
        await asyncio.gather(manager._resume_restart_handoffs(), manager._resume_restart_handoffs())
        await runtime._drain_task
        manager.recover_session.assert_awaited_once_with(runtime.session_id, _startup_recovery=True, _defer_drain=True)
        runtime._run_prompt.assert_awaited_once()
        assert store.get_restart_handoff(receipt.id).continuation_prompt_id == receipt.continuation_prompt_id
    asyncio.run(scenario())


def test_watchdog_during_dequeue_admission_gap_does_not_fail_receipt(tmp_path):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        entered, release = asyncio.Event(), asyncio.Event()
        async def admission(item):
            entered.set()
            await release.wait()
            _record_completed_turn(store, runtime, item)
        runtime._run_prompt.side_effect = admission
        await manager._resume_restart_handoffs()
        await entered.wait()
        assert runtime._in_flight is None and not runtime._queue
        await manager._resume_restart_handoffs()
        assert store.get_restart_handoff(receipt.id).status == 'continuation_queued'
        release.set()
        await runtime._drain_task
        assert store.get_restart_handoff(receipt.id).status == 'continuation_delivered'
        runtime._run_prompt.assert_awaited_once()
    asyncio.run(scenario())


def test_restart_continuation_does_not_reactivate_taken_over_automated_run(tmp_path):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        runtime.session.purpose = 'automated_run'
        store.save_session(runtime.session)
        manager.recover_session = AsyncMock()
        await manager._resume_restart_handoffs()
        manager.recover_session.assert_not_called()
        runtime._run_prompt.assert_not_called()
        assert store.get_restart_handoff(receipt.id).status == 'restarting'
    asyncio.run(scenario())


def test_admission_in_progress_keeps_same_receipt_retryable(tmp_path):
    async def scenario():
        from pa.instance.agent_session import SessionAdmissionInProgress
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        manager.get = MagicMock(return_value=None)
        manager.recover_session = AsyncMock(side_effect=SessionAdmissionInProgress('exact session admission owned'))
        await manager._resume_restart_handoffs()
        pending = store.get_restart_handoff(receipt.id)
        assert pending.status == 'resuming'
        assert pending.error is None
        assert pending.continuation_prompt_id == receipt.continuation_prompt_id
        manager.recover_session.side_effect = None
        manager.recover_session.return_value = runtime
        manager._startup_complete = True
        await manager._recover_unscheduled_restart_handoffs()
        await runtime._drain_task
        delivered = store.get_restart_handoff(receipt.id)
        assert delivered.id == receipt.id
        assert delivered.status == 'continuation_delivered'
        runtime._run_prompt.assert_awaited_once()
        assert runtime._run_prompt.call_args.args[0].id == receipt.continuation_prompt_id
    asyncio.run(scenario())


def test_delayed_receipt_read_keeps_heartbeat_and_snapshot_responsive(tmp_path):
    async def scenario():
        import threading
        import time
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        _advance_restart_fixture(store, receipt.id, status='continuation_queued')
        runtime.enqueue(receipt.continuation_prompt, prompt_id=receipt.continuation_prompt_id,
                        source='restart-handoff:' + receipt.id, _defer_drain=True)
        entered, release = threading.Event(), threading.Event()
        original = store.get_restart_handoff
        threads = []
        def delayed(handoff_id):
            threads.append(threading.get_ident())
            entered.set()
            assert release.wait(2), 'test release deadline exceeded'
            return original(handoff_id)
        timer = threading.Timer(1, release.set)
        timer.start()
        try:
            with patch.object(store, 'get_restart_handoff', side_effect=delayed):
                started = time.monotonic()
                runtime._start_drain()
                assert time.monotonic() - started < .2
                for _ in range(100):
                    if entered.is_set(): break
                    await asyncio.sleep(.005)
                assert entered.is_set() and not release.is_set()
                ticks = []
                async def heartbeat():
                    for _ in range(4):
                        await asyncio.sleep(.01)
                        ticks.append(time.monotonic())
                pulse = asyncio.create_task(heartbeat())
                started = time.monotonic()
                snapshot = runtime.snapshot(include_transcript=False)
                assert time.monotonic() - started < .2
                assert snapshot['queue'][0]['id'] == receipt.continuation_prompt_id
                await asyncio.wait_for(pulse, timeout=.2)
                assert len(ticks) == 4 and not release.is_set()
                assert all(t != threading.get_ident() for t in threads)
                runtime._run_prompt.assert_not_called()
                release.set()
                await runtime._drain_task
                runtime._run_prompt.assert_awaited_once()
        finally:
            release.set()
            timer.cancel()
    asyncio.run(scenario())


def test_cached_receipt_is_revalidated_at_execution_after_revocation(tmp_path):
    async def scenario():
        from pa.instance.agent_session import PromptAdmissionBlocked
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        _advance_restart_fixture(store, receipt.id, status='continuation_queued')
        item = runtime.enqueue(receipt.continuation_prompt,
                               prompt_id=receipt.continuation_prompt_id,
                               source='restart-handoff:' + receipt.id, _defer_drain=True)
        await runtime._refresh_restart_receipt(item)
        assert runtime._prompt_eligible(item)
        async def revoke(_runtime):
            _advance_restart_fixture(store, receipt.id, status='failed')
        manager.collaboration_service = SimpleNamespace(prepare_turn=revoke)
        with patch('pa.execution.selection_settings.apply_pending', AsyncMock()), patch(
            'pa.execution.selection_audit.begin_prompt', return_value=None
        ):
            with pytest.raises(PromptAdmissionBlocked, match='authorization changed'):
                await AgentSessionRuntime._run_prompt(runtime, item)
        assert not runtime._prompt_eligible(item)
        assert runtime._in_flight is None
        runtime.connection.prompt.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize('failure', [False, True])
def test_user_ahead_of_restart_receipt_runs_before_slow_or_failed_lookup(tmp_path, failure):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        _advance_restart_fixture(store, receipt.id, status='continuation_queued')
        runtime.enqueue(receipt.continuation_prompt, prompt_id=receipt.continuation_prompt_id,
                        source='restart-handoff:' + receipt.id, _defer_drain=True)
        runtime.enqueue('user first', source='ui', prompt_id='user-first',
                        _defer_drain=True)
        entered, release = asyncio.Event(), asyncio.Event()
        original = runtime._refresh_restart_receipt
        async def delayed(item):
            if item.id == receipt.continuation_prompt_id:
                entered.set()
                await release.wait()
                if failure:
                    raise RuntimeError('receipt store unavailable')
            await original(item)
        with patch.object(runtime, '_refresh_restart_receipt', side_effect=delayed):
            runtime._start_drain()
            try:
                await asyncio.wait_for(entered.wait(), 2)
                assert runtime._run_prompt.call_args_list[0].args[0].id == 'user-first'
            finally:
                release.set()
                await runtime._drain_task
            assert runtime._run_prompt.await_count == (1 if failure else 2)
    asyncio.run(scenario())


@pytest.mark.parametrize('change', ['pause', 'remove', 'scope', 'higher_priority'])
def test_restart_selection_rechecks_queue_after_receipt_read(tmp_path, change):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        _advance_restart_fixture(store, receipt.id, status='continuation_queued')
        item = runtime.enqueue(receipt.continuation_prompt, prompt_id=receipt.continuation_prompt_id,
                               source='restart-handoff:' + receipt.id, _defer_drain=True)
        entered, release = asyncio.Event(), asyncio.Event()
        original = runtime._refresh_restart_receipt
        async def delayed(candidate):
            entered.set()
            await release.wait()
            await original(candidate)
        with patch.object(runtime, '_refresh_restart_receipt', side_effect=delayed):
            runtime._start_drain()
            await asyncio.wait_for(entered.wait(), 2)
            if change == 'pause':
                runtime._queue_paused = True
            elif change == 'remove':
                runtime._queue.remove(item)
            elif change == 'scope':
                runtime.session.cwd = '/changed-scope'
            else:
                runtime.enqueue('new user turn', source='ui', prompt_id='new-user',
                                _defer_drain=True)
            release.set()
            await runtime._drain_task
            if change == 'higher_priority':
                assert [c.args[0].id for c in runtime._run_prompt.call_args_list] == [
                    'new-user', receipt.continuation_prompt_id]
            else:
                runtime._run_prompt.assert_not_called()
    asyncio.run(scenario())


def test_failed_restart_lookup_does_not_block_later_user_prompt(tmp_path):
    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        runtime.enqueue(receipt.continuation_prompt, prompt_id=receipt.continuation_prompt_id,
                        source='restart-handoff:' + receipt.id, _defer_drain=True)
        runtime.enqueue('user next', source='ui', prompt_id='user-next',
                        _defer_drain=True)
        runtime._queue.sort(key=lambda item: item.source == 'ui')
        original = runtime._refresh_restart_receipt
        async def failing(item):
            if item.id == receipt.continuation_prompt_id:
                raise RuntimeError('receipt store unavailable')
            await original(item)
        with patch.object(runtime, '_refresh_restart_receipt', side_effect=failing):
            runtime._start_drain()
            await runtime._drain_task
        runtime._run_prompt.assert_awaited_once()
        assert runtime._run_prompt.call_args.args[0].id == 'user-next'
        assert [item.id for item in runtime._queue] == [receipt.continuation_prompt_id]
    asyncio.run(scenario())


def test_snapshot_restore_initializes_selection_audit_without_fresh_admission(tmp_path):
    from pa.instance.quiesce import QuiesceSnapshot
    from pa.execution.selection_service import SelectionService

    async def scenario():
        (tmp_path / "data").mkdir()
        store, manager, runtime, receipt = _human_handoff(tmp_path / "data")
        manager.settings.workspace_root = tmp_path / "workspaces"
        manager = AgentSessionManager(manager.settings, store)
        runtime.manager = manager
        original_binding = {'version': 1, 'execution_card_id': None,
                            'execution_project_id': None, 'origin_instance_id': None}
        runtime.session.execution_binding = original_binding
        store.save_session(runtime.session)
        env = await manager._prepare_workspace(runtime.session, requested_cwd=None,
                                               provider_id='codex', fresh_admission=False)
        assert env['PA_EXECUTION_CONTEXT']
        binding = dict(runtime.session.execution_binding)
        runtime.agent_env.update(env)
        item = runtime.enqueue('accepted continuation', source='restart-handoff:old-receipt',
                               prompt_id='old-prompt', _defer_drain=True)
        snapshot = runtime.to_session_snapshot()
        cold = AgentSessionManager(manager.settings, store)
        assert not hasattr(cold, '_selection_service')
        async def provider_start(restored, **kwargs):
            # The provider can start draining accepted work here, before any
            # create_session call has initialized fresh-admission dependencies.
            assert isinstance(cold._selection_service, SelectionService)
            assert kwargs['resume_external_id'] == 'exact-provider-thread'
            queued, = kwargs['queued_prompts']
            assert (queued.id, queued.message) == (item.id, item.message)
            assert queued.agent_env == restored.agent_env
            assert queued.agent_env['PA_EXECUTION_CONTEXT']
        with patch.object(AgentSessionRuntime, 'start', provider_start):
            restored = await cold._resume_from_snapshot(snapshot, QuiesceSnapshot())
        assert restored.session.execution_binding == binding
        assert store.get_session(runtime.session_id).execution_binding == binding
        assert not any(k in binding for k in ('dispatch_id', 'realm_id', 'principal_id'))
    asyncio.run(scenario())


def test_enqueue_survives_failed_receipt_phase_write_without_inventing_absence(tmp_path):
    from pa.instance.restart_lifecycle import restart_observation_fields

    async def scenario():
        store, manager, runtime, receipt = _human_handoff(tmp_path)
        # Exercise the actual durable enqueue producer; only the later receipt
        # write loses its acknowledgement, before deferred drain can start.
        runtime._checkpoint_runtime = AgentSessionRuntime._checkpoint_runtime.__get__(runtime)
        original = store.update_restart_handoff

        def fail_queued_write(*args, **kwargs):
            if kwargs.get('status') == 'continuation_queued':
                raise OSError('receipt phase write unavailable after enqueue')
            return original(*args, **kwargs)

        with patch.object(store, 'update_restart_handoff', side_effect=fail_queued_write):
            await manager._resume_pending_restart_handoffs()
        failed = store.get_restart_handoff(receipt.id)
        assert failed.status == 'failed'
        assert failed.failure_stage == 'resuming'
        assert failed.error == 'receipt phase write unavailable after enqueue'
        assert runtime._drain_task is None
        assert [item.id for item in runtime._queue] == [receipt.continuation_prompt_id]
        persisted = store.get_session(runtime.session_id)
        for context in ({'runtime': runtime, 'session': persisted}, {'session': persisted}):
            observed = restart_observation_fields(failed, **context)
            assert observed['worker_state'] == 'queued'
            assert observed['phase'] == 'failed'
            assert observed['effect_state'] == 'unknown'
            assert observed['attempt'] == failed.attempts
        assert restart_observation_fields(failed)['worker_state'] == 'unconfirmed'
        item = runtime._queue.pop()
        runtime._draining_prompt = item
        assert restart_observation_fields(failed, runtime=runtime)['worker_state'] == 'active'
        runtime._in_flight = item
        runtime._draining_prompt = None
        assert restart_observation_fields(failed, runtime=runtime)['worker_state'] == 'active'
        runtime._in_flight = None
        assert restart_observation_fields(failed, runtime=runtime)['worker_state'] == 'unconfirmed'
        # The passive adapter never rewrites the failed attempt or its error.
        assert store.get_restart_handoff(receipt.id) == failed

    asyncio.run(scenario())


@pytest.mark.parametrize('read_stage', ['sqlite.restart_handoffs_watchdog', 'sqlite.restart_handoffs_pending', 'sqlite.restart_handoff_session', 'sqlite.restart_handoff_completion'])
@pytest.mark.parametrize('continuation', ['', 'Continue exact work'])
def test_coordinator_read_crossing_committed_quiesce_cannot_infer_restart(tmp_path, read_stage, continuation):
    async def scenario():
        store = CardProjection(tmp_path / 'pa.db')
        settings = Settings(data_dir=tmp_path, instance_id='owner')
        manager = AgentSessionManager(settings, store)
        session = store.save_session(AgentSession(id='s', agent_name='codex', status='active'))
        handoff = store.create_restart_handoff(RestartHandoff(session_id=session.id, idempotency_key='recovery',
            continuation_prompt=continuation, continuation_prompt_id='exact-prompt', instance_id='owner'))
        # A pending-list read can see the very restart its sweep scheduled. Later
        # reads may belong to an earlier restart while a new handoff quiesces us.
        trigger = handoff
        if read_stage not in {'sqlite.restart_handoffs_watchdog', 'sqlite.restart_handoffs_pending'}:
            _advance_restart_fixture(store, handoff.id, status='restarting')
            trigger_session = store.save_session(AgentSession(id='trigger-s', agent_name='codex', status='active'))
            trigger = store.create_restart_handoff(RestartHandoff(session_id=trigger_session.id, idempotency_key='new-restart',
                continuation_prompt='', continuation_prompt_id='new-prompt', instance_id='owner'))
        read_entered = asyncio.Event()
        release_read = asyncio.Event()
        host_entered = asyncio.Event()
        release_host = asyncio.Event()
        original = manager._offload
        held = False

        async def barrier(operation, func, *args, **kwargs):
            nonlocal held
            if operation == 'sqlite.restart_handoff_read':
                await read_entered.wait()
            if operation == read_stage and not held:
                held = True
                read_entered.set()
                await release_read.wait()
            if operation == 'service.restart_handoff':
                # The real handoff executor has committed quiescence and the
                # restarting phase, but has not issued the host restart yet.
                assert manager.quiescing and not manager._accepting
                assert store.get_restart_handoff(trigger.id).status == 'restarting'
                host_entered.set()
                await release_host.wait()
                return None  # Never call a host service in this test.
            return await original(operation, func, *args, **kwargs)

        manager._offload = barrier
        manager.recover_session = AsyncMock()
        if read_stage == 'sqlite.restart_handoffs_watchdog':
            manager._schedule_restart_handoff(trigger.id)
        sweep = asyncio.create_task(manager._recover_unscheduled_restart_handoffs())
        try:
            await asyncio.wait_for(host_entered.wait(), timeout=5)
            release_read.set()
            await asyncio.wait_for(sweep, timeout=5)
            current = store.get_restart_handoff(handoff.id)
            assert current.status == 'restarting'
            assert current.delivered_at is None
            assert current.continuation_prompt_id == 'exact-prompt'
            manager.recover_session.assert_not_called()
            assert manager._restart_handoff_tasks  # Host request still delayed.
        finally:
            release_read.set()
            release_host.set()
            await asyncio.gather(sweep, *list(manager._restart_handoff_tasks.values()))
    asyncio.run(scenario())


def test_quiesced_replay_preserves_authoritative_completed_turn(tmp_path):
    async def scenario():
        from pa.domain.models import TranscriptEvent
        store = CardProjection(tmp_path / 'pa.db')
        manager = AgentSessionManager(Settings(data_dir=tmp_path, instance_id='owner'), store)
        session = store.save_session(AgentSession(id='s', agent_name='codex', status='active'))
        handoff = store.create_restart_handoff(RestartHandoff(session_id=session.id, idempotency_key='completed',
            continuation_prompt='Continue', continuation_prompt_id='exact-prompt', instance_id='owner'))
        _advance_restart_fixture(store, handoff.id, status='restarting')
        store.append_transcript_events([TranscriptEvent(session_id=session.id, seq=1,
            event_type='turn_completed', payload={'queued_prompt_id': 'exact-prompt', 'stop_reason': 'end_turn'})])
        await manager.quiesce(reason='test-committed-quiescence')
        manager.recover_session = AsyncMock()
        await manager._recover_unscheduled_restart_handoffs()
        assert store.get_restart_handoff(handoff.id).status == 'continuation_delivered'
        manager.recover_session.assert_not_called()
    asyncio.run(scenario())


@pytest.mark.parametrize('continuation', ['', 'Continue exact work'])
def test_coordinator_queued_effect_is_fenced_by_distinct_committed_quiesce(tmp_path, continuation):
    from threading import Event

    async def scenario():
        store = CardProjection(tmp_path / 'pa.db')
        manager = AgentSessionManager(Settings(data_dir=tmp_path, instance_id='owner'), store)
        for session_id in ('old-session', 'trigger-session'):
            store.save_session(AgentSession(id=session_id, agent_name='codex', status='active'))
        old = store.create_restart_handoff(RestartHandoff(session_id='old-session',
            idempotency_key='old-restart', continuation_prompt=continuation,
            continuation_prompt_id='old-exact-prompt', instance_id='owner'))
        _advance_restart_fixture(store, old.id, status='restarting')
        before = store.get_restart_handoff(old.id)
        trigger = store.create_restart_handoff(RestartHandoff(session_id='trigger-session',
            idempotency_key='new-restart', continuation_prompt='',
            continuation_prompt_id='new-exact-prompt', instance_id='owner'))
        effect_entered = asyncio.Event()
        release_effect = Event()
        host_entered = asyncio.Event()
        release_host = asyncio.Event()
        loop = asyncio.get_running_loop()
        original = manager._offload
        operation_to_hold = ('sqlite.restart_handoff_resuming' if continuation
                             else 'sqlite.restart_handoff_no_continuation')

        async def barrier(operation, func, *args, **kwargs):
            if operation == 'sqlite.restart_handoff_read':
                await effect_entered.wait()
            if operation == operation_to_hold:
                # Run the barrier in the actual offloaded worker, after the
                # event-loop admission check and before the queued mutation.
                def queued_effect(*call_args, **call_kwargs):
                    loop.call_soon_threadsafe(effect_entered.set)
                    assert release_effect.wait(10)
                    return func(*call_args, **call_kwargs)
                return await original(operation, queued_effect, *args, **kwargs)
            if operation == 'service.restart_handoff':
                assert manager.quiescing and not manager._accepting
                assert store.get_restart_handoff(trigger.id).status == 'restarting'
                host_entered.set()
                await release_host.wait()
                return None  # The host restart has not been issued.
            return await original(operation, func, *args, **kwargs)

        manager._offload = barrier
        manager.recover_session = AsyncMock()
        sweep = asyncio.create_task(manager._recover_unscheduled_restart_handoffs())
        try:
            await asyncio.wait_for(host_entered.wait(), 5)
            release_effect.set()
            await asyncio.wait_for(sweep, 5)
            assert store.get_restart_handoff(old.id) == before
            manager.recover_session.assert_not_called()
            assert manager._restart_handoff_tasks
        finally:
            release_effect.set()
            release_host.set()
            await asyncio.gather(sweep, *list(manager._restart_handoff_tasks.values()))
    asyncio.run(scenario())


@pytest.mark.parametrize('cancel_sweep', [False, True])
@pytest.mark.parametrize('blocked_at', ['mutation_lock', 'sqlite_write'])
def test_blocked_restart_store_keeps_reads_and_quiesce_deadline_responsive(tmp_path, cancel_sweep, blocked_at):
    import threading
    import time
    from pa.instance.quiesce import load_quiesce_snapshot

    async def scenario():
        store = CardProjection(tmp_path / 'pa.db')
        manager = AgentSessionManager(Settings(data_dir=tmp_path, instance_id='owner'), store)
        store.save_session(AgentSession(id='s', agent_name='codex', status='active'))
        receipt = store.create_restart_handoff(RestartHandoff(session_id='s',
            idempotency_key='blocked-write', continuation_prompt='',
            continuation_prompt_id='exact-prompt', instance_id='owner'))
        _advance_restart_fixture(store, receipt.id, status='restarting')
        held, release, entered, finished = (threading.Event() for _ in range(4))
        def hold_store():
            if blocked_at == 'mutation_lock':
                with store._mutation_lock:
                    held.set()
                    assert release.wait(5)
            else:
                import sqlite3
                with sqlite3.connect(tmp_path / 'pa.db') as conn:
                    conn.execute('BEGIN IMMEDIATE')
                    held.set()
                    assert release.wait(5)
        holder = threading.Thread(target=hold_store)
        holder.start()
        assert await asyncio.to_thread(held.wait, 2)
        original = store.update_restart_handoff
        def blocked_update(*args, **kwargs):
            entered.set()
            try:
                return original(*args, **kwargs)  # Actual global mutation lock wait.
            finally:
                finished.set()
        sweep = None
        try:
            with patch.object(store, 'update_restart_handoff', side_effect=blocked_update):
                sweep = asyncio.create_task(manager._resume_restart_handoffs())
                assert await asyncio.to_thread(entered.wait, 2)
                if cancel_sweep:
                    sweep.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await sweep
                ticks = []
                async def heartbeat():
                    for _ in range(4):
                        assert manager.get('s') is None
                        ticks.append(time.monotonic())
                        await asyncio.sleep(.01)
                pulse = asyncio.create_task(heartbeat())
                started = time.monotonic()
                with pytest.raises(TimeoutError, match='restart recovery write is still outstanding'):
                    await manager.quiesce(reason='blocked-store', timeout=.1)
                assert time.monotonic() - started < 1
                await asyncio.wait_for(pulse, .5)
                assert len(ticks) == 4
                assert not finished.is_set()
                assert manager._accepting and not manager.quiescing
                assert load_quiesce_snapshot(tmp_path) is None
                release.set()
                assert await asyncio.to_thread(finished.wait, 2)
                if not cancel_sweep:
                    await sweep
                await asyncio.sleep(0)
                assert not manager.label_lock('restart-handoff-effect').locked()
                # The write remains owned until completion; failed quiescence
                # does not pretend it was cancelled or commit a restart snapshot.
                assert store.get_restart_handoff(receipt.id).status == 'restart_completed'
        finally:
            release.set()
            await asyncio.to_thread(holder.join, 2)
            if sweep is not None and not sweep.done():
                await sweep
    asyncio.run(scenario())


def test_public_restart_receipts_consume_shared_observation_without_losing_residual_work(tmp_path):
    from fastapi.testclient import TestClient
    from pa.core.kernel import Kernel
    from pa.core.operation_observation import OperationObservation
    from pa.domain.models import TranscriptEvent
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces',
                        agent_enabled=False, peers=[], sync_token='isolated-peer-token')
    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app) as client:
        settings.subscribed_realms.append("other")
        app.state.ctx.services["membership"].ensure_owner_membership("other", "local")
        store = app.state.ctx.store
        session = store.save_session(AgentSession(id='exact-session', agent_name='codex',
            realm_id='other', execution_binding={}))
        manager = AgentSessionManager(settings, store)
        app.state.ctx.services["instance_agent"] = manager
        client.get('/')
        headers = {'Authorization': 'Bearer isolated-peer-token', 'Idempotency-Key': 'exact-key',
                   'X-CSRF-Token': client.cookies.get('pa_csrf')}
        with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), \
             patch.object(manager, '_schedule_restart_handoff'):
            posted = client.post(f'/api/agent/sessions/{session.id}/restart-handoffs',
                json={'idempotency_key': 'exact-key', 'continuation_prompt': 'Continue exact work'}, headers=headers)
            assert posted.status_code == 200, posted.text
            data = posted.json()
            observed = OperationObservation.from_receipt(data['observation'])
            assert observed.identity.owner == 'restart'
            assert observed.identity.realm_id == 'other'
            assert observed.identity.idempotency_key == 'exact-key'
            assert observed.accepted and observed.committed and observed.projected is None
            assert observed.effect == 'not_started'
            def common_matches(observation):
                before = store.get_restart_handoff(data['id'])
                live = manager.get(session.id)
                queue_before = list(live._queue) if live is not None else None
                scheduled_before = manager._schedule_restart_handoff.call_count
                response = client.get('/api/operations/exact-key', params={
                    'realm': 'other', 'owner': 'restart', 'operation': 'agent_restart_handoff'}, headers=headers)
                assert response.status_code == 200, response.text
                common = response.json()
                for field in ('identity', 'status', 'effect', 'phase', 'phase_version',
                              'attempt', 'reason_code', 'next_action', 'worker_state', 'effect_state'):
                    assert common[field] == observation[field], field
                assert common['recovery_action'] == observation['next_action']
                assert common['result']['handoff_id'] == data['id']
                assert store.get_restart_handoff(data['id']) == before
                assert (list(live._queue) if live is not None else None) == queue_before
                assert manager._schedule_restart_handoff.call_count == scheduled_before
                return common
            common_matches(data['observation'])
            receipt = store.get_restart_handoff(data['id'])
            failed = store.update_restart_handoff(receipt.id, status='failed', expected_status=receipt.status,
                expected_version=receipt.phase_version, owner_instance_id=settings.instance_id,
                failure_stage='resuming', reason_code='lost_ack', error='enqueue acknowledgement lost')
            runtime = SimpleNamespace(_queue=[SimpleNamespace(id=receipt.continuation_prompt_id)],
                _in_flight=None, _draining_prompt=None, _queue_paused=False)
            with patch.object(manager, 'get', return_value=runtime):
                def read():
                    response = client.get(f'/api/agent/sessions/{session.id}/restart-handoffs', headers=headers)
                    assert response.status_code == 200, response.text
                    observation = response.json()['handoffs'][0]['observation']
                    common_matches(observation)
                    return observation
                queued = read()
                assert queued['worker_state'] == 'queued' and queued['effect'] == 'unknown'
                assert queued['phase_version'] == failed.phase_version and queued['domain_stage'] == 'failed'
                runtime._in_flight = runtime._queue.pop()
                assert read()['worker_state'] == 'active'
                runtime._queue_paused = True
                assert read()['next_action'] == 'resume_by_operator'
                runtime._queue_paused = False
                session.execution_binding = {'workspace': 'different'}
                store.save_session(session)
                assert read()['reason_code'] == 'execution_binding_mismatch'
                assert store.get_restart_handoff(receipt.id) == failed
            assert read()['worker_state'] == 'unconfirmed'
            store.append_transcript_events([TranscriptEvent(session_id=session.id, seq=1, event_type='turn_completed',
                payload={'queued_prompt_id': receipt.continuation_prompt_id, 'stop_reason': 'end_turn'})])
            store.update_restart_handoff(receipt.id, status='continuation_delivered', expected_status=failed.status,
                expected_version=failed.phase_version, owner_instance_id=settings.instance_id)
            done = read()
            assert done['effect'] == 'complete' and done['worker_state'] == 'absent'
            assert done['continuation_prompt_id'] == receipt.continuation_prompt_id
            assert OperationObservation.from_receipt(done).as_outcome() == done
            assert any(item['error'] == 'enqueue acknowledgement lost' for item in store.get_restart_handoff(receipt.id).transition_history)
    reset_store()
    reset_instance_agent()
