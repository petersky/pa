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
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
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
    request_assigned_restart_handoff,
)


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
    assert len(manager._restart_handoff_tasks) == 1
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

    store.update_restart_handoff(created[0].id, status="continuation_delivered")
    assert create("later").idempotency_key == "later"
    store.update_restart_handoff(
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
    manager = SimpleNamespace(store=store)
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
    manager = SimpleNamespace(store=store, request_restart_handoff=AsyncMock())
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
    store.update_restart_handoff(receipt.id, status="restarting")
    runtime = MagicMock(session=session)
    runtime.enqueue = MagicMock()
    manager.recover_session = AsyncMock(return_value=runtime)

    asyncio.run(manager._resume_restart_handoffs())
    asyncio.run(manager._resume_restart_handoffs())

    runtime.enqueue.assert_called_once_with(
        "Resume work",
        prompt_id=receipt.continuation_prompt_id,
        source=f"restart-handoff:{receipt.id}",
        card_id=None,
        project_id=None,
    )
    assert store.get_restart_handoff(receipt.id).status == "continuation_queued"


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
    store.update_restart_handoff(receipt.id, status="restarting")
    runtime = AgentSessionRuntime(manager, session)
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
        realm_id="default", provider_id="codex",
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
    store.update_restart_handoff(receipt.id, status="restarting")
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
    store.update_restart_handoff(receipt.id, status="restarting")
    manager.recover_session = AsyncMock(side_effect=RuntimeError("exact workspace blocker"))
    manager.create_session = AsyncMock()

    asyncio.run(manager._resume_restart_handoffs())
    failed = store.get_restart_handoff(receipt.id)
    assert failed.status == "failed"
    assert failed.error == "exact workspace blocker"
    manager.create_session.assert_not_called()

    runtime = MagicMock(session=session)
    runtime.enqueue = MagicMock()
    manager.recover_session = AsyncMock(return_value=runtime)
    first = asyncio.run(
        manager.retry_restart_handoff(session_id=session.id, handoff_id=receipt.id)
    )
    repeated = asyncio.run(
        manager.retry_restart_handoff(session_id=session.id, handoff_id=receipt.id)
    )

    assert first.status == "continuation_queued"
    assert repeated.status == "continuation_queued"
    manager.recover_session.assert_awaited_once_with(
        session.id, _startup_recovery=True
    )
    runtime.enqueue.assert_called_once_with(
        "deterministic continuation",
        prompt_id=receipt.continuation_prompt_id,
        source=f"restart-handoff:{receipt.id}",
        card_id=None,
        project_id=None,
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
    manager = SimpleNamespace(store=store, retry_restart_handoff=AsyncMock())
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
