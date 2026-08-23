from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager


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
    with pytest.raises(ValueError, match="different content"):
        asyncio.run(
            manager.request_restart_handoff(
                session_id="s", continuation_prompt="Different", idempotency_key="stable"
            )
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
