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
