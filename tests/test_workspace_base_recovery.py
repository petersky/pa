"""Unresolved provisioning is distinct from a changed immutable Git base."""
import asyncio
from pathlib import Path

import pytest

from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, WorkspaceBindingMismatch
from tests.test_repository_workspaces import manager_for, git


@pytest.mark.parametrize("base,native", [("pending", False), ("a" * 40, False), ("pending", True)])
def test_resolve_only_pending_base_on_exact_verified_lease(tmp_path, monkeypatch, base, native):
    workspace, repository, _ = manager_for(tmp_path)
    workspace.settings.data_dir.mkdir(exist_ok=True)
    store = CardProjection(workspace.settings.data_dir / "pa.db")
    monkeypatch.setattr(store, "get_project", workspace.store.get_project)
    monkeypatch.setattr(store, "list_project_repositories", workspace.store.list_project_repositories)
    manager = AgentSessionManager(workspace.settings, store)
    lease = manager.workspace_manager.provision_project(
        project_id="project-1", session_id="failed", card_id="card-1",
        realm_id="default", provider_id="codex",
    ).repositories[0]
    binding = dict(version=1, execution_card_id="card-1", execution_project_id="project-1",
                   repository_ids=[repository.id], lease_ids=[lease.id],
                   worktree_paths=[lease.worktree_path], branch=lease.branch, base_sha=base,
                   origin_instance_id=workspace.settings.instance_id)
    session = store.save_session(AgentSession(
        id="failed", agent_name="codex", card_id="card-1", project_id="project-1",
        execution_binding=binding,
        external_session_id="already-started" if native else None,
    ))
    if base != "pending" or native:
        with pytest.raises(WorkspaceBindingMismatch, match="base_sha"):
            asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
        assert store.get_session(session.id).execution_binding == binding
        return
    asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
    assert store.get_session(session.id).execution_binding["base_sha"] == lease.base_sha
    history = store.list_session_execution_binding_history(session.id)
    assert history[0]["reason"] == "workspace_pending_base_resolved"
    assert history[0]["prior_binding"] == binding
    for key, value in binding.items():
        if key != "base_sha":
            assert history[0]["binding"][key] == value
    # Repeated recovery does not resolve or rewrite it again.
    asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
    assert len(store.list_session_execution_binding_history(session.id)) == len(history)


def test_cleaned_lease_keeps_original_base_when_remote_advances(tmp_path):
    manager, repository, linked = manager_for(tmp_path)
    original = manager.provision_repository(linked, project_id="project-1", session_id="same", card_id="card")
    git(tmp_path / "source", "commit", "--allow-empty", "-m", "advance")
    git(tmp_path / "source", "push", str(tmp_path / "remote.git"), "main")
    git(Path(original.cache_path), "worktree", "remove", original.worktree_path)
    original.state = "cleaned"
    original.completed = original.merged = True
    manager._save(original)
    recovered = manager.provision_repository(linked, project_id="project-1", session_id="same", card_id="card")
    assert recovered.base_sha == original.base_sha
    assert recovered.branch == original.branch
    assert recovered.id == original.id
    assert recovered.fencing_token > original.fencing_token
    assert not recovered.completed and not recovered.merged
    assert git(Path(recovered.worktree_path), "rev-parse", "HEAD").stdout.strip() == original.base_sha


def test_failed_clone_retry_does_not_bind_pending_base(tmp_path, monkeypatch):
    workspace, _, _ = manager_for(tmp_path)
    workspace.settings.data_dir.mkdir(exist_ok=True)
    store = CardProjection(workspace.settings.data_dir / "pa.db")
    monkeypatch.setattr(store, "get_project", workspace.store.get_project)
    monkeypatch.setattr(store, "list_project_repositories", workspace.store.list_project_repositories)
    manager = AgentSessionManager(workspace.settings, store)
    session = store.save_session(AgentSession(
        id="clone-failed", agent_name="codex", card_id="card-1", project_id="project-1",
    ))
    original = manager.workspace_manager._ensure_cache
    def fail(*args):
        raise RuntimeError("clone authentication failed")
    monkeypatch.setattr(manager.workspace_manager, "_ensure_cache", fail)
    with pytest.raises(RuntimeError, match="clone authentication failed"):
        asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
    assert manager.workspace_manager.list()[0].base_sha == "pending"
    assert not store.get_session(session.id).execution_binding
    monkeypatch.setattr(manager.workspace_manager, "_ensure_cache", original)
    asyncio.run(manager._prepare_workspace(session, requested_cwd=None, provider_id="codex"))
    binding = store.get_session(session.id).execution_binding
    assert len(binding["base_sha"]) == 40
    history = store.list_session_execution_binding_history(session.id)
    assert all(h["binding"].get("base_sha") != "pending" for h in history)


@pytest.mark.parametrize("change", ["branch", "lease_ids", "base_sha"])
def test_pending_resolution_transition_rejects_retargeting(tmp_path, change):
    store = CardProjection(tmp_path / "pa.db")
    prior = dict(base_sha="pending" if change != "base_sha" else "a" * 40,
                 lease_ids=["lease"], repository_ids=["repo"], worktree_paths=["/original"], branch="original")
    store.save_session(AgentSession(id="source", agent_name="codex", execution_binding=prior))
    proposed = {**prior, "base_sha": "b" * 40}
    if change != "base_sha":
        proposed[change] = ["other"] if change == "lease_ids" else "other"
    with pytest.raises(ValueError, match="immutable provenance"):
        store.set_session_execution_binding("source", proposed,
            reason="workspace_pending_base_resolved", expected_binding=prior)
    assert store.get_session("source").execution_binding == prior
    assert not store.list_session_execution_binding_history("source")


