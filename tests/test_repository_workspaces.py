from __future__ import annotations

import asyncio
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.config import Settings
from pa.acp.providers.base import AgentProviderSpec
from pa.domain.models import ProjectRepository, Repository
from pa.repository.workspace import (
    LinkedRepository,
    RepositoryPolicy,
    WorkspaceManager,
    WorkspaceProvisioningError,
    canonical_repository_identity,
)
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime


def git(path: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", "-C", str(path), *args],
        text=True,
        capture_output=True,
        check=check,
    )


def make_remote(root: Path) -> Path:
    source = root / "source"
    remote = root / "remote.git"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.email", "test@pa.invalid")
    git(source, "config", "user.name", "PA Test")
    (source / "README.md").write_text("base\n")
    git(source, "add", "README.md")
    git(source, "commit", "-m", "base")
    subprocess.run(["git", "clone", "--bare", str(source), str(remote)], check=True)
    return remote


def manager_for(
    tmp_path: Path,
) -> tuple[WorkspaceManager, Repository, LinkedRepository]:
    remote = make_remote(tmp_path)
    repository = Repository(id="repo-1", url=str(remote), name="PA")
    project = SimpleNamespace(tool_config={})
    store = MagicMock()
    store.get_project.return_value = project
    store.list_project_repositories.return_value = [
        (
            repository,
            ProjectRepository(
                project_id="project-1", repository_id=repository.id, branch="main"
            ),
        )
    ]
    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspace",
        instance_id="instance-1",
        instance_name="mini",
    )
    manager = WorkspaceManager(settings, store)
    linked = LinkedRepository(repository=repository, branch="main")
    return manager, repository, linked


def test_provisions_cached_fenced_worktree_and_provider_context(tmp_path: Path) -> None:
    manager, repository, _ = manager_for(tmp_path)

    workspace = manager.provision_project(
        project_id="project-1",
        session_id="session-1",
        card_id="card-1",
        realm_id="default",
        provider_id="codex",
    )

    assert workspace is not None
    lease = workspace.repositories[0]
    assert Path(workspace.cwd).is_dir()
    assert Path(lease.cache_path).is_dir()
    assert Path(workspace.cwd).is_relative_to(manager.root)
    assert not Path(workspace.cwd).is_relative_to(manager.settings.data_dir)
    assert (
        git(Path(workspace.cwd), "branch", "--show-current").stdout.strip()
        == lease.branch
    )
    assert (
        git(Path(workspace.cwd), "rev-parse", "HEAD").stdout.strip() == lease.base_sha
    )
    assert lease.repository_id == repository.id
    context = workspace.execution_context(manager.settings, "codex")
    assert context["provider_context"]["sandbox"] == "workspace-write"
    assert context["repositories"][0]["fencing_token"] == lease.fencing_token
    manager.store.set_repository_checkout.assert_called_once()
    assert manager.metrics()["provisioned_workspaces"] == 1
    assert manager.metrics()["cache_clones"] == 1


def test_retry_is_idempotent_and_preserves_dirty_resume(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    first = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    (Path(first.worktree_path) / "resume.txt").write_text("keep me\n")

    resumed = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )

    assert resumed.id == first.id
    assert resumed.fencing_token == first.fencing_token
    assert resumed.worktree_path == first.worktree_path
    assert resumed.untracked == 1
    assert (Path(resumed.worktree_path) / "resume.txt").read_text() == "keep me\n"


def test_concurrent_sessions_get_distinct_worktrees_and_fences(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)

    def provision(session_id: str):
        return manager.provision_repository(
            linked,
            project_id="project-1",
            session_id=session_id,
            card_id="card-1",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(provision, ["session-a", "session-b"])

    assert first.worktree_path != second.worktree_path
    assert first.branch != second.branch
    assert first.fencing_token != second.fencing_token
    assert Path(first.worktree_path).is_dir()
    assert Path(second.worktree_path).is_dir()


def test_duplicate_concurrent_dispatch_reuses_one_fence(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)

    def provision():
        return manager.provision_repository(
            linked,
            project_id="project-1",
            session_id="same-session",
            card_id="card-1",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        first, second = pool.map(lambda _: provision(), range(2))

    assert first.id == second.id
    assert first.fencing_token == second.fencing_token
    assert first.worktree_path == second.worktree_path


def test_truncated_identifiers_cannot_collide_and_expired_lease_refences(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    prefix = "session-with-a-very-long-shared-prefix-"
    first = manager.provision_repository(
        linked,
        project_id="project-1",
        session_id=prefix + "one",
        card_id="card-1",
    )
    second = manager.provision_repository(
        linked,
        project_id="project-1",
        session_id=prefix + "two",
        card_id="card-1",
    )
    assert first.worktree_path != second.worktree_path
    with manager._connect() as conn:
        conn.execute(
            "UPDATE workspace_leases SET expires_at=? WHERE id=?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), first.id),
        )

    renewed = manager.provision_repository(
        linked,
        project_id="project-1",
        session_id=prefix + "one",
        card_id="card-1",
    )
    assert renewed.fencing_token > first.fencing_token


def test_missing_worktree_is_recovered_but_detached_worktree_fails_closed(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    git(Path(lease.cache_path), "worktree", "remove", lease.worktree_path)

    recovered = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    assert recovered.fencing_token == lease.fencing_token
    git(Path(recovered.worktree_path), "checkout", "--detach")

    with pytest.raises(WorkspaceProvisioningError, match="detached|wrong branch"):
        manager.provision_repository(
            linked, project_id="project-1", session_id="session-1", card_id="card-1"
        )
    failed = manager.get(linked.repository.id, "session-1")
    assert failed is not None
    assert failed.state == "failed"
    assert Path(failed.worktree_path).is_dir()


def test_restart_finishes_worktree_left_in_provisioning_state(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    readme = Path(lease.worktree_path) / "README.md"
    readme.unlink()
    with manager._connect() as conn:
        conn.execute(
            "UPDATE workspace_leases SET state='provisioning',stage='worktree' WHERE id=?",
            (lease.id,),
        )

    recovered = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )

    assert recovered.state == "ready"
    assert readme.read_text() == "base\n"


def test_invalid_partial_cache_and_credential_url_are_retained_or_rejected(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    identity, _ = canonical_repository_identity(linked.repository.url)
    cache = manager.cache_root / manager._repo_key(linked.repository.id, identity)
    cache.mkdir(parents=True)
    (cache / "diagnostic.txt").write_text("partial clone artifact")

    with pytest.raises(WorkspaceProvisioningError, match="retained for diagnosis"):
        manager.provision_repository(
            linked, project_id="project-1", session_id="session-1", card_id="card-1"
        )
    assert (cache / "diagnostic.txt").exists()
    assert manager.get(linked.repository.id, "session-1").state == "failed"

    with pytest.raises(
        WorkspaceProvisioningError, match="must not contain credentials"
    ):
        canonical_repository_identity("https://user:secret@github.com/org/repo.git")


def test_cached_origin_mismatch_fails_closed(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    git(
        Path(lease.cache_path),
        "remote",
        "set-url",
        "origin",
        str(tmp_path / "other.git"),
    )

    with pytest.raises(WorkspaceProvisioningError, match="origin does not match"):
        manager.provision_repository(
            linked, project_id="project-1", session_id="session-1", card_id="card-1"
        )


def test_interrupted_clone_directory_is_removed_before_atomic_retry(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    identity, _ = canonical_repository_identity(linked.repository.url)
    cache = manager.cache_root / manager._repo_key(linked.repository.id, identity)
    partial = cache.parent / f".{cache.name}.clone-interrupted"
    partial.mkdir(parents=True)
    (partial / "partial.pack").write_text("incomplete")

    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )

    assert Path(lease.cache_path).is_dir()
    assert not partial.exists()


def test_declared_setup_and_submodule_defaults_are_explicit(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    linked.policy = RepositoryPolicy(
        partial_clone=False,
        submodules="none",
        lfs=False,
        setup_commands=[["git", "config", "pa.setup-complete", "true"]],
    )

    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )

    assert (
        git(
            Path(lease.worktree_path), "config", "--get", "pa.setup-complete"
        ).stdout.strip()
        == "true"
    )


def test_cleanup_requires_merge_expiry_clean_tree_and_pushed_commits(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    worktree = Path(lease.worktree_path)
    manager.mark_card_completed("card-1", merged=True)
    with manager._connect() as conn:
        conn.execute(
            "UPDATE workspace_leases SET expires_at=? WHERE id=?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), lease.id),
        )
    (worktree / "uncommitted.txt").write_text("retain\n")
    assert manager.collect_garbage(active_session_ids={"session-1"})["retained"] == 1
    assert manager.collect_garbage()["blocked"] == 1
    assert worktree.exists()

    (worktree / "uncommitted.txt").unlink()
    git(worktree, "config", "user.email", "test@pa.invalid")
    git(worktree, "config", "user.name", "PA Test")
    (worktree / "README.md").write_text("changed\n")
    git(worktree, "add", "README.md")
    git(worktree, "commit", "-m", "change")
    assert manager.collect_garbage()["blocked"] == 1
    assert worktree.exists()

    git(worktree, "push", "-u", "origin", lease.branch)
    assert manager.collect_garbage()["cleaned"] == 1
    assert not worktree.exists()
    assert manager.get(linked.repository.id, "session-1").state == "cleaned"


def test_active_session_renewal_extends_lease(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    with manager._connect() as conn:
        conn.execute(
            "UPDATE workspace_leases SET expires_at=? WHERE id=?",
            ((datetime.now(UTC) - timedelta(seconds=1)).isoformat(), lease.id),
        )

    assert manager.renew_session("session-1") == 1
    renewed = manager.get(linked.repository.id, "session-1")
    assert renewed is not None
    assert renewed.expires_at > datetime.now(UTC)


def test_scratch_workspace_rejects_pa_data_dir(tmp_path: Path) -> None:
    manager, _, _ = manager_for(tmp_path)

    with pytest.raises(WorkspaceProvisioningError, match="outside PA_DATA_DIR"):
        manager.scratch_workspace(
            session_id="session-1",
            card_id=None,
            project_id=None,
            requested_cwd=str(manager.settings.data_dir),
            provider_id="cursor",
        )
    external = tmp_path / "external"
    external.mkdir()
    with pytest.raises(WorkspaceProvisioningError, match="workspace_root"):
        manager.scratch_workspace(
            session_id="session-1",
            card_id=None,
            project_id=None,
            requested_cwd=str(external),
            provider_id="cursor",
        )

    workspace = manager.scratch_workspace(
        session_id="session-2",
        card_id=None,
        project_id=None,
        requested_cwd=None,
        provider_id="cursor",
    )
    assert Path(workspace.cwd).is_dir()
    assert Path(workspace.cwd).is_relative_to(manager.root)


def test_managed_directory_symlink_cannot_escape_workspace_root(
    tmp_path: Path,
) -> None:
    manager, _, _ = manager_for(tmp_path)
    external = tmp_path / "escaped"
    external.mkdir()
    manager.scratch_root.rmdir()
    manager.scratch_root.symlink_to(external, target_is_directory=True)

    with pytest.raises(WorkspaceProvisioningError, match="escapes workspace_root"):
        manager.scratch_workspace(
            session_id="session-1",
            card_id=None,
            project_id=None,
            requested_cwd=None,
            provider_id="codex",
        )


def test_agent_session_provisions_before_provider_start_and_persists_context(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    settings = workspace_manager.settings
    store = workspace_manager.store
    manager = AgentSessionManager(settings, store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="instance")

    async def run():
        with (
            patch(
                "pa.instance.agent_session.resolve_agent_provider",
                return_value=resolved,
            ),
            patch.object(AgentSessionRuntime, "start", new=AsyncMock()) as start,
        ):
            runtime = await manager.create_session(
                label="card:card-1",
                title="Provisioned",
                card_id="card-1",
                project_id="project-1",
                provider_override="codex",
            )
        return runtime, start

    runtime, start = asyncio.run(run())

    context = runtime.session.config_json["execution_context"]
    assert runtime.session.config_json["provisioning"]["state"] == "ready"
    assert context["provider_context"]["sandbox"] == "workspace-write"
    assert context["repositories"][0]["branch"].startswith("pa/card-1-")
    assert runtime.session.cwd == context["cwd"]
    assert Path(runtime.session.cwd).is_dir()
    assert spec.env["PA_EXECUTION_CONTEXT"]
    start.assert_awaited_once()


def test_agent_session_records_retryable_provisioning_failure(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    settings = workspace_manager.settings
    store = workspace_manager.store
    bad = Repository(
        id="repo-bad", url="https://user:secret@github.com/org/private.git"
    )
    store.list_project_repositories.return_value = [
        (
            bad,
            ProjectRepository(
                project_id="project-1", repository_id=bad.id, branch="main"
            ),
        )
    ]
    manager = AgentSessionManager(settings, store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="instance")

    async def run() -> None:
        with patch(
            "pa.instance.agent_session.resolve_agent_provider", return_value=resolved
        ):
            await manager.create_session(
                card_id="card-1",
                project_id="project-1",
                provider_override="codex",
            )

    with pytest.raises(
        WorkspaceProvisioningError, match="must not contain credentials"
    ):
        asyncio.run(run())
    session = store.save_session.call_args.args[0]
    assert session.status == "provisioning_failed"
    assert session.config_json["provisioning"]["retryable"] is True
    assert "credentials" in session.config_json["provisioning"]["error"]
