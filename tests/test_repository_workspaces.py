from __future__ import annotations

import asyncio
import json
import subprocess
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.acp.configuration import SessionConfigurationRequest
from pa.acp.providers.base import AgentProviderSpec
from pa.config import Settings
from pa.domain.models import AgentSession, ProjectRepository, Repository
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.instance.quiesce import QuiesceSnapshot, SessionSnapshot
from pa.repository.workspace import (
    LinkedRepository,
    RepositoryPolicy,
    WorkspaceManager,
    WorkspaceProvisioningError,
    canonical_repository_identity,
)


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


def test_startup_failure_fences_lease_and_retry_preserves_worktree(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    first = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    evidence = Path(first.worktree_path) / "startup-audit.txt"
    evidence.write_text("preserve\n")

    assert (
        manager.fence_session(
            "session-1", stage="session_configuration", error="unsupported model"
        )
        == 1
    )
    fenced = manager.get(linked.repository.id, "session-1")
    assert fenced is not None
    assert fenced.state == "failed"
    assert fenced.stage == "session_configuration"
    assert fenced.fencing_token > first.fencing_token
    assert "unsupported model" in (fenced.error or "")

    retried = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    assert retried.state == "ready"
    assert retried.fencing_token == fenced.fencing_token
    assert evidence.read_text() == "preserve\n"


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
        setup_commands=[
            ["git", "config", "pa.setup-complete", "true"],
            [
                "sh",
                "-c",
                'printf "%s" "$PA_DEPENDENCY_CACHE" > dependency-cache.txt',
            ],
        ],
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
    expected_cache = manager.dependency_root / manager._entity_key("session-1")
    assert Path(lease.worktree_path, "dependency-cache.txt").read_text() == str(
        expected_cache
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


def test_reconcile_done_card_on_lease_owner_enables_safe_cleanup(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    manager.store.list_cards.return_value = [SimpleNamespace(id="card-1", lane="done")]
    manager.store.list_sessions.return_value = [
        AgentSession(id="session-1", agent_name="codex", status="closed")
    ]

    result = manager.reconcile_terminal_state()
    reconciled = manager.get(linked.repository.id, "session-1")

    assert result["cards_completed"] == 1
    assert result["closed_expired"] == 1
    assert reconciled is not None
    assert reconciled.completed is True
    assert reconciled.merged is True
    assert reconciled.stage == "reconciled_card_done"
    assert reconciled.expires_at <= datetime.now(UTC)
    assert manager.collect_garbage()["cleaned"] == 1
    assert not Path(lease.worktree_path).exists()


def test_reconcile_closed_standalone_session_preserves_unpushed_work(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id=None, session_id="session-1", card_id=None
    )
    worktree = Path(lease.worktree_path)
    git(worktree, "config", "user.email", "test@pa.invalid")
    git(worktree, "config", "user.name", "PA Test")
    (worktree / "README.md").write_text("unpushed\n")
    git(worktree, "add", "README.md")
    git(worktree, "commit", "-m", "preserve me")
    manager.store.list_cards.return_value = []
    manager.store.list_sessions.return_value = [
        AgentSession(id="session-1", agent_name="codex", status="closed")
    ]

    result = manager.reconcile_terminal_state()

    assert result["standalone_completed"] == 1
    assert result["closed_expired"] == 1
    assert manager.collect_garbage()["blocked"] == 1
    blocked = manager.get(linked.repository.id, "session-1")
    assert blocked is not None
    assert blocked.state == "cleanup_blocked"
    assert "unique commits require merged PR evidence" in blocked.error
    assert blocked.cleanup_evidence["unique_commits"]
    assert worktree.exists()


def test_reconcile_retains_nonterminal_card_workspace(tmp_path: Path) -> None:
    manager, _, linked = manager_for(tmp_path)
    manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    manager.store.list_cards.return_value = [
        SimpleNamespace(id="card-1", lane="active")
    ]
    manager.store.list_sessions.return_value = [
        AgentSession(id="session-1", agent_name="codex", status="closed")
    ]

    result = manager.reconcile_terminal_state()
    retained = manager.get(linked.repository.id, "session-1")

    assert result["cards_completed"] == 0
    assert result["nonterminal_cards"] == 1
    assert result["closed_expired"] == 1
    assert retained is not None
    assert retained.completed is False
    assert retained.merged is False
    assert manager.collect_garbage()["retained"] == 1


def test_cleanup_refreshes_remote_refs_before_classifying_commits(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    worktree = Path(lease.worktree_path)
    git(worktree, "config", "user.email", "test@pa.invalid")
    git(worktree, "config", "user.name", "PA Test")
    (worktree / "README.md").write_text("preserved remotely\n")
    git(worktree, "add", "README.md")
    git(worktree, "commit", "-m", "preserved remotely")
    git(worktree, "push", "origin", f"HEAD:refs/heads/archive/{lease.id}")
    git(
        Path(lease.cache_path),
        "update-ref",
        "-d",
        f"refs/remotes/origin/archive/{lease.id}",
    )
    manager.mark_card_completed("card-1", merged=True)
    manager.expire_session("session-1")

    assert manager.collect_garbage()["cleaned"] == 1
    assert manager.metrics()["cleanup_fetches"] == 1
    assert not worktree.exists()


def _terminal_watch(lease, head: str, merge_sha: str, *, status: str = "merged"):
    return SimpleNamespace(
        id="watch-1",
        card_id=lease.card_id,
        repository="example/pa",
        pr_number=42,
        originating_session_id=lease.session_id,
        authority_instance_id="authority-1",
        head_sha=head,
        status=status,
        state={
            "state": status,
            "head_sha": head,
            "confirmed_head_sha": head,
            "merge_commit_sha": merge_sha,
            "supervisor_state": "retired_after_merge",
        },
        updated_at=datetime.now(UTC),
    )


@pytest.mark.parametrize("merge_method", ["squash", "rebase"])
def test_deleted_pr_branch_is_cleaned_after_content_equivalence(
    tmp_path: Path, merge_method: str
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    worktree = Path(lease.worktree_path)
    git(worktree, "config", "user.email", "test@pa.invalid")
    git(worktree, "config", "user.name", "PA Test")
    (worktree / "README.md").write_text("first\n")
    git(worktree, "add", "README.md")
    git(worktree, "commit", "-m", "first")
    if merge_method == "rebase":
        (worktree / "feature.txt").write_text("second\n")
        git(worktree, "add", "feature.txt")
        git(worktree, "commit", "-m", "second")
    head = git(worktree, "rev-parse", "HEAD").stdout.strip()
    git(worktree, "push", "origin", f"HEAD:refs/heads/{lease.branch}")

    integrator = tmp_path / "integrator"
    subprocess.run(["git", "clone", str(linked.repository.url), str(integrator)], check=True)
    git(integrator, "config", "user.email", "test@pa.invalid")
    git(integrator, "config", "user.name", "PA Test")
    (integrator / "main-only.txt").write_text("concurrent\n")
    git(integrator, "add", "main-only.txt")
    git(integrator, "commit", "-m", "concurrent main change")
    if merge_method == "squash":
        git(integrator, "merge", "--squash", f"origin/{lease.branch}")
        git(integrator, "commit", "-m", "squash feature")
    else:
        commits = git(
            worktree, "rev-list", "--reverse", f"{lease.base_sha}..{head}"
        ).stdout.splitlines()
        git(integrator, "fetch", "origin", lease.branch)
        for commit in commits:
            git(integrator, "cherry-pick", commit)
    merge_sha = git(integrator, "rev-parse", "HEAD").stdout.strip()
    git(integrator, "push", "origin", "main")
    git(integrator, "push", "origin", "--delete", lease.branch)

    manager.set_pr_watch_provider(
        lambda **_: [_terminal_watch(lease, head, merge_sha)]
    )
    manager.mark_card_completed("card-1", merged=True)
    manager.expire_session("session-1")

    result = manager.collect_garbage()
    cleaned = manager.get(linked.repository.id, "session-1")
    assert result["cleaned"] == 1
    assert cleaned is not None
    assert cleaned.cleanup_decision == "safe_non_ancestor"
    assert cleaned.cleanup_evidence["merge_method"] == merge_method
    assert cleaned.cleanup_evidence["remote_branch_deleted"] is True
    assert not worktree.exists()
    assert not worktree.parent.exists()
    assert (
        git(
            Path(lease.cache_path),
            "show-ref",
            "--verify",
            f"refs/heads/{lease.branch}",
            check=False,
        ).returncode
        != 0
    )


def test_closed_unmerged_pr_does_not_authorize_unique_commit_cleanup(
    tmp_path: Path,
) -> None:
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(
        linked, project_id="project-1", session_id="session-1", card_id="card-1"
    )
    worktree = Path(lease.worktree_path)
    git(worktree, "config", "user.email", "test@pa.invalid")
    git(worktree, "config", "user.name", "PA Test")
    (worktree / "README.md").write_text("unique\n")
    git(worktree, "add", "README.md")
    git(worktree, "commit", "-m", "unique")
    head = git(worktree, "rev-parse", "HEAD").stdout.strip()
    manager.set_pr_watch_provider(
        lambda **_: [_terminal_watch(lease, head, head, status="closed")]
    )
    manager.mark_card_completed("card-1", merged=True)
    manager.expire_session("session-1")

    result = manager.collect_garbage()
    blocked = manager.get(linked.repository.id, "session-1")
    assert result["missing_evidence"] == 1
    assert blocked is not None
    assert blocked.state == "cleanup_blocked"
    assert blocked.cleanup_evidence["unique_commits"] == [head]
    assert worktree.exists()


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
    lease = manager.get_artifact_lease("session-2")
    assert lease is not None
    assert lease.path == workspace.cwd
    assert lease.state == "ready"
    assert (
        workspace.execution_context(manager.settings, "cursor")["artifact_workspace"][
            "id"
        ]
        == lease.id
    )


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


def test_agent_full_access_mode_is_applied_before_provider_and_mcp_startup(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="override")
    requested = SessionConfigurationRequest.from_values(mode_id="agent-full-access")

    async def run():
        with (
            patch(
                "pa.instance.agent_session.resolve_agent_provider",
                return_value=resolved,
            ),
            patch.object(AgentSessionRuntime, "start", new=AsyncMock()) as start,
        ):
            runtime = await manager.create_session(
                label="card:card-1:dispatch:dispatch-1",
                card_id="card-1",
                project_id="project-1",
                dispatch_id="dispatch-1",
                provider_override="codex",
                initial_configuration=requested,
            )
        return runtime, start

    runtime, start = asyncio.run(run())

    context = runtime.session.config_json["execution_context"]
    assert runtime.session.mode_id == "agent-full-access"
    assert spec.env["INITIAL_AGENT_MODE"] == "agent-full-access"
    assert context["provider_context"]["sandbox"] == "danger-full-access"
    assert context["provider_context"]["approval_policy"] == "never"
    assert context["approval_policy"] == "never"
    assert (
        start.await_args.kwargs["initial_configuration"].mode_id == "agent-full-access"
    )


def test_remote_provider_environment_keeps_full_ids_while_slugs_stay_short(
    tmp_path: Path,
) -> None:
    workspace_manager, repository, _ = manager_for(tmp_path)
    target_id = "2d22a9e1-a1a0-4900-8a8e-8284627aa6bf"
    authority_id = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
    dispatch_id = "33333333-3333-4333-8333-333333333333"
    session_id = "1cb4d40f-773d-4363-8fd5-312da92dee7c"
    colliding_session_id = "1cb4d40f-773d-4363-8fd5-ffffffffffff"
    card_id = "45cd58e9-1dd7-44b9-9e07-2ae58d12e685"
    colliding_card_id = "45cd58e9-1dd7-44b9-9e07-ffffffffffff"
    repository_id = "66666666-6666-4666-8666-666666666666"
    workspace_manager.settings.instance_id = target_id
    repository.id = repository_id
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="instance")

    async def run():
        with (
            patch(
                "pa.instance.agent_session.resolve_agent_provider",
                return_value=resolved,
            ),
            patch.object(AgentSessionRuntime, "start", new=AsyncMock()),
        ):
            return await manager.create_session(
                session_id=session_id,
                label=f"card:{card_id}:dispatch:{dispatch_id}",
                card_id=card_id,
                project_id="project-1",
                principal_id="user:operator",
                authority_instance_id=authority_id,
                dispatch_id=dispatch_id,
                realm_id="engineering",
                provider_override="codex",
            )

    runtime = asyncio.run(run())
    context = json.loads(spec.env["PA_EXECUTION_CONTEXT"])
    lease = context["repositories"][0]

    assert runtime.session.id == session_id
    assert runtime.session.card_id == card_id
    assert runtime.session.dispatch_id == dispatch_id
    assert runtime.session.authority_instance_id == authority_id
    assert context["session_id"] == session_id
    assert context["card_id"] == card_id
    assert context["instance"]["id"] == target_id
    assert context["authority_instance"]["id"] == authority_id
    assert context["provenance"] == {
        "version": 1,
        "realm_id": "engineering",
        "principal_id": "user:operator",
        "dispatch_id": dispatch_id,
    }
    assert lease["repository_id"] == repository_id
    assert session_id not in lease["worktree_path"]
    assert card_id not in lease["branch"]
    assert WorkspaceManager._entity_key(card_id) != WorkspaceManager._entity_key(
        colliding_card_id
    )
    assert len(WorkspaceManager._entity_key(session_id)) < len(session_id)
    assert WorkspaceManager._entity_key(session_id) != WorkspaceManager._entity_key(
        colliding_session_id
    )


def test_workspace_reprovision_preserves_remote_authority(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    authority = {"id": "authority-1", "name": "Authority One"}
    session = AgentSession(
        id="remote-session",
        agent_name="codex",
        card_id="card-1",
        project_id="project-1",
        config_json={"execution_context": {"authority_instance": authority}},
    )

    asyncio.run(
        manager._prepare_workspace(session, requested_cwd=None, provider_id="codex")
    )

    assert session.config_json["execution_context"]["authority_instance"] == authority
    assert session.config_json["execution_context"]["instance"]["id"] == "instance-1"


def test_workspace_recovery_rematerializes_cwd_from_data_dir(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    stale_cwd = workspace_manager.settings.data_dir / "agent-workspaces" / "stale"
    stale_cwd.mkdir(parents=True)
    session = AgentSession(
        id="recovery-session", agent_name="codex", cwd=str(stale_cwd)
    )

    asyncio.run(
        manager._prepare_workspace(
            session, requested_cwd=str(stale_cwd), provider_id="codex"
        )
    )

    assert session.cwd != str(stale_cwd)
    assert Path(session.cwd).is_dir()
    assert workspace_manager.settings.data_dir not in Path(session.cwd).parents


def test_missing_project_records_actionable_blocked_state(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    workspace_manager.store.get_project.return_value = None
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    session = AgentSession(
        id="recovery-session",
        agent_name="codex",
        project_id="missing-project",
    )

    with pytest.raises(WorkspaceProvisioningError, match="sync or link"):
        asyncio.run(
            manager._prepare_workspace(session, requested_cwd=None, provider_id="codex")
        )

    state = session.config_json["provisioning"]
    assert session.status == "recovery_blocked"
    assert state["state"] == "blocked"
    assert state["retryable"] is False
    assert state["manual_retry"] is True
    assert state["automatic_retry"] is False
    assert state["retry_on"] == "project_availability_change"
    assert state["error_code"] == "project_unavailable_on_instance"
    assert "Sync the project" in state["action"]
    assert "Close the session" in state["action"]


def test_unmaterialized_project_links_record_blocked_state(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    workspace_manager.store.get_project.return_value = SimpleNamespace(
        realm_id="default",
        repos=["repo-1"],
        tool_config={},
    )
    workspace_manager.store.list_project_repositories.return_value = []
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    session = AgentSession(
        id="recovery-session",
        agent_name="codex",
        project_id="project-1",
    )

    with pytest.raises(WorkspaceProvisioningError, match="links are not materialized"):
        asyncio.run(
            manager._prepare_workspace(session, requested_cwd=None, provider_id="codex")
        )

    state = session.config_json["provisioning"]
    assert session.status == "recovery_blocked"
    assert state["state"] == "blocked"
    assert state["retry_on"] == "project_availability_change"


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


def test_project_session_uses_project_realm_and_requires_repository(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    settings = workspace_manager.settings
    store = workspace_manager.store
    project = SimpleNamespace(realm_id="shared", tool_config={}, repos=[])
    store.get_project.return_value = project
    manager = AgentSessionManager(settings, store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="instance")

    async def run_success() -> None:
        with (
            patch(
                "pa.instance.agent_session.resolve_agent_provider",
                return_value=resolved,
            ),
            patch.object(AgentSessionRuntime, "start", new=AsyncMock()),
        ):
            await manager.create_session(
                project_id="project-1", provider_override="codex"
            )

    asyncio.run(run_success())
    assert any(
        call.kwargs.get("realm_id") == "shared"
        for call in store.list_project_repositories.call_args_list
    )

    store.list_project_repositories.return_value = []

    async def run_missing() -> None:
        with patch(
            "pa.instance.agent_session.resolve_agent_provider", return_value=resolved
        ):
            await manager.create_session(
                project_id="project-1", provider_override="codex"
            )

    with pytest.raises(WorkspaceProvisioningError, match="no linked repositories"):
        asyncio.run(run_missing())


def test_failed_reprovision_clears_stale_cwd_and_execution_fence(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
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
    existing = AgentSession(
        id="existing",
        agent_name="codex",
        status="disconnected",
        cwd="/stale/worktree",
        project_id="project-1",
        config_json={"execution_context": {"fencing_token": 41}},
    )
    manager = AgentSessionManager(workspace_manager.settings, store)
    spec = AgentProviderSpec(id="codex", display_name="Codex", command="codex-acp")
    resolved = SimpleNamespace(provider_id="codex", spec=spec, source="instance")

    async def run() -> None:
        with patch(
            "pa.instance.agent_session.resolve_agent_provider", return_value=resolved
        ):
            await manager.create_session(
                existing=existing,
                cwd="/caller/override",
                project_id="project-1",
                provider_override="codex",
            )

    with pytest.raises(WorkspaceProvisioningError, match="credentials"):
        asyncio.run(run())
    saved = store.save_session.call_args.args[0]
    assert saved.cwd is None
    assert "execution_context" not in saved.config_json
    assert saved.config_json["provisioning"]["state"] == "failed"


def test_startup_gc_protects_open_persisted_sessions(tmp_path: Path) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    store = workspace_manager.store
    store.list_sessions.return_value = [
        AgentSession(id="open", agent_name="codex", status="disconnected"),
        AgentSession(id="closed", agent_name="codex", status="closed"),
    ]
    manager = AgentSessionManager(workspace_manager.settings, store)
    manager.workspace_manager.collect_garbage = MagicMock(return_value={})
    manager.attach_default = AsyncMock()

    with patch("pa.instance.agent_session.load_quiesce_snapshot", return_value=None):
        asyncio.run(manager.start())

    manager.workspace_manager.collect_garbage.assert_called_once_with(
        active_session_ids={"open"}
    )


def test_stale_quiesce_snapshot_does_not_block_gc_when_agent_disabled(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    workspace_manager.settings.agent_enabled = False
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    manager.workspace_manager.collect_garbage = MagicMock(return_value={})
    snapshot = QuiesceSnapshot(sessions=[SessionSnapshot(session_id="stale-session")])

    with (
        patch(
            "pa.instance.agent_session.load_quiesce_snapshot",
            return_value=snapshot,
        ),
        patch("pa.instance.agent_session.clear_quiesce_snapshot") as clear,
    ):
        asyncio.run(manager.start())

    manager.workspace_manager.collect_garbage.assert_called_once_with(
        active_session_ids=set()
    )
    clear.assert_called_once_with(workspace_manager.settings.data_dir)


def test_execution_surface_reuses_live_and_persisted_card_session(
    tmp_path: Path,
) -> None:
    workspace_manager, _, _ = manager_for(tmp_path)
    manager = AgentSessionManager(workspace_manager.settings, workspace_manager.store)
    session = AgentSession(
        id="execution-session",
        agent_name="codex",
        label="execution",
        card_id="card-1",
        project_id="project-1",
        status="connected",
    )
    runtime = SimpleNamespace(
        session=session,
        connected=True,
        _closed=False,
        prompt=AsyncMock(return_value="ok"),
    )
    manager._runtimes[session.id] = runtime
    manager.create_session = AsyncMock()

    async def run_live() -> None:
        await manager.prompt(
            "first",
            item_id="card-1",
            project_id="project-1",
            surface="execution",
        )
        await manager.prompt(
            "second",
            item_id="card-1",
            surface="execution",
        )

    asyncio.run(run_live())
    assert runtime.prompt.await_count == 2
    manager.create_session.assert_not_awaited()

    manager._runtimes.clear()
    session.status = "disconnected"
    workspace_manager.store.list_sessions.return_value = [session]
    resumed_runtime = SimpleNamespace(prompt=AsyncMock(return_value="resumed"))
    manager.create_session = AsyncMock(return_value=resumed_runtime)

    asyncio.run(
        manager.prompt(
            "resume",
            item_id="card-1",
            project_id="project-1",
            surface="execution",
        )
    )
    manager.create_session.assert_awaited_once()
    kwargs = manager.create_session.await_args.kwargs
    assert kwargs["existing"] is session
    assert kwargs["card_id"] == "card-1"
    assert kwargs["project_id"] == "project-1"

    manager.create_session.reset_mock()
    with pytest.raises(RuntimeError, match="fenced to a different project"):
        asyncio.run(
            manager.prompt(
                "wrong project",
                item_id="card-1",
                project_id="project-2",
                surface="execution",
            )
        )
    manager.create_session.assert_not_awaited()
    assert session.project_id == "project-1"
