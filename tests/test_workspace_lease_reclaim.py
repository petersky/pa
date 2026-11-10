"""A dead session's worktree lease is reclaimed, with its work saved first."""
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate, Repository
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager
from pa.repository.workspace import (
    LinkedRepository,
    WorkspaceLeaseConflict,
    WorkspaceProvisioningError,
)


def git(path, *args):
    return subprocess.run(
        ["git", "-C", str(path), *args], text=True, capture_output=True, check=True
    ).stdout.strip()


def make_remote(root: Path) -> Path:
    source = root / "source"
    source.mkdir()
    git(source, "init", "-b", "main")
    git(source, "config", "user.email", "t@pa.invalid")
    git(source, "config", "user.name", "T")
    (source / "README.md").write_text("base\n")
    git(source, "add", "README.md")
    git(source, "commit", "-m", "base")
    remote = root / "remote.git"
    subprocess.run(["git", "clone", "--bare", str(source), str(remote)], check=True)
    return remote


@pytest.fixture
def lab(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    remote = make_remote(tmp_path)
    settings = Settings(
        data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
        instance_id="instance-1",
    )
    settings.data_dir.mkdir()
    store = CardProjection(settings.data_dir / "pa.db")
    card = store.create_card(CardCreate(title="Blocked behind a dead session"), via_log=False)
    manager = AgentSessionManager(settings, store)
    repository = Repository(id="repo-1", url=str(remote), name="PA")
    linked = LinkedRepository(repository=repository, branch="main")
    owner = store.save_session(AgentSession(
        id="dead-owner", agent_name="codex", status="recovery_blocked",
        purpose="automated_run", card_id=card.id,
        recovery_json={"blocked": True, "attempts": 3, "code": "acp_internal_error"},
    ))
    lease = manager.workspace_manager.provision_repository(
        linked, project_id="project-1", card_id=card.id, session_id=owner.id,
    )
    return SimpleNamespace(
        settings=settings, store=store, card=card, manager=manager,
        wm=manager.workspace_manager, linked=linked, owner=owner, lease=lease,
        tree=Path(lease.worktree_path),
    )


def successor(lab):
    return lab.wm.provision_repository(
        lab.linked, project_id="project-1", card_id=lab.card.id,
        session_id="successor", allow_concurrent=False,
    )


def test_blocked_owner_lease_is_reclaimed_and_work_is_saved(lab):
    (lab.tree / "README.md").write_text("dirty tracked change\n")
    (lab.tree / "notes.txt").write_text("untracked work\n")
    index_before = git(lab.tree, "status", "--porcelain=v1")

    new = successor(lab)

    assert new.state == "ready" and new.session_id == "successor"
    old = lab.wm.get(lab.lease.repository_id, "dead-owner")
    assert (old.state, old.stage) == ("failed", "reclaimed_dead_owner")
    assert old.fencing_token > lab.lease.fencing_token
    assert old.completed is False and old.merged is False
    reclaim = old.cleanup_evidence["reclaim"]
    assert reclaim["reason"] == "recovery_blocked"
    assert reclaim["requested_by"] == "successor"
    ref = reclaim["rescue"]["ref"]
    assert ref.startswith("refs/pa/rescue/dead-owner/")
    # The rescue commit holds the tracked edit and the untracked file.
    assert git(lab.tree, "show", f"{ref}:README.md") == "dirty tracked change"
    assert git(lab.tree, "show", f"{ref}:notes.txt") == "untracked work"
    assert git(lab.tree, "rev-parse", f"{ref}^") == git(lab.tree, "rev-parse", "HEAD")
    # The dead owner's worktree is left exactly as it was.
    assert (lab.tree / "README.md").read_text() == "dirty tracked change\n"
    assert (lab.tree / "notes.txt").read_text() == "untracked work\n"
    assert git(lab.tree, "status", "--porcelain=v1") == index_before
    events = [e for e in lab.store.list_transcript_events("dead-owner")
              if e.event_type == "workspace_lease_reclaimed"]
    assert len(events) == 1
    assert events[0].payload["rescue_ref"] == ref
    assert events[0].payload["requested_by"] == "successor"


def test_clean_owner_still_gets_a_rescue_ref_at_head(lab):
    successor(lab)
    old = lab.wm.get(lab.lease.repository_id, "dead-owner")
    rescue = old.cleanup_evidence["reclaim"]["rescue"]
    assert rescue["preserved"] is True
    assert rescue["sha"] == git(lab.tree, "rev-parse", "HEAD")
    assert git(lab.tree, "rev-parse", rescue["ref"]) == rescue["sha"]


def test_exhausted_owner_is_reclaimed(lab):
    owner = lab.store.get_session("dead-owner")
    owner.recovery_json = {"blocked": True, "exhausted": True, "attempts": 8}
    lab.store.save_session(owner)
    successor(lab)
    old = lab.wm.get(lab.lease.repository_id, "dead-owner")
    assert old.cleanup_evidence["reclaim"]["reason"] == "recovery_exhausted"


def test_observer_is_told_about_the_reclaim(lab):
    seen = []
    lab.wm.reclaim_observer = seen.append
    successor(lab)
    assert len(seen) == 1
    assert seen[0]["session_id"] == "dead-owner"
    assert seen[0]["card_id"] == lab.card.id
    assert seen[0]["rescue"]["ref"].startswith("refs/pa/rescue/")


def test_rescue_failure_does_not_block_or_delete(lab, monkeypatch):
    monkeypatch.setattr(
        lab.wm, "_preserve_worktree",
        lambda lease: {"preserved": False, "ref": None, "error": "git exploded"},
    )
    successor(lab)
    old = lab.wm.get(lab.lease.repository_id, "dead-owner")
    assert old.cleanup_evidence["reclaim"]["rescue"]["error"] == "git exploded"
    assert lab.tree.is_dir()


@pytest.mark.parametrize(
    "case",
    ["live_runtime", "admitting", "idle_owner", "not_blocked", "restart_pending",
     "setting_off", "provisioning_owner_live"],
)
def test_live_or_undecided_owners_are_never_reclaimed(lab, case, monkeypatch):
    owner = lab.store.get_session("dead-owner")
    if case == "live_runtime":
        lab.manager._runtimes["dead-owner"] = SimpleNamespace(_closed=False)
    elif case == "admitting":
        lab.manager._admitting_sessions.add("dead-owner")
    elif case == "provisioning_owner_live":
        lab.manager._admitting_sessions.add("dead-owner")
        lab.wm._save(lab.lease.model_copy(update={"state": "provisioning"}))
    elif case == "idle_owner":
        owner.status = "idle"
        lab.store.save_session(owner)
    elif case == "not_blocked":
        owner.recovery_json = {"attempts": 1, "next_retry_at": datetime.now(UTC).isoformat()}
        lab.store.save_session(owner)
    elif case == "restart_pending":
        monkeypatch.setattr(lab.store, "pending_restart_continuation", lambda **kw: True)
    elif case == "setting_off":
        lab.settings.agent_reclaim_dead_session_leases = False
    with pytest.raises(WorkspaceLeaseConflict, match="live worktree lease"):
        successor(lab)
    kept = lab.wm.get(lab.lease.repository_id, "dead-owner")
    assert kept.state in {"ready", "provisioning"}
    assert "reclaim" not in kept.cleanup_evidence
    assert lab.wm.get(lab.lease.repository_id, "successor") is None


def test_lease_conflict_is_a_provisioning_error(lab):
    lab.manager._admitting_sessions.add("dead-owner")
    with pytest.raises(WorkspaceProvisioningError):
        successor(lab)


def test_admission_view_marks_only_dead_owners_reclaimable(lab):
    assert lab.wm.admission_workspaces()[0]["reclaimable"] == "recovery_blocked"
    lab.manager._admitting_sessions.add("dead-owner")
    assert lab.wm.admission_workspaces()[0]["reclaimable"] is None


def test_manager_announces_the_reclaim_with_the_rescue_ref(lab):
    created = []
    lab.manager.notification_service = SimpleNamespace(
        create=lambda data, *, principal_id, instance_id=None: created.append(data)
    )
    (lab.tree / "notes.txt").write_text("untracked work\n")
    successor(lab)
    ref = lab.wm.get(lab.lease.repository_id, "dead-owner").cleanup_evidence[
        "reclaim"]["rescue"]["ref"]
    assert len(created) == 1
    assert created[0].priority == "high"
    assert ref in created[0].body
    assert "dead-owner" in created[0].body and "Blocked behind a dead session" in created[0].body
