"""`pa sessions repair-recovery` re-evaluates stranded sessions, dry-run first."""
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from pa.cli.main import app
from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate, CardLane, Repository
from pa.domain.projection import CardProjection
from pa.instance.recovery_repair import repair_recovery
from pa.repository.workspace import LinkedRepository, WorkspaceManager

LEASE = (
    "Card already has a live worktree lease; resume its owner or explicitly "
    "allow concurrent dispatch"
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
def stranded(tmp_path, monkeypatch):
    monkeypatch.delenv("PA_WORKSPACE_ROOT", raising=False)
    settings = Settings(
        data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
        instance_id="instance-1",
    )
    settings.data_dir.mkdir()
    store = CardProjection(settings.data_dir / "pa.db")
    wm = WorkspaceManager(settings, store)
    repo = Repository(id="repo-1", url=str(make_remote(tmp_path)), name="PA")
    linked = LinkedRepository(repository=repo, branch="main")

    def card(title, lane=CardLane.INBOX):
        return store.create_card(CardCreate(title=title, lane=lane), via_log=False)

    def session(sid, status, card_id=None, **kw):
        return store.save_session(AgentSession(
            id=sid, agent_name="codex", status=status, purpose="automated_run",
            card_id=card_id, **kw,
        ))

    blocked = lambda code, error="", **kw: {  # noqa: E731
        "blocked": True, "attempts": 127, "code": code, "last_error": error, **kw}

    blocked_card, done_card, mis_card, held_card = (
        card("Blocked card"), card("Finished card", CardLane.DONE),
        card("Misclassified card"), card("Held card"),
    )
    # A dead owner holds the blocked card's lease with uncommitted work.
    dead = session("dead-owner", "recovery_blocked", blocked_card.id,
                   recovery_json=blocked("workspace_binding_mismatch", "base_sha differs"))
    lease = wm.provision_repository(
        linked, project_id="project-1", card_id=blocked_card.id, session_id=dead.id)
    (Path(lease.worktree_path) / "README.md").write_text("half-done work\n")
    # A session stranded behind that lease, retried 127 times.
    session("behind-lease", "recovery_blocked", blocked_card.id,
            recovery_json=blocked("acp_internal_error", LEASE))
    # Zombies: one on a finished card, one unlinked and old, and a chat.
    session("zombie-done", "quiesced", done_card.id)
    old = session("zombie-old", "disconnected")
    old.updated_at = datetime.now(UTC) - timedelta(days=30)
    store.save_session(old)
    session("fresh-unlinked", "disconnected")
    store.save_session(AgentSession(id="chat", agent_name="codex", purpose="chat",
                                    status="recovery_blocked",
                                    recovery_json=blocked("auth_missing")))
    # Block caused by the old auth substring match.
    session("misclassified", "recovery_blocked", mis_card.id,
            recovery_json=blocked("auth_missing", "workspace authority mismatch"))
    # Blocked behind an idle owner that is not reclaimable.
    session("idle-owner", "idle", held_card.id)
    wm.provision_repository(
        linked, project_id="project-1", card_id=held_card.id, session_id="idle-owner")
    session("behind-idle", "recovery_blocked", held_card.id,
            recovery_json=blocked("acp_internal_error", LEASE))
    return SimpleNamespace(settings=settings, store=store, wm=wm, lease=lease,
                           tree=Path(lease.worktree_path), linked=linked)


def by_session(report):
    result = {}
    for entry in report["actions"]:
        result.setdefault(entry["session_id"], []).append(entry)
    return result


def snapshot(lab):
    return (
        {s.id: (s.status, dict(s.recovery_json or {})) for s in lab.store.list_sessions()},
        [(l.id, l.state, l.fencing_token) for l in lab.wm.list()],
    )


def test_dry_run_reports_everything_and_changes_nothing(stranded):
    before = snapshot(stranded)
    report = repair_recovery(stranded.settings, stranded.store, stranded.wm)
    assert report["applied"] is False
    actions = by_session(report)
    assert [a["action"] for a in actions["zombie-done"]] == ["close"]
    assert [a["action"] for a in actions["zombie-old"]] == ["close"]
    assert "fresh-unlinked" not in actions and "chat" not in actions
    assert [a["action"] for a in actions["dead-owner"]] == ["reclaim_lease", "needs_operator"]
    assert actions["behind-lease"][0]["action"] == "rearm"
    assert actions["behind-lease"][0]["code"] == "workspace_lease_conflict"
    assert actions["misclassified"][0]["action"] == "rearm"
    held = actions["behind-idle"][0]
    assert held["action"] == "needs_operator" and held["held_by"] == ["idle-owner"]
    assert not any(a.get("applied") for a in report["actions"])
    assert snapshot(stranded) == before
    assert (stranded.tree / "README.md").read_text() == "half-done work\n"


def test_apply_reclaims_closes_and_rearms_through_audited_paths(stranded):
    report = repair_recovery(stranded.settings, stranded.store, stranded.wm, apply=True)
    store = stranded.store
    assert store.get_session("zombie-done").status == "closed"
    assert store.get_session("zombie-old").status == "closed"
    assert store.get_session("fresh-unlinked").status == "disconnected"
    audit = [e.payload for e in store.list_transcript_events("zombie-done")
             if e.event_type == "session_closed"]
    assert audit == [{"reason": "repair_card_done", "prior_status": "quiesced"}]
    # The dead owner's work is saved before its lease is released.
    reclaim = next(a for a in report["actions"] if a["action"] == "reclaim_lease")
    assert reclaim["applied"] and reclaim["rescue_ref"].startswith("refs/pa/rescue/dead-owner/")
    assert git(stranded.tree, "show", f"{reclaim['rescue_ref']}:README.md") == "half-done work"
    assert (stranded.tree / "README.md").read_text() == "half-done work\n"
    assert stranded.wm.get("repo-1", "dead-owner").state == "failed"
    # Sessions whose block is resolvable are re-armed for the next recovery pass.
    for sid in ("behind-lease", "misclassified"):
        rearmed = store.get_session(sid)
        assert (rearmed.status, rearmed.recovery_json) == ("recoverable_interrupted", {}), sid
    # The ones that need a person stay blocked, with their attempts untouched.
    assert store.get_session("dead-owner").status == "recovery_blocked"
    assert store.get_session("behind-idle").recovery_json["attempts"] == 127
    assert store.get_session("chat").status == "recovery_blocked"
    # Applying twice finds nothing left to reclaim, close or re-arm.
    again = repair_recovery(stranded.settings, stranded.store, stranded.wm, apply=True)
    assert {a["action"] for a in again["actions"]} <= {"needs_operator"}


def test_lease_waits_for_a_second_pass_only_when_a_live_owner_holds_it(stranded):
    report = repair_recovery(stranded.settings, stranded.store, stranded.wm, apply=True)
    assert not any(
        a["session_id"] == "behind-idle" and a["action"] == "rearm"
        for a in report["actions"]
    )


def test_cli_is_dry_run_unless_apply_is_given(stranded, monkeypatch):
    monkeypatch.setattr("pa.cli.sessions.get_settings", lambda: stranded.settings)
    monkeypatch.setattr("pa.domain.store.get_store", lambda settings=None: stranded.store)
    before = snapshot(stranded)
    result = CliRunner().invoke(app, ["sessions", "repair-recovery"])
    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output and "--apply" in result.output
    assert "close" in result.output and "reclaim_lease" in result.output
    assert "rearm" in result.output and "session=zombie-done" in result.output
    assert snapshot(stranded) == before
    applied = CliRunner().invoke(app, ["sessions", "repair-recovery", "--apply"])
    assert applied.exit_code == 0, applied.output
    assert "Applied" in applied.output
    assert stranded.store.get_session("zombie-done").status == "closed"


def test_apply_refuses_while_the_server_owns_the_data_dir(stranded, monkeypatch):
    from pa.core.writer_lock import DataDirWriterLock

    monkeypatch.setattr("pa.cli.sessions.get_settings", lambda: stranded.settings)
    monkeypatch.setattr("pa.domain.store.get_store", lambda settings=None: stranded.store)
    server = DataDirWriterLock(stranded.settings.data_dir)
    server.acquire()
    try:
        refused = CliRunner().invoke(app, ["sessions", "repair-recovery", "--apply"])
        assert refused.exit_code == 1
        assert "Stop the PA server" in refused.output
        assert stranded.store.get_session("zombie-done").status == "quiesced"
        dry = CliRunner().invoke(app, ["sessions", "repair-recovery"])
        assert dry.exit_code == 0 and "Dry run" in dry.output
    finally:
        server.release()
