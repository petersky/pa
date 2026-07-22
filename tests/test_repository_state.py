from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.repository.state import RepositorySnapshot, RepositoryStateService


def git(path: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(path), *args], check=True, capture_output=True)


def make_repo(path: Path) -> None:
    path.mkdir()
    git(path, "init", "-b", "main")
    git(path, "config", "user.email", "test@example.com")
    git(path, "config", "user.name", "Test")
    (path / "tracked.txt").write_text("one\n")
    git(path, "add", "tracked.txt")
    git(path, "commit", "-m", "initial")


def test_inspects_head_dirty_untracked_remotes_fetch_and_worktrees(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    git(repo, "remote", "add", "origin", "https://example.test/repo.git")
    (repo / ".git" / "FETCH_HEAD").write_text("")
    (repo / "tracked.txt").write_text("two\n")
    (repo / "new.txt").write_text("new\n")

    service = RepositoryStateService(tmp_path / "data", "macmini")
    result = service.refresh(repo)

    assert result.state == "fresh"
    assert result.authoritative is False
    assert result.snapshot.branch == "main"
    assert result.snapshot.head
    assert result.snapshot.dirty is True
    assert result.snapshot.untracked == 1
    assert result.snapshot.remotes[0].fetch_url == "https://example.test/repo.git"
    assert result.snapshot.last_fetch_at is not None
    assert result.snapshot.worktrees[0].path == str(repo)
    assert service.list()[0].snapshot.instance_id == "macmini"


def test_redacts_passwords_from_remote_urls(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    git(repo, "remote", "add", "origin", "https://user:secret@example.test/repo.git")
    result = RepositoryStateService(tmp_path / "data", "macmini").refresh(repo)
    assert result.snapshot.remotes[0].fetch_url == "https://user@example.test/repo.git"


def test_missing_repo_is_error_and_is_not_persisted(tmp_path: Path) -> None:
    service = RepositoryStateService(tmp_path / "data", "macmini")
    result = service.refresh(tmp_path / "missing")
    assert result.state == "error"
    assert result.snapshot.inspection_error
    assert service.list() == []


def test_reconcile_keeps_newest_observation_per_instance_and_repo(
    tmp_path: Path,
) -> None:
    service = RepositoryStateService(tmp_path, "local")
    newer = RepositorySnapshot(
        repository_id="repo",
        path="/repo",
        instance_id="peer",
        head="new",
        observed_at=datetime.now(UTC),
    )
    older = newer.model_copy(
        update={"head": "old", "observed_at": newer.observed_at - timedelta(hours=1)}
    )
    service.reconcile([newer])
    service.reconcile([older])
    assert service.list()[0].snapshot.head == "new"


def test_presentation_surfaces_stale_unreachable_and_error(tmp_path: Path) -> None:
    service = RepositoryStateService(
        tmp_path, "local", stale_after=timedelta(seconds=1)
    )
    stale = RepositorySnapshot(
        repository_id="repo",
        path="/repo",
        instance_id="peer",
        observed_at=datetime.now(UTC) - timedelta(minutes=1),
    )
    assert service.present(stale).state == "stale"
    assert service.present(stale, unreachable=True).state == "unreachable"
    failed = stale.model_copy(update={"inspection_error": "not a repository"})
    assert service.present(failed).state == "error"
