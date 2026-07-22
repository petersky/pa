from __future__ import annotations

import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from pa.repository.state import (
    GitInspector,
    GitInspectionError,
    RepositorySnapshot,
    RepositorySnapshotInput,
    RepositoryStateService,
)


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


def test_untracked_only_is_not_tracked_dirty(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    (repo / "new.txt").write_text("new\n")

    result = RepositoryStateService(tmp_path / "data", "macmini").refresh(repo)

    assert result.snapshot.dirty is False
    assert result.snapshot.untracked == 1


def test_fetch_head_stat_oserror_is_tolerated(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    (repo / ".git" / "FETCH_HEAD").write_text("")
    original_stat = Path.stat

    def bad_stat(self: Path, *args: object, **kwargs: object) -> object:
        if self.name == "FETCH_HEAD":
            raise OSError("permission denied")
        return original_stat(self, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", bad_stat)

    result = RepositoryStateService(tmp_path / "data", "macmini").refresh(repo)

    assert result.state == "fresh"
    assert result.snapshot.last_fetch_at is None


def test_redacts_passwords_from_remote_urls(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    git(repo, "remote", "add", "origin", "https://user:secret@example.test/repo.git")
    result = RepositoryStateService(tmp_path / "data", "macmini").refresh(repo)
    assert result.snapshot.remotes[0].fetch_url == "https://user@example.test/repo.git"


def test_missing_repo_is_persisted_as_error(tmp_path: Path) -> None:
    service = RepositoryStateService(tmp_path / "data", "macmini")
    missing = tmp_path / "missing"
    result = service.refresh(missing)
    assert result.state == "error"
    assert result.snapshot.inspection_error
    listed = service.list()
    assert len(listed) == 1
    assert listed[0].state == "error"
    assert listed[0].snapshot.repository_id == str(missing.resolve())


def test_error_snapshot_uses_git_common_dir_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    service = RepositoryStateService(tmp_path / "data", "macmini")

    def fail_inspect(self: GitInspector, path: Path, instance_id: str) -> RepositorySnapshot:
        raise GitInspectionError("broken")

    monkeypatch.setattr(GitInspector, "inspect", fail_inspect)

    result = service.refresh(repo)
    common_dir = str((repo / ".git").resolve())

    assert result.snapshot.repository_id == common_dir
    assert service.list()[0].snapshot.repository_id == common_dir


def test_successful_refresh_replaces_path_keyed_error(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    service = RepositoryStateService(tmp_path / "data", "macmini")
    assert service.refresh(repo).state == "error"
    make_repo(repo)

    successful = service.refresh(repo)

    listed = service.list()
    assert successful.state == "fresh"
    assert len(listed) == 1
    assert listed[0].state == "fresh"
    assert listed[0].snapshot.repository_id == successful.snapshot.repository_id


def test_failed_refresh_replaces_prior_path_observation_with_error(
    tmp_path: Path,
) -> None:
    repo = tmp_path / "repo"
    make_repo(repo)
    service = RepositoryStateService(tmp_path / "data", "macmini")
    successful = service.refresh(repo)

    with patch.object(
        service.inspector,
        "inspect",
        side_effect=GitInspectionError("inspection failed"),
    ):
        failed = service.refresh(repo)

    listed = service.list()
    assert len(listed) == 1
    assert listed[0].state == "error"
    assert listed[0].snapshot.repository_id == successful.snapshot.repository_id
    assert listed[0].snapshot.head == successful.snapshot.head
    assert failed.snapshot.observed_at > successful.snapshot.observed_at


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


def test_reconcile_marks_unhealthy_instance_unreachable(tmp_path: Path) -> None:
    service = RepositoryStateService(tmp_path, "local")
    incoming = RepositorySnapshot(
        repository_id="repo",
        path="/repo",
        instance_id="peer",
        observed_at=datetime.now(UTC),
    )
    result = service.reconcile([incoming], unreachable_instances={"peer"})
    assert result[0].state == "unreachable"


def test_reconcile_input_requires_observed_at() -> None:
    with pytest.raises(ValidationError):
        RepositorySnapshotInput.model_validate(
            {"repository_id": "repo", "path": "/repo", "instance_id": "peer"}
        )

    observed_at = datetime.now(UTC) - timedelta(hours=1)
    incoming = RepositorySnapshotInput.model_validate(
        {
            "repository_id": "repo",
            "path": "/repo",
            "instance_id": "peer",
            "observed_at": observed_at.isoformat(),
        }
    )
    assert incoming.observed_at == observed_at


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
    assert service.present(failed, unreachable=True).state == "unreachable"


def test_unreachable_repository_instances_excludes_local(tmp_path: Path) -> None:
    from unittest.mock import MagicMock

    from pa.config import Settings
    from pa.domain.models import FleetInstance
    from pa.fleet.registry import FleetRegistry
    from pa.modules.instance import _unreachable_repository_instances

    settings = Settings(data_dir=tmp_path, instance_id="local")
    fleet = FleetRegistry(tmp_path, settings.fleet_id)
    fleet.upsert_instance(
        FleetInstance(
            instance_id="local",
            name="local",
            url="http://local:8080",
            healthy=False,
        )
    )
    fleet.upsert_instance(
        FleetInstance(
            instance_id="peer",
            name="peer",
            url="http://peer:8080",
            healthy=False,
        )
    )
    ctx = MagicMock()
    ctx.settings = settings
    ctx.services = {"fleet_registry": fleet}

    assert _unreachable_repository_instances(ctx) == {"peer"}
