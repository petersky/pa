"""Safe, read-only Git inspection and per-instance observation storage."""

from __future__ import annotations

import json
import os
import subprocess
from typing import TYPE_CHECKING
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, Field

from pa.core.io import atomic_write_json
from pa.core.subprocesses import ProcessOutputLimitExceeded, run_process

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime


class GitRemote(BaseModel):
    name: str
    fetch_url: str | None = None
    push_url: str | None = None


class GitWorktree(BaseModel):
    path: str
    head: str | None = None
    branch: str | None = None
    detached: bool = False
    bare: bool = False
    locked: str | None = None
    prunable: str | None = None


class RepositorySnapshot(BaseModel):
    repository_id: str
    path: str
    instance_id: str
    observed_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    head: str | None = None
    branch: str | None = None
    upstream: str | None = None
    ahead: int | None = None
    behind: int | None = None
    dirty: bool = False
    untracked: int = 0
    remotes: list[GitRemote] = Field(default_factory=list)
    last_fetch_at: datetime | None = None
    worktrees: list[GitWorktree] = Field(default_factory=list)
    inspection_error: str | None = None


class RepositorySnapshotInput(RepositorySnapshot):
    """A reconciled observation must carry its source observation time."""

    observed_at: datetime


class RepositoryObservation(BaseModel):
    snapshot: RepositorySnapshot
    state: Literal["fresh", "stale", "unreachable", "error"] = "fresh"
    state_reason: str | None = None
    authoritative: bool = False
    source: Literal["observation"] = "observation"


class GitInspectionError(RuntimeError):
    pass


class GitInspector:
    """Run only fixed, non-mutating Git commands against a repository."""

    def __init__(self, *, timeout: float = 5.0) -> None:
        self.timeout = timeout

    def _run(self, path: Path, *args: str, check: bool = True) -> str:
        env = os.environ.copy()
        env.update(
            GIT_OPTIONAL_LOCKS="0",
            GIT_TERMINAL_PROMPT="0",
            GIT_CONFIG_NOSYSTEM="1",
        )
        try:
            result = subprocess.run(
                [
                    "git",
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "maintenance.auto=false",
                    "-C",
                    str(path),
                    *args,
                ],
                capture_output=True,
                text=True,
                env=env,
                timeout=self.timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise GitInspectionError(str(exc)) from exc
        if check and result.returncode:
            message = result.stderr.strip() or result.stdout.strip() or "Git failed"
            raise GitInspectionError(message[:500])
        return result.stdout

    async def _run_async(
        self, path: Path, *args: str, check: bool = True
    ) -> str:
        env = os.environ.copy()
        env.update(
            GIT_OPTIONAL_LOCKS="0",
            GIT_TERMINAL_PROMPT="0",
            GIT_CONFIG_NOSYSTEM="1",
        )
        try:
            result = await run_process(
                [
                    "git",
                    "-c",
                    "core.fsmonitor=false",
                    "-c",
                    "maintenance.auto=false",
                    "-C",
                    str(path),
                    *args,
                ],
                env=env,
                timeout=self.timeout,
                output_limit=4 * 1024 * 1024,
            )
        except (OSError, TimeoutError, ProcessOutputLimitExceeded) as exc:
            raise GitInspectionError(str(exc)) from exc
        if check and result.returncode:
            message = result.stderr.strip() or result.stdout.strip() or "Git failed"
            raise GitInspectionError(message[:500])
        return result.stdout

    async def inspect_async(
        self,
        path: Path,
        instance_id: str,
        runtime: AsyncRuntime,
    ) -> RepositorySnapshot:
        requested = await runtime.run_blocking(
            "repository.path_resolve", lambda: path.expanduser().resolve()
        )
        root = Path(
            (await self._run_async(requested, "rev-parse", "--show-toplevel")).strip()
        )
        root = await runtime.run_blocking("repository.path_resolve", root.resolve)
        head = (
            await self._run_async(
                root, "rev-parse", "--verify", "HEAD", check=False
            )
        ).strip() or None
        branch = (
            await self._run_async(
                root,
                "symbolic-ref",
                "--quiet",
                "--short",
                "HEAD",
                check=False,
            )
        ).strip() or None
        upstream = (
            await self._run_async(
                root,
                "rev-parse",
                "--abbrev-ref",
                "--symbolic-full-name",
                "@{upstream}",
                check=False,
            )
        ).strip() or None
        ahead = behind = None
        if upstream:
            counts = (
                await self._run_async(
                    root,
                    "rev-list",
                    "--left-right",
                    "--count",
                    f"HEAD...{upstream}",
                )
            ).split()
            if len(counts) == 2:
                ahead, behind = int(counts[0]), int(counts[1])
        status = await self._run_async(
            root, "status", "--porcelain=v1", "-z", "--untracked-files=normal"
        )
        entries = [entry for entry in status.split("\0") if entry]
        untracked = sum(entry.startswith("?? ") for entry in entries)

        remotes: dict[str, GitRemote] = {}
        for line in (await self._run_async(root, "remote", "-v")).splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            name, url, direction = parts[0], parts[1], parts[2]
            remote = remotes.setdefault(name, GitRemote(name=name))
            if direction == "(fetch)":
                remote.fetch_url = self._redact_url(url)
            elif direction == "(push)":
                remote.push_url = self._redact_url(url)

        common_dir_raw = (
            await self._run_async(root, "rev-parse", "--git-common-dir")
        ).strip()
        common_dir = Path(common_dir_raw)
        if not common_dir.is_absolute():
            common_dir = await runtime.run_blocking(
                "repository.path_resolve", lambda: (root / common_dir).resolve()
            )
        fetch_head = common_dir / "FETCH_HEAD"

        def fetched_at() -> datetime | None:
            if not fetch_head.exists():
                return None
            try:
                return datetime.fromtimestamp(fetch_head.stat().st_mtime, UTC)
            except OSError:
                return None

        fetched = await runtime.run_blocking("repository.fetch_stat", fetched_at)
        worktrees = self._parse_worktrees(
            await self._run_async(root, "worktree", "list", "--porcelain")
        )
        repository_id = str(
            await runtime.run_blocking(
                "repository.path_resolve", common_dir.resolve
            )
        )
        return RepositorySnapshot(
            repository_id=repository_id,
            path=str(root),
            instance_id=instance_id,
            head=head,
            branch=branch,
            upstream=upstream,
            ahead=ahead,
            behind=behind,
            dirty=any(not entry.startswith("?? ") for entry in entries),
            untracked=untracked,
            remotes=sorted(remotes.values(), key=lambda item: item.name),
            last_fetch_at=fetched,
            worktrees=worktrees,
        )

    async def resolve_repository_id_async(
        self, path: Path, runtime: AsyncRuntime
    ) -> str:
        requested = await runtime.run_blocking(
            "repository.path_resolve", lambda: path.expanduser().resolve()
        )
        try:
            common_dir_raw = (
                await self._run_async(
                    requested, "rev-parse", "--git-common-dir"
                )
            ).strip()
            common_dir = Path(common_dir_raw)
            if not common_dir.is_absolute():
                common_dir = requested / common_dir
            resolved = await runtime.run_blocking(
                "repository.path_resolve", common_dir.resolve
            )
            return str(resolved)
        except GitInspectionError:
            return str(requested)

    def inspect(self, path: Path, instance_id: str) -> RepositorySnapshot:
        requested = path.expanduser().resolve()
        root = Path(
            self._run(requested, "rev-parse", "--show-toplevel").strip()
        ).resolve()
        head = (
            self._run(root, "rev-parse", "--verify", "HEAD", check=False).strip()
            or None
        )
        branch = (
            self._run(
                root, "symbolic-ref", "--quiet", "--short", "HEAD", check=False
            ).strip()
            or None
        )
        upstream = (
            self._run(
                root,
                "rev-parse",
                "--abbrev-ref",
                "--symbolic-full-name",
                "@{upstream}",
                check=False,
            ).strip()
            or None
        )
        ahead = behind = None
        if upstream:
            counts = self._run(
                root, "rev-list", "--left-right", "--count", f"HEAD...{upstream}"
            ).split()
            if len(counts) == 2:
                ahead, behind = int(counts[0]), int(counts[1])

        status = self._run(
            root, "status", "--porcelain=v1", "-z", "--untracked-files=normal"
        )
        entries = [entry for entry in status.split("\0") if entry]
        untracked = sum(entry.startswith("?? ") for entry in entries)

        remotes: dict[str, GitRemote] = {}
        for line in self._run(root, "remote", "-v").splitlines():
            parts = line.split()
            if len(parts) < 3:
                continue
            name, url, direction = parts[0], parts[1], parts[2]
            remote = remotes.setdefault(name, GitRemote(name=name))
            if direction == "(fetch)":
                remote.fetch_url = self._redact_url(url)
            elif direction == "(push)":
                remote.push_url = self._redact_url(url)

        common_dir_raw = self._run(root, "rev-parse", "--git-common-dir").strip()
        common_dir = Path(common_dir_raw)
        if not common_dir.is_absolute():
            common_dir = (root / common_dir).resolve()
        fetch_head = common_dir / "FETCH_HEAD"
        fetched = None
        if fetch_head.exists():
            try:
                fetched = datetime.fromtimestamp(fetch_head.stat().st_mtime, UTC)
            except OSError:
                fetched = None

        worktrees = self._parse_worktrees(
            self._run(root, "worktree", "list", "--porcelain")
        )
        repository_id = str(common_dir.resolve())
        return RepositorySnapshot(
            repository_id=repository_id,
            path=str(root),
            instance_id=instance_id,
            head=head,
            branch=branch,
            upstream=upstream,
            ahead=ahead,
            behind=behind,
            dirty=any(not entry.startswith("?? ") for entry in entries),
            untracked=untracked,
            remotes=sorted(remotes.values(), key=lambda item: item.name),
            last_fetch_at=fetched,
            worktrees=worktrees,
        )

    @staticmethod
    def _redact_url(url: str) -> str:
        """Remove HTTP credentials before an observation is persisted or shared."""
        try:
            parsed = urlsplit(url)
            if parsed.password is None:
                return url
            hostname = parsed.hostname or ""
            if parsed.port:
                hostname = f"{hostname}:{parsed.port}"
            user = f"{parsed.username}@" if parsed.username else ""
            return urlunsplit(parsed._replace(netloc=f"{user}{hostname}"))
        except ValueError:
            return url

    @staticmethod
    def _parse_worktrees(output: str) -> list[GitWorktree]:
        parsed: list[GitWorktree] = []
        current: dict[str, object] = {}
        for line in [*output.splitlines(), ""]:
            if not line:
                if current.get("path"):
                    parsed.append(GitWorktree.model_validate(current))
                current = {}
                continue
            key, _, value = line.partition(" ")
            if key == "worktree":
                current["path"] = value
            elif key in {"HEAD", "branch", "locked", "prunable"}:
                target = "head" if key == "HEAD" else key
                current[target] = value or (
                    "true" if key in {"locked", "prunable"} else None
                )
            elif key in {"detached", "bare"}:
                current[key] = True
        return parsed

    def resolve_repository_id(self, path: Path) -> str:
        requested = path.expanduser().resolve()
        try:
            common_dir_raw = self._run(
                requested, "rev-parse", "--git-common-dir"
            ).strip()
            common_dir = Path(common_dir_raw)
            if not common_dir.is_absolute():
                common_dir = (requested / common_dir).resolve()
            return str(common_dir.resolve())
        except GitInspectionError:
            return str(requested)


class RepositoryStateStore:
    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "repository_snapshots.json"

    def load(self) -> dict[str, RepositorySnapshot]:
        try:
            payload = json.loads(self.path.read_text())
        except OSError, json.JSONDecodeError:
            return {}
        snapshots: dict[str, RepositorySnapshot] = {}
        for key, value in payload.get("snapshots", {}).items():
            try:
                snapshots[key] = RepositorySnapshot.model_validate(value)
            except TypeError, ValueError:
                continue
        return snapshots

    def save(self, snapshots: dict[str, RepositorySnapshot]) -> None:
        atomic_write_json(
            self.path,
            {
                "version": 1,
                "snapshots": {
                    k: v.model_dump(mode="json") for k, v in snapshots.items()
                },
            },
            mode=0o600,
        )


class RepositoryStateService:
    def __init__(
        self,
        data_dir: Path,
        instance_id: str,
        *,
        stale_after: timedelta = timedelta(minutes=15),
    ) -> None:
        self.instance_id = instance_id
        self.stale_after = stale_after
        self.inspector = GitInspector()
        self.store = RepositoryStateStore(data_dir)

    @staticmethod
    def _key(snapshot: RepositorySnapshot) -> str:
        return f"{snapshot.instance_id}:{snapshot.repository_id}"

    def refresh(self, path: Path) -> RepositoryObservation:
        requested = path.expanduser().resolve()
        try:
            snapshot = self.inspector.inspect(path, self.instance_id)
        except GitInspectionError as exc:
            snapshots = self.store.load()
            previous = next(
                (
                    item
                    for item in snapshots.values()
                    if item.instance_id == self.instance_id
                    and Path(item.path).expanduser().resolve() == requested
                ),
                None,
            )
            if previous:
                snapshot = previous.model_copy(
                    update={
                        "observed_at": datetime.now(UTC),
                        "inspection_error": str(exc),
                    }
                )
            else:
                snapshot = RepositorySnapshot(
                    repository_id=self.inspector.resolve_repository_id(requested),
                    path=str(requested),
                    instance_id=self.instance_id,
                    inspection_error=str(exc),
                )
            snapshots[self._key(snapshot)] = snapshot
            self.store.save(snapshots)
            return RepositoryObservation(
                snapshot=snapshot, state="error", state_reason=str(exc)
            )
        snapshots = self.store.load()
        for key, existing in list(snapshots.items()):
            if (
                key != self._key(snapshot)
                and existing.instance_id == self.instance_id
                and Path(existing.path).expanduser().resolve() == requested
            ):
                del snapshots[key]
        snapshots[self._key(snapshot)] = snapshot
        self.store.save(snapshots)
        return self.present(snapshot)

    async def refresh_async(
        self, path: Path, runtime: AsyncRuntime
    ) -> RepositoryObservation:
        """Inspect with cancellable Git processes and off-loop durable writes."""
        requested = await runtime.run_blocking(
            "repository.path_resolve", lambda: path.expanduser().resolve()
        )
        try:
            snapshot = await self.inspector.inspect_async(
                path, self.instance_id, runtime
            )
        except GitInspectionError as exc:
            repository_id = await self.inspector.resolve_repository_id_async(
                requested, runtime
            )
            def persist_error() -> RepositoryObservation:
                snapshots = self.store.load()
                previous = next(
                    (
                        item
                        for item in snapshots.values()
                        if item.instance_id == self.instance_id
                        and Path(item.path).expanduser().resolve() == requested
                    ),
                    None,
                )
                if previous:
                    failed = previous.model_copy(
                        update={
                            "observed_at": datetime.now(UTC),
                            "inspection_error": str(exc),
                        }
                    )
                else:
                    failed = RepositorySnapshot(
                        repository_id=repository_id,
                        path=str(requested),
                        instance_id=self.instance_id,
                        inspection_error=str(exc),
                    )
                snapshots[self._key(failed)] = failed
                self.store.save(snapshots)
                return RepositoryObservation(
                    snapshot=failed, state="error", state_reason=str(exc)
                )

            return await runtime.run_blocking(
                "repository.snapshot_write", persist_error
            )

        def persist_success() -> RepositoryObservation:
            snapshots = self.store.load()
            for key, existing in list(snapshots.items()):
                if (
                    key != self._key(snapshot)
                    and existing.instance_id == self.instance_id
                    and Path(existing.path).expanduser().resolve() == requested
                ):
                    del snapshots[key]
            snapshots[self._key(snapshot)] = snapshot
            self.store.save(snapshots)
            return self.present(snapshot)

        return await runtime.run_blocking(
            "repository.snapshot_write", persist_success
        )

    def reconcile(
        self,
        incoming: list[RepositorySnapshot],
        *,
        unreachable_instances: set[str] | None = None,
    ) -> list[RepositoryObservation]:
        """Merge newer observations; never apply snapshot data to a repository."""
        snapshots = self.store.load()
        for snapshot in incoming:
            key = self._key(snapshot)
            existing = snapshots.get(key)
            if existing is None or snapshot.observed_at > existing.observed_at:
                snapshots[key] = snapshot
        self.store.save(snapshots)
        unreachable_instances = unreachable_instances or set()
        return [
            self.present(item, unreachable=item.instance_id in unreachable_instances)
            for item in snapshots.values()
        ]

    def list(
        self, *, unreachable_instances: set[str] | None = None
    ) -> list[RepositoryObservation]:
        unreachable_instances = unreachable_instances or set()
        return [
            self.present(item, unreachable=item.instance_id in unreachable_instances)
            for item in self.store.load().values()
        ]

    def present(
        self, snapshot: RepositorySnapshot, *, unreachable: bool = False
    ) -> RepositoryObservation:
        if unreachable:
            return RepositoryObservation(
                snapshot=snapshot,
                state="unreachable",
                state_reason="instance unreachable",
            )
        if snapshot.inspection_error:
            return RepositoryObservation(
                snapshot=snapshot, state="error", state_reason=snapshot.inspection_error
            )
        age = datetime.now(UTC) - snapshot.observed_at
        if age > self.stale_after:
            return RepositoryObservation(
                snapshot=snapshot,
                state="stale",
                state_reason=f"observation is {int(age.total_seconds())}s old",
            )
        return RepositoryObservation(snapshot=snapshot)
