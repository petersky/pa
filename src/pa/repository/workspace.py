"""Durable repository caches and fenced card/session worktrees.

All mutable source checkouts managed here live outside ``PA_DATA_DIR``.  The
SQLite ledger is intentionally colocated with those checkouts so moving or
isolating a PA data directory cannot accidentally turn it into an agent cwd.
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import tempfile
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Iterator, Literal
from urllib.parse import urlsplit, urlunsplit

from pydantic import BaseModel, Field, field_validator

from pa.config import Settings
from pa.domain.models import Repository, RepositoryCheckout


class WorkspaceProvisioningError(RuntimeError):
    """A visible, retryable failure while preparing an execution workspace."""


class RepositoryPolicy(BaseModel):
    partial_clone: bool = True
    submodules: Literal["none", "checkout"] = "none"
    lfs: bool = False
    setup_commands: list[list[str]] = Field(default_factory=list)
    command_timeout_seconds: int = Field(default=600, ge=1, le=3600)

    @field_validator("setup_commands", mode="before")
    @classmethod
    def _normalize_commands(cls, value: object) -> object:
        if value is None:
            return []
        if not isinstance(value, list):
            raise ValueError("setup_commands must be a list")
        commands: list[list[str]] = []
        for command in value:
            if isinstance(command, str):
                parsed = shlex.split(command)
            elif isinstance(command, list) and all(
                isinstance(part, str) for part in command
            ):
                parsed = list(command)
            else:
                raise ValueError("setup commands must be strings or argv lists")
            if parsed:
                commands.append(parsed)
        return commands


class LinkedRepository(BaseModel):
    repository: Repository
    branch: str | None = None
    policy: RepositoryPolicy = Field(default_factory=RepositoryPolicy)


class WorkspaceLease(BaseModel):
    id: str
    repository_id: str
    repository_url: str
    card_id: str | None = None
    session_id: str
    project_id: str | None = None
    cache_path: str
    worktree_path: str
    branch: str
    base_sha: str
    fencing_token: int
    state: Literal[
        "provisioning", "ready", "failed", "completed", "cleanup_blocked", "cleaned"
    ] = "provisioning"
    stage: str = "requested"
    error: str | None = None
    dirty: bool = False
    untracked: int = 0
    completed: bool = False
    merged: bool = False
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime


class ProvisionedWorkspace(BaseModel):
    session_id: str
    card_id: str | None = None
    project_id: str | None = None
    cwd: str
    writable_roots: list[str]
    repositories: list[WorkspaceLease] = Field(default_factory=list)
    dependency_cache: str
    provider_context: dict[str, Any] = Field(default_factory=dict)

    def execution_context(self, settings: Settings, provider_id: str) -> dict[str, Any]:
        repositories = [
            {
                "repository_id": lease.repository_id,
                "repository_url": lease.repository_url,
                "workspace": lease.worktree_path,
                "checkout_path": lease.cache_path,
                "worktree_path": lease.worktree_path,
                "branch": lease.branch,
                "base_sha": lease.base_sha,
                "lease_id": lease.id,
                "fencing_token": lease.fencing_token,
            }
            for lease in self.repositories
        ]
        return {
            "version": 1,
            "instance": {
                "id": settings.instance_id,
                "name": settings.instance_name,
            },
            "session_id": self.session_id,
            "card_id": self.card_id,
            "project_id": self.project_id,
            "cwd": self.cwd,
            "writable_roots": self.writable_roots,
            "dependency_cache": self.dependency_cache,
            "repositories": repositories,
            "network_policy": "provider-default",
            "approval_policy": "on-request",
            "provider": provider_id,
            "provider_context": self.provider_context,
        }


def canonical_repository_identity(url: str) -> tuple[str, str]:
    """Return a credential-free canonical identity and safe clone URL."""
    value = url.strip()
    if not value:
        raise WorkspaceProvisioningError("Repository URL is empty")
    # Normalize common SCP-style SSH URLs without changing their clone syntax.
    scp = re.fullmatch(r"(?P<user>[^@/:]+)@(?P<host>[^:/]+):(?P<path>.+)", value)
    if scp:
        host = scp.group("host").lower()
        path = scp.group("path").strip("/")
        canonical_path = path[:-4] if path.lower().endswith(".git") else path
        if host == "github.com":
            canonical_path = canonical_path.lower()
        return f"ssh://{host}/{canonical_path}", value
    parsed = urlsplit(value)
    if not parsed.scheme or not parsed.hostname:
        # Local paths are useful for deterministic/offline installations and tests.
        local = Path(value).expanduser().resolve()
        return f"file://{local}", str(local)
    if (
        parsed.password is not None
        or parsed.username
        and parsed.scheme in {"http", "https"}
    ):
        raise WorkspaceProvisioningError(
            "Repository URLs must not contain credentials; use an instance credential helper"
        )
    host = parsed.hostname.lower()
    if parsed.port:
        host = f"{host}:{parsed.port}"
    path = parsed.path.strip("/")
    canonical_path = path[:-4] if path.lower().endswith(".git") else path
    if parsed.hostname.lower() == "github.com":
        canonical_path = canonical_path.lower()
    canonical = urlunsplit((parsed.scheme.lower(), host, f"/{canonical_path}", "", ""))
    clone_url = urlunsplit((parsed.scheme, parsed.netloc, parsed.path, "", ""))
    return canonical, clone_url


class WorkspaceManager:
    LEASE_TTL = timedelta(hours=24)

    def __init__(self, settings: Settings, store: Any) -> None:
        if settings.workspace_root is None:
            raise ValueError("workspace_root is not configured")
        self.settings = settings
        self.store = store
        self.root = settings.workspace_root.expanduser().resolve()
        self.cache_root = self.root / "repositories"
        self.worktree_root = self.root / "worktrees"
        self.scratch_root = self.root / "sessions"
        self.dependency_root = self.root / "dependencies"
        self.lock_root = self.root / ".locks"
        self.db_path = self.root / "workspace_leases.db"
        for path in (
            self.cache_root,
            self.worktree_root,
            self.scratch_root,
            self.dependency_root,
            self.lock_root,
        ):
            path.mkdir(parents=True, exist_ok=True)
            self._assert_managed_path(path)
        self._init_db()

    def _assert_managed_path(self, path: Path) -> Path:
        resolved = path.expanduser().resolve()
        if resolved != self.root and self.root not in resolved.parents:
            raise WorkspaceProvisioningError(
                f"Managed workspace path escapes workspace_root: {resolved}"
            )
        return resolved

    def _connect(self) -> sqlite3.Connection:
        self._assert_managed_path(self.db_path)
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workspace_leases (
                    id TEXT PRIMARY KEY,
                    repository_id TEXT NOT NULL,
                    repository_url TEXT NOT NULL,
                    card_id TEXT,
                    session_id TEXT NOT NULL,
                    project_id TEXT,
                    cache_path TEXT NOT NULL,
                    worktree_path TEXT NOT NULL,
                    branch TEXT NOT NULL,
                    base_sha TEXT NOT NULL,
                    fencing_token INTEGER NOT NULL,
                    state TEXT NOT NULL,
                    stage TEXT NOT NULL,
                    error TEXT,
                    dirty INTEGER NOT NULL DEFAULT 0,
                    untracked INTEGER NOT NULL DEFAULT 0,
                    completed INTEGER NOT NULL DEFAULT 0,
                    merged INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    UNIQUE(repository_id, session_id)
                );
                CREATE TABLE IF NOT EXISTS workspace_fences (
                    repository_id TEXT PRIMARY KEY,
                    next_token INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workspace_metrics (
                    key TEXT PRIMARY KEY,
                    value INTEGER NOT NULL DEFAULT 0
                );
                CREATE INDEX IF NOT EXISTS idx_workspace_card
                    ON workspace_leases(card_id, state);
                CREATE INDEX IF NOT EXISTS idx_workspace_expiry
                    ON workspace_leases(expires_at, state);
                """
            )

    @staticmethod
    def _slug(value: str, *, limit: int = 32) -> str:
        slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", value).strip("-._").lower()
        return (slug or "workspace")[:limit]

    @staticmethod
    def _repo_key(repository_id: str, identity: str) -> str:
        digest = hashlib.sha256(identity.encode()).hexdigest()[:16]
        return f"{WorkspaceManager._slug(repository_id, limit=20)}-{digest}"

    @staticmethod
    def _entity_key(value: str) -> str:
        digest = hashlib.sha256(value.encode()).hexdigest()[:8]
        return f"{WorkspaceManager._slug(value, limit=16)}-{digest}"

    @contextmanager
    def _repository_lock(self, repo_key: str) -> Iterator[None]:
        lock_path = self.lock_root / f"{repo_key}.lock"
        self._assert_managed_path(lock_path)
        with lock_path.open("a+") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    def _git(
        self,
        *args: str,
        cwd: Path | None = None,
        timeout: int = 300,
        check: bool = True,
    ) -> subprocess.CompletedProcess[str]:
        env = os.environ.copy()
        env.update(
            GIT_TERMINAL_PROMPT="0",
            GIT_CONFIG_NOSYSTEM="1",
            GIT_LFS_SKIP_SMUDGE="1",
        )
        try:
            result = subprocess.run(
                ["git", "-c", "credential.interactive=never", *args],
                cwd=cwd,
                env=env,
                text=True,
                capture_output=True,
                timeout=timeout,
                check=False,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise WorkspaceProvisioningError(f"Git invocation failed: {exc}") from exc
        if check and result.returncode:
            detail = (result.stderr or result.stdout or "Git failed").strip()
            raise WorkspaceProvisioningError(self._redact_error(detail)[-1000:])
        return result

    @staticmethod
    def _redact_error(value: str) -> str:
        def replace(match: re.Match[str]) -> str:
            raw = match.group(0)
            suffix = ""
            while raw and raw[-1] in '.,;:)"]':
                suffix = raw[-1] + suffix
                raw = raw[:-1]
            try:
                parsed = urlsplit(raw)
                host = parsed.hostname or "redacted"
                if parsed.port:
                    host = f"{host}:{parsed.port}"
                safe = urlunsplit((parsed.scheme, host, parsed.path, "", ""))
                return safe + suffix
            except ValueError:
                return "[redacted-url]" + suffix

        return re.sub(r"https?://\S+", replace, value)

    def _next_fence(self, repository_id: str) -> int:
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT next_token FROM workspace_fences WHERE repository_id=?",
                (repository_id,),
            ).fetchone()
            token = int(row["next_token"]) if row else 1
            conn.execute(
                "INSERT OR REPLACE INTO workspace_fences(repository_id,next_token) VALUES(?,?)",
                (repository_id, token + 1),
            )
            return token

    def _increment_metric(self, key: str, amount: int = 1) -> None:
        with self._connect() as conn:
            conn.execute(
                """INSERT INTO workspace_metrics(key,value) VALUES(?,?)
                   ON CONFLICT(key) DO UPDATE SET value=value+excluded.value""",
                (key, amount),
            )

    def metrics(self) -> dict[str, int]:
        with self._connect() as conn:
            rows = conn.execute("SELECT key,value FROM workspace_metrics").fetchall()
        return {str(row["key"]): int(row["value"]) for row in rows}

    def _save(self, lease: WorkspaceLease) -> WorkspaceLease:
        data = lease.model_dump(mode="json")
        with self._connect() as conn:
            conn.execute(
                """INSERT OR REPLACE INTO workspace_leases
                (id, repository_id, repository_url, card_id, session_id, project_id,
                 cache_path, worktree_path, branch, base_sha, fencing_token, state,
                 stage, error, dirty, untracked, completed, merged, created_at,
                 updated_at, expires_at)
                VALUES (:id,:repository_id,:repository_url,:card_id,:session_id,:project_id,
                        :cache_path,:worktree_path,:branch,:base_sha,:fencing_token,:state,
                        :stage,:error,:dirty,:untracked,:completed,:merged,:created_at,
                        :updated_at,:expires_at)""",
                data,
            )
        return lease

    @staticmethod
    def _row(row: sqlite3.Row) -> WorkspaceLease:
        return WorkspaceLease.model_validate(dict(row))

    def get(self, repository_id: str, session_id: str) -> WorkspaceLease | None:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM workspace_leases WHERE repository_id=? AND session_id=?",
                (repository_id, session_id),
            ).fetchone()
        return self._row(row) if row else None

    def list(self, *, card_id: str | None = None) -> list[WorkspaceLease]:
        query = "SELECT * FROM workspace_leases"
        params: tuple[str, ...] = ()
        if card_id is not None:
            query += " WHERE card_id=?"
            params = (card_id,)
        query += " ORDER BY created_at"
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row(row) for row in rows]

    def linked_repositories(
        self, project_id: str, *, realm_id: str
    ) -> list[LinkedRepository]:
        getter = getattr(self.store, "list_project_repositories", None)
        if not callable(getter):
            return []
        rows = getter(project_id, realm_id=realm_id)
        project = self.store.get_project(project_id, realm_id=realm_id)
        if project is None:
            raise WorkspaceProvisioningError(
                "Project is not available on this instance"
            )
        if not rows and getattr(project, "repos", None):
            raise WorkspaceProvisioningError(
                "Project repository links are not materialized on this instance"
            )
        tool_config = dict(getattr(project, "tool_config", None) or {})
        default_policy = tool_config.get("repository_policy") or {}
        policies = tool_config.get("repository_policies") or {}
        linked: list[LinkedRepository] = []
        for repository, project_link in rows:
            identity, _ = canonical_repository_identity(repository.url)
            policy_data = dict(default_policy)
            policy_data.update(
                policies.get(repository.id) or policies.get(identity) or {}
            )
            linked.append(
                LinkedRepository(
                    repository=repository,
                    branch=project_link.branch,
                    policy=RepositoryPolicy.model_validate(policy_data),
                )
            )
        return linked

    def provision_project(
        self,
        *,
        project_id: str,
        session_id: str,
        card_id: str | None,
        realm_id: str,
        provider_id: str,
    ) -> ProvisionedWorkspace | None:
        linked = self.linked_repositories(project_id, realm_id=realm_id)
        if not linked:
            return None
        leases = [
            self.provision_repository(
                linked_repository,
                project_id=project_id,
                session_id=session_id,
                card_id=card_id,
            )
            for linked_repository in linked
        ]
        cwd = leases[0].worktree_path
        dependency_cache = self.dependency_root / self._entity_key(session_id)
        self._assert_managed_path(dependency_cache)
        dependency_cache.mkdir(parents=True, exist_ok=True)
        provider_context: dict[str, Any] = {
            "codex": {"sandbox": "workspace-write", "approval_policy": "on-request"},
            "cursor": {"workspace_mode": "isolated-checkout"},
            "open-interpreter": {
                "workspace_mode": "workspace-write",
                "container_volume": cwd,
                "approval_policy": "on-request",
            },
        }
        return ProvisionedWorkspace(
            session_id=session_id,
            card_id=card_id,
            project_id=project_id,
            cwd=cwd,
            writable_roots=[lease.worktree_path for lease in leases],
            repositories=leases,
            dependency_cache=str(dependency_cache),
            provider_context=provider_context.get(provider_id, {}),
        )

    def scratch_workspace(
        self,
        *,
        session_id: str,
        card_id: str | None,
        project_id: str | None,
        requested_cwd: str | None,
        provider_id: str,
    ) -> ProvisionedWorkspace:
        if requested_cwd:
            cwd = Path(requested_cwd).expanduser().resolve()
            data_dir = self.settings.data_dir.expanduser().resolve()
            if cwd == data_dir or data_dir in cwd.parents:
                raise WorkspaceProvisioningError(
                    "Agent cwd must be outside PA_DATA_DIR"
                )
            if cwd != self.root and self.root not in cwd.parents:
                raise WorkspaceProvisioningError(
                    "Agent cwd must be within the configured workspace_root"
                )
            if not cwd.is_dir():
                raise WorkspaceProvisioningError(f"Agent cwd does not exist: {cwd}")
            root = cwd
        else:
            root = self.scratch_root / self._entity_key(session_id)
            self._assert_managed_path(root)
            root.mkdir(parents=True, exist_ok=True)
        dependency_cache = self.dependency_root / self._entity_key(session_id)
        self._assert_managed_path(dependency_cache)
        dependency_cache.mkdir(parents=True, exist_ok=True)
        return ProvisionedWorkspace(
            session_id=session_id,
            card_id=card_id,
            project_id=project_id,
            cwd=str(root),
            writable_roots=[str(root)],
            dependency_cache=str(dependency_cache),
            provider_context={
                "sandbox": "workspace-write" if provider_id == "codex" else "workspace"
            },
        )

    def provision_repository(
        self,
        linked: LinkedRepository,
        *,
        project_id: str,
        session_id: str,
        card_id: str | None,
    ) -> WorkspaceLease:
        repository = linked.repository
        identity, clone_url = canonical_repository_identity(repository.url)
        repo_key = self._repo_key(repository.id, identity)
        cache_path = self.cache_root / repo_key
        session_key = self._entity_key(session_id)
        card_key = self._entity_key(card_id or "standalone")
        worktree_path = self.worktree_root / card_key / session_key / repo_key
        dependency_cache = self.dependency_root / session_key
        self._assert_managed_path(cache_path)
        self._assert_managed_path(worktree_path)
        self._assert_managed_path(dependency_cache)
        dependency_cache.mkdir(parents=True, exist_ok=True)
        branch = f"pa/{card_key}-{session_key}-{repo_key[-8:]}"
        with self._repository_lock(repo_key):
            return self._provision_repository_locked(
                linked,
                identity=identity,
                clone_url=clone_url,
                cache_path=cache_path,
                worktree_path=worktree_path,
                branch=branch,
                project_id=project_id,
                session_id=session_id,
                card_id=card_id,
                dependency_cache=dependency_cache,
            )

    def _provision_repository_locked(
        self,
        linked: LinkedRepository,
        *,
        identity: str,
        clone_url: str,
        cache_path: Path,
        worktree_path: Path,
        branch: str,
        project_id: str,
        session_id: str,
        card_id: str | None,
        dependency_cache: Path,
    ) -> WorkspaceLease:
        repository = linked.repository
        self._increment_metric("provision_attempts")
        existing = self.get(repository.id, session_id)
        now = datetime.now(UTC)
        if existing and (
            existing.card_id != card_id or existing.project_id != project_id
        ):
            raise WorkspaceProvisioningError(
                "Session is already fenced to a different card or project"
            )
        if existing and existing.state == "cleaned":
            existing = None
        previous_state = existing.state if existing else None
        lease = existing or WorkspaceLease(
            id=hashlib.sha256(f"{repository.id}\0{session_id}".encode()).hexdigest(),
            repository_id=repository.id,
            repository_url=identity,
            card_id=card_id,
            session_id=session_id,
            project_id=project_id,
            cache_path=str(cache_path),
            worktree_path=str(worktree_path),
            branch=branch,
            base_sha="pending",
            fencing_token=self._next_fence(repository.id),
            expires_at=now + self.LEASE_TTL,
        )
        if existing and existing.expires_at <= now:
            lease.fencing_token = self._next_fence(repository.id)
        lease.updated_at = now
        lease.expires_at = now + self.LEASE_TTL
        lease.state = "provisioning"
        lease.stage = "cache"
        lease.error = None
        self._save(lease)
        try:
            self._ensure_cache(cache_path, identity, clone_url, linked.policy)
            lease.stage = "resolve_base"
            self._save(lease)
            resolved_base = self._resolve_base(cache_path, linked.branch)
            base_sha = (
                lease.base_sha
                if existing and lease.base_sha != "pending"
                else resolved_base
            )
            lease.base_sha = base_sha
            lease.stage = "worktree"
            self._save(lease)
            self._ensure_worktree(
                cache_path,
                worktree_path,
                branch,
                base_sha,
                lease,
                recover_incomplete=previous_state == "provisioning",
            )
            self._apply_policy(worktree_path, linked.policy, dependency_cache)
            dirty, untracked = self._status(worktree_path)
            lease.dirty = dirty
            lease.untracked = untracked
            lease.state = "ready"
            lease.stage = "verified"
            lease.updated_at = datetime.now(UTC)
            self._save(lease)
            self._record_checkout(repository, cache_path, linked.branch)
            self._increment_metric("provisioned_workspaces")
            return lease
        except Exception as exc:
            lease.state = "failed"
            lease.stage = "failed"
            lease.error = str(exc)[:1000]
            lease.updated_at = datetime.now(UTC)
            self._save(lease)
            self._increment_metric("provision_failures")
            if isinstance(exc, WorkspaceProvisioningError):
                raise
            raise WorkspaceProvisioningError(str(exc)) from exc

    def _ensure_cache(
        self,
        cache_path: Path,
        identity: str,
        clone_url: str,
        policy: RepositoryPolicy,
    ) -> None:
        if cache_path.exists():
            probe = self._git(
                "-C", str(cache_path), "rev-parse", "--git-dir", check=False
            )
            if probe.returncode:
                raise WorkspaceProvisioningError(
                    f"Repository cache is invalid and retained for diagnosis: {cache_path}"
                )
            origin = self._git(
                "-C", str(cache_path), "remote", "get-url", "origin"
            ).stdout.strip()
            cached_identity, _ = canonical_repository_identity(origin)
            if cached_identity != identity:
                raise WorkspaceProvisioningError(
                    "Repository cache origin does not match the normalized repository"
                )
        else:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            for partial in cache_path.parent.glob(f".{cache_path.name}.clone-*"):
                shutil.rmtree(partial, ignore_errors=True)
            # Temp lives beside the destination so rename is atomic on the same volume.
            temp = Path(
                tempfile.mkdtemp(
                    prefix=f".{cache_path.name}.clone-", dir=cache_path.parent
                )
            )
            try:
                args = ["clone", "--no-checkout"]
                if policy.partial_clone:
                    args.extend(["--filter=blob:none"])
                args.extend([clone_url, str(temp)])
                try:
                    self._git(*args, timeout=900)
                except WorkspaceProvisioningError as exc:
                    message = str(exc).lower()
                    if not policy.partial_clone or "filter" not in message:
                        raise
                    shutil.rmtree(temp, ignore_errors=True)
                    temp.mkdir()
                    self._git(
                        "clone", "--no-checkout", clone_url, str(temp), timeout=900
                    )
                    self._increment_metric("partial_clone_fallbacks")
                os.replace(temp, cache_path)
                self._increment_metric("cache_clones")
            except Exception:
                shutil.rmtree(temp, ignore_errors=True)
                raise
        self._git(
            "-C",
            str(cache_path),
            "fetch",
            "--prune",
            "--tags",
            "origin",
            "+refs/heads/*:refs/remotes/origin/*",
            timeout=900,
        )
        self._increment_metric("cache_fetches")

    def _resolve_base(self, cache_path: Path, requested_branch: str | None) -> str:
        candidates: list[str] = []
        if requested_branch:
            candidates.extend(
                [f"refs/remotes/origin/{requested_branch}", requested_branch]
            )
        symbolic = self._git(
            "-C",
            str(cache_path),
            "symbolic-ref",
            "--quiet",
            "refs/remotes/origin/HEAD",
            check=False,
        ).stdout.strip()
        if symbolic:
            candidates.append(symbolic)
        candidates.extend(["refs/remotes/origin/main", "refs/remotes/origin/master"])
        for candidate in candidates:
            result = self._git(
                "-C",
                str(cache_path),
                "rev-parse",
                "--verify",
                f"{candidate}^{{commit}}",
                check=False,
            )
            if result.returncode == 0 and result.stdout.strip():
                return result.stdout.strip()
        raise WorkspaceProvisioningError(
            f"Could not resolve requested/default branch in {cache_path.name}"
        )

    def _ensure_worktree(
        self,
        cache_path: Path,
        worktree_path: Path,
        branch: str,
        base_sha: str,
        lease: WorkspaceLease,
        *,
        recover_incomplete: bool = False,
    ) -> None:
        if worktree_path.exists():
            self._verify_worktree(cache_path, worktree_path, lease)
            if recover_incomplete:
                # A provider cannot start before the lease reaches ready, so a
                # provisioning-state checkout is safe to finish after a crash.
                self._git("-C", str(worktree_path), "reset", "--hard", lease.branch)
            return
        worktree_path.parent.mkdir(parents=True, exist_ok=True)
        branch_exists = (
            self._git(
                "-C",
                str(cache_path),
                "show-ref",
                "--verify",
                f"refs/heads/{branch}",
                check=False,
            ).returncode
            == 0
        )
        args = ["-C", str(cache_path), "worktree", "add", "--no-checkout"]
        if branch_exists:
            args.extend([str(worktree_path), branch])
        else:
            args.extend(["-b", branch, str(worktree_path), base_sha])
        self._git(*args)
        self._git("-C", str(worktree_path), "checkout", branch)
        self._verify_worktree(cache_path, worktree_path, lease)

    def _verify_worktree(
        self, cache_path: Path, worktree_path: Path, lease: WorkspaceLease
    ) -> None:
        root = self._git(
            "-C", str(worktree_path), "rev-parse", "--show-toplevel"
        ).stdout.strip()
        if Path(root).resolve() != worktree_path.resolve():
            raise WorkspaceProvisioningError(
                "Worktree root does not match the leased path"
            )
        common = self._git(
            "-C", str(worktree_path), "rev-parse", "--git-common-dir"
        ).stdout.strip()
        common_path = Path(common)
        if not common_path.is_absolute():
            common_path = (worktree_path / common_path).resolve()
        expected = (cache_path / ".git").resolve()
        if common_path.resolve() != expected:
            raise WorkspaceProvisioningError(
                "Worktree belongs to a different repository cache"
            )
        branch = self._git(
            "-C",
            str(worktree_path),
            "symbolic-ref",
            "--quiet",
            "--short",
            "HEAD",
            check=False,
        ).stdout.strip()
        if branch != lease.branch:
            raise WorkspaceProvisioningError(
                "Leased worktree is detached or on the wrong branch"
            )

    def _apply_policy(
        self,
        worktree_path: Path,
        policy: RepositoryPolicy,
        dependency_cache: Path,
    ) -> None:
        if policy.submodules == "checkout":
            self._git(
                "-C",
                str(worktree_path),
                "submodule",
                "update",
                "--init",
                "--recursive",
                timeout=900,
            )
        if policy.lfs:
            self._git("-C", str(worktree_path), "lfs", "pull", timeout=900)
        env = os.environ.copy()
        env["PA_DEPENDENCY_CACHE"] = str(dependency_cache)
        for command in policy.setup_commands:
            try:
                result = subprocess.run(
                    command,
                    cwd=worktree_path,
                    env=env,
                    text=True,
                    capture_output=True,
                    timeout=policy.command_timeout_seconds,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise WorkspaceProvisioningError(
                    f"Setup command failed: {exc}"
                ) from exc
            if result.returncode:
                raise WorkspaceProvisioningError(
                    f"Setup command {command[0]!r} failed with exit code {result.returncode}"
                )

    def _status(self, worktree_path: Path) -> tuple[bool, int]:
        output = self._git(
            "-C", str(worktree_path), "status", "--porcelain=v1", "-z"
        ).stdout
        entries = [entry for entry in output.split("\0") if entry]
        return any(not entry.startswith("?? ") for entry in entries), sum(
            entry.startswith("?? ") for entry in entries
        )

    def _record_checkout(
        self, repository: Repository, cache_path: Path, branch: str | None
    ) -> None:
        setter = getattr(self.store, "set_repository_checkout", None)
        if not callable(setter):
            return
        lister = getattr(self.store, "list_repository_checkouts", None)
        if callable(lister):
            for checkout in lister(repository.id):
                if (
                    checkout.instance_id == self.settings.instance_id
                    and Path(checkout.path).expanduser().resolve()
                    == cache_path.resolve()
                    and checkout.branch == branch
                ):
                    return
        setter(
            RepositoryCheckout(
                repository_id=repository.id,
                instance_id=self.settings.instance_id,
                path=str(cache_path),
                branch=branch,
            ),
            realm_id=repository.realm_id,
            principal_id="instance:workspace-manager",
            instance_id=self.settings.instance_id,
        )

    def mark_card_completed(self, card_id: str, *, merged: bool) -> int:
        now = datetime.now(UTC).isoformat()
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE workspace_leases SET completed=1, merged=?, state='completed',
                   updated_at=? WHERE card_id=? AND state IN ('ready','cleanup_blocked','completed')""",
                (int(merged), now, card_id),
            )
            count = cursor.rowcount
        if count:
            self._increment_metric("completed_workspaces", count)
        return count

    def reconcile_terminal_state(self, *, now: datetime | None = None) -> dict[str, int]:
        """Reconcile private leases from synced card and session terminal state.

        Workspace lease databases are intentionally instance-local, while cards
        and sessions are durable control-plane records. A PR supervisor running
        on another fleet member cannot update this database directly, so every
        lease owner must independently observe terminal state before cleanup.
        """
        now = now or datetime.now(UTC)
        result = {
            "examined": 0,
            "cards_completed": 0,
            "standalone_completed": 0,
            "closed_expired": 0,
            "retained": 0,
        }
        for lease in self.list():
            if lease.state == "cleaned":
                continue
            result["examined"] += 1
            session = self.store.get_session(lease.session_id)
            session_closed = bool(session and session.status == "closed")
            terminal = False
            if lease.card_id:
                card = self.store.get_card(lease.card_id)
                terminal = bool(
                    card and str(getattr(card.lane, "value", card.lane)) == "done"
                )
                if terminal and not (lease.completed and lease.merged):
                    lease.completed = True
                    lease.merged = True
                    lease.state = "completed"
                    lease.stage = "reconciled_card_done"
                    lease.error = None
                    result["cards_completed"] += 1
            elif session_closed:
                terminal = True
                if not (lease.completed and lease.merged):
                    lease.completed = True
                    lease.merged = True
                    lease.state = "completed"
                    lease.stage = "reconciled_session_closed"
                    lease.error = None
                    result["standalone_completed"] += 1

            if session_closed and lease.expires_at > now:
                lease.expires_at = now
                result["closed_expired"] += 1
            if terminal or session_closed:
                lease.updated_at = now
                self._save(lease)
            else:
                result["retained"] += 1
        if result["cards_completed"] or result["standalone_completed"]:
            self._increment_metric(
                "reconciled_workspaces",
                result["cards_completed"] + result["standalone_completed"],
            )
        if result["closed_expired"]:
            self._increment_metric("expired_closed_workspaces", result["closed_expired"])
        return result

    def expire_session(self, session_id: str, *, now: datetime | None = None) -> int:
        """Make a closed session's leases immediately eligible for safe cleanup."""
        now = now or datetime.now(UTC)
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE workspace_leases SET updated_at=?, expires_at=?
                   WHERE session_id=? AND state!='cleaned'""",
                (now.isoformat(), now.isoformat(), session_id),
            )
            return cursor.rowcount

    def renew_session(self, session_id: str) -> int:
        now = datetime.now(UTC)
        with self._connect() as conn:
            cursor = conn.execute(
                """UPDATE workspace_leases SET updated_at=?, expires_at=?
                   WHERE session_id=? AND state!='cleaned'""",
                (now.isoformat(), (now + self.LEASE_TTL).isoformat(), session_id),
            )
            return cursor.rowcount

    def fence_session(self, session_id: str, *, stage: str, error: str) -> int:
        """Fence leases after provider admission fails, retaining them for retry/audit."""
        leases = [
            lease
            for lease in self.list()
            if lease.session_id == session_id and lease.state != "cleaned"
        ]
        now = datetime.now(UTC)
        for lease in leases:
            lease.fencing_token = self._next_fence(lease.repository_id)
            lease.state = "failed"
            lease.stage = stage
            lease.error = self._redact_error(str(error))[:1000]
            lease.updated_at = now
            lease.expires_at = now + self.LEASE_TTL
            self._save(lease)
        if leases:
            self._increment_metric("fenced_startup_failures", len(leases))
        return len(leases)

    def collect_garbage(
        self,
        *,
        now: datetime | None = None,
        active_session_ids: set[str] | None = None,
    ) -> dict[str, int]:
        now = now or datetime.now(UTC)
        active_session_ids = active_session_ids or set()
        result = {"cleaned": 0, "blocked": 0, "retained": 0}
        for lease in self.list():
            if lease.state == "cleaned":
                continue
            if (
                lease.session_id in active_session_ids
                or not lease.completed
                or not lease.merged
                or lease.expires_at > now
            ):
                result["retained"] += 1
                continue
            if self._cleanup_lease(lease):
                result["cleaned"] += 1
                self._increment_metric("cleaned_workspaces")
            else:
                result["blocked"] += 1
                self._increment_metric("cleanup_blocked")
        return result

    def _cleanup_lease(self, lease: WorkspaceLease) -> bool:
        with self._repository_lock(Path(lease.cache_path).name):
            return self._cleanup_lease_locked(lease)

    def _cleanup_lease_locked(self, lease: WorkspaceLease) -> bool:
        worktree = Path(lease.worktree_path)
        cache = Path(lease.cache_path)
        try:
            self._assert_managed_path(worktree)
            self._assert_managed_path(cache)
            if worktree.exists():
                dirty, untracked = self._status(worktree)
                if dirty or untracked:
                    raise WorkspaceProvisioningError("worktree has uncommitted changes")
                unpushed = self._git(
                    "-C",
                    str(worktree),
                    "rev-list",
                    lease.branch,
                    "--not",
                    "--remotes=origin",
                ).stdout.strip()
                if unpushed:
                    raise WorkspaceProvisioningError("worktree has unpushed commits")
                self._git("-C", str(cache), "worktree", "remove", str(worktree))
            self._git("-C", str(cache), "branch", "-D", lease.branch, check=False)
            lease.state = "cleaned"
            lease.stage = "cleaned"
            lease.error = None
            lease.updated_at = datetime.now(UTC)
            self._save(lease)
            return True
        except Exception as exc:
            lease.state = "cleanup_blocked"
            lease.stage = "cleanup"
            lease.error = str(exc)[:1000]
            lease.updated_at = datetime.now(UTC)
            self._save(lease)
            return False


def context_environment(context: dict[str, Any]) -> dict[str, str]:
    """Provider-neutral, secret-free structured execution context."""
    return {
        "PA_EXECUTION_CONTEXT": json.dumps(
            context, sort_keys=True, separators=(",", ":")
        ),
        "PA_WORKSPACE_ROOT": str(context["cwd"]),
        "PA_WRITABLE_ROOTS": os.pathsep.join(context.get("writable_roots") or []),
        "PA_DEPENDENCY_CACHE": str(context.get("dependency_cache") or ""),
    }
