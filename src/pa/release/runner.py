"""Prepare releases on a branch, then tag the merged main commit."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from pa.release.notes import notes_path_for_tag, write_release_notes
from pa.release.version import (
    ROOT,
    bump_major,
    bump_minor,
    bump_patch,
    bump_prerelease,
    read_version,
    set_version,
    tag_for_version,
    track_for_version,
    validate_version,
)


class ReleaseError(RuntimeError):
    """User-facing release failure with optional recovery hints."""

    def __init__(self, message: str, *, hints: list[str] | None = None) -> None:
        super().__init__(message)
        self.hints = list(hints or [])


@dataclass
class ReleaseResult:
    old_version: str
    new_version: str
    tag: str
    track: str
    notes_path: Path | None = None


@dataclass
class ExistingTag:
    name: str
    local_target: str | None = None
    remote_target: str | None = None
    locations: list[str] = field(default_factory=list)

    @property
    def exists(self) -> bool:
        return bool(self.locations)


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(f"{' '.join(cmd)}: {msg}")


def _capture(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)


def _capture_checked(cmd: list[str], *, cwd: Path | None = None) -> str:
    result = _capture(cmd, cwd=cwd)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(f"{' '.join(cmd)}: {msg}")
    return result.stdout.strip()


def current_branch() -> str:
    """Return the checked-out branch, rejecting detached HEAD."""
    result = _capture(["git", "branch", "--show-current"], cwd=ROOT)
    branch = result.stdout.strip()
    if result.returncode != 0 or not branch:
        raise ReleaseError("release preparation requires a checked-out branch")
    return branch


def ensure_release_branch(tag: str, *, amend: bool = False) -> str:
    """Create/switch to the branch that carries a release-related PR."""
    prefix = "release-notes" if amend else "release"
    branch = f"{prefix}/{_normalize_tag(tag)}"
    current = current_branch()
    if current == branch:
        return branch
    if current != "main":
        raise ReleaseError(
            f"release preparation must start from main or {branch}; currently on {current}",
            hints=["Switch to an up-to-date main branch, then rerun the release command."],
        )
    if _capture(["git", "status", "--porcelain"], cwd=ROOT).stdout.strip():
        raise ReleaseError(
            "release preparation requires a clean working tree",
            hints=["Commit, stash, or discard unrelated changes before preparing the release PR."],
        )
    local = _capture(["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"], cwd=ROOT)
    if local.returncode == 0:
        _run(["git", "switch", branch], cwd=ROOT)
        return branch
    remote = _capture(["git", "show-ref", "--verify", "--quiet", f"refs/remotes/origin/{branch}"], cwd=ROOT)
    if remote.returncode == 0:
        _run(["git", "switch", "-c", branch, "--track", f"origin/{branch}"], cwd=ROOT)
        return branch
    _run(["git", "switch", "-c", branch], cwd=ROOT)
    return branch


def origin_main_release_notes(tag: str, *, commit: str = "origin/main") -> str:
    """Return a release-notes file from a merged main commit."""
    if not tag.startswith("v"):
        tag = f"v{tag}"
    notes_rel = notes_path_for_tag(tag).relative_to(ROOT)
    notes_result = _capture(["git", "show", f"{commit}:{notes_rel}"], cwd=ROOT)
    if notes_result.returncode != 0:
        raise ReleaseError(
            f"{commit} does not contain {notes_rel}",
            hints=["Merge the release PR first, then rerun --publish."],
        )
    return notes_result.stdout


def release_commit_on_origin_main(tag: str) -> str:
    """Return the origin/main commit that introduced this release, not a later tip."""
    tag = _normalize_tag(tag)
    expected = tag[1:]
    notes_rel = str(notes_path_for_tag(tag).relative_to(ROOT))
    _run(["git", "fetch", "origin", "main"], cwd=ROOT)

    notes_intro = _capture(
        [
            "git",
            "log",
            "-1",
            "--format=%H",
            "--diff-filter=A",
            "origin/main",
            "--",
            notes_rel,
        ],
        cwd=ROOT,
    )
    version_intro = _capture(
        [
            "git",
            "log",
            "-1",
            "--format=%H",
            "-S",
            f'version = "{expected}"',
            "origin/main",
            "--",
            "pyproject.toml",
        ],
        cwd=ROOT,
    )
    sha = (notes_intro.stdout.strip() or version_intro.stdout.strip())
    if not sha:
        raise ReleaseError(
            f"could not find the {tag} release commit on origin/main",
            hints=["Merge the release PR first, then rerun --publish."],
        )

    ancestor = _capture(["git", "merge-base", "--is-ancestor", sha, "origin/main"], cwd=ROOT)
    if ancestor.returncode != 0:
        raise ReleaseError(f"release commit {sha[:12]} is not on origin/main")

    version_result = _capture(["git", "show", f"{sha}:pyproject.toml"], cwd=ROOT)
    if version_result.returncode != 0 or f'version = "{expected}"' not in version_result.stdout:
        raise ReleaseError(
            f"origin/main release commit for {tag} does not contain version {expected}",
            hints=["Merge the release PR first, then rerun --publish."],
        )
    origin_main_release_notes(tag, commit=sha)
    return sha


def _normalize_tag(tag: str) -> str:
    return tag if tag.startswith("v") else f"v{tag}"


def local_tag_target(tag: str) -> str | None:
    """Return the commit (or object) a local tag points at, or None if missing."""
    tag = _normalize_tag(tag)
    result = _capture(["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}^{{}}"], cwd=ROOT)
    if result.returncode == 0 and result.stdout.strip():
        return result.stdout.strip()
    # Lightweight tags (or missing peeled object) fall back to the tag object itself.
    result = _capture(["git", "rev-parse", "--verify", "--quiet", f"refs/tags/{tag}"], cwd=ROOT)
    text = result.stdout.strip()
    return text or None


def remote_tag_target(tag: str, *, remote: str = "origin") -> str | None:
    """Return the commit a remote tag points at, or None if missing / unreachable."""
    tag = _normalize_tag(tag)
    result = _capture(["git", "ls-remote", "--tags", remote, f"refs/tags/{tag}"], cwd=ROOT)
    if result.returncode != 0:
        return None
    peeled: str | None = None
    direct: str | None = None
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) < 2:
            continue
        sha, ref = parts[0], parts[1]
        if ref == f"refs/tags/{tag}^{{}}":
            peeled = sha
        elif ref == f"refs/tags/{tag}":
            direct = sha
    return peeled or direct


def find_existing_tag(tag: str, *, check_remote: bool = True, remote: str = "origin") -> ExistingTag:
    """Locate a tag locally and optionally on the remote."""
    tag = _normalize_tag(tag)
    local = local_tag_target(tag)
    remote_target = remote_tag_target(tag, remote=remote) if check_remote else None
    locations: list[str] = []
    if local:
        locations.append("local")
    if remote_target:
        locations.append(remote)
    return ExistingTag(
        name=tag,
        local_target=local,
        remote_target=remote_target,
        locations=locations,
    )


def _short_commit(sha: str | None) -> str:
    if not sha:
        return "unknown"
    return sha[:12]


def _subject_for_commit(sha: str) -> str | None:
    result = _capture(["git", "log", "-1", "--pretty=%s", sha], cwd=ROOT)
    if result.returncode != 0:
        return None
    return result.stdout.strip() or None


def ensure_tag_available(tag: str, *, check_remote: bool = True) -> None:
    """Abort early if the release tag already exists locally or on origin."""
    existing = find_existing_tag(tag, check_remote=check_remote)
    if not existing.exists:
        return

    tag = existing.name
    where = " and ".join(existing.locations)
    details: list[str] = []
    if existing.local_target:
        subject = _subject_for_commit(existing.local_target)
        detail = f"local -> {_short_commit(existing.local_target)}"
        if subject:
            detail += f" ({subject})"
        details.append(detail)
    if existing.remote_target:
        details.append(f"origin -> {_short_commit(existing.remote_target)}")

    message = f"tag {tag} already exists ({where})"
    if details:
        message += ": " + "; ".join(details)

    version = tag[1:] if tag.startswith("v") else tag
    try:
        next_patch = bump_patch(version)
        next_example = f"./scripts/release.sh {next_patch}"
    except ValueError:
        next_example = "./scripts/release.sh <next-version>"

    on_remote = bool(existing.remote_target)
    hints: list[str] = []
    if existing.local_target and not on_remote:
        hints.append(
            f"If {tag} is a leftover local tag you want to replace:\n"
            f"    git tag -d {tag}\n"
            f"    ./scripts/release.sh <bump>"
        )
    hints.append(
        f"If {tag} was already released, bump to the next version instead:\n"
        f"    {next_example}   # or: minor / major / explicit semver"
    )
    hints.append(
        f"If the existing tag is correct and you only need to push/publish notes:\n"
        f"    ./scripts/release.sh --publish --tag {tag}"
    )
    if existing.local_target and not on_remote:
        hints.append(
            f"If a release commit already exists but tagging failed mid-run:\n"
            f"    git tag -d {tag} && git tag -a {tag} -m 'Release {tag}'\n"
            f"    ./scripts/release.sh --publish --tag {tag}"
        )
    elif on_remote:
        hints.append(
            f"Do not delete/reuse {tag}: it already exists on origin. "
            "Prefer bumping to the next version (option above)."
        )
    raise ReleaseError(message, hints=hints)


def commits_behind_origin_main(
    *,
    remote: str = "origin",
    branch: str = "main",
    fetch: bool = True,
) -> int:
    """Return how many commits HEAD is behind ``origin/main`` (0 if up to date or unknown)."""
    ref = f"{remote}/{branch}"
    if fetch:
        fetched = _capture(["git", "fetch", remote, branch], cwd=ROOT)
        if fetched.returncode != 0:
            msg = fetched.stderr.strip() or fetched.stdout.strip() or "fetch failed"
            raise RuntimeError(f"git fetch {remote} {branch}: {msg}")
    counted = _capture(["git", "rev-list", "--count", f"HEAD..{ref}"], cwd=ROOT)
    if counted.returncode != 0:
        msg = counted.stderr.strip() or counted.stdout.strip() or "rev-list failed"
        raise RuntimeError(f"git rev-list --count HEAD..{ref}: {msg}")
    text = counted.stdout.strip()
    return int(text) if text else 0


def resolve_version(bump: str) -> str:
    bump = bump.lower()
    old = read_version()
    if bump == "patch":
        return bump_patch(old)
    if bump == "minor":
        return bump_minor(old)
    if bump == "major":
        return bump_major(old)
    if bump in {"alpha", "beta", "rc"}:
        return bump_prerelease(old, bump)
    return validate_version(bump)


def _require_release_branch(branch: str, *, amend: bool = False) -> None:
    prefix = "release-notes/" if amend else "release/"
    if branch.startswith(prefix):
        return
    kind = "release-notes/*" if amend else "release/*"
    raise ReleaseError(
        f"refusing to push release changes from {branch}; expected a {kind} branch",
        hints=[
            "Start from an up-to-date main branch so the release command can create the PR branch.",
        ],
    )


def ensure_release_pr(tag: str, branch: str) -> str:
    """Create the release PR if needed and return its URL."""
    tag = _normalize_tag(tag)
    existing = _capture_checked(
        [
            "gh",
            "pr",
            "list",
            "--base",
            "main",
            "--head",
            branch,
            "--state",
            "open",
            "--limit",
            "1",
            "--json",
            "url",
        ],
        cwd=ROOT,
    )
    try:
        matches = json.loads(existing or "[]")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"could not parse gh pr list output: {exc}") from exc
    if matches:
        url = str(matches[0]["url"])
        print(f"  Using existing release PR: {url}", flush=True)
        return url

    print(f"==> Creating release PR for {tag}...", flush=True)
    url = _capture_checked(
        [
            "gh",
            "pr",
            "create",
            "--base",
            "main",
            "--head",
            branch,
            "--title",
            f"Release {tag}",
            "--body",
            f"Prepare PA {tag} for publication.",
        ],
        cwd=ROOT,
    )
    if not url:
        raise RuntimeError("gh pr create did not return a pull request URL")
    print(f"  Created release PR: {url}", flush=True)
    return url


def merge_release_pr(
    pr: str,
    *,
    head_commit: str,
    check_discovery_timeout: float = 60.0,
) -> None:
    """Wait for PR checks and merge the unchanged release head."""
    print("==> Waiting for release PR checks...", flush=True)
    deadline = time.monotonic() + check_discovery_timeout
    while True:
        checks = _capture(
            ["gh", "pr", "checks", pr, "--watch", "--fail-fast"],
            cwd=ROOT,
        )
        if checks.returncode == 0:
            break
        message = checks.stderr.strip() or checks.stdout.strip() or "command failed"
        if "no checks reported" not in message.lower():
            raise ReleaseError(
                "release PR checks did not pass",
                hints=[f"Inspect or resume the release from {pr}"],
            )
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ReleaseError(
                "no checks appeared for the release PR",
                hints=[f"Inspect the Actions configuration and release PR at {pr}"],
            )
        delay = min(2.0, remaining)
        print(f"  Checks not registered yet; retrying in {delay:.0f}s...", flush=True)
        time.sleep(delay)

    print("==> Merging release PR...", flush=True)
    try:
        _run(
            [
                "gh",
                "pr",
                "merge",
                pr,
                "--merge",
                "--match-head-commit",
                head_commit,
            ],
            cwd=ROOT,
        )
    except RuntimeError as exc:
        raise ReleaseError(
            "could not merge the release PR",
            hints=[
                f"Resolve any review or branch-protection requirements at {pr}, then run:\n"
                f"    ./scripts/release.sh --publish"
            ],
        ) from exc
    print(f"  Merged release PR: {pr}", flush=True)


def cleanup_release_branch(branch: str, *, remote: str = "origin") -> None:
    """Switch back to main and delete the finished release branch locally and on origin.

    Safe to call after the release PR has merged (or the remote branch is already
    gone). Refuses to delete anything outside ``release/`` / ``release-notes/``.
    Idempotent when the branch is already absent.
    """
    if not branch.startswith(("release/", "release-notes/")):
        raise ReleaseError(
            f"refusing to clean up non-release branch {branch}",
            hints=["Pass the release/* or release-notes/* branch created for this release."],
        )

    print(f"==> Cleaning up {branch}...", flush=True)
    _run(["git", "fetch", remote, "main"], cwd=ROOT)

    local = _capture(
        ["git", "show-ref", "--verify", "--quiet", f"refs/heads/{branch}"],
        cwd=ROOT,
    )
    remote_heads = _capture(["git", "ls-remote", "--heads", remote, branch], cwd=ROOT)
    if remote_heads.returncode != 0:
        # Network/auth failures must not be treated as "already deleted".
        remote_probe_ok = False
        remote_exists = True
        probe_error = (
            remote_heads.stderr.strip()
            or remote_heads.stdout.strip()
            or "ls-remote failed"
        )
        print(
            f"  warning: could not probe {remote}/{branch} ({probe_error}); "
            "will still attempt remote delete.",
            flush=True,
        )
    else:
        remote_probe_ok = True
        remote_exists = bool(remote_heads.stdout.strip())

    if local.returncode != 0 and remote_probe_ok and not remote_exists:
        print(f"  Branch {branch} already cleaned up.", flush=True)
        try:
            on_main = current_branch() == "main"
        except ReleaseError:
            on_main = False
        if not on_main:
            _run(["git", "switch", "main"], cwd=ROOT)
        ff = _capture(["git", "merge", "--ff-only", f"{remote}/main"], cwd=ROOT)
        if ff.returncode != 0:
            msg = ff.stderr.strip() or ff.stdout.strip() or "fast-forward failed"
            print(
                f"  warning: could not fast-forward main onto {remote}/main: {msg}",
                flush=True,
            )
        return

    current = current_branch()
    if current == branch:
        _run(["git", "switch", "main"], cwd=ROOT)
    elif current != "main":
        print(
            f"  Leaving checkout on {current}; still removing {branch}.",
            flush=True,
        )

    if current_branch() == "main":
        ff = _capture(["git", "merge", "--ff-only", f"{remote}/main"], cwd=ROOT)
        if ff.returncode != 0:
            msg = ff.stderr.strip() or ff.stdout.strip() or "fast-forward failed"
            print(
                f"  warning: could not fast-forward main onto {remote}/main: {msg}",
                flush=True,
            )

    if local.returncode == 0:
        deleted = _capture(["git", "branch", "-d", branch], cwd=ROOT)
        if deleted.returncode != 0:
            # GitHub merge can leave the local tip not considered merged until
            # main is updated; after fetch/ff, force-delete leftovers.
            msg = deleted.stderr.strip() or deleted.stdout.strip() or "not fully merged"
            print(f"  git branch -d failed ({msg}); deleting with -D.", flush=True)
            _run(["git", "branch", "-D", branch], cwd=ROOT)
        print(f"  Deleted local branch {branch}.", flush=True)

    if remote_exists:
        removed = _capture(["git", "push", remote, "--delete", branch], cwd=ROOT)
        if removed.returncode != 0:
            msg = removed.stderr.strip() or removed.stdout.strip() or "delete failed"
            print(f"  warning: could not delete {remote}/{branch}: {msg}", flush=True)
        else:
            print(f"  Deleted {remote}/{branch}.", flush=True)
    elif remote_probe_ok:
        print(f"  Remote branch {remote}/{branch} already gone.", flush=True)

    _capture(["git", "fetch", "--prune", remote], cwd=ROOT)


def head_commit() -> str:
    """Return the current commit SHA."""
    sha = _capture_checked(["git", "rev-parse", "HEAD"], cwd=ROOT)
    if not sha:
        raise RuntimeError("git rev-parse HEAD returned no commit")
    return sha


def create_release(
    bump: str,
    *,
    target_version: str | None = None,
    channel: str | None = None,
    commit: bool = True,
    push: bool = False,
    message: str | None = None,
    notes_content: str | None = None,
    notes_path: Path | None = None,
    check_tag: bool = True,
    create_tag: bool = False,
) -> ReleaseResult:
    old = read_version()
    # Callers that create/switch the release branch first must carry forward the
    # version resolved from main.  Re-resolving a relative bump on a reused
    # release branch can otherwise bump an already-bumped or stale version.
    new = (
        validate_version(target_version)
        if target_version
        else resolve_version(bump)
    )
    tag = tag_for_version(new)
    track = channel or track_for_version(new)

    if check_tag:
        ensure_tag_available(tag)

    print(f"==> Bumping version {old} -> {new}...", flush=True)
    set_version(new)
    # Keep the editable package version in uv.lock aligned with pyproject.toml.
    # channels.json is updated by CI after the tag exists (avoids advertising a missing tag).
    print("==> Updating uv.lock...", flush=True)
    _run(["uv", "lock"], cwd=ROOT)

    written_notes: Path | None = None
    if notes_content:
        written_notes = write_release_notes(tag, notes_content, path=notes_path)

    if commit:
        files = ["pyproject.toml", "src/pa/__init__.py", "uv.lock"]
        if written_notes:
            files.append(str(written_notes.relative_to(ROOT)))
        print(f"==> Committing release ({', '.join(files)})...", flush=True)
        _run(["git", "add", *files])
        commit_msg = message or f"Release {tag}"
        _run(["git", "commit", "-m", commit_msg])
    else:
        print("==> Skipping commit (--no-commit).", flush=True)

    if push:
        branch = current_branch()
        _require_release_branch(branch)
        print(f"==> Pushing release branch {branch} to origin...", flush=True)
        _run(["git", "push", "-u", "origin", branch])
    else:
        print("==> Skipping release-branch push (--no-push).", flush=True)

    if create_tag:
        print(f"==> Creating annotated tag {tag}...", flush=True)
        _run(["git", "tag", "-a", tag, "-m", message or f"Release {tag}"])

    return ReleaseResult(
        old_version=old,
        new_version=new,
        tag=tag,
        track=track,
        notes_path=written_notes,
    )


def amend_release_notes(
    tag: str,
    notes_content: str,
    *,
    commit: bool = True,
    push: bool = False,
    message: str | None = None,
    notes_path: Path | None = None,
) -> Path:
    """Update release notes for an existing tag without bumping version."""
    if not tag.startswith("v"):
        tag = f"v{tag}"
    written = write_release_notes(tag, notes_content, path=notes_path)
    if commit:
        rel = str(written.relative_to(ROOT))
        print(f"==> Committing amended notes ({rel})...", flush=True)
        _run(["git", "add", rel])
        _run(["git", "commit", "-m", message or f"Amend release notes for {tag}"])
    else:
        print("==> Skipping commit (--no-commit).", flush=True)
    if push:
        branch = current_branch()
        _require_release_branch(branch, amend=True)
        print(f"==> Pushing amended-notes branch {branch} to origin...", flush=True)
        _run(["git", "push", "-u", "origin", branch])
    else:
        print("==> Skipping push (--no-push).", flush=True)
    return written


def tag_merged_release(tag: str, *, message: str | None = None, push: bool = True) -> Path:
    """Tag the verified release commit on origin/main and optionally push that tag."""
    if not tag.startswith("v"):
        tag = f"v{tag}"
    ensure_tag_available(tag)
    commit = release_commit_on_origin_main(tag)
    tip = _capture(["git", "rev-parse", "origin/main"], cwd=ROOT).stdout.strip()
    if tip and tip != commit:
        print(
            f"==> origin/main has moved past the {tag} release commit; "
            f"tagging {commit[:12]} (not {tip[:12]})...",
            flush=True,
        )
    print(f"==> Creating annotated tag {tag} on {commit[:12]}...", flush=True)
    _run(["git", "tag", "-a", tag, commit, "-m", message or f"Release {tag}"], cwd=ROOT)
    if push:
        print(f"==> Pushing tag {tag} to origin...", flush=True)
        _run(["git", "push", "origin", tag])
    else:
        print("==> Skipping tag push (--no-push).", flush=True)
    return notes_path_for_tag(tag)


def github_release_exists(tag: str) -> bool:
    """Return True if a GitHub release for the tag already exists."""
    result = subprocess.run(
        ["gh", "release", "view", tag],
        capture_output=True,
        text=True,
    )
    return result.returncode == 0


def wait_for_github_release(
    tag: str,
    *,
    timeout: float,
    initial_delay: float = 2.0,
    max_delay: float = 15.0,
) -> bool:
    """Poll until CI creates the GitHub release.

    Uses exponential backoff between checks. Returns True if the release
    appears before ``timeout`` seconds elapse, otherwise False.
    """
    if timeout <= 0:
        return github_release_exists(tag)

    deadline = time.monotonic() + timeout
    delay = initial_delay

    if github_release_exists(tag):
        return True

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return False
        sleep_for = min(delay, remaining)
        print(
            f"  GitHub release not ready yet; retrying in {sleep_for:.0f}s "
            f"({max(0, remaining - sleep_for):.0f}s left)...",
            flush=True,
        )
        time.sleep(sleep_for)
        if github_release_exists(tag):
            return True
        delay = min(delay * 2, max_delay)


def publish_github_release(tag: str, notes_path: Path, *, amend: bool = False) -> None:
    """Create or update GitHub release with notes file."""
    if amend:
        print(f"==> Updating GitHub release notes for {tag}...", flush=True)
        _run(
            [
                "gh",
                "release",
                "edit",
                tag,
                "--notes-file",
                str(notes_path),
            ]
        )
        return

    # Release may be created by CI on tag push; try edit first, then create.
    print(f"==> Publishing GitHub release notes for {tag}...", flush=True)
    edit = subprocess.run(
        ["gh", "release", "edit", tag, "--notes-file", str(notes_path)],
        capture_output=True,
        text=True,
    )
    if edit.returncode == 0:
        return
    print(f"  Release not found yet; creating GitHub release {tag}...", flush=True)
    _run(
        [
            "gh",
            "release",
            "create",
            tag,
            "--notes-file",
            str(notes_path),
            "--title",
            tag,
        ]
    )
