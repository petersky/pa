"""Create releases: bump version, commit, tag, push."""

from __future__ import annotations

import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from pa.release.notes import notes_path_for_tag, write_release_notes
from pa.release.version import (
    CHANNELS_JSON,
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


def _update_channels_manifest(version: str, tag: str, *, channel: str | None = None) -> None:
    track = channel or track_for_version(version)
    data: dict[str, str] = {}
    if CHANNELS_JSON.exists():
        try:
            data = json.loads(CHANNELS_JSON.read_text())
        except json.JSONDecodeError:
            data = {}
    data[track] = tag
    if track == "release" and not channel:
        data["release"] = tag
    data.setdefault("dev", "main")
    CHANNELS_JSON.write_text(json.dumps(data, indent=2) + "\n")


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


def create_release(
    bump: str,
    *,
    channel: str | None = None,
    commit: bool = True,
    push: bool = False,
    message: str | None = None,
    notes_content: str | None = None,
    notes_path: Path | None = None,
    check_tag: bool = True,
) -> ReleaseResult:
    old = read_version()
    new = resolve_version(bump)
    tag = tag_for_version(new)
    track = channel or track_for_version(new)

    if check_tag:
        ensure_tag_available(tag)

    print(f"==> Bumping version {old} -> {new}...", flush=True)
    set_version(new)
    _update_channels_manifest(new, tag, channel=channel)
    # Keep the editable package version in uv.lock aligned with pyproject.toml.
    print("==> Updating uv.lock...", flush=True)
    _run(["uv", "lock"], cwd=ROOT)

    written_notes: Path | None = None
    if notes_content:
        written_notes = write_release_notes(tag, notes_content, path=notes_path)

    if commit:
        files = ["pyproject.toml", "src/pa/__init__.py", "channels.json", "uv.lock"]
        if written_notes:
            files.append(str(written_notes.relative_to(ROOT)))
        print(f"==> Committing release ({', '.join(files)})...", flush=True)
        _run(["git", "add", *files])
        commit_msg = message or f"Release {tag}"
        _run(["git", "commit", "-m", commit_msg])
    else:
        print("==> Skipping commit (--no-commit).", flush=True)

    print(f"==> Creating annotated tag {tag}...", flush=True)
    try:
        _run(["git", "tag", "-a", tag, "-m", message or f"Release {tag}"])
    except RuntimeError as exc:
        err = str(exc)
        if "already exists" in err:
            # Convert late failures (e.g. race after preflight) into actionable advice.
            try:
                ensure_tag_available(tag)
            except ReleaseError:
                raise
            raise ReleaseError(err) from exc
        raise

    if push:
        print("==> Pushing commit to origin...", flush=True)
        _run(["git", "push"])
        print(f"==> Pushing tag {tag} to origin...", flush=True)
        _run(["git", "push", "origin", tag])
    else:
        print("==> Skipping push (--no-push).", flush=True)

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
        print("==> Pushing amended notes to origin...", flush=True)
        _run(["git", "push"])
    else:
        print("==> Skipping push (--no-push).", flush=True)
    return written


def push_existing_release(tag: str) -> None:
    """Push main and an existing local tag to origin."""
    if not tag.startswith("v"):
        tag = f"v{tag}"
    print("==> Pushing commit to origin...", flush=True)
    _run(["git", "push"])
    print(f"==> Pushing tag {tag} to origin...", flush=True)
    _run(["git", "push", "origin", tag])


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
