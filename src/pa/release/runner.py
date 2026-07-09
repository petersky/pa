"""Create releases: bump version, commit, tag, push."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
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


@dataclass
class ReleaseResult:
    old_version: str
    new_version: str
    tag: str
    track: str
    notes_path: Path | None = None


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(f"{' '.join(cmd)}: {msg}")


def _capture(cmd: list[str], *, cwd: Path | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)


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
) -> ReleaseResult:
    old = read_version()
    new = resolve_version(bump)
    tag = tag_for_version(new)
    track = channel or track_for_version(new)

    set_version(new)
    _update_channels_manifest(new, tag, channel=channel)
    # Keep the editable package version in uv.lock aligned with pyproject.toml.
    _run(["uv", "lock"], cwd=ROOT)

    written_notes: Path | None = None
    if notes_content:
        written_notes = write_release_notes(tag, notes_content, path=notes_path)

    if commit:
        files = ["pyproject.toml", "src/pa/__init__.py", "channels.json", "uv.lock"]
        if written_notes:
            files.append(str(written_notes.relative_to(ROOT)))
        _run(["git", "add", *files])
        commit_msg = message or f"Release {tag}"
        _run(["git", "commit", "-m", commit_msg])

    _run(["git", "tag", "-a", tag, "-m", message or f"Release {tag}"])

    if push:
        _run(["git", "push"])
        _run(["git", "push", "origin", tag])

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
        _run(["git", "add", str(written.relative_to(ROOT))])
        _run(["git", "commit", "-m", message or f"Amend release notes for {tag}"])
    if push:
        _run(["git", "push"])
    return written


def push_existing_release(tag: str) -> None:
    """Push main and an existing local tag to origin."""
    if not tag.startswith("v"):
        tag = f"v{tag}"
    _run(["git", "push"])
    _run(["git", "push", "origin", tag])


def publish_github_release(tag: str, notes_path: Path, *, amend: bool = False) -> None:
    """Create or update GitHub release with notes file."""
    if amend:
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
    edit = subprocess.run(
        ["gh", "release", "edit", tag, "--notes-file", str(notes_path)],
        capture_output=True,
        text=True,
    )
    if edit.returncode == 0:
        return
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
