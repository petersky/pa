"""Create releases: bump version, commit, tag, push."""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass
from pathlib import Path

from pa.release.version import (
    CHANNELS_JSON,
    bump_major,
    bump_minor,
    bump_patch,
    bump_prerelease,
    is_prerelease_version,
    read_version,
    tag_for_version,
    track_for_version,
    write_version,
)


@dataclass
class ReleaseResult:
    old_version: str
    new_version: str
    tag: str
    track: str


def _run(cmd: list[str], *, cwd: Path | None = None) -> None:
    result = subprocess.run(cmd, cwd=cwd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "command failed"
        raise RuntimeError(f"{' '.join(cmd)}: {msg}")


def _update_channels_manifest(version: str, tag: str) -> None:
    track = track_for_version(version)
    data: dict[str, str] = {}
    if CHANNELS_JSON.exists():
        try:
            data = json.loads(CHANNELS_JSON.read_text())
        except json.JSONDecodeError:
            data = {}
    data[track] = tag
    if track == "release":
        data["release"] = tag
    data.setdefault("dev", "main")
    CHANNELS_JSON.write_text(json.dumps(data, indent=2) + "\n")


def create_release(
    bump: str,
    *,
    commit: bool = True,
    push: bool = False,
    message: str | None = None,
) -> ReleaseResult:
    old = read_version()
    bump = bump.lower()

    if bump == "patch":
        new = bump_patch(old)
    elif bump == "minor":
        new = bump_minor(old)
    elif bump == "major":
        new = bump_major(old)
    elif bump in {"alpha", "beta", "rc"}:
        new = bump_prerelease(old, bump)
    else:
        raise ValueError("bump must be patch, minor, major, alpha, beta, or rc")

    tag = tag_for_version(new)
    write_version(new)
    _update_channels_manifest(new, tag)

    if commit:
        _run(["git", "add", "pyproject.toml", "src/pa/__init__.py", "channels.json"])
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
        track=track_for_version(new),
    )
