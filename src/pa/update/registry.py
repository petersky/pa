"""Release track definitions (release, beta, alpha, dev)."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ReleaseTrack(StrEnum):
    RELEASE = "release"
    BETA = "beta"
    ALPHA = "alpha"
    DEV = "dev"


@dataclass(frozen=True)
class TrackInfo:
    name: str
    description: str


TRACKS: dict[str, TrackInfo] = {
    ReleaseTrack.RELEASE: TrackInfo(
        name="release",
        description="Latest stable release (non-prerelease)",
    ),
    ReleaseTrack.BETA: TrackInfo(
        name="beta",
        description="Latest beta prerelease",
    ),
    ReleaseTrack.ALPHA: TrackInfo(
        name="alpha",
        description="Latest alpha prerelease",
    ),
    ReleaseTrack.DEV: TrackInfo(
        name="dev",
        description="Development branch (main)",
    ),
}


def normalize_track(name: str) -> str:
    normalized = name.strip().lower()
    # Backward compatibility with old update_channel values
    if normalized in ("github", "stable"):
        return ReleaseTrack.RELEASE
    if normalized == "main":
        return ReleaseTrack.DEV
    if normalized not in TRACKS and normalized != "pypi":
        raise ValueError(
            f"Unknown release track: {name}. "
            f"Choose from: {', '.join(list_tracks())}, pypi"
        )
    return normalized


def list_tracks() -> list[str]:
    return [info.name for info in TRACKS.values()]


def describe_tracks() -> list[dict[str, str]]:
    return [
        {"name": info.name, "description": info.description}
        for info in TRACKS.values()
    ]
