"""Update channels for pa update."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

import httpx

from pa.update.registry import ReleaseTrack, normalize_track


@dataclass(frozen=True)
class ReleaseInfo:
    version: str
    install_spec: str
    url: str | None = None
    tag: str | None = None
    track: str | None = None
    notes: str | None = None
    name: str | None = None


class UpdateChannel(ABC):
    track: str

    @abstractmethod
    def latest(self) -> ReleaseInfo | None: ...

    @abstractmethod
    def install(self, release: ReleaseInfo) -> None: ...


def _parse_version(version: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", version)
    return tuple(int(p) for p in parts) if parts else (0,)


def is_newer(current: str, latest: str, *, track: str | None = None) -> bool:
    if latest == "dev" or track == "dev":
        return True
    return _parse_version(latest) > _parse_version(current)


def _github_headers() -> dict[str, str]:
    return {"Accept": "application/vnd.github+json"}


class GitHubTrackChannel(UpdateChannel):
    """Resolve a release track from GitHub releases and tags."""

    def __init__(self, track: str, repo: str) -> None:
        self.track = normalize_track(track)
        self.repo = repo.strip().strip("/")

    def latest(self) -> ReleaseInfo | None:
        if self.track == ReleaseTrack.DEV:
            ref = _ref_from_channels_json(self.track, repo=self.repo) or "main"
            return ReleaseInfo(
                version="dev",
                install_spec=f"git+https://github.com/{self.repo}.git@{ref}",
                url=f"https://github.com/{self.repo}",
                tag=ref,
                track=self.track,
            )

        releases = self._list_releases()
        if releases:
            match = self._pick_release(releases)
            if match:
                return self._release_info(match)

        ref = _ref_from_channels_json(self.track, repo=self.repo)
        if ref:
            version = ref.lstrip("v") if ref != "main" else "dev"
            return ReleaseInfo(
                version=version,
                install_spec=f"git+https://github.com/{self.repo}.git@{ref}",
                url=f"https://github.com/{self.repo}",
                tag=ref,
                track=self.track,
            )

        return self._latest_tag_fallback()

    def _list_releases(self) -> list[dict]:
        url = f"https://api.github.com/repos/{self.repo}/releases"
        try:
            resp = httpx.get(
                url,
                timeout=15.0,
                headers=_github_headers(),
                params={"per_page": 30},
            )
            resp.raise_for_status()
            return resp.json()
        except httpx.HTTPError:
            return []

    def _pick_release(self, releases: list[dict]) -> dict | None:
        if self.track == ReleaseTrack.RELEASE:
            for item in releases:
                if not item.get("prerelease"):
                    return item
            return None

        if self.track == ReleaseTrack.BETA:
            for item in releases:
                tag = item.get("tag_name", "").lower()
                if item.get("prerelease") and "beta" in tag:
                    return item
            return None

        if self.track == ReleaseTrack.ALPHA:
            for item in releases:
                tag = item.get("tag_name", "").lower()
                if item.get("prerelease") and "alpha" in tag:
                    return item
            return None

        return None

    def _release_info(self, data: dict) -> ReleaseInfo:
        tag_name = data.get("tag_name", "")
        version = tag_name.lstrip("v")

        wheel_url = None
        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if name.endswith(".whl"):
                wheel_url = asset.get("browser_download_url")
                break

        install_spec = (
            wheel_url
            if wheel_url
            else f"git+https://github.com/{self.repo}.git@{tag_name}"
        )
        return ReleaseInfo(
            version=version,
            install_spec=install_spec,
            url=data.get("html_url"),
            tag=tag_name,
            track=self.track,
            notes=(data.get("body") or "").strip() or None,
            name=data.get("name") or None,
        )

    def _latest_tag_fallback(self) -> ReleaseInfo | None:
        url = f"https://api.github.com/repos/{self.repo}/tags"
        try:
            resp = httpx.get(url, timeout=15.0, headers=_github_headers())
            resp.raise_for_status()
            tags = resp.json()
        except httpx.HTTPError:
            return None

        for item in tags:
            tag_name = item.get("name", "")
            if self._tag_matches_track(tag_name):
                version = tag_name.lstrip("v")
                return ReleaseInfo(
                    version=version,
                    install_spec=f"git+https://github.com/{self.repo}.git@{tag_name}",
                    url=f"https://github.com/{self.repo}/releases/tag/{tag_name}",
                    tag=tag_name,
                    track=self.track,
                )
        return None

    def _tag_matches_track(self, tag_name: str) -> bool:
        lower = tag_name.lower()
        if self.track == ReleaseTrack.RELEASE:
            return bool(re.fullmatch(r"v?\d+\.\d+\.\d+", tag_name))
        if self.track == ReleaseTrack.BETA:
            return "beta" in lower
        if self.track == ReleaseTrack.ALPHA:
            return "alpha" in lower
        return False

    def install(self, release: ReleaseInfo) -> None:
        _uv_tool_install(release.install_spec)


class PyPIChannel(UpdateChannel):
    track = "pypi"

    def __init__(self, package: str = "pa") -> None:
        self.package = package

    def latest(self) -> ReleaseInfo | None:
        url = f"https://pypi.org/pypi/{self.package}/json"
        try:
            resp = httpx.get(url, timeout=15.0)
            resp.raise_for_status()
            version = resp.json()["info"]["version"]
        except (httpx.HTTPError, KeyError):
            return None
        return ReleaseInfo(
            version=version,
            install_spec=f"{self.package}=={version}",
            track="pypi",
        )

    def install(self, release: ReleaseInfo) -> None:
        _uv_tool_install(release.install_spec)


def _uv_tool_install(spec: str) -> None:
    uv = resolve_uv_binary()
    result = subprocess.run(
        [str(uv), "tool", "install", "--force", spec],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"uv tool install failed for {spec}")


def resolve_uv_binary() -> str:
    """Find uv in interactive shells and sparse launchd/systemd environments."""
    configured = os.environ.get("PA_UV_BIN", "").strip()
    candidates = [
        configured,
        shutil.which("uv") or "",
        str(Path.home() / ".local" / "bin" / "uv"),
        str(Path.home() / ".cargo" / "bin" / "uv"),
        "/opt/homebrew/bin/uv",
        "/usr/local/bin/uv",
        "/usr/bin/uv",
    ]
    for candidate in candidates:
        if candidate and Path(candidate).is_file() and os.access(candidate, os.X_OK):
            return candidate
    raise RuntimeError(
        "uv was not found; install uv or set PA_UV_BIN to its absolute path "
        "for the PA service environment"
    )


def get_channel(name: str, *, repo: str = "petersky/pa") -> UpdateChannel:
    normalized = normalize_track(name)
    if normalized == "pypi":
        return PyPIChannel()
    return GitHubTrackChannel(normalized, repo)


def resolve_track_ref(track: str, *, repo: str = "petersky/pa") -> str:
    """Return git ref (tag or branch) for a release track."""
    ref = _ref_from_channels_json(track, repo=repo)
    if ref:
        return ref
    channel = get_channel(track, repo=repo)
    release = channel.latest()
    if not release:
        raise RuntimeError(f"Could not resolve release track: {track}")
    if release.tag:
        return release.tag
    return "main"


def _ref_from_channels_json(track: str, *, repo: str = "petersky/pa") -> str | None:
    """Read channels.json from main branch (same source as install-remote.sh)."""
    import json

    normalized = normalize_track(track)
    url = f"https://raw.githubusercontent.com/{repo.strip().strip('/')}/main/channels.json"
    try:
        resp = httpx.get(url, timeout=15.0, follow_redirects=True)
        resp.raise_for_status()
        data = json.loads(resp.text)
        ref = data.get(normalized) or data.get(track)
        return ref if ref else None
    except (httpx.HTTPError, json.JSONDecodeError, KeyError):
        return None
