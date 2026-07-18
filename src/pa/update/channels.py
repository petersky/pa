"""Update channels for pa update."""

from __future__ import annotations

import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass
from urllib.parse import quote

import httpx

from pa.packaging.uv import resolve_uv_binary
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
    revision: str | None = None


class UpdateChannel(ABC):
    track: str

    @abstractmethod
    def latest(self) -> ReleaseInfo | None: ...

    @abstractmethod
    def install(self, release: ReleaseInfo) -> None: ...


def _parse_version(version: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", version)
    return tuple(int(p) for p in parts) if parts else (0,)


def compare_versions(left: str, right: str) -> int:
    """Compare semantic PA versions, including common prerelease suffixes."""

    def key(value: str) -> tuple[tuple[int, ...], tuple[int, int, tuple[int, ...]]]:
        normalized = value.strip().lower().removeprefix("v")
        match = re.fullmatch(
            r"(?P<release>\d+(?:\.\d+)*)"
            r"(?:[-.]?(?P<label>alpha|a|beta|b|rc|pre|preview)"
            r"[-.]?(?P<number>\d*)?)?",
            normalized,
        )
        if not match:
            raise ValueError(f"Invalid version {value!r}; expected a semantic version")
        release_parts = [int(part) for part in match.group("release").split(".")]
        while len(release_parts) > 3 and release_parts[-1] == 0:
            release_parts.pop()
        release = tuple(release_parts) + (0,) * max(0, 3 - len(release_parts))
        label = match.group("label")
        if not label:
            prerelease = (1, 0, ())
        else:
            rank = {
                "alpha": 0,
                "a": 0,
                "beta": 1,
                "b": 1,
                "pre": 2,
                "preview": 2,
                "rc": 2,
            }[label]
            number = int(match.group("number") or 0)
            prerelease = (0, rank, (number,))
        return release, prerelease

    left_key = key(left)
    right_key = key(right)
    return (left_key > right_key) - (left_key < right_key)


def is_newer(current: str, latest: str, *, track: str | None = None) -> bool:
    if latest == "dev" or track == "dev":
        return True
    try:
        return compare_versions(latest, current) > 0
    except ValueError:
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
            revision = _resolve_github_revision(self.repo, ref)
            if not revision:
                return None
            return ReleaseInfo(
                version="dev",
                install_spec=f"git+https://github.com/{self.repo}.git@{revision}",
                url=f"https://github.com/{self.repo}",
                tag=ref,
                track=self.track,
                revision=revision,
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
        except httpx.HTTPError, KeyError:
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


def get_channel(name: str, *, repo: str = "petersky/pa") -> UpdateChannel:
    normalized = normalize_track(name)
    if normalized == "pypi":
        return PyPIChannel()
    return GitHubTrackChannel(normalized, repo)


def resolve_release(
    name: str,
    version: str,
    *,
    repo: str = "petersky/pa",
    revision: str | None = None,
) -> ReleaseInfo:
    """Resolve an exact fleet target using the selected channel's native semantics."""
    normalized = normalize_track(name)
    if normalized == ReleaseTrack.DEV:
        if revision:
            if not re.fullmatch(r"[0-9a-fA-F]{40}", revision):
                raise ValueError(
                    "Dev update revision must be a full 40-character commit SHA"
                )
            return ReleaseInfo(
                version="dev",
                install_spec=(
                    f"git+https://github.com/{repo.strip().strip('/')}.git@{revision}"
                ),
                tag=revision,
                track=normalized,
                revision=revision.lower(),
            )
        release = get_channel(normalized, repo=repo).latest()
        if not release:
            raise RuntimeError("Could not resolve the dev channel ref")
        if version != release.version:
            raise ValueError(
                f"Dev channel resolved version {release.version}, not requested {version}"
            )
        return release
    if normalized == "pypi":
        return ReleaseInfo(
            version=version,
            install_spec=f"pa=={version}",
            track=normalized,
        )
    return ReleaseInfo(
        version=version,
        install_spec=f"git+https://github.com/{repo.strip().strip('/')}.git@v{version}",
        tag=f"v{version}",
        track=normalized,
    )


def _resolve_github_revision(repo: str, ref: str) -> str | None:
    url = (
        f"https://api.github.com/repos/{repo.strip().strip('/')}/commits/"
        f"{quote(ref, safe='')}"
    )
    try:
        response = httpx.get(url, timeout=15.0, headers=_github_headers())
        response.raise_for_status()
        revision = str(response.json().get("sha") or "").lower()
    except httpx.HTTPError, ValueError, AttributeError:
        return None
    return revision if re.fullmatch(r"[0-9a-f]{40}", revision) else None


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
    except httpx.HTTPError, json.JSONDecodeError, KeyError:
        return None
