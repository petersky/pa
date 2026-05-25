"""Update channels for pa update."""

from __future__ import annotations

import re
import subprocess
from abc import ABC, abstractmethod
from dataclasses import dataclass

import httpx

from pa import __version__


@dataclass(frozen=True)
class ReleaseInfo:
    version: str
    install_spec: str
    url: str | None = None


class UpdateChannel(ABC):
    @abstractmethod
    def latest(self) -> ReleaseInfo | None: ...

    @abstractmethod
    def install(self, release: ReleaseInfo) -> None: ...


def _parse_version(version: str) -> tuple[int, ...]:
    parts = re.findall(r"\d+", version)
    return tuple(int(p) for p in parts) if parts else (0,)


def is_newer(current: str, latest: str) -> bool:
    return _parse_version(latest) > _parse_version(current)


class GitHubReleaseChannel(UpdateChannel):
    def __init__(self, repo: str) -> None:
        self.repo = repo.strip().strip("/")

    def latest(self) -> ReleaseInfo | None:
        url = f"https://api.github.com/repos/{self.repo}/releases/latest"
        try:
            resp = httpx.get(url, timeout=15.0, headers={"Accept": "application/vnd.github+json"})
            if resp.status_code == 404:
                return self._latest_tag_fallback()
            resp.raise_for_status()
            data = resp.json()
        except httpx.HTTPError:
            return self._latest_tag_fallback()

        tag = data.get("tag_name", "").lstrip("v")
        if not tag:
            return None

        wheel_url = None
        for asset in data.get("assets", []):
            name = asset.get("name", "")
            if name.endswith(".whl"):
                wheel_url = asset.get("browser_download_url")
                break

        if wheel_url:
            return ReleaseInfo(version=tag, install_spec=wheel_url, url=wheel_url)

        return ReleaseInfo(
            version=tag,
            install_spec=f"git+https://github.com/{self.repo}.git@{data.get('tag_name', tag)}",
            url=data.get("html_url"),
        )

    def _latest_tag_fallback(self) -> ReleaseInfo | None:
        url = f"https://api.github.com/repos/{self.repo}/tags"
        try:
            resp = httpx.get(url, timeout=15.0, headers={"Accept": "application/vnd.github+json"})
            resp.raise_for_status()
            tags = resp.json()
        except httpx.HTTPError:
            return None
        if not tags:
            return None
        tag_name = tags[0].get("name", "")
        version = tag_name.lstrip("v")
        return ReleaseInfo(
            version=version,
            install_spec=f"git+https://github.com/{self.repo}.git@{tag_name}",
            url=f"https://github.com/{self.repo}/releases/tag/{tag_name}",
        )

    def install(self, release: ReleaseInfo) -> None:
        _uv_tool_install(release.install_spec)


class PyPIChannel(UpdateChannel):
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
        )

    def install(self, release: ReleaseInfo) -> None:
        _uv_tool_install(release.install_spec)


def _uv_tool_install(spec: str) -> None:
    result = subprocess.run(
        ["uv", "tool", "install", "--force", spec],
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"uv tool install failed for {spec}")


def get_channel(name: str, *, repo: str = "petersky/pa") -> UpdateChannel:
    if name == "pypi":
        return PyPIChannel()
    return GitHubReleaseChannel(repo)
