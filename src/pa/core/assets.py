from __future__ import annotations

import hashlib
from dataclasses import dataclass
from pathlib import Path

from pa import __version__


@dataclass(frozen=True)
class AssetManifest:
    """Versioned URLs for static assets."""

    version: str
    root: Path

    def url(self, path: str) -> str:
        clean = path.lstrip("/")
        return f"/static/{clean}?v={self.version}"


def build_asset_manifest(static_root: Path) -> AssetManifest:
    version = compute_asset_version(static_root)
    return AssetManifest(version=version, root=static_root)


def compute_asset_version(static_root: Path) -> str:
    """Fingerprint static tree from file mtimes + app version."""
    if not static_root.exists():
        return __version__.replace(".", "")

    mtimes: list[float] = []
    for path in static_root.rglob("*"):
        if path.is_file():
            mtimes.append(path.stat().st_mtime)

    if not mtimes:
        return __version__.replace(".", "")

    payload = f"{__version__}:{max(mtimes)}:{len(mtimes)}"
    return hashlib.sha256(payload.encode()).hexdigest()[:12]
