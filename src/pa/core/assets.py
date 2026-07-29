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
    """Fingerprint static tree contents and paths with the app version."""
    if not static_root.exists():
        return __version__.replace(".", "")

    files = sorted(
        (path for path in static_root.rglob("*") if path.is_file()),
        key=lambda path: path.relative_to(static_root).as_posix(),
    )

    if not files:
        return __version__.replace(".", "")

    digest = hashlib.sha256()
    digest.update(__version__.encode())
    digest.update(b"\0")
    for path in files:
        relative = path.relative_to(static_root).as_posix().encode()
        digest.update(len(relative).to_bytes(8, "big"))
        digest.update(relative)
        with path.open("rb") as handle:
            while chunk := handle.read(1024 * 1024):
                digest.update(len(chunk).to_bytes(8, "big"))
                digest.update(chunk)
        digest.update((0).to_bytes(8, "big"))
    return digest.hexdigest()[:12]
