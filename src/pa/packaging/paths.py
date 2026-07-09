"""Executable resolution for host services with minimal PATH."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def service_path_dirs() -> list[Path]:
    home = Path.home()
    candidates = [
        home / ".local" / "bin",
        Path("/opt/homebrew/bin"),
        Path("/usr/local/bin"),
    ]
    return [path for path in candidates if path.is_dir()]


def build_service_path() -> str:
    extra = [str(path) for path in service_path_dirs()]
    inherited = os.environ.get("PATH", "/usr/bin:/bin:/usr/sbin:/sbin")
    parts = extra + [part for part in inherited.split(os.pathsep) if part]
    seen: set[str] = set()
    ordered: list[str] = []
    for part in parts:
        if part not in seen:
            seen.add(part)
            ordered.append(part)
    return os.pathsep.join(ordered)


def resolve_executable(name: str, *, path: str | None = None) -> Path | None:
    if os.path.sep in name or (os.path.altsep and os.path.altsep in name):
        candidate = Path(name)
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
        return None
    found = shutil.which(name, path=path or build_service_path())
    return Path(found) if found else None
