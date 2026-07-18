"""Locate the uv executable for installs and self-updates."""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Iterable, Mapping
from pathlib import Path


def _uv_name() -> str:
    return "uv.exe" if os.name == "nt" else "uv"


def _running_install_candidates(executables: Iterable[str]) -> Iterable[Path]:
    """Infer uv's installer location from a standard uv tool environment."""
    uv_name = _uv_name()
    for executable in executables:
        if not executable:
            continue
        path = Path(executable).expanduser()
        parts = path.parts
        for index in range(len(parts) - 2):
            if parts[index : index + 3] == (".local", "share", "uv"):
                yield Path(*parts[: index + 1]) / "bin" / uv_name
                break


def _user_candidates(home: Path, environ: Mapping[str, str]) -> Iterable[Path]:
    uv_name = _uv_name()
    yield home / ".local" / "bin" / uv_name
    yield home / ".cargo" / "bin" / uv_name

    appdata = environ.get("APPDATA", "").strip()
    if appdata:
        yield Path(appdata) / "uv" / "bin" / "uv.exe"


def resolve_uv_binary() -> str:
    """Return an executable uv path without requiring an interactive-shell PATH."""
    configured = os.environ.get("PA_UV_BIN", "").strip()
    candidates: list[Path] = []
    if configured:
        candidates.append(Path(configured).expanduser())

    candidates.extend(
        _running_install_candidates((sys.executable, sys.argv[0] if sys.argv else ""))
    )
    candidates.extend(_user_candidates(Path.home(), os.environ))

    path_uv = shutil.which("uv")
    if path_uv:
        candidates.append(Path(path_uv))

    if os.name != "nt":
        candidates.extend(
            Path(path)
            for path in ("/opt/homebrew/bin/uv", "/usr/local/bin/uv", "/usr/bin/uv")
        )

    seen: set[Path] = set()
    for candidate in candidates:
        try:
            resolved = candidate.resolve()
        except OSError:
            continue
        if resolved in seen:
            continue
        seen.add(resolved)
        if resolved.is_file() and os.access(resolved, os.X_OK):
            return str(resolved)

    raise RuntimeError(
        "uv is required but was not found. Install uv from "
        "https://docs.astral.sh/uv/ or set PA_UV_BIN to its absolute path."
    )
