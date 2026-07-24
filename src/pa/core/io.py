"""Safe filesystem helpers."""

from __future__ import annotations

import json
import os
import secrets
from pathlib import Path
from typing import Any


def atomic_write_text(path: Path, content: str, *, mode: int | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    while True:
        tmp = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
        try:
            fd = os.open(tmp, flags, mode if mode is not None else 0o666)
        except FileExistsError:
            continue
        break
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        _fsync_directory(path.parent)
    except BaseException:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass
        raise


def _fsync_directory(path: Path) -> None:
    """Persist a directory entry update where the platform supports it."""
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(
    path: Path, data: Any, *, indent: int = 2, mode: int | None = None
) -> None:
    text = json.dumps(data, indent=indent) + "\n"
    atomic_write_text(path, text, mode=mode)
