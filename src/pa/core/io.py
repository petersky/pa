"""Safe filesystem helpers."""

from __future__ import annotations

import json
import os
import secrets
import time
from pathlib import Path
from typing import Any

from pa.core.performance import current_observation


def _observed_fsync(fd: int, *, directory: bool = False) -> None:
    observation = current_observation.get()
    if observation is None:
        os.fsync(fd)
        return
    prefix = "directory" if directory else "file"
    calls = prefix + "_fsync_calls"
    setattr(observation, calls, getattr(observation, calls) + 1)
    started = time.perf_counter()
    try:
        os.fsync(fd)
    except OSError:
        observation.fsync_failures += 1
        raise
    finally:
        metric = prefix + "_fsync_ms"
        setattr(observation, metric, getattr(observation, metric) + (time.perf_counter() - started) * 1000)


def atomic_write_text(path: Path, content: str, *, mode: int | None = None) -> None:
    observation = current_observation.get()
    if observation is not None:
        observation.atomic_write_calls += 1
        # Intended logical UTF-8 bytes, including failed attempts. These are
        # not physical device writes or proof that publication committed.
        observation.atomic_write_bytes += len(content) if content.isascii() else len(content.encode("utf-8"))
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
            _observed_fsync(handle.fileno())
        started = time.perf_counter() if observation is not None else 0
        try:
            os.replace(tmp, path)
        finally:
            if observation is not None:
                observation.atomic_replace_ms += (time.perf_counter() - started) * 1000
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
        _observed_fsync(fd, directory=True)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write_json(
    path: Path, data: Any, *, indent: int = 2, mode: int | None = None
) -> None:
    text = json.dumps(data, indent=indent) + "\n"
    atomic_write_text(path, text, mode=mode)
