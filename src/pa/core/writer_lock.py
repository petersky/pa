"""Exclusive ownership of a PA data directory by one server process."""

from __future__ import annotations

import fcntl
import json
import os
import socket
from pathlib import Path


class DataDirAlreadyOwnedError(RuntimeError):
    pass


class DataDirWriterLock:
    """Hold an advisory process lock for the lifetime of the PA server.

    PA is distributed across instances, but a data directory is a single local
    repository. Each instance gets its own directory and its own writer.
    """

    def __init__(self, data_dir: Path) -> None:
        self.path = data_dir / "server-writer.lock"
        self._fd: int | None = None

    def acquire(self) -> None:
        if self._fd is not None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            owner = os.read(fd, 4096).decode(errors="replace").strip()
            os.close(fd)
            detail = f" ({owner})" if owner else ""
            raise DataDirAlreadyOwnedError(
                f"PA data directory {self.path.parent} already has a running writer{detail}. "
                "Run one PA server per PA_DATA_DIR and send mutations through its API/MCP tools."
            ) from exc
        os.ftruncate(fd, 0)
        os.write(
            fd,
            json.dumps(
                {"pid": os.getpid(), "host": socket.gethostname()},
                sort_keys=True,
            ).encode(),
        )
        os.fsync(fd)
        self._fd = fd

    def release(self) -> None:
        if self._fd is None:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = None

    @property
    def held(self) -> bool:
        return self._fd is not None
