from __future__ import annotations

import os
import shutil
import stat
from pathlib import Path
from typing import Protocol


class BackupBackend(Protocol):
    """Storage boundary kept stable for future remote/object-store backends."""

    def health(self) -> dict: ...
    def publish(self, temporary: Path, final_name: str) -> Path: ...
    def list_archives(self) -> list[Path]: ...
    def delete(self, path: Path) -> None: ...


class LocalFilesystemBackend:
    def __init__(self, destination: Path) -> None:
        self.destination = destination

    def health(self) -> dict:
        exists = self.destination.is_dir()
        result = {
            "backend": "local",
            "path": str(self.destination),
            "exists": exists,
            "writable": False,
            "free_bytes": None,
            "total_bytes": None,
            "error": None,
        }
        if not exists:
            result["error"] = "destination directory does not exist"
            return result
        try:
            metadata = self.destination.stat()
            mode = stat.S_IMODE(metadata.st_mode)
            if metadata.st_uid != os.geteuid():
                result["error"] = "destination must be owned by the PA service account"
                return result
            if mode & 0o077:
                result["error"] = (
                    "destination permissions expose metadata to other local users"
                )
                return result
            if not mode & stat.S_IWUSR:
                result["error"] = "destination is not writable by its owner"
                return result
            probe = self.destination / f".pa-backup-health-{os.getpid()}"
            fd = os.open(probe, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            os.close(fd)
            probe.unlink()
            usage = shutil.disk_usage(self.destination)
            result.update(
                writable=True,
                free_bytes=usage.free,
                total_bytes=usage.total,
            )
        except OSError as exc:
            result["error"] = f"{type(exc).__name__}: {exc}"
        return result

    def publish(self, temporary: Path, final_name: str) -> Path:
        target = self.destination / final_name
        if target.exists():
            raise FileExistsError(f"backup target already exists: {target.name}")
        os.chmod(temporary, 0o600)
        os.replace(temporary, target)
        fd = os.open(self.destination, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return target

    def list_archives(self) -> list[Path]:
        if not self.destination.is_dir():
            return []
        return sorted(
            [
                *self.destination.glob("*.pa-backup.tgz"),
                *self.destination.glob("*.pa-backup.tar"),
            ]
        )

    def delete(self, path: Path) -> None:
        path.unlink()
