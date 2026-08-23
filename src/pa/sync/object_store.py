"""Content-addressed object store."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pa.sync.object_catalog import ObjectCatalog


def object_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ObjectStore:
    def __init__(
        self, base_dir: Path, *, catalog: ObjectCatalog | None = None
    ) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.catalog = catalog

    def attach_catalog(self, catalog: ObjectCatalog) -> None:
        self.catalog = catalog

    def _path_for(self, obj_hash: str) -> Path:
        return self.base_dir / obj_hash[:2] / obj_hash[2:]

    def _record_catalog(self, h: str, data: bytes) -> None:
        if self.catalog is None:
            return
        try:
            mtime_ns = self._path_for(h).stat().st_mtime_ns
        except OSError:
            mtime_ns = time.time_ns()
        self.catalog.record(h, data, mtime_ns=mtime_ns)

    def put(self, data: bytes) -> str:
        h = object_hash(data)
        path = self._path_for(h)
        path.parent.mkdir(parents=True, exist_ok=True)
        created = not path.exists()
        if created:
            path.write_bytes(data)
        # Always upsert catalog metadata; avoid an extra catalog.has() round-trip.
        self._record_catalog(h, data)
        return h

    def put_json(self, obj: Any) -> str:
        return self.put(json.dumps(obj, default=str).encode())

    def repair(self, expected_hash: str, data: bytes) -> str:
        """Atomically install verified content, including over a corrupt object."""
        actual = object_hash(data)
        if actual != expected_hash:
            raise ValueError("object content hash does not match requested hash")
        path = self._path_for(expected_hash)
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.")
        try:
            with os.fdopen(fd, "wb") as handle:
                handle.write(data)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
        finally:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
        self._record_catalog(expected_hash, data)
        return actual

    def get(self, obj_hash: str) -> bytes | None:
        path = self._path_for(obj_hash)
        if not path.exists():
            return None
        return path.read_bytes()

    def get_json(self, obj_hash: str) -> dict | None:
        raw = self.get(obj_hash)
        if raw is None:
            return None
        return json.loads(raw.decode())

    def has(self, obj_hash: str) -> bool:
        return self._path_for(obj_hash).exists()

    def list_hashes(self) -> list[str]:
        """Full filesystem scan — maintenance/diagnostics only, never hot status."""
        hashes: list[str] = []
        try:
            entries = os.scandir(self.base_dir)
        except FileNotFoundError:
            return hashes
        with entries:
            for sub in entries:
                if not sub.is_dir(follow_symlinks=False) or len(sub.name) != 2:
                    continue
                with os.scandir(sub.path) as files:
                    for item in files:
                        if item.is_file(follow_symlinks=False):
                            hashes.append(sub.name + item.name)
        return hashes

    def indexed_count(self) -> int:
        if self.catalog is not None:
            return self.catalog.count()
        return len(self.list_hashes())

    def get_many(self, hashes: list[str]) -> dict[str, bytes]:
        result: dict[str, bytes] = {}
        for h in hashes:
            data = self.get(h)
            if data is not None:
                result[h] = data
        return result
