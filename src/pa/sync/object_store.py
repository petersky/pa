"""Content-addressed object store."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def object_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class ObjectStore:
    def __init__(self, base_dir: Path) -> None:
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _path_for(self, obj_hash: str) -> Path:
        return self.base_dir / obj_hash[:2] / obj_hash[2:]

    def put(self, data: bytes) -> str:
        h = object_hash(data)
        path = self._path_for(h)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            path.write_bytes(data)
        return h

    def put_json(self, obj: Any) -> str:
        return self.put(json.dumps(obj, default=str).encode())

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
        hashes: list[str] = []
        for sub in self.base_dir.iterdir():
            if not sub.is_dir() or len(sub.name) != 2:
                continue
            for f in sub.iterdir():
                if f.is_file():
                    hashes.append(sub.name + f.name)
        return hashes

    def get_many(self, hashes: list[str]) -> dict[str, bytes]:
        result: dict[str, bytes] = {}
        for h in hashes:
            data = self.get(h)
            if data is not None:
                result[h] = data
        return result
