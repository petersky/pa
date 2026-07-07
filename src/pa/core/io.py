"""Safe filesystem helpers."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content)
    os.replace(tmp, path)


def atomic_write_json(path: Path, data: Any, *, indent: int = 2) -> None:
    text = json.dumps(data, indent=indent) + "\n"
    atomic_write_text(path, text)
