from __future__ import annotations

import tomllib
from pathlib import Path


def test_published_metadata_excludes_incompatible_mcp_2() -> None:
    root = Path(__file__).parents[1]
    metadata = tomllib.loads((root / "pyproject.toml").read_text())
    requirement = next(
        item
        for item in metadata["project"]["dependencies"]
        if item.startswith("mcp")
    )
    assert requirement == "mcp>=1.9.0,<2"
    assert 'name = "mcp", specifier = ">=1.9.0,<2"' in (
        root / "uv.lock"
    ).read_text()
