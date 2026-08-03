from __future__ import annotations

import tomllib
from pathlib import Path


def test_published_metadata_requires_supported_mcp_2() -> None:
    root = Path(__file__).parents[1]
    metadata = tomllib.loads((root / "pyproject.toml").read_text())
    requirement = next(
        item
        for item in metadata["project"]["dependencies"]
        if item.startswith("mcp")
    )
    assert requirement == "mcp>=2.0.0,<3"
    assert 'name = "mcp", specifier = ">=2.0.0,<3"' in (
        root / "uv.lock"
    ).read_text()
