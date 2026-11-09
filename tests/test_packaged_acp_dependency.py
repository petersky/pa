from __future__ import annotations

import tomllib
from pathlib import Path

from packaging.requirements import Requirement


def test_published_acp_requirement_excludes_incompatible_sdk() -> None:
    root = Path(__file__).parents[1]
    metadata = tomllib.loads((root / "pyproject.toml").read_text())
    requirement = next(
        Requirement(item)
        for item in metadata["project"]["dependencies"]
        if Requirement(item).name == "agent-client-protocol"
    )
    assert "0.10.1" in requirement.specifier
    assert "1.0.0" not in requirement.specifier
    lock = tomllib.loads((root / "uv.lock").read_text())
    sdk = next(p for p in lock["package"] if p["name"] == requirement.name)
    assert sdk["version"] in requirement.specifier
    pa = next(p for p in lock["package"] if p["name"] == "pa")
    declared = next(
        d for d in pa["metadata"]["requires-dist"] if d["name"] == requirement.name
    )
    assert declared["specifier"] == str(requirement.specifier)


def test_supported_sdk_imports_client_and_content_helpers() -> None:
    from acp import image_block, text_block
    from pa.acp.client import PAClient

    assert PAClient is not None
    assert text_block("hello").text == "hello"
    block = image_block("aGVsbG8=", "image/png")
    assert block.data == "aGVsbG8="
    assert block.mime_type == "image/png"
