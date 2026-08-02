from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

from pa.acp.auxiliary_mcp import (
    AuxiliaryMcpCollection,
    AuxiliaryMcpServer,
    import_common_mcp_json,
    probe_auxiliary_server,
    resolve_auxiliary_mcp_servers,
)


def test_blender_common_json_import() -> None:
    collection = import_common_mcp_json(
        {"mcpServers": {"blender": {"command": "uvx", "args": ["blender-mcp"]}}}
    )
    assert collection.servers[0].model_dump()["args"] == ["blender-mcp"]


@pytest.mark.parametrize("name", ["pa", "pa-mcp", "PA_bridge", "bad name", "1bad"])
def test_reserved_and_malformed_names_are_rejected(name: str) -> None:
    with pytest.raises(ValidationError):
        AuxiliaryMcpServer(name=name, command="tool")


def test_duplicate_names_are_rejected() -> None:
    with pytest.raises(ValidationError, match="duplicate"):
        AuxiliaryMcpCollection(
            servers=[
                AuxiliaryMcpServer(name="one", command="tool"),
                AuxiliaryMcpServer(name="one", command="other"),
            ]
        )


def test_malformed_common_json_is_rejected() -> None:
    with pytest.raises(TypeError, match="mcpServers"):
        import_common_mcp_json({"servers": {}})


def test_optional_command_not_found_is_visible_but_omitted() -> None:
    servers, provenance = resolve_auxiliary_mcp_servers(
        [AuxiliaryMcpServer(name="missing", command="definitely-not-a-command")]
    )
    assert servers == []
    assert provenance[0]["error"] == "command_not_found"


def test_required_command_not_found_rejects_session() -> None:
    with pytest.raises(RuntimeError, match="required auxiliary MCP"):
        resolve_auxiliary_mcp_servers(
            [
                AuxiliaryMcpServer(
                    name="missing", command="definitely-not-a-command", required=True
                )
            ]
        )


def test_secret_reference_is_resolved_but_never_in_provenance() -> None:
    secret = "should-never-appear-in-metadata"
    servers, provenance = resolve_auxiliary_mcp_servers(
        [
            AuxiliaryMcpServer(
                name="secure",
                command=sys.executable,
                env={"API_TOKEN": "PA_TEST_AUX_SECRET"},
            )
        ],
        environment={"PA_TEST_AUX_SECRET": secret},
    )
    assert servers[0].env[0].value == secret
    assert secret not in repr(provenance)
    assert provenance[0]["env_references"] == ["PA_TEST_AUX_SECRET"]


def test_per_instance_and_provider_applicability() -> None:
    definition = AuxiliaryMcpServer(
        name="blender",
        command=sys.executable,
        applicability={"providers": ["codex"]},
    )
    assert resolve_auxiliary_mcp_servers([definition], provider="cursor") == ([], [])
    servers, _ = resolve_auxiliary_mcp_servers([definition], provider="codex")
    assert len(servers) == 1


def test_missing_working_directory_is_unavailable(tmp_path: Path) -> None:
    servers, provenance = resolve_auxiliary_mcp_servers(
        [
            AuxiliaryMcpServer(
                name="cwd", command=sys.executable, cwd=str(tmp_path / "missing")
            )
        ]
    )
    assert servers == []
    assert provenance[0]["error"] == "working_directory_unavailable"


@pytest.mark.asyncio
async def test_probe_startup_timeout_is_bounded() -> None:
    result = await probe_auxiliary_server(
        AuxiliaryMcpServer(
            name="slow",
            command=sys.executable,
            args=["-c", "import time; time.sleep(5)"],
            startup_timeout_seconds=0.05,
        )
    )
    assert result["state"] == "unavailable"
    assert result["error"] == "startup_timeout"
