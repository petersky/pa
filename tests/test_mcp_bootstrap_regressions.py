from __future__ import annotations

import asyncio
import json
import tempfile
from contextlib import AbstractAsyncContextManager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from pa import __version__
from pa.acp.mcp_config import (
    McpHandshakeError,
    _ensure_supported_mcp_sdk,
    _probe_pa_mcp_stdio_async,
)
from pa.cli.main import app
from pa.config import Settings
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import (
    MAX_PROGRESS_PAYLOAD_BYTES,
    MAX_VALIDATION_COMMAND,
    DispatchProgressEventV1,
    ProgressValidationV1,
)
from pa.fleet.placement import (
    PlacementCandidate,
    PlacementError,
    PlacementRequest,
    PlacementService,
    RoundRobinCursorStore,
)
from pa.fleet.overview import (
    MCP_BOOTSTRAP_TIMEOUT,
    MCP_STDIO_HANDSHAKE_TIMEOUT,
    probe_dimension,
)
from pa.domain.models import FleetInstance


def _event_payload(*, command: str = "pytest") -> dict:
    return {
        "dispatch_id": "dispatch-1",
        "acp_session_id": "session-1",
        "originating_instance_id": "target-1",
        "authority_instance_id": "authority-1",
        "sequence": 1,
        "idempotency_key": "event-1",
        "phase": "testing",
        "summary": "running validation",
        "validations": [{"command": command, "status": "running"}],
    }


def test_validation_command_boundaries_are_sanitized_at_ingress() -> None:
    for size in (MAX_VALIDATION_COMMAND - 1, MAX_VALIDATION_COMMAND):
        validation = ProgressValidationV1(command="x" * size, status="running")
        assert len(validation.command) == size
    validation = ProgressValidationV1(
        command="x" * (MAX_VALIDATION_COMMAND + 1), status="running"
    )
    assert len(validation.command) == MAX_VALIDATION_COMMAND


def _payload_sized_event(size: int) -> DispatchProgressEventV1:
    payload = _event_payload()
    payload["operator_input"] = ""
    base = DispatchProgressEventV1.model_validate(payload)
    overhead = len(base.model_dump_json().encode())
    payload["operator_input"] = "x" * (size - overhead)
    return DispatchProgressEventV1.model_validate(payload)


def test_progress_event_payload_boundaries_are_64_kib() -> None:
    below = _payload_sized_event(MAX_PROGRESS_PAYLOAD_BYTES - 1)
    exact = _payload_sized_event(MAX_PROGRESS_PAYLOAD_BYTES)
    assert len(below.model_dump_json().encode()) == MAX_PROGRESS_PAYLOAD_BYTES - 1
    assert len(exact.model_dump_json().encode()) == MAX_PROGRESS_PAYLOAD_BYTES
    with pytest.raises(ValueError, match="64 KB"):
        _payload_sized_event(MAX_PROGRESS_PAYLOAD_BYTES + 1)


def test_legacy_oversized_progress_is_migrated_without_losing_dispatch() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        record = DispatchRecord(
            dispatch_id="dispatch-1",
            mutation_id="mutation-1",
            authority_instance_id="authority-1",
            authority_url="https://authority.example",
            target_instance_id="target-1",
        ).model_dump(mode="json")
        record["progress_events"] = [
            _event_payload(command="git " + "x" * 4_000),
            {"invalid": "historical record"},
        ]
        (root / "dispatch_mutations.json").write_text(
            json.dumps({"dispatch-1": record})
        )

        loaded = DispatchStore(root).get("dispatch-1")
        assert loaded is not None
        assert len(loaded.progress_events) == 1
        assert len(loaded.progress_events[0].validations[0].command) == 2_000


class _FailingStdio(AbstractAsyncContextManager):
    def __init__(self, errlog) -> None:
        self.errlog = errlog

    async def __aenter__(self):
        self.errlog.write(
            "ModuleNotFoundError: No module named 'mcp.server.fastmcp'\n"
        )
        self.errlog.flush()
        raise ExceptionGroup(
            "outer task group",
            [ExceptionGroup("inner task group", [RuntimeError("Connection closed")])],
        )

    async def __aexit__(self, *_args):
        return False


def test_nested_taskgroup_retains_child_stderr_and_bootstrap_context(
    tmp_path: Path,
) -> None:
    settings = Settings(data_dir=tmp_path, agent_enabled=False)

    def failing_stdio(_params, *, errlog):
        return _FailingStdio(errlog)

    with (
        patch("pa.acp.mcp_config.stdio_client", side_effect=failing_stdio),
        patch("pa.acp.mcp_config._ensure_supported_mcp_sdk"),
        pytest.raises(McpHandshakeError) as raised,
    ):
        asyncio.run(
            _probe_pa_mcp_stdio_async(
                settings,
                timeout=1,
                owner_environment={"PA_OWNER_SOCKET": str(tmp_path / "owner.sock")},
                session_environment={},
            )
        )
    error = raised.value
    assert "mcp.server.fastmcp" in error.detail
    assert "unhandled errors in a TaskGroup" not in error.detail
    assert "RuntimeError: Connection closed" in (error.root_exception or "")
    assert error.context["owner_endpoint_source"] == "PA_OWNER_SOCKET"
    assert error.context["pa_executable"]
    assert error.context["cwd"]
    assert error.context["process_exit_code"] is None


@pytest.mark.parametrize("version", ["1.27.1", "3.0.0"])
def test_unsupported_mcp_major_is_classified_before_child_spawn(
    version: str,
) -> None:
    with pytest.raises(McpHandshakeError) as raised:
        _ensure_supported_mcp_sdk({"mcp_sdk_version": version})
    assert raised.value.classification == "dependency_incompatible"
    assert raised.value.phase == "dependency_preflight"
    assert version in raised.value.detail


def test_mcp_2_passes_dependency_preflight() -> None:
    _ensure_supported_mcp_sdk({"mcp_sdk_version": "2.0.0"})


def test_fleet_bootstrap_budget_exceeds_healthy_cold_handshake() -> None:
    assert MCP_STDIO_HANDSHAKE_TIMEOUT > 4.0
    assert MCP_BOOTSTRAP_TIMEOUT > MCP_STDIO_HANDSHAKE_TIMEOUT


def test_forced_bootstrap_refreshes_coalesce(tmp_path: Path) -> None:
    calls = 0

    async def slow_probe(_ctx, _instance, _dimension):
        nonlocal calls
        calls += 1
        await asyncio.sleep(0.02)
        return {
            "state": "fresh",
            "value": {"state": "connected"},
            "observed_at": "2026-01-01T00:00:00+00:00",
        }

    ctx = SimpleNamespace(
        settings=SimpleNamespace(data_dir=tmp_path),
        services={},
    )
    instance = FleetInstance(
        instance_id="target",
        name="target",
        url="http://target.test",
    )
    async def exercise():
        with patch("pa.fleet.overview._probe", side_effect=slow_probe):
            return await asyncio.gather(
                probe_dimension(ctx, instance, "mcp_bootstrap", force=True),
                probe_dimension(ctx, instance, "mcp_bootstrap", force=True),
            )

    first, second = asyncio.run(exercise())
    assert calls == 1
    assert first["state"] == second["state"] == "fresh"


def _fresh(value):
    return {"state": "fresh", "value": value}


def test_placement_rejects_unavailable_bootstrap_before_admission(
    tmp_path: Path,
) -> None:
    candidate = PlacementCandidate(
        instance_id="macmini",
        name="macmini",
        reachability=_fresh({"health": "up"}),
        activity=_fresh({"state": "idle"}),
        providers=_fresh(
            [{"id": "codex", "available": True, "auth_state": "authenticated"}]
        ),
        mcp_bootstrap=_fresh(
            {"state": "unavailable", "classification": "dependency_incompatible"}
        ),
    )
    service = PlacementService(RoundRobinCursorStore(tmp_path))
    with pytest.raises(PlacementError) as raised:
        service.resolve(
            PlacementRequest(
                realm_id="default",
                fleet_id="fleet",
                instance_id="macmini",
                provider="codex",
            ),
            [candidate],
        )
    assert raised.value.code == "mcp_bootstrap_unavailable"


def test_version_command_and_option_are_supported() -> None:
    runner = CliRunner()
    command = runner.invoke(app, ["version"])
    option = runner.invoke(app, ["--version"])
    assert command.exit_code == option.exit_code == 0
    assert f"pa {__version__}" in command.stdout
    assert f"pa {__version__}" in option.stdout


def test_ui_keeps_validation_commands_compact_and_expandable() -> None:
    script = (
        Path(__file__).parents[1] / "src/pa/server/static/js/fleet.js"
    ).read_text()
    assert "compactDetail(command, 240)" in script
    assert '"<li><details><summary><code>"' in script
