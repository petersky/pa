from __future__ import annotations

import asyncio
import json
import tempfile
import threading
import time
from contextlib import AbstractAsyncContextManager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from mcp.server.mcpserver import MCPServer
from typer.testing import CliRunner

from pa import __version__
from pa.acp.mcp_config import (
    McpHandshakeError,
    _ensure_supported_mcp_sdk,
    _probe_pa_mcp_stdio_async,
)
from pa.cli.main import app
from pa.config import Settings
from pa.core.mcp_registration import (
    DuplicateMcpToolError,
    UniqueToolRegistrationProxy,
)
from pa.core.kernel import Kernel
from pa.domain.models import FleetInstance
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import (
    MAX_PROGRESS_PAYLOAD_BYTES,
    MAX_VALIDATION_COMMAND,
    DispatchProgressEventV1,
    ProgressValidationV1,
)
from pa.fleet import overview as fleet_overview
from pa.fleet.overview import (
    MCP_BOOTSTRAP_TIMEOUT,
    MCP_BOOTSTRAP_WARM_CACHE_SLO,
    MCP_STDIO_HANDSHAKE_TIMEOUT,
    probe_dimension,
)
from pa.fleet.placement import (
    PlacementCandidate,
    PlacementError,
    PlacementRequest,
    PlacementService,
    RoundRobinCursorStore,
)
from pa.modules.items import ItemsModule


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
        self.errlog.write("ModuleNotFoundError: No module named 'mcp.server.fastmcp'\n")
        self.errlog.flush()
        raise ExceptionGroup(
            "outer task group",
            [ExceptionGroup("inner task group", [RuntimeError("Connection closed")])],
        )

    async def __aexit__(self, *_args):
        return False


class _SlowShutdownStdio(AbstractAsyncContextManager):
    async def __aenter__(self):
        return object(), object()

    async def __aexit__(self, *_args):
        await asyncio.sleep(0.05)
        return False


class _HealthySession(AbstractAsyncContextManager):
    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def initialize(self):
        return None

    async def list_tools(self):
        return SimpleNamespace(tools=[object()])


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


def test_successful_handshake_is_not_failed_by_slow_child_teardown(
    tmp_path: Path,
) -> None:
    settings = Settings(data_dir=tmp_path, agent_enabled=False)

    with (
        patch(
            "pa.acp.mcp_config.stdio_client",
            side_effect=lambda *_args, **_kwargs: _SlowShutdownStdio(),
        ),
        patch(
            "pa.acp.mcp_config.ClientSession",
            side_effect=lambda *_args, **_kwargs: _HealthySession(),
        ),
        patch("pa.acp.mcp_config._ensure_supported_mcp_sdk"),
    ):
        result = asyncio.run(
            _probe_pa_mcp_stdio_async(
                settings,
                timeout=0.01,
                owner_environment={"PA_OWNER_SOCKET": str(tmp_path / "owner.sock")},
                session_environment={},
            )
        )

    assert result == {"state": "connected", "classification": "ok", "tool_count": 1}


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


class _RecordingMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}
        self.registration_count: dict[str, int] = {}
        self.lock = threading.Lock()

    def tool(self, name: str | None = None, **_kwargs):
        def register(fn):
            resolved = name or fn.__name__
            with self.lock:
                self.functions[resolved] = fn
                self.registration_count[resolved] = (
                    self.registration_count.get(resolved, 0) + 1
                )
            return fn

        return register


def test_items_module_registers_each_tool_once_and_rejects_reload() -> None:
    delegate = _RecordingMcp()
    guarded = UniqueToolRegistrationProxy(delegate)
    ctx = SimpleNamespace(settings=SimpleNamespace())

    ItemsModule().register_mcp(guarded, ctx)

    assert delegate.registration_count["create_card"] == 1
    assert delegate.registration_count["update_card"] == 1
    assert all(count == 1 for count in delegate.registration_count.values())
    with pytest.raises(
        DuplicateMcpToolError,
        match="duplicate MCP tool registration for 'list_items'",
    ):
        ItemsModule().register_mcp(guarded, ctx)
    assert all(count == 1 for count in delegate.registration_count.values())


def test_full_kernel_tool_discovery_is_unique_and_repeat_registration_fails(
    tmp_path: Path,
) -> None:
    kernel = Kernel.boot(settings=Settings(data_dir=tmp_path, agent_enabled=False))
    server = MCPServer("registration-contract")

    kernel.register_mcp(server)
    tools = asyncio.run(server.list_tools())
    names = [tool.name for tool in tools]

    assert len(names) == len(set(names))
    assert names.count("create_card") == names.count("update_card") == 1
    with pytest.raises(
        DuplicateMcpToolError, match="duplicate MCP tool registration"
    ):
        kernel.register_mcp(server)


def test_concurrent_duplicate_registration_has_one_deterministic_winner() -> None:
    delegate = _RecordingMcp()
    guarded = UniqueToolRegistrationProxy(delegate)
    start = threading.Barrier(3)
    errors: list[BaseException] = []

    def register(origin: str) -> None:
        def handler() -> str:
            return origin

        handler.__name__ = f"handler_{origin}"
        start.wait()
        try:
            guarded.tool(name="same_name")(handler)
        except BaseException as exc:
            errors.append(exc)

    threads = [
        threading.Thread(target=register, args=(origin,)) for origin in ("a", "b")
    ]
    for thread in threads:
        thread.start()
    start.wait()
    for thread in threads:
        thread.join()

    assert delegate.registration_count == {"same_name": 1}
    assert len(errors) == 1
    assert isinstance(errors[0], DuplicateMcpToolError)


def test_cancelled_bootstrap_waiter_observes_failure_and_allows_retry(
    tmp_path: Path, caplog
) -> None:
    ctx = SimpleNamespace(
        settings=SimpleNamespace(data_dir=tmp_path),
        services={},
    )
    instance = FleetInstance(
        instance_id="target-cancelled",
        name="target-cancelled",
        url="http://target.test",
    )

    async def exercise() -> dict:
        started = asyncio.Event()
        release = asyncio.Event()

        async def late_failure(_ctx, _instance, _dimension):
            started.set()
            await release.wait()
            raise AssertionError("late bootstrap failure")

        with patch("pa.fleet.overview._probe", side_effect=late_failure):
            waiter = asyncio.create_task(
                probe_dimension(ctx, instance, "mcp_bootstrap", force=True)
            )
            await started.wait()
            waiter.cancel()
            with pytest.raises(asyncio.CancelledError):
                await waiter
            release.set()
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        async def successful_retry(_ctx, _instance, _dimension):
            return {
                "state": "fresh",
                "value": {"state": "connected"},
                "observed_at": datetime.now(UTC).isoformat(),
            }

        with patch("pa.fleet.overview._probe", side_effect=successful_retry):
            return await probe_dimension(ctx, instance, "mcp_bootstrap", force=True)

    result = asyncio.run(exercise())
    assert result["value"]["state"] == "connected"
    assert "probe task failed" in caplog.text
    key = (str(tmp_path), instance.instance_id, "mcp_bootstrap")
    assert key not in fleet_overview._probe_tasks


def test_warm_bootstrap_is_cache_only_and_within_slo(tmp_path: Path) -> None:
    ctx = SimpleNamespace(
        settings=SimpleNamespace(data_dir=tmp_path),
        services={},
    )
    instance = FleetInstance(
        instance_id="target-warm",
        name="target-warm",
        url="http://target.test",
    )

    async def successful_probe(_ctx, _instance, _dimension):
        return {
            "state": "fresh",
            "value": {"state": "connected"},
            "observed_at": datetime.now(UTC).isoformat(),
        }

    async def exercise() -> tuple[dict, float]:
        with patch("pa.fleet.overview._probe", side_effect=successful_probe):
            await probe_dimension(ctx, instance, "mcp_bootstrap", force=True)
        started_at = time.perf_counter()
        with patch(
            "pa.fleet.overview._probe",
            side_effect=AssertionError("warm cache must not spawn a probe"),
        ):
            cached = await probe_dimension(ctx, instance, "mcp_bootstrap")
        return cached, time.perf_counter() - started_at

    cached, elapsed = asyncio.run(exercise())
    assert cached["cache_hit"] is True
    assert elapsed < MCP_BOOTSTRAP_WARM_CACHE_SLO


def test_failed_handshake_is_cached_as_fresh_unavailable_state(
    tmp_path: Path,
) -> None:
    settings = Settings(
        data_dir=tmp_path,
        instance_id="local-bootstrap",
        agent_enabled=False,
    )
    ctx = SimpleNamespace(settings=settings, services={})
    instance = FleetInstance(
        instance_id=settings.instance_id,
        name="local-bootstrap",
        url="http://127.0.0.1:8090",
    )
    failure = McpHandshakeError(
        "initialize_failed",
        "repair the duplicate registration",
        "duplicate MCP tool registration for 'create_card'",
        phase="initialize",
    )

    with patch("pa.acp.mcp_config.probe_pa_mcp_stdio", side_effect=failure):
        result = asyncio.run(
            probe_dimension(ctx, instance, "mcp_bootstrap", force=True)
        )

    assert result["state"] == "fresh"
    assert result["value"]["state"] == "unavailable"
    assert result["value"]["classification"] == "initialize_failed"
    assert result["value"]["phase"] == "initialize"


def test_card_tools_round_trip_the_union_of_supported_fields() -> None:
    delegate = _RecordingMcp()
    ctx = SimpleNamespace(settings=SimpleNamespace())
    with patch("pa.mcp.local_api.request_local_pa", return_value={}) as request:
        ItemsModule().register_mcp(delegate, ctx)
        delegate.functions["create_card"](
            title="Child",
            idempotency_key="create-card-round-trip",
            body="Body",
            lane="active",
            realm="team",
            parent_id="parent",
            project_id="project",
            tags=["one"],
            auto_enrich=False,
        )
        create = request.call_args
        assert create.args[1:3] == ("POST", "/api/cards")
        assert create.kwargs["json"] == {
            "realm_id": "team",
            "title": "Child",
            "body": "Body",
            "lane": "active",
            "parent_id": "parent",
            "project_id": "project",
            "tags": ["one"],
            "auto_enrich": False,
        }
        assert create.kwargs["headers"] == {
            "Idempotency-Key": "create-card-round-trip"
        }

        delegate.functions["update_card"](
            card_id="card",
            idempotency_key="update-card-round-trip",
            parent_id="new-parent",
            project_id="new-project",
            tags=["two"],
            expected_version="version",
            field_intent=["parent_id", "project_id", "tags"],
        )
        update = request.call_args
        assert update.args[1:3] == ("PATCH", "/api/cards/card")
        assert update.kwargs["json"] == {
            "parent_id": "new-parent",
            "project_id": "new-project",
            "tags": ["two"],
            "updated_at": "version",
            "field_intent": ["parent_id", "project_id", "tags"],
        }
        assert update.kwargs["headers"] == {
            "Idempotency-Key": "update-card-round-trip"
        }


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
