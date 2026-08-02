from __future__ import annotations

import os
import stat
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa import __version__
from pa.acp.mcp_config import McpHandshakeError, OwnerChannelError
from pa.cli.doctor import (
    Finding,
    _canonical,
    _dedupe_plan,
    _provider_findings,
    _safe,
    _service_findings,
    _socket_evidence,
    run_doctor,
)
from pa.cli.service import LoadedServiceDefinition
from pa.config import Settings
from pa.packaging.service_env import service_environment


def _status(settings: Settings, socket_path: str | None = None) -> dict:
    return {
        "version": __version__,
        "instance_id": settings.instance_id,
        "owner_channel": {
            "endpoint_type": "unix",
            "state": "bound",
            "failure_classification": None,
            **({"socket_path": socket_path} if socket_path else {}),
        },
        "provider_execution_environment": {
            "sanitized": True,
            "private_variables_present": [],
        },
    }


def _run(
    tmp_path: Path,
    *,
    owner_error: OwnerChannelError | None = None,
    mcp_error: McpHandshakeError | None = None,
    live_socket: str | None = None,
) -> int:
    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        instance_id="instance-1",
        fleet_id="fleet-1",
        instance_name="test",
        agent_enabled=False,
    )
    config = SimpleNamespace(
        instance_id=settings.instance_id,
        fleet_id=settings.fleet_id,
        subscribed_realms=list(settings.subscribed_realms),
        release_track=settings.release_track,
    )
    service_status = SimpleNamespace(
        installed=False,
        loaded=False,
        running=False,
        backend="none",
    )
    event_log = MagicMock()
    event_log.get_head.return_value = "head"
    object_store = MagicMock()
    object_store.list_hashes.return_value = []

    def owner_probe_result(*_args, **kwargs):
        if live_socket:
            assert kwargs["environment"]["PA_OWNER_SOCKET"] == live_socket
        if owner_error:
            raise owner_error
        return {"state": "connected", "endpoint_type": "unix"}

    owner_probe = MagicMock(side_effect=owner_probe_result)
    mcp_probe = (
        MagicMock(side_effect=mcp_error)
        if mcp_error
        else MagicMock(
            return_value={"state": "connected", "classification": "ok", "tool_count": 5}
        )
    )
    socket_path = tmp_path / "runtime" / "owner.sock"
    with (
        patch.dict(
            os.environ,
            {"PATH": "/bin", "PA_OWNER_SOCKET": str(socket_path)},
            clear=True,
        ),
        patch("pa.cli.doctor.get_settings", return_value=settings),
        patch("pa.cli.doctor.load_instance_config", return_value=config),
        patch("pa.cli.doctor.load_install_metadata", return_value=None),
        patch("pa.cli.doctor.svc.get_status", return_value=service_status),
        patch("pa.cli.doctor.svc.find_pa_binary", return_value=Path("/bin/pa")),
        patch("pa.cli.doctor.svc.find_service_binary", return_value=Path("/bin/pa")),
        patch("pa.cli.doctor.svc.service_supported", return_value=False),
        patch("pa.cli.doctor._binary_version", return_value=__version__),
        patch("pa.cli.doctor._check_health", new=AsyncMock(return_value=True)),
        patch(
            "pa.cli.doctor._fetch_status",
            new=AsyncMock(return_value=_status(settings, live_socket)),
        ),
        patch("pa.cli.doctor.probe_owner_channel", owner_probe),
        patch("pa.cli.doctor.probe_pa_mcp_stdio", mcp_probe),
        patch("pa.acp.providers.resolve.list_provider_summaries", return_value=[]),
        patch("pa.sync.infrastructure.get_event_log", return_value=event_log),
        patch("pa.sync.infrastructure.get_object_store", return_value=object_store),
    ):
        result = run_doctor()
    if owner_error:
        mcp_probe.assert_not_called()
    return result


def test_doctor_fails_when_public_api_is_healthy_but_owner_is_unreachable(
    tmp_path: Path,
) -> None:
    result = _run(
        tmp_path,
        owner_error=OwnerChannelError(
            "unreachable",
            "unix",
            "Restart PA to recreate its private owner socket.",
        ),
    )
    assert result == 1


def test_doctor_fails_when_owner_is_healthy_but_mcp_handshake_fails(
    tmp_path: Path,
) -> None:
    result = _run(
        tmp_path,
        mcp_error=McpHandshakeError(
            "tools_list_failed",
            "Inspect the pinned PA MCP child.",
        ),
    )
    assert result == 1


def test_doctor_accepts_end_to_end_owner_and_mcp_health(tmp_path: Path) -> None:
    assert _run(tmp_path) == 0


def test_list_service_environment_compares_semantically_after_restart(
    tmp_path: Path,
) -> None:
    settings = Settings(
        data_dir=tmp_path,
        subscribed_realms=["primary", "secondary"],
        peers=["https://b.example", "https://a.example"],
    )
    environment = service_environment(settings)
    environment["PA_SUBSCRIBED_REALMS"] = '[ "secondary", "primary" ]'
    environment["PA_PEERS"] = '["https://a.example","https://b.example"]'
    loaded = LoadedServiceDefinition(
        "systemd",
        "/bin/pa",
        environment,
        unit_path="/home/test/.config/systemd/user/pa-server.service",
        process_environment=dict(environment),
    )
    status = SimpleNamespace(installed=True, running=True, backend="systemd")
    with (
        patch("pa.cli.doctor.svc.service_supported", return_value=True),
        patch("pa.cli.doctor.svc.loaded_service_definition", return_value=loaded),
    ):
        _, findings, causes = _service_findings(settings, status, Path("/bin/pa"), 0)
    codes = {finding.code for finding in findings}
    assert "PA-DOC-SERVICE-ENV-DRIFT" not in codes
    assert "PA-DOC-SERVICE-ENV-NORMALIZATION" in codes
    assert "service_drift" not in causes


def test_stale_process_environment_is_distinct_from_loaded_unit(tmp_path: Path) -> None:
    settings = Settings(data_dir=tmp_path, subscribed_realms=["one", "two"])
    environment = service_environment(settings)
    process_environment = dict(environment)
    process_environment["PA_SUBSCRIBED_REALMS"] = '["one"]'
    loaded = LoadedServiceDefinition(
        "systemd",
        "/bin/pa",
        environment,
        unit_path="pa-server.service",
        process_environment=process_environment,
    )
    status = SimpleNamespace(installed=True, running=True, backend="systemd")
    with (
        patch("pa.cli.doctor.svc.service_supported", return_value=True),
        patch("pa.cli.doctor.svc.loaded_service_definition", return_value=loaded),
    ):
        _, findings, causes = _service_findings(settings, status, Path("/bin/pa"), 2)
    stale = next(
        finding
        for finding in findings
        if finding.code == "PA-DOC-SERVICE-PROCESS-ENV-STALE"
    )
    assert "stale_process_environment" in causes
    assert stale.next_commands[0].may_interrupt_sessions is True


def test_socket_evidence_classifies_missing_and_stale_socket(tmp_path: Path) -> None:
    missing = _socket_evidence(tmp_path / "missing.sock")
    assert missing["listener"] == "missing"
    fake_socket = MagicMock()
    fake_socket.connect.side_effect = ConnectionRefusedError()
    socket_stat = SimpleNamespace(st_mode=stat.S_IFSOCK | 0o600, st_uid=os.getuid())
    with (
        patch.object(Path, "lstat", return_value=socket_stat),
        patch("pa.cli.doctor.socket.socket", return_value=fake_socket),
    ):
        assert _socket_evidence(tmp_path / "stale.sock")["listener"] == "stale_socket"


def test_redaction_and_repair_plan_root_cause_ordering() -> None:
    assert _safe("PA_SYNC_TOKEN", "super-secret") == "<redacted>"
    assert _canonical("PA_PEERS", '["b", "a", "a"]') == ["a", "b"]
    service = Finding(
        "SERVICE",
        "error",
        "cause",
        "impact",
        next_commands=[],
        root_cause="service_drift",
    )
    owner = Finding(
        "OWNER",
        "error",
        "cause",
        "impact",
        root_cause="owner_channel",
    )
    assert _dedupe_plan([owner, service])[-1].command == "pa doctor --verbose"


def test_optional_providers_are_informational_when_codex_is_usable(
    tmp_path: Path,
) -> None:
    settings = Settings(data_dir=tmp_path, agent_provider="codex")
    summaries = [
        {"id": "codex", "available": True},
        {"id": "cursor", "available": False},
        {"id": "openinterpreter", "available": False},
    ]
    with patch(
        "pa.acp.providers.resolve.list_provider_summaries", return_value=summaries
    ):
        findings = _provider_findings(settings)

    assert {finding.severity for finding in findings} == {"info"}
    assert {finding.code for finding in findings} == {
        "PA-DOC-PROVIDER-OPTIONAL-UNINSTALLED"
    }
    assert all(not finding.next_commands for finding in findings)


def test_doctor_uses_live_owner_socket_instead_of_fabricated_local_path(
    tmp_path: Path,
) -> None:
    live_socket = "/run/user/1000/pa/instance/owner.sock"
    assert _run(tmp_path, live_socket=live_socket) == 0
