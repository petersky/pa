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
    _github_agent_auth,
    _canonical,
    _dedupe_plan,
    _provider_findings,
    _safe,
    _service_findings,
    _serving_findings,
    _socket_evidence,
    _sync_findings,
    run_doctor,
)
from pa.cli.service import LoadedServiceDefinition
from pa.config import Settings
from pa.packaging.service_env import service_environment
from pa.status.serving import (
    BindReport,
    HealthProbe,
    ServingDiagnosis,
    SyncDiagnosis,
)


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


def test_github_agent_auth_reports_modes_without_token_values(tmp_path: Path) -> None:
    integration = tmp_path / "integrations"
    integration.mkdir()
    token = "github-token-that-must-not-leak"
    (integration / "github.json").write_text(
        '{"token":"' + token + '","allowed_repositories":["petersky/pa"]}'
    )
    settings = Settings(
        data_dir=tmp_path, agent_github_token_enabled=True
    )
    completed = SimpleNamespace(returncode=0)
    with patch("pa.cli.doctor.subprocess.run", return_value=completed):
        evidence = _github_agent_auth(settings)
    assert evidence == {
        "mode": "both",
        "oauth_keyring": True,
        "injected_gh_token": True,
        "injection_enabled": True,
        "token_source": "instance_file",
        "allowed_repository_count": 1,
    }
    assert token not in repr(evidence)


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
        patch(
            "pa.cli.doctor.diagnose_sync",
            return_value=SyncDiagnosis("default", None, None, True),
        ),
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


def test_doctor_loopback_only_bind_recommends_wildcard_host() -> None:
    settings = Settings(
        host="127.0.0.1",
        port=8080,
        instance_url="http://100.113.226.91:8080",
    )
    findings = _serving_findings(
        ServingDiagnosis(
            service_running=True,
            bind=BindReport((("127.0.0.1", 8080),), True, False, "loopback"),
            advertised_url="http://100.113.226.91:8080",
            loopback_url="http://127.0.0.1:8080",
            loopback=HealthProbe(
                "http://127.0.0.1:8080/api/health", True, 2.0, 200, None
            ),
            advertised=HealthProbe(
                "http://100.113.226.91:8080/api/health", False, 6.0, None, "refused"
            ),
            serving="loopback_only",
            health_ok=False,
        ),
        settings,
        0,
    )
    assert {finding.code for finding in findings} == {"PA-DOC-BIND-LOOPBACK-ONLY"}
    commands = [command.command for finding in findings for command in finding.next_commands]
    assert "pa config set host 0.0.0.0" in commands
    assert "pa install --service-only" in commands
    assert "pa restart" in commands


def test_doctor_timeout_without_loopback_recommends_restart_and_rebind() -> None:
    settings = Settings(host="100.78.2.112", port=8080, instance_url="http://100.78.2.112:8080")
    findings = _serving_findings(
        ServingDiagnosis(
            service_running=True,
            bind=BindReport((("100.78.2.112", 8080),), False, True, "specific"),
            advertised_url="http://100.78.2.112:8080",
            loopback_url="http://127.0.0.1:8080",
            loopback=HealthProbe(
                "http://127.0.0.1:8080/api/health", False, 3.0, None, "refused"
            ),
            advertised=HealthProbe(
                "http://100.78.2.112:8080/api/health", False, 3000.0, None, "timeout"
            ),
            serving="timeout",
            health_ok=False,
        ),
        settings,
        0,
    )
    assert {finding.code for finding in findings} == {
        "PA-DOC-BIND-NO-LOOPBACK",
        "PA-DOC-HEALTH-TIMEOUT",
    }
    commands = [command.command for finding in findings for command in finding.next_commands]
    assert "pa config set host 0.0.0.0" in commands
    assert "pa restart" in commands


def test_doctor_sync_inconsistency_recommends_reconcile() -> None:
    findings = _sync_findings(
        SyncDiagnosis("default", "durable-head", "projection-head", False)
    )
    assert {finding.code for finding in findings} == {"PA-DOC-SYNC-INCONSISTENT"}
    commands = [command.command for finding in findings for command in finding.next_commands]
    assert commands == ["pa sync status", "pa sync reconcile"]
