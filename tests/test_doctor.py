from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa import __version__
from pa.acp.mcp_config import McpHandshakeError, OwnerChannelError
from pa.cli.doctor import run_doctor
from pa.config import Settings


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


def test_doctor_uses_live_owner_socket_instead_of_fabricated_local_path(
    tmp_path: Path,
) -> None:
    live_socket = "/run/user/1000/pa/instance/owner.sock"
    assert _run(tmp_path, live_socket=live_socket) == 0
