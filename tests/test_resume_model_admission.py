"""Disposable ACP subprocesses: assert the model at restore, before configure."""
import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pa.acp.client import AgentConnection
from pa.acp.transport import spawn_agent
from pa.acp.configuration import ACPConfigurationError, SessionConfigurationRequest, confirmed_session_configuration
from pa.acp.providers.base import AgentProviderSpec
from pa.config import Settings
from pa.domain.models import AgentSession


@pytest.mark.parametrize("fallback", [False, True])
@pytest.mark.parametrize("explicit", [None, "fixture-balanced"])
def test_restore_launch_honors_request_before_configuration(tmp_path, fallback, explicit):
    requested = SessionConfigurationRequest.from_values(model_id="fixture-deep", reasoning="xhigh")
    session = AgentSession(agent_name="codex", external_session_id="exact-native-id", model_id="stale-model", config_json={
        "configuration": {"state": "ready", "requested": requested.as_dict(), "effective": {"model_id": "stale-model"}},
        "options": [{"id": "model", "currentValue": "stale-model"}],
    })
    native = {"model": "global-default", "model_reasoning_effort": "high", "service_tier": "fast", "approval_policy": "on-request", "sandbox_mode": "workspace-write"}
    env = {"CODEX_CONFIG": json.dumps(native)}
    if fallback:
        env["FIXTURE_RESUME_FAIL"] = "1"
    spec = AgentProviderSpec(id="codex", display_name="fixture", command=sys.executable, args=[str(Path(__file__).parent / "fixtures/selection_acp_agent.py")], env=env)
    expected = explicit or "fixture-deep"

    async def run():
        # Repeat with a new provider process, as recovery after a host restart does.
        for _ in range(2):
            connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), provider_spec=spec)
            with patch("pa.acp.client.pa_mcp_servers", return_value=[]), patch("pa.acp.client.spawn_agent", wraps=spawn_agent) as spawn:
                try:
                    restored = await connection.connect(existing_session=session, resume_external_id="exact-native-id", require_restore=True, initial_configuration=SessionConfigurationRequest.from_values(model_id=explicit) if explicit else None)
                    # No configure call has happened: this is the restore response.
                    assert restored is session
                    assert restored.external_session_id == "exact-native-id"
                    assert confirmed_session_configuration(restored.config_json)["model_id"] == expected
                    assert restored.model_id == expected
                    launched = json.loads(spawn.call_args.kwargs["env"]["CODEX_CONFIG"])
                    assert {key: launched[key] for key in native} == native | {"model": expected}
                    assert spec.env == env
                    await connection.configure(SessionConfigurationRequest.from_values(model_id=expected), force=True)
                finally:
                    await connection.disconnect()
    asyncio.run(run())


@pytest.mark.parametrize("provider", ["codex", "generic"])
def test_fresh_default_and_other_provider_keep_native_environment(tmp_path, provider):
    spec = AgentProviderSpec(id=provider, display_name="fixture", command=sys.executable, args=[str(Path(__file__).parent / "fixtures/selection_acp_agent.py")])
    async def run():
        connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), provider_spec=spec)
        with patch("pa.acp.client.pa_mcp_servers", return_value=[]):
            try:
                session = await connection.connect()
                assert session.model_id == "fixture-balanced"
            finally:
                await connection.disconnect()
    asyncio.run(run())


def test_unavailable_model_does_not_gain_ready_receipt(tmp_path):
    request = SessionConfigurationRequest.from_values(model_id="unavailable")
    spec = AgentProviderSpec(id="codex", display_name="fixture", command=sys.executable, args=[str(Path(__file__).parent / "fixtures/selection_acp_agent.py")])
    async def run():
        connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), provider_spec=spec)
        with patch("pa.acp.client.pa_mcp_servers", return_value=[]):
            try:
                session = await connection.connect(initial_configuration=request)
                with pytest.raises(ACPConfigurationError):
                    await connection.configure(request, force=True)
                assert session.config_json["configuration"]["state"] == "failed"
                assert "effective" not in session.config_json["configuration"]
            finally:
                await connection.disconnect()
    asyncio.run(run())


@pytest.mark.parametrize("fault", ["FIXTURE_IGNORE_MODEL", "FIXTURE_NO_METADATA"])
def test_startup_acknowledgement_is_live_and_failure_is_not_silently_corrected(tmp_path, fault):
    request = SessionConfigurationRequest.from_values(model_id="fixture-deep")
    session = AgentSession(agent_name="codex", external_session_id="exact-id", model_id="fixture-deep", config_json={
        "models": {"currentModelId": "fixture-deep"},
        "configuration": {"state": "ready", "requested": request.as_dict(), "effective": {"model_id": "fixture-deep"}},
    })
    spec = AgentProviderSpec(id="codex", display_name="fixture", command=sys.executable, args=[str(Path(__file__).parent / "fixtures/selection_acp_agent.py")], env={fault: "1"})
    async def run():
        connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), provider_spec=spec)
        with patch("pa.acp.client.pa_mcp_servers", return_value=[]):
            try:
                if fault == "FIXTURE_IGNORE_MODEL":
                    with pytest.raises(ACPConfigurationError, match="startup model differs"):
                        await connection.connect(existing_session=session, resume_external_id="exact-id", require_restore=True)
                    assert session.config_json["configuration"]["state"] == "failed"
                    assert session.config_json["model_startup"]["effective"] == "fixture-balanced"
                else:
                    await connection.connect(existing_session=session, resume_external_id="exact-id", require_restore=True)
                    assert session.model_id is None
                    assert session.config_json["model_startup"]["effective"] is None
                    assert session.config_json["configuration"]["state"] == "pending"
                    assert "effective" not in session.config_json["configuration"]
                assert session.external_session_id == "exact-id"
            finally:
                await connection.disconnect()
    asyncio.run(run())


@pytest.mark.parametrize("raw", ["{invalid", "[]"])
def test_invalid_native_config_fails_before_spawn(tmp_path, raw):
    spec = AgentProviderSpec(id="codex", display_name="fixture", command=sys.executable, env={"CODEX_CONFIG": raw})
    connection = AgentConnection(Settings(data_dir=tmp_path), MagicMock(), provider_spec=spec)
    async def run():
        with patch("pa.acp.client.pa_mcp_servers", return_value=[]), patch("pa.acp.client.spawn_agent") as spawn:
            with pytest.raises(ACPConfigurationError, match="CODEX_CONFIG"):
                await connection.connect(initial_configuration=SessionConfigurationRequest.from_values(model_id="fixture-deep"))
            spawn.assert_not_called()
            assert spec.env["CODEX_CONFIG"] == raw
    asyncio.run(run())
