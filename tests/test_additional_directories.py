from pathlib import Path

import pytest

from pa.acp.configuration import ACPConfigurationError, SessionConfigurationRequest
from pa.config import Settings
from tests.test_execution_selection_runtime import wire_app


def test_directory_configuration_roundtrip_and_validation(tmp_path):
    root = str(tmp_path)
    request = SessionConfigurationRequest.from_values(additional_directories=[root, root + "/."])
    assert request.additional_directories == (root,)
    assert SessionConfigurationRequest.from_dict(request.as_dict()) == request
    assert request.merged(SessionConfigurationRequest(mode_id="read-only")).additional_directories == (root,)
    with pytest.raises(ACPConfigurationError, match="absolute"):
        SessionConfigurationRequest.from_values(additional_directories=["relative"])


def test_directory_api_persists_requested_workspace(wire_app, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("additional-workspace")
    client, _ = wire_app
    response = client.post("/api/agent/sessions", json={
        "label": "additional-workspace", "additional_directories": [str(tmp_path)],
    })
    assert response.status_code == 200, response.text
    config = response.json()
    assert config["configuration"]["requested"]["additional_directories"] == [str(tmp_path)]


def test_instance_directory_environment(monkeypatch, tmp_path):
    import json
    monkeypatch.setenv("PA_AGENT_ADDITIONAL_DIRECTORIES", json.dumps([str(tmp_path)]))
    assert Settings().agent_additional_directories == [str(tmp_path)]


@pytest.mark.parametrize("target", ["data", "managed"])
def test_directory_grants_preserve_service_and_workspace_boundaries(tmp_path, target):
    import asyncio
    from unittest.mock import MagicMock
    from pa.acp.client import AgentConnection
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "managed")
    root = tmp_path / target
    root.mkdir(exist_ok=True)
    connection = AgentConnection(settings, MagicMock())
    with pytest.raises(ACPConfigurationError, match="service data|another managed workspace"):
        asyncio.run(connection.connect(initial_configuration=SessionConfigurationRequest.from_values(
            additional_directories=[str(root)],
        )))
