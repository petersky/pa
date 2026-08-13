from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest
from typer.testing import CliRunner

from pa.config import Settings
from pa.domain.models import FleetInstance
from pa.fleet.registry import (
    FleetInstanceResolveError,
    FleetRegistry,
    resolve_fleet_instance,
)


def _member(
    instance_id: str, name: str, url: str = "http://peer.example:8080"
) -> FleetInstance:
    return FleetInstance(instance_id=instance_id, name=name, url=url)


def test_resolve_fleet_instance_accepts_id_and_casefold_name() -> None:
    instances = [
        _member("02dbcd47-8f40-44eb-8403-5eb57545afc8", "macmini"),
        _member("0c7d8ecb-7e45-4579-8fa0-35159492d3f1", "macbook"),
    ]
    by_id = resolve_fleet_instance(
        instances, "02dbcd47-8f40-44eb-8403-5eb57545afc8"
    )
    by_name = resolve_fleet_instance(instances, "MacMini")
    assert by_id.instance_id == "02dbcd47-8f40-44eb-8403-5eb57545afc8"
    assert by_name.instance_id == by_id.instance_id


def test_resolve_fleet_instance_rejects_unknown_and_duplicate_names() -> None:
    instances = [
        _member("id-a", "twin"),
        _member("id-b", "Twin"),
        _member("id-c", "solo"),
    ]
    with pytest.raises(FleetInstanceResolveError, match="ambiguous") as ambiguous:
        resolve_fleet_instance(instances, "twin")
    assert "id-a" in str(ambiguous.value)
    assert "id-b" in str(ambiguous.value)

    with pytest.raises(FleetInstanceResolveError, match="Unknown fleet instance") as missing:
        resolve_fleet_instance(instances, "missing")
    message = str(missing.value)
    assert "pa fleet list" in message
    assert "solo (id-c)" in message


def test_registry_resolve_instance_uses_shared_helper(tmp_path: Path) -> None:
    registry = FleetRegistry(tmp_path, "fleet-test")
    registry.upsert_instance(_member("uuid-1", "macmini", "http://mini:8080"))
    resolved = registry.resolve_instance("macmini")
    assert resolved.instance_id == "uuid-1"
    assert registry.resolve_instance("uuid-1").name == "macmini"


def test_agent_provider_status_resolves_name_and_uuid(tmp_path: Path) -> None:
    from pa.cli.main import app

    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        fleet_id="fleet-test",
        sync_token="tok",
    )
    settings.data_dir.mkdir(parents=True)
    registry = FleetRegistry(settings.data_dir, settings.fleet_id)
    registry.upsert_instance(
        _member(
            "02dbcd47-8f40-44eb-8403-5eb57545afc8",
            "macmini",
            "http://macmini.example:8080",
        )
    )
    payload = {"provider": "openinterpreter", "ok": True}

    def request(method: str, url: str, **kwargs) -> httpx.Response:
        assert method == "GET"
        assert url == (
            "http://macmini.example:8080/api/agent/providers/openinterpreter"
        )
        assert kwargs["headers"]["Authorization"] == "Bearer tok"
        return httpx.Response(
            200,
            content=json.dumps(payload),
            headers={"content-type": "application/json"},
            request=httpx.Request(method, url),
        )

    with (
        patch("pa.cli.agent_provider.get_settings", return_value=settings),
        patch("pa.cli.agent_provider.httpx.Client") as client_cls,
    ):
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.request.side_effect = request
        client_cls.return_value = client

        by_name = CliRunner().invoke(
            app,
            [
                "agent-provider",
                "status",
                "--provider",
                "openinterpreter",
                "--instance",
                "macmini",
            ],
        )
        by_id = CliRunner().invoke(
            app,
            [
                "agent-provider",
                "status",
                "--provider",
                "openinterpreter",
                "--instance",
                "02dbcd47-8f40-44eb-8403-5eb57545afc8",
            ],
        )

    assert by_name.exit_code == 0, by_name.output
    assert by_id.exit_code == 0, by_id.output
    assert '"provider": "openinterpreter"' in by_name.output
    assert client.request.call_count == 2


def test_agent_provider_rejects_unknown_and_ambiguous_instance(
    tmp_path: Path,
) -> None:
    from pa.cli.main import app

    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        fleet_id="fleet-test",
    )
    settings.data_dir.mkdir(parents=True)
    # Persist a duplicate-name roster directly so a fresh CLI registry loads it.
    (settings.data_dir / "fleet_instances.json").write_text(
        json.dumps(
            {
                "schema_version": FleetRegistry.SCHEMA_VERSION,
                "fleet_id": "fleet-test",
                "generation": 1,
                "instances": [
                    _member("id-a", "twin", "http://a.example:8080").model_dump(
                        mode="json"
                    ),
                    _member("id-b", "twin", "http://b.example:8080").model_dump(
                        mode="json"
                    ),
                ],
            }
        ),
        encoding="utf-8",
    )

    with patch("pa.cli.agent_provider.get_settings", return_value=settings):
        unknown = CliRunner().invoke(
            app,
            [
                "agent-provider",
                "status",
                "--provider",
                "openinterpreter",
                "--instance",
                "missing",
            ],
        )
        ambiguous = CliRunner().invoke(
            app,
            [
                "agent-provider",
                "status",
                "--provider",
                "openinterpreter",
                "--instance",
                "twin",
            ],
        )

    assert unknown.exit_code != 0
    assert "Unknown fleet instance: missing" in unknown.output
    assert "pa fleet list" in unknown.output
    assert ambiguous.exit_code != 0
    assert "ambiguous" in ambiguous.output
    assert "id-a" in ambiguous.output
    assert "id-b" in ambiguous.output


def test_fleet_list_prints_instance_id(tmp_path: Path) -> None:
    from pa.cli.main import app

    settings = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        fleet_id="fleet-test",
    )
    settings.data_dir.mkdir(parents=True)
    registry = FleetRegistry(settings.data_dir, settings.fleet_id)
    registry.upsert_instance(
        _member(
            "02dbcd47-8f40-44eb-8403-5eb57545afc8",
            "macmini",
            "http://macmini.example:8080",
        )
    )

    healthy = httpx.Response(
        200,
        content=b'{"ok":true}',
        request=httpx.Request("GET", "http://macmini.example:8080/api/health"),
    )
    with (
        patch("pa.cli.main.get_settings", return_value=settings),
        patch("httpx.Client") as client_cls,
    ):
        client = MagicMock()
        client.__enter__.return_value = client
        client.__exit__.return_value = False
        client.get.return_value = healthy
        client_cls.return_value = client
        result = CliRunner().invoke(app, ["fleet", "list"])

    assert result.exit_code == 0, result.output
    assert "macmini" in result.output
    assert "02dbcd47-8f40-44eb-8403-5eb57545afc8" in result.output
    assert "http://macmini.example:8080" in result.output
