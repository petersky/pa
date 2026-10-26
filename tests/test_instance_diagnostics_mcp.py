from __future__ import annotations

import gzip
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from tests.test_instance_mcp import FakeMcp
from pa.acp.environment import sanitize_provider_environment
from pa.config import Settings
from pa.mcp import local_api
from pa.mcp.tools.instance import register_mcp


def test_diagnostics_fixed_routes_and_complete_response_budget():
    mcp = FakeMcp()
    settings = SimpleNamespace()
    for section, path in [("status", "/api/status"), ("runtime", "/api/runtime"), ("maintenance", "/api/instance/maintenance")]:
        with patch.object(local_api, "request_local_pa", return_value={"complete": True}) as request:
            register_mcp(mcp, SimpleNamespace(settings=settings))
            assert mcp.functions["get_instance_diagnostics"](section) == {"complete": True}
        request.assert_called_once_with(settings, "GET", path, timeout_seconds=5.0, max_response_bytes=2 * 1024 * 1024)
    with pytest.raises(ValueError):
        mcp.functions["get_instance_diagnostics"]("../configuration")


def test_provider_environment_never_receives_bridge_identity_even_in_overrides():
    assert sanitize_provider_environment(
        {"PA_LOCAL_API_TOKEN": "fixture-private", "PA_INSTANCE_ID": "fixture-owner", "SAFE": "visible"},
        {"PA_LOCAL_API_TOKEN": "fixture-override", "PA_INSTANCE_ID": "fixture-other"},
    ) == {"SAFE": "visible"}


@pytest.mark.parametrize("compressed", [False, True])
def test_bounded_diagnostics_reject_large_decoded_json_before_hydration(tmp_path, monkeypatch, compressed):
    for name in ("PA_ASSIGNED_SERVICE_MODE", "PA_ASSIGNED_SERVICE_SESSION_ID", "PA_ASSIGNED_SERVICE_DISPATCH_ID", "PA_LOCAL_API_SOCKET"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("PA_LOCAL_API_TOKEN", "private-fixture-only")
    monkeypatch.setenv("PA_INSTANCE_ID", "fixture-owner")
    monkeypatch.setenv("PA_LOCAL_API_URL", "http://owner.test")
    monkeypatch.setattr(local_api, "_circuit", local_api._Circuit())
    body = b'{"payload":"' + b'x' * 4096 + b'"}'
    client_type = httpx.Client
    transport = httpx.MockTransport(lambda request: httpx.Response(
        200, content=gzip.compress(body) if compressed else body,
        headers={"X-PA-Instance-ID": "fixture-owner", "X-Request-ID": "owner-correlation", **({"Content-Encoding": "gzip"} if compressed else {})},
    ))
    with patch.object(httpx, "Client", side_effect=lambda **_: client_type(transport=transport)), patch.object(local_api, "UserDirectory", side_effect=AssertionError("no fallback")):
        with pytest.raises(local_api.LocalPARequestError) as error:
            local_api.request_local_pa(Settings(data_dir=tmp_path), "GET", "/api/runtime", max_response_bytes=1024)
        assert error.value.code == "diagnostics_response_too_large"
        assert error.value.correlation_id == "owner-correlation"
        assert local_api.request_local_pa(Settings(data_dir=tmp_path), "GET", "/api/runtime", max_response_bytes=8192) == {"payload": "x" * 4096}


def test_byte_budget_cannot_be_used_for_mutations():
    with pytest.raises(ValueError):
        local_api.request_local_pa(SimpleNamespace(), "POST", "/api/instance/maintenance/run", max_response_bytes=1024)


def test_diagnostics_cannot_expand_assigned_session_authority(tmp_path, monkeypatch):
    monkeypatch.setenv("PA_ASSIGNED_SERVICE_MODE", "1")
    monkeypatch.setenv("PA_ASSIGNED_SERVICE_SESSION_ID", "private-session")
    monkeypatch.setenv("PA_ASSIGNED_SERVICE_DISPATCH_ID", "private-dispatch")
    with patch.object(local_api, "UserDirectory", side_effect=AssertionError("no credential fallback")):
        with pytest.raises(local_api.LocalPAServerUnavailable, match="cannot invoke"):
            local_api.request_local_pa(Settings(data_dir=tmp_path), "GET", "/api/status", max_response_bytes=1024)
