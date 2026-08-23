from __future__ import annotations

import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import httpx
import pytest
from pydantic import ValidationError

from pa.cloud import CloudCoordinator, CloudLeaseResult
from pa.config import Settings
from pa.execution.lease import LeaseManager


def settings(tmp_path: Path, **overrides) -> Settings:
    values = {
        "data_dir": tmp_path / "data",
        "workspace_root": tmp_path / "workspaces",
        "instance_id": "instance-1",
        "fleet_id": "fleet-1",
        "cloud_endpoint": "https://coordination.example",
        "cloud_token": "secret",
    }
    values.update(overrides)
    return Settings(**values)


def test_cloud_endpoint_requires_tls(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="must use HTTPS"):
        settings(tmp_path, cloud_endpoint="http://coordination.example")


def test_cloud_endpoint_requires_authentication(tmp_path: Path) -> None:
    with pytest.raises(ValidationError, match="cloud_token is required"):
        settings(tmp_path, cloud_token="")


def test_cloud_lease_acquisition_is_fenced_and_authenticated(tmp_path: Path) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(409)

    client = httpx.Client(
        base_url="https://coordination.example",
        headers={"Authorization": "Bearer secret"},
        transport=httpx.MockTransport(handler),
    )
    coordinator = CloudCoordinator(settings(tmp_path), client=client)

    result = coordinator.acquire_lease(
        {"realm_id": "default", "card_id": "card-1"}
    )

    assert result == CloudLeaseResult.DENIED
    assert requests[0].url.path == "/v1/leases/acquire"
    assert requests[0].headers["Authorization"] == "Bearer secret"
    envelope = __import__("json").loads(requests[0].content)
    assert envelope["protocol_version"] == 1
    assert envelope["fleet_id"] == "fleet-1"
    assert envelope["payload"]["card_id"] == "card-1"


def test_cloud_outage_is_reported_without_blocking_publication(tmp_path: Path) -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline")

    client = httpx.Client(
        base_url="https://coordination.example",
        transport=httpx.MockTransport(handler),
    )
    coordinator = CloudCoordinator(settings(tmp_path), client=client)
    coordinator.start()
    coordinator.publish_event({"commit_hash": "abc"})
    deadline = time.monotonic() + 2
    while coordinator.status()["last_error"] is None and time.monotonic() < deadline:
        time.sleep(0.01)
    coordinator.close()

    assert coordinator.status()["last_error"] == "ConnectError"
    assert (
        coordinator.acquire_lease({"card_id": "card-1"})
        == CloudLeaseResult.UNAVAILABLE
    )


def test_unconfigured_cloud_module_is_absent(tmp_path: Path) -> None:
    from pa.core.kernel import Kernel

    local = Settings(
        data_dir=tmp_path / "data",
        workspace_root=tmp_path / "workspaces",
        agent_enabled=False,
    )
    kernel = Kernel.boot(settings=local)

    assert "cloud_coordinator" not in kernel.ctx.services
    assert any(module["name"] == "cloud" for module in kernel.registry.describe())


@pytest.mark.parametrize(
    ("result", "fail_open", "expected"),
    [
        (CloudLeaseResult.ACQUIRED, False, True),
        (CloudLeaseResult.DENIED, True, False),
        (CloudLeaseResult.UNAVAILABLE, False, False),
        (CloudLeaseResult.UNAVAILABLE, True, True),
    ],
)
def test_cloud_lease_policy(result, fail_open, expected) -> None:
    store = MagicMock()
    store.get_card.return_value = SimpleNamespace(
        lease_holder_instance=None, lease_expires_at=None
    )
    cloud = MagicMock(fail_open=fail_open)
    cloud.acquire_lease.return_value = result
    manager = LeaseManager(store, MagicMock(), "instance-1", cloud=cloud)

    assert manager.grant(
        "card-1",
        "default",
        holder_instance="instance-1",
        holder_principal="user:1",
    ) is expected
    assert store.commit_event.call_count == int(expected)
