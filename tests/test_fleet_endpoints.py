from __future__ import annotations

from types import SimpleNamespace

import httpx
import pytest

from pa.domain.models import FleetInstance
from pa.fleet.endpoints import (
    EndpointHealthRegistry,
    EndpointUnavailable,
    request_peer,
)


def _ctx(tmp_path, *, instance_id="local", port=8080):
    settings = SimpleNamespace(instance_id=instance_id, port=port, data_dir=tmp_path)
    registry = EndpointHealthRegistry(tmp_path)
    return SimpleNamespace(
        settings=settings,
        services={"endpoint_health_registry": registry},
    )


def test_local_instance_prefers_loopback_without_rewriting_advertised_url(tmp_path):
    ctx = _ctx(tmp_path, port=8123)
    inst = FleetInstance(
        instance_id="local",
        name="macbook",
        url="http://100.120.151.77:8080",
    )

    choices = ctx.services["endpoint_health_registry"].choices(ctx.settings, inst)

    assert choices[0].url == "http://127.0.0.1:8123"
    assert inst.url == "http://100.120.151.77:8080"
    assert inst.endpoints == ["http://100.120.151.77:8080"]


@pytest.mark.asyncio
async def test_transport_failure_falls_back_and_open_circuit_fails_fast(tmp_path):
    ctx = _ctx(tmp_path, instance_id="authority")
    inst = FleetInstance(
        instance_id="peer",
        name="peer",
        url="http://bad",
        endpoints=["http://bad", "http://good"],
    )
    requested = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requested.append(str(request.url))
        if request.url.host == "bad":
            raise httpx.ConnectTimeout("timed out", request=request)
        return httpx.Response(200, json={"ready": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        response, selected = await request_peer(
            ctx, client, inst, "GET", "/api/ready", timeout=0.1
        )
        response2, selected2 = await request_peer(
            ctx, client, inst, "GET", "/api/ready", timeout=0.1
        )

    assert response.status_code == response2.status_code == 200
    assert selected == selected2 == "http://good"
    assert requested == [
        "http://bad/api/ready",
        "http://good/api/ready",
        "http://good/api/ready",
    ]
    state = ctx.services["endpoint_health_registry"].snapshot()["http://bad"]
    assert state["state"] == "down"
    assert state["next_probe_at"] > state["last_failure_at"]


def test_endpoint_health_survives_restart(tmp_path):
    registry = EndpointHealthRegistry(tmp_path)
    registry.success("http://fast", 12.5)
    registry.failure("http://down", TimeoutError("connect timed out"))

    restarted = EndpointHealthRegistry(tmp_path).snapshot()

    assert restarted["http://fast"]["state"] == "healthy"
    assert restarted["http://fast"]["latency_ms"] == 12.5
    assert restarted["http://down"]["state"] == "down"
    assert restarted["http://down"]["error_class"] == "TimeoutError"


@pytest.mark.asyncio
async def test_all_failures_preserve_timeout_endpoint_and_retry_provenance(tmp_path):
    ctx = _ctx(tmp_path, instance_id="authority")
    inst = FleetInstance(instance_id="peer", name="peer", url="http://down")

    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("connect timed out", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(EndpointUnavailable) as raised:
            await request_peer(ctx, client, inst, "GET", "/api/ready", timeout=0.1)

    error = raised.value
    assert isinstance(error, TimeoutError)
    assert error.selected_endpoint == "http://down"
    assert error.endpoints == ["http://down"]
    assert error.retry_after > 0
    assert "retry after" in str(error)
