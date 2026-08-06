from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from pa.auth.users import UserDirectory
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardCreate, CardLane, FleetInstance
from pa.domain.store import reset_store
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.agent_session import reset_instance_agent


@pytest.fixture(autouse=True)
def _reset_pa_singletons():
    reset_settings()
    reset_store()
    reset_instance_agent()
    yield
    reset_instance_agent()
    reset_store()
    reset_settings()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


def _hardened_app(
    path: Path,
    *,
    instance_id: str,
    instance_name: str,
    instance_url: str,
):
    kernel = Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id=instance_id,
            instance_name=instance_name,
            instance_url=instance_url,
            fleet_id="repair-auth-fleet",
            sync_token="fleet-secret",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
            auth_required=True,
        )
    )
    app = kernel.build_app()
    app.state.kernel = kernel
    app.state.ctx = kernel.ctx
    return app


class _RecordingASGITransport(httpx.AsyncBaseTransport):
    def __init__(self, apps: dict[str, object]) -> None:
        self._transports = {
            host: httpx.ASGITransport(app=app) for host, app in apps.items()
        }
        self.requests: list[dict[str, str | None]] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(
            {
                "host": request.url.host,
                "method": request.method,
                "path": request.url.path,
                "authorization": request.headers.get("Authorization"),
                "origin_instance_id": request.headers.get("X-PA-Origin-Instance-ID"),
            }
        )
        transport = self._transports.get(request.url.host)
        if transport is None:
            raise httpx.ConnectError(
                f"No test fleet route for {request.url.host}",
                request=request,
            )
        return await transport.handle_async_request(request)

    async def aclose(self) -> None:
        for transport in self._transports.values():
            await transport.aclose()


@pytest.mark.anyio
async def test_hardened_auth_allows_full_authority_to_target_terminal_commit_flow(
    tmp_path: Path,
) -> None:
    origin_app = _hardened_app(
        tmp_path / "origin",
        instance_id="origin",
        instance_name="Origin",
        instance_url="http://origin.test:8080",
    )
    authority_app = _hardened_app(
        tmp_path / "authority",
        instance_id="authority",
        instance_name="Authority",
        instance_url="http://authority.test:8080",
    )
    target_app = _hardened_app(
        tmp_path / "target",
        instance_id="target",
        instance_name="Target",
        instance_url="http://target.test:8080",
    )
    origin_ctx = origin_app.state.ctx
    authority_ctx = authority_app.state.ctx
    target_ctx = target_app.state.ctx
    authority_ctx.services["dispatch_store"] = DispatchStore(
        tmp_path / "authority-dispatch-ledger"
    )
    target_ctx.services["dispatch_store"] = DispatchStore(
        tmp_path / "target-dispatch-ledger"
    )

    origin_ctx.require_service("fleet_registry").upsert_instance(
        FleetInstance(
            instance_id="authority",
            name="Authority",
            url="http://authority.test:8080",
        ),
        actor="test",
    )
    authority_ctx.require_service("fleet_registry").upsert_instance(
        FleetInstance(
            instance_id="target",
            name="Target",
            url="http://target.test:8080",
        ),
        actor="test",
    )

    card = authority_ctx.store.create_card(
        CardCreate(title="Already completed card", lane=CardLane.DONE)
    )
    authority_record = DispatchRecord(
        dispatch_id="dispatch-two-hop-repair",
        mutation_id="mutation-two-hop-repair",
        card_id=card.id,
        realm_id=card.realm_id,
        authority_instance_id="authority",
        authority_url="http://authority.test:8080",
        target_instance_id="target",
        session_id="session-two-hop-repair",
        state="running",
        recoverable=False,
    )
    authority_ledger = authority_ctx.require_service("dispatch_store")
    authority_ledger.put(authority_record)
    target_ledger = target_ctx.require_service("dispatch_store")
    target_ledger.put(authority_record.model_copy(deep=True))
    target_ctx.store.save_session(
        AgentSession(
            id="session-two-hop-repair",
            agent_name="codex",
            origin_instance_id="target",
            authority_instance_id="authority",
            dispatch_id=authority_record.dispatch_id,
            status="closed",
        )
    )
    target_ctx.services["instance_agent"] = type(
        "NoTargetRuntime",
        (),
        {"get": lambda _self, _session_id: None},
    )()
    origin_ctx.services.pop("async_runtime", None)
    authority_ctx.services.pop("async_runtime", None)

    origin_to_authority = _RecordingASGITransport({"authority.test": authority_app})
    authority_to_target = _RecordingASGITransport({"target.test": target_app})
    caller_transport = httpx.ASGITransport(app=origin_app)
    user = UserDirectory(tmp_path / "origin").ensure_default_user()
    body = {
        "idempotency_key": "repair-two-hop-auth-1",
        "mode": "abandoned_without_acknowledgement",
        "expected_state": "running",
        "reason": "Verified full hardened-auth terminal proof flow.",
        "confirm_no_outcome_inference": True,
    }

    async with (
        httpx.AsyncClient(
            transport=origin_to_authority,
            base_url="http://authority.test:8080",
        ) as origin_peer_client,
        httpx.AsyncClient(
            transport=authority_to_target,
            base_url="http://target.test:8080",
        ) as authority_peer_client,
        httpx.AsyncClient(
            transport=caller_transport,
            base_url="http://origin.test:8080",
        ) as caller,
    ):
        origin_ctx.services["fleet_http_client"] = origin_peer_client
        authority_ctx.services["fleet_http_client"] = authority_peer_client
        response = await caller.post(
            (
                "/api/fleet/instances/authority/dispatch-jobs/"
                "dispatch-two-hop-repair/repair-terminal"
            ),
            headers={"Authorization": f"Bearer {user.cli_token}"},
            json=body,
        )

    assert response.status_code == 200, response.text
    assert response.json()["state"] == "cancelled"
    repaired = authority_ledger.get(authority_record.dispatch_id)
    assert repaired is not None
    assert repaired.state == "cancelled"
    assert repaired.lifecycle_inconsistencies[-1]["evidence"]["source"] == (
        "authenticated_remote_target"
    )
    target_after = target_ledger.get(authority_record.dispatch_id)
    assert target_after is not None
    assert target_after.state == "cancelled"

    assert origin_to_authority.requests == [
        {
            "host": "authority.test",
            "method": "POST",
            "path": (
                "/api/fleet/dispatch-jobs/dispatch-two-hop-repair/repair-terminal"
            ),
            "authorization": "Bearer fleet-secret",
            "origin_instance_id": "origin",
        }
    ]
    assert authority_to_target.requests == [
        {
            "host": "target.test",
            "method": "POST",
            "path": (
                "/api/fleet/dispatch-jobs/dispatch-two-hop-repair/"
                "terminal-repair-evidence"
            ),
            "authorization": "Bearer fleet-secret",
            "origin_instance_id": "authority",
        },
        {
            "host": "target.test",
            "method": "POST",
            "path": (
                "/api/fleet/dispatch-jobs/dispatch-two-hop-repair/"
                "terminal-repair-commit"
            ),
            "authorization": "Bearer fleet-secret",
            "origin_instance_id": "authority",
        },
    ]
