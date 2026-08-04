from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.ui.instance_identity import canonicalize_dispatch_public
from pa.domain.config_edit import set_config_value
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import FleetInstance
from pa.fleet.convergence import MembershipConvergenceStore
from pa.fleet.registry import FleetRegistry, reconcile_snapshots
from pa.modules.fleet import _deliver_membership, rename_instance


def _member(instance_id: str, name: str, url: str) -> FleetInstance:
    return FleetInstance(instance_id=instance_id, name=name, url=url)


def test_rename_is_fenced_uuid_stable_collision_safe_and_audited(tmp_path) -> None:
    fleet = FleetRegistry(tmp_path, "fleet")
    original = fleet.upsert_instance(_member("stable-id", "local", "http://one"))
    fleet.upsert_instance(_member("peer-id", "peer", "http://two"))
    before = fleet.generation

    renamed = fleet.rename_instance(
        "stable-id",
        "macbook",
        actor="user:operator",
        source="configuration.web",
        expected_generation=before,
    )

    assert renamed.instance_id == original.instance_id == "stable-id"
    assert renamed.name == "macbook"
    assert renamed.membership_generation == fleet.generation == before + 1
    event = fleet.audit_events()[-1]
    assert event["action"] == "member.rename"
    assert event["actor"] == "user:operator"
    assert event["detail"] == {
        "instance_id": "stable-id",
        "old_name": "local",
        "new_name": "macbook",
        "source": "configuration.web",
        "member_generation": before + 1,
    }
    with pytest.raises(ValueError, match="already belongs"):
        fleet.rename_instance(
            "stable-id",
            "peer",
            actor="user:operator",
            source="test",
            expected_generation=fleet.generation,
        )
    with pytest.raises(ValueError, match="generation changed"):
        fleet.rename_instance(
            "stable-id",
            "other",
            actor="user:operator",
            source="test",
            expected_generation=before,
        )


def test_restart_registration_cannot_revert_canonical_name(tmp_path) -> None:
    fleet = FleetRegistry(tmp_path, "fleet")
    fleet.upsert_instance(_member("stable-id", "macbook", "http://one"))
    generation = fleet.generation

    restarted = FleetRegistry(tmp_path, "fleet")
    member = restarted.register_self("stable-id", "local", "http://one")

    assert member.instance_id == "stable-id"
    assert member.name == "macbook"
    assert restarted.generation >= generation


def test_rollout_store_persists_pending_failure_and_applied_generation(
    tmp_path,
) -> None:
    store = MembershipConvergenceStore(tmp_path, "local")
    members = [
        _member("local", "Local", "http://local"),
        _member("offline", "Offline", "http://offline"),
    ]
    store.plan(7, members)
    store.failed("offline", 7, "connection refused")

    reloaded = MembershipConvergenceStore(tmp_path, "local").snapshot()
    assert reloaded["pending"] == 1
    assert reloaded["peers"][0]["target_generation"] == 7
    assert reloaded["peers"][0]["error_code"] == "delivery_failed"

    store.applied("offline", 7)
    applied = MembershipConvergenceStore(tmp_path, "local").snapshot()
    assert applied["applied"] == 1
    assert applied["peers"][0]["applied_generation"] == 7


@pytest.mark.asyncio
async def test_online_rollout_sends_complete_roster_and_records_ack(tmp_path) -> None:
    fleet = FleetRegistry(tmp_path, "fleet")
    for member in (
        _member("local", "Local", "http://local"),
        _member("mini", "Mini", "http://mini"),
        _member("monica", "Monica", "http://monica"),
    ):
        fleet.upsert_instance(member)
    convergence = MembershipConvergenceStore(tmp_path, "local")
    client = AsyncMock()
    client.post.return_value = httpx.Response(
        200,
        json={"after_generation": fleet.generation},
        request=httpx.Request("POST", "http://peer/api/fleet/membership/apply"),
    )
    settings = SimpleNamespace(
        instance_id="local", sync_token="shared", data_dir=tmp_path
    )
    services = {
        "fleet_registry": fleet,
        "membership_convergence": convergence,
    }
    ctx = SimpleNamespace(
        settings=settings,
        require_service=lambda name: services[name],
    )

    result = await _deliver_membership(ctx, client, force=True)

    assert len(result) == 2
    assert {item["status"] for item in result} == {"applied"}
    assert client.post.await_count == 2
    for call in client.post.await_args_list:
        membership = call.kwargs["json"]["membership"]
        assert {item["instance_id"] for item in membership["instances"]} == {
            "local",
            "mini",
            "monica",
        }


@pytest.mark.asyncio
async def test_rolling_upgrade_incompatibility_is_actionable_and_retried(
    tmp_path,
) -> None:
    fleet = FleetRegistry(tmp_path, "fleet")
    fleet.upsert_instance(_member("local", "Local", "http://local"))
    fleet.upsert_instance(_member("old", "Old", "http://old"))
    convergence = MembershipConvergenceStore(tmp_path, "local")
    client = AsyncMock()
    client.post.return_value = httpx.Response(
        404,
        text="membership endpoint unavailable",
        request=httpx.Request("POST", "http://old/api/fleet/membership/apply"),
    )
    services = {
        "fleet_registry": fleet,
        "membership_convergence": convergence,
    }
    ctx = SimpleNamespace(
        settings=SimpleNamespace(instance_id="local", sync_token="shared"),
        require_service=lambda name: services[name],
    )

    result = await _deliver_membership(ctx, client, force=True)

    assert result[0]["status"] == "failed"
    assert result[0]["error_code"] == "incompatible_peer"
    assert result[0]["next_attempt_at"]


@pytest.mark.asyncio
async def test_instance_authentication_can_rename_only_self(tmp_path) -> None:
    request = MagicMock()
    request.state.user = None
    request.state.principal_id = "instance:caller"
    request.state.instance_authenticated = True
    request.headers = {"X-PA-Origin-Instance-ID": "caller"}

    with pytest.raises(HTTPException) as raised:
        await rename_instance(
            request,
            "someone-else",
            {"name": "renamed", "expected_generation": 1},
        )
    assert raised.value.status_code == 403


def test_legacy_cli_config_rename_updates_same_canonical_uuid(tmp_path) -> None:
    save_instance_config(
        tmp_path,
        InstanceConfig(
            instance_id="stable-id",
            instance_name="local",
            fleet_id="fleet",
            data_dir=str(tmp_path),
        ),
    )
    fleet = FleetRegistry(tmp_path, "fleet")
    fleet.upsert_instance(_member("stable-id", "local", "http://local"))
    before = fleet.generation

    result = set_config_value(tmp_path, "instance_name", "macbook")

    assert result.after == "macbook"
    reloaded = FleetRegistry(tmp_path, "fleet")
    assert reloaded.get_instance("stable-id").name == "macbook"
    assert reloaded.generation == before + 1
    assert reloaded.audit_events()[-1]["detail"]["source"] == (
        "configuration.legacy-cli"
    )


def test_configuration_rename_commits_runtime_registry_and_rollout(tmp_path) -> None:
    save_instance_config(
        tmp_path,
        InstanceConfig(
            instance_id="stable-id",
            instance_name="local",
            fleet_id="fleet",
            data_dir=str(tmp_path),
            session_secret="rename-secret",
        ),
    )
    settings = Settings(
        data_dir=tmp_path,
        instance_id="stable-id",
        instance_name="local",
        fleet_id="fleet",
        session_secret="rename-secret",
        agent_enabled=False,
        peers=[],
    )
    from pa.core.kernel import Kernel

    app = Kernel.boot(settings=settings).build_app()
    with TestClient(app) as client:
        snapshot = client.get("/api/configuration")
        csrf = client.cookies.get("pa_csrf")
        response = client.patch(
            "/api/configuration",
            json={
                "changes": {"instance_name": "macbook"},
                "expected_revision": snapshot.json()["revision"],
                "idempotency_key": "rename-config-1",
                "interface": "api",
            },
            headers={"X-CSRF-Token": csrf},
        )

        assert response.status_code == 200, response.text
        transaction = response.json()["rename_transaction"]
        assert transaction["state"] == "committed"
        assert transaction["stable_instance_id"] == "stable-id"
        assert transaction["rollout"] == []
        assert app.state.ctx.settings.instance_name == "macbook"
        fleet = app.state.ctx.require_service("fleet_registry")
        assert fleet.get_instance("stable-id").name == "macbook"
        assert fleet.audit_events()[-1]["action"] == "member.rename"


def test_current_dispatch_name_wins_and_snapshot_is_labelled() -> None:
    fleet = MagicMock()
    fleet.list_instances.return_value = [
        _member("worker-id", "macbook", "http://worker")
    ]
    ctx = SimpleNamespace(services={"fleet_registry": fleet})
    record = MagicMock(
        authority_instance_id="worker-id",
        authority_instance_name="local",
        target_instance_id="worker-id",
        target_instance_name="local",
    )
    record.public_dict.return_value = {"dispatch_id": "dispatch"}

    public = canonicalize_dispatch_public(ctx, record)

    assert public["target_instance_name"] == "macbook"
    assert public["target_instance_name_snapshot"] == "local"
    assert public["target_instance_name_at_dispatch"] == "local"


def test_repair_prefers_newer_rename_and_completes_inventory() -> None:
    local_id = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
    old_local = _member(local_id, "local", "http://macbook")
    macbook = _member(local_id, "macbook", "http://macbook")
    mini = _member("macmini-id", "macmini", "http://macmini")
    monica = _member("monica-id", "Monica", "http://monica")

    stale_macmini = {
        "schema_version": FleetRegistry.SCHEMA_VERSION,
        "fleet_id": "fleet",
        "generation": 6,
        "instances": [
            old_local.model_dump(mode="json"),
            mini.model_dump(mode="json"),
        ],
    }
    canonical = {
        "schema_version": FleetRegistry.SCHEMA_VERSION,
        "fleet_id": "fleet",
        "generation": 7,
        "instances": [
            macbook.model_dump(mode="json"),
            mini.model_dump(mode="json"),
            monica.model_dump(mode="json"),
        ],
    }
    incomplete_monica = {
        "schema_version": FleetRegistry.SCHEMA_VERSION,
        "fleet_id": "fleet",
        "generation": 2,
        "instances": [monica.model_dump(mode="json")],
    }

    repaired = reconcile_snapshots([stale_macmini, incomplete_monica, canonical])

    assert repaired["generation"] == 7
    by_id = {item["instance_id"]: item for item in repaired["instances"]}
    assert by_id[local_id]["name"] == "macbook"
    assert set(by_id) == {local_id, "macmini-id", "monica-id"}
