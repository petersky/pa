from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import httpx
import pytest

from pa.config import Settings
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import FleetInstance, PeerRoute
from pa.fleet.join import apply_join_response
from pa.fleet.registry import (
    FleetRegistry,
    reconcile_snapshots,
    semantic_snapshot,
)
from pa.modules.fleet import _verify_membership_envelope
from pa.network.peer_table import PeerTable


def _member(instance_id: str, url: str, name: str | None = None) -> FleetInstance:
    return FleetInstance(
        instance_id=instance_id,
        name=name or instance_id,
        url=url,
        healthy=True,
    )


def _three_member_snapshot(tmp_path: Path) -> dict:
    authority = FleetRegistry(tmp_path / "authority", "fleet-a")
    authority.upsert_instance(_member("local", "http://100.120.151.77:8080"))
    authority.upsert_instance(_member("macmini", "http://100.78.2.112:8080"))
    authority.upsert_instance(_member("monica", "http://monica:8080"))
    return authority.snapshot()


def test_join_installs_complete_roster_and_derived_routes(tmp_path: Path) -> None:
    save_instance_config(
        tmp_path,
        InstanceConfig(
            instance_id="monica",
            instance_name="Monica",
            fleet_id="old",
        ),
    )
    snapshot = _three_member_snapshot(tmp_path)

    apply_join_response(
        tmp_path,
        fleet_id="fleet-a",
        owner_url="http://100.120.151.77:8080",
        subscribed_realms=["personal"],
        sync_token="shared",
        membership_snapshot=snapshot,
    )

    registry = FleetRegistry(tmp_path, "fleet-a")
    assert {member.instance_id for member in registry.list_instances()} == {
        "local",
        "macmini",
        "monica",
    }
    routes = PeerTable(tmp_path).routes_for_realm("personal")
    assert {(route.target_instance_id, route.target_url) for route in routes} == {
        ("local", "http://100.120.151.77:8080"),
        ("macmini", "http://100.78.2.112:8080"),
    }


def test_monica_legacy_inventory_is_repaired_idempotently(tmp_path: Path) -> None:
    monica_dir = tmp_path / "monica"
    monica_dir.mkdir()
    (monica_dir / "fleet_instances.json").write_text(
        json.dumps(
            {
                "instances": [
                    _member("monica", "http://monica:8080").model_dump(mode="json")
                ]
            },
            default=str,
        )
    )
    registry = FleetRegistry(monica_dir, "fleet-a")
    assert registry.generation == 1
    snapshot = _three_member_snapshot(tmp_path)

    first = registry.apply_snapshot(snapshot, actor="migration")
    second = registry.apply_snapshot(snapshot, actor="migration", require_newer=False)

    assert first == {
        "changed": True,
        "before_generation": 1,
        "after_generation": 3,
        "members": 3,
    }
    assert second["changed"] is False
    assert len(registry.audit_events()) == 2


def test_snapshot_rejects_cross_fleet_stale_and_ambiguous_identity(
    tmp_path: Path,
) -> None:
    registry = FleetRegistry(tmp_path, "fleet-a")
    registry.upsert_instance(_member("local", "http://local:8080"))

    cross_fleet = registry.snapshot() | {"fleet_id": "fleet-b"}
    with pytest.raises(ValueError, match="different fleet"):
        registry.apply_snapshot(cross_fleet)

    newer = _three_member_snapshot(tmp_path)
    registry.apply_snapshot(newer)
    with pytest.raises(ValueError, match="stale"):
        registry.apply_snapshot(
            {
                "schema_version": 2,
                "fleet_id": "fleet-a",
                "generation": 1,
                "instances": [],
            }
        )

    duplicate_endpoint = newer | {
        "generation": 4,
        "instances": [
            _member("one", "http://same:8080").model_dump(mode="json"),
            _member("two", "http://same:8080").model_dump(mode="json"),
        ],
    }
    with pytest.raises(ValueError, match="duplicate_endpoint"):
        registry.apply_snapshot(duplicate_endpoint)


def test_removal_tombstone_blocks_stale_resurrection(tmp_path: Path) -> None:
    registry = FleetRegistry(tmp_path, "fleet-a")
    registry.upsert_instance(_member("macmini", "http://mini:8080"))
    assert registry.remove_instance("macmini", actor="operator")
    assert registry.get_instance("macmini") is None
    assert (
        registry.get_instance("macmini", include_removed=True).lifecycle_state
        == "removed"
    )

    with pytest.raises(ValueError, match="cannot be reintroduced"):
        registry.upsert_instance(_member("macmini", "http://mini:8080"))


def test_routes_are_projection_not_membership_truth(tmp_path: Path) -> None:
    peers = PeerTable(tmp_path)
    peers.add_route(
        PeerRoute(
            realm_id="personal",
            target_url="http://orphan:8080",
            target_instance_id="orphan",
        )
    )
    result = peers.reconcile_membership(
        [
            _member("local", "http://local:8080"),
            _member("monica", "http://monica:8080"),
        ],
        realms=["personal"],
        local_instance_id="local",
    )

    assert result == {"before": 1, "after": 1}
    assert [
        (route.target_instance_id, route.target_url)
        for route in peers.routes_for_realm("personal")
    ] == [("monica", "http://monica:8080")]


def test_signed_roster_is_bound_to_issuer_and_reached_endpoint(tmp_path: Path) -> None:
    snapshot = _three_member_snapshot(tmp_path)
    unsigned = {"issuer_instance_id": "local", "membership": snapshot}
    payload = json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    envelope = {
        **unsigned,
        "signature": hmac.new(b"shared", payload, hashlib.sha256).hexdigest(),
    }
    settings = Settings(data_dir=tmp_path / "receiver", sync_token="shared")

    assert (
        _verify_membership_envelope(
            settings,
            envelope,
            expected_issuer="local",
            expected_endpoint="http://100.120.151.77:8080",
        )
        == snapshot
    )
    with pytest.raises(ValueError, match="authenticated origin"):
        _verify_membership_envelope(
            settings,
            envelope,
            expected_issuer="macmini",
        )
    with pytest.raises(ValueError, match="reached endpoint"):
        _verify_membership_envelope(
            settings,
            envelope,
            expected_endpoint="http://attacker:8080",
        )


def _snapshot(generation: int, *members: FleetInstance) -> dict:
    return {
        "schema_version": FleetRegistry.SCHEMA_VERSION,
        "fleet_id": "fleet-a",
        "generation": generation,
        "instances": [member.model_dump(mode="json") for member in members],
    }


def test_equal_generation_ignores_timestamps_health_and_observation_order(
    tmp_path: Path,
) -> None:
    members = [
        _member("local", "http://local:8080"),
        _member("monica", "http://monica:8080"),
        _member("macmini", "http://mini:8080"),
    ]
    first = _snapshot(3, *members)
    second = json.loads(json.dumps(first))
    for index, member in enumerate(second["instances"]):
        member["joined_at"] = datetime(2020 + index, 1, 1, tzinfo=UTC).isoformat()
        member["updated_at"] = datetime(2024, 1, index + 1, tzinfo=UTC).isoformat()
        member["last_seen"] = datetime(2025, 1, index + 1, tzinfo=UTC).isoformat()
        member["healthy"] = not member["healthy"]
        member["membership_generation"] = index + 1
        member["endpoints"].reverse()
    second["instances"].reverse()

    assert semantic_snapshot(first) == semantic_snapshot(second)
    selected = reconcile_snapshots([second, first])
    assert selected == reconcile_snapshots([first, second])
    assert selected["generation"] == 3

    registry = FleetRegistry(tmp_path, "fleet-a")
    registry.apply_snapshot(first)
    result = registry.apply_snapshot(second, require_newer=False)
    assert result["changed"] is False
    assert registry.generation == 3


def test_compatible_legacy_subset_rosters_merge_monotonically_and_idempotently() -> (
    None
):
    local = _member("local", "http://local:8080")
    monica = _member("monica", "http://monica:8080")
    mini = _member("macmini", "http://mini:8080")
    left = _snapshot(2, local, monica)
    right = _snapshot(2, local, mini)

    merged = reconcile_snapshots([left, right])
    repeated = reconcile_snapshots([right, merged, left])

    assert merged["generation"] == 3
    assert [item["instance_id"] for item in merged["instances"]] == [
        "local",
        "macmini",
        "monica",
    ]
    assert repeated == merged


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("name", "renamed"),
        ("lifecycle_state", "disabled"),
        ("credential_fingerprint", "other-credential"),
        ("dispatch_capacity", 9),
        ("zone", "other-zone"),
    ],
)
def test_same_generation_stable_member_conflicts_require_resolution(
    field: str, value: object
) -> None:
    original = _member("local", "http://local:8080")
    changed = original.model_copy(deep=True, update={field: value})
    with pytest.raises(ValueError, match="requires operator resolution"):
        reconcile_snapshots([_snapshot(3, original), _snapshot(3, changed)])


def test_compatible_subsets_reject_endpoint_identity_collision() -> None:
    with pytest.raises(ValueError, match="conflicting canonical endpoint"):
        reconcile_snapshots(
            [
                _snapshot(1, _member("one", "http://shared:8080")),
                _snapshot(1, _member("two", "http://shared:8080")),
            ]
        )


@pytest.mark.asyncio
async def test_reconcile_reports_unreachable_peer_without_changing_membership(
    tmp_path: Path,
) -> None:
    from pa.modules.fleet import reconcile_fleet_membership

    settings = Settings(
        data_dir=tmp_path,
        instance_id="local",
        instance_name="Local",
        fleet_id="fleet-a",
        sync_token="shared",
    )
    fleet = FleetRegistry(tmp_path, "fleet-a")
    fleet.upsert_instance(_member("local", "http://local:8080"))
    peers = PeerTable(tmp_path)
    peers.add_route(
        PeerRoute(
            realm_id="personal",
            target_url="http://unreachable:8080",
            target_instance_id="missing",
        )
    )
    client = MagicMock()
    client.get.side_effect = httpx.ConnectError("unreachable")
    ctx = MagicMock(settings=settings, services={"fleet_http_client": client})
    ctx.require_service.side_effect = lambda name: {
        "fleet_registry": fleet,
        "peer_table": peers,
    }[name]
    request = MagicMock()
    request.app.state.ctx = ctx
    request.headers = {}

    with patch("pa.modules.fleet.require_user", return_value=object()):
        result = await reconcile_fleet_membership(request)

    assert result["status"] == "converged"
    assert result["before_generation"] == result["after_generation"] == 1
    assert result["authenticated_peers"] == 0
    assert result["unreachable_or_incompatible"][0]["url"] == (
        "http://unreachable:8080"
    )
    assert result["rollout"] == []
