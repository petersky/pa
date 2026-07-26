from __future__ import annotations

import hashlib
import hmac
import json
from pathlib import Path

import pytest

from pa.config import Settings
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import FleetInstance, PeerRoute
from pa.fleet.join import apply_join_response
from pa.fleet.registry import FleetRegistry
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
