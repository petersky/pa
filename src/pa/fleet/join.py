"""Fleet join client — register a joiner with the fleet owner."""

from __future__ import annotations

import httpx

from pa.domain.instance_config import InstanceConfig, load_instance_config, update_instance_config
from pa.domain.models import FleetInstance, PeerRoute
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable


def apply_join_response(
    data_dir,
    *,
    fleet_id: str,
    owner_url: str,
    owner_instance: dict | None = None,
    subscribed_realms: list[str] | None = None,
) -> InstanceConfig:
    """Persist fleet join results into config.json."""
    peers: list[str] = []
    if owner_url:
        peers.append(owner_url.rstrip("/"))
    config = load_instance_config(data_dir)
    if config:
        for peer in config.peers:
            if peer.rstrip("/") not in peers:
                peers.append(peer.rstrip("/"))
    updates: dict = {
        "fleet_id": fleet_id,
        "fleet_owner_url": owner_url.rstrip("/") if owner_url else "",
    }
    if peers:
        updates["peers"] = peers
    if subscribed_realms:
        updates["subscribed_realms"] = subscribed_realms
    return update_instance_config(data_dir, **updates)


async def join_fleet(
    owner_url: str,
    token: str,
    *,
    instance_id: str,
    name: str,
    url: str,
    zone: str = "default",
    capabilities: list[str] | None = None,
    sync_token: str = "",
) -> dict:
    """POST to fleet owner to join."""
    headers: dict[str, str] = {}
    if sync_token:
        headers["Authorization"] = f"Bearer {sync_token}"

    payload = {
        "token": token,
        "instance_id": instance_id,
        "name": name,
        "url": url.rstrip("/"),
        "zone": zone,
        "capabilities": capabilities or [],
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        resp = await client.post(
            f"{owner_url.rstrip('/')}/api/fleet/join",
            json=payload,
            headers=headers,
        )
        resp.raise_for_status()
        return resp.json()


def wire_owner_peers(
    peer_table: PeerTable,
    *,
    realms: list[str],
    joiner: FleetInstance,
) -> None:
    for realm in realms:
        peer_table.add_route(
            PeerRoute(
                realm_id=realm,
                target_url=joiner.url,
                target_instance_id=joiner.instance_id,
                zone=joiner.zone,
            )
        )


def register_joiner_on_owner(
    fleet: FleetRegistry,
    peer_table: PeerTable,
    *,
    joiner_id: str,
    name: str,
    url: str,
    zone: str = "default",
    capabilities: list[str] | None = None,
    realms: list[str],
) -> FleetInstance:
    inst = FleetInstance(
        instance_id=joiner_id,
        name=name,
        url=url.rstrip("/"),
        zone=zone,
        capabilities=capabilities or [],
        healthy=True,
    )
    fleet.upsert_instance(inst)
    wire_owner_peers(peer_table, realms=realms, joiner=inst)
    return inst
