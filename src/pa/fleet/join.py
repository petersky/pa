"""Fleet join client and owner-side wiring helpers."""

from __future__ import annotations

import secrets
from pathlib import Path
from urllib.parse import urlparse

import httpx

from pa.config import Settings
from pa.domain.instance_config import InstanceConfig, load_instance_config, update_instance_config
from pa.domain.models import FleetInstance, PeerRoute
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable


def owner_public_url(settings: Settings) -> str:
    """Reachable owner URL for joiners — prefer instance_url, never advertise 0.0.0.0."""
    if settings.instance_url:
        return settings.instance_url.rstrip("/")
    host = settings.host
    if host in ("0.0.0.0", "::", "[::]"):
        host = "127.0.0.1"
    return f"http://{host}:{settings.port}"


def readiness_warnings(settings: Settings) -> list[str]:
    """Human-readable issues that block reliable fleet sync."""
    warnings: list[str] = []
    url = (settings.instance_url or "").rstrip("/")
    if not url:
        warnings.append(
            "PA_INSTANCE_URL is not set — peers cannot reliably reach this host. "
            "Set a Tailscale or LAN URL (e.g. http://macbook:8080)."
        )
    else:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host in ("127.0.0.1", "localhost", "::1"):
            warnings.append(
                f"instance_url is {url} (loopback) — remote peers cannot reach it. "
                "Use a Tailscale or LAN hostname."
            )
    if settings.host in ("127.0.0.1", "localhost"):
        warnings.append(
            f"PA_HOST={settings.host} — bind 0.0.0.0 so peers can connect."
        )
    if not settings.sync_token:
        warnings.append("No sync token yet — one will be generated on first join/install.")
    return warnings


def ensure_sync_token(settings: Settings) -> str:
    """Return a sync token, generating and persisting one if missing."""
    if settings.sync_token:
        return settings.sync_token
    token = secrets.token_hex(32)
    update_instance_config(settings.data_dir, sync_token=token)
    settings.sync_token = token
    return token


def add_peer_url(settings: Settings, peer_url: str) -> list[str]:
    """Persist peer URL into config.json and in-memory settings.peers."""
    url = peer_url.rstrip("/")
    peers = [p.rstrip("/") for p in settings.peers]
    if url and url not in peers:
        peers.append(url)
    update_instance_config(settings.data_dir, peers=peers)
    settings.peers = peers
    return peers


def remove_peer_url(settings: Settings, peer_url: str) -> list[str]:
    url = peer_url.rstrip("/")
    peers = [p.rstrip("/") for p in settings.peers if p.rstrip("/") != url]
    update_instance_config(settings.data_dir, peers=peers)
    settings.peers = peers
    return peers


def refresh_service_env(settings: Settings) -> bool:
    """Rewrite host service unit env from current settings. Returns True if applied."""
    try:
        from pa.cli import service as svc

        status = svc.status()
        if not status.installed:
            return False
        svc.install_service(settings)
        return True
    except Exception:
        return False


def apply_join_response(
    data_dir: Path,
    *,
    fleet_id: str,
    owner_url: str,
    owner_instance: dict | None = None,
    subscribed_realms: list[str] | None = None,
    sync_token: str | None = None,
    peers: list[str] | None = None,
) -> InstanceConfig:
    """Persist fleet join results into config.json."""
    peer_list: list[str] = []
    if owner_url:
        peer_list.append(owner_url.rstrip("/"))
    if peers:
        for peer in peers:
            p = peer.rstrip("/")
            if p and p not in peer_list:
                peer_list.append(p)
    config = load_instance_config(data_dir)
    if config:
        for peer in config.peers:
            p = peer.rstrip("/")
            if p and p not in peer_list:
                peer_list.append(p)
    updates: dict = {
        "fleet_id": fleet_id,
        "fleet_owner_url": owner_url.rstrip("/") if owner_url else "",
    }
    if peer_list:
        updates["peers"] = peer_list
    if subscribed_realms:
        updates["subscribed_realms"] = subscribed_realms
    if sync_token:
        updates["sync_token"] = sync_token
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


def unwire_instance_peers(peer_table: PeerTable, *, instance_id: str = "", url: str = "") -> None:
    """Remove peer routes for a removed fleet instance."""
    target_url = url.rstrip("/") if url else ""
    remaining = []
    for route in peer_table.all_routes():
        if instance_id and route.target_instance_id == instance_id:
            continue
        if target_url and route.target_url.rstrip("/") == target_url:
            continue
        remaining.append(route)
    peer_table.set_routes(remaining)


def register_joiner_on_owner(
    fleet: FleetRegistry,
    peer_table: PeerTable,
    settings: Settings,
    *,
    joiner_id: str,
    name: str,
    url: str,
    zone: str = "default",
    capabilities: list[str] | None = None,
    realms: list[str] | None = None,
) -> tuple[FleetInstance, str]:
    """Register joiner, wire peers/sync, return (instance, sync_token)."""
    realm_list = list(realms if realms is not None else settings.subscribed_realms)
    sync_token = ensure_sync_token(settings)
    inst = FleetInstance(
        instance_id=joiner_id,
        name=name,
        url=url.rstrip("/"),
        zone=zone,
        capabilities=capabilities or [],
        healthy=True,
    )
    fleet.upsert_instance(inst)
    wire_owner_peers(peer_table, realms=realm_list, joiner=inst)
    add_peer_url(settings, inst.url)
    return inst, sync_token
