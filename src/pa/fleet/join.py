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


def readiness_issues(settings: Settings) -> list[dict]:
    """Structured readiness problems with fix hints and UI actions."""
    issues: list[dict] = []
    url = (settings.instance_url or "").rstrip("/")
    if not url:
        issues.append(
            {
                "id": "missing_instance_url",
                "message": "Advertised URL is not set — peers cannot reliably reach this host.",
                "fix": (
                    "Set a Tailscale or LAN URL that other machines can open "
                    "(e.g. http://macbook:8080). Use your Tailscale hostname, not localhost."
                ),
                "action": "set_instance_url",
            }
        )
    else:
        parsed = urlparse(url)
        host = (parsed.hostname or "").lower()
        if host in ("127.0.0.1", "localhost", "::1"):
            issues.append(
                {
                    "id": "loopback_instance_url",
                    "message": f"Advertised URL is {url} (loopback) — remote peers cannot reach it.",
                    "fix": (
                        "Replace it with a Tailscale or LAN hostname URL "
                        "(e.g. http://macbook:8080)."
                    ),
                    "action": "set_instance_url",
                }
            )
    if settings.host in ("127.0.0.1", "localhost"):
        issues.append(
            {
                "id": "loopback_bind",
                "message": f"Server is bound to {settings.host} — only this machine can connect.",
                "fix": (
                    "Bind to 0.0.0.0 so Tailscale/LAN peers can reach PA. "
                    "Saving this updates config and restarts the service so the new bind takes effect."
                ),
                "action": "set_bind_all",
            }
        )
    if not settings.sync_token:
        issues.append(
            {
                "id": "missing_sync_token",
                "message": "No sync token yet — peers need a shared secret for sync APIs.",
                "fix": "Generate a sync token here (or it will be created automatically on first join/install).",
                "action": "ensure_sync_token",
            }
        )
    return issues


def readiness_warnings(settings: Settings) -> list[str]:
    """Human-readable warning strings (CLI / legacy)."""
    return [f"{i['message']} {i['fix']}" for i in readiness_issues(settings)]


def apply_reachability_settings(
    settings: Settings,
    *,
    instance_url: str | None = None,
    host: str | None = None,
) -> dict:
    """Persist reachability settings and update in-memory Settings.

    Returns ``{"restart_required": bool, "service_refreshed": bool}``.
    """
    updates: dict = {}
    restart_required = False

    if instance_url is not None:
        url = instance_url.strip().rstrip("/")
        if url:
            parsed = urlparse(url)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise ValueError("instance_url must be an http(s) URL like http://macbook:8080")
            if (parsed.hostname or "").lower() in ("127.0.0.1", "localhost", "::1"):
                raise ValueError(
                    "instance_url cannot be localhost/127.0.0.1 — use a Tailscale or LAN hostname"
                )
        updates["instance_url"] = url
        settings.instance_url = url

    if host is not None:
        bind = host.strip()
        if not bind:
            raise ValueError("host cannot be empty")
        if bind.lower() in ("localhost",):
            bind = "127.0.0.1"
        allowed = {"0.0.0.0", "127.0.0.1", "::", "::1"}
        # Allow hostname-like binds and IPv4/IPv6 literals; reject junk schemes.
        if "://" in bind or " " in bind:
            raise ValueError("host must be a bind address like 0.0.0.0 or 127.0.0.1")
        if bind not in allowed and not all(c.isalnum() or c in ".-:[]" for c in bind):
            raise ValueError(f"invalid bind host: {bind}")
        if bind != settings.host:
            restart_required = True
        updates["host"] = bind
        settings.host = bind

    if updates:
        update_instance_config(settings.data_dir, **updates)

    service_refreshed = refresh_service_env(settings)
    return {
        "restart_required": restart_required,
        "service_refreshed": service_refreshed,
        "instance_url": settings.instance_url,
        "host": settings.host,
        "owner_url": owner_public_url(settings),
        "warnings": readiness_warnings(settings),
        "issues": readiness_issues(settings),
    }


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
        "token": token.strip(),
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
        if resp.status_code >= 400:
            detail = ""
            try:
                data = resp.json()
                detail = data.get("detail") or ""
                if isinstance(detail, list):
                    detail = "; ".join(str(x) for x in detail)
            except Exception:
                detail = (resp.text or "")[:300]
            msg = f"Fleet join failed ({resp.status_code})"
            if detail:
                msg = f"{msg}: {detail}"
            raise httpx.HTTPStatusError(msg, request=resp.request, response=resp)
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
