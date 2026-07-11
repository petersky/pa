from __future__ import annotations

from datetime import UTC, datetime

import httpx
from fastapi import APIRouter, HTTPException, Request

from pa.auth.middleware import get_principal_id, require_user
from pa.config import get_settings
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.core.ui.pages import PageDefinition, PageRegistry
from pa.domain.instance_config import InstanceConfig, save_instance_config
from pa.domain.models import FleetInstance, PeerRoute, RealmRole
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable

router = APIRouter()
ui_router = APIRouter()


def _refresh_fleet_health(fleet: FleetRegistry) -> None:
    for inst in fleet.list_instances():
        healthy = False
        try:
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(f"{inst.url.rstrip('/')}/api/health")
                healthy = resp.status_code == 200
        except httpx.HTTPError:
            pass
        inst.healthy = healthy
        inst.last_seen = datetime.now(UTC)
        fleet.upsert_instance(inst)


def _fleet_context(request: Request) -> dict:
    ctx = request.app.state.ctx
    settings = ctx.settings
    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    _refresh_fleet_health(fleet)
    membership: MembershipStore = ctx.require_service("membership")
    peer_table: PeerTable = ctx.require_service("peer_table")
    provider_status: dict[str, list] = {}
    for inst in fleet.list_instances():
        if not inst.healthy:
            provider_status[inst.instance_id] = []
            continue
        try:
            headers = {}
            if settings.sync_token:
                headers["Authorization"] = f"Bearer {settings.sync_token}"
            with httpx.Client(timeout=5.0) as client:
                resp = client.get(
                    f"{inst.url.rstrip('/')}/api/agent/providers", headers=headers
                )
                if resp.status_code == 200:
                    provider_status[inst.instance_id] = resp.json()
                else:
                    provider_status[inst.instance_id] = []
        except httpx.HTTPError:
            provider_status[inst.instance_id] = []
    return {
        "fleet_instances": fleet.list_instances(),
        "realms": membership.list_realms(),
        "memberships": membership.list_memberships(),
        "peer_routes": peer_table.all_routes(),
        "settings": settings,
        "fleet_id": settings.fleet_id,
        "zone": settings.zone,
        "provider_status": provider_status,
    }


@router.get("/fleet/instances")
def list_fleet_instances(request: Request) -> list[dict]:
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    return [i.model_dump(mode="json") for i in fleet.list_instances()]


@router.post("/fleet/join")
async def fleet_join(request: Request, body: dict) -> dict:
    token = body.get("token", "")
    joiner_id = body.get("instance_id", "")
    name = body.get("name", "remote")
    url = body.get("url", "")
    zone = body.get("zone", "default")
    capabilities = body.get("capabilities", [])
    if not token or not joiner_id:
        raise HTTPException(status_code=400, detail="token and instance_id required")

    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    join = fleet.consume_join_token(token)
    if not join:
        raise HTTPException(status_code=400, detail="Invalid or expired join token")

    settings = get_settings()
    owner_url = settings.instance_url or f"http://{settings.host}:{settings.port}"
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    realms = list(settings.subscribed_realms)

    from pa.fleet.join import register_joiner_on_owner

    inst = register_joiner_on_owner(
        fleet,
        peer_table,
        joiner_id=joiner_id,
        name=name,
        url=url or owner_url,
        zone=zone,
        capabilities=capabilities,
        realms=realms,
    )
    owner_inst = fleet.get_instance(settings.instance_id)
    return {
        "fleet_id": join.fleet_id,
        "owner_url": owner_url.rstrip("/"),
        "owner_instance": owner_inst.model_dump(mode="json") if owner_inst else None,
        "subscribed_realms": realms,
        "instance": inst.model_dump(mode="json"),
    }


@router.post("/fleet/join-token")
def create_join_token(request: Request) -> dict:
    require_user(request)
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    principal = get_principal_id(request)
    join = fleet.create_join_token(created_by=principal)
    return {
        "token": join.token,
        "expires_at": join.expires_at.isoformat(),
        "fleet_id": join.fleet_id,
    }


@router.post("/fleet/register-remote")
async def register_remote(request: Request, body: dict) -> dict:
    require_user(request)
    inst = FleetInstance.model_validate(body)
    if inst.url.lower().startswith(("javascript:", "data:", "vbscript:")):
        raise HTTPException(status_code=400, detail="Invalid instance URL scheme")
    inst.last_seen = datetime.now(UTC)
    inst.healthy = True
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    fleet.upsert_instance(inst)
    peer_table: PeerTable = request.app.state.ctx.require_service("peer_table")
    for realm in request.app.state.ctx.settings.subscribed_realms:
        peer_table.add_route(
            PeerRoute(realm_id=realm, target_url=inst.url, target_instance_id=inst.instance_id, zone=inst.zone)
        )
    return inst.model_dump(mode="json")


@router.delete("/fleet/instances/{instance_id}")
def remove_instance(request: Request, instance_id: str) -> dict:
    require_user(request)
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    if not fleet.remove_instance(instance_id):
        raise HTTPException(status_code=404, detail="Instance not found")
    return {"removed": instance_id}


@router.get("/fleet/health")
async def fleet_health(request: Request) -> list[dict]:
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    results = []
    async with httpx.AsyncClient(timeout=5.0) as client:
        for inst in fleet.list_instances():
            healthy = False
            try:
                resp = await client.get(f"{inst.url.rstrip('/')}/api/health")
                healthy = resp.status_code == 200
            except httpx.HTTPError:
                pass
            inst.healthy = healthy
            inst.last_seen = datetime.now(UTC)
            fleet.upsert_instance(inst)
            results.append(inst.model_dump(mode="json"))
    return results


@router.get("/realms")
def list_realms(request: Request) -> list[dict]:
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    return [r.model_dump() for r in membership.list_realms()]


@router.post("/realms")
def create_realm(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    realm_id = body.get("id", "")
    if not realm_id:
        raise HTTPException(status_code=400, detail="realm id required")
    realm = membership.ensure_realm(realm_id, body.get("name", ""))
    principal = get_principal_id(request)
    uid = principal[5:] if principal.startswith("user:") else "local"
    membership.ensure_owner_membership(realm_id, uid, fleet_id=request.app.state.ctx.settings.fleet_id)
    return realm.model_dump()


@router.post("/realms/invite")
def realm_invite(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    realm_id = body.get("realm_id", request.app.state.ctx.settings.primary_realm)
    role = RealmRole(body.get("role", "editor"))
    invite = membership.create_invite(realm_id, role, created_by=get_principal_id(request))
    return {
        "token": invite.token,
        "realm_id": invite.realm_id,
        "role": invite.role.value,
        "expires_at": invite.expires_at.isoformat() if invite.expires_at else None,
    }


@router.post("/realms/accept-invite")
def accept_invite(request: Request, body: dict) -> dict:
    require_user(request)
    membership: MembershipStore = request.app.state.ctx.require_service("membership")
    token = body.get("token", "")
    principal = get_principal_id(request)
    uid = principal[5:] if principal.startswith("user:") else "local"
    m = membership.accept_invite(token, uid, fleet_id=request.app.state.ctx.settings.fleet_id)
    if not m:
        raise HTTPException(status_code=400, detail="Invalid invite")
    return m.model_dump(mode="json")


def _fleet_instance_or_404(request: Request, instance_id: str):
    fleet: FleetRegistry = request.app.state.ctx.require_service("fleet_registry")
    for inst in fleet.list_instances():
        if inst.instance_id == instance_id:
            return inst
    raise HTTPException(status_code=404, detail="Fleet instance not found")


def _proxy_agent_providers(
    request: Request,
    instance_id: str,
    method: str,
    suffix: str,
    body: dict | None = None,
) -> dict | list:
    require_user(request)
    inst = _fleet_instance_or_404(request, instance_id)
    settings = request.app.state.ctx.settings
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}/api/agent/providers{suffix}"
    try:
        with httpx.Client(timeout=120.0) as client:
            resp = client.request(method, url, headers=headers, json=body)
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=502, detail=f"Peer unreachable: {exc}") from exc
    if resp.status_code >= 400:
        raise HTTPException(status_code=resp.status_code, detail=resp.text[:500])
    return resp.json()


@router.get("/fleet/instances/{instance_id}/agent-providers")
def fleet_agent_providers(request: Request, instance_id: str):
    return _proxy_agent_providers(request, instance_id, "GET", "")


@router.get("/fleet/instances/{instance_id}/agent-providers/{provider_id}")
def fleet_agent_provider(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(request, instance_id, "GET", f"/{provider_id}")


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/install")
def fleet_agent_provider_install(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/install"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/update")
def fleet_agent_provider_update(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/update"
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/configure")
async def fleet_agent_provider_configure(
    request: Request, instance_id: str, provider_id: str, body: dict
):
    return _proxy_agent_providers(
        request, instance_id, "POST", f"/{provider_id}/configure", body=body
    )


@router.post("/fleet/instances/{instance_id}/agent-providers/{provider_id}/probe")
def fleet_agent_provider_probe(request: Request, instance_id: str, provider_id: str):
    return _proxy_agent_providers(request, instance_id, "POST", f"/{provider_id}/probe")


@ui_router.get("/fleet")
def fleet_page(request: Request):
    from fastapi.responses import HTMLResponse
    from pa.modules.ui_shell import render_page

    page = request.app.state.ctx.require_service("pages").get_by_path("/fleet")
    if not page:
        raise HTTPException(status_code=404)
    return render_page(request, page)


class FleetModule(Module):
    @property
    def name(self) -> str:
        return "fleet"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Fleet management, realms, and membership"

    def on_load(self, ctx: AppContext) -> None:
        settings = ctx.settings
        from pa.sync.infrastructure import get_membership_store, get_peer_table

        fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
        fleet.register_self(
            settings.instance_id,
            settings.instance_name,
            f"http://{settings.host}:{settings.port}",
            zone=settings.zone,
            capabilities=settings.capabilities,
            relay_enabled=settings.relay_enabled,
        )
        ctx.register_service("fleet_registry", fleet)
        membership = get_membership_store(settings)
        for realm in settings.subscribed_realms:
            membership.ensure_realm(realm)
            membership.ensure_owner_membership(realm, "local", fleet_id=settings.fleet_id)
        ctx.register_service("membership", membership)
        peer_table = get_peer_table(settings)
        for realm in settings.subscribed_realms:
            peer_table.sync_from_settings_peers(realm, settings.peers, settings.zone)
        ctx.register_service("peer_table", peer_table)

        pages: PageRegistry = ctx.require_service("pages")
        pages.register(
            PageDefinition(
                id="fleet",
                path="/fleet",
                label="Fleet",
                icon="fleet",
                template="pages/fleet.html",
                nav_order=50,
                context_builder=_fleet_context,
            )
        )

    def api_routers(self):
        return [("/api", router, ["fleet"])]

    def ui_routers(self):
        return [ui_router]
