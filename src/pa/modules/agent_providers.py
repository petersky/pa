"""REST + MCP APIs for ACP provider install/status/configure on this host."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pa.acp.providers.base import ProviderConfigureBody
from pa.acp.providers.registry import get_provider
from pa.acp.providers.resolve import list_provider_summaries
from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.domain.instance_config import update_instance_config

router = APIRouter(prefix="/agent/providers")


class ConfigureBody(BaseModel):
    env: dict[str, str] = Field(default_factory=dict)
    secrets: dict[str, str] = Field(default_factory=dict)
    no_browser: bool | None = None
    codex_path: str | None = None
    initial_agent_mode: str | None = None


class InstanceProviderBody(BaseModel):
    """Set the instance-default ACP provider (persisted to config.json)."""

    provider: str


def _data_dir(request: Request):
    return request.app.state.ctx.settings.data_dir


@router.get("")
def list_local_providers(request: Request) -> list[dict]:
    return list_provider_summaries(_data_dir(request))


@router.get("/{provider_id}")
def get_local_provider(request: Request, provider_id: str) -> dict:
    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return provider.status(_data_dir(request)).model_dump(mode="json")


@router.post("/{provider_id}/install")
def install_provider(request: Request, provider_id: str) -> dict:
    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    result = provider.install(_data_dir(request))
    return result.model_dump(mode="json")


@router.post("/{provider_id}/update")
def update_provider(request: Request, provider_id: str) -> dict:
    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    result = provider.update(_data_dir(request))
    return result.model_dump(mode="json")


@router.post("/{provider_id}/configure")
def configure_provider(request: Request, provider_id: str, body: ConfigureBody) -> dict:
    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    status = provider.configure(
        _data_dir(request),
        ProviderConfigureBody(
            env=body.env,
            secrets=body.secrets,
            no_browser=body.no_browser,
            codex_path=body.codex_path,
            initial_agent_mode=body.initial_agent_mode,
        ),
    )
    return status.model_dump(mode="json")


@router.post("/{provider_id}/probe")
def probe_provider(request: Request, provider_id: str) -> dict:
    try:
        provider = get_provider(provider_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return provider.probe(_data_dir(request))


@router.put("/default")
def set_default_provider(request: Request, body: InstanceProviderBody) -> dict:
    try:
        get_provider(body.provider)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    settings = request.app.state.ctx.settings
    update_instance_config(settings.data_dir, agent_provider=body.provider)
    settings.agent_provider = body.provider
    return {"agent_provider": body.provider}


class AgentProvidersModule(Module):
    @property
    def name(self) -> str:
        return "agent_providers"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "ACP provider install, configure, update, and probe APIs"

    def api_routers(self):
        return [("/api", router, ["agent-providers"])]

    def register_mcp(self, mcp, ctx: AppContext) -> None:
        settings = ctx.settings

        @mcp.tool()
        def agent_providers_list(instance_id: str | None = None) -> list[dict]:
            """List ACP providers and install status on this host or a fleet peer."""
            if instance_id:
                return _fleet_proxy(ctx, instance_id, "GET", "/api/agent/providers")
            return list_provider_summaries(settings.data_dir)

        @mcp.tool()
        def agent_provider_status(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Status for one ACP provider (cursor, codex, …)."""
            if instance_id:
                return _fleet_proxy(
                    ctx,
                    instance_id,
                    "GET",
                    f"/api/agent/providers/{provider_id}",
                )
            return get_provider(provider_id).status(settings.data_dir).model_dump(
                mode="json"
            )

        @mcp.tool()
        def agent_provider_install(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Install or verify an ACP provider on this host or a fleet peer."""
            if instance_id:
                return _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/install",
                )
            return get_provider(provider_id).install(settings.data_dir).model_dump(
                mode="json"
            )

        @mcp.tool()
        def agent_provider_update(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Update an ACP provider package/binary."""
            if instance_id:
                return _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/update",
                )
            return get_provider(provider_id).update(settings.data_dir).model_dump(
                mode="json"
            )

        @mcp.tool()
        def agent_provider_configure(
            provider_id: str,
            env: dict[str, str] | None = None,
            secrets: dict[str, str] | None = None,
            no_browser: bool | None = None,
            instance_id: str | None = None,
        ) -> dict:
            """Configure provider env/secrets (secrets stay on the target host)."""
            body = {
                "env": env or {},
                "secrets": secrets or {},
                "no_browser": no_browser,
            }
            if instance_id:
                return _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/configure",
                    json_body=body,
                )
            status = get_provider(provider_id).configure(
                settings.data_dir,
                ProviderConfigureBody(
                    env=body["env"],
                    secrets=body["secrets"],
                    no_browser=no_browser,
                ),
            )
            return status.model_dump(mode="json")

        @mcp.tool()
        def agent_provider_probe(
            provider_id: str, instance_id: str | None = None
        ) -> dict:
            """Probe ACP initialize handshake for a provider."""
            if instance_id:
                return _fleet_proxy(
                    ctx,
                    instance_id,
                    "POST",
                    f"/api/agent/providers/{provider_id}/probe",
                )
            return get_provider(provider_id).probe(settings.data_dir)


def _fleet_proxy(
    ctx: AppContext,
    instance_id: str,
    method: str,
    path: str,
    json_body: dict[str, Any] | None = None,
) -> Any:
    import httpx

    from pa.fleet.registry import FleetRegistry

    fleet: FleetRegistry = ctx.require_service("fleet_registry")
    inst = None
    for candidate in fleet.list_instances():
        if candidate.instance_id == instance_id:
            inst = candidate
            break
    if not inst:
        raise ValueError(f"Unknown fleet instance: {instance_id}")
    settings = ctx.settings
    headers: dict[str, str] = {}
    if settings.sync_token:
        headers["Authorization"] = f"Bearer {settings.sync_token}"
    url = f"{inst.url.rstrip('/')}{path}"
    with httpx.Client(timeout=120.0) as client:
        resp = client.request(method, url, headers=headers, json=json_body)
        if resp.status_code >= 400:
            raise RuntimeError(f"{method} {path} → {resp.status_code}: {resp.text[:300]}")
        return resp.json()
