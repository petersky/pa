from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.integrations.base import ExternalRef, ExternalSystem, SyncBinding, SyncDirection
from pa.integrations.registry import IntegrationsRegistry

router = APIRouter()


@router.get("/integrations")
def list_integrations(request: Request, realm: str | None = None) -> dict:
    registry: IntegrationsRegistry = request.app.state.ctx.require_service("integrations_registry")
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return {
        "systems": registry.list_systems(),
        "bindings": [b.model_dump(mode="json") for b in registry.list_bindings(realm_id)],
    }


@router.post("/integrations/bindings", status_code=201)
async def create_binding(request: Request, body: dict) -> dict:
    registry: IntegrationsRegistry = request.app.state.ctx.require_service("integrations_registry")
    hooks = request.app.state.ctx.hooks
    realm_id = body.get("realm_id", request.app.state.ctx.settings.primary_realm)
    try:
        system = ExternalSystem(body["system"])
    except (KeyError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid system") from exc
    external_ref = ExternalRef(
        system=system,
        external_id=body.get("external_id", ""),
        url=body.get("url"),
    )
    binding = SyncBinding(
        realm_id=realm_id,
        pa_type=body.get("pa_type", "card"),
        pa_id=body["pa_id"],
        external_ref=external_ref,
        direction=SyncDirection(body.get("direction", "bidirectional")),
        field_map=body.get("field_map", {}),
    )
    runtime = request.app.state.ctx.require_service("async_runtime")
    await runtime.run_blocking(
        "integration.binding_write", registry.add_binding, binding
    )
    await hooks.emit("integration.binding.created", binding=binding.model_dump(mode="json"))
    return binding.model_dump(mode="json")


@router.post("/integrations/sync/{binding_id}")
async def sync_binding(request: Request, binding_id: str) -> dict:
    registry: IntegrationsRegistry = request.app.state.ctx.require_service("integrations_registry")
    binding = registry.get_binding(binding_id)
    if not binding:
        raise HTTPException(status_code=404, detail="Binding not found")
    if registry.is_stub(binding.external_ref.system):
        raise HTTPException(
            status_code=501,
            detail=f"Connector {binding.external_ref.system.value} is a stub and cannot sync yet",
        )
    await request.app.state.ctx.hooks.emit(
        "integration.sync.requested",
        binding_id=binding_id,
    )
    data = await registry.pull_binding(binding)
    return {"binding_id": binding_id, "data": data}


class IntegrationsModule(Module):
    @property
    def name(self) -> str:
        return "integrations"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "External system connectors and sync bindings (scaffold)"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service(
            "integrations_registry",
            IntegrationsRegistry(ctx.settings.data_dir),
        )

    def api_routers(self):
        return [("/api", router, ["integrations"])]
