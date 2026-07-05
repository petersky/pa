from __future__ import annotations

from fastapi import APIRouter, Request

from pa.core.contracts import Module
from pa.core.context import AppContext
from pa.domain.models import RealmGrant
from pa.trust.hooks import CommitSigner, FederationHooks, GrantStore, OIDCConfig, RealmEncryption

router = APIRouter()


@router.get("/trust/grants")
def list_grants(request: Request, realm: str | None = None) -> list[dict]:
    grants: GrantStore = request.app.state.ctx.require_service("grant_store")
    realm_id = realm or request.app.state.ctx.settings.primary_realm
    return [g.model_dump(mode="json") for g in grants.list_for_realm(realm_id)]


@router.post("/trust/grants")
def create_grant(request: Request, body: dict) -> dict:
    grants: GrantStore = request.app.state.ctx.require_service("grant_store")
    grant = RealmGrant.model_validate(body)
    grants.add(grant)
    return grant.model_dump(mode="json")


@router.get("/trust/oidc/status")
def oidc_status(request: Request) -> dict:
    oidc: OIDCConfig = request.app.state.ctx.require_service("oidc_config")
    return {"enabled": oidc.enabled, "issuer": oidc.issuer or None}


class TrustModule(Module):
    @property
    def name(self) -> str:
        return "trust"

    @property
    def version(self) -> str:
        return "0.1.0"

    @property
    def description(self) -> str:
        return "Cross-realm grants, OIDC hooks, encryption, and federation extension points"

    def on_load(self, ctx: AppContext) -> None:
        settings = ctx.settings
        ctx.register_service("grant_store", GrantStore(settings.data_dir))
        ctx.register_service(
            "oidc_config",
            OIDCConfig(settings.oidc_issuer, settings.oidc_client_id, settings.oidc_client_secret),
        )
        ctx.register_service("realm_encryption", RealmEncryption(settings.data_dir))
        ctx.register_service("commit_signer", CommitSigner())
        ctx.register_service("federation_hooks", FederationHooks())

    def api_routers(self):
        return [("/api", router, ["trust"])]
