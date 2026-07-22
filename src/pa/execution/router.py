"""Route agent execution to local or remote instances."""

from __future__ import annotations

import logging

import httpx

from pa.agent.context import augment_message_with_context
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.domain.models import FleetInstance
from pa.execution.lease import LeaseManager
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable

logger = logging.getLogger(__name__)


class ExecutionRouter:
    def __init__(
        self,
        settings: Settings,
        lease_manager: LeaseManager,
        fleet_registry: FleetRegistry,
        peer_table: PeerTable,
        users: UserDirectory,
    ) -> None:
        self.settings = settings
        self.leases = lease_manager
        self.fleet = fleet_registry
        self.peer_table = peer_table
        self.users = users

    def _user_env(self, principal_id: str) -> dict[str, str]:
        if principal_id.startswith("user:"):
            uid = principal_id[5:]
            user = self.users.get(uid)
            if user:
                return dict(user.agent_env)
        return {}

    def _user_data_dir(self, principal_id: str) -> str:
        assert self.settings.workspace_root is not None
        if principal_id.startswith("user:"):
            uid = principal_id[5:]
            path = self.settings.workspace_root / "users" / uid
            path.mkdir(parents=True, exist_ok=True)
            return str(path)
        path = self.settings.workspace_root / "users" / "local"
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    async def prompt(
        self,
        message: str,
        *,
        principal_id: str = "user:local",
        card_id: str | None = None,
        project_id: str | None = None,
        realm_id: str | None = None,
        target_instance_id: str | None = None,
        local_agent,
    ) -> str:
        realm_id = realm_id or self.settings.primary_realm
        message = augment_message_with_context(
            self.leases.store,
            message,
            card_id=card_id,
            project_id=project_id,
            realm_id=realm_id,
        )

        lease_held = False
        if card_id:
            if not self.leases.grant(
                card_id,
                realm_id,
                holder_instance=self.settings.instance_id,
                holder_principal=principal_id,
            ):
                target = await self._resolve_target(card_id, realm_id, target_instance_id)
                if target and target != self.settings.instance_id:
                    return await self._remote_prompt(
                        target,
                        message,
                        principal_id=principal_id,
                        card_id=card_id,
                        project_id=project_id,
                        realm_id=realm_id,
                    )
                raise RuntimeError(f"Card {card_id} is leased by another instance")
            lease_held = True

        try:
            target = await self._resolve_target(card_id, realm_id, target_instance_id)
            if target and target != self.settings.instance_id:
                return await self._remote_prompt(
                    target,
                    message,
                    principal_id=principal_id,
                    card_id=card_id,
                    project_id=project_id,
                    realm_id=realm_id,
                )

            return await local_agent.prompt(
                message,
                item_id=card_id,
                principal_id=principal_id,
                project_id=project_id,
                agent_env=self._user_env(principal_id),
                cwd=self._user_data_dir(principal_id),
                surface="execution",
            )
        finally:
            if card_id and lease_held:
                self.leases.release(
                    card_id,
                    realm_id,
                    principal_id=principal_id,
                    holder_instance=self.settings.instance_id,
                )

    async def _resolve_target(
        self,
        card_id: str | None,
        realm_id: str,
        explicit: str | None,
    ) -> str | None:
        if explicit:
            return explicit
        if not card_id:
            return None
        card = self.leases.store.get_card(card_id, realm_id=realm_id)
        if not card:
            return None
        if card.preferred_instance:
            return card.preferred_instance
        if card.preferred_capabilities:
            inst = self._find_capable_instance(card.preferred_capabilities)
            if inst:
                return inst.instance_id
        return None

    def _find_capable_instance(self, capabilities: list[str]) -> FleetInstance | None:
        for inst in self.fleet.list_instances():
            if all(c in inst.capabilities for c in capabilities):
                return inst
        return None

    async def _remote_prompt(
        self,
        target_instance_id: str,
        message: str,
        *,
        principal_id: str,
        card_id: str | None,
        project_id: str | None = None,
        realm_id: str | None = None,
    ) -> str:
        inst = self.fleet.get_instance(target_instance_id)
        url = inst.url if inst else None
        if not url:
            for route in self.peer_table.all_routes():
                if route.target_instance_id == target_instance_id:
                    url = route.target_url
                    break
        if not url:
            raise RuntimeError(f"Cannot resolve URL for instance {target_instance_id}")

        headers = {}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"

        async with httpx.AsyncClient(timeout=120.0) as client:
            resp = await client.post(
                f"{url.rstrip('/')}/api/agent/prompt",
                json={
                    "message": message,
                    "item_id": card_id,
                    "card_id": card_id,
                    "project_id": project_id,
                    "realm_id": realm_id,
                    "principal_id": principal_id,
                    "target_instance_id": target_instance_id,
                },
                headers=headers,
            )
            resp.raise_for_status()
            return resp.json().get("stop_reason", "end_turn")
