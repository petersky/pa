"""Route agent execution to local or remote instances."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import TYPE_CHECKING

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.domain.models import FleetInstance
from pa.execution.lease import LeaseManager
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime


class ExecutionRouter:
    def __init__(
        self,
        settings: Settings,
        lease_manager: LeaseManager,
        fleet_registry: FleetRegistry,
        peer_table: PeerTable,
        users: UserDirectory,
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.settings = settings
        self.leases = lease_manager
        self.fleet = fleet_registry
        self.peer_table = peer_table
        self.users = users
        self.async_runtime = async_runtime
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3.0, read=120.0, write=15.0, pool=2.0),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def close(self) -> None:
        await self._client.aclose()

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
        lease_held = False
        if card_id:
            granted = await self._offload(
                "execution.lease_grant",
                self.leases.grant,
                card_id,
                realm_id,
                holder_instance=self.settings.instance_id,
                holder_principal=principal_id,
            )
            if not granted:
                target = await self._resolve_target(
                    card_id, realm_id, target_instance_id
                )
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

            cwd = await self._offload(
                "execution.user_workspace",
                self._user_data_dir,
                principal_id,
            )
            return await local_agent.prompt(
                message,
                item_id=card_id,
                principal_id=principal_id,
                project_id=project_id,
                agent_env=self._user_env(principal_id),
                cwd=cwd,
                surface="execution",
            )
        finally:
            if card_id and lease_held:
                release = asyncio.create_task(
                    self._offload(
                        "execution.lease_release",
                        self.leases.release,
                        card_id,
                        realm_id,
                        principal_id=principal_id,
                        holder_instance=self.settings.instance_id,
                    )
                )
                try:
                    await asyncio.shield(release)
                except asyncio.CancelledError:
                    # Lease fencing is stronger than prompt cancellation: the
                    # caller still observes cancellation, but only after the
                    # durable release has completed off-loop.
                    await release
                    raise

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
        card = await self._offload(
            "sqlite.card_read",
            self.leases.store.get_card,
            card_id,
            realm_id=realm_id,
        )
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

        headers["Content-Type"] = "application/json"
        payload = await self._offload(
            "execution.remote_json_encode",
            lambda: json.dumps(
                {
                    "message": message,
                    "item_id": card_id,
                    "card_id": card_id,
                    "project_id": project_id,
                    "realm_id": realm_id,
                    "principal_id": principal_id,
                    "target_instance_id": target_instance_id,
                },
                separators=(",", ":"),
            ).encode(),
        )
        request = self._client.post(
            f"{url.rstrip('/')}/api/agent/prompt",
            content=payload,
            headers=headers,
        )
        if self.async_runtime:
            resp = await self.async_runtime.observe(
                "execution.remote_http", request, timeout=125.0
            )
        else:
            resp = await request
        resp.raise_for_status()
        data = await self._offload("execution.remote_json_decode", resp.json)
        return data.get("stop_reason", "end_turn")
