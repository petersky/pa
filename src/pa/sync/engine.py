"""Sync engine — push/pull between peers."""

from __future__ import annotations

import asyncio
import logging
from typing import Callable

import httpx

from pa.config import Settings
from pa.domain.models import PeerRouteMode
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore

logger = logging.getLogger(__name__)


class SyncEngine:
    def __init__(
        self,
        settings: Settings,
        object_store: ObjectStore,
        event_log: EventLog,
        peer_table: PeerTable,
        membership: MembershipStore,
    ) -> None:
        self.settings = settings
        self.store = object_store
        self.log = event_log
        self.peer_table = peer_table
        self.membership = membership
        self._push_callbacks: list[Callable] = []
        self._debounce_tasks: dict[str, asyncio.Task] = {}

    def on_commit(self, callback: Callable) -> None:
        self._push_callbacks.append(callback)

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        return headers

    async def notify_commit(self, realm_id: str) -> None:
        if realm_id in self._debounce_tasks:
            self._debounce_tasks[realm_id].cancel()
        self._debounce_tasks[realm_id] = asyncio.create_task(self._debounced_push(realm_id))

    async def _debounced_push(self, realm_id: str) -> None:
        await asyncio.sleep(0.5)
        await self.push_to_peers(realm_id)

    async def push_to_peers(self, realm_id: str) -> None:
        head = self.log.get_head(realm_id)
        if not head:
            return
        objects = self._collect_objects(head)
        routes = self.peer_table.prefer_same_zone(realm_id, self.settings.zone)
        async with httpx.AsyncClient(timeout=15.0) as client:
            for route in routes:
                try:
                    if route.mode == PeerRouteMode.RELAY and route.relay_instance_id:
                        relay = self.peer_table.routes_for_realm(realm_id)
                        relay_url = next(
                            (r.target_url for r in relay if r.target_instance_id == route.relay_instance_id),
                            None,
                        )
                        if relay_url:
                            await client.post(
                                f"{relay_url}/api/sync/relay",
                                json={
                                    "realm_id": realm_id,
                                    "target_url": route.target_url,
                                    "head_hash": head,
                                    "objects": objects,
                                },
                                headers=self._headers(),
                            )
                    else:
                        await client.post(
                            f"{route.target_url}/api/sync/push",
                            json={
                                "realm_id": realm_id,
                                "head_hash": head,
                                "objects": objects,
                            },
                            headers=self._headers(),
                        )
                except httpx.HTTPError as exc:
                    logger.warning("Sync push to %s failed: %s", route.target_url, exc)

    def _collect_objects(self, head_hash: str) -> dict[str, str]:
        import base64

        objects: dict[str, str] = {}
        seen: set[str] = set()

        def walk(commit_hash: str) -> None:
            if commit_hash in seen:
                return
            seen.add(commit_hash)
            data = self.store.get(commit_hash)
            if data:
                objects[commit_hash] = base64.b64encode(data).decode()
            commit = self.log.get_commit(commit_hash)
            if not commit:
                return
            for eh in commit.event_hashes:
                if eh not in seen:
                    seen.add(eh)
                    edata = self.store.get(eh)
                    if edata:
                        objects[eh] = base64.b64encode(edata).decode()
            for parent in commit.parent_hashes:
                walk(parent)

        walk(head_hash)
        return objects

    async def pull_from_peer(self, realm_id: str, peer_url: str) -> str | None:
        """Pull missing objects from peer. Returns peer head hash if advanced."""
        base = peer_url.rstrip("/")
        async with httpx.AsyncClient(timeout=15.0) as client:
            local_hashes = set(self.store.list_hashes())
            try:
                resp = await client.post(
                    f"{base}/api/sync/have",
                    json={"realm_id": realm_id, "hashes": list(local_hashes)},
                    headers=self._headers(),
                )
                resp.raise_for_status()
                missing = resp.json().get("missing", [])
                if not missing:
                    return None
                resp = await client.post(
                    f"{base}/api/sync/get",
                    json={"hashes": missing},
                    headers=self._headers(),
                )
                resp.raise_for_status()
                self.ingest_objects(resp.json().get("objects", {}))

                refs_resp = await client.get(
                    f"{base}/api/sync/refs?realm={realm_id}",
                    headers=self._headers(),
                )
                refs_resp.raise_for_status()
                peer_head = None
                for ref in refs_resp.json():
                    if ref.get("realm_id") == realm_id:
                        peer_head = ref.get("head_hash")
                        break
                if not peer_head:
                    return None
                local_head = self.log.get_head(realm_id)
                if local_head == peer_head:
                    return None
                if not local_head or self.log.is_ancestor(local_head, peer_head):
                    self.log.advance_ref(realm_id, peer_head)
                    return peer_head
            except httpx.HTTPError as exc:
                logger.warning("Sync pull from %s failed: %s", peer_url, exc)
        return None

    def ingest_objects(self, objects_b64: dict[str, str]) -> list[str]:
        import base64

        imported: list[str] = []
        for h, b64 in objects_b64.items():
            if self.store.has(h):
                continue
            self.store.put(base64.b64decode(b64))
            imported.append(h)
        return imported

    async def anti_entropy(self, realm_id: str) -> bool:
        routes = self.peer_table.routes_for_realm(realm_id)
        results = await asyncio.gather(
            *(self.pull_from_peer(realm_id, r.target_url) for r in routes),
            return_exceptions=True,
        )
        return any(isinstance(result, str) and result for result in results)

    def status(self, realm_id: str) -> dict:
        head = self.log.get_head(realm_id)
        routes = self.peer_table.routes_for_realm(realm_id)
        return {
            "realm_id": realm_id,
            "head": head,
            "object_count": len(self.store.list_hashes()),
            "peer_count": len(routes),
            "zone": self.settings.zone,
        }
