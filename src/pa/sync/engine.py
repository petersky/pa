"""Recoverable anti-entropy and realm-head convergence between peers."""

from __future__ import annotations

import asyncio
import base64
import logging
from datetime import UTC, datetime
from typing import Callable

import httpx

from pa.config import Settings
from pa.domain.models import PeerRoute, PeerRouteMode
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.object_store import ObjectStore

logger = logging.getLogger(__name__)


class SyncEngine:
    """Exchange objects, merge compatible histories, and track convergence."""

    def __init__(
        self,
        settings: Settings,
        object_store: ObjectStore,
        event_log: EventLog,
        peer_table: PeerTable,
        membership: MembershipStore,
        fleet_registry: FleetRegistry | None = None,
    ) -> None:
        self.settings = settings
        self.store = object_store
        self.log = event_log
        self.peer_table = peer_table
        self.membership = membership
        self.fleet_registry = fleet_registry
        self._push_callbacks: list[Callable] = []
        self._debounce_tasks: dict[str, asyncio.Task] = {}
        self._convergence_tasks: dict[str, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._states: dict[str, dict] = {}
        self._periodic_task: asyncio.Task | None = None
        self._rebuild_projection: Callable[[str], None] | None = None

    def on_commit(self, callback: Callable) -> None:
        self._push_callbacks.append(callback)

    def on_head_advanced(self, callback: Callable[[str], None]) -> None:
        self._rebuild_projection = callback

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.settings.sync_token:
            headers["Authorization"] = f"Bearer {self.settings.sync_token}"
        return headers

    def _now(self) -> str:
        return datetime.now(UTC).isoformat()

    def _instance(self, instance_id: str | None, url: str) -> dict[str, str]:
        registered = None
        if self.fleet_registry:
            if instance_id:
                registered = self.fleet_registry.get_instance(instance_id)
            if not registered:
                registered = next(
                    (
                        item
                        for item in self.fleet_registry.list_instances()
                        if item.url.rstrip("/") == url.rstrip("/")
                    ),
                    None,
                )
        resolved_id = instance_id or (registered.instance_id if registered else url)
        return {
            "instance_id": resolved_id,
            "name": registered.name if registered else resolved_id,
            "url": registered.url if registered else url,
        }

    def _local_instance(self) -> dict[str, str]:
        return {
            "instance_id": self.settings.instance_id,
            "name": self.settings.instance_name,
            "url": self.settings.instance_url,
        }

    def _set_state(self, realm_id: str, **updates) -> dict:
        state = self._states.setdefault(
            realm_id,
            {
                "realm_id": realm_id,
                "phase": "idle",
                "started_at": None,
                "updated_at": self._now(),
                "head": self.log.get_head(realm_id),
                "instances": [],
                "conflicts": [],
                "attempt": 0,
            },
        )
        state.update(updates)
        state["updated_at"] = self._now()
        return state

    def convergence_status(self, realm_id: str) -> dict:
        state = dict(self._states.get(realm_id) or {})
        if not state:
            state = self._set_state(realm_id)
        state["head"] = self.log.get_head(realm_id)
        state["running"] = bool(
            self._convergence_tasks.get(realm_id)
            and not self._convergence_tasks[realm_id].done()
        )
        return state

    async def notify_commit(self, realm_id: str) -> None:
        task = self._debounce_tasks.get(realm_id)
        if task and not task.done():
            task.cancel()
        self._debounce_tasks[realm_id] = asyncio.create_task(
            self._debounced_converge(realm_id)
        )

    async def _debounced_converge(self, realm_id: str) -> None:
        try:
            await asyncio.sleep(0.5)
            await self.converge_realm(realm_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Realm convergence failed for %s", realm_id)

    def request_convergence(self, realm_id: str) -> asyncio.Task:
        existing = self._convergence_tasks.get(realm_id)
        if existing and not existing.done():
            return existing
        task = asyncio.create_task(self.converge_realm(realm_id))
        self._convergence_tasks[realm_id] = task
        return task

    def start(self, interval_seconds: float = 10.0) -> None:
        if self._periodic_task and not self._periodic_task.done():
            return
        self._periodic_task = asyncio.create_task(
            self._periodic_anti_entropy(interval_seconds)
        )

    async def close(self) -> None:
        tasks = [
            *self._debounce_tasks.values(),
            *self._convergence_tasks.values(),
        ]
        if self._periodic_task:
            tasks.append(self._periodic_task)
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _periodic_anti_entropy(self, interval_seconds: float) -> None:
        while True:
            try:
                for realm_id in self.settings.subscribed_realms:
                    await self.converge_realm(realm_id)
                await asyncio.sleep(interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Periodic anti-entropy pass failed")
                await asyncio.sleep(interval_seconds)

    async def _fetch_peer(
        self, client: httpx.AsyncClient, realm_id: str, route: PeerRoute
    ) -> dict:
        base = route.target_url.rstrip("/")
        descriptor = self._instance(route.target_instance_id, base)
        try:
            have = await client.post(
                f"{base}/api/sync/have",
                json={"realm_id": realm_id, "hashes": self.store.list_hashes()},
                headers=self._headers(),
            )
            have.raise_for_status()
            missing = have.json().get("missing", [])
            imported = 0
            if missing:
                objects = await client.post(
                    f"{base}/api/sync/get",
                    json={"hashes": missing},
                    headers=self._headers(),
                )
                objects.raise_for_status()
                imported = len(self.ingest_objects(objects.json().get("objects", {})))
            refs = await client.get(
                f"{base}/api/sync/refs",
                params={"realm": realm_id},
                headers=self._headers(),
            )
            refs.raise_for_status()
            peer_ref = next(
                (ref for ref in refs.json() if ref.get("realm_id") == realm_id),
                None,
            )
            peer_head = peer_ref.get("head_hash") if peer_ref else None
            if peer_ref:
                descriptor = self._instance(peer_ref.get("instance_id"), base)
            if peer_head and not self.log.get_commit(peer_head):
                return {
                    **descriptor,
                    "status": "invalid_response",
                    "head": peer_head,
                    "imported": imported,
                    "error": "peer head object was not transferred",
                }
            return {
                **descriptor,
                "status": "reachable",
                "head": peer_head,
                "imported": imported,
            }
        except httpx.HTTPError as exc:
            logger.warning("Sync exchange with %s failed: %s", base, exc)
            return {
                **descriptor,
                "status": "unavailable",
                "head": None,
                "imported": 0,
                "error": str(exc),
            }
        except ValueError as exc:
            return {
                **descriptor,
                "status": "invalid_response",
                "head": None,
                "imported": 0,
                "error": str(exc),
            }

    def _source_name(self, source: dict | None) -> dict | None:
        if not source:
            return source
        result = dict(source)
        instance_id = result.get("instance_id")
        result["instance_name"] = self._instance(instance_id, instance_id or "")[
            "name"
        ]
        return result

    def _reconcile_remote_head(self, realm_id: str, remote_head: str) -> dict:
        for attempt in range(1, 4):
            local_head = self.log.get_head(realm_id)
            if local_head == remote_head:
                return {"advanced": False, "head": local_head, "attempts": attempt}
            try:
                if not local_head:
                    if not self.log.get_commit(remote_head):
                        return {"advanced": False, "missing_head": remote_head}
                    self.log.advance_ref(realm_id, remote_head, expected_head=None)
                    advanced_head = remote_head
                elif self.log.is_ancestor(local_head, remote_head):
                    self.log.advance_ref(
                        realm_id, remote_head, expected_head=local_head
                    )
                    advanced_head = remote_head
                elif self.log.is_ancestor(remote_head, local_head):
                    return {
                        "advanced": False,
                        "head": local_head,
                        "attempts": attempt,
                    }
                else:
                    compatible, health = self.log.compatible_histories(
                        local_head, remote_head
                    )
                    if not compatible:
                        conflicts = []
                        for conflict in health["conflicts"]:
                            conflicts.append(
                                {
                                    **conflict,
                                    "local": self._source_name(conflict.get("local")),
                                    "remote": self._source_name(conflict.get("remote")),
                                    "local_head": local_head,
                                    "remote_head": remote_head,
                                }
                            )
                        return {
                            "advanced": False,
                            "head": local_head,
                            "conflicts": conflicts,
                            "common_ancestors": health["common_ancestors"],
                            "attempts": attempt,
                        }
                    merge = self.log.merge_heads(
                        realm_id,
                        local_head,
                        remote_head,
                        "sync:auto",
                        expected_head=local_head,
                    )
                    advanced_head = merge.hash
                if self._rebuild_projection:
                    self._rebuild_projection(realm_id)
                return {
                    "advanced": True,
                    "head": advanced_head,
                    "attempts": attempt,
                }
            except StaleSyncHeadError:
                if attempt == 3:
                    return {
                        "advanced": False,
                        "head": self.log.get_head(realm_id),
                        "stale": True,
                        "attempts": attempt,
                    }
        return {"advanced": False, "head": self.log.get_head(realm_id)}

    async def _push_peer(
        self,
        client: httpx.AsyncClient,
        realm_id: str,
        route: PeerRoute,
        head: str,
    ) -> dict:
        descriptor = self._instance(route.target_instance_id, route.target_url)
        try:
            endpoint = f"{route.target_url.rstrip('/')}/api/sync/push"
            payload = {
                "realm_id": realm_id,
                "head_hash": head,
                "objects": self._collect_objects(head),
            }
            if route.mode == PeerRouteMode.RELAY and route.relay_instance_id:
                relay_route = next(
                    (
                        item
                        for item in self.peer_table.routes_for_realm(realm_id)
                        if item.target_instance_id == route.relay_instance_id
                    ),
                    None,
                )
                if not relay_route:
                    return {
                        **descriptor,
                        "status": "unavailable",
                        "head": None,
                        "error": "configured relay route is unavailable",
                    }
                endpoint = f"{relay_route.target_url.rstrip('/')}/api/sync/relay"
                payload = {**payload, "target_url": route.target_url}
            response = await client.post(
                endpoint,
                json=payload,
                headers=self._headers(),
            )
            data = response.json()
            if response.status_code >= 400:
                detail = data.get("detail", data)
                return {
                    **descriptor,
                    "status": "conflict"
                    if isinstance(detail, dict)
                    and detail.get("code") == "sync_conflict"
                    else "error",
                    "head": detail.get("local_head")
                    if isinstance(detail, dict)
                    else None,
                    "error": detail,
                }
            return {
                **descriptor,
                "status": "reachable",
                "head": data.get("head"),
            }
        except httpx.HTTPError as exc:
            return {
                **descriptor,
                "status": "unavailable",
                "head": None,
                "error": str(exc),
            }
        except ValueError as exc:
            return {
                **descriptor,
                "status": "invalid_response",
                "head": None,
                "error": str(exc),
            }

    async def converge_realm(self, realm_id: str, *, max_passes: int = 3) -> dict:
        lock = self._locks.setdefault(realm_id, asyncio.Lock())
        async with lock:
            routes = self.peer_table.prefer_same_zone(realm_id, self.settings.zone)
            started_at = self._now()
            self._set_state(
                realm_id,
                phase="checking",
                started_at=started_at,
                conflicts=[],
                attempt=0,
            )
            if not routes:
                return self._set_state(
                    realm_id,
                    phase="converged",
                    head=self.log.get_head(realm_id),
                    instances=[
                        {
                            **self._local_instance(),
                            "status": "reachable",
                            "head": self.log.get_head(realm_id),
                        }
                    ],
                )

            instances: list[dict] = []
            all_conflicts: list[dict] = []
            async with httpx.AsyncClient(timeout=15.0) as client:
                for pass_number in range(1, max_passes + 1):
                    self._set_state(
                        realm_id, phase="exchanging", attempt=pass_number
                    )
                    instances = []
                    all_conflicts = []
                    for route in routes:
                        peer = await self._fetch_peer(client, realm_id, route)
                        if peer["head"] and peer["status"] == "reachable":
                            result = self._reconcile_remote_head(
                                realm_id, peer["head"]
                            )
                            if result.get("conflicts"):
                                for conflict in result["conflicts"]:
                                    conflict["peer"] = {
                                        key: peer[key]
                                        for key in ("instance_id", "name", "url")
                                    }
                                all_conflicts.extend(result["conflicts"])
                                peer["status"] = "conflict"
                        instances.append(peer)

                    local_head = self.log.get_head(realm_id)
                    local = {
                        **self._local_instance(),
                        "status": "reachable",
                        "head": local_head,
                    }
                    self._set_state(
                        realm_id,
                        phase="conflict" if all_conflicts else "propagating",
                        head=local_head,
                        instances=[local, *instances],
                        conflicts=all_conflicts,
                    )
                    if all_conflicts or not local_head:
                        break

                    pushed = []
                    for route, observed in zip(routes, instances, strict=True):
                        if observed.get("status") in {
                            "invalid_response",
                            "unavailable",
                        }:
                            pushed.append(observed)
                        else:
                            pushed.append(
                                await self._push_peer(
                                    client, realm_id, route, local_head
                                )
                            )
                    instances = pushed
                    matching = all(
                        item.get("status") == "reachable"
                        and item.get("head") == local_head
                        for item in pushed
                    )
                    if matching:
                        break
                    if all(
                        item.get("status") in {"reachable", "unavailable"}
                        and (
                            item.get("status") == "unavailable"
                            or item.get("head") == local_head
                        )
                        for item in pushed
                    ):
                        break

            local_head = self.log.get_head(realm_id)
            local = {
                **self._local_instance(),
                "status": "reachable",
                "head": local_head,
            }
            if local_head:
                for item in instances:
                    if item.get("status") == "reachable" and not item.get("head"):
                        item["status"] = "missing_head"
            unavailable = any(
                item.get("status")
                in {"unavailable", "invalid_response", "error", "missing_head"}
                for item in instances
            )
            push_conflict = any(
                item.get("status") == "conflict" for item in instances
            )
            mismatched = any(
                item.get("status") == "reachable"
                and item.get("head") != local_head
                for item in instances
            )
            phase = (
                "conflict"
                if all_conflicts
                else "degraded"
                if unavailable
                else "retrying"
                if mismatched or push_conflict
                else "converged"
            )
            return self._set_state(
                realm_id,
                phase=phase,
                head=local_head,
                instances=[local, *instances],
                conflicts=all_conflicts,
                completed_at=self._now(),
            )

    async def push_to_peers(self, realm_id: str) -> None:
        await self.converge_realm(realm_id)

    async def pull_from_peer(self, realm_id: str, peer_url: str) -> str | None:
        before = self.log.get_head(realm_id)
        route = PeerRoute(realm_id=realm_id, target_url=peer_url)
        async with httpx.AsyncClient(timeout=15.0) as client:
            peer = await self._fetch_peer(client, realm_id, route)
        if peer.get("head"):
            self._reconcile_remote_head(realm_id, peer["head"])
        after = self.log.get_head(realm_id)
        return after if after != before else None

    def ingest_objects(self, objects_b64: dict[str, str]) -> list[str]:
        imported: list[str] = []
        for expected_hash, encoded in objects_b64.items():
            if self.store.has(expected_hash):
                continue
            actual_hash = self.store.put(base64.b64decode(encoded))
            if actual_hash != expected_hash:
                logger.warning("Rejected sync object with mismatched hash %s", expected_hash)
                continue
            imported.append(expected_hash)
        return imported

    async def anti_entropy(self, realm_id: str) -> bool:
        before = self.log.get_head(realm_id)
        await self.converge_realm(realm_id)
        return self.log.get_head(realm_id) != before

    def _collect_objects(self, head_hash: str) -> dict[str, str]:
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
            for event_hash in commit.event_hashes:
                if event_hash not in seen:
                    seen.add(event_hash)
                    event_data = self.store.get(event_hash)
                    if event_data:
                        objects[event_hash] = base64.b64encode(event_data).decode()
            for parent in commit.parent_hashes:
                walk(parent)

        walk(head_hash)
        return objects

    def status(self, realm_id: str) -> dict:
        head = self.log.get_head(realm_id)
        routes = self.peer_table.routes_for_realm(realm_id)
        return {
            "realm_id": realm_id,
            "head": head,
            "object_count": len(self.store.list_hashes()),
            "peer_count": len(routes),
            "zone": self.settings.zone,
            "convergence": self.convergence_status(realm_id),
        }
