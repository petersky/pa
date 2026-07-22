"""Recoverable anti-entropy and realm-head convergence between peers."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable

import httpx

from pa.config import Settings
from pa.domain.models import PeerRoute, PeerRouteMode
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.network.peer_table import PeerTable
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.object_store import ObjectStore

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from pa.core.async_runtime import AsyncRuntime

MAX_SYNC_OBJECTS = 20_000
MAX_SYNC_ENCODED_BYTES = 128 * 1024 * 1024


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
        async_runtime: AsyncRuntime | None = None,
    ) -> None:
        self.settings = settings
        self.store = object_store
        self.log = event_log
        self.peer_table = peer_table
        self.membership = membership
        self.fleet_registry = fleet_registry
        self.async_runtime = async_runtime
        self._push_callbacks: list[Callable] = []
        self._debounce_tasks: dict[str, asyncio.Task] = {}
        self._convergence_tasks: dict[str, asyncio.Task] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._states: dict[str, dict] = {}
        self._periodic_task: asyncio.Task | None = None
        self._rebuild_projection: Callable[[str], None] | None = None
        self._client: httpx.AsyncClient | None = None
        self._peer_slots = asyncio.Semaphore(8)

    async def _offload(
        self,
        operation: str,
        call: Callable[..., Any],
        /,
        *args: Any,
        timeout: float = 30.0,
        **kwargs: Any,
    ) -> Any:
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def _observe_http(self, awaitable):
        if self.async_runtime:
            return await self.async_runtime.observe(
                "sync.peer_http", awaitable, timeout=16.0
            )
        async with asyncio.timeout(16.0):
            return await awaitable

    async def _request(
        self,
        method: str,
        url: str,
        *,
        payload: dict | None = None,
        params: dict | None = None,
        headers: dict[str, str] | None = None,
    ) -> httpx.Response:
        if self._client is None:
            self._open_client()
        content = None
        if payload is not None:
            content = await self._offload(
                "sync.json_encode",
                lambda: json.dumps(
                    payload, separators=(",", ":"), default=str
                ).encode(),
                timeout=10.0,
            )
            if len(content) > MAX_SYNC_ENCODED_BYTES:
                raise ValueError("sync payload exceeds the 128 MiB transfer limit")
        assert self._client is not None
        async with self._peer_slots:
            requester = getattr(self._client, "request", None)
            if requester is None:
                # Lightweight transports used by embedded/offline deployments
                # may expose only verb methods while retaining the same async
                # cancellation contract.
                requester = getattr(self._client, method.lower())
                request = requester(
                    url,
                    json=payload,
                    params=params,
                    headers=headers or self._headers(),
                )
            else:
                request = requester(
                    method,
                    url,
                    content=content,
                    params=params,
                    headers=headers or self._headers(),
                )
            return await self._observe_http(
                request
            )

    async def _response_json(self, response: httpx.Response) -> dict | list:
        return await self._offload(
            "sync.json_decode", response.json, timeout=10.0
        )

    def _open_client(self) -> None:
        if self._client is not None:
            return
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=3.0, read=12.0, write=12.0, pool=2.0),
            limits=httpx.Limits(max_connections=16, max_keepalive_connections=8),
        )

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
                "head": None,
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
        self._open_client()
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
        if self._client is not None:
            await self._client.aclose()
            self._client = None

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
            local_hashes = await self._offload(
                "sync.object_list", self.store.list_hashes
            )
            have = await self._request(
                "POST",
                f"{base}/api/sync/have",
                payload={"realm_id": realm_id, "hashes": local_hashes},
            )
            have.raise_for_status()
            have_data = await self._response_json(have)
            missing = have_data.get("missing", []) if isinstance(have_data, dict) else []
            imported = 0
            if missing:
                objects = await self._request(
                    "POST",
                    f"{base}/api/sync/get",
                    payload={"hashes": missing},
                )
                objects.raise_for_status()
                objects_data = await self._response_json(objects)
                encoded = (
                    objects_data.get("objects", {})
                    if isinstance(objects_data, dict)
                    else {}
                )
                imported = len(
                    await self._offload(
                        "sync.object_ingest", self.ingest_objects, encoded
                    )
                )
            refs = await self._request(
                "GET",
                f"{base}/api/sync/refs",
                params={"realm": realm_id},
            )
            refs.raise_for_status()
            refs_data = await self._response_json(refs)
            peer_ref = next(
                (
                    ref
                    for ref in refs_data
                    if isinstance(ref, dict) and ref.get("realm_id") == realm_id
                ),
                None,
            ) if isinstance(refs_data, list) else None
            peer_head = peer_ref.get("head_hash") if peer_ref else None
            if peer_ref:
                descriptor = self._instance(peer_ref.get("instance_id"), base)
            if peer_head and not await self._offload(
                "sync.commit_read", self.log.get_commit, peer_head
            ):
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
        except (httpx.HTTPError, TimeoutError) as exc:
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
            objects = await self._offload(
                "sync.object_collect", self._collect_objects, head, timeout=60.0
            )
            payload = {
                "realm_id": realm_id,
                "head_hash": head,
                "objects": objects,
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
            response = await self._request(
                "POST",
                endpoint,
                payload=payload,
            )
            data = await self._response_json(response)
            if not isinstance(data, dict):
                raise ValueError("peer returned a non-object sync response")
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
        except (httpx.HTTPError, TimeoutError) as exc:
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
                head = await self._offload(
                    "sync.ref_read", self.log.get_head, realm_id
                )
                return self._set_state(
                    realm_id,
                    phase="converged",
                    head=head,
                    instances=[
                        {
                            **self._local_instance(),
                            "status": "reachable",
                            "head": head,
                        }
                    ],
                )

            instances: list[dict] = []
            all_conflicts: list[dict] = []
            if self._client is None:
                self._open_client()
            assert self._client is not None
            client = self._client
            for pass_number in range(1, max_passes + 1):
                self._set_state(realm_id, phase="exchanging", attempt=pass_number)
                instances = []
                all_conflicts = []
                fetched = await asyncio.gather(
                    *(self._fetch_peer(client, realm_id, route) for route in routes)
                )
                for peer in fetched:
                    if peer["head"] and peer["status"] == "reachable":
                        result = await self._offload(
                            "sync.reconcile_head",
                            self._reconcile_remote_head,
                            realm_id,
                            peer["head"],
                            timeout=60.0,
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

                local_head = await self._offload(
                    "sync.ref_read", self.log.get_head, realm_id
                )
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

                push_calls = [
                    asyncio.sleep(0, result=observed)
                    if observed.get("status")
                    in {"invalid_response", "unavailable"}
                    else self._push_peer(client, realm_id, route, local_head)
                    for route, observed in zip(routes, instances, strict=True)
                ]
                pushed = list(await asyncio.gather(*push_calls))
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

            local_head = await self._offload(
                "sync.ref_read", self.log.get_head, realm_id
            )
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
        before = await self._offload("sync.ref_read", self.log.get_head, realm_id)
        route = PeerRoute(realm_id=realm_id, target_url=peer_url)
        if self._client is None:
            self._open_client()
        assert self._client is not None
        peer = await self._fetch_peer(self._client, realm_id, route)
        if peer.get("head"):
            await self._offload(
                "sync.reconcile_head", self._reconcile_remote_head, realm_id, peer["head"]
            )
        after = await self._offload("sync.ref_read", self.log.get_head, realm_id)
        return after if after != before else None

    def ingest_objects(self, objects_b64: dict[str, str]) -> list[str]:
        if len(objects_b64) > MAX_SYNC_OBJECTS:
            raise ValueError(f"sync transfer exceeds {MAX_SYNC_OBJECTS} objects")
        if sum(len(value) for value in objects_b64.values()) > MAX_SYNC_ENCODED_BYTES:
            raise ValueError("sync transfer exceeds the 128 MiB encoded object limit")
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
        before = await self._offload("sync.ref_read", self.log.get_head, realm_id)
        await self.converge_realm(realm_id)
        after = await self._offload("sync.ref_read", self.log.get_head, realm_id)
        return after != before

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
                if len(objects) > MAX_SYNC_OBJECTS:
                    raise ValueError(
                        f"sync history exceeds {MAX_SYNC_OBJECTS} objects"
                    )
            commit = self.log.get_commit(commit_hash)
            if not commit:
                return
            for event_hash in commit.event_hashes:
                if event_hash not in seen:
                    seen.add(event_hash)
                    event_data = self.store.get(event_hash)
                    if event_data:
                        objects[event_hash] = base64.b64encode(event_data).decode()
                        if len(objects) > MAX_SYNC_OBJECTS:
                            raise ValueError(
                                f"sync history exceeds {MAX_SYNC_OBJECTS} objects"
                            )
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
