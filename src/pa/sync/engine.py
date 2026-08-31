"""Recoverable anti-entropy and realm-head convergence between peers."""

from __future__ import annotations

import asyncio
import base64
import inspect
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Callable

import httpx

from pa.config import Settings
from pa.core.io import atomic_write_json
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
SYNC_PROTOCOL = 3
SYNC_INVENTORY_MAX_OBJECTS = 512
SYNC_INVENTORY_MAX_BYTES = 128 * 1024
SYNC_BATCH_MAX_OBJECTS = 256
SYNC_BATCH_MAX_ENCODED_BYTES = 2 * 1024 * 1024
# Legacy full-history bundles are never prepared above this for normal
# anti-entropy; incompatible peers are quarantined instead.
LEGACY_BUNDLE_SOFT_LIMIT = 2_000
SYNC_HAVE_MAX_HASHES = 512


@dataclass(frozen=True)
class PreparedObjects:
    """Immutable, bounded transfer material for one durable realm head."""

    realm_id: str
    head_hash: str
    objects: dict[str, str]
    encoded_bytes: int


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
        self._projection_locks: dict[str, asyncio.Lock] = {}
        self._projection_tasks: dict[tuple[str, str], asyncio.Task[Any]] = {}
        self._projection_stats: dict[str, dict[str, Any]] = {}
        self._states: dict[str, dict] = {}
        self._periodic_task: asyncio.Task | None = None
        self._rebuild_projection: Callable[[str], dict[str, Any] | None] | None = None
        self._client: httpx.AsyncClient | None = None
        self._peer_slots = asyncio.Semaphore(8)
        # Legacy full bundles are retained only when they fit the old safety
        # limits. Current peers use bounded protocol-v3 pages below. In-flight
        # legacy construction remains single-flight across cancelled waiters.
        self._prepared: PreparedObjects | None = None
        self._preparing: dict[tuple[str, str], asyncio.Task[PreparedObjects]] = {}
        self._prepare_metrics: dict[str, int | float | str | None] = {
            "phase": "idle",
            "builds": 0,
            "hits": 0,
            "waiters": 0,
            "last_head": None,
            "last_collect_ms": 0.0,
            "last_prepare_ms": 0.0,
            "last_object_count": 0,
            "last_encoded_bytes": 0,
            "last_error": None,
            "last_error_code": None,
            "active_residual_work": 0,
            "inventory_batches": 0,
            "object_batches": 0,
            "retry_count": 0,
            "legacy_fallbacks": 0,
            "quarantined_peers": 0,
        }
        self._quarantined_peers: dict[str, dict[str, Any]] = {}
        self._prepare_diagnostics_path = (
            self.settings.data_dir / "sync_preparation.json"
        )
        self._load_prepare_diagnostics()

    def _load_prepare_diagnostics(self) -> None:
        try:
            saved = json.loads(self._prepare_diagnostics_path.read_text())
        except (OSError, ValueError, TypeError):
            return
        if not isinstance(saved, dict):
            return
        for key in self._prepare_metrics:
            if key in saved:
                self._prepare_metrics[key] = saved[key]
        if self._prepare_metrics.get("active_residual_work"):
            self._prepare_metrics.update(
                phase="retrying",
                active_residual_work=0,
                retry_count=int(self._prepare_metrics["retry_count"]) + 1,
                last_error_code="interrupted_preparation",
                last_error="Preparation was interrupted by application shutdown; retry scheduled.",
            )
            self._persist_prepare_diagnostics()

    def _persist_prepare_diagnostics(self) -> None:
        atomic_write_json(
            self._prepare_diagnostics_path,
            {"version": 1, **self._prepare_metrics},
            mode=0o600,
        )

    async def _offload(
        self,
        operation: str,
        call: Callable[..., Any],
        /,
        *args: Any,
        timeout: float | None = 30.0,
        **kwargs: Any,
    ) -> Any:
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, timeout=timeout, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def apply_realm_head(
        self,
        realm_id: str,
        target_head: str,
        operation: str,
        call: Callable[..., Any],
        /,
        *args: Any,
        timeout: float = 60.0,
    ) -> Any:
        """Coalesce a realm/head mutation before it occupies a worker.

        The owner task is deliberately independent of every request. A timed
        out or cancelled waiter leaves it running and later callers join the
        same protected work instead of admitting a lock convoy.
        """
        key = (realm_id, target_head)
        task = self._projection_tasks.get(key)
        coalesced = task is not None and not task.done()
        if not coalesced:
            task = asyncio.create_task(
                self._run_realm_head(
                    realm_id, target_head, operation, call, *args
                ),
                name=f"pa-projection:{realm_id}:{target_head[:12]}",
            )
            self._projection_tasks[key] = task
            task.add_done_callback(
                lambda done, owned=task, task_key=key: (
                    self._projection_tasks.pop(task_key, None)
                    if self._projection_tasks.get(task_key) is owned
                    else None
                )
            )
        stats = self._projection_stats.setdefault(realm_id, {})
        stats["coalesced"] = int(stats.get("coalesced", 0)) + int(coalesced)
        try:
            async with asyncio.timeout(timeout):
                return await asyncio.shield(task)
        except TimeoutError:
            stats["deadline_overruns"] = int(stats.get("deadline_overruns", 0)) + 1
            stats["active_residual_worker"] = not task.done()
            raise

    async def _run_realm_head(
        self,
        realm_id: str,
        target_head: str,
        operation: str,
        call: Callable[..., Any],
        /,
        *args: Any,
    ) -> Any:
        lock = self._projection_locks.setdefault(realm_id, asyncio.Lock())
        queued = time.perf_counter()
        async with lock:
            started = time.perf_counter()
            stats = self._projection_stats.setdefault(realm_id, {})
            stats.update(
                target_head=target_head,
                lock_wait_ms=round((started - queued) * 1000, 3),
                active_residual_worker=True,
            )
            try:
                # No inner timeout: caller deadlines must not detach ownership
                # from the shielded worker that still owns this realm.
                result = await self._offload(operation, call, *args, timeout=None)
                if isinstance(result, dict):
                    stats.update(
                        commits_applied=result.get("commits_applied", 0),
                        rebuild_reason=result.get("rebuild_reason")
                        or result.get("reason"),
                        sqlite_ms=result.get("sqlite_ms", 0.0),
                    )
                return result
            finally:
                stats["active_residual_worker"] = False
                stats["runtime_ms"] = round((time.perf_counter() - started) * 1000, 3)

    def projection_work_status(self, realm_id: str) -> dict[str, Any]:
        status = dict(self._projection_stats.get(realm_id, {}))
        status["active_residual_worker"] = any(
            key_realm == realm_id and not task.done()
            for (key_realm, _), task in self._projection_tasks.items()
        )
        return status

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

    def on_head_advanced(
        self, callback: Callable[..., dict[str, Any] | None]
    ) -> None:
        # Callback may accept (realm_id) or (realm_id, target_head=None).
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
        tasks.extend(self._preparing.values())
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._preparing.clear()
        self._prepare_metrics["active_residual_work"] = 0
        self._persist_prepare_diagnostics()
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def invalidate_prepared(self, realm_id: str | None = None) -> None:
        """Invalidate retained bundles after compaction/corruption repair."""
        if realm_id is None or (
            self._prepared is not None and self._prepared.realm_id == realm_id
        ):
            self._prepared = None

    async def _prepare_objects(self, realm_id: str, head: str) -> PreparedObjects:
        started = time.perf_counter()
        cached = self._prepared
        if cached and cached.realm_id == realm_id and cached.head_hash == head:
            self._prepare_metrics["hits"] = int(self._prepare_metrics["hits"]) + 1
            self._prepare_metrics["last_prepare_ms"] = round(
                (time.perf_counter() - started) * 1000, 3
            )
            return cached

        key = (realm_id, head)
        task = self._preparing.get(key)
        if task is None:
            task = asyncio.create_task(
                self._build_prepared(realm_id, head),
                name=f"sync-prepare:{realm_id}:{head[:12]}",
            )
            self._preparing[key] = task
            self._prepare_metrics["active_residual_work"] = len(self._preparing)
            self._persist_prepare_diagnostics()
            task.add_done_callback(
                lambda done, prepare_key=key: self._finish_preparation(
                    prepare_key, done
                )
            )
        else:
            self._prepare_metrics["waiters"] = int(
                self._prepare_metrics["waiters"]
            ) + 1
        # asyncio.wait does not propagate waiter cancellation into its input
        # task and, unlike asyncio.shield on Python 3.14, does not create an
        # abandoned proxy future that reports the inner exception separately.
        await asyncio.wait({task})
        result = task.result()
        self._prepare_metrics["last_prepare_ms"] = round(
            (time.perf_counter() - started) * 1000, 3
        )
        return result

    def _finish_preparation(
        self, key: tuple[str, str], task: asyncio.Task[PreparedObjects]
    ) -> None:
        """Retrieve abandoned task failures and retire single-flight ownership."""
        if self._preparing.get(key) is task:
            self._preparing.pop(key, None)
        self._prepare_metrics["active_residual_work"] = len(self._preparing)
        if task.cancelled():
            self._persist_prepare_diagnostics()
            return
        error = task.exception()
        if error is not None:
            self._prepare_metrics.update(
                phase="failed",
                last_error=str(error),
                last_error_code=(
                    "legacy_bundle_too_large"
                    if isinstance(error, ValueError)
                    else "preparation_failed"
                ),
            )
        self._persist_prepare_diagnostics()

    async def _build_prepared(self, realm_id: str, head: str) -> PreparedObjects:
        self._prepare_metrics["phase"] = "collecting"
        started = time.perf_counter()
        try:
            objects = await self._offload(
                "sync.object_collect", self._collect_objects, head, timeout=60.0
            )
            encoded_bytes = sum(len(value) for value in objects.values())
            if encoded_bytes > MAX_SYNC_ENCODED_BYTES:
                raise ValueError("sync history exceeds the 128 MiB encoded object limit")
            prepared = PreparedObjects(
                realm_id=realm_id,
                head_hash=head,
                objects=objects,
                encoded_bytes=encoded_bytes,
            )
            # A newer head may have completed first; never replace it with stale
            # work. The caller may still safely use this immutable result.
            current = self._prepared
            durable_head = await self._offload(
                "sync.ref_read", self.log.get_head, realm_id
            )
            if durable_head == head and (
                current is None
                or current.realm_id != realm_id
                or current.head_hash != durable_head
            ):
                self._prepared = prepared
            self._prepare_metrics.update(
                phase="ready",
                builds=int(self._prepare_metrics["builds"]) + 1,
                last_head=head,
                last_collect_ms=round((time.perf_counter() - started) * 1000, 3),
                last_object_count=len(objects),
                last_encoded_bytes=encoded_bytes,
                last_error=None,
                last_error_code=None,
            )
            self._persist_prepare_diagnostics()
            return prepared
        except Exception as exc:
            self._prepare_metrics.update(
                phase="failed",
                last_error=str(exc),
                last_error_code="legacy_bundle_too_large",
            )
            self._persist_prepare_diagnostics()
            raise

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
        self,
        client: httpx.AsyncClient,
        realm_id: str,
        route: PeerRoute,
        *,
        local_hashes: list[str] | None = None,
    ) -> dict:
        base = route.target_url.rstrip("/")
        descriptor = self._instance(route.target_instance_id, base)
        quarantined = self._quarantined_peers.get(base)
        if quarantined:
            return {
                **descriptor,
                "status": "protocol_incompatible",
                "head": None,
                "imported": 0,
                "error": quarantined,
            }
        try:
            # Head-first: learn the peer tip before any object inventory.
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

            imported = 0
            if peer_head and await self._offload(
                "sync.commit_read", self.log.get_commit, peer_head
            ):
                # Converged or peer is behind our store: nothing to pull.
                return {
                    **descriptor,
                    "status": "reachable",
                    "head": peer_head,
                    "imported": 0,
                }

            if peer_head:
                # O(delta) pull: ask the peer only for objects we lack along the
                # peer head frontier instead of exchanging the full store.
                imported = await self._pull_peer_v3(
                    realm_id, route, peer_head, descriptor
                )
                if not await self._offload(
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

            # Peer has no tip. Bounded compatibility probe only — never send the
            # entire object catalog on the hot anti-entropy path.
            catalog = getattr(self.store, "catalog", None)
            if local_hashes is None:
                if catalog is not None:
                    local_hashes = catalog.iter_hashes(limit=SYNC_HAVE_MAX_HASHES)
                else:
                    local_hashes = []
            if local_hashes:
                have = await self._request(
                    "POST",
                    f"{base}/api/sync/have",
                    payload={
                        "realm_id": realm_id,
                        "hashes": local_hashes[:SYNC_HAVE_MAX_HASHES],
                        "protocol": SYNC_PROTOCOL,
                        "bounded": True,
                    },
                )
                have.raise_for_status()
                have_data = await self._response_json(have)
                missing = (
                    have_data.get("missing", []) if isinstance(have_data, dict) else []
                )
                missing = missing[:SYNC_HAVE_MAX_HASHES]
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
            return {
                **descriptor,
                "status": "reachable",
                "head": peer_head,
                "imported": imported,
            }
        except (httpx.HTTPError, TimeoutError) as exc:
            detail = str(exc).strip()
            error = f"{type(exc).__name__}: {detail}" if detail else type(exc).__name__
            logger.warning("Sync exchange with %s failed: %s", base, error)
            return {
                **descriptor,
                "status": "unavailable",
                "head": None,
                "imported": 0,
                "error": error,
            }
        except ValueError as exc:
            return {
                **descriptor,
                "status": "invalid_response",
                "head": None,
                "imported": 0,
                "error": str(exc),
            }

    async def _pull_peer_v3(
        self,
        realm_id: str,
        route: PeerRoute,
        peer_head: str,
        descriptor: dict[str, str],
    ) -> int:
        """Fetch missing objects for a peer tip using bounded head-first pages."""
        base = route.target_url.rstrip("/")
        pending_commits = [peer_head]
        pending_events: list[str] = []
        seen_commits: set[str] = set()
        seen_events: set[str] = set()
        imported_total = 0
        while pending_commits or pending_events:
            page: list[str] = []
            page_commits: list[str] = []
            while pending_events and len(page) < SYNC_INVENTORY_MAX_OBJECTS:
                event_hash = pending_events.pop()
                if not event_hash or event_hash in seen_events:
                    continue
                seen_events.add(event_hash)
                if await self._offload("sync.object_has", self.store.has, event_hash):
                    continue
                page.append(event_hash)
            while pending_commits and len(page) < SYNC_INVENTORY_MAX_OBJECTS:
                candidate = pending_commits.pop()
                if not candidate or candidate in seen_commits:
                    continue
                seen_commits.add(candidate)
                if await self._offload("sync.object_has", self.store.has, candidate):
                    commit = await self._offload(
                        "sync.commit_read", self.log.get_commit, candidate
                    )
                    if commit:
                        pending_commits.extend(
                            parent
                            for parent in commit.parent_hashes
                            if parent and parent not in seen_commits
                        )
                        pending_events.extend(
                            event_hash
                            for event_hash in commit.event_hashes
                            if event_hash and event_hash not in seen_events
                        )
                    continue
                page.append(candidate)
                page_commits.append(candidate)
            if not page:
                continue
            response = await self._request(
                "POST",
                f"{base}/api/sync/get",
                payload={"hashes": page, "protocol": SYNC_PROTOCOL},
            )
            response.raise_for_status()
            data = await self._response_json(response)
            encoded = data.get("objects", {}) if isinstance(data, dict) else {}
            imported = await self._offload(
                "sync.object_ingest", self.ingest_objects, encoded
            )
            imported_total += len(imported)
            for commit_hash in page_commits:
                commit = await self._offload(
                    "sync.commit_read", self.log.get_commit, commit_hash
                )
                if not commit:
                    continue
                pending_commits.extend(
                    parent
                    for parent in commit.parent_hashes
                    if parent and parent not in seen_commits
                )
                pending_events.extend(
                    event_hash
                    for event_hash in commit.event_hashes
                    if event_hash and event_hash not in seen_events
                )
        return imported_total

    def _source_name(self, source: dict | None) -> dict | None:
        if not source:
            return source
        result = dict(source)
        instance_id = result.get("instance_id")
        result["instance_name"] = self._instance(instance_id, instance_id or "")[
            "name"
        ]
        return result

    def _projection_callback_accepts_target(self) -> bool:
        callback = self._rebuild_projection
        if callback is None:
            return False
        try:
            signature = inspect.signature(callback)
        except (TypeError, ValueError):
            return False
        parameters = list(signature.parameters.values())
        if any(
            parameter.kind == inspect.Parameter.VAR_POSITIONAL
            for parameter in parameters
        ):
            return True
        # Bound methods omit self; accept an optional/required second parameter.
        return len(parameters) >= 2

    def _rebuild_to_head(
        self, realm_id: str, target_head: str | None = None
    ) -> dict[str, Any] | None:
        if not self._rebuild_projection:
            return None
        if target_head is not None and self._projection_callback_accepts_target():
            return self._rebuild_projection(realm_id, target_head)
        if (
            target_head is not None
            and self.log.get_head(realm_id) != target_head
        ):
            # Legacy one-arg callbacks read get_head(); callers must publish the
            # durable tip first, then invoke without a target.
            return {
                "commits_applied": 0,
                "rebuilt": False,
                "reason": "deferred_legacy_callback",
            }
        return self._rebuild_projection(realm_id)

    def _publish_head_with_projection(
        self,
        realm_id: str,
        new_head: str,
        *,
        expected_head: str | None,
    ) -> dict[str, Any] | None:
        """Catch up target-aware projections before CAS; legacy after."""
        if self._projection_callback_accepts_target():
            projection = self._rebuild_to_head(realm_id, new_head)
            self.log.advance_ref(
                realm_id, new_head, expected_head=expected_head
            )
            return projection
        self.log.advance_ref(realm_id, new_head, expected_head=expected_head)
        return self._rebuild_to_head(realm_id)

    def _reconcile_remote_head(self, realm_id: str, remote_head: str) -> dict:
        for attempt in range(1, 4):
            local_head = self.log.get_head(realm_id)
            if local_head == remote_head:
                # Repair projection lag even when the durable tip already matches.
                projection = self._rebuild_to_head(realm_id, remote_head) or {}
                if projection.get("reason") == "deferred_legacy_callback":
                    projection = self._rebuild_to_head(realm_id) or {}
                if projection.get("commits_applied") or projection.get("rebuilt"):
                    return {
                        "advanced": False,
                        "repaired_projection": True,
                        "head": local_head,
                        "attempts": attempt,
                        **projection,
                    }
                return {"advanced": False, "head": local_head, "attempts": attempt}
            try:
                if not local_head:
                    if not self.log.get_commit(remote_head):
                        return {"advanced": False, "missing_head": remote_head}
                    projection = self._publish_head_with_projection(
                        realm_id, remote_head, expected_head=None
                    )
                    advanced_head = remote_head
                elif self.log.is_ancestor(local_head, remote_head):
                    projection = self._publish_head_with_projection(
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
                        automatic_resolutions=health.get(
                            "automatic_resolutions", []
                        ),
                    )
                    advanced_head = merge.hash
                    # merge_heads already published the tip; rebuild after.
                    projection = self._rebuild_to_head(realm_id, advanced_head)
                    if projection and projection.get("reason") == "deferred_legacy_callback":
                        projection = self._rebuild_to_head(realm_id)
                self.invalidate_prepared(realm_id)
                return {
                    "advanced": True,
                    "head": advanced_head,
                    "attempts": attempt,
                    **(projection or {}),
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
        prepared: PreparedObjects | None = None,
    ) -> dict:
        descriptor = self._instance(route.target_instance_id, route.target_url)
        try:
            endpoint = f"{route.target_url.rstrip('/')}/api/sync/push"
            if route.mode != PeerRouteMode.RELAY:
                return await self._push_peer_v3(realm_id, route, head, descriptor)

            # A relay cannot negotiate with its target. Preserve the legacy full
            # transaction only while it can be represented without truncation.
            prepared = prepared or await self._prepare_objects(realm_id, head)
            objects = prepared.objects
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
                "objects_sent": len(objects),
                "encoded_bytes_sent": sum(len(value) for value in objects.values()),
                "inventory_protocol": 1,
            }
        except (httpx.HTTPError, TimeoutError) as exc:
            return {
                **descriptor,
                "status": "unavailable",
                "head": None,
                "error": str(exc),
            }
        except ValueError as exc:
            incompatible = "sync history exceeds" in str(exc)
            self._prepare_metrics.update(
                phase="failed",
                last_head=head,
                last_error=str(exc),
                last_error_code=(
                    "legacy_bundle_too_large"
                    if incompatible
                    else "invalid_sync_response"
                ),
                retry_count=int(self._prepare_metrics["retry_count"]) + 1,
            )
            self._persist_prepare_diagnostics()
            return {
                **descriptor,
                "status": "protocol_incompatible"
                if incompatible
                else "invalid_response",
                "head": None,
                "error": {
                    "code": "legacy_bundle_too_large",
                    "message": (
                        "Peer requires a legacy full-history bundle that exceeds "
                        "safe transfer limits; upgrade the peer to sync protocol v3."
                    ),
                    "detail": str(exc),
                }
                if incompatible
                else str(exc),
            }

    def _inventory_page(
        self,
        pending: list[str],
        seen_commits: set[str],
        pending_events: list[str] | None = None,
    ) -> tuple[dict[str, bytes], dict[str, list[str]]]:
        """Read one bounded head-first page without walking known ancestry.

        Oversized single-commit event fanouts spill into ``pending_events`` and
        continue on later pages instead of aborting anti-entropy.
        """
        if pending_events is None:
            pending_events = []
        raw: dict[str, bytes] = {}
        parents: dict[str, list[str]] = {}
        inventory_bytes = 0

        def _can_add(estimated: int) -> bool:
            if len(raw) >= SYNC_INVENTORY_MAX_OBJECTS:
                return False
            if raw and inventory_bytes + estimated > SYNC_INVENTORY_MAX_BYTES:
                return False
            return True

        while pending_events:
            event_hash = pending_events[-1]
            if not event_hash or event_hash in raw:
                pending_events.pop()
                continue
            estimated = len(event_hash) + 8
            if not _can_add(estimated):
                break
            pending_events.pop()
            event_data = self.store.get(event_hash)
            if event_data is not None:
                raw[event_hash] = event_data
                inventory_bytes += estimated

        while pending and len(raw) < SYNC_INVENTORY_MAX_OBJECTS:
            commit_hash = pending.pop()
            if not commit_hash or commit_hash in seen_commits:
                continue
            seen_commits.add(commit_hash)
            data = self.store.get(commit_hash)
            commit = self.log.get_commit(commit_hash)
            if data is None or commit is None:
                continue
            estimated = len(commit_hash) + 8
            if raw and inventory_bytes + estimated > SYNC_INVENTORY_MAX_BYTES:
                seen_commits.remove(commit_hash)
                pending.append(commit_hash)
                break
            raw[commit_hash] = data
            inventory_bytes += estimated
            parents[commit_hash] = list(commit.parent_hashes)
            pending.extend(
                item for item in reversed(commit.parent_hashes) if item
            )
            spill_events = False
            for event_hash in commit.event_hashes:
                if not event_hash or event_hash in raw:
                    continue
                if spill_events:
                    pending_events.append(event_hash)
                    continue
                estimated = len(event_hash) + 8
                if not _can_add(estimated):
                    pending_events.append(event_hash)
                    spill_events = True
                    continue
                event_data = self.store.get(event_hash)
                if event_data is not None:
                    raw[event_hash] = event_data
                    inventory_bytes += estimated
        return raw, parents

    @staticmethod
    def _encoded_batches(objects: dict[str, bytes]):
        batch: dict[str, str] = {}
        encoded_bytes = 0
        for object_hash, data in objects.items():
            encoded = base64.b64encode(data).decode()
            if len(encoded) > SYNC_BATCH_MAX_ENCODED_BYTES:
                raise ValueError("one sync object exceeds the encoded batch limit")
            if batch and (
                len(batch) >= SYNC_BATCH_MAX_OBJECTS
                or encoded_bytes + len(encoded) > SYNC_BATCH_MAX_ENCODED_BYTES
            ):
                yield batch, encoded_bytes
                batch = {}
                encoded_bytes = 0
            batch[object_hash] = encoded
            encoded_bytes += len(encoded)
        if batch:
            yield batch, encoded_bytes

    async def _push_peer_v3(
        self, realm_id: str, route: PeerRoute, head: str, descriptor: dict[str, str]
    ) -> dict:
        base = route.target_url.rstrip("/")
        pending = [head]
        pending_events: list[str] = []
        seen_commits: set[str] = set()
        sent_count = 0
        sent_bytes = 0
        inventory_batches = 0
        object_batches = 0
        started = time.perf_counter()
        while pending or pending_events:
            raw, parents = await self._offload(
                "sync.object_collect",
                self._inventory_page,
                pending,
                seen_commits,
                pending_events,
                timeout=60.0,
            )
            if not raw:
                continue
            inventory = await self._request(
                "POST",
                f"{base}/api/sync/need",
                payload={
                    "realm_id": realm_id,
                    "head_hash": head,
                    "hashes": list(raw),
                    "commit_hashes": list(parents),
                    "protocol": SYNC_PROTOCOL,
                },
            )
            if inventory.status_code == 404:
                # Legacy peers lack /need. Only prepare a full bundle when the
                # indexed history is small enough; otherwise quarantine without
                # walking/encoding the entire DAG on every anti-entropy pass.
                index_status = self.log.index_status(realm_id)
                reachable = int(index_status.get("commit_count") or 0) + int(
                    index_status.get("event_count") or 0
                )
                if reachable > LEGACY_BUNDLE_SOFT_LIMIT or (
                    index_status.get("ready")
                    and reachable == 0
                    and self.store.indexed_count() > LEGACY_BUNDLE_SOFT_LIMIT
                ):
                    detail = {
                        "code": "legacy_bundle_too_large",
                        "message": (
                            "Peer requires a legacy full-history bundle that exceeds "
                            "safe transfer limits; upgrade the peer to sync protocol v3."
                        ),
                        "reachable_objects": reachable,
                        "soft_limit": LEGACY_BUNDLE_SOFT_LIMIT,
                    }
                    self._quarantined_peers[base] = detail
                    self._prepare_metrics["quarantined_peers"] = len(
                        self._quarantined_peers
                    )
                    self._prepare_metrics["legacy_fallbacks"] = int(
                        self._prepare_metrics["legacy_fallbacks"]
                    ) + 1
                    self._persist_prepare_diagnostics()
                    return {
                        **descriptor,
                        "status": "protocol_incompatible",
                        "head": None,
                        "error": detail,
                    }
                prepared = await self._prepare_objects(realm_id, head)
                self._prepare_metrics["legacy_fallbacks"] = int(
                    self._prepare_metrics["legacy_fallbacks"]
                ) + 1
                legacy = await self._request(
                    "POST",
                    f"{base}/api/sync/push",
                    payload={
                        "realm_id": realm_id,
                        "head_hash": head,
                        "objects": prepared.objects,
                    },
                )
                legacy_data = await self._response_json(legacy)
                legacy.raise_for_status()
                return {
                    **descriptor,
                    "status": "reachable",
                    "head": legacy_data.get("head"),
                    "objects_sent": len(prepared.objects),
                    "encoded_bytes_sent": prepared.encoded_bytes,
                    "inventory_protocol": 1,
                }
            inventory.raise_for_status()
            data = await self._response_json(inventory)
            if not isinstance(data, dict) or data.get("protocol") != SYNC_PROTOCOL:
                raise ValueError("peer returned an incompatible sync inventory")
            missing = data.get("missing")
            present_commits = data.get("present_commits")
            if not isinstance(missing, list) or not isinstance(present_commits, list):
                raise ValueError("peer returned an invalid sync inventory")
            if any(item not in raw for item in missing):
                raise ValueError("peer requested an unadvertised sync object")
            missing_set = set(missing)
            # A present commit proves its reachable ancestry is already local.
            # Remove parent frontiers that have not been included in this page.
            known_frontier = {
                parent
                for commit_hash in present_commits
                for parent in parents.get(commit_hash, [])
            }
            if known_frontier:
                pending[:] = [item for item in pending if item not in known_frontier]
            requested = {item: raw[item] for item in missing_set}
            for batch, batch_bytes in self._encoded_batches(requested):
                response = await self._request(
                    "POST",
                    f"{base}/api/sync/push",
                    payload={"realm_id": realm_id, "head_hash": "", "objects": batch},
                )
                if response.status_code == 409:
                    conflict_data = await self._response_json(response)
                    detail = (
                        conflict_data.get("detail", conflict_data)
                        if isinstance(conflict_data, dict)
                        else conflict_data
                    )
                    return {
                        **descriptor,
                        "status": "conflict",
                        "head": detail.get("local_head")
                        if isinstance(detail, dict)
                        else None,
                        "error": detail,
                    }
                response.raise_for_status()
                sent_count += len(batch)
                sent_bytes += batch_bytes
                object_batches += 1
            inventory_batches += 1

        final = await self._request(
            "POST",
            f"{base}/api/sync/push",
            payload={"realm_id": realm_id, "head_hash": head, "objects": {}},
        )
        final_data = await self._response_json(final)
        if final.status_code == 409:
            detail = final_data.get("detail", final_data)
            return {
                **descriptor,
                "status": "conflict",
                "head": detail.get("local_head")
                if isinstance(detail, dict)
                else None,
                "error": detail,
            }
        final.raise_for_status()
        self._prepare_metrics.update(
            phase="ready",
            last_head=head,
            last_prepare_ms=round((time.perf_counter() - started) * 1000, 3),
            last_object_count=sent_count,
            last_encoded_bytes=sent_bytes,
            inventory_batches=inventory_batches,
            object_batches=object_batches,
            last_error=None,
            last_error_code=None,
        )
        self._persist_prepare_diagnostics()
        return {
            **descriptor,
            "status": "reachable",
            "head": final_data.get("head"),
            "objects_sent": sent_count,
            "encoded_bytes_sent": sent_bytes,
            "inventory_protocol": SYNC_PROTOCOL,
            "inventory_batches": inventory_batches,
            "object_batches": object_batches,
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
                local_hashes = None
                fetched = await asyncio.gather(
                    *(
                        self._fetch_peer(
                            client, realm_id, route, local_hashes=local_hashes
                        )
                        for route in routes
                    )
                )
                for peer in fetched:
                    if peer["head"] and peer["status"] == "reachable":
                        result = await self.apply_realm_head(
                            realm_id,
                            peer["head"],
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
                    else self._push_peer(
                        client, realm_id, route, local_head
                    )
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
                in {
                    "unavailable", "invalid_response", "protocol_incompatible",
                    "error", "missing_head",
                }
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
            await self.apply_realm_head(
                realm_id,
                peer["head"],
                "sync.reconcile_head",
                self._reconcile_remote_head,
                realm_id,
                peer["head"],
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
        pending = [head_hash]
        encoded_bytes = 0

        def add(object_hash: str, data: bytes) -> None:
            nonlocal encoded_bytes
            encoded = base64.b64encode(data).decode()
            encoded_bytes += len(encoded)
            if len(objects) >= MAX_SYNC_OBJECTS:
                raise ValueError(f"sync history exceeds {MAX_SYNC_OBJECTS} objects")
            if encoded_bytes > MAX_SYNC_ENCODED_BYTES:
                raise ValueError(
                    "sync history exceeds the 128 MiB encoded object limit"
                )
            objects[object_hash] = encoded

        while pending:
            commit_hash = pending.pop()
            if commit_hash in seen:
                continue
            seen.add(commit_hash)
            data = self.store.get(commit_hash)
            if data:
                add(commit_hash, data)
            commit = self.log.get_commit(commit_hash)
            if not commit:
                continue
            for event_hash in commit.event_hashes:
                if event_hash not in seen:
                    seen.add(event_hash)
                    event_data = self.store.get(event_hash)
                    if event_data:
                        add(event_hash, event_data)
            pending.extend(commit.parent_hashes)

        return objects

    def status(self, realm_id: str) -> dict:
        head = self.log.get_head(realm_id)
        routes = self.peer_table.routes_for_realm(realm_id)
        catalog = getattr(self.store, "catalog", None)
        if catalog is not None:
            # Refresh realm reachability stats from the DAG index when ready.
            index_status = self.log.index_status(realm_id)
            commit_count = 0
            event_count = 0
            if index_status.get("ready"):
                commit_count = int(index_status.get("commit_count") or 0)
                event_count = int(index_status.get("event_count") or 0)
                expected = commit_count + event_count
                coverage = catalog.coverage(expected_reachable=expected)
                store_total = catalog.count()
                # Only compute unreachable when the catalog covers the DAG.
                unreachable = (
                    max(0, store_total - expected)
                    if coverage.get("ready")
                    else 0
                )
                oldest, newest = catalog.age_bounds_ns()
                catalog.publish_realm_stats(
                    realm_id,
                    commit_count=commit_count,
                    event_count=event_count,
                    auxiliary_count=0,
                    unreachable_count=unreachable,
                    reachable_bytes=0,
                    head_hash=head,
                    oldest_reachable_ns=oldest,
                    newest_reachable_ns=newest,
                )
            history = catalog.status_payload(
                realm_id,
                expected_reachable=commit_count + event_count,
            )
            object_count = history["object_count"]
        else:
            history = None
            object_count = self.store.indexed_count()
        return {
            "realm_id": realm_id,
            "head": head,
            "object_count": object_count,
            "history": history,
            "peer_count": len(routes),
            "zone": self.settings.zone,
            "convergence": self.convergence_status(realm_id),
            "object_preparation": dict(self._prepare_metrics),
            "projection_work": self.projection_work_status(realm_id),
            "quarantined_peers": dict(self._quarantined_peers),
            "protocol": SYNC_PROTOCOL,
        }
