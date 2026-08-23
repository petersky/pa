from __future__ import annotations

import asyncio
import base64
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import urlsplit

from fastapi import Response
import httpx
from fastapi import HTTPException
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.core.async_runtime import AsyncRuntime
from pa.core.live_updates import LiveUpdateBroker
from pa.core.writer_lock import DataDirWriterLock
from pa.domain.models import (
    Card,
    CardEvent,
    CardLane,
    CardUpdate,
    EventType,
    FleetInstance,
    PeerRoute,
    SyncCommit,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.fleet.membership import MembershipStore
from pa.fleet.policy import InstanceGroupCreate, InstanceGroupUpdate
from pa.fleet.registry import FleetRegistry
from pa.instance.agent_session import reset_instance_agent
from pa.modules.fleet import (
    RemoteAgentStartBody,
    _assert_dispatch_sync_health,
    start_remote_agent_work,
)
from pa.modules.sync import get_sync_convergence, resolve_sync_conflicts
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.infrastructure import reset_infrastructure
from pa.sync.object_store import ObjectStore


class _Node:
    def __init__(self, root: Path, instance_id: str, name: str) -> None:
        self.url = f"http://{instance_id}"
        self.settings = Settings(
            data_dir=root / instance_id,
            instance_id=instance_id,
            instance_name=name,
            instance_url=self.url,
            subscribed_realms=["default"],
            sync_token="shared",
            agent_enabled=False,
        )
        self.objects = ObjectStore(self.settings.objects_dir)
        self.log = EventLog(
            self.objects, self.settings.data_dir, self.settings.instance_id
        )
        self.membership = MembershipStore(self.settings.data_dir)
        self.membership.ensure_owner_membership("default", "local")
        self.peers = PeerTable(self.settings.data_dir)
        self.fleet = FleetRegistry(self.settings.data_dir, self.settings.fleet_id)
        self.engine = SyncEngine(
            self.settings,
            self.objects,
            self.log,
            self.peers,
            self.membership,
            self.fleet,
        )


class ProjectionWorkCoordinatorTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.node = _Node(Path(self.tmp.name), "coordinator", "Coordinator")
        self.runtime = AsyncRuntime(max_workers=2, max_queue=2)
        self.node.engine.async_runtime = self.runtime

    async def asyncTearDown(self) -> None:
        await self.runtime.close(drain_timeout=1)
        self.tmp.cleanup()

    async def test_eight_identical_waiters_use_one_worker(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def apply() -> dict:
            nonlocal calls
            calls += 1
            entered.set()
            release.wait(2)
            return {"commits_applied": 1, "reason": "fast_forward"}

        waiters = [
            asyncio.create_task(
                self.node.engine.apply_realm_head(
                    "default", "same-head", "sync.test_projection", apply
                )
            )
            for _ in range(8)
        ]
        await asyncio.to_thread(entered.wait, 1)
        await asyncio.sleep(0)
        self.assertEqual(calls, 1)
        self.assertEqual(self.runtime.snapshot()["executor"]["active"], 1)
        release.set()
        self.assertEqual(len(await asyncio.gather(*waiters)), 8)
        status = self.node.engine.projection_work_status("default")
        self.assertEqual(status["coalesced"], 7)
        self.assertFalse(status["active_residual_worker"])

    async def test_timeout_and_cancellation_preserve_residual_ownership(self) -> None:
        entered = threading.Event()
        release = threading.Event()
        calls = 0

        def apply() -> dict:
            nonlocal calls
            calls += 1
            entered.set()
            release.wait(2)
            return {"commits_applied": 1, "reason": "fast_forward"}

        with self.assertRaises(TimeoutError):
            await self.node.engine.apply_realm_head(
                "default", "protected-head", "sync.test_projection", apply, timeout=0.01
            )
        self.assertTrue(entered.is_set())
        follower = asyncio.create_task(
            self.node.engine.apply_realm_head(
                "default", "protected-head", "sync.test_projection", apply
            )
        )
        cancelled = asyncio.create_task(
            self.node.engine.apply_realm_head(
                "default", "protected-head", "sync.test_projection", apply
            )
        )
        cancelled.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await cancelled
        await asyncio.sleep(0)
        status = self.node.engine.projection_work_status("default")
        self.assertTrue(status["active_residual_worker"])
        self.assertEqual(status["deadline_overruns"], 1)
        self.assertEqual(calls, 1)
        release.set()
        await follower
        self.assertEqual(calls, 1)


class _SyncNetwork:
    def __init__(self, nodes: list[_Node]) -> None:
        self.nodes = {urlsplit(node.url).hostname: node for node in nodes}
        self.unavailable: set[str] = set()
        self.omit_push_head: set[str] = set()
        self.reject_push: set[str] = set()
        self.legacy_need: set[str] = set()

    def client(self, *args, **kwargs):
        return _SyncClient(self)

    def response(self, method: str, url: str, **kwargs) -> httpx.Response:
        host = urlsplit(url).hostname or ""
        request = httpx.Request(method, url)
        if host in self.unavailable:
            raise httpx.ConnectError("peer offline", request=request)
        node = self.nodes[host]
        path = urlsplit(url).path
        body = kwargs.get("json") or {}
        if path == "/api/sync/have":
            missing = sorted(set(node.objects.list_hashes()) - set(body["hashes"]))
            return httpx.Response(200, json={"missing": missing}, request=request)
        if path == "/api/sync/need":
            if host in self.legacy_need:
                return httpx.Response(404, json={"detail": "Not Found"}, request=request)
            missing = [
                object_hash
                for object_hash in body["hashes"]
                if not node.objects.has(object_hash)
            ]
            return httpx.Response(
                200,
                json={
                    "protocol": 3,
                    "missing": missing,
                    "present_commits": [
                        object_hash
                        for object_hash in body.get("commit_hashes", [])
                        if node.objects.has(object_hash)
                    ],
                },
                request=request,
            )
        if path == "/api/sync/get":
            objects = {
                object_hash: base64.b64encode(node.objects.get(object_hash)).decode()
                for object_hash in body["hashes"]
                if node.objects.get(object_hash) is not None
            }
            return httpx.Response(200, json={"objects": objects}, request=request)
        if path == "/api/sync/refs":
            realm = kwargs.get("params", {}).get("realm", "default")
            head = node.log.get_head(realm)
            refs = (
                [
                    {
                        "realm_id": realm,
                        "instance_id": node.settings.instance_id,
                        "head_hash": head,
                    }
                ]
                if head
                else []
            )
            return httpx.Response(200, json=refs, request=request)
        if path == "/api/sync/push":
            if host in self.reject_push:
                return httpx.Response(
                    409,
                    json={
                        "detail": {
                            "code": "sync_conflict",
                            "local_head": node.log.get_head(
                                body.get("realm_id", "default")
                            ),
                            "conflicts": [],
                        }
                    },
                    request=request,
                )
            node.engine.ingest_objects(body.get("objects", {}))
            result = (
                node.engine._reconcile_remote_head(
                    body.get("realm_id", "default"), body["head_hash"]
                )
                if body.get("head_hash")
                else {"conflicts": []}
            )
            if result.get("conflicts"):
                return httpx.Response(
                    409,
                    json={
                        "detail": {
                            "code": "sync_conflict",
                            "local_head": result.get("head"),
                            "conflicts": result["conflicts"],
                        }
                    },
                    request=request,
                )
            return httpx.Response(
                200,
                json={
                    "head": None
                    if host in self.omit_push_head
                    else node.log.get_head(body.get("realm_id", "default"))
                },
                request=request,
            )
        raise AssertionError(f"Unexpected sync request: {method} {url}")


class _SyncClient:
    def __init__(self, network: _SyncNetwork) -> None:
        self.network = network

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def get(self, url, **kwargs):
        return self.network.response("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return self.network.response("POST", url, **kwargs)


class RealmConvergenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_thirty_thousand_object_history_converges_in_bounded_batches(
        self,
    ) -> None:
        hashes = [f"{index:064x}" for index in range(1, 30_002)]
        objects = {item: b"{}" for item in hashes}
        commits = {
            item: SimpleNamespace(
                parent_hashes=[hashes[index - 1]] if index else [],
                event_hashes=[],
            )
            for index, item in enumerate(hashes)
        }
        received: set[str] = set()
        remote_head: str | None = None

        self.authority.engine.store = SimpleNamespace(
            get=lambda object_hash: objects.get(object_hash)
        )
        self.authority.engine.log = SimpleNamespace(
            get_commit=lambda commit_hash: commits.get(commit_hash)
        )

        async def request(method: str, url: str, *, payload=None, **_kwargs):
            nonlocal remote_head
            request = httpx.Request(method, url)
            body = payload or {}
            if url.endswith("/api/sync/need"):
                return httpx.Response(
                    200,
                    json={
                        "protocol": 3,
                        "missing": [
                            item for item in body["hashes"] if item not in received
                        ],
                        "present_commits": [
                            item
                            for item in body["commit_hashes"]
                            if item in received
                        ],
                    },
                    request=request,
                )
            received.update(body.get("objects", {}))
            remote_head = body.get("head_hash") or remote_head
            return httpx.Response(200, json={"head": remote_head}, request=request)

        self.authority.engine._request = request
        head = hashes[-1]

        result = await self.authority.engine._push_peer(
            MagicMock(),
            "default",
            self.authority.peers.routes_for_realm("default")[0],
            head,
        )

        self.assertEqual(result["status"], "reachable")
        self.assertEqual(result["inventory_protocol"], 3)
        self.assertEqual(result["objects_sent"], 30_001)
        self.assertGreater(result["inventory_batches"], 1)
        self.assertGreater(result["object_batches"], 1)
        self.assertEqual(remote_head, head)
        self.assertEqual(len(received), 30_001)

    async def test_peer_missing_one_commit_receives_only_commit_and_event(self) -> None:
        card = self._shared_card()
        head = self._update(self.authority, card.id, title="One delta")
        self.authority.engine._open_client()
        assert self.authority.engine._client is not None

        result = await self.authority.engine._push_peer(
            self.authority.engine._client,
            "default",
            self.authority.peers.routes_for_realm("default")[0],
            head,
        )

        self.assertEqual(result["status"], "reachable")
        self.assertEqual(result["objects_sent"], 2)
        self.assertEqual(result["inventory_protocol"], 3)
        self.assertEqual(self.target.log.get_head("default"), head)

    async def test_interrupted_transfer_restarts_idempotently(self) -> None:
        card = self._shared_card()
        head = self._update(self.authority, card.id, title="Resumable")
        original_response = self.network.response
        interrupted = False

        def interrupt_once(method: str, url: str, **kwargs) -> httpx.Response:
            nonlocal interrupted
            body = kwargs.get("json") or {}
            if (
                not interrupted
                and urlsplit(url).hostname == "target"
                and urlsplit(url).path == "/api/sync/push"
                and body.get("objects")
            ):
                interrupted = True
                response = original_response(method, url, **kwargs)
                raise httpx.ConnectError(
                    "connection lost after durable batch",
                    request=httpx.Request(method, url),
                )
            return original_response(method, url, **kwargs)

        self.network.response = interrupt_once
        route = self.authority.peers.routes_for_realm("default")[0]
        first = await self.authority.engine._push_peer(
            MagicMock(), "default", route, head
        )
        second = await self.authority.engine._push_peer(
            MagicMock(), "default", route, head
        )

        self.assertEqual(first["status"], "unavailable")
        self.assertEqual(second["status"], "reachable")
        self.assertEqual(second["objects_sent"], 0)
        self.assertEqual(self.target.log.get_head("default"), head)

    async def test_cancelled_preparation_failure_is_observed_and_retired(self) -> None:
        card = self._shared_card()
        head = self._update(self.authority, card.id, title="Cancelled")
        contexts: list[dict] = []
        loop = asyncio.get_running_loop()
        previous = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: contexts.append(context))

        def delayed_failure(_head: str) -> dict[str, str]:
            time.sleep(0.05)
            raise ValueError("collector failed after waiter cancellation")

        try:
            self.authority.engine._collect_objects = delayed_failure
            waiter = asyncio.create_task(
                self.authority.engine._prepare_objects("default", head)
            )
            await asyncio.sleep(0.01)
            waiter.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await waiter
            async with asyncio.timeout(2):
                while self.authority.engine._preparing:
                    await asyncio.sleep(0.01)
            self.assertEqual(self.authority.engine._preparing, {})
            diagnostics = self.authority.engine.status("default")[
                "object_preparation"
            ]
            self.assertEqual(diagnostics["phase"], "failed")
            self.assertEqual(diagnostics["active_residual_work"], 0)
            self.assertEqual(contexts, [])
        finally:
            loop.set_exception_handler(previous)

    async def test_legacy_oversize_is_actionable_and_does_not_advance_ref(self) -> None:
        card = self._shared_card()
        head = self._update(self.authority, card.id, title="Too large for legacy")
        self.network.legacy_need.add("target")
        route = self.authority.peers.routes_for_realm("default")[0]
        with patch("pa.sync.engine.MAX_SYNC_OBJECTS", 1):
            result = await self.authority.engine._push_peer(
                MagicMock(), "default", route, head
            )

        self.assertEqual(result["status"], "protocol_incompatible")
        self.assertEqual(result["error"]["code"], "legacy_bundle_too_large")
        self.assertNotEqual(self.target.log.get_head("default"), head)

    async def test_prepared_objects_are_single_flight_and_reused_per_head(self) -> None:
        card = self._shared_card()
        head = self._update(self.authority, card.id, title="Prepared once")
        original = self.authority.engine._collect_objects
        calls = 0

        def counted(head_hash: str) -> dict[str, str]:
            nonlocal calls
            calls += 1
            time.sleep(0.02)
            return original(head_hash)

        self.authority.engine._collect_objects = counted
        prepared = await asyncio.gather(
            *(
                self.authority.engine._prepare_objects("default", head)
                for _ in range(8)
            )
        )
        warm_started = time.perf_counter()
        warm = await self.authority.engine._prepare_objects("default", head)

        self.assertEqual(calls, 1)
        self.assertTrue(all(item is warm for item in prepared))
        self.assertLess(time.perf_counter() - warm_started, 0.2)
        self.assertEqual(
            self.authority.engine.status("default")["object_preparation"]["builds"],
            1,
        )

    async def test_unchanged_peers_receive_no_objects(self) -> None:
        self._shared_card()
        transferred: list[int] = []
        original_response = self.network.response

        def observe(method: str, url: str, **kwargs) -> httpx.Response:
            if urlsplit(url).path == "/api/sync/push":
                transferred.append(len((kwargs.get("json") or {}).get("objects", {})))
            return original_response(method, url, **kwargs)

        self.network.response = observe
        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "converged")
        self.assertEqual(transferred, [0, 0])
        self.assertEqual(
            self.authority.engine.status("default")["object_preparation"]["builds"], 0
        )

    async def test_legacy_peer_falls_back_to_complete_bundle(self) -> None:
        card = Card(id="legacy-card", title="Legacy")
        _, commit = self.authority.log.append_event(
            CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id=card.id,
                author_principal="user:test",
                author_instance="authority",
                payload=card.model_dump(mode="json"),
            )
        )
        self.network.legacy_need.add("target")
        self.authority.engine._open_client()
        assert self.authority.engine._client is not None
        result = await self.authority.engine._push_peer(
            self.authority.engine._client,
            "default",
            self.authority.peers.routes_for_realm("default")[0],
            commit.hash,
        )

        self.assertEqual(result["status"], "reachable")
        self.assertEqual(self.target.log.get_head("default"), commit.hash)

    async def test_ten_thousand_commit_merge_history_is_iterative_and_bounded(
        self,
    ) -> None:
        event = CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id="default",
            card_id="stress-card",
            author_principal="user:test",
            author_instance="authority",
            payload={"title": "stress"},
        )
        event_hash = self.authority.objects.put_json(event.model_dump(mode="json"))
        parent: str | None = None
        merge_parent: str | None = None
        for index in range(10_000):
            parents = [parent] if parent else []
            if merge_parent and index % 1_000 == 0:
                parents.append(merge_parent)
            commit = SyncCommit(
                hash="",
                realm_id="default",
                instance_id="authority",
                parent_hashes=parents,
                event_hashes=[event_hash],
                author_principal="user:test",
            )
            commit.hash = self.authority.objects.put_json(
                commit.model_dump(mode="json")
            )
            if index % 1_000 == 500:
                merge_parent = commit.hash
            parent = commit.hash

        collected = self.authority.engine._collect_objects(parent or "")

        self.assertEqual(len(collected), 10_001)
        self.assertIn(parent, collected)
        self.assertLessEqual(
            sum(len(value) for value in collected.values()), 128 * 1024 * 1024
        )

    async def test_object_collection_handles_history_deeper_than_recursion_limit(
        self,
    ) -> None:
        parent: str | None = None

        for index in range(1_200):
            event = CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id="default",
                card_id=f"card-{index}",
                author_principal="user:test",
                author_instance="authority",
                payload={"title": f"Card {index}"},
            )
            event_hash = self.authority.objects.put_json(
                event.model_dump(mode="json")
            )
            commit = SyncCommit(
                hash="",
                realm_id="default",
                instance_id=self.authority.settings.instance_id,
                parent_hashes=[parent] if parent else [],
                event_hashes=[event_hash],
                author_principal="user:test",
            )
            commit.hash = self.authority.objects.put_json(
                commit.model_dump(mode="json")
            )
            parent = commit.hash

        collected = self.authority.engine._collect_objects(parent or "")

        self.assertEqual(len(collected), 2_400)
        self.assertIn(parent, collected)

    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        root = Path(self.temp.name)
        self.authority = _Node(root, "authority", "MacBook")
        self.target = _Node(root, "target", "Monica")
        self.observer = _Node(root, "observer", "Mac mini")
        self.nodes = [self.authority, self.target, self.observer]
        for node in self.nodes:
            for peer in self.nodes:
                node.fleet.upsert_instance(
                    FleetInstance(
                        instance_id=peer.settings.instance_id,
                        name=peer.settings.instance_name,
                        url=peer.url,
                    )
                )
                if node is not peer:
                    node.peers.add_route(
                        PeerRoute(
                            realm_id="default",
                            target_url=peer.url,
                            target_instance_id=peer.settings.instance_id,
                        )
                    )
        self.network = _SyncNetwork(self.nodes)
        self.network_patch = patch(
            "pa.sync.engine.httpx.AsyncClient", side_effect=self.network.client
        )
        self.network_patch.start()

    def tearDown(self) -> None:
        self.network_patch.stop()
        self.temp.cleanup()

    def _copy_objects(self, source: _Node, target: _Node) -> None:
        for object_hash in source.objects.list_hashes():
            data = source.objects.get(object_hash)
            assert data is not None
            target.objects.put(data)

    def _shared_card(self) -> Card:
        card = Card(id="card-1", title="Base")
        _, base = self.authority.log.append_event(
            CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id=card.id,
                author_principal="user:test",
                author_instance="authority",
                payload=card.model_dump(mode="json"),
            )
        )
        for node in (self.target, self.observer):
            self._copy_objects(self.authority, node)
            node.log.advance_ref("default", base.hash, expected_head=None)
        return card

    def _update(self, node: _Node, card_id: str, **fields) -> str:
        _, commit = node.log.append_event(
            CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id="default",
                card_id=card_id,
                author_principal="user:test",
                author_instance=node.settings.instance_id,
                payload=fields,
            )
        )
        return commit.hash

    async def test_three_node_compatible_divergence_converges_and_propagates(
        self,
    ) -> None:
        card = self._shared_card()
        original_heads = {
            self._update(self.authority, card.id, title="Automatic convergence"),
            self._update(self.target, card.id, body="Keep both histories"),
            self._update(self.observer, card.id, lane=CardLane.ACTIVE.value),
        }

        state = await self.authority.engine.converge_realm("default")

        heads = {node.log.get_head("default") for node in self.nodes}
        self.assertEqual(len(heads), 1)
        final_head = heads.pop()
        self.assertEqual(state["phase"], "converged")
        self.assertEqual(state["head"], final_head)
        self.assertEqual(
            {item["name"] for item in state["instances"]},
            {"MacBook", "Monica", "Mac mini"},
        )
        for old_head in original_heads:
            self.assertTrue(self.authority.log.is_ancestor(old_head, final_head))

    async def test_instance_group_versions_converge_with_both_parents_and_audit(
        self,
    ) -> None:
        authority_projection = CardProjection(
            self.authority.settings.db_path, self.authority.log
        )
        group = authority_projection.create_instance_group(
            InstanceGroupCreate(
                name="Code workers",
                included_instance_ids=["authority", "target"],
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        base = self.authority.log.get_head("default")
        assert base is not None
        for node in (self.target, self.observer):
            self._copy_objects(self.authority, node)
            node.log.advance_ref("default", base, expected_head=None)

        target_projection = CardProjection(
            self.target.settings.db_path, self.target.log
        )
        authority_projection.rebuild_from_log("default")
        target_projection.rebuild_from_log("default")
        local = authority_projection.update_instance_group(
            group.id,
            InstanceGroupUpdate(
                description="Authority description",
                expected_version=1,
            ),
            principal_id="user:admin",
            instance_id="authority",
        )
        await asyncio.sleep(0.001)
        remote = target_projection.update_instance_group(
            group.id,
            InstanceGroupUpdate(
                description="Newer target description",
                expected_version=1,
            ),
            principal_id="user:admin",
            instance_id="target",
        )
        assert local and remote
        divergent_heads = {
            self.authority.log.get_head("default"),
            self.target.log.get_head("default"),
        }

        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "converged")
        self.assertEqual(
            {node.log.get_head("default") for node in self.nodes}, {state["head"]}
        )
        for old_head in divergent_heads:
            assert old_head is not None
            self.assertTrue(self.authority.log.is_ancestor(old_head, state["head"]))
        authority_projection.rebuild_from_log("default")
        merged = authority_projection.get_instance_group(group.id, "default")
        assert merged is not None
        self.assertEqual(merged.description, "Newer target description")
        self.assertTrue(
            any(
                resolution.get("entity") == "instance_group"
                and resolution.get("field") == "description"
                and resolution.get("strategy")
                == "highest_policy_version_then_event_identity"
                for entry in self.authority.log.merge_audit("default")
                for resolution in entry["automatic_resolutions"]
            )
        )

    async def test_remote_lane_move_rebuilds_projection_and_notifies_browser(
        self,
    ) -> None:
        card = self._shared_card()
        local_projection = CardProjection(
            self.authority.settings.db_path, self.authority.log
        )
        local_projection.rebuild_from_log("default")
        broker = LiveUpdateBroker()
        broker.start()
        updates = broker.subscribe("default")

        def rebuild_and_notify(realm_id: str) -> None:
            local_projection.rebuild_from_log(realm_id)
            broker.publish(
                realm_id,
                {
                    "type": "cards_changed",
                    "realm_id": realm_id,
                    "head": self.authority.log.get_head(realm_id),
                    "source": "sync",
                },
            )

        self.authority.engine.on_head_advanced(rebuild_and_notify)
        self._update(self.target, card.id, lane=CardLane.DONE.value)

        state = await self.authority.engine.converge_realm("default")
        update = await asyncio.wait_for(updates.get(), timeout=1.0)

        synced = local_projection.get_card(card.id, realm_id="default")
        assert synced is not None
        self.assertEqual(state["phase"], "converged")
        self.assertEqual(synced.lane, CardLane.DONE)
        self.assertEqual(update["source"], "sync")
        self.assertEqual(update["head"], state["head"])

    async def test_real_card_updates_auto_merge_timestamp_and_remain_listable(
        self,
    ) -> None:
        card = self._shared_card()
        projections = {
            node.settings.instance_id: CardProjection(node.settings.db_path, node.log)
            for node in self.nodes
        }
        for node in self.nodes:
            projection = projections[node.settings.instance_id]
            projection.rebuild_from_log("default")
            node.engine.on_head_advanced(projection.rebuild_from_log)
            self.assertEqual(
                [item.id for item in projection.list_cards()],
                [card.id],
            )

        authority_card = projections["authority"].update_card(
            card.id,
            CardUpdate(title="Automatic convergence"),
            instance_id="authority",
        )
        time.sleep(0.001)
        target_card = projections["target"].update_card(
            card.id,
            CardUpdate(body="Keep both histories"),
            instance_id="target",
        )
        time.sleep(0.001)
        observer_card = projections["observer"].update_card(
            card.id,
            CardUpdate(lane=CardLane.ACTIVE),
            instance_id="observer",
        )
        assert authority_card and target_card and observer_card
        original_heads = {node.log.get_head("default") for node in self.nodes}
        expected_updated_at = max(
            authority_card.updated_at,
            target_card.updated_at,
            observer_card.updated_at,
        )

        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "converged")
        final_heads = {node.log.get_head("default") for node in self.nodes}
        self.assertEqual(final_heads, {state["head"]})
        for original_head in original_heads:
            assert original_head is not None
            self.assertTrue(
                self.authority.log.is_ancestor(original_head, state["head"])
            )
        for projection in projections.values():
            listed = projection.list_cards()
            self.assertEqual(len(listed), 1)
            merged = listed[0]
            self.assertEqual(merged.title, "Automatic convergence")
            self.assertEqual(merged.body, "Keep both histories")
            self.assertEqual(merged.lane, CardLane.ACTIVE)
            self.assertEqual(merged.updated_at, expected_updated_at)
        audit = self.authority.log.merge_audit("default")
        self.assertTrue(
            any(
                resolution.get("field") == "updated_at"
                and resolution.get("strategy") == "latest_timestamp"
                for entry in audit
                for resolution in entry["automatic_resolutions"]
            )
        )

    async def test_incompatible_values_are_named_and_manual_resolution_is_audited(
        self,
    ) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="Local title")
        remote_head = self._update(self.target, card.id, title="Remote title")
        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "conflict")
        conflict = state["conflicts"][0]
        self.assertEqual((conflict["entity"], conflict["id"]), ("card", card.id))
        self.assertEqual(conflict["field"], "title")
        self.assertEqual(conflict["local"]["value"], "Local title")
        self.assertEqual(conflict["remote"]["value"], "Remote title")
        self.assertEqual(conflict["local"]["instance_name"], "MacBook")
        self.assertEqual(conflict["remote"]["instance_name"], "Monica")

        projection = CardProjection(self.authority.settings.db_path, self.authority.log)
        projection.rebuild_from_log("default")
        self.authority.engine.on_head_advanced(projection.rebuild_from_log)
        ctx = MagicMock()
        ctx.settings = self.authority.settings
        ctx.services = {
            "membership": self.authority.membership,
            "event_log": self.authority.log,
            "sync_engine": self.authority.engine,
        }
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        request = MagicMock()
        request.state = SimpleNamespace(principal_id="user:local")
        request.app.state.ctx = ctx
        request.headers = {"Idempotency-Key": "manual-resolution-update"}
        resolution = {
            "realm_id": "default",
            "remote_head": remote_head,
            "resolutions": [
                {
                    "entity": "card",
                    "id": card.id,
                    "action": "update",
                    "fields": {"title": "Remote title"},
                }
            ],
        }
        self.assertEqual(
            projection.get_operation_outcome("manual-resolution-update")["status"],
            "not_found",
        )
        with patch("pa.modules.sync.get_store", return_value=projection):
            result = await resolve_sync_conflicts(
                request,
                resolution,
                Response(),
                "manual-resolution-update",
            )
            replay_response = Response()
            replayed = await resolve_sync_conflicts(
                request,
                resolution,
                replay_response,
                "manual-resolution-update",
            )
            self.assertEqual(replayed, result)
            self.assertEqual(
                replay_response.headers["X-PA-Operation-Replayed"], "true"
            )

        self.assertEqual(result["convergence"]["phase"], "converged")
        self.assertEqual(
            {node.log.get_head("default") for node in self.nodes}, {result["head"]}
        )
        peer_projection = CardProjection(
            self.target.settings.data_dir / "peer-operation-rebuild.db",
            self.target.log,
        )
        peer_outcome = peer_projection.get_operation_outcome(
            "manual-resolution-update", realm_id="default"
        )
        self.assertEqual(peer_outcome["status"], "succeeded")
        self.assertEqual(peer_outcome["result"], result)
        projection.rebuild_from_log("default")
        self.assertEqual(
            projection.get_operation_outcome(
                "manual-resolution-update", realm_id="default"
            )["result"],
            result,
        )
        self.assertEqual(projection.get_card(card.id).title, "Remote title")
        audit = self.authority.log.merge_audit("default")
        self.assertEqual(audit[0]["mode"], "manual")
        self.assertEqual(audit[0]["author_principal"], "user:local")
        self.assertEqual(
            set(audit[0]["parents"]), {conflict["local_head"], remote_head}
        )

    async def test_conflict_resolution_recovers_after_merge_before_outcome(self) -> None:
        card = self._shared_card()
        local = CardProjection(
            self.authority.settings.db_path, self.authority.log
        )
        local.rebuild_from_log("default")
        local.update_card(card.id, CardUpdate(title="Local crash title"))
        remote_head = self._update(
            self.target, card.id, title="Remote crash title"
        )
        state = await self.authority.engine.converge_realm("default")
        conflict = state["conflicts"][0]

        ctx = MagicMock()
        ctx.settings = self.authority.settings
        ctx.services = {
            "membership": self.authority.membership,
            "event_log": self.authority.log,
            "sync_engine": self.authority.engine,
        }
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        request = MagicMock()
        request.state = SimpleNamespace(principal_id="user:local")
        request.app.state.ctx = ctx
        request.headers = {"Idempotency-Key": "manual-resolution-crash"}
        resolution = {
            "realm_id": "default",
            "remote_head": remote_head,
            "resolutions": [
                {
                    "entity": "card",
                    "id": card.id,
                    "action": "update",
                    "fields": {"title": conflict["remote"]["value"]},
                }
            ],
        }
        with (
            patch("pa.modules.sync.get_store", return_value=local),
            patch(
                "pa.modules.sync._finalize_conflict_operation",
                side_effect=RuntimeError("crash after merge commit"),
            ),
            self.assertRaisesRegex(RuntimeError, "crash after merge"),
        ):
            await resolve_sync_conflicts(
                request,
                resolution,
                Response(),
                "manual-resolution-crash",
            )

        durable = self.authority.log.find_operation_event(
            "default", "manual-resolution-crash"
        )
        self.assertIsNotNone(durable)
        self.assertFalse(durable[2].operation_result_complete)

        restarted = CardProjection(
            self.authority.settings.data_dir / "conflict-restart.db",
            self.authority.log,
        )
        pending = restarted.get_operation_outcome(
            "manual-resolution-crash", realm_id="default"
        )
        self.assertEqual(pending["status"], "resumable")
        self.assertTrue(pending["durable"])
        self.assertEqual(
            pending["recovery_state"], "durable_append_resume_required"
        )
        self.assertEqual(
            pending["recovery_action"], "retry_same_operation_with_same_key"
        )
        self.authority.engine.on_head_advanced(restarted.rebuild_from_log)
        with patch("pa.modules.sync.get_store", return_value=restarted):
            recovered_response = Response()
            recovered = await resolve_sync_conflicts(
                request,
                resolution,
                recovered_response,
                "manual-resolution-crash",
            )
            replayed = await resolve_sync_conflicts(
                request,
                resolution,
                Response(),
                "manual-resolution-crash",
            )

        self.assertEqual(replayed, recovered)
        self.assertEqual(
            recovered_response.headers["X-PA-Operation-Replayed"], "true"
        )
        self.assertEqual(recovered["resolution_head"], durable[0])
        self.assertEqual(recovered["convergence"]["phase"], "converged")
        fresh_peer = CardProjection(
            self.target.settings.data_dir / "crash-peer-rebuild.db",
            self.target.log,
        )
        self.assertEqual(
            fresh_peer.get_operation_outcome(
                "manual-resolution-crash", realm_id="default"
            )["result"],
            recovered,
        )

    async def test_conflict_resolution_fences_overlapping_same_key_retry(self) -> None:
        card = self._shared_card()
        local = CardProjection(
            self.authority.settings.db_path, self.authority.log
        )
        local.rebuild_from_log("default")
        local.update_card(card.id, CardUpdate(title="Local overlap title"))
        remote_head = self._update(
            self.target, card.id, title="Remote overlap title"
        )
        state = await self.authority.engine.converge_realm("default")
        conflict = state["conflicts"][0]

        ctx = MagicMock()
        ctx.settings = self.authority.settings
        ctx.services = {
            "membership": self.authority.membership,
            "event_log": self.authority.log,
            "sync_engine": self.authority.engine,
        }
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        request = MagicMock()
        request.state = SimpleNamespace(principal_id="user:local")
        request.app.state.ctx = ctx
        request.headers = {"Idempotency-Key": "manual-resolution-overlap"}
        resolution = {
            "realm_id": "default",
            "remote_head": remote_head,
            "resolutions": [
                {
                    "entity": "card",
                    "id": card.id,
                    "action": "update",
                    "fields": {"title": conflict["remote"]["value"]},
                }
            ],
        }
        first_convergence = asyncio.Event()
        release_convergence = asyncio.Event()
        real_converge = self.authority.engine.converge_realm
        convergence_calls = 0

        async def fenced_convergence(realm_id: str):
            nonlocal convergence_calls
            convergence_calls += 1
            if convergence_calls == 1:
                first_convergence.set()
                await release_convergence.wait()
            return await real_converge(realm_id)

        with (
            patch("pa.modules.sync.get_store", return_value=local),
            patch.object(
                self.authority.engine,
                "converge_realm",
                side_effect=fenced_convergence,
            ),
        ):
            first = asyncio.create_task(
                resolve_sync_conflicts(
                    request,
                    resolution,
                    Response(),
                    "manual-resolution-overlap",
                )
            )
            await asyncio.wait_for(first_convergence.wait(), timeout=5)
            with self.assertRaises(HTTPException) as retry:
                await resolve_sync_conflicts(
                    request,
                    resolution,
                    Response(),
                    "manual-resolution-overlap",
                )
            self.assertEqual(retry.exception.status_code, 409)
            self.assertEqual(retry.exception.detail["code"], "operation_in_progress")
            release_convergence.set()
            result = await asyncio.wait_for(first, timeout=10)

        durable = self.authority.log.find_operation_event(
            "default", "manual-resolution-overlap"
        )
        assert durable is not None
        self.assertTrue(durable[2].operation_result_complete)
        self.assertEqual(
            local.get_operation_outcome(
                "manual-resolution-overlap", realm_id="default"
            )["result"],
            result,
        )
        attributable = []
        head = self.authority.log.get_head("default")
        assert head is not None
        for _commit_hash, commit in self.authority.log._iter_commits_parent_first(
            head
        ):
            for event_hash in commit.event_hashes:
                event = self.authority.log.get_event(event_hash)
                if event and event.idempotency_key == "manual-resolution-overlap":
                    attributable.append(event)
        self.assertEqual(len(attributable), 2)
        self.assertEqual(
            sum(event.operation_result_complete for event in attributable), 1
        )

    async def test_conflicts_from_every_divergent_peer_remain_reported(self) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="Authority")
        self._update(self.target, card.id, title="Monica")
        self._update(self.observer, card.id, title="Mac mini")

        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "conflict")
        self.assertEqual(len({item["remote_head"] for item in state["conflicts"]}), 2)
        self.assertEqual(
            {item["peer"]["name"] for item in state["conflicts"]},
            {"Monica", "Mac mini"},
        )

    async def test_unavailable_peer_is_retried_and_eventually_adopts_merge(
        self,
    ) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="new")
        self.network.unavailable.add("observer")

        degraded = await self.authority.engine.converge_realm("default")
        self.assertEqual(degraded["phase"], "degraded")
        repaired_head = self.authority.log.get_head("default")
        self.assertEqual(self.target.log.get_head("default"), repaired_head)
        self.assertNotEqual(self.observer.log.get_head("default"), repaired_head)

        self.network.unavailable.clear()
        converged = await self.authority.engine.converge_realm("default")
        self.assertEqual(converged["phase"], "converged")
        self.assertEqual(
            {node.log.get_head("default") for node in self.nodes}, {repaired_head}
        )

    async def test_delete_edit_resolution_restores_intentionally_active_card(
        self,
    ) -> None:
        card = self._shared_card()
        _, deleted = self.authority.log.append_event(
            CardEvent(
                type=EventType.CARD_DELETED,
                realm_id="default",
                card_id=card.id,
                author_principal="user:test",
                author_instance="authority",
            )
        )
        remote_head = self._update(
            self.target, card.id, lane=CardLane.ACTIVE.value, title="Keep active"
        )
        state = await self.authority.engine.converge_realm("default")
        conflict = state["conflicts"][0]
        self.assertEqual(conflict["field"], "__terminal__")
        self.assertEqual(conflict["remote"]["snapshot"]["lane"], "active")

        projection = CardProjection(self.authority.settings.db_path, self.authority.log)
        projection.rebuild_from_log("default")
        ctx = MagicMock()
        ctx.settings = self.authority.settings
        ctx.services = {
            "membership": self.authority.membership,
            "event_log": self.authority.log,
            "sync_engine": self.authority.engine,
        }
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        request = MagicMock()
        request.state = SimpleNamespace(principal_id="user:local")
        request.app.state.ctx = ctx
        with patch("pa.modules.sync.get_store", return_value=projection):
            result = await resolve_sync_conflicts(
                request,
                {
                    "realm_id": "default",
                    "remote_head": remote_head,
                    "resolutions": [
                        {
                            "entity": "card",
                            "id": card.id,
                            "action": "upsert",
                            "fields": conflict["remote"]["snapshot"],
                        }
                    ],
                },
                Response(),
                "manual-resolution-upsert",
            )

        restored = projection.get_card(card.id)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.lane, CardLane.ACTIVE)
        self.assertEqual(restored.title, "Keep active")
        self.assertTrue(self.authority.log.is_ancestor(deleted.hash, result["head"]))
        self.assertTrue(self.authority.log.is_ancestor(remote_head, result["head"]))

    async def test_stale_head_compare_and_swap_retries(self) -> None:
        card = self._shared_card()
        remote_head = self._update(self.target, card.id, title="advanced")
        original = self.authority.log.advance_ref
        attempts = 0

        def flaky_advance(realm_id, commit_hash, *, expected_head=...):
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise StaleSyncHeadError(
                    realm_id, expected_head, self.authority.log.get_head(realm_id)
                )
            return original(realm_id, commit_hash, expected_head=expected_head)

        with patch.object(self.authority.log, "advance_ref", side_effect=flaky_advance):
            state = await self.authority.engine.converge_realm("default")
        self.assertEqual(state["phase"], "converged")
        self.assertGreaterEqual(attempts, 2)
        self.assertEqual(self.authority.log.get_head("default"), remote_head)

    async def test_peer_that_omits_its_head_never_marks_realm_converged(self) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="must be acknowledged")
        self.network.omit_push_head.add("target")

        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "degraded")
        target = next(item for item in state["instances"] if item["name"] == "Monica")
        self.assertEqual(target["status"], "missing_head")

    async def test_peer_that_rejects_propagation_never_marks_realm_converged(
        self,
    ) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="racing update")
        self.network.reject_push.add("target")

        state = await self.authority.engine.converge_realm("default")

        self.assertEqual(state["phase"], "retrying")
        target = next(item for item in state["instances"] if item["name"] == "Monica")
        self.assertEqual(target["status"], "conflict")

    async def test_dispatch_health_succeeds_after_automatic_repair(self) -> None:
        card = self._shared_card()
        self._update(self.authority, card.id, title="authority")
        self._update(self.target, card.id, body="target")
        projection = CardProjection(self.authority.settings.db_path, self.authority.log)
        projection.rebuild_from_log("default")
        self.authority.engine.on_head_advanced(projection.rebuild_from_log)
        self.authority.settings.peers = [self.target.url, self.observer.url]
        ctx = MagicMock()
        ctx.settings = self.authority.settings
        ctx.store = projection
        ctx.services = {
            "sync_engine": self.authority.engine,
            "event_log": self.authority.log,
            "fleet_registry": self.authority.fleet,
        }
        writer_lock = DataDirWriterLock(self.authority.settings.data_dir)
        writer_lock.acquire()
        ctx.services["writer_lock"] = writer_lock
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        ctx.register_service.side_effect = lambda name, value: ctx.services.__setitem__(
            name, value
        )
        request = MagicMock()
        request.app.state.ctx = ctx

        await _assert_dispatch_sync_health(request, "default")
        self.assertEqual(
            {node.log.get_head("default") for node in self.nodes},
            {self.authority.log.get_head("default")},
        )
        peer_agent = AsyncMock(
            side_effect=[
                {"session": {"id": "remote-session", "title": card.title}},
                {
                    "started": True,
                    "queued": False,
                    "accepted": True,
                    "accepted_event": "queue_enqueued",
                    "session_id": "remote-session",
                },
            ]
        )

        async def acknowledge_materialization(_request, _target, payload):
            return {
                "resolvable": True,
                "dispatch_id": payload["dispatch_id"],
                "card_id": payload["card"]["id"],
                "card_version": payload["card_version"],
            }

        materialize = AsyncMock(side_effect=acknowledge_materialization)
        try:
            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.get_principal_id", return_value="user:local"),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
                patch("pa.modules.fleet._peer_dispatch_json", materialize),
            ):
                result = await start_remote_agent_work(
                    request,
                    self.target.settings.instance_id,
                    RemoteAgentStartBody(
                        card_id=card.id,
                        message="Continue",
                        idempotency_key="realm-repair-dispatch",
                        execution_contract={
                            "version": 1,
                            "profile": "research",
                            "confirmed": True,
                        },
                    ),
                )
                from pa.modules.fleet import _process_remote_dispatch

                app = MagicMock()
                app.state.ctx = ctx
                record = ctx.services["dispatch_store"].get(result["dispatch_id"])
                peer_agent.side_effect = [
                    {"session": {"id": "remote-session", "title": card.title}},
                    {
                        "started": True,
                        "queued": False,
                        "accepted": True,
                        "accepted_event": "queue_enqueued",
                        "session_id": "remote-session",
                        "dispatch_id": result["dispatch_id"],
                    },
                ]
                await _process_remote_dispatch(app, record)
                record = ctx.services["dispatch_store"].get(result["dispatch_id"])
        finally:
            dispatch_store = ctx.services.get("dispatch_store")
            if dispatch_store:
                dispatch_store.close()
            writer_lock.release()
        dispatched_card = materialize.await_args.args[2]["card"]
        self.assertEqual(dispatched_card["title"], "authority")
        self.assertEqual(dispatched_card["body"], "target")
        self.assertEqual(record.session_id, "remote-session")
        self.assertEqual(record.state, "running")


class RealmSyncWebUiTests(unittest.TestCase):
    def test_fleet_ui_exposes_recovery_progress_resolution_and_dispatch_retry(
        self,
    ) -> None:
        template = Path("src/pa/server/templates/pages/fleet.html").read_text()
        script = Path("src/pa/server/static/js/fleet.js").read_text()
        self.assertIn("Realm sync", template)
        self.assertIn('id="pa-sync-instances"', template)
        self.assertIn('id="pa-sync-resolution-form"', template)
        self.assertIn("/api/sync/conflicts/resolve", script)
        self.assertIn("Open realm sync recovery", script)
        self.assertIn("data-remote-dispatch-retry", script)
        self.assertIn('id="pa-sync-conflict-head"', script)
        self.assertIn("Other divergent peer heads remain queued", script)
        refresh_handler = script.split('if (e.target.closest("#pa-sync-refresh"))', 1)[
            1
        ]
        self.assertIn("startSyncConvergence()", refresh_handler.split("return;", 1)[0])

    def test_realm_sync_reads_require_membership(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            node = _Node(Path(tmp), "access-node", "Access node")
            ctx = MagicMock()
            ctx.settings = node.settings
            ctx.services = {
                "membership": node.membership,
                "sync_engine": node.engine,
            }
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            request = MagicMock()
            request.state = SimpleNamespace(principal_id="user:outsider")
            request.app.state.ctx = ctx
            with self.assertRaises(HTTPException) as raised:
                get_sync_convergence(request, "default")
            self.assertEqual(raised.exception.status_code, 403)

    def test_fleet_route_renders_recovery_surface_and_live_status_contract(
        self,
    ) -> None:
        reset_settings()
        reset_store()
        reset_infrastructure()
        reset_instance_agent()
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="ui-node",
                instance_name="UI node",
                agent_enabled=False,
                peers=[],
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                page = client.get("/fleet?section=sync")
                self.assertEqual(page.status_code, 200)
                self.assertIn('data-section="sync"', page.text)
                self.assertIn("Record resolution and converge", page.text)
                status = client.get("/api/sync/convergence?realm=default")
                self.assertEqual(status.status_code, 200)
                self.assertEqual(status.json()["realm_id"], "default")
                self.assertIn(status.json()["phase"], {"idle", "converged"})
        reset_instance_agent()
        reset_infrastructure()
        reset_store()
        reset_settings()


class SyncObjectListTests(unittest.IsolatedAsyncioTestCase):
    async def test_converge_lists_local_hashes_once_per_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_name="Local",
                instance_url="http://local",
                subscribed_realms=["default"],
                agent_enabled=False,
            )
            store = MagicMock()
            store.list_hashes.return_value = ["abc"]
            log = MagicMock()
            log.get_head.return_value = None
            peer_table = MagicMock()
            peer_table.prefer_same_zone.return_value = [
                PeerRoute(realm_id="default", target_url="http://peer-a"),
                PeerRoute(realm_id="default", target_url="http://peer-b"),
                PeerRoute(realm_id="default", target_url="http://peer-c"),
            ]
            engine = SyncEngine(
                settings,
                store,
                log,
                peer_table,
                MagicMock(),
            )
            fetches: list[list[str] | None] = []

            async def fetch(_client, _realm_id, route, *, local_hashes=None):
                fetches.append(local_hashes)
                return {
                    "instance_id": route.target_url,
                    "name": route.target_url,
                    "url": route.target_url,
                    "status": "unavailable",
                    "head": None,
                    "imported": 0,
                }

            engine._client = object()
            engine._fetch_peer = fetch
            await engine.converge_realm("default")
            store.list_hashes.assert_called_once()
            self.assertEqual(fetches, [["abc"], ["abc"], ["abc"]])


if __name__ == "__main__":
    unittest.main()
