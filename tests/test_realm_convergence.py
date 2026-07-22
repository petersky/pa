from __future__ import annotations

import base64
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch
from urllib.parse import urlsplit

import httpx
from fastapi import HTTPException
from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import (
    Card,
    CardEvent,
    CardLane,
    EventType,
    FleetInstance,
    PeerRoute,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.modules.fleet import (
    RemoteAgentStartBody,
    _assert_dispatch_sync_health,
    start_remote_agent_work,
)
from pa.modules.sync import get_sync_convergence, resolve_sync_conflicts
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventLog, StaleSyncHeadError
from pa.sync.object_store import ObjectStore
from pa.sync.infrastructure import reset_infrastructure
from pa.instance.agent_session import reset_instance_agent


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


class _SyncNetwork:
    def __init__(self, nodes: list[_Node]) -> None:
        self.nodes = {urlsplit(node.url).hostname: node for node in nodes}
        self.unavailable: set[str] = set()
        self.omit_push_head: set[str] = set()
        self.reject_push: set[str] = set()

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
            result = node.engine._reconcile_remote_head(
                body.get("realm_id", "default"), body["head_hash"]
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
                            "action": "update",
                            "fields": {"title": "Remote title"},
                        }
                    ],
                },
            )

        self.assertEqual(result["convergence"]["phase"], "converged")
        self.assertEqual(
            {node.log.get_head("default") for node in self.nodes}, {result["head"]}
        )
        projection.rebuild_from_log("default")
        self.assertEqual(projection.get_card(card.id).title, "Remote title")
        audit = self.authority.log.merge_audit("default")
        self.assertEqual(audit[0]["mode"], "manual")
        self.assertEqual(audit[0]["author_principal"], "user:local")
        self.assertEqual(
            set(audit[0]["parents"]), {conflict["local_head"], remote_head}
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
        materialize = AsyncMock(return_value={"resolvable": True})
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


if __name__ == "__main__":
    unittest.main()
