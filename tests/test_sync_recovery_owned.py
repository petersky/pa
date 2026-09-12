"""Real canonical projection, authenticated protocol, admission, and runtime ownership."""

from __future__ import annotations

import asyncio
import json
import threading
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI

from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.kernel import _SyncRecoveryAdmissionMiddleware
from pa.domain.models import CardEvent, EventType, PeerRoute, SyncCommit
from pa.domain.store import Store
from pa.fleet.membership import MembershipStore
from pa.modules.sync import router
from pa.network.peer_table import PeerTable
from pa.sync.engine import SyncEngine
from pa.sync.event_log import EventHistoryObjectError, EventLog
from pa.sync.object_store import ObjectStore
from pa.sync.recovery import SyncRecovery


def event(title="first"):
    return CardEvent(
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="card",
        author_principal="user:local",
        author_instance="local",
        payload={"title": title},
    )


def app_for(settings, services, store=None, *, gate=False):
    ctx = SimpleNamespace(
        settings=settings,
        services=services,
        store=store,
        require_service=services.__getitem__,
    )
    app = FastAPI()
    app.state.ctx = ctx
    app.include_router(router, prefix="/api")

    @app.post("/api/ordinary")
    def ordinary():
        return {"written": True}

    if gate:
        app.add_middleware(_SyncRecoveryAdmissionMiddleware, ctx=ctx)
    users = UserDirectory(settings.data_dir)
    users.ensure_default_user()
    app.add_middleware(
        AuthMiddleware,
        settings=settings,
        users=users,
        sessions=SessionManager(settings.session_secret),
    )
    return app


class Harness:
    def __init__(self, root: Path, *, count=1, missing_raw=None):
        self.settings = Settings(
            data_dir=root,
            instance_id="local",
            agent_enabled=False,
            sync_token="test-peer-token",
        )
        self.objects = ObjectStore(self.settings.objects_dir)
        self.log = EventLog(self.objects, root, "local")
        self.store = Store(root / "projection.db", self.objects, self.log)
        created = event().model_copy(
            update={
                "type": EventType.CARD_CREATED,
                "payload": {"id": "card", "realm_id": "default", "title": "first"},
            }
        )
        _, base = self.log.append_event(created)
        self.store.catch_up_projection("default", base.hash)
        parent = base.hash
        # Canonical content-addressed history, with a present merge tip and an
        # indexed first parent. No synthetic hashes or fake verifier.
        for i in range(count):
            ehash = self.objects.put_json(event(str(i)).model_dump(mode="json"))
            if missing_raw is not None and i == count - 1:
                ehash = self.objects.put(missing_raw)
            c = SyncCommit(
                hash="",
                realm_id="default",
                instance_id="local",
                parent_hashes=[parent],
                event_hashes=[ehash],
                author_principal="user:local",
            )
            parent = self.objects.put_json(c.model_dump(mode="json"))
        merge = SyncCommit(
            hash="",
            realm_id="default",
            instance_id="local",
            parent_hashes=[base.hash, parent],
            event_hashes=[],
            author_principal="user:local",
        )
        self.head = self.objects.put_json(merge.model_dump(mode="json"))
        try:
            self.log.advance_ref("default", self.head, expected_head=base.hash)
        except EventHistoryObjectError:
            if missing_raw is None:
                raise
            # advance_ref durably records the imported head before validating
            # its index; this is the actual degraded-history producer.
            assert self.log.get_head("default") == self.head
        self.hash, self.raw = ehash, self.objects.get(ehash)
        self.objects._path_for(ehash).unlink()
        self.parents = merge.parent_hashes
        peers = PeerTable(root)
        peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy"))
        self.runtime = AsyncRuntime(default_timeout=0.01, max_workers=4)
        membership = MembershipStore(root)
        membership.ensure_owner_membership("default", "local")
        self.engine = SyncEngine(
            self.settings,
            self.objects,
            self.log,
            peers,
            membership,
            async_runtime=self.runtime,
        )
        self.services = {
            "membership": membership,
            "object_store": self.objects,
            "event_log": self.log,
            "async_runtime": self.runtime,
            "sync_startup_repaired": False,
        }
        self.recovery = SyncRecovery(
            self.settings,
            self.engine,
            lambda realm, head: self.store.catch_up_projection(realm, head),
            projection_head=self.store.get_projection_head,
            on_health_change=lambda healthy: self.services.update(
                sync_startup_repaired=healthy
            ),
        )
        self.recovery.request_timeout = 0.02
        self.recovery.worker_queue_timeout = 0.01
        self.services["sync_recovery"] = self.recovery
        self.app = app_for(self.settings, self.services, self.store, gate=True)
        peer_root = root / "peer"
        peer_settings = Settings(
            data_dir=peer_root,
            instance_id="peer",
            agent_enabled=False,
            sync_token="test-peer-token",
        )
        self.peer_objects = ObjectStore(peer_settings.objects_dir)
        self.peer_objects.put(self.raw)
        self.peer_app = app_for(peer_settings, {"object_store": self.peer_objects})
        self.fetches = []
        self.peer_response = None
        self.fetch_started = asyncio.Event()
        self.fetch_release = asyncio.Event()
        self.fetch_release.set()

        async def transport(request):
            self.fetches.append(json.loads(request.content))
            self.fetch_started.set()
            await self.fetch_release.wait()
            if self.peer_response:
                return self.peer_response(request)
            return await httpx.ASGITransport(app=self.peer_app).handle_async_request(
                request
            )

        self.engine._client = httpx.AsyncClient(
            transport=httpx.MockTransport(transport)
        )
        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://local",
            headers={"Authorization": "Bearer test-peer-token"},
        )

    def diagnose(self):
        with pytest.raises(EventHistoryObjectError) as caught:
            self.store.catch_up_projection("default", self.head)
        assert caught.value.code == "missing_event"
        assert caught.value.diagnostic == {
            "object_hash": self.hash,
            "object_kind": "event",
            "realm_id": "default",
            "head_hash": self.head,
            "reference_hash": self.parents[1],
        }
        assert self.recovery.degraded()
        return caught.value

    async def close(self):
        self.fetch_release.set()
        await asyncio.gather(*self.recovery._jobs.values(), return_exceptions=True)
        await self.client.aclose()
        await self.engine._client.aclose()
        await self.runtime.close()


@pytest.mark.asyncio
async def test_real_suffix_exact_fetch_pending_concurrent_cancel_and_late_success(
    tmp_path,
):
    h = Harness(tmp_path, count=2_005)
    release, entered = threading.Event(), threading.Event()
    original = h.log.verify_index
    passes = []

    def slow_verify(realm, head):
        passes.append(head)
        assert h.objects.get(h.hash) == h.raw  # fetch happened before any full scan
        entered.set()
        assert release.wait(5)
        return original(realm, head)

    h.log.verify_index = slow_verify
    try:
        h.diagnose()
        calls = await asyncio.gather(
            *(
                h.client.post(
                    "/api/sync/recovery",
                    json={},
                    headers={"Idempotency-Key": "same-request"},
                )
                for _ in range(4)
            )
        )
        assert all(r.status_code == 200 and r.json()["pending"] for r in calls)
        ids = {r.json()["recovery"]["operation_id"] for r in calls}
        assert len(ids) == 1
        assert await asyncio.to_thread(entered.wait, 5)
        assert all(r.json()["recovered"] is None for r in calls)
        assert all(r.json()["recovery"]["active_residual_worker"] for r in calls)
        waiter = asyncio.create_task(h.recovery.retry("default"))
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
        assert (await h.client.post("/api/ordinary")).status_code == 503
        # Object-only ordinary push remains blocked, including an empty head.
        assert (
            await h.client.post("/api/sync/push", json={"head_hash": "", "objects": {}})
        ).status_code == 503
        public = json.dumps(h.recovery.public())
        assert "test-peer-token" not in public and str(tmp_path) not in public
        assert "http://healthy" not in public and "same-request" not in public
        assert passes == [h.head] and h.fetches == [{"hashes": [h.hash]}]
        release.set()
        assert await h.recovery._jobs["default"] is True
        assert h.services["sync_startup_repaired"] is True  # no follow-up request
        durable = json.loads(h.recovery.path.read_text())["realms"]["default"]
        assert durable["state"] == "healthy" and not durable["active_residual_worker"]
        assert (
            h.log.get_head("default")
            == h.store.get_projection_head("default")
            == h.head
        )
        assert h.log.get_commit(h.head).parent_hashes == h.parents
        assert (await h.client.post("/api/ordinary")).status_code == 200
        again = await h.client.post(
            "/api/sync/recovery", json={}, headers={"Idempotency-Key": "same-request"}
        )
        assert again.json()["recovered"] is True
        assert again.json()["recovery"]["operation_id"] in ids
        assert passes == [h.head] and len(h.fetches) == 1
    finally:
        release.set()
        await h.close()


@pytest.mark.asyncio
async def test_late_verification_failure_is_durable_and_same_operation_does_not_restart(
    tmp_path,
):
    h = Harness(tmp_path)
    release = threading.Event()
    calls = []

    def fail(*args):
        calls.append(1)
        release.wait(5)
        raise ValueError("private content must not leak")

    h.log.verify_index = fail
    try:
        h.diagnose()
        pending = await h.client.post(
            "/api/sync/recovery", json={}, headers={"Idempotency-Key": "operation"}
        )
        assert pending.json()["pending"]
        release.set()
        assert await h.recovery._jobs["default"] is False
        assert h.recovery.public()["state"] == "unrecoverable"
        assert not h.recovery.public()["active_residual_worker"]
        assert "private content" not in h.recovery.path.read_text()
        again = await h.client.post(
            "/api/sync/recovery", json={}, headers={"Idempotency-Key": "operation"}
        )
        assert again.json()["recovered"] is False
        assert len(calls) == 1
        assert (await h.client.post("/api/ordinary")).status_code == 503
    finally:
        release.set()
        await h.close()


@pytest.mark.asyncio
async def test_stale_head_during_fetch_rejects_install(tmp_path):
    h = Harness(tmp_path)
    h.fetch_release.clear()
    try:
        h.diagnose()
        await h.fetch_started.wait()
        # Legitimate canonical writer races an already-admitted repair.
        _, changed = h.log.append_event(event("concurrent"))
        h.fetch_release.set()
        assert await h.recovery._jobs["default"] is False
        assert h.objects.get(h.hash) is None
        assert h.log.get_head("default") == changed.hash
        assert h.recovery.degraded()
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_authentication_and_untrusted_request_proof_rejected(tmp_path):
    h = Harness(tmp_path)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=h.peer_app), base_url="http://peer"
        ) as c:
            assert (
                await c.post("/api/sync/get", json={"hashes": [h.hash]})
            ).status_code == 401
        response = await h.client.post(
            "/api/sync/recovery", json={"realm_id": "default", "object_hash": h.hash}
        )
        assert response.status_code == 422 and not h.fetches
        assert (
            await h.client.post("/api/sync/recovery", json={"realm_id": "other"})
        ).status_code == 403
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_unreferenced_evidence_rejected_without_fetch(tmp_path):
    h = Harness(tmp_path)
    try:
        failure = EventHistoryObjectError("missing_event", "a" * 64, "event")
        failure.diagnostic.update(head_hash=h.head, reference_hash=h.head)
        assert await h.recovery.recover([("default", failure)]) is False
        assert not h.fetches and h.recovery.degraded()
    finally:
        await h.close()


@pytest.mark.parametrize(
    "value",
    [
        b"not-json",
        b"[]",
        b'{"schema_version":true}',
        b'{"schema_version":2}',
        b'{"schema_version":1}',
        event().model_copy(update={"realm_id": "other"}).model_dump_json().encode(),
    ],
    ids=[
        "malformed",
        "list",
        "bool-schema",
        "future-schema",
        "missing-fields",
        "wrong-realm",
    ],
)
def test_exact_hash_still_requires_schema_and_realm(value):
    from pa.sync.object_store import object_hash

    with pytest.raises(EventHistoryObjectError):
        SyncRecovery._validate(object_hash(value), value, "event", "default")


@pytest.mark.asyncio
async def test_malformed_peer_bytes_and_hash_rejected_by_real_fetch(tmp_path):
    import base64

    h = Harness(tmp_path)
    h.peer_response = lambda request: httpx.Response(
        200,
        request=request,
        json={"objects": {h.hash: base64.b64encode(b"bad bytes").decode()}},
    )
    try:
        h.diagnose()
        await h.recovery.retry("default")
        assert await h.recovery._jobs["default"] is False
        assert h.objects.get(h.hash) is None and h.recovery.degraded()
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_other_realm_failure_survives_success(tmp_path):
    h = Harness(tmp_path)
    h.recovery._save(realm_id="other", state="unrecoverable", code="missing_parent")
    try:
        h.diagnose()
        await h.recovery.retry("default")
        assert await h.recovery._jobs["default"] is True
        assert h.recovery.public("default")["state"] == "healthy"
        assert h.recovery.public("other")["state"] == "unrecoverable"
        assert h.recovery.degraded() and not h.services["sync_startup_repaired"]
        assert (await h.client.post("/api/ordinary")).status_code == 503
    finally:
        await h.close()


@pytest.mark.parametrize(
    "raw",
    [
        b"not-json",
        b"[]",
        b'{"schema_version":2}',
        event().model_copy(update={"realm_id": "other"}).model_dump_json().encode(),
    ],
    ids=["malformed", "list", "future-schema", "wrong-realm"],
)
@pytest.mark.asyncio
async def test_referenced_exact_hash_invalid_schema_or_realm_rejected_on_fetch(
    tmp_path, raw
):
    h = Harness(tmp_path, missing_raw=raw)
    try:
        h.diagnose()
        await h.recovery.retry("default")
        assert await h.recovery._jobs["default"] is False
        assert h.fetches == [{"hashes": [h.hash]}]
        assert h.objects.get(h.hash) is None
        assert h.log.get_head("default") == h.head
        assert h.recovery.degraded()
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_stale_head_after_long_verification_does_not_reproject_or_clear(tmp_path):
    h = Harness(tmp_path)
    entered, release = threading.Event(), threading.Event()
    verify = h.log.verify_index

    def slow(realm, head):
        entered.set()
        release.wait(5)
        verify(realm, head)

    h.log.verify_index = slow
    try:
        h.diagnose()
        assert await h.recovery.retry("default") is None
        assert await asyncio.to_thread(entered.wait, 5)
        _, changed = h.log.append_event(event("new head"))
        release.set()
        assert await h.recovery._jobs["default"] is False
        assert h.log.get_head("default") == changed.hash
        assert h.store.get_projection_head("default") == h.parents[0]
        assert h.recovery.degraded()
    finally:
        release.set()
        await h.close()


@pytest.mark.asyncio
async def test_real_runtime_projection_owner_outlives_default_and_caller_deadlines(
    tmp_path,
):
    h = Harness(tmp_path)
    release, entered = threading.Event(), threading.Event()
    calls = []

    def slow():
        calls.append(1)
        entered.set()
        release.wait(5)
        return {"commits_applied": 1}

    try:
        with pytest.raises(TimeoutError):
            await h.engine.apply_realm_head(
                "default", h.head, "sync.test.projection", slow, timeout=0.02
            )
        assert entered.is_set()
        assert h.engine.projection_work_status("default")["active_residual_worker"]
        with pytest.raises(TimeoutError):
            await h.engine.apply_realm_head(
                "default", h.head, "sync.test.projection", slow, timeout=0.02
            )
        assert calls == [1]
        release.set()
        assert await h.engine.apply_realm_head(
            "default", h.head, "sync.test.projection", slow
        ) == {"commits_applied": 1}
        assert calls == [1]
    finally:
        release.set()
        await h.close()


@pytest.mark.asyncio
async def test_new_request_verifies_present_tip_even_after_previous_healthy_operation(
    tmp_path,
):
    h = Harness(tmp_path)
    try:
        h.objects.repair(h.hash, h.raw)
        await h.recovery.retry("default", request_key="first")
        assert await h.recovery._jobs["default"] is True
        first_id = h.recovery.public()["operation_id"]
        h.objects._path_for(h.hash).unlink()
        # Projection and indexed tip remain present; they cannot certify object
        # completeness. A new verification request must inspect canonical data.
        assert h.store.get_projection_head("default") == h.head
        await h.recovery.retry("default", request_key="second")
        assert await h.recovery._jobs["default"] is True
        assert h.recovery.public()["operation_id"] != first_id
        assert h.fetches == [{"hashes": [h.hash]}]
        assert h.objects.get(h.hash) == h.raw
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_completed_request_identity_is_fenced_to_its_head(tmp_path):
    h = Harness(tmp_path)
    try:
        h.objects.repair(h.hash, h.raw)
        await h.recovery.retry("default", request_key="same-head-only")
        assert await h.recovery._jobs["default"] is True
        operation_id = h.recovery.public()["operation_id"]
        _, changed = h.log.append_event(event("next generation"))
        response = await h.client.post(
            "/api/sync/recovery",
            json={},
            headers={"Idempotency-Key": "same-head-only"},
        )
        assert response.status_code == 409
        assert response.json()["detail"]["code"] == "stale_recovery_head"
        assert h.recovery.public()["operation_id"] == operation_id
        assert h.log.get_head("default") == changed.hash
        assert not h.fetches
    finally:
        await h.close()
