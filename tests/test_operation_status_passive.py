"""Authenticated receipt reads during real damaged-history recovery and load."""
from __future__ import annotations

import asyncio
import threading
import json
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI

from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.core.kernel import Kernel, _SyncRecoveryAdmissionMiddleware
from pa.core.operation_dependencies import local_operational
from pa.core.operation_status import OperationStatusService
from pa.domain.models import AgentSession, RestartHandoff
from pa.domain.models import CardCreate
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.modules.items import router as items_router
from pa.modules.sync import router as sync_router
from pa.modules.instance import router as instance_router
from tests.test_sync_recovery_owned import Harness


@pytest.mark.asyncio
async def test_completed_lookup_reacquires_durable_repair_capacity(tmp_path):
    from pa.core.async_runtime import BlockingQueueFull

    service = OperationStatusService(tmp_path, capacity=1)

    async def finished(_runtime):
        return {"status": "not_found"}

    try:
        first = service.admission("canonical", "realm", "first", "head-1")
        service.schedule(first, finished)
        await asyncio.gather(*service.tasks.values())
        second = service.admission("canonical", "realm", "second", "head-1")
        with pytest.raises(BlockingQueueFull):
            service.admission("canonical", "realm", "first", "head-2")
        assert service.read_job("canonical", "realm", "first")["state"] == "completed"
        service.schedule(second, finished)
        await asyncio.gather(*service.tasks.values())
        resumed = service.admission("canonical", "realm", "first", "head-2")
        assert resumed["id"] == first["id"]
        assert resumed["state"] == "queued"
    finally:
        await service.close()


def application(h):
    app = FastAPI()
    from pa.sync.compaction import SyncMetrics
    h.services.setdefault("sync_engine", h.engine)
    h.services.setdefault("sync_metrics", SyncMetrics(h.settings.data_dir))
    app.state.ctx = SimpleNamespace(settings=h.settings, services=h.services, store=h.store,
                                   require_service=h.services.__getitem__)
    app.include_router(items_router, prefix="/api")
    app.include_router(sync_router, prefix="/api")
    app.include_router(instance_router, prefix="/api")

    @app.post("/api/test-journal-report")
    @local_operational
    async def report():
        return {"recorded": True}

    app.add_middleware(_SyncRecoveryAdmissionMiddleware, ctx=app.state.ctx, routes=app.routes)
    users = UserDirectory(h.settings.data_dir)
    token = users.ensure_default_user().cli_token
    app.add_middleware(AuthMiddleware, settings=h.settings, users=users,
                       sessions=SessionManager(h.settings.session_secret))
    Kernel(app.state.ctx, None)._install_runtime_error_handlers(app)
    return app, {"Authorization": f"Bearer {token}"}


@pytest.mark.asyncio
async def test_known_owner_receipts_bypass_held_canonical_repair_and_saturated_workers(tmp_path):
    h = Harness(tmp_path)
    status = OperationStatusService(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    h.services.update(operation_status=status, dispatch_store=ledger)
    h.store.save_session(AgentSession(id="session", agent_name="codex"))
    handoff = h.store.create_restart_handoff(RestartHandoff(
        session_id="session", idempotency_key="restart-key", continuation_prompt="private",
        continuation_prompt_id="continuation", status="failed", failure_stage="resuming",
    ))
    record = ledger.put(DispatchRecord(dispatch_id="dispatch", mutation_id="mutation",
        idempotency_key="dispatch-key", request_fingerprint="fingerprint", state="acknowledged",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local"))
    app, auth = application(h)
    release, entered = threading.Event(), threading.Event()

    def held_repair():
        with h.store.mutation(), h.recovery._state_lock:
            entered.set()
            assert release.wait(10)

    worker = asyncio.create_task(h.runtime.run_blocking("held-canonical", held_repair,
                                                       wait_for_completion=True))
    ordinary = []
    try:
        assert await asyncio.to_thread(entered.wait, 2)
        for n in range(h.runtime.max_workers - 1):
            ordinary.append(asyncio.create_task(h.runtime.run_blocking(
                f"ordinary-{n}", release.wait, 10, wait_for_completion=True)))
        with patch("pa.modules.items.get_store", return_value=h.store), patch.object(
                h.log, "find_operation_event", side_effect=AssertionError("GET scanned history")):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                for key, owner, expected in [("restart-key", "restart", handoff.id),
                                             ("dispatch-key", "dispatch", record.dispatch_id)]:
                    responses = await asyncio.wait_for(asyncio.gather(*[
                        client.get(f"/api/operations/{key}", params={"owner": owner}) for _ in range(6)
                    ]), 5)
                    assert all(r.status_code == 200 for r in responses)
                    assert all(expected in r.text for r in responses)
                    assert "private" not in responses[0].text
                assert not status.tasks
                metrics = status.reads.snapshot()["operations"]["operation.outcome_read"]
                assert metrics["max_active"] <= 2
    finally:
        release.set()
        await asyncio.gather(worker, *ordinary)
        await status.close()
        await h.close()


@pytest.mark.asyncio
async def test_repeated_pending_polls_one_durable_owner_and_late_result(tmp_path):
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    h.services["operation_status"] = service
    app, auth = application(h)
    h.store.begin_operation(idempotency_key="pending", operation="card.create",
                            request_fingerprint="same", realm_id="default", correlation_id="lost-ack")
    entered, release = threading.Event(), threading.Event()
    original = h.store.get_operation_outcome
    calls = []

    def repair(*args, **kwargs):
        calls.append(args)
        entered.set()
        assert release.wait(10)
        return original(*args, **kwargs)

    try:
        with patch("pa.modules.items.get_store", return_value=h.store), patch.object(h.store, "get_operation_outcome", side_effect=repair):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                first = await client.get("/api/operations/pending", params={"owner": "canonical"})
                assert first.json()["status"] == "pending"
                assert await asyncio.to_thread(entered.wait, 2)
                more = await asyncio.gather(*[client.get("/api/operations/pending", params={"owner": "canonical"}) for _ in range(6)])
                ids = {r.json()["reconciliation"]["id"] for r in [first, *more]}
                assert len(ids) == len(calls) == len(service.tasks) == 1
                assert all(r.json()["status"] == "pending" for r in more)
                release.set()
                await asyncio.gather(*service.tasks.values())
    finally:
        release.set()
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_real_bad_realm_keeps_healthy_realm_and_authenticated_local_seam_available(tmp_path):
    h = Harness(tmp_path)
    h.settings.subscribed_realms = ["default", "healthy"]
    h.services["membership"].ensure_owner_membership("healthy", "local")
    app, auth = application(h)
    h.fetch_release.clear()
    h.diagnose()
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
                denied = await client.post("/api/test-journal-report", json={})
                assert denied.status_code in {401, 403}
                allowed = await client.post("/api/test-journal-report", json={}, headers=auth)
                assert allowed.status_code == 200
                control_status = await client.get("/api/agent/quiesce", headers=auth)
                assert control_status.status_code == 200
                blocked = await client.post("/api/cards", json={"realm_id": "default", "title": "blocked"}, headers={**auth, "Idempotency-Key": "blocked"})
                assert blocked.status_code == 503
                # Real canonical create path: healthy B remains independently writable.
                healthy = await client.post("/api/cards", json={"realm_id": "healthy", "title": "healthy", "summary": "provided", "auto_enrich": False}, headers={**auth, "Idempotency-Key": "healthy"})
                assert healthy.status_code == 201, healthy.text
                card_path = f"/api/cards/{healthy.json()['id']}"
                updated = await client.patch(card_path, params={"realm": "healthy"}, json={"lane": "active"}, headers={**auth, "Idempotency-Key": "healthy-update"})
                assert updated.status_code == 200, updated.text
                duplicate_realm = await client.patch(card_path + "?realm=healthy&realm=default", json={"lane": "done"}, headers={**auth, "Idempotency-Key": "ambiguous-realm"})
                assert duplicate_realm.status_code == 503
                for path, body in [("/api/cards", {"title": "missing realm"}),
                                   ("/api/cards", {"realm_id": "unknown", "title": "unknown"}),
                                   ("/api/sync/push", {"realm_id": "default"}),
                                   ("/api/auth/login", {})]:
                    headers = {**auth, "Idempotency-Key": "blocked-other"}
                    if path == "/api/sync/push":
                        headers["Authorization"] = "Bearer test-peer-token"
                    response = await client.post(path, json=body, headers=headers)
                    assert response.status_code == 503
                legacy_check = await client.get("/api/sync/check", params={"realm": "default"}, headers=auth)
                assert legacy_check.status_code == 503
                # Healthy B's push still passes the actual hash/ref validator.
                pushed = await client.post("/api/sync/push", json={
                    "realm_id": "healthy", "head_hash": h.log.get_head("healthy"), "objects": {},
                }, headers={"Authorization": "Bearer test-peer-token"})
                assert pushed.status_code == 200, pushed.text
                malformed_push = await client.post("/api/sync/push", json={
                    "realm_id": "healthy", "head_hash": "not-a-hash", "objects": {},
                }, headers={"Authorization": "Bearer test-peer-token"})
                assert malformed_push.status_code == 400
                assert h.recovery.degraded("default") and not h.recovery.degraded("healthy")
    finally:
        await h.close()


@pytest.mark.asyncio
async def test_legacy_collisions_and_typed_fingerprint_owner_realm_conflicts(tmp_path):
    h = Harness(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    service = OperationStatusService(tmp_path)
    h.services.update(dispatch_store=ledger, operation_status=service)
    ledger.put(DispatchRecord(dispatch_id="dispatch", mutation_id="mutation", idempotency_key="key",
        request_fingerprint="original", authority_instance_id="local", authority_url="http://local",
        target_instance_id="local", state="acknowledged"))
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                for params in [{"owner": "canonical"}, {"owner": "dispatch", "request_fingerprint": "different"},
                               {"operation": "dispatch.followup"}]:
                    r = await client.get("/api/operations/key", params=params)
                    assert r.status_code == 409 and r.json()["detail"]["code"] == "operation_identity_conflict"
                good = await client.get("/api/operations/key", params={"owner": "dispatch", "request_fingerprint": "original"})
                assert good.status_code == 200 and good.json()["status"] == "accepted"
                # Legacy owners could independently admit the same key. Never
                # use newest-record ordering to silently change its meaning.
                h.store.begin_operation(idempotency_key="key", operation="card.create",
                    request_fingerprint="canonical", realm_id="healthy", correlation_id="legacy")
                for params in [{}, {"owner": "dispatch"}]:
                    collision = await client.get("/api/operations/key", params=params)
                    assert collision.status_code == 409
                    assert collision.json()["detail"]["code"] == "operation_namespace_conflict"
                assert not service.tasks
    finally:
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_reconcile_admits_owned_recovery_before_canonical_receipt(tmp_path):
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    h.services["operation_status"] = service
    h.recovery.request_timeout = 0.01
    app, auth = application(h)
    h.fetch_release.clear()
    h.diagnose()
    try:
        with patch("pa.modules.sync.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                with patch.object(h.store, "begin_operation", side_effect=AssertionError("ordinary admission before dependency repair")):
                    response = await client.post("/api/sync/reconcile", json={"realm_id": "default"}, headers={"Idempotency-Key": "repair"})
                    assert response.status_code == 202, response.text
                    assert response.json()["pending"] and response.json()["durable"]
                h.fetch_release.set()
                await asyncio.gather(*h.recovery._jobs.values())
                with patch("pa.modules.items.get_store", return_value=h.store):
                    finished = await client.post("/api/sync/reconcile", json={"realm_id": "default"}, headers={"Idempotency-Key": "repair"})
                    assert finished.status_code == 200, finished.text
                    replay = await client.post("/api/sync/reconcile", json={"realm_id": "default"}, headers={"Idempotency-Key": "repair"})
                    assert replay.json() == finished.json()
                    assert replay.headers["X-PA-Operation-Replayed"] == "true"
    finally:
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_lost_append_ack_and_process_boundary_repair_receipt_preserve_one_effect(tmp_path):
    h = Harness(tmp_path)
    h.settings.subscribed_realms.append("healthy")
    h.services["membership"].ensure_owner_membership("healthy", "local")
    h.store.begin_operation(idempotency_key="lost-ack", operation="card.create",
        request_fingerprint="same", realm_id="healthy", correlation_id="request")
    append = h.log.append_event

    class ProcessLost(BaseException):
        pass

    def lost_ack(event, **kwargs):
        result = append(event)
        raise ProcessLost(result[1].hash)

    with patch.object(h.log, "append_event", side_effect=lost_ack), pytest.raises(ProcessLost):
        h.store.create_card(CardCreate(title="one effect", realm_id="healthy"),
            principal_id="user:local", instance_id="local", idempotency_key="lost-ack",
            request_fingerprint="same")
    # A separate process durably admits repair and dies before publishing its
    # result. Startup must preserve the job identity and show interruption.
    script = """
import json, sys
from pathlib import Path
from pa.core.operation_status import OperationStatusService
s = OperationStatusService(Path(sys.argv[1]))
j = s.admission('canonical','healthy','lost-ack')
s._record(j['id'], 'running')
print(j['id'])
"""
    process = await asyncio.to_thread(subprocess.run, [sys.executable, "-c", script, str(tmp_path)], capture_output=True, text=True, check=True)
    service = OperationStatusService(tmp_path)
    assert service.admission("canonical", "healthy", "lost-ack")["state"] == "interrupted"
    restarted = CardProjection(h.store.db_path, h.log)
    h.services["operation_status"] = service
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=restarted):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                pending = await client.get("/api/operations/lost-ack", params={"realm": "healthy", "owner": "canonical"})
                assert pending.json()["status"] == "pending"
                assert pending.json()["durable"] is None
                assert pending.json()["reconciliation"]["id"] == process.stdout.strip()
                await asyncio.gather(*service.tasks.values())
                done = await client.get("/api/operations/lost-ack", params={"realm": "healthy", "owner": "canonical"})
                assert done.json()["status"] == "succeeded", done.text
                original = done.json()["result"]
                replay = restarted.begin_operation(idempotency_key="lost-ack", operation="card.create",
                    request_fingerprint="same", realm_id="healthy", correlation_id="retry")
                assert replay == original
                assert len(restarted.list_cards(realm_id="healthy")) == 1
    finally:
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_actual_mcp_tool_uses_authenticated_passive_asgi_route(tmp_path, monkeypatch):
    from mcp.server.mcpserver import MCPServer
    from pa.mcp.tools.items import register_mcp
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    h.services["operation_status"] = service
    h.store.save_session(AgentSession(id="mcp-session", agent_name="codex"))
    h.store.create_restart_handoff(RestartHandoff(session_id="mcp-session", idempotency_key="mcp/key:legacy",
        continuation_prompt_id="mcp-continuation", status="failed"))
    app, auth = application(h)
    from pa.core.kernel import _IdentityHeadersMiddleware
    app.add_middleware(_IdentityHeadersMiddleware, instance_id="local")
    monkeypatch.setenv("PA_LOCAL_API_TOKEN", auth["Authorization"].removeprefix("Bearer "))
    monkeypatch.setenv("PA_INSTANCE_ID", "local")
    monkeypatch.delenv("PA_LOCAL_API_SOCKET", raising=False)
    loop = asyncio.get_running_loop()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as client:
        def transport(method, url, **kwargs):
            return asyncio.run_coroutine_threadsafe(client.request(method, url, **kwargs), loop).result(3)
        mcp = MCPServer("passive-status-test")
        register_mcp(mcp, app.state.ctx)
        try:
            with patch("httpx.request", side_effect=transport), patch("pa.modules.items.get_store", return_value=h.store), patch.object(
                    h.log, "find_operation_event", side_effect=AssertionError("MCP scanned history")):
                result = await asyncio.to_thread(lambda: asyncio.run(mcp.call_tool("get_operation_outcome", {
                    "idempotency_key": "mcp/key:legacy", "realm": "default", "owner": "restart"})))
                assert "failed" in str(result) and "mcp/key:legacy" in str(result)
        finally:
            await service.close()
            await h.close()


@pytest.mark.asyncio
async def test_status_client_timeout_keeps_late_worker_charged_and_truthful(tmp_path):
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    service.reads.default_timeout = 0.01
    h.services["operation_status"] = service
    h.store.save_session(AgentSession(id="late-session", agent_name="codex"))
    h.store.create_restart_handoff(RestartHandoff(session_id="late-session", idempotency_key="late",
        continuation_prompt_id="late-prompt", status="failed"))
    app, auth = application(h)
    entered, release = threading.Event(), threading.Event()
    read = h.store.read_operation_claims

    def slow(*args, **kwargs):
        entered.set()
        assert release.wait(10)
        return read(*args, **kwargs)

    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                with patch.object(h.store, "read_operation_claims", side_effect=slow):
                    request = asyncio.create_task(client.get("/api/operations/late", params={"owner": "restart"}))
                    assert await asyncio.to_thread(entered.wait, 2)
                    timed_out = await request
                    assert timed_out.status_code == 504
                    assert service.reads.snapshot()["executor"]["active"] == 1
                    assert not release.is_set() and not service.tasks
                    release.set()
                    await asyncio.gather(*service.reads._pending)
                service.reads.default_timeout = 0.5
                observed = await client.get("/api/operations/late", params={"owner": "restart"})
                assert observed.status_code == 200
                assert observed.json()["status"] == "failed"
                assert observed.json()["durable"] is True
    finally:
        release.set()
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_authenticated_plain_progress_works_but_canonical_operator_input_stays_gated(tmp_path):
    from pa.execution.progress import ProgressService
    from pa.modules.fleet import router as fleet_router
    h = Harness(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    ledger.put(DispatchRecord(dispatch_id="progress-dispatch", mutation_id="progress-mutation",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local",
        principal_id="user:local", session_id="progress-session", state="running", progress_protocol_version=1))
    progress = ProgressService(ledger, instance_id="local", token="test-peer-token", async_runtime=h.runtime)
    h.services.update(dispatch_store=ledger, progress_service=progress)
    app, auth = application(h)
    app.include_router(fleet_router, prefix="/api")
    h.fetch_release.clear()
    h.diagnose()
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
            payload = {"phase": "testing", "summary": "Tests running", "idempotency_key": "report"}
            endpoint = "/api/fleet/dispatch-jobs/progress-dispatch/checkpoint"
            reported = await client.post(endpoint, json=payload)
            assert reported.status_code == 200, reported.text
            assert reported.json()["accepted"] is True
            refused = await client.post(endpoint, json={**payload, "idempotency_key": "input", "operator_input": "Choose an option"})
            assert refused.status_code == 503
            malformed = await client.post(endpoint, json={**payload, "unexpected": "field"})
            assert malformed.status_code == 422
            assert h.recovery.degraded("default")
    finally:
        await progress.close()
        ledger.close()
        await h.close()


@pytest.mark.asyncio
async def test_legacy_resolution_never_chooses_a_ledger_over_a_lost_canonical_row(tmp_path):
    h = Harness(tmp_path)
    h.settings.subscribed_realms.append("healthy")
    h.services["membership"].ensure_owner_membership("healthy", "local")
    h.store.create_card(CardCreate(title="existing history", realm_id="healthy"),
                        principal_id="user:local", instance_id="local")
    service = OperationStatusService(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    ledger.put(DispatchRecord(dispatch_id="legacy-dispatch", mutation_id="legacy-mutation",
        idempotency_key="legacy-key", request_fingerprint="dispatch-fingerprint", realm_id="healthy",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local",
        state="acknowledged"))
    h.services.update(operation_status=service, dispatch_store=ledger)
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                path = "/api/operations/legacy-key"
                params = {"realm": "healthy"}
                typed = await client.get(path, params={**params, "owner": "dispatch"})
                assert typed.json()["status"] == "accepted" and not service.tasks
                unresolved = await client.get(path, params=params)
                assert unresolved.json()["status"] == "lookup_pending"
                assert unresolved.json()["owner"] is None
                assert unresolved.json()["observed_receipts"][0]["result"]["dispatch_id"] == "legacy-dispatch"
                job_id = unresolved.json()["reconciliation"]["id"]
                await asyncio.gather(*service.tasks.values())
                for _ in range(3):
                    resolved = await client.get(path, params=params)
                    assert resolved.json()["status"] == "accepted"
                assert service.repairs.snapshot()["operations"]["operation.canonical_repair"]["submitted"] == 1

                # The durable head advances with a same-key canonical effect,
                # then its derived receipt row is lost. A previous negative
                # proof must neither hide this claim nor choose the other ledger.
                h.store.begin_operation(idempotency_key="legacy-key", operation="card.create",
                    request_fingerprint="canonical-fingerprint", realm_id="healthy", correlation_id="lost-row")
                card = h.store.create_card(CardCreate(title="canonical effect", realm_id="healthy"),
                    principal_id="user:local", instance_id="local", idempotency_key="legacy-key",
                    request_fingerprint="canonical-fingerprint")
                with h.store._conn() as conn:
                    conn.execute("DELETE FROM mutation_operations WHERE idempotency_key=?", ("legacy-key",))
                pending = await client.get(path, params=params)
                assert pending.json()["status"] == "lookup_pending"
                assert pending.json()["reconciliation"]["id"] == job_id
                await asyncio.gather(*service.tasks.values())
                conflict = await client.get(path, params=params)
                assert conflict.status_code == 409
                assert conflict.json()["detail"]["code"] == "operation_namespace_conflict"
                assert h.store.get_card(card.id, realm_id="healthy").title == "canonical effect"
                assert len(h.store.list_cards(realm_id="healthy")) == 2
    finally:
        await service.close()
        ledger.close()
        await h.close()
