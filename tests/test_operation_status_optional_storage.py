"""Real optional SQLite failures must not replace authoritative owner facts."""
import asyncio
import sqlite3
import time
from unittest.mock import patch

import httpx
import pytest

from pa.core.async_runtime import BlockingOperationTimeout
from pa.core.operation_status import OperationStatusService
from pa.domain.models import AgentSession, CardCreate, RestartHandoff
from pa.execution.dispatch import DispatchRecord, DispatchStore
from tests.test_operation_status_passive import application
from tests.test_sync_recovery_owned import Harness


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["missing", "locked", "corrupt"])
async def test_optional_storage_failure_preserves_known_facts_and_validation(tmp_path, failure):
    h = Harness(tmp_path)
    h.settings.subscribed_realms.append("healthy")
    h.services["membership"].ensure_owner_membership("healthy", "local")
    service = OperationStatusService(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    h.services.update(operation_status=service, dispatch_store=ledger)
    h.store.begin_operation(idempotency_key="canonical", operation="card.create",
        request_fingerprint="original", realm_id="healthy", correlation_id="actual")
    card = h.store.create_card(CardCreate(title="actual", realm_id="healthy"),
        principal_id="user:local", instance_id="local", idempotency_key="canonical", request_fingerprint="original")
    h.store.complete_operation("canonical", card.model_dump(mode="json"))
    h.store.save_session(AgentSession(id="session", realm_id="healthy", agent_name="codex"))
    h.store.create_restart_handoff(RestartHandoff(session_id="session", idempotency_key="restart",
        continuation_prompt_id="continuation", status="failed"))
    ledger.put(DispatchRecord(dispatch_id="dispatch-id", mutation_id="mutation", idempotency_key="dispatch",
        realm_id="healthy", request_fingerprint="dispatch-fingerprint", authority_instance_id="local",
        authority_url="http://local", target_instance_id="local"))
    # A real completed negative exists, but unavailable proof cannot be used.
    job = service.admission("canonical", "healthy", "absent")
    service._record(job["id"], "completed", result={"status": "not_found", "lookup_head": h.log.get_head("healthy")})
    original_path = service.path
    blocker = None
    if failure == "missing":
        service.path = tmp_path / "missing-parent" / "optional.db"
    elif failure == "corrupt":
        service.path = tmp_path / "corrupt.db"
        service.path.write_bytes(b"invalid sqlite database")
    else:
        blocker = sqlite3.connect(service.path)
        blocker.execute("PRAGMA journal_mode=DELETE")
        blocker.execute("BEGIN EXCLUSIVE")
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=h.store), patch.object(service, "read_job", wraps=service.read_job) as reads:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                for key, owner, status in [("canonical", "canonical", "succeeded"), ("restart", "restart", "failed"), ("dispatch", "dispatch", "accepted")]:
                    reads.reset_mock()
                    response = await client.get(f"/api/operations/{key}", params={"realm": "healthy", "owner": owner})
                    assert response.status_code == 200, response.text
                    result = response.json()
                    assert result["status"] == status and result["accepted"] is True
                    assert result["result"]
                    assert result["reconciliation"] == {"state": "unavailable", "accepted": None, "code": "reconciliation_observation_unavailable"}
                    assert reads.call_count == 1
                for key in ["absent", "dispatch"]:
                    reads.reset_mock()
                    response = await client.get(f"/api/operations/{key}", params={"realm": "healthy"})
                    assert response.status_code == 200, response.text
                    result = response.json()
                    assert result["status"] == "lookup_pending" and result["committed"] is None
                    assert result["reconciliation"]["state"] == "unavailable"
                    assert result["recovery_action"] == "recover_operation_outcome"
                    assert reads.call_count == 1
                reads.reset_mock()
                wrong = await client.get("/api/operations/canonical", params={"realm": "healthy", "request_fingerprint": "wrong"})
                assert wrong.status_code == 409
                forbidden = await client.get("/api/operations/canonical", params={"realm": "inaccessible"})
                assert forbidden.status_code == 403
                ledger.put(DispatchRecord(dispatch_id="collision", mutation_id="collision", idempotency_key="canonical",
                    realm_id="healthy", authority_instance_id="local", authority_url="http://local", target_instance_id="local"))
                collision = await client.get("/api/operations/canonical", params={"realm": "healthy"})
                assert collision.status_code == 409
                assert reads.call_count == 0
                with patch.object(h.store, "read_operation_claims", side_effect=BlockingOperationTimeout("owner storage unavailable")):
                    failed_owner = await client.get("/api/operations/restart", params={"realm": "healthy", "owner": "restart"})
                    assert failed_owner.status_code == 504
                assert not service.tasks
    finally:
        if blocker:
            blocker.rollback()
            blocker.close()
        service.path = original_path
        await service.close()
        ledger.close()
        await h.close()


@pytest.mark.asyncio
async def test_real_held_dispatch_index_returns_bounded_typed_route_timeout(tmp_path):
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    h.services.update(operation_status=service, dispatch_store=ledger)
    ledger.put(DispatchRecord(dispatch_id="dispatch-id", mutation_id="mutation", idempotency_key="key",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local"))
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                ready = await client.get("/api/operations/key", params={"owner": "dispatch"})
                assert ready.status_code == 200 and ready.json()["status"] == "accepted"
                ledger._index_lock.acquire()
                try:
                    started = time.monotonic()
                    response = await asyncio.wait_for(client.get("/api/operations/key", params={"owner": "dispatch"}), 2)
                    assert response.status_code == 504, response.text
                    assert response.json()["code"] == "blocking_operation_timeout"
                    assert time.monotonic() - started < 1
                    assert "accepted" not in response.json() and not service.tasks
                finally:
                    ledger._index_lock.release()
                response = await client.get("/api/operations/key", params={"owner": "dispatch"})
                assert response.status_code == 200 and response.json()["status"] == "accepted"
    finally:
        await service.close()
        ledger.close()
        await h.close()
