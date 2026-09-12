"""Combined authenticated regression coverage for the review contracts."""
import asyncio
from unittest.mock import patch

import httpx
import pytest

from pa.core.operation_status import OperationStatusService
from pa.domain.models import CardCreate
from pa.execution.dispatch import DispatchRecord, DispatchStore
from tests.test_operation_status_passive import application
from tests.test_sync_recovery_owned import Harness


@pytest.mark.asyncio
async def test_passive_negative_head_proof_never_masks_later_receipt(tmp_path):
    h = Harness(tmp_path)
    h.settings.subscribed_realms.append("healthy")
    h.services["membership"].ensure_owner_membership("healthy", "local")
    service = OperationStatusService(tmp_path)
    h.services["operation_status"] = service
    app, auth = application(h)
    params = {"realm": "healthy"}
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                for _ in range(4):
                    observed = await client.get("/api/operations/absent", params=params)
                    assert observed.json()["status"] == "lookup_pending"
                    assert "reconciliation" not in observed.json()
                assert not service.tasks and service.read_job("canonical", "healthy", "absent") is None
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local") as anonymous:
                    denied = await anonymous.post("/api/operation-recovery/absent", params=params)
                    assert denied.status_code in {401, 403}
                admitted = await client.post("/api/operation-recovery/absent", params=params)
                assert admitted.status_code == 202
                await asyncio.gather(*service.tasks.values())
                absent = (await client.get("/api/operations/absent", params=params)).json()
                assert absent["status"] == "not_found" and absent["committed"] is False
                assert absent["lookup_head"] == h.log.get_head("healthy")
                h.store.begin_operation(idempotency_key="absent", operation="card.create",
                    request_fingerprint="new", realm_id="healthy", correlation_id="real")
                card = h.store.create_card(CardCreate(title="later effect", realm_id="healthy"),
                    principal_id="user:local", instance_id="local", idempotency_key="absent", request_fingerprint="new")
                later = (await client.get("/api/operations/absent", params=params)).json()
                assert later["accepted"] is True and later["status"] != "not_found"
                assert h.store.get_card(card.id, realm_id="healthy") is not None
    finally:
        await service.close()
        await h.close()


@pytest.mark.asyncio
async def test_real_delegated_receipt_equivalence_and_foreign_realm_non_disclosure(tmp_path):
    h = Harness(tmp_path)
    service = OperationStatusService(tmp_path)
    ledger = DispatchStore(tmp_path / "dispatch")
    h.services.update(operation_status=service, dispatch_store=ledger)
    # Real owner producers persist a canonical wrapper and its delegated record.
    h.store.begin_operation(idempotency_key="delegate", operation="dispatch.create",
        request_fingerprint="same", realm_id="default", correlation_id="wrapper")
    ledger.put(DispatchRecord(dispatch_id="delegated-id", mutation_id="mutation",
        idempotency_key="delegate", request_fingerprint="same", realm_id="default",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local"))
    h.store.complete_operation("delegate", {"dispatch_id": "delegated-id"})
    h.store.begin_operation(idempotency_key="secret", operation="card.create",
        request_fingerprint="private", realm_id="secret-realm", correlation_id="hidden")
    ledger.put(DispatchRecord(dispatch_id="hidden-id", mutation_id="hidden-mutation",
        idempotency_key="secret", request_fingerprint="private", realm_id="secret-realm",
        authority_instance_id="local", authority_url="http://local", target_instance_id="local"))
    app, auth = application(h)
    try:
        with patch("pa.modules.items.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                for owner in [None, "canonical", "dispatch"]:
                    params = {"owner": owner} if owner else {}
                    result = await client.get("/api/operations/delegate", params=params)
                    assert result.status_code == 200, result.text
                    assert result.json()["result"]["dispatch_id"] == "delegated-id"
                    assert result.json()["effect"] == "unknown"
                conflict = await client.get("/api/operations/delegate", params={"request_fingerprint": "other"})
                assert conflict.status_code == 409
                for owner in [None, "canonical", "dispatch"]:
                    params = {"owner": owner} if owner else {}
                    hidden = (await client.get("/api/operations/secret", params=params)).json()
                    absent = (await client.get("/api/operations/absent", params=params)).json()
                    assert hidden["status"] == absent["status"] == "lookup_pending"
                    assert hidden["accepted"] is absent["accepted"] is None
                    assert "secret-realm" not in str(hidden)
                original_read = ledger.read_operation_receipts
                reads = []
                def changed_between_identity_and_result(*args, **kwargs):
                    reads.append(True)
                    if len(reads) == 2:
                        record = ledger.get("delegated-id")
                        record.request_fingerprint = "incompatible"
                        ledger.put(record)
                    return original_read(*args, **kwargs)
                with patch.object(ledger, "read_operation_receipts", side_effect=changed_between_identity_and_result):
                    assert (await client.get("/api/operations/delegate")).status_code == 409
                assert len(reads) == 2
                assert (await client.get("/api/operations/delegate")).status_code == 409
    finally:
        await service.close()
        ledger.close()
        await h.close()


@pytest.mark.asyncio
async def test_healthy_reconcile_ui_aliases_and_local_login_preserve_validation(tmp_path):
    from pa.modules.items import ui_router
    from pa.modules.auth import router as auth_router
    h = Harness(tmp_path)
    h.settings.subscribed_realms.append("healthy")
    h.services["membership"].ensure_owner_membership("healthy", "local")
    h.fetch_release.clear()
    h.diagnose()
    app, auth = application(h)
    app.include_router(ui_router)
    app.include_router(auth_router, prefix="/api")
    try:
        with patch("pa.modules.items.get_store", return_value=h.store), patch("pa.modules.sync.get_store", return_value=h.store):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://local", headers=auth) as client:
                card = h.store.create_card(CardCreate(title="healthy", realm_id="healthy"), principal_id="user:local", instance_id="local")
                with patch.object(h.recovery, "retry_result", side_effect=AssertionError("healthy path spent recovery budget")):
                    for i in range(3):
                        result = await client.post("/api/sync/reconcile", json={"realm_id": "healthy"}, headers={"Idempotency-Key": f"healthy-{i}"})
                        assert result.status_code == 200, result.text
                        assert result.json()["consistent"] and not result.json()["rebuilt"]
                await client.get("/api/operations/csrf")
                client.headers["X-CSRF-Token"] = client.cookies["pa_csrf"]
                moved = await client.post(f"/partials/cards/{card.id}/move", params={"realm": "healthy"}, data={"lane": "active"})
                assert moved.status_code == 204, moved.text
                assert h.store.get_card(card.id, realm_id="healthy").lane == "active"
                blocked = await client.post("/partials/cards/card/move", params={"realm": "default"}, data={"lane": "active"})
                assert blocked.status_code == 503
                for path in ["/api/auth/users", "/api/permissions/grant", "/api/workspaces"]:
                    assert (await client.post(path, json={})).status_code == 503
                # Wrong credentials reach the real verifier, not the history gate.
                login = await client.post("/api/auth/login", data={"username": "bad", "password": "bad"})
                assert login.status_code == 401, login.text
                logout = await client.post("/api/auth/logout")
                assert logout.status_code == 200, logout.text
    finally:
        await h.close()
