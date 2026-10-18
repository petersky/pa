"""Closed dispatch sessions resume only through admitted, exact-identity retries."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.execution.dispatch import DispatchRecord, DispatchSessionRetry, DispatchStore
from pa.modules.agent_chat import CreateSessionBody, create_session
from pa.modules.fleet import DispatchMaterializeBody, materialize_dispatch
from tests.test_dispatch_notification_binding import admitted, _reset_singletons  # noqa: F401
from tests.test_dispatch_queue import capacity


def test_retry_restores_original_provider_session_workspace_and_prompt(admitted):
    r = admitted
    ledger = r.manager.dispatch_store
    original = r.store.get_session(r.successor.id)
    original.status = "closed"
    r.store.save_session(original)
    r.runtime._closed = True
    prior_selection = original.config_json["execution_selection"]
    failed = ledger.get(r.record.dispatch_id)
    original_prompt = dict(failed.initial_prompt_operation)
    failed.state, failed.error_code, failed.recoverable = "failed", "linked_session_unavailable", False
    ledger.put(failed)

    assert failed.public_dict()["can_retry"]
    admitted_capacity = capacity()
    retry = ledger.retry_with_capacity(failed, admitted_capacity, idempotency_key="recover-original")
    assert retry.session_retry.session_id == original.id
    asyncio.run(r.dispatch(retry))

    restored = r.store.get_session(original.id)
    current = ledger.get(failed.dispatch_id)
    assert current.state == "running"
    assert current.session_id == original.id
    assert current.mutation_id == failed.mutation_id
    assert current.initial_prompt_operation["prompt_id"] == original_prompt["prompt_id"]
    assert restored.external_session_id == original.external_session_id
    assert restored.config_json["execution_selection"] == prior_selection
    assert restored.cwd == original.cwd
    assert restored.execution_binding["dispatch_id"] == failed.dispatch_id
    assert r.start_calls[-1]["require_restore"] is True
    assert r.start_calls[-1]["resume_external_id"] == original.external_session_id
    leases = r.manager.workspace_manager.list(card_id=r.card.id)
    assert len(leases) == 1 and leases[0].session_id == original.id
    assert ledger.retry_with_capacity(failed, admitted_capacity, idempotency_key="recover-original").session_id == original.id
    assert len(r.start_calls) == 2


@pytest.mark.parametrize("case", ["unadmitted", "different-session", "completed", "missing-provider", "wrong-dispatch"])
def test_closed_session_restore_requires_admission_and_exact_binding(admitted, case):
    r = admitted
    ledger = r.manager.dispatch_store
    stored = r.store.get_session(r.successor.id)
    stored.status, stored.external_session_id = "closed", "provider-thread"
    r.runtime._closed = True
    record = ledger.get(r.record.dispatch_id)
    record.state = "provisioning"
    record.session_retry = DispatchSessionRetry(key="admitted", session_id=stored.id, generation=1)
    if case == "unadmitted":
        record.session_retry = None
    elif case == "different-session":
        record.session_retry.session_id = "another-session"
    elif case == "completed":
        record.completion_payload = {"stop_reason": "end_turn"}
    elif case == "missing-provider":
        stored.external_session_id = None
    else:
        stored.dispatch_id = "another-dispatch"
    r.store.save_session(stored)
    ledger.put(record)
    with pytest.raises(HTTPException) as denied:
        asyncio.run(create_session(r.request, CreateSessionBody(
            dispatch_id=record.dispatch_id, card_id=record.card_id,
            project_id=record.project_id, provider="codex",
        )))
    assert denied.value.status_code == 409
    assert len(r.start_calls) == 1
    assert r.store.get_session(stored.id).status == "closed"


@pytest.mark.parametrize("block", [
    {"error_code": "permission_denied"}, {"session_id": None},
    {"completion_payload": {"stop_reason": "end_turn"}},
    {"terminal_repair_reservation": {"state": "committed"}},
    {"completion_delivery_class": "acknowledged"},
])
def test_retry_does_not_widen_other_permanent_failures_or_completion(tmp_path, block):
    ledger = DispatchStore(tmp_path)
    try:
        record = DispatchRecord(
            mutation_id="same-mutation", authority_instance_id="authority",
            authority_url="http://authority", target_instance_id="target",
            session_id="original", state="failed", recoverable=False,
            error_code="linked_session_unavailable",
        ).model_copy(update=block)
        ledger.put(record)
        assert not record.public_dict()["can_retry"]
        with pytest.raises(ValueError, match="not retryable"):
            ledger.retry_with_capacity(record, capacity(), idempotency_key="retry")
        assert ledger.get(record.dispatch_id).state == "failed"
    finally:
        ledger.close()


def test_peer_materialization_carries_monotonic_exact_session_recovery(tmp_path):
    authority, target, session = (str(uuid4()) for _ in range(3))
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "work", instance_id=target)
    ledger = DispatchStore(settings.data_dir)
    try:
        original = DispatchRecord(
            dispatch_id=str(uuid4()), mutation_id=str(uuid4()),
            authority_instance_id=authority, authority_url="http://authority",
            target_instance_id=target, session_id=session, state="failed",
        )
        ledger.put(original)
        ctx = SimpleNamespace(settings=settings, store=MagicMock(), services={"dispatch_store": ledger})
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),
            state=SimpleNamespace(instance_authenticated=True),
            headers={"X-PA-Origin-Instance-ID": authority})
        marker = DispatchSessionRetry(key="retry-a", session_id=session, generation=1)
        body = DispatchMaterializeBody(
            dispatch_id=original.dispatch_id, mutation_id=original.mutation_id,
            realm_id="default", authority_instance_id=authority, authority_url="http://authority",
            target_instance_id=target, session_retry=marker,
        )
        assert materialize_dispatch(request, body)["duplicate"]
        assert ledger.get(original.dispatch_id).session_retry == marker
        assert ledger.get(original.dispatch_id).state == "materializing"
        for bad in [
            marker.model_copy(update={"session_id": str(uuid4())}),
            marker.model_copy(update={"key": "forged-same-generation"}),
        ]:
            with pytest.raises(HTTPException) as denied:
                materialize_dispatch(request, body.model_copy(update={"session_retry": bad}))
            assert denied.value.detail["code"] == "dispatch_session_retry_fence_mismatch"
        second = marker.model_copy(update={"key": "retry-b", "generation": 2})
        materialize_dispatch(request, body.model_copy(update={"session_retry": second}))
        with pytest.raises(HTTPException):
            materialize_dispatch(request, body)
        assert ledger.get(original.dispatch_id).session_retry == second
    finally:
        ledger.close()
