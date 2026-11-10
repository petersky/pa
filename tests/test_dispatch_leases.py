"""Dispatch leases (stage 2): lease + fence on the authority's record.

The authority grants a lease when a remote target acknowledges the prompt, renews
it from progress/heartbeat/probe ingest, fences out a superseded holder, and its
sweeper treats an expired lease as "target lost". Local dispatches are implicitly
fresh. Older targets (no negotiated lease) keep the stage-1 behaviour.
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.execution.dispatch import (
    CapacityAdmission, CompletionOutbox, DispatchRecord, DispatchStore, StaleLeaseFence,
)
from pa.execution.liveness import RunningLivenessSweeper, TargetStatus
from pa.execution.progress import (
    LEASE_FENCE_HEADER, LEASE_PROTOCOL, DispatchProgressEventV1, DispatchProgressHeartbeatV1,
    ProgressPhase, ProgressService,
)
from pa.modules.fleet import (
    DispatchCompletionBody, DispatchMaterializeBody, complete_dispatch,
    ingest_dispatch_progress, materialize_dispatch,
)
from tests.test_dispatch_liveness import running as plain_running
from tests.test_dispatch_two_ledger_completion import (  # noqa: F401 - pytest fixture
    AUTHORITY, DISPATCH, SESSION, TARGET, pair,
)

NOW = datetime(2026, 11, 9, 12, 0, tzinfo=UTC)
CARD = "card-1"


def remote(state="delivering_prompt", **extra) -> DispatchRecord:
    values = dict(
        dispatch_id="d1", mutation_id="m-d1", card_id=CARD, session_id="s1", state=state,
        authority_instance_id="auth", authority_url="http://auth.invalid",
        target_instance_id="tgt", progress_protocol_version=1,
        lease_protocol=LEASE_PROTOCOL,
    )
    values.update(extra)
    return DispatchRecord(**values)


def checkpoint(sequence: int, *, originating="tgt", dispatch_id="d1") -> DispatchProgressEventV1:
    return DispatchProgressEventV1(
        card_id=CARD, dispatch_id=dispatch_id, acp_session_id="s1",
        originating_instance_id=originating, authority_instance_id="auth",
        sequence=sequence, idempotency_key=f"cp-{dispatch_id}-{sequence}",
        phase=ProgressPhase.IMPLEMENTING, summary=f"Working {sequence}",
    )


def heartbeat(sequence: int, *, originating="tgt", dispatch_id="d1") -> DispatchProgressHeartbeatV1:
    return DispatchProgressHeartbeatV1(
        card_id=CARD, dispatch_id=dispatch_id, acp_session_id="s1",
        originating_instance_id=originating, authority_instance_id="auth",
        sequence=sequence, idempotency_key=f"hb-{dispatch_id}-{sequence}",
        phase=ProgressPhase.IMPLEMENTING, summary="Agent active.",
    )


@pytest.fixture
def ledger(tmp_path):
    store = DispatchStore(tmp_path / "auth", lease_ttl_seconds=300)
    yield store
    store.close()


def start_running(ledger, **extra) -> DispatchRecord:
    """Drive a remote dispatch to running exactly as the authority worker does."""
    record = ledger.put(remote(**extra))
    return ledger.commit_worker_stage(record, "running", "Prompt accepted.")


def capacity() -> CapacityAdmission:
    return CapacityAdmission(limit=2, source="configured", provider="codex",
                             observed_active=0, observed_queued=0, observed_reservations=0,
                             observed_at=NOW)


# --- grant ------------------------------------------------------------------------------

def test_lease_is_granted_when_the_target_acknowledges_the_prompt(ledger):
    before = datetime.now(UTC)
    record = start_running(ledger)
    assert record.state == "running"
    assert record.lease_fence == 1
    assert record.lease_id == "d1:1"
    assert record.lease_holder_instance_id == "tgt"
    assert record.lease_granted_at >= before
    assert record.lease_expires_at == record.lease_renewed_at + timedelta(seconds=300)
    assert record.lease_ttl_seconds == 300
    assert record.lease_is_fresh() is True
    assert record.lease_is_fresh(record.lease_expires_at + timedelta(seconds=1)) is False
    assert ledger.get("d1").lease_expires_at == record.lease_expires_at  # persisted


def test_no_lease_before_running_and_none_for_a_peer_that_never_negotiated_it(ledger):
    queued = ledger.put(remote())
    assert queued.lease_expires_at is None
    old = start_running(ledger, dispatch_id="old", mutation_id="m-old", lease_protocol=None)
    assert old.state == "running" and old.lease_expires_at is None
    assert old.lease_is_fresh() is None   # unmanaged: stage-1 liveness applies


def test_ttl_zero_grants_no_leases(tmp_path):
    store = DispatchStore(tmp_path, lease_ttl_seconds=0)
    try:
        record = store.commit_worker_stage(store.put(remote()), "running", "Prompt accepted.")
        assert record.lease_expires_at is None
    finally:
        store.close()


def test_ttl_is_read_live(tmp_path):
    ttl = [60]
    store = DispatchStore(tmp_path, lease_ttl_seconds=lambda: ttl[0])
    try:
        first = store.commit_worker_stage(store.put(remote()), "running", "go")
        assert first.lease_ttl_seconds == 60
        ttl[0] = 900
        store.ingest_progress(checkpoint(1), delivered=True, lease_fence=1)
        assert store.get("d1").lease_ttl_seconds == 900
    finally:
        store.close()


# --- renewal ----------------------------------------------------------------------------

def test_progress_and_heartbeat_ingest_renew_the_lease(ledger):
    record = start_running(ledger)
    # Age the lease so a renewal is observable.
    old = NOW - timedelta(minutes=30)
    ledger.mutate_current("d1", mutate=lambda r: (
        setattr(r, "lease_expires_at", old + timedelta(seconds=300)),
        setattr(r, "lease_renewed_at", old),
    ))
    ledger.ingest_progress(checkpoint(1), delivered=True, lease_fence=1)
    after_progress = ledger.get("d1")
    assert after_progress.lease_renewed_at > old
    assert after_progress.lease_expires_at > datetime.now(UTC) + timedelta(seconds=290)
    assert after_progress.lease_granted_at == record.lease_granted_at   # a renewal, not a re-grant

    ledger.mutate_current("d1", mutate=lambda r: setattr(r, "lease_expires_at", old + timedelta(seconds=300)))
    ledger.ingest_heartbeat(heartbeat(2), delivered=True, lease_fence=1)
    assert ledger.get("d1").lease_expires_at > datetime.now(UTC) + timedelta(seconds=290)


def test_a_confirmed_probe_renews_the_lease(ledger):
    start_running(ledger)
    old = NOW - timedelta(minutes=30)
    ledger.mutate_current("d1", mutate=lambda r: setattr(r, "lease_expires_at", old))
    ledger.note_target_confirmed("d1", "Target confirmed the dispatch is still running.")
    assert ledger.get("d1").lease_expires_at > datetime.now(UTC) + timedelta(seconds=290)


def test_renewal_never_shortens_and_never_revives_a_settled_dispatch(ledger):
    start_running(ledger)
    far = datetime.now(UTC) + timedelta(hours=3)
    ledger.mutate_current("d1", mutate=lambda r: setattr(r, "lease_expires_at", far))
    ledger.ingest_progress(checkpoint(1), delivered=True, lease_fence=1)
    assert ledger.get("d1").lease_expires_at == far
    ledger.fail(ledger.get("d1"), "lost", code="remote_dispatch_unknown")
    stale_expiry = ledger.get("d1").lease_expires_at
    ledger.ingest_heartbeat(heartbeat(5), delivered=True, lease_fence=1)
    assert ledger.get("d1").lease_expires_at == stale_expiry


def test_lease_starts_on_first_signal_when_ttl_is_enabled_mid_run(tmp_path):
    ttl = [0]
    store = DispatchStore(tmp_path, lease_ttl_seconds=lambda: ttl[0])
    try:
        store.commit_worker_stage(store.put(remote()), "running", "go")
        assert store.get("d1").lease_expires_at is None
        ttl[0] = 300
        store.ingest_heartbeat(heartbeat(1), delivered=True)
        assert store.get("d1").lease_expires_at is not None
    finally:
        store.close()


# --- local dispatches -------------------------------------------------------------------

def test_local_dispatch_lease_is_implicit_and_never_written(tmp_path):
    store = DispatchStore(tmp_path, lease_ttl_seconds=300)
    try:
        local = remote(authority_instance_id="me", target_instance_id="me")
        record = store.commit_worker_stage(store.put(local), "running", "go")
        assert record.state == "running"
        assert record.lease_expires_at is None and record.lease_holder_instance_id is None
        assert record.lease_is_fresh() is True
        assert record.lease_is_fresh(NOW + timedelta(days=400)) is True   # always fresh
        store.ingest_progress(
            checkpoint(1, originating="me").model_copy(update={"authority_instance_id": "me"}),
            delivered=True,
        )
        store.note_target_confirmed("d1", "ok")
        after = store.get("d1")
        assert (after.lease_expires_at, after.lease_renewed_at, after.lease_id) == (None, None, None)
        view = after.public_lease()
        assert view["mode"] == "implicit" and view["fresh"] is True and view["id"] is None
        assert store.running_remote_records("me") == []   # never swept
    finally:
        store.close()


# --- fence ------------------------------------------------------------------------------

def test_fence_is_bumped_by_every_reassignment_and_clears_the_lease(ledger):
    start_running(ledger)
    failed = ledger.fail(ledger.get("d1"), "lost", code="remote_dispatch_unknown")
    assert failed.lease_fence == 1 and failed.lease_expires_at is not None   # failing alone keeps the holder's claim
    retried = ledger.retry_with_capacity(
        failed, capacity(), idempotency_key="reoffer-lost:d1:tgt",
        retarget={"target_instance_id": "tgt2"},
    )
    assert retried.lease_fence == 2
    assert retried.target_instance_id == "tgt2"
    assert (retried.lease_protocol, retried.lease_id, retried.lease_expires_at,
            retried.lease_holder_instance_id) == (None, None, None, None)
    # A same-target operator retry is a new assignment too.
    again = ledger.commit_worker_stage(retried, "running", "go")
    assert again.lease_expires_at is None    # target must renegotiate before a new lease
    failed_again = ledger.fail(ledger.get("d1"), "boom", code="x")
    retried_again = ledger.retry_with_capacity(failed_again, capacity(), idempotency_key="retry-2")
    assert retried_again.lease_fence == 3
    # A replayed retry (same idempotency key) does not bump again.
    replay = ledger.retry_with_capacity(failed_again, capacity(), idempotency_key="retry-2")
    assert replay.lease_fence == 3


def test_progress_from_a_stale_fence_is_rejected_and_changes_nothing(ledger):
    start_running(ledger)
    ledger.mutate_current("d1", mutate=lambda r: setattr(r, "lease_fence", 2))
    before = ledger.get("d1")
    with pytest.raises(StaleLeaseFence) as raised:
        ledger.ingest_progress(checkpoint(1), delivered=True, lease_fence=1)
    assert (raised.value.presented, raised.value.current) == (1, 2)
    with pytest.raises(StaleLeaseFence):
        ledger.ingest_heartbeat(heartbeat(2), delivered=True, lease_fence=1)
    after = ledger.get("d1")
    assert after.progress_events == [] and after.progress_heartbeat is None
    assert after.lease_expires_at == before.lease_expires_at
    # The current fence, and an old target that sends none, are accepted.
    assert ledger.ingest_progress(checkpoint(3), delivered=True, lease_fence=2).accepted
    assert ledger.ingest_progress(checkpoint(4), delivered=True).accepted


def request_for_authority(ledger, *, headers):
    request = MagicMock()
    request.state.instance_authenticated = True
    request.headers = headers
    request.app.state.ctx.settings = SimpleNamespace(instance_id="auth")
    request.app.state.ctx.services = {"dispatch_store": ledger, "writer_lock": MagicMock()}
    return request


def post_progress(ledger, payload, *, fence, caller="tgt"):
    headers = {"X-PA-Origin-Instance-ID": caller, "idempotency-key": payload.idempotency_key}
    if fence is not None:
        headers[LEASE_FENCE_HEADER] = str(fence)
    return ingest_dispatch_progress(request_for_authority(ledger, headers=headers), "d1", payload)


def test_progress_endpoint_rejects_a_stale_holder_with_a_distinguishable_409(ledger):
    start_running(ledger)
    ledger.mutate_current("d1", mutate=lambda r: (
        setattr(r, "lease_fence", 3), setattr(r, "target_instance_id", "tgt2"),
    ))
    # The superseded holder is no longer the target: it must see the lease code, not a 403.
    with pytest.raises(HTTPException) as raised:
        post_progress(ledger, checkpoint(1), fence=1)
    assert raised.value.status_code == 409
    assert raised.value.detail["code"] == "stale_lease_fence"
    assert raised.value.detail["current_fence"] == 3 and raised.value.detail["recoverable"] is False
    assert ledger.get("d1").progress_events == []
    # The current holder is accepted and renews.
    ok = post_progress(ledger, checkpoint(2, originating="tgt2"), fence=3, caller="tgt2")
    assert ok.status_code == 200
    assert len(ledger.get("d1").progress_events) == 1


def test_progress_endpoint_still_accepts_an_old_target_without_a_fence_and_renews(ledger):
    start_running(ledger)
    ledger.mutate_current("d1", mutate=lambda r: setattr(r, "lease_expires_at", NOW))
    assert post_progress(ledger, checkpoint(1), fence=None).status_code == 200
    assert ledger.get("d1").lease_expires_at > datetime.now(UTC)


def test_progress_endpoint_from_a_stranger_without_a_fence_is_still_a_403(ledger):
    start_running(ledger)
    with pytest.raises(HTTPException) as raised:
        post_progress(ledger, checkpoint(1, originating="stranger"), fence=None, caller="stranger")
    assert raised.value.status_code == 403
    assert raised.value.detail["code"] == "progress_provenance_mismatch"


# --- /complete fencing ------------------------------------------------------------------

def _completion_request(pair, mutation_id, fence):
    fake = MagicMock()
    fake.headers = {"idempotency-key": mutation_id}
    if fence is not None:
        fake.headers[LEASE_FENCE_HEADER] = str(fence)
    fake.app = pair.authority_request.app
    return fake


def _completion_body(**extra) -> DispatchCompletionBody:
    return DispatchCompletionBody(
        mutation_id="mutation-two-ledger", card_id="card-1", realm_id="default",
        source_instance_id=TARGET, session_id=SESSION, result={"stop_reason": "end_turn"},
        **extra,
    )


def _arm_authority(pair, *, fence=1, target=TARGET):
    pair.authority.mutate_current(DISPATCH, mutate=lambda r: (
        setattr(r, "state", "running"), setattr(r, "lease_protocol", LEASE_PROTOCOL),
        setattr(r, "lease_fence", fence), setattr(r, "target_instance_id", target),
    ))


def test_completion_from_a_stale_fence_is_rejected_and_the_record_is_untouched(pair):
    _arm_authority(pair, fence=2, target="other-target")
    with pytest.raises(HTTPException) as raised:
        complete_dispatch(_completion_request(pair, "mutation-two-ledger", 1), DISPATCH, _completion_body())
    assert raised.value.status_code == 409
    assert raised.value.detail["code"] == "stale_lease_fence"
    after = pair.authority.get(DISPATCH)
    assert after.state == "running" and after.acknowledged_at is None
    assert after.completion_payload is None and after.completion_envelope is None


def test_completion_from_the_current_holder_still_wins(pair):
    _arm_authority(pair, fence=2)
    complete_dispatch(_completion_request(pair, "mutation-two-ledger", 2), DISPATCH, _completion_body())
    done = pair.authority.get(DISPATCH)
    assert done.state == "completed" and done.acknowledged_at is not None
    # A replay of the acknowledged completion is still idempotent, whatever fence it carries.
    replay = complete_dispatch(_completion_request(pair, "mutation-two-ledger", 1), DISPATCH, _completion_body())
    assert replay["duplicate"] is True


def test_completion_from_an_older_target_without_a_fence_is_accepted(pair):
    _arm_authority(pair, fence=2)
    complete_dispatch(_completion_request(pair, "mutation-two-ledger", None), DISPATCH, _completion_body())
    assert pair.authority.get(DISPATCH).state == "completed"


# --- target side: superseded -----------------------------------------------------------

def _stale_response(current=2):
    return httpx.Response(409, json={"detail": {
        "code": "stale_lease_fence", "current_fence": current, "recoverable": False,
    }})


@pytest.mark.asyncio
async def test_progress_service_stops_reporting_when_superseded(tmp_path):
    target = DispatchStore(tmp_path / "target")
    try:
        mirror = remote(state="running", lease_fence=1)
        target.put(mirror)
        target.ingest_progress(checkpoint(1))
        seen: list[httpx.Request] = []
        notes: list[tuple[str, dict]] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request)
            return _stale_response()

        async def notifier(dispatch_id, detail):
            notes.append((dispatch_id, detail))

        service = ProgressService(target, instance_id="tgt", token="t", superseded_notifier=notifier)
        service._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        pending = target.pending_progress_deliveries("tgt")
        assert len(pending) == 1 and pending[0][0].lease_fence == 1
        await service._send(*pending[0])
        assert seen[0].headers[LEASE_FENCE_HEADER] == "1"
        after = target.get("d1")
        assert after.lease_superseded_at is not None and after.lease_superseded_by_fence == 2
        assert after.progress_events[0].delivery_attempts == 0   # not a transport failure
        assert notes == [("d1", {"code": "stale_lease_fence", "current_fence": 2, "recoverable": False})]
        # No retry: nothing is pending for a superseded mirror any more.
        assert target.pending_progress_deliveries("tgt") == []
        assert target.pending_progress("tgt") == []
        await service.close()
    finally:
        target.close()


@pytest.mark.asyncio
async def test_old_authority_error_is_still_a_retryable_failure(tmp_path):
    """Only the explicit lease code supersedes; any other 409 keeps the old retry path."""
    target = DispatchStore(tmp_path / "target")
    try:
        target.put(remote(state="running"))
        target.ingest_progress(checkpoint(1))
        service = ProgressService(target, instance_id="tgt", token="t")
        service._client = httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(409, json={"detail": {"code": "other"}}))
        )
        await service._send(*target.pending_progress_deliveries("tgt")[0])
        after = target.get("d1")
        assert after.lease_superseded_at is None
        assert after.progress_events[0].delivery_attempts == 1
        await service.close()
    finally:
        target.close()


@pytest.mark.asyncio
async def test_outbox_marks_the_completion_superseded_instead_of_retrying_forever(tmp_path):
    target = DispatchStore(tmp_path / "target")
    try:
        mirror = remote(state="completion_pending", completion_payload={"stop_reason": "end_turn"})
        target.put(mirror)
        seen: list[httpx.Request] = []
        notes: list[tuple[str, dict]] = []

        def handler(request):
            seen.append(request)
            return _stale_response(current=4)

        outbox = CompletionOutbox(
            target, "t", retry_seconds=0.01,
            superseded_notifier=lambda dispatch_id, detail: notes.append((dispatch_id, detail)),
        )
        outbox._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        await outbox._send(target.get("d1"))
        assert seen[0].headers[LEASE_FENCE_HEADER] == "1"
        after = target.get("d1")
        assert after.completion_delivery_class == "superseded"
        assert after.completion_next_retry_at is None and after.recoverable is False
        assert after.lease_superseded_at is not None and after.lease_superseded_by_fence == 4
        assert after.attempts == 1
        assert notes and notes[0][0] == "d1"
        assert "reassigned" in after.events[-1].message
        assert len(seen) == 1
        await outbox._client.aclose()
    finally:
        target.close()


@pytest.mark.asyncio
async def test_two_ledgers_stale_completion_after_reoffer_does_not_touch_the_new_assignment(pair):
    """Authority re-offers to another target; the old holder's completion is fenced out."""
    _arm_authority(pair, fence=1)
    pair.authority.fail(pair.authority.get(DISPATCH), "lost", code="remote_dispatch_unknown")
    retried = pair.authority.retry_with_capacity(
        pair.authority.get(DISPATCH), capacity(), idempotency_key="reoffer-lost:x",
        retarget={"target_instance_id": "new-target"},
    )
    assert retried.lease_fence == 2
    # The old target, still on fence 1, delivers through the real outbox.
    pair.target.mutate_current(DISPATCH, mutate=lambda r: (
        setattr(r, "state", "completion_pending"), setattr(r, "completion_payload", {"stop_reason": "end_turn"}),
        setattr(r, "lease_protocol", LEASE_PROTOCOL), setattr(r, "lease_fence", 1),
    ))

    def handler(request: httpx.Request) -> httpx.Response:
        fake = MagicMock()
        fake.headers = {k.lower(): v for k, v in request.headers.items()}
        fake.headers[LEASE_FENCE_HEADER] = request.headers[LEASE_FENCE_HEADER]
        fake.app = pair.authority_request.app
        try:
            body = DispatchCompletionBody.model_validate(json.loads(request.content))
            return httpx.Response(200, json=complete_dispatch(fake, DISPATCH, body))
        except HTTPException as exc:
            return httpx.Response(exc.status_code, json={"detail": exc.detail})

    outbox = CompletionOutbox(pair.target, "t", retry_seconds=0.01)
    outbox._client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    await outbox._send(pair.target.get(DISPATCH))
    assert pair.target.get(DISPATCH).completion_delivery_class == "superseded"
    untouched = pair.authority.get(DISPATCH)
    assert untouched.state in {"queued", "waiting_capacity"} and untouched.acknowledged_at is None
    assert untouched.completion_payload is None and untouched.target_instance_id == "new-target"
    await outbox._client.aclose()


# --- materialize handshake --------------------------------------------------------------

def materialize_request(tmp_path, instance_id):
    settings = Settings(data_dir=tmp_path, instance_id=instance_id)
    ctx = MagicMock(settings=settings)
    ctx.services = {"writer_lock": MagicMock()}
    ctx.require_service.side_effect = lambda name: ctx.services[name]
    ctx.register_service.side_effect = lambda name, value: ctx.services.__setitem__(name, value)
    request = MagicMock()
    request.app.state.ctx = ctx
    request.state.instance_authenticated = True
    request.headers = {"X-PA-Origin-Instance-ID": M_AUTH}
    return request


M_AUTH = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
M_TGT = "02dbcd47-8f40-44eb-8403-5eb57545afc8"
MAT_ID = "33333333-3333-4333-8333-333333333333"
MAT_MUTATION = "44444444-4444-4444-8444-444444444444"


def materialize_body(**extra) -> DispatchMaterializeBody:
    return DispatchMaterializeBody(
        dispatch_id=MAT_ID, mutation_id=MAT_MUTATION, realm_id="default", authority_instance_id=M_AUTH,
        authority_url="http://auth.invalid", target_instance_id=M_TGT, progress_versions=[1], **extra,
    )


def test_target_negotiates_the_lease_and_records_its_fence(tmp_path):
    request = materialize_request(tmp_path, M_TGT)
    result = materialize_dispatch(request, materialize_body(
        lease_protocols=[LEASE_PROTOCOL], lease_fence=1, lease_id=f"{MAT_ID}:1",
    ))
    assert result["lease_protocol"] == LEASE_PROTOCOL and result["lease_fence"] == 1
    mirror = DispatchStore(tmp_path).get(MAT_ID)
    assert (mirror.lease_protocol, mirror.lease_fence, mirror.lease_id) == (LEASE_PROTOCOL, 1, f"{MAT_ID}:1")
    assert mirror.lease_holder_instance_id == M_TGT and mirror.lease_expires_at is None


def test_a_reassigned_materialize_raises_the_fence_and_clears_superseded_and_a_delayed_one_is_refused(tmp_path):
    request = materialize_request(tmp_path, M_TGT)
    materialize_dispatch(request, materialize_body(lease_protocols=[LEASE_PROTOCOL], lease_fence=1))
    store = request.app.state.ctx.services["dispatch_store"]
    store.mark_lease_superseded(MAT_ID, 2)
    assert store.get(MAT_ID).lease_superseded_at is not None
    result = materialize_dispatch(request, materialize_body(lease_protocols=[LEASE_PROTOCOL], lease_fence=2))
    assert result["lease_fence"] == 2
    mirror = store.get(MAT_ID)
    assert mirror.lease_fence == 2 and mirror.lease_superseded_at is None
    with pytest.raises(HTTPException) as raised:
        materialize_dispatch(request, materialize_body(lease_protocols=[LEASE_PROTOCOL], lease_fence=1))
    assert raised.value.detail["code"] == "stale_lease_fence" and raised.value.detail["current_fence"] == 2
    assert store.get(MAT_ID).lease_fence == 2


def test_authority_that_does_not_offer_a_lease_gets_none(tmp_path):
    request = materialize_request(tmp_path, M_TGT)
    result = materialize_dispatch(request, materialize_body())
    assert result["lease_protocol"] is None and result["lease_fence"] is None
    mirror = request.app.state.ctx.services["dispatch_store"].get(MAT_ID)
    assert mirror.lease_protocol is None


def test_local_dispatch_never_negotiates_a_lease(tmp_path):
    request = materialize_request(tmp_path, M_AUTH)
    request.headers = {"X-PA-Origin-Instance-ID": M_AUTH}
    store = DispatchStore(tmp_path)
    store.put(remote(dispatch_id=MAT_ID, mutation_id=MAT_MUTATION, card_id=None, authority_instance_id=M_AUTH, target_instance_id=M_AUTH,
                     lease_protocol=None,
                     state="materializing"))
    request.app.state.ctx.services["dispatch_store"] = store
    body = materialize_body(lease_protocols=[LEASE_PROTOCOL], lease_fence=1).model_copy(
        update={"target_instance_id": M_AUTH}
    )
    result = materialize_dispatch(request, body)
    assert result["lease_protocol"] is None
    assert store.get(MAT_ID).lease_protocol is None


# --- sweeper ----------------------------------------------------------------------------

def leased_running(ledger, dispatch_id="l1", *, expires_in: timedelta, activity_age=timedelta(hours=3),
                   target="peer") -> DispatchRecord:
    record = plain_running(dispatch_id, silent=activity_age, target=target)
    record.lease_protocol = LEASE_PROTOCOL
    record.lease_expires_at = NOW + expires_in
    record.lease_renewed_at = record.lease_expires_at - timedelta(seconds=300)
    record.lease_holder_instance_id = target
    return ledger.put(record)


def sweeper(ledger, status: TargetStatus, **overrides):
    hooks = {
        "probe": AsyncMock(return_value=status), "adopt": AsyncMock(),
        "reoffer": AsyncMock(return_value=True), "notify": MagicMock(),
    }

    async def fail_lost(record, reason, code):
        return ledger.fail(record, reason, code=code, recoverable=True)

    hooks["fail_lost"] = AsyncMock(side_effect=fail_lost)
    hooks.update(overrides)
    return RunningLivenessSweeper(
        ledger, instance_id="local", stale_seconds=1800, lost_seconds=21600,
        lease_grace_seconds=300, **hooks,
    ), hooks


@pytest.fixture
def local_ledger(tmp_path):
    store = DispatchStore(tmp_path, lease_ttl_seconds=300)
    yield store
    store.close()


@pytest.mark.asyncio
async def test_a_fresh_lease_is_trusted_even_when_the_last_progress_is_old(local_ledger):
    # Stage 1 would probe this (3 h silent); the fresh lease is the liveness signal.
    leased_running(local_ledger, expires_in=timedelta(seconds=120))
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=False))
    assert await sw.sweep_once(NOW) == []
    hooks["probe"].assert_not_awaited()


@pytest.mark.asyncio
async def test_an_expired_lease_is_probed_even_when_progress_is_recent(local_ledger):
    # Recent last_activity (5 min) but the lease lapsed: stage 1 would not probe yet.
    leased_running(local_ledger, expires_in=-timedelta(seconds=10), activity_age=timedelta(minutes=5))
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=True, state="running"))
    assert await sw.sweep_once(NOW) == [("l1", "confirmed_running")]
    hooks["probe"].assert_awaited_once()


@pytest.mark.asyncio
async def test_a_quiet_but_healthy_turn_keeps_its_lease_through_the_probe(local_ledger):
    leased_running(local_ledger, expires_in=-timedelta(minutes=45))
    sw, _hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=True, state="running"))
    now = datetime.now(UTC)
    assert await sw.sweep_once(now + timedelta(hours=1)) == [("l1", "confirmed_running")]
    record = local_ledger.get("l1")
    assert record.state == "running"
    assert record.lease_expires_at > now   # renewed (stamped with the real clock)
    assert await sw.sweep_once(now) == []   # fresh again: not re-probed


@pytest.mark.asyncio
@pytest.mark.parametrize("status,code", [
    (TargetStatus(reachable=True, known=False), "remote_dispatch_unknown"),
    (TargetStatus(reachable=True, known=True, state="failed"), "remote_dispatch_dead"),
])
async def test_expired_lease_and_target_that_lost_it_fails_and_reoffers_immediately(local_ledger, status, code):
    leased_running(local_ledger, expires_in=-timedelta(seconds=30))
    sw, hooks = sweeper(local_ledger, status)
    assert await sw.sweep_once(NOW) == [("l1", "failed_and_reoffered")]
    failed = local_ledger.get("l1")
    assert failed.state == "failed" and failed.recoverable and failed.error_code == code
    hooks["notify"].assert_called_once()
    hooks["reoffer"].assert_awaited_once()


@pytest.mark.asyncio
async def test_expired_lease_with_an_unreachable_target_waits_out_the_grace_then_fails(local_ledger):
    leased_running(local_ledger, "brief", expires_in=-timedelta(seconds=60))
    leased_running(local_ledger, "long", expires_in=-timedelta(seconds=900))
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=False, error="timed out"))
    assert await sw.sweep_once(NOW) == [("long", "failed_and_reoffered")]
    assert local_ledger.get("brief").state == "running"
    failed = local_ledger.get("long")
    assert failed.error_code == "remote_dispatch_lease_expired"
    assert "lease expired" in failed.last_error
    hooks["notify"].assert_called_once()


@pytest.mark.asyncio
async def test_expired_lease_and_unusable_answer_is_treated_like_unreachable(local_ledger):
    leased_running(local_ledger, expires_in=-timedelta(seconds=900))
    sw, _hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=None, error="HTTP 404"))
    assert await sw.sweep_once(NOW) == [("l1", "failed_and_reoffered")]


@pytest.mark.asyncio
async def test_a_completion_held_by_a_target_with_an_expired_lease_is_adopted(local_ledger):
    leased_running(local_ledger, expires_in=-timedelta(minutes=10))
    completion = {"mutation_id": "m-l1", "result": {}}
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=True, completion=completion))
    assert await sw.sweep_once(NOW) == [("l1", "adopted_completion")]
    hooks["adopt"].assert_awaited_once()


@pytest.mark.asyncio
async def test_unleased_records_keep_the_stage_one_behaviour(local_ledger):
    local_ledger.put(plain_running("old", silent=timedelta(hours=2)))
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=False, error="timed out"))
    assert await sw.sweep_once(NOW) == []          # probed, but unreachable < lost_seconds
    hooks["probe"].assert_awaited_once()
    local_ledger.put(plain_running("older", silent=timedelta(hours=7)))
    assert await sw.sweep_once(NOW) == [("older", "failed_and_reoffered")]
    assert local_ledger.get("older").error_code == "remote_dispatch_unreachable"


@pytest.mark.asyncio
async def test_old_record_with_a_lease_expiry_but_no_protocol_is_not_lease_swept(local_ledger):
    record = plain_running("odd", silent=timedelta(minutes=5))
    record.lease_expires_at = NOW - timedelta(hours=1)    # no negotiated protocol
    local_ledger.put(record)
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=False))
    assert await sw.sweep_once(NOW) == []
    hooks["probe"].assert_not_awaited()


@pytest.mark.asyncio
async def test_reoffer_bumps_the_fence_so_the_lost_holder_is_fenced_out(local_ledger):
    from tests.test_dispatch_liveness import reoffer_app
    from pa.modules.fleet import _reoffer_lost_dispatch
    leased_running(local_ledger, target="dead", expires_in=-timedelta(minutes=30))
    sw, hooks = sweeper(local_ledger, TargetStatus(reachable=True, known=False))
    app, _placement = reoffer_app(local_ledger, None)
    sw.reoffer = lambda failed: _reoffer_lost_dispatch(app, failed)
    # capacity_limit etc. so the re-offer admits
    local_ledger.mutate_current("l1", mutate=lambda r: (
        setattr(r, "capacity_limit", 1), setattr(r, "capacity_source", "configured")))
    with patch("pa.modules.fleet._placement_candidates", AsyncMock(return_value=[])), patch(
        "pa.modules.fleet._placement_policy_inputs", MagicMock(return_value=(None, False, [])),
    ), patch("pa.modules.fleet.notify_dispatch_reoffered"):
        assert await sw.sweep_once(NOW) == [("l1", "failed_and_reoffered")]
    again = local_ledger.get("l1")
    assert again.target_instance_id == "alive" and again.lease_fence == 2
    assert again.lease_expires_at is None and again.lease_protocol is None
    with pytest.raises(StaleLeaseFence):
        local_ledger.ingest_progress(checkpoint(1, dispatch_id="l1"), delivered=True, lease_fence=1)
