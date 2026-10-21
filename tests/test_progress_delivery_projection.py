from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
import asyncio
import json
from threading import Event
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.dispatch import DispatchStoreReadOnlyError
from pa.execution.progress import (
    DispatchProgressEventV1, DispatchProgressHeartbeatV1, ProgressPhase,
    ProgressService, ProgressToolDetailV1,
)

AUTHORITY = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
ORIGIN = "02dbcd47-8f40-44eb-8403-5eb57545afc8"
DISPATCH = "33333333-3333-4333-8333-333333333333"
SESSION = "44444444-4444-4444-8444-444444444444"
CARD = "55555555-5555-4555-8555-555555555555"


def seed(root):
    stamp = datetime.now(UTC)
    record = DispatchRecord(
        dispatch_id=DISPATCH, mutation_id="fixture", card_id=CARD,
        authority_instance_id=AUTHORITY, authority_url="https://authority.example",
        target_instance_id=ORIGIN, session_id=SESSION,
        materialization_plan={"steps": [{"evidence": ["retained" * 128]}]},
    )
    fields = dict(card_id=CARD, dispatch_id=DISPATCH, acp_session_id=SESSION,
                  originating_instance_id=ORIGIN, authority_instance_id=AUTHORITY,
                  phase=ProgressPhase.TURN_ENDED, occurred_at=stamp)
    record.progress_events = [DispatchProgressEventV1(
        **fields, sequence=sequence, idempotency_key=f"event-{sequence}",
        summary=f"checkpoint {sequence}",
        tool_details=[ProgressToolDetailV1(title="fixture", kind="tool", status="completed")],
        delivered_at=stamp if sequence == 3 else None,
        delivery_attempts=2, delivery_error="retained failure",
    ) for sequence in (2, 1, 3)]
    record.progress_heartbeat = DispatchProgressHeartbeatV1(
        **fields, sequence=1, idempotency_key="a-heartbeat", summary="heartbeat",
    )
    other = record.model_copy(deep=True, update={"dispatch_id": "other", "target_instance_id": "other-origin"})
    for event in other.progress_events:
        event.dispatch_id = other.dispatch_id
        event.originating_instance_id = other.target_instance_id
    other.progress_heartbeat.dispatch_id = other.dispatch_id
    other.progress_heartbeat.originating_instance_id = other.target_instance_id
    (root / "dispatch_mutations.json").write_text(json.dumps({
        row.dispatch_id: row.model_dump(mode="json") for row in (record, other)
    }))
    return DispatchStore(root)


def transport(rows):
    return [(r.dispatch_id, r.authority_instance_id, r.authority_url,
             p.model_dump(mode="json")) for r, p in rows]


def test_delivery_projection_preserves_order_fields_reload_and_legacy_aliases(tmp_path):
    store = seed(tmp_path)
    try:
        legacy = store.pending_progress(ORIGIN)
        expected = transport(legacy)
        rows = store.pending_progress_deliveries(ORIGIN)
        assert transport(rows) == expected
        assert [p.idempotency_key for _, p in rows] == ["a-heartbeat", "event-1", "event-2"]
        assert all(r is rows[0][0] for r, _ in rows)
        with pytest.raises(FrozenInstanceError):
            rows[0][0].authority_url = "changed"
        rows[1][1].tool_details[0].title = "changed"
        rows[0][1].summary = "changed"
        assert transport(store.pending_progress_deliveries(ORIGIN)) == expected
        assert legacy[0][0] is not legacy[1][0]
        assert legacy[1][1] is next(p for p in legacy[1][0].progress_events if p.sequence == 1)
        legacy[1][1].tool_details[0].title = "changed"
        assert legacy[2][0].progress_events[1].tool_details[0].title == "fixture"
        with patch.object(store, "_snapshot", side_effect=AssertionError("No full retained record copy")):
            assert transport(store.pending_progress_deliveries(ORIGIN)) == expected
        store.close()
        store = DispatchStore(tmp_path)
        assert transport(store.pending_progress_deliveries(ORIGIN)) == expected
        assert store.pending_progress_deliveries("absent") == []
    finally:
        store.close()


def test_deferred_auxiliary_reader_remains_fenced(tmp_path):
    store = DispatchStore(tmp_path, read_only=True, deferred_read_only=True)
    try:
        with pytest.raises(DispatchStoreReadOnlyError, match="reads are deferred"):
            store.pending_progress_deliveries(ORIGIN)
        assert not (tmp_path / "dispatch_mutations.db").exists()
    finally:
        store.close()


def test_delivery_read_sees_committed_publication_without_waiting_for_writer(tmp_path):
    store = seed(tmp_path)
    entered, release = Event(), Event()
    def pause(boundary):
        if boundary == "commit_before":
            entered.set()
            assert release.wait(5), "test must release writer"
    store._fault_injector = pause
    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            writer = pool.submit(store.mark_progress_delivered, DISPATCH, "event-1")
            try:
                assert entered.wait(5)
                rows = pool.submit(store.pending_progress_deliveries, ORIGIN).result(1)
                assert "event-1" in [p.idempotency_key for _, p in rows]
                assert not writer.done()
            finally:
                release.set()
            writer.result(5)
        assert "event-1" not in [p.idempotency_key for _, p in store.pending_progress_deliveries(ORIGIN)]
    finally:
        release.set()
        store._fault_injector = None
        store.close()


@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_delivery_publication_matches_commit_outcome(tmp_path, boundary, committed):
    store = seed(tmp_path)
    def fail(observed):
        if observed == boundary:
            raise RuntimeError("disposable commit boundary")
    try:
        store._fault_injector = fail
        with pytest.raises(RuntimeError, match="disposable commit boundary"):
            store.mark_progress_delivered(DISPATCH, "event-1")
        assert ("event-1" not in [p.idempotency_key for _, p in store.pending_progress_deliveries(ORIGIN)]) is committed
        store._fault_injector = None
        store.close()
        store = DispatchStore(tmp_path)
        assert ("event-1" not in [p.idempotency_key for _, p in store.pending_progress_deliveries(ORIGIN)]) is committed
    finally:
        store._fault_injector = None
        store.close()


@pytest.mark.asyncio
async def test_projection_retries_transport_and_acknowledges_locally(tmp_path):
    store = seed(tmp_path)
    calls = []
    def respond(request):
        calls.append((str(request.url), request.headers["Idempotency-Key"],
                      request.headers["X-PA-Origin-Instance-ID"], json.loads(request.content)))
        return httpx.Response(503 if len(calls) == 1 else 200)
    service = ProgressService(store, instance_id=ORIGIN, token="")
    service._client = httpx.AsyncClient(transport=httpx.MockTransport(respond))
    try:
        target, event = store.pending_progress_deliveries(ORIGIN)[1]
        await service._send(target, event)
        retry = store.pending_progress_deliveries(ORIGIN)[1]
        assert retry[1].delivery_attempts == event.delivery_attempts + 1
        await service._send(*retry)
        assert calls[0] == calls[1]
        assert calls[0][0] == f"https://authority.example/api/fleet/dispatch/{DISPATCH}/progress"
        assert calls[0][1:3] == ("event-1", ORIGIN)
        assert "event-1" not in [p.idempotency_key for _, p in store.pending_progress_deliveries(ORIGIN)]
        local = ProgressService(store, instance_id=AUTHORITY, token="")
        await local._send(*store.pending_progress_deliveries(ORIGIN)[0])
        assert "a-heartbeat" not in [p.idempotency_key for _, p in store.pending_progress_deliveries(ORIGIN)]
        await local.close()
    finally:
        await service.close()
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["native", "adapter", "subclass", "instance_override", "class_override"])
async def test_worker_uses_native_projection_and_preserves_custom_readers(tmp_path, kind):
    store = seed(tmp_path)
    marker = object()
    payload = store.pending_progress(ORIGIN)[0][1]
    custom_rows = [(marker, payload)]
    calls = []
    def custom(origin):
        calls.append(origin)
        return custom_rows
    class CustomStore(DispatchStore):
        pending_progress = staticmethod(custom)
    selected = store
    if kind == "adapter":
        selected = SimpleNamespace(pending_progress=custom)
    elif kind == "subclass":
        selected = object.__new__(CustomStore)
    elif kind == "instance_override":
        store.pending_progress = custom
    service = ProgressService(selected, instance_id=ORIGIN, token="")
    service._send = AsyncMock(side_effect=asyncio.CancelledError)
    try:
        with patch.object(store, "_snapshot", side_effect=AssertionError("No full dispatch hydration")):
            if kind == "class_override":
                with patch.object(DispatchStore, "pending_progress", side_effect=custom):
                    with pytest.raises(asyncio.CancelledError):
                        await service._run()
            else:
                with pytest.raises(asyncio.CancelledError):
                    await service._run()
        target, sent = service._send.call_args.args
        if kind == "native":
            assert target.dispatch_id == DISPATCH
            assert not isinstance(target, DispatchRecord)
            assert calls == []
        else:
            assert target is marker and sent is payload
            assert calls == [ORIGIN]
    finally:
        await service.close()
        store.close()
