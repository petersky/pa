"""Correctness boundaries for bounded status and short transcript writes."""
from __future__ import annotations

import asyncio
import json
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from pa.core.async_runtime import AsyncRuntime
from pa.core.kernel import _ResponsivenessMiddleware
from pa.core.operation_budget import measured_lock
from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage
from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchWorker
from pa.modules.instance import _runtime_status_snapshot, async_runtime_status
from pa.modules.fleet import get_dispatch, authority_dispatch_status


def record(**kwargs):
    return DispatchRecord(mutation_id="m", authority_instance_id="local",
                          authority_url="http://fixture.invalid", target_instance_id="target", **kwargs)


def test_runtime_does_not_copy_history_scan_sql_or_take_dispatch_writer_lock(tmp_path):
    store = DispatchStore(tmp_path)
    store.put(record(state="blocked"))
    runtime = AsyncRuntime()
    worker = DispatchWorker(store, lambda _: asyncio.sleep(0))
    services = {"async_runtime": runtime, "dispatch_store": store, "dispatch_worker": worker}
    ctx = SimpleNamespace(services=services, require_service=services.__getitem__, store=SimpleNamespace())
    entered, release = threading.Event(), threading.Event()
    def writer():
        with store._lock:
            entered.set()
            assert release.wait(5)
    with ThreadPoolExecutor() as pool:
        future = pool.submit(writer)
        assert entered.wait(2)
        try:
            with patch.object(store, "_snapshot", side_effect=AssertionError("copied history")), patch.object(
                store, "storage_metrics", side_effect=AssertionError("scanned SQLite")
            ), patch.object(store, "queue_snapshot", side_effect=AssertionError("hydrated queue")):
                snapshot = _runtime_status_snapshot(ctx)
            assert snapshot["dispatch_worker"]["waiting_dispatch_count"] == 1
            assert snapshot["dispatch_storage"]["measurement_state"] == "not_measured"
        finally:
            release.set()
        future.result()
    store.close()


def test_status_projection_preserves_polling_fields_without_copying_plans(tmp_path):
    store = DispatchStore(tmp_path)
    item = record(state="running", session_id="session", placement_decision={
        "tie_breaking_reason": "chosen target", "proof": "large " * 10000,
    }, request_payload={"execution_selection": {"proof": "large " * 10000}},
        materialization_plan={"summary": "ready", "proof": "large " * 10000})
    item = store.transition(item, "running", "Working")
    with patch.object(store, "_snapshot", side_effect=AssertionError("deep copied")):
        status = store.public_status(item.dispatch_id)
    assert status["state"] == "running"
    assert status["events"][-1]["message"] == "Working"
    assert status["placement_decision"] == {"tie_breaking_reason": "chosen target"}
    assert status["materialization_plan"] == {"summary": "ready"}
    assert "execution_selection" not in status
    assert len(json.dumps(status)) < 4000
    status["events"][0]["message"] = "caller change"
    assert store.get(item.dispatch_id).events[-1].message == "Working"
    assert len(json.dumps(store.get(item.dispatch_id).public_dict())) > 100000
    assert store.public_status("missing") is None
    store.close()


def test_transcript_fsync_precedes_writer_transaction_and_gc_waits_for_publication(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    entered, release = threading.Event(), threading.Event()
    def slow_fsync(_fd):
        entered.set()
        assert release.wait(5)
    event = TranscriptEvent(session_id="s", seq=1, event_type="output", payload={"text": "cold " * 2000})
    with ThreadPoolExecutor() as pool, patch("pa.domain.transcript_storage.os.fsync", side_effect=slow_fsync):
        append = pool.submit(store.append, [event])
        assert entered.wait(2)
        try:
            # A separate process/connection can acquire SQLite's writer while
            # the object filesystem is stalled. No timeout or retry required.
            with sqlite3.connect(store.db_path, timeout=0.05) as conn:
                conn.execute("BEGIN IMMEDIATE")
                assert conn.execute("SELECT COUNT(*) FROM transcript_events").fetchone()[0] == 0
            gc = pool.submit(store.collect_unreferenced_objects)
        finally:
            release.set()
        append.result()
        assert gc.result() == 0
    assert store.list("s")[0].payload == event.payload
    assert store.metrics()["references"] == 1
    assert store.metrics()["missing_objects"] == 0


def test_sparse_replay_is_exact_and_duplicate_sequence_references_balance(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    events = [TranscriptEvent(session_id="s", seq=i, event_type="chunk", payload={"text": str(i)})
              for i in range(1, 1001)]
    store.append(events)
    payload = {"text": "cold " * 1000}
    replacements = [TranscriptEvent(session_id="s", seq=seq, event_type="chunk", payload=payload)
                    for seq in (1, 1000, 1)]
    before = store.performance.snapshot()["metrics"]["lookup_rows_returned"]["total"]
    store.append(replacements)
    after = store.performance.snapshot()["metrics"]["lookup_rows_returned"]["total"]
    assert after - before == 2  # Never hydrate the 998 intervening rows.
    assert store.metrics()["references"] == 2
    assert store.metrics()["events"] == 1000
    assert store.list("s", after_seq=1, limit=1)[0].payload == {"text": "2"}
    # Moving an event id also replaces its old row; balance both unique keys.
    store.append([replacements[-1].model_copy(update={"seq": 2})])
    assert store.metrics()["references"] == 2
    assert store.metrics()["events"] == 999
    store.prune(["s"])
    assert store.metrics()["references"] == 0


@pytest.mark.asyncio
async def test_http_metrics_use_route_templates_bytes_and_exclude_sse():
    runtime = AsyncRuntime()
    app = FastAPI()
    app.add_middleware(_ResponsivenessMiddleware, runtime_getter=lambda: runtime)
    @app.get("/item/{item_id}")
    async def item(item_id: str):
        return {"id": item_id}
    @app.get("/events")
    async def events():
        async def body():
            yield "data: ready\n\n"
        return StreamingResponse(body(), media_type="text/event-stream")
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
            result = await client.get("/item/private-id?private=query")
            await client.get("/events")
        requests = runtime.snapshot()["requests"]
        assert requests["count"] == 1
        assert requests["excluded_sse_requests"] == 1
        assert requests["response_bytes"] == len(result.content)
        assert list(requests["routes"]) == ["/item/{item_id}"]
        assert "private" not in json.dumps(requests)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_lock_observation_covers_operations_without_deadline_policy():
    runtime = AsyncRuntime()
    def work():
        with measured_lock(threading.Lock()):
            pass
    try:
        for _ in range(260):
            await runtime.run_blocking("sqlite.card_write", work)
        metrics = runtime.snapshot()["operations"]["sqlite.card_write"]
        assert metrics["completed"] == 260
        assert metrics["runtime_ms"]["samples"] == 256
        assert metrics["queue_ms"]["samples"] == 256
        assert metrics["python_lock_hold_ms"]["samples"] == 256
        assert metrics["python_lock_wait_ms"]["p99"] is not None
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_sse_section_does_not_start_full_runtime_diagnostics():
    request = SimpleNamespace(app=None)
    result = await async_runtime_status(request, section="sse")
    assert list(result) == ["sse_connections"]


@pytest.mark.asyncio
async def test_dispatch_status_http_and_authority_forwarding_preserve_identity(tmp_path):
    store = DispatchStore(tmp_path)
    item = store.put(record(state="running", card_version="version-1", target_instance_name="old name",
                            placement_decision={"proof": "fixture " * 1000}))
    ctx = SimpleNamespace(services={"dispatch_store": store}, settings=SimpleNamespace(instance_id="local"))
    app = FastAPI()
    app.state.ctx = ctx
    app.add_api_route("/dispatch/{dispatch_id}", get_dispatch)
    app.add_api_route("/authority/{authority_instance_id}/{dispatch_id}", authority_dispatch_status)
    try:
        with patch("pa.modules.fleet.require_user"), patch(
            "pa.modules.fleet._peer_authority_json", new_callable=AsyncMock, return_value={"view": "status"}
        ) as forward:
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                full = (await client.get(f"/dispatch/{item.dispatch_id}")).json()
                status = (await client.get(f"/dispatch/{item.dispatch_id}?view=status")).json()
                local = (await client.get(f"/authority/local/{item.dispatch_id}?view=status")).json()
                await client.get(f"/authority/remote/{item.dispatch_id}?view=status")
                assert (await client.get("/dispatch/missing?view=status")).status_code == 404
                assert (await client.get(f"/dispatch/{item.dispatch_id}?view=invalid")).status_code == 422
        assert full["placement_decision"] == item.placement_decision
        assert status["card_version"] == "version-1"
        assert status["target_instance_name"] == full["target_instance_name"] == "old name"
        assert status == local
        assert forward.call_args.args[1:] == ("remote", "GET", f"dispatch-jobs/{item.dispatch_id}?view=status")
    finally:
        store.close()


def test_repeated_cold_payloads_coalesce_refs_and_rollback_keeps_canonical_rows(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    events = [TranscriptEvent(session_id="s", seq=i, event_type="chunk", payload={"text": "cold " * 1000})
              for i in range(1, 11)]
    store.append(events)
    assert store.performance.snapshot()["metrics"]["sqlite_rows_committed"]["total"] == 11
    assert store.metrics()["references"] == 10
    # Abort after event replacement but before reference publication. Both
    # event identity and refcounts must roll back together.
    with sqlite3.connect(store.db_path) as conn:
        conn.execute("""CREATE TRIGGER fail_fixture BEFORE INSERT ON transcript_objects
                        BEGIN SELECT RAISE(ABORT, 'fixture failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="fixture failure"):
        store.append([events[0].model_copy(update={"payload": {"text": "different " * 1000}})])
    assert store.list("s", limit=1)[0].payload == events[0].payload
    assert store.metrics()["references"] == 10
    assert store.collect_unreferenced_objects() == 1


def test_sqlite_writer_acquisition_is_measured_separately_from_hold(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    attempt = threading.Event()
    original = store._conn
    @contextmanager
    def traced_connection():
        with original() as conn:
            conn.set_trace_callback(lambda sql: attempt.set() if sql == "BEGIN IMMEDIATE" else None)
            yield conn
    with sqlite3.connect(store.db_path) as other, ThreadPoolExecutor() as pool:
        other.execute("BEGIN IMMEDIATE")
        with patch.object(store, "_conn", traced_connection):
            pending = pool.submit(store.append, [TranscriptEvent(session_id="s", seq=1, event_type="chunk")])
            try:
                assert attempt.wait(2)
                time.sleep(0.05)
            finally:
                other.commit()
            pending.result()
    metrics = store.performance.snapshot()["metrics"]
    assert metrics["sqlite_writer_acquire_ms"]["p50"] >= 40
    assert metrics["transaction_hold_ms"]["p50"] < metrics["sqlite_writer_acquire_ms"]["p50"]
