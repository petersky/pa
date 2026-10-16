"""Repairs measured after the complete retained-log performance inventory."""
import json
import asyncio
import threading
import sqlite3
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch, MagicMock

import httpx
import pytest
from fastapi import FastAPI

from pa.core.async_runtime import AsyncRuntime
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.modules.fleet import list_dispatches
from pa.execution.reconciliation import CompletionReconciler
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardEvent, EventType, SyncCommit
from pa.sync.event_log import StaleSyncHeadError
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore
from pa.sync.object_catalog import ObjectCatalog
from pa.core.sqlite_metrics import ObservedConnection


def fixture_record(**kwargs):
    return DispatchRecord(mutation_id="fixture", authority_instance_id="local",
                          authority_url="http://fixture", target_instance_id="target", **kwargs)


def test_derived_sync_connections_close_after_publication_and_rollback(tmp_path):
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    objects = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(objects, tmp_path, "fixture")
    original_connect, opened = sqlite3.connect, []
    def tracking_connect(*args, **kwargs):
        connection = original_connect(*args, **kwargs)
        opened.append(connection)  # Prevent GC from hiding a leaked connection.
        return connection
    def event(title):
        return CardEvent(type=EventType.CARD_CREATED, realm_id="default", card_id=title,
            author_principal="user:fixture", author_instance="fixture", payload={"id": title, "title": title})
    try:
        with patch("sqlite3.connect", side_effect=tracking_connect):
            log.append_event(event("first"))
            _, second = log.append_event(event("second"))
            wrong_realm = SyncCommit(hash="wrong", realm_id="other", instance_id="fixture",
                                    parent_hashes=[], event_hashes=[], author_principal="user:fixture")
            with pytest.raises(ValueError, match="another realm"):
                log.index.try_incremental_advance("default", second.hash, "wrong",
                    [("wrong", wrong_realm, [])], lambda e: ("card", e.card_id))
            assert log.index.indexed_head("default") == second.hash
            log.index.reset_realm("default")
            assert log.ensure_indexed("default", second.hash)
        assert len(opened) > 10
        assert all(isinstance(connection, ObservedConnection) for connection in opened)
        for connection in opened:
            with pytest.raises(sqlite3.ProgrammingError, match="closed"):
                connection.execute("SELECT 1")
        assert len(log.entity_history("default", "card", "first")) == 1
        assert log.get_head("default") == second.hash
    finally:
        for connection in opened:
            connection.close()


def test_dispatch_list_preserves_protocol_gaps_controls_and_detached_rows(tmp_path):
    store = DispatchStore(tmp_path)
    proof = "fixture " * 25000
    item = store.put(fixture_record(
        state="running", session_id="session", progress_protocol_version=1,
        progress_highest_accepted_sequence=16,
        progress_accepted_ranges=[[i, i] for i in range(2, 17, 2)],
        progress_compacted_ranges=[[i, i] for i in range(2, 17, 2)],
        placement_decision={"proof": proof}, materialization_plan={"proof": proof},
        sync_evidence={"degraded_peers": [{"instance_id": str(i), "proof": proof} for i in range(8)]},
    ))
    try:
        expected = item.public_dict()
        with patch.object(store, "_snapshot", side_effect=AssertionError("copied stored proofs")):
            rows = store.public_statuses(target_instance_id="target")
            assert store.public_statuses(target_instance_id="other") == []
        row, = rows
        assert row["stale_session_recovery"]["eligible_candidate"]
        assert row["sync_evidence"]["degraded_peer_count"] == 8
        assert row["progress"]["sequence_gap"]
        for field in ("missing_ranges", "compacted_ranges"):
            assert len(row["progress"][field]) == 8
            assert row["progress"][field] == expected["progress"][field]
        assert len(json.dumps(rows)) < 5000
        row["progress"]["compacted_ranges"][0][0] = 999
        assert store.get(item.dispatch_id).progress_compacted_ranges[0][0] == 2
        assert store.get(item.dispatch_id).placement_decision["proof"] == proof
    finally:
        store.close()


@pytest.mark.asyncio
async def test_dispatch_list_http_defaults_compact_and_keeps_explicit_full_details(tmp_path):
    store = DispatchStore(tmp_path)
    runtime = AsyncRuntime()
    item = store.put(fixture_record(state="blocked", placement_decision={"proof": "fixture " * 25000}))
    services = {"dispatch_store": store, "async_runtime": runtime}
    app = FastAPI()
    app.state.ctx = SimpleNamespace(services=services, require_service=services.__getitem__)
    app.add_api_route("/jobs", list_dispatches)
    try:
        with patch("pa.modules.fleet.require_user"), patch.object(
            store, "list", side_effect=AssertionError("default list copied full records")
        ):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                status = await client.get("/jobs")
                assert status.status_code == 200
                assert status.json()[0]["view"] == "status"
                assert (await client.get("/jobs?view=invalid")).status_code == 422
        with patch("pa.modules.fleet.require_user"):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                full = await client.get("/jobs?view=full")
        assert full.json()[0]["placement_decision"] == item.placement_decision
        assert len(full.content) > 20 * len(status.content)
        assert full.json()[0]["dispatch_id"] == status.json()[0]["dispatch_id"]
        metrics = runtime.snapshot()["work"]["metrics"]
        assert metrics["http.dispatch_list.rows_returned"]["count"] == 2
        assert metrics["http.dispatch_list.response_bytes"]["count"] == 2
    finally:
        await runtime.close()
        store.close()


@pytest.mark.asyncio
async def test_reconciliation_scan_avoids_proofs_and_reports_real_progress_between_candidates(tmp_path):
    store = DispatchStore(tmp_path)
    due = datetime.now(UTC)
    proof = {"rows": [{"proof": "fixture " * 1000} for _ in range(100)]}
    for i in range(3):
        store.put(DispatchRecord(mutation_id=f"m-{i}", dispatch_id=f"d-{i}",
            authority_instance_id="local", authority_url="http://fixture", target_instance_id="target",
            reconciliation_state="pending", placement_decision=proof))
    later = store.put(DispatchRecord(mutation_id="later", authority_instance_id="local",
        authority_url="http://fixture", target_instance_id="target", reconciliation_state="blocked",
        reconciliation_next_retry_at=due + timedelta(days=1)))
    prompted = store.put(DispatchRecord(mutation_id="prompted", authority_instance_id="local",
        authority_url="http://fixture", target_instance_id="target", reconciliation_state="prompted",
        reconciliation_next_retry_at=due + timedelta(days=1)))
    with patch.object(store, "_snapshot", side_effect=AssertionError("scan copied retained proofs")):
        candidates = store.reconciliation_candidates(due_at=due)
    assert later.dispatch_id not in {x.dispatch_id for x in candidates}
    assert prompted.dispatch_id in {x.dispatch_id for x in candidates}
    assert all(not hasattr(x, "placement_decision") for x in candidates)
    # The real scanner must publish progress between completed candidates,
    # while a held individual candidate receives no synthetic timer heartbeat.
    service = CompletionReconciler(store, SimpleNamespace(async_runtime=None),
                                  MagicMock(), MagicMock(), lambda: None)
    finished, beats = [], []
    held = asyncio.Event()
    async def advance(record):
        if finished:
            held.set()
            await asyncio.Event().wait()
        finished.append(record.dispatch_id)
    task = None
    try:
        with patch.object(service, "_advance", side_effect=advance), patch.object(
            service, "_recover_prompted", side_effect=advance
        ), patch.object(store, "_snapshot", side_effect=AssertionError("scan copied proofs")):
            task = asyncio.create_task(service._run(lambda: beats.append(len(finished))))
            await asyncio.wait_for(held.wait(), 2)
            assert beats[-1] == 1
            before = len(beats)
            await asyncio.sleep(0)
            assert len(beats) == before
    finally:
        if task:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("seed_local", [False, True])
async def test_kernel_sync_engine_fast_forward_publishes_matching_projection(tmp_path, seed_local):
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                        agent_enabled=False, telemetry_enabled=False, peers=[], instance_id="fixture")
    app = Kernel.boot(settings=settings).build_app()
    async with app.router.lifespan_context(app):
        ctx = app.state.ctx
        log = ctx.require_service("event_log")
        engine = ctx.require_service("sync_engine")
        def create(title):
            return CardEvent(type=EventType.CARD_CREATED, realm_id="default", card_id=title,
                author_principal="user:fixture", author_instance="fixture", payload={"id": title, "title": title})
        def prepare():
            if seed_local:
                ctx.store.commit_event(create("local"))
            parent = log.get_head("default")
            digest = log.store.put_json(create("remote").model_dump(mode="json"))
            remote = SyncCommit(hash="", realm_id="default", instance_id="peer", parent_hashes=[parent] if parent else [],
                                event_hashes=[digest], author_principal="user:fixture")
            remote.hash = log.store.put_json(remote.model_dump(mode="json"))
            return remote
        remote = await asyncio.to_thread(prepare)
        old_head = log.get_head("default")
        old_projection = ctx.store.get_projection_head("default")
        # A stale expected ref must be rejected before touching projections.
        with pytest.raises(StaleSyncHeadError):
            await asyncio.to_thread(engine._publish_head_with_projection,
                "default", remote.hash, expected_head="another-head")
        assert log.get_head("default") == old_head
        assert ctx.store.get_projection_head("default") == old_projection
        assert ctx.store.get_card("remote") is None
        # A projection error must not publish a target that cannot be served.
        original_apply = ctx.store.apply_event
        def failed_apply(event):
            if event.card_id == "remote":
                raise RuntimeError("fixture projection failure")
            return original_apply(event)
        with patch.object(ctx.store, "apply_event", side_effect=failed_apply):
            with pytest.raises(RuntimeError, match="fixture projection failure"):
                await engine.apply_realm_head("default", remote.hash, "sync.reconcile_head",
                    engine._reconcile_remote_head, "default", remote.hash)
        assert log.get_head("default") == old_head
        assert ctx.store.get_projection_head("default") == old_projection
        assert ctx.store.get_card("remote") is None
        # Derived index publication must not convoy unrelated startup metadata.
        entered, release = threading.Event(), threading.Event()
        original_index = log.ensure_indexed
        def held_index(realm_id, head=None):
            if head == remote.hash:
                entered.set()
                release.wait(10)
            return original_index(realm_id, head)
        await asyncio.to_thread(ctx.store.save_session, AgentSession(id="binding", agent_name="codex"))
        with patch.object(log, "ensure_indexed", side_effect=held_index):
            applying = asyncio.create_task(engine.apply_realm_head("default", remote.hash, "sync.reconcile_head",
                                                engine._reconcile_remote_head, "default", remote.hash))
            try:
                assert await asyncio.to_thread(entered.wait, 3)
                await asyncio.wait_for(asyncio.to_thread(ctx.store.set_session_execution_binding,
                    "binding", {"version": 1, "origin_instance_id": "fixture"},
                    reason="workspace_binding_initialized", expected_binding={}), 3)
                assert ctx.store.get_session("binding").execution_binding["origin_instance_id"] == "fixture"
            finally:
                release.set()
            result = await applying
        assert result["advanced"], result
        assert log.get_head("default") == remote.hash
        assert ctx.store.get_projection_head("default") == remote.hash
        assert ctx.store.get_card("remote").title == "remote"
