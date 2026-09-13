"""Regression checks for the measured idle churn and page amplification."""
from __future__ import annotations

import asyncio
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pa.config import Settings
from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
from pa.domain.models import AgentSession, CardCreate, PeerRoute
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.selection import ExecutionCandidate
from pa.execution.selection_service import SelectionService
from pa.fleet.overview import FleetOverviewCache, field
from pa.instance.maintenance import compact_database
from pa.sync.object_catalog import ObjectCatalog


def test_projection_reuses_only_its_thread_and_rolls_back_nested_transactions(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    with store._conn() as first:
        first_id = id(first)
    with store._conn(busy_timeout_ms=7) as second:
        assert id(second) == first_id
        assert second.execute("PRAGMA busy_timeout").fetchone()[0] == 7
    with pytest.raises(ValueError), store._conn():
        store.create_card(CardCreate(title="must roll back"))
        raise ValueError("abort outer transaction")
    assert store.list_cards() == []
    with store._conn() as conn:
        assert conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert not conn.in_transaction

    def worker():
        with store._conn() as conn:
            connection_id = id(conn)
            assert store.list_cards() == []
        store.close_thread_connection()
        return connection_id

    with ThreadPoolExecutor(max_workers=1) as pool:
        assert pool.submit(worker).result() != first_id
    store.close_thread_connection()


def test_fleet_persists_only_changed_dimension_and_compacts_legacy_and_remote(tmp_path):
    legacy_dispatch = {"dispatch_id": "d", "card_id": "c", "state": "running",
                       "materialization_plan": {"body": "x" * 100_000},
                       "goal_admission_validation_proof": "y" * 100_000}
    (tmp_path / "fleet_overview_cache.json").write_text(json.dumps({
        "revision": 1, "instances": {"local": {"activity": field("fresh", {"dispatches": [legacy_dispatch]})}}
    }))
    cache = FleetOverviewCache(tmp_path)
    assert "materialization_plan" not in cache.get("local", "activity")["value"]["dispatches"][0]
    cache.put("remote", "activity", field("fresh", {"dispatches": [legacy_dispatch]}))
    activity_files = {path: path.read_bytes() for path in cache.dimensions_path.glob("*.json")}
    cache.put("local", "sync", field("fresh", {"head": "h"}))
    assert all(path.read_bytes() == body for path, body in activity_files.items())
    assert sum(path.stat().st_size for path in cache.dimensions_path.glob("*.json")) < 4000
    cache.invalidate("local", "activity")
    reopened = FleetOverviewCache(tmp_path)
    assert reopened.get("local", "activity") is None
    assert reopened.get("local", "sync")["value"] == {"head": "h"}
    reopened.put("remote", "activity", field("timeout", error="offline"))
    assert reopened.get("remote", "activity")["state"] == "stale"


def _dispatch(**kwargs):
    return DispatchRecord(mutation_id="m", authority_instance_id="local",
                          authority_url="http://local", target_instance_id="local", **kwargs)


def test_reconciliation_filters_before_limit_and_copying(tmp_path):
    store = DispatchStore(tmp_path)
    now = datetime.now(UTC)
    due = _dispatch(reconciliation_state="pending", updated_at=now - timedelta(days=1))
    records = [due, _dispatch(reconciliation_state="blocked", reconciliation_next_retry_at=now + timedelta(hours=1))]
    records.extend(_dispatch(state="completed", updated_at=now) for _ in range(1001))
    store._records = {record.dispatch_id: record for record in records}
    with patch.object(store, "_snapshot", wraps=store._snapshot) as copy:
        selected = store.list(limit=1, reconciliation_states={"pending", "blocked", "prompted"}, reconciliation_due_at=now)
        assert [record.dispatch_id for record in selected] == [due.dispatch_id]
        assert copy.call_count == 1
    store.close()


def test_catalog_totals_follow_upserts_deletes_rollback_and_reopen(tmp_path):
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    catalog.record("a", b"123", mtime_ns=10)
    catalog.record("b", b"12345", mtime_ns=20)
    catalog.record("a", b"1", mtime_ns=30)
    assert (catalog.count(), catalog.total_bytes(), catalog.age_bounds_ns()) == (2, 6, (20, 30))
    with pytest.raises(ValueError), catalog._db() as conn:
        conn.execute("DELETE FROM objects")
        raise ValueError("rollback")
    catalog.discard("b")
    reopened = ObjectCatalog(catalog.path)
    assert (reopened.count(), reopened.total_bytes(), reopened.age_bounds_ns()) == (1, 1, (30, 30))
    with patch.object(reopened, "_conn", wraps=reopened._conn) as connect:
        statements = []
        original = connect.side_effect = lambda: sqlite3.connect(catalog.path)
        def traced():
            conn = original()
            conn.row_factory = sqlite3.Row
            conn.set_trace_callback(statements.append)
            return conn
        connect.side_effect = traced
        reopened.status_payload("default", expected_reachable=1)
    assert not any("COUNT(*) FROM objects" in sql or "SUM(size_bytes)" in sql for sql in statements)


@pytest.mark.asyncio
async def test_periodic_sync_backoff_is_per_peer_and_explicit_retry_bypasses_it(tmp_path):
    from tests.test_realm_convergence import _Node

    node = _Node(tmp_path, "local", "Local")
    node.peers.add_route(PeerRoute(realm_id="default", target_url="http://dead", target_instance_id="dead"))
    node.peers.add_route(PeerRoute(realm_id="default", target_url="http://healthy", target_instance_id="healthy"))
    async def fetch(client, realm, route, **kwargs):
        return {"instance_id": route.target_instance_id, "name": route.target_instance_id,
                "url": route.target_url, "head": None,
                "status": "unavailable" if route.target_instance_id == "dead" else "reachable"}
    with patch.object(node.engine, "_fetch_peer", AsyncMock(side_effect=fetch)) as probe:
        await node.engine.converge_realm("default", background=True)
        assert probe.await_count == 2
        await node.engine.converge_realm("default", background=True)
        assert probe.await_count == 3
        state = await node.engine.converge_realm("default")
        assert probe.await_count == 5
        assert state["phase"] == "degraded"
    await node.engine._client.aclose()


@pytest.mark.asyncio
async def test_valid_catalog_returns_while_refresh_is_pending(tmp_path):
    settings = Settings(data_dir=tmp_path, instance_id="local")
    service = SelectionService(settings, None)
    stamp = datetime.now(UTC) - timedelta(seconds=90)
    candidate = ExecutionCandidate(instance_id="local", harness="codex", connection="default",
        catalog_source="test", catalog_version="1", observed_at=stamp, freshness="fresh", readiness="ready")
    service.store.catalog = lambda scope: ([candidate.model_dump(mode="json")], stamp)
    started, release = asyncio.Event(), asyncio.Event()
    async def refresh():
        started.set()
        await release.wait()
    with patch.object(service, "_refresh_catalog", side_effect=refresh) as probe:
        result = await asyncio.wait_for(service.local_catalog(refresh=True, stale_while_revalidate=True), 1)
        await started.wait()
        assert result[0].freshness == "fresh"
        await service.local_catalog(refresh=True, stale_while_revalidate=True)
        assert probe.call_count == 1
        release.set()
        await service.close()


def test_sidebar_filters_in_sql_before_hydrating_sessions(tmp_path):
    store = CardProjection(tmp_path / "pa.db")
    store.save_session(AgentSession(id="chat", agent_name="codex", purpose="chat"))
    store.save_session(AgentSession(id="job", agent_name="codex", purpose="automated_run"))
    with patch.object(store, "_row_to_session", wraps=store._row_to_session) as hydrate:
        assert [session.id for session in store.list_sessions(purposes=("chat",), archived=False)] == ["chat"]
        assert hydrate.call_count == 1
    store.close_thread_connection()


def test_offline_compaction_reclaims_space_and_refuses_running_server(tmp_path):
    settings = SimpleNamespace(data_dir=tmp_path, db_path=tmp_path / "pa.db")
    with sqlite3.connect(settings.db_path) as conn:
        conn.execute("CREATE TABLE evidence(id TEXT PRIMARY KEY, body BLOB)")
        conn.executemany("INSERT INTO evidence VALUES (?, ?)", [(str(i), b"x" * 10_000) for i in range(100)])
        conn.execute("DELETE FROM evidence WHERE id != '0'")
    lock = DataDirWriterLock(tmp_path)
    lock.acquire()
    try:
        with pytest.raises(DataDirAlreadyOwnedError):
            compact_database(settings)
    finally:
        lock.release()
    result = compact_database(settings)
    assert result["compacted"]
    assert result["after"]["bytes"] < result["before"]["bytes"] / 5
    assert result["after"]["free_bytes"] == 0
    with sqlite3.connect(settings.db_path) as conn:
        assert conn.execute("SELECT id, length(body) FROM evidence").fetchall() == [("0", 10_000)]
