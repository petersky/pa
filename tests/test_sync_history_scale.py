"""Scale, epoch, GC, and indexed-status coverage for sync history."""

from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.domain.models import Card, CardEvent, EventType, PeerRoute, SyncCommit
from pa.fleet.membership import MembershipStore
from pa.network.peer_table import PeerTable
from pa.sync.compaction import compact_realm
from pa.sync.engine import LEGACY_BUNDLE_SOFT_LIMIT, SYNC_PROTOCOL, SyncEngine
from pa.sync.epochs import EpochRegistry, acknowledge_epoch_object
from pa.sync.event_log import EventLog
from pa.sync.gc import GcPlanner
from pa.sync.object_catalog import ObjectCatalog
from pa.sync.object_store import ObjectStore


def _append_chain(log: EventLog, count: int, *, realm: str = "default") -> str:
    head = None
    for index in range(count):
        _, commit = log.append_event(
            CardEvent(
                type=EventType.CARD_CREATED if index == 0 else EventType.CARD_UPDATED,
                realm_id=realm,
                card_id="scale-card",
                author_principal="user:test",
                author_instance="local",
                payload={
                    "id": "scale-card",
                    "title": f"t-{index}",
                    "updated_at": f"v{index}",
                },
            )
        )
        head = commit.hash
    assert head
    return head


def test_object_catalog_status_avoids_filesystem_scan(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    head = _append_chain(log, 5)
    log.ensure_indexed("default", head)
    catalog.publish_realm_stats(
        "default",
        commit_count=5,
        event_count=5,
        auxiliary_count=0,
        unreachable_count=0,
        reachable_bytes=100,
        head_hash=head,
        oldest_reachable_ns=1,
        newest_reachable_ns=2,
    )
    catalog.sample_growth()
    time.sleep(0.01)
    catalog.sample_growth()

    with patch.object(store, "list_hashes", side_effect=AssertionError("scandir")):
        payload = catalog.status_payload("default")
        engine = SyncEngine(
            Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False),
            store,
            log,
            PeerTable(tmp_path),
            MembershipStore(tmp_path),
        )
        status = engine.status("default")

    assert payload["object_count"] == 10
    assert status["history"]["realm"]["reachable"]["commits"] == 5
    assert status["object_count"] == 10
    assert status["protocol"] == SYNC_PROTOCOL


def test_snapshot_epoch_requires_ack_before_gc(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "authority", cursor_secret="test")
    registry = EpochRegistry(tmp_path)
    head = _append_chain(log, 3)
    # Orphan unreachable object.
    orphan = store.put_json({"type": "snapshot", "realm_id": "default", "cards": []})
    catalog.record(orphan, store.get(orphan) or b"{}")

    epoch_hash = compact_realm(
        store,
        log,
        "default",
        [Card(id="scale-card", title="scale")],
        registry=registry,
        authority_instance_id="authority",
        advance_epoch=True,
    )
    assert epoch_hash
    assert registry.current("default")["fencing_token"] == 1

    planner = GcPlanner(tmp_path, store, catalog, registry)
    reachable = {head, *log.get_commit(head).event_hashes}  # type: ignore[union-attr]
    # Include full ancestry roughly via index rebuild.
    log.ensure_indexed("default", head)
    for commit_hash, commit in log._iter_commits_parent_first(head):
        reachable.add(commit_hash)
        reachable.update(commit.event_hashes)

    dry = planner.plan(
        "default",
        reachable_hashes=reachable,
        pins=[],
        required_ack_instances=["authority", "peer-offline"],
        dry_run=True,
        safety_window=timedelta(seconds=0),
        now=datetime.now(UTC) + timedelta(days=1),
    )
    assert dry.reclaimable is False
    assert "peer-offline" in dry.missing_acks
    assert orphan in dry.candidates

    acknowledge_epoch_object(
        store,
        registry,
        realm_id="default",
        epoch_hash=epoch_hash,
        instance_id="peer-offline",
    )
    ready = planner.plan(
        "default",
        reachable_hashes=reachable,
        pins=[],
        required_ack_instances=["authority", "peer-offline"],
        dry_run=False,
        safety_window=timedelta(seconds=0),
        now=datetime.now(UTC) + timedelta(days=1),
    )
    assert ready.reclaimable is True
    result = planner.execute(ready.plan_id, confirm=True)
    assert result["deleted_count"] >= 1
    assert store.get(orphan) is None
    assert result["rebootstrap_required"] is True


def test_legacy_large_peer_is_quarantined_without_full_prepare(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    head = _append_chain(log, 3)
    log.ensure_indexed("default", head)
    # Pretend the indexed history is huge.
    with patch.object(
        log,
        "index_status",
        return_value={
            "ready": True,
            "commit_count": LEGACY_BUNDLE_SOFT_LIMIT + 10,
            "event_count": LEGACY_BUNDLE_SOFT_LIMIT + 10,
        },
    ):
        peers = PeerTable(tmp_path)
        peers.add_route(PeerRoute(realm_id="default", target_url="http://legacy"))
        engine = SyncEngine(
            Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False),
            store,
            log,
            peers,
            MembershipStore(tmp_path),
        )
        prepare_calls = 0

        async def boom(*_args, **_kwargs):
            nonlocal prepare_calls
            prepare_calls += 1
            raise AssertionError("must not prepare full bundle")

        engine._prepare_objects = boom  # type: ignore[method-assign]

        async def need_404(method, url, **kwargs):
            import httpx

            if url.endswith("/api/sync/need"):
                return httpx.Response(404, request=httpx.Request(method, url))
            raise AssertionError(url)

        engine._request = need_404  # type: ignore[method-assign]
        result = None

        async def run():
            nonlocal result
            result = await engine._push_peer_v3(
                "default",
                peers.routes_for_realm("default")[0],
                head,
                {"instance_id": "legacy", "name": "legacy", "url": "http://legacy"},
            )

        import asyncio

        asyncio.run(run())
        assert result is not None
        assert result["status"] == "protocol_incompatible"
        assert result["error"]["code"] == "legacy_bundle_too_large"
        assert prepare_calls == 0


def test_dag_index_rebuild_is_checkpointable_and_cancellable(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    head = _append_chain(log, 40)
    from pa.sync.event_log import _event_entity

    inserted = {"count": 0}
    original = log.index._insert_commit

    def counted(conn, realm_id, position, commit_hash, commit, events, event_entity):
        inserted["count"] += 1
        if inserted["count"] == 5:
            log.index.request_cancel(realm_id)
        return original(conn, realm_id, position, commit_hash, commit, events, event_entity)

    with patch.object(log.index, "_insert_commit", side_effect=counted):
        with pytest.raises(RuntimeError, match="cancelled"):
            log.index.rebuild(
                "default",
                head,
                log._authoritative_index_records(head),
                _event_entity,
                force=True,
                resume=False,
            )
    assert log.index.status("default")["state"] == "cancelled"

    # Resume completes without rescanning forever.
    log.index.clear_cancel("default")
    log.index.rebuild(
        "default",
        head,
        log._authoritative_index_records(head),
        _event_entity,
        force=False,
        resume=True,
    )
    assert log.index.indexed_head("default") == head
    assert log.index.status("default")["ready"] is True or log.index.status(
        "default", durable_head=head
    )["ready"]


@pytest.mark.parametrize("object_count", [100_000, 250_000, 500_000])
def test_scale_catalog_and_status_are_sublinear(tmp_path: Path, object_count: int) -> None:
    """Synthetic catalog scale: status must stay fast without object IO."""
    catalog = ObjectCatalog(tmp_path / f"catalog-{object_count}.db")
    now = time.time_ns()
    with catalog._conn() as conn:
        rows = [
            (
                f"{index:064x}",
                64,
                now - index,
                "commit" if index % 2 == 0 else "event",
                "default",
                datetime.now(UTC).isoformat(),
            )
            for index in range(object_count)
        ]
        conn.executemany(
            """INSERT INTO objects(object_hash,size_bytes,mtime_ns,object_class,realm_id,recorded_at)
               VALUES(?,?,?,?,?,?)""",
            rows,
        )
    catalog.publish_realm_stats(
        "default",
        commit_count=object_count // 2,
        event_count=object_count // 2,
        auxiliary_count=0,
        unreachable_count=0,
        reachable_bytes=object_count * 64,
        head_hash="head",
        oldest_reachable_ns=now - object_count,
        newest_reachable_ns=now,
    )
    catalog.sample_growth()
    started = time.perf_counter()
    payload = catalog.status_payload("default")
    elapsed = time.perf_counter() - started
    assert payload["store"]["object_count"] == object_count
    assert elapsed < 0.5


def test_merge_heavy_incremental_index_advance(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    parent = None
    side = None
    for index in range(200):
        event = CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id="default",
            card_id="merge-card",
            author_principal="user:test",
            author_instance="local",
            payload={"title": f"m-{index}", "updated_at": f"v{index}"},
        )
        event_hash = store.put_json(event.model_dump(mode="json"))
        parents = [parent] if parent else []
        if side and index % 20 == 0:
            parents.append(side)
        commit = SyncCommit(
            hash="",
            realm_id="default",
            instance_id="local",
            parent_hashes=parents,
            event_hashes=[event_hash],
            author_principal="user:test",
        )
        commit.hash = store.put_json(commit.model_dump(mode="json"))
        if index % 20 == 10:
            side = commit.hash
        parent = commit.hash
    assert parent
    log.advance_ref("default", parent, expected_head=None)
    assert log.ensure_indexed("default", parent)
    mid = log.index.indexed_head("default")
    assert mid == parent
    # Append one more commit and ensure incremental path works.
    _, newer = log.append_event(
        CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id="default",
            card_id="merge-card",
            author_principal="user:test",
            author_instance="local",
            payload={"title": "final", "updated_at": "final"},
        )
    )
    assert log.index.indexed_head("default") == newer.hash


def test_oversized_commit_event_fanout_is_paginated(tmp_path: Path) -> None:
    """Single-commit fanout must spill across inventory pages, never hard-fail."""
    from pa.sync.engine import SYNC_INVENTORY_MAX_OBJECTS

    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    base = _append_chain(log, 1)
    event_hashes: list[str] = []
    fanout = SYNC_INVENTORY_MAX_OBJECTS + 40
    for index in range(fanout):
        event = CardEvent(
            type=EventType.CARD_UPDATED,
            realm_id="default",
            card_id="fanout-card",
            author_principal="user:test",
            author_instance="local",
            payload={"title": f"e-{index}", "updated_at": f"v{index}"},
        )
        event_hashes.append(store.put_json(event.model_dump(mode="json")))
    commit = SyncCommit(
        hash="",
        realm_id="default",
        instance_id="local",
        parent_hashes=[base],
        event_hashes=event_hashes,
        author_principal="user:test",
    )
    commit.hash = store.put_json(commit.model_dump(mode="json"))
    log.advance_ref("default", commit.hash, expected_head=base)

    engine = SyncEngine(
        Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False),
        store,
        log,
        PeerTable(tmp_path),
        MembershipStore(tmp_path),
    )
    pending = [commit.hash]
    pending_events: list[str] = []
    seen: set[str] = set()
    seen_objects: set[str] = set()
    pages = 0
    while pending or pending_events:
        raw, _parents = engine._inventory_page(pending, seen, pending_events)
        assert len(raw) <= SYNC_INVENTORY_MAX_OBJECTS
        seen_objects.update(raw)
        pages += 1
        assert pages < 20
    assert commit.hash in seen_objects
    assert event_hashes[0] in seen_objects
    assert event_hashes[-1] in seen_objects
    assert pages >= 2


def test_reconcile_catches_up_before_advancing_durable_head(tmp_path: Path) -> None:
    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    _append_chain(log, 1)
    mid_event = CardEvent(
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="scale-card",
        author_principal="user:test",
        author_instance="local",
        payload={"id": "scale-card", "title": "mid", "updated_at": "mid"},
    )
    _, mid_commit = log.append_event(mid_event)
    tip_event = CardEvent(
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="scale-card",
        author_principal="user:test",
        author_instance="local",
        payload={"id": "scale-card", "title": "tip", "updated_at": "tip"},
    )
    _, tip_commit = log.append_event(tip_event)
    # Roll durable tip back to mid while tip objects remain available.
    log.advance_ref("default", mid_commit.hash, expected_head=tip_commit.hash)

    engine = SyncEngine(
        Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False),
        store,
        log,
        PeerTable(tmp_path),
        MembershipStore(tmp_path),
    )
    order: list[str] = []

    def rebuild(realm_id: str, target_head: str | None = None) -> dict:
        order.append(f"catch_up:{target_head}")
        return {"commits_applied": 1, "rebuilt": False, "reason": "fast_forward"}

    engine.on_head_advanced(rebuild)
    original = log.advance_ref

    def tracked_advance(realm_id, head_hash, *, expected_head=None):
        order.append(f"advance:{head_hash}")
        return original(realm_id, head_hash, expected_head=expected_head)

    with patch.object(log, "advance_ref", side_effect=tracked_advance):
        result = engine._reconcile_remote_head("default", tip_commit.hash)
    assert result["advanced"] is True
    assert order[0].startswith("catch_up:")
    assert order[1].startswith("advance:")
    assert log.get_head("default") == tip_commit.hash


def test_catalog_reports_stale_when_below_dag_population(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    head = _append_chain(log, 20)
    log.ensure_indexed("default", head)
    # Simulate a post-upgrade catalog that only saw a few new puts.
    with catalog._conn() as conn:
        conn.execute("DELETE FROM objects")
        conn.commit()
    for object_hash in store.list_hashes()[:3]:
        raw = store.get(object_hash)
        assert raw is not None
        catalog.record(object_hash, raw)

    coverage = catalog.coverage(expected_reachable=40)
    assert coverage["stale"] is True
    assert coverage["ready"] is False
    payload = catalog.status_payload("default", expected_reachable=40)
    assert payload["catalog"]["stale"] is True
    assert payload["catalog"]["ready"] is False
    assert payload["store"]["authoritative"] is False
    assert payload["realm"]["unreachable"]["retained_because"] == (
        "unknown_until_catalog_backfill"
    )


def test_catalog_rebuild_is_checkpointable_and_resumable(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    _append_chain(log, 30)
    # Drop catalog rows to force a full rescan.
    with catalog._conn() as conn:
        conn.execute("DELETE FROM objects")
        conn.commit()

    recorded = {"count": 0}
    original_get = store.get

    def counted(object_hash: str):
        recorded["count"] += 1
        if recorded["count"] == 8:
            catalog.request_cancel()
        return original_get(object_hash)

    with patch.object(store, "get", side_effect=counted):
        with pytest.raises(RuntimeError, match="cancelled"):
            catalog.rebuild_from_store(store, resume=False, force=True)
    partial = catalog.count()
    assert partial > 0
    assert partial < 60

    catalog.clear_cancel()
    result = catalog.rebuild_from_store(store, resume=True)
    assert result["state"] == "completed"
    assert result["resumed"] is True
    assert catalog.count() == len(store.list_hashes())
    assert catalog.coverage(expected_reachable=catalog.count())["ready"] is True


def test_gc_resume_interrupted_after_crash(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "authority", cursor_secret="test")
    registry = EpochRegistry(tmp_path)
    head = _append_chain(log, 2)
    orphan = store.put_json({"type": "snapshot", "realm_id": "default", "cards": []})
    catalog.record(orphan, store.get(orphan) or b"{}")
    epoch_hash = compact_realm(
        store,
        log,
        "default",
        [Card(id="scale-card", title="scale")],
        registry=registry,
        authority_instance_id="authority",
        advance_epoch=True,
    )
    acknowledge_epoch_object(
        store,
        registry,
        realm_id="default",
        epoch_hash=epoch_hash,
        instance_id="authority",
    )
    planner = GcPlanner(tmp_path, store, catalog, registry)
    reachable = {head, *log.get_commit(head).event_hashes}  # type: ignore[union-attr]
    log.ensure_indexed("default", head)
    for commit_hash, commit in log._iter_commits_parent_first(head):
        reachable.add(commit_hash)
        reachable.update(commit.event_hashes)
    plan = planner.plan(
        "default",
        reachable_hashes=reachable,
        pins=[],
        required_ack_instances=["authority"],
        dry_run=False,
        safety_window=timedelta(seconds=0),
        now=datetime.now(UTC) + timedelta(days=1),
    )
    assert plan.reclaimable is True
    # Simulate crash after journalled intent but before deletes complete.
    journal = planner._load_journal()
    journal.setdefault("deletions", []).append(
        {
            "plan_id": plan.plan_id,
            "started_at": datetime.now(UTC).isoformat(),
            "candidates": plan.candidates,
            "state": "in_progress",
        }
    )
    planner._save_journal(journal)
    resumed = planner.resume_interrupted()
    assert resumed["resumed"] == 1
    assert store.get(orphan) is None


def test_offline_peer_pin_blocks_reclaim_with_explicit_rebootstrap(
    tmp_path: Path,
) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    log = EventLog(store, tmp_path, "authority", cursor_secret="test")
    registry = EpochRegistry(tmp_path)
    head = _append_chain(log, 2)
    orphan = store.put_json({"type": "snapshot", "realm_id": "default", "cards": []})
    catalog.record(orphan, store.get(orphan) or b"{}")
    epoch_hash = compact_realm(
        store,
        log,
        "default",
        [Card(id="scale-card", title="scale")],
        registry=registry,
        authority_instance_id="authority",
        advance_epoch=True,
    )
    assert epoch_hash
    planner = GcPlanner(tmp_path, store, catalog, registry)
    reachable = {head}
    log.ensure_indexed("default", head)
    for commit_hash, commit in log._iter_commits_parent_first(head):
        reachable.add(commit_hash)
        reachable.update(commit.event_hashes)
    dry = planner.plan(
        "default",
        reachable_hashes=reachable,
        pins=[],
        required_ack_instances=["authority", "offline-peer"],
        dry_run=True,
        safety_window=timedelta(seconds=0),
        now=datetime.now(UTC) + timedelta(days=1),
    )
    assert dry.reclaimable is False
    assert "offline-peer" in dry.missing_acks
    assert dry.rebootstrap_required is True
    assert any("acknowledge" in note.lower() or "rebootstrap" in note.lower() for note in dry.notes)


def test_corrupt_object_is_classified_without_failing_rebuild(tmp_path: Path) -> None:
    catalog = ObjectCatalog(tmp_path / "catalog.db")
    store = ObjectStore(tmp_path / "objects", catalog=catalog)
    good = store.put(b'{"type":"snapshot","realm_id":"default","cards":[]}')
    # Plant a corrupt blob directly on disk without going through put_json.
    bad_hash = "ab" + ("cd" * 31)
    path = store._path_for(bad_hash)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(b"\xff\xfe not-json")
    result = catalog.rebuild_from_store(store, force=True)
    assert result["recorded"] >= 2
    assert catalog.has(good)
    assert catalog.has(bad_hash)
    with catalog._db() as conn:
        row = conn.execute(
            "SELECT object_class FROM objects WHERE object_hash=?", (bad_hash,)
        ).fetchone()
    assert row is not None
    assert row[0] == "unknown"


def test_mixed_version_peer_without_need_is_quarantined(tmp_path: Path) -> None:
    # Alias of legacy quarantine coverage for the mixed-version acceptance matrix.
    test_legacy_large_peer_is_quarantined_without_full_prepare(tmp_path)


def test_reconcile_skips_advance_when_projection_catch_up_fails(
    tmp_path: Path,
) -> None:
    store = ObjectStore(tmp_path / "objects")
    log = EventLog(store, tmp_path, "local", cursor_secret="test")
    _append_chain(log, 1)
    mid_event = CardEvent(
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="scale-card",
        author_principal="user:test",
        author_instance="local",
        payload={"id": "scale-card", "title": "mid", "updated_at": "mid"},
    )
    _, mid_commit = log.append_event(mid_event)
    tip_event = CardEvent(
        type=EventType.CARD_UPDATED,
        realm_id="default",
        card_id="scale-card",
        author_principal="user:test",
        author_instance="local",
        payload={"id": "scale-card", "title": "tip", "updated_at": "tip"},
    )
    _, tip_commit = log.append_event(tip_event)
    log.advance_ref("default", mid_commit.hash, expected_head=tip_commit.hash)

    engine = SyncEngine(
        Settings(data_dir=tmp_path, instance_id="local", agent_enabled=False),
        store,
        log,
        PeerTable(tmp_path),
        MembershipStore(tmp_path),
    )

    def boom(realm_id: str, target_head: str | None = None) -> dict:
        raise RuntimeError("projection catch-up failed")

    engine.on_head_advanced(boom)
    advanced = {"called": False}
    original = log.advance_ref

    def guarded(realm_id, head_hash, *, expected_head=None):
        advanced["called"] = True
        return original(realm_id, head_hash, expected_head=expected_head)

    with patch.object(log, "advance_ref", side_effect=guarded):
        with pytest.raises(RuntimeError, match="projection catch-up failed"):
            engine._reconcile_remote_head("default", tip_commit.hash)
    assert advanced["called"] is False
    assert log.get_head("default") == mid_commit.hash
