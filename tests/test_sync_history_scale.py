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
