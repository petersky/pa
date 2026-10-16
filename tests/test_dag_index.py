from __future__ import annotations

import concurrent.futures
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.domain.projection import CardProjection
from pa.sync.event_log import DuplicateCardCreateError, EventLog
from pa.sync.object_store import ObjectStore
from pa.sync.dag_index import DagIndex


def _commit(objects: ObjectStore, parent: str | None, event: CardEvent) -> str:
    event_hash = objects.put_json(event.model_dump(mode="json"))
    commit = SyncCommit(
        hash="",
        realm_id="default",
        instance_id="authority",
        parent_hashes=[parent] if parent else [],
        event_hashes=[event_hash],
        author_principal="user:test",
    )
    commit.hash = objects.put_json(commit.model_dump(mode="json"))
    return commit.hash


def test_10k_history_and_concurrent_reads_do_not_reopen_dag_objects(tmp_path: Path) -> None:
    objects = ObjectStore(tmp_path / "objects")
    log = EventLog(objects, tmp_path, "authority", cursor_secret="test")
    parent = None
    for index in range(10_020):
        target = index in {7, 10_019}
        card_id = "small-history" if target else f"unrelated-{index}"
        parent = _commit(
            objects,
            parent,
            CardEvent(
                id=f"event-{index}",
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id=card_id,
                author_principal="user:test",
                author_instance="authority",
                payload={"id": card_id, "title": card_id},
            ),
        )
    assert parent
    log.advance_ref("default", parent, expected_head=None)

    started = time.perf_counter()
    with patch.object(objects, "get", side_effect=AssertionError("object read")):
        page = log.entity_history_page(
            "default", "card", "small-history", limit=10
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            pages = list(
                pool.map(
                    lambda _: log.entity_history_page(
                        "default", "card", "small-history", limit=10
                    ),
                    range(4),
                )
            )
    assert time.perf_counter() - started < 1.0
    assert page["scanned_commits"] == 0
    assert page["index_result"] == "hit"
    assert [len(item["events"]) for item in pages] == [2, 2, 2, 2]


def test_index_rebuild_is_identical_and_append_preflight_is_indexed(tmp_path: Path) -> None:
    objects = ObjectStore(tmp_path / "objects")
    log = EventLog(objects, tmp_path, "authority", cursor_secret="test")
    created, first = log.append_event(
        CardEvent(
            type=EventType.CARD_CREATED,
            realm_id="default",
            card_id="card-1",
            author_principal="user:test",
            author_instance="authority",
            payload={"id": "card-1", "title": "first", "updated_at": "v1"},
        )
    )
    before = log.entity_history("default", "card", "card-1")
    cursor_page = log.entity_history_page(
        "default", "card", "card-1", limit=1
    )

    log.index.reset_realm("default")
    assert log.ensure_indexed("default", first.hash)
    assert log.entity_history("default", "card", "card-1") == before
    assert cursor_page["head"] == first.hash

    with patch.object(log, "apply_commit_chain", side_effect=AssertionError("DAG replay")):
        with pytest.raises(DuplicateCardCreateError):
            log.append_event(created.model_copy(update={"id": "duplicate"}))
        event, _commit_result = log.append_event(
            CardEvent(
                type=EventType.CARD_UPDATED,
                realm_id="default",
                card_id="card-1",
                author_principal="user:test",
                author_instance="authority",
                payload={"title": "updated", "updated_at": "v2"},
            )
        )
    assert event.causal_card_version == "v1"


def test_direct_child_index_advance_never_enumerates_indexed_ancestry(tmp_path: Path) -> None:
    objects = ObjectStore(tmp_path / "objects")
    log = EventLog(objects, tmp_path, "authority", cursor_secret="test")
    parent, old_objects = None, set()
    for index in range(80):
        event = CardEvent(type=EventType.CARD_CREATED, realm_id="default", card_id=f"old-{index}",
            author_principal="user:test", author_instance="authority",
            payload={"id": f"old-{index}", "title": "retained"})
        parent = _commit(objects, parent, event)
        old_objects.add(parent)
    log.advance_ref("default", parent, expected_head=None)
    projection = CardProjection(tmp_path / "projection.db", log)
    projection.rebuild_from_log("default")
    child = _commit(objects, parent, CardEvent(type=EventType.CARD_CREATED,
        realm_id="default", card_id="new", author_principal="user:test", author_instance="authority",
        payload={"id": "new", "title": "incremental"}))
    original_get = objects.get
    def novel_only(digest):
        assert digest == parent or digest not in old_objects, "reopened retained history"
        return original_get(digest)
    with patch.object(log.index, "ancestors", side_effect=AssertionError("enumerated retained ancestry")), patch.object(
        objects, "get", side_effect=novel_only
    ) as reads:
        result = projection.catch_up_projection("default", child)
        assert result["commits_applied"] == 1
        log.advance_ref("default", child, expected_head=parent)
    assert reads.call_count <= 8
    assert projection.get_projection_head("default") == child
    assert log.index.indexed_head("default") == child
    assert log.entity_snapshot(child, "card", "new")["title"] == "incremental"
    assert len(log.entity_history("default", "card", "old-0")) == 1


def test_imported_fork_reuses_only_reachable_verified_ancestry(tmp_path: Path) -> None:
    objects = ObjectStore(tmp_path / "objects")
    log = EventLog(objects, tmp_path, "authority", cursor_secret="test")
    def event(identity):
        return CardEvent(type=EventType.CARD_CREATED, realm_id="default", card_id=identity,
            author_principal="user:test", author_instance="authority",
            payload={"id": identity, "title": identity})
    base, shared = None, set()
    for i in range(80):
        base = _commit(objects, base, event(f"shared-{i}"))
        shared.add(base)
    log.advance_ref("default", base, expected_head=None)
    local = _commit(objects, base, event("local"))
    remote = _commit(objects, base, event("remote"))
    log.advance_ref("default", local, expected_head=base)
    original_get = objects.get
    def novel_only(digest):
        assert digest not in shared, "reopened verified shared history"
        return original_get(digest)
    with patch.object(objects, "get", side_effect=novel_only) as reads:
        ancestry = log._ancestors(remote)
        assert reads.call_count <= 2
        assert ancestry == shared | {remote}
        assert local not in ancestry
        compatible, health = log.compatible_histories(local, remote)
        assert compatible
        assert set(health["common_ancestors"]) == shared
    # A reset forces the original canonical traversal with the same answer.
    log.index.reset_realm("default")
    assert log._ancestors(remote) == ancestry


@pytest.mark.parametrize("edges", [1000, 2000])
def test_recursive_ancestry_work_is_linear_in_visited_edges(tmp_path: Path, edges: int) -> None:
    index = DagIndex(tmp_path / "index.db")
    with index._db() as conn:
        conn.executemany("INSERT INTO commit_parents VALUES(?,?,?,?)",
            [("default", f"c{i}", f"c{i-1}", 0) for i in range(1, edges + 1)])
        instructions = 0
        def progress():
            nonlocal instructions
            instructions += 100
            return 0
        conn.set_progress_handler(progress, 100)
        try:
            ancestors = index._ancestor_hashes(conn, "default", f"c{edges}")
        finally:
            conn.set_progress_handler(None, 0)
    assert ancestors == {f"c{i}" for i in range(edges + 1)}
    # Measure SQLite VM work rather than noisy shared-host wall time. A realm
    # scan per frontier node uses millions of instructions at these sizes.
    assert instructions < 100 * (edges + 1)
