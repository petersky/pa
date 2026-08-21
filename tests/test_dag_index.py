from __future__ import annotations

import concurrent.futures
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.event_log import DuplicateCardCreateError, EventLog
from pa.sync.object_store import ObjectStore


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
