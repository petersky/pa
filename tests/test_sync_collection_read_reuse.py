"""Collection exports the same complete bytes it validates, once per object."""
import base64
from collections import Counter

import pytest

from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.event_log import EventHistoryObjectError
from tests.test_realm_convergence import _Node


def test_legacy_and_inventory_read_each_shared_dag_object_once(tmp_path, monkeypatch):
    node = _Node(tmp_path, "collector", "Collector")
    events = [node.objects.put_json(CardEvent(type=EventType.CARD_CREATED,
        realm_id="default", author_instance="collector", author_principal="user:local",
        payload={"fixture": n}).model_dump(mode="json")) for n in range(3)]

    def commit(parents, event_hashes):
        return node.objects.put_json(SyncCommit(hash="", realm_id="default",
            instance_id="collector", parent_hashes=parents, event_hashes=event_hashes).model_dump(mode="json"))

    first = commit([], [events[0]])
    left, right = commit([first], [events[1]]), commit([first], [events[2]])
    merge = commit([left, right], [events[0]])
    expected = {h: node.objects.get(h) for h in [first, left, right, merge, *events]}
    reads = Counter()
    get = node.objects.get

    def counted(h):
        reads[h] += 1
        return get(h)

    monkeypatch.setattr(node.objects, "get", counted)
    exported = node.engine._collect_objects(merge)
    assert {h: base64.b64decode(data) for h, data in exported.items()} == expected
    assert reads == Counter({h: 1 for h in expected})
    reads.clear()
    pending, seen, pending_events = [merge], set(), []
    raw, parents = node.engine._inventory_page(pending, seen, pending_events)
    assert raw == expected and parents[merge] == [left, right]
    assert not pending and not pending_events
    assert reads == Counter({h: 1 for h in expected})


@pytest.mark.parametrize("raw,code", [
    (b"not-json", "corrupt_object"), (b"\xff", "corrupt_object"),
    (b"[]", "corrupt_object"), (b"{}", "corrupt_object"),
    (b'{"schema_version":2}', "unsupported_object_version"),
    (b'{"schema_version":true}', "unsupported_object_version"),
])
def test_reused_commit_bytes_keep_exact_original_integrity_guards(tmp_path, raw, code):
    node = _Node(tmp_path, "collector", "Collector")
    head = node.objects.put(raw)
    for call in (lambda: node.log.get_commit(head),
                 lambda: node.log.decode_commit(head, raw),
                 lambda: node.engine._collect_objects(head),
                 lambda: node.engine._inventory_page([head], set(), [])):
        with pytest.raises(EventHistoryObjectError) as caught:
            call()
        assert caught.value.code == code


def test_hash_mismatch_is_rejected_on_exact_exported_bytes(tmp_path, monkeypatch):
    node = _Node(tmp_path, "collector", "Collector")
    head = node.objects.put_json(SyncCommit(hash="", realm_id="default", instance_id="collector").model_dump(mode="json"))
    monkeypatch.setattr(node.objects, "get", lambda _: b"changed bytes")
    for call in (lambda: node.log.get_commit(head),
                 lambda: node.engine._collect_objects(head),
                 lambda: node.engine._inventory_page([head], set(), [])):
        with pytest.raises(EventHistoryObjectError) as caught:
            call()
        assert caught.value.code == "corrupt_object"


@pytest.mark.parametrize("limit", [1, 4, 512, 10000])
def test_small_initial_probe_preserves_every_spilled_event_and_parent(tmp_path, limit):
    node = _Node(tmp_path, "collector", "Collector")
    events = [node.objects.put_json(CardEvent(type=EventType.CARD_CREATED,
        realm_id="default", author_instance="collector", author_principal="user:local",
        payload={"fixture": n}).model_dump(mode="json")) for n in range(20)]
    base = node.objects.put_json(SyncCommit(hash="", realm_id="default", instance_id="collector").model_dump(mode="json"))
    head = node.objects.put_json(SyncCommit(hash="", realm_id="default", instance_id="collector",
        parent_hashes=[base], event_hashes=events).model_dump(mode="json"))
    pending, seen, pending_events, collected = [head], set(), [], set()
    while pending or pending_events:
        raw, _ = node.engine._inventory_page(pending, seen, pending_events, max_objects=limit)
        assert len(raw) <= min(limit, 512)
        collected.update(raw)
    assert collected == {head, base, *events}
