"""Imported forks need indexed membership, not a copy of retained ancestry."""
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.sync.event_log import EventHistoryObjectError, EventLog
from pa.sync.object_store import ObjectStore


def history(tmp_path):
    objects = ObjectStore(tmp_path / 'objects')
    log = EventLog(objects, tmp_path, 'authority')

    def commit(identity, parents):
        event = CardEvent(type=EventType.CARD_CREATED, realm_id='default',
                          card_id=identity, author_principal='user:test',
                          author_instance='authority', payload={'id': identity, 'title': identity})
        event_hash = objects.put_json(event.model_dump(mode='json'))
        value = SyncCommit(hash='', realm_id='default', instance_id='authority',
                           parent_hashes=parents, event_hashes=[event_hash],
                           author_principal='user:test')
        return objects.put_json(value.model_dump(mode='json'))

    retained = []
    for i in range(80):
        retained.append(commit(str(i), retained[-1:]))
    log.advance_ref('default', retained[-1], expected_head=None)
    return log, objects, retained, commit


def test_imported_fork_checks_ready_membership_without_retained_hash_transfer(tmp_path):
    log, objects, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    descendant = commit('descendant', [retained[-1], fork])
    original_get = objects.get

    def novel_only(digest):
        assert digest not in retained, 'reopened verified retained history'
        return original_get(digest)

    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with patch.object(log.index, 'ancestors', side_effect=AssertionError('ancestry copied')), \
             patch.object(objects, 'get', side_effect=novel_only):
            assert not log.is_ancestor(retained[-1], fork)
            assert log.is_ancestor(retained[-1], descendant)
        assert observation.sqlite_aggregate_items == 0
        assert observation.sqlite_aggregate_bytes == 0
    finally:
        current_observation.reset(token)


def test_membership_is_realm_scoped_and_pins_ready_generation_across_reset(tmp_path):
    log, _, retained, _ = history(tmp_path)
    # A row in another realm must not be accepted as part of this ready head.
    with log.index._db() as conn:
        conn.execute('INSERT INTO commits VALUES(?,?,?,?,?,?)',
                     ('other', 'unrelated', 0, 'other', 'user:test', '2026-01-01'))
    with log.index.ready_ancestor_membership(retained[-1]) as membership:
        assert membership is not None
        assert retained[0] in membership
        assert 'unrelated' not in membership
        with ThreadPoolExecutor(max_workers=1) as pool:
            pool.submit(log.index.reset_realm, 'default').result(timeout=5)
        assert retained[0] in membership  # The already verified WAL read view.
    with log.index.ready_ancestor_membership(retained[-1]) as membership:
        assert membership is None


def test_older_or_incomplete_generation_keeps_canonical_fallback(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    with log.index.ready_ancestor_membership(retained[-2]) as membership:
        assert membership is None
    assert log.is_ancestor(retained[-2], fork)
    log.index.mark_failed('default', 'fixture incomplete generation')
    with log.index.ready_ancestor_membership(retained[-1]) as membership:
        assert membership is None
    assert not log.is_ancestor(retained[-1], fork)


def test_missing_novel_parent_still_rejected_and_read_view_released(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('missing', ['f' * 64])
    with pytest.raises(EventHistoryObjectError):
        log.is_ancestor(retained[-1], fork)
    # No leaked read transaction prevents a subsequent index reset/rebuild.
    log.index.reset_realm('default')
    assert log.ensure_indexed('default', retained[-1])


def test_ready_metadata_without_indexed_tip_is_not_ancestry_evidence(tmp_path):
    log, _, retained, commit = history(tmp_path)
    fork = commit('fork', [retained[-2]])
    with log.index._db() as conn:
        conn.execute('DELETE FROM commits WHERE realm_id=? AND commit_hash=?',
                     ('default', retained[-1]))
    with log.index.ready_ancestor_membership(retained[-1]) as membership:
        assert membership is None
    assert not log.is_ancestor(retained[-1], fork)


def test_large_suffix_uses_one_bulk_fallback_with_exact_membership(tmp_path):
    log, _, retained, _ = history(tmp_path)
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with log.index.ready_ancestor_membership(retained[-1]) as membership:
            assert membership is not None
            for i in range(200):
                assert f'novel-{i}' not in membership
            calls_after_bulk_fallback = observation.sqlite_calls
            assert all(h in membership for h in retained)
            assert observation.sqlite_calls == calls_after_bulk_fallback
        assert observation.sqlite_aggregate_items == len(retained)
        # execute and fetch are each observed: this is bounded independently of
        # the 280 distinct hashes checked, including connection setup calls.
        assert observation.sqlite_calls < 150
    finally:
        current_observation.reset(token)
