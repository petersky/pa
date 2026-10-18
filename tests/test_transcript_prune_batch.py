"""Canonical retention keeps proof, cold-reference and rollback semantics."""
import sqlite3
import threading
from contextlib import contextmanager
from unittest.mock import patch

import pytest

from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


def event(session, seq, payload=None, kind='chunk'):
    return TranscriptEvent(session_id=session, seq=seq, event_type=kind,
                           payload=payload or {'text': 'hot fixture'})


def references(storage):
    with storage._conn() as conn:
        return {row['hash']: row['ref_count']
                for row in conn.execute('SELECT hash,ref_count FROM transcript_objects')}


def test_hot_prune_deletes_one_session_in_one_statement_and_keeps_latest_proofs(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    storage.append([event('expired', i, kind='turn_completed') for i in range(1, 2001)])
    original = storage._conn
    deletes = []

    @contextmanager
    def observed():
        with original() as conn:
            conn.set_trace_callback(lambda sql: deletes.append(1) if sql.lstrip().upper().startswith(
                'DELETE FROM TRANSCRIPT_EVENTS') else None)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(storage, '_conn', observed):
        assert storage.prune(['expired']) == 2000
    assert len(deletes) == 1
    with storage._conn() as conn:
        assert [row[0] for row in conn.execute(
            "SELECT seq FROM transcript_evidence WHERE session_id='expired' ORDER BY seq"
        )] == list(range(1985, 2001))


def test_grouped_cold_release_preserves_shared_live_reference_and_gc_guard(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    shared = {'text': 'shared cold fixture ' * 300}
    unique = {'text': 'expired only cold fixture ' * 300}
    storage.append([event('expired', 1, shared), event('expired', 2, shared),
                    event('expired', 3, unique), event('live', 1, shared)])
    before = references(storage)
    assert sorted(before.values()) == [1, 3]
    shared_hash = next(h for h, count in before.items() if count == 3)
    expired_hash = next(h for h, count in before.items() if count == 1)
    with storage.retain_objects():
        assert storage.prune(['expired'], keep_audit=False) == 3
        assert references(storage) == {shared_hash: 1}
        assert storage._object_path(expired_hash).is_file()
        assert storage.collect_unreferenced_objects() == 0
    assert storage.collect_unreferenced_objects() == 1
    assert storage._object_path(shared_hash).is_file()
    assert storage.list('live')[0].payload == shared


def test_batch_release_matches_repeated_positive_decrements_and_duplicate_sessions(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    payload = {'text': 'shared cold fixture ' * 300}
    storage.append([event(s, i, payload) for s in ('first', 'second', 'live') for i in (1, 2)])
    digest = next(iter(references(storage)))
    with storage.retain_objects():
        assert storage.prune(['first', 'first', 'second'], keep_audit=False) == 4
        assert references(storage) == {digest: 2}
        assert len(storage.list('live')) == 2


def test_repeated_positive_release_does_not_underflow_a_fixture_count(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    payload = {'text': 'expired cold fixture ' * 300}
    storage.append([event('expired', i, payload) for i in (1, 2, 3)])
    digest = next(iter(references(storage)))
    # A disposable inconsistent count exercises the original positive-only
    # decrement behavior; every targeted event is expired, no live row is hidden.
    with storage._conn() as conn:
        conn.execute('UPDATE transcript_objects SET ref_count=1 WHERE hash=?', (digest,))
    assert storage.prune(['expired'], keep_audit=False) == 3
    assert references(storage) == {}


def test_cold_references_are_released_by_one_grouped_update(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    payload = {'text': 'repeated cold fixture ' * 300}
    storage.append([event('expired', i, payload) for i in range(1, 101)])
    original = storage._conn
    updates = []

    @contextmanager
    def observed():
        with original() as conn:
            conn.set_trace_callback(lambda sql: updates.append(1) if
                                    'UPDATE TRANSCRIPT_OBJECTS' in sql.upper() else None)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(storage, '_conn', observed):
        assert storage.prune(['expired'], keep_audit=False) == 100
    assert len(updates) == 1
    assert references(storage) == {}


@pytest.mark.parametrize('keep_audit', [False, True])
@pytest.mark.parametrize('earlier_session', [False, True])
def test_abort_rolls_back_events_cold_counts_and_new_audit_evidence(
    tmp_path, keep_audit, earlier_session
):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    payload = {'text': 'cold transactional fixture ' * 300}
    storage.append([event('expired', i, payload, 'turn_completed') for i in (1, 2)])
    sessions = ['expired']
    if earlier_session:
        storage.append([event('earlier', 1, payload, 'turn_completed')])
        sessions.insert(0, 'earlier')
    before = references(storage)
    with storage._conn() as conn:
        conn.execute("""CREATE TRIGGER stop_prune BEFORE DELETE ON transcript_events
                        WHEN OLD.session_id='expired' AND OLD.seq=2
                        BEGIN SELECT RAISE(ABORT,'fixture interruption'); END""")
    with patch.object(storage, 'collect_unreferenced_objects') as collect:
        with pytest.raises(sqlite3.IntegrityError, match='fixture interruption'):
            storage.prune(sessions, keep_audit=keep_audit)
        collect.assert_not_called()
    assert references(storage) == before
    assert [e.seq for e in storage.list('expired')] == [1, 2]
    if earlier_session:
        assert [e.seq for e in storage.list('earlier')] == [1]
    assert all(storage._object_path(digest).is_file() for digest in before)
    with storage._conn() as conn:
        assert conn.execute('SELECT COUNT(*) FROM transcript_evidence').fetchone()[0] == 0
        conn.execute('DROP TRIGGER stop_prune')
    assert storage.prune(sessions, keep_audit=keep_audit) == 2 + int(earlier_session)
    assert references(storage) == {}
    with storage._conn() as conn:
        assert conn.execute('SELECT COUNT(*) FROM transcript_evidence').fetchone()[0] == (
            (2 + int(earlier_session)) if keep_audit else 0
        )


def test_empty_followup_still_collects_bytes_deferred_by_reader(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    storage.append([event('expired', 1, {'text': 'deferred bytes ' * 300})])
    digest = next(iter(references(storage)))
    with storage.retain_objects():
        assert storage.prune(['expired'], keep_audit=False) == 1
        assert storage._object_path(digest).is_file()
    # The backup lifetime guard promises a next normal GC pass. A repeated
    # session with zero event rows must still reach the unchanged collector.
    assert storage.prune(['expired'], keep_audit=False) == 0
    assert not storage._object_path(digest).exists()


def test_empty_session_list_keeps_original_no_gc_behavior(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    with patch.object(storage, 'collect_unreferenced_objects',
                      side_effect=AssertionError('empty input collected objects')):
        assert storage.prune([]) == 0


def test_large_empty_inventory_uses_one_indexed_discovery_and_still_collects(tmp_path):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    storage.append([event('live', 1)])
    original = storage._conn
    selects = []

    @contextmanager
    def observed():
        with original() as conn:
            conn.set_trace_callback(lambda sql: selects.append(sql) if
                                    sql.lstrip().upper().startswith('SELECT') else None)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(storage, '_conn', observed), patch.object(
        storage, 'collect_unreferenced_objects', return_value=0,
    ) as collect:
        assert storage.prune([f'expired-{i}' for i in range(6000)]) == 0
        collect.assert_called_once_with()
    assert len(selects) == 1
    assert [e.seq for e in storage.list('live')] == [1]
    with storage._conn() as conn:
        # The discovery must search the existing session/sequence index rather
        # than scan every retained event for an empty expired inventory.
        plan = ' '.join(str(tuple(row)) for row in conn.execute(
            'EXPLAIN QUERY PLAN SELECT DISTINCT session_id FROM transcript_events '
            'WHERE session_id IN (SELECT value FROM json_each(?))', ('["expired"]',),
        ))
    assert 'SEARCH transcript_events USING COVERING INDEX' in plan


@pytest.mark.parametrize('keep_audit', [False, True])
def test_publication_waits_for_discovery_and_survives_prune(tmp_path, keep_audit):
    storage = TranscriptStorage(tmp_path / 'transcripts.db')
    payload = {'text': 'shared publication fixture ' * 300}
    storage.append([event('expired', 1, payload, 'turn_completed'),
                    event('live', 1, payload)])
    original = storage._conn
    entered, finished = threading.Event(), threading.Event()
    errors = []
    discovery_locked = []
    publisher = None

    def publish():
        acquired = storage._lock.acquire(blocking=False)
        discovery_locked.append(not acquired)
        if acquired:
            storage._lock.release()
        entered.set()
        try:
            storage.append([event('expired', 2, payload, 'turn_completed')])
        except BaseException as exc:
            errors.append(exc)
        finally:
            finished.set()

    @contextmanager
    def observed():
        nonlocal publisher
        with original() as conn:
            def trace(sql):
                nonlocal publisher
                if publisher is None and 'json_each' in sql:
                    publisher = threading.Thread(target=publish)
                    publisher.start()
                    assert entered.wait(5)
                    assert not finished.is_set()
            conn.set_trace_callback(trace)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(storage, '_conn', observed):
        assert storage.prune(['empty', 'expired', 'expired'], keep_audit=keep_audit) == 1
    assert publisher is not None
    publisher.join(5)
    assert finished.is_set() and not errors
    assert discovery_locked == [True]
    assert [e.seq for e in storage.list('expired')] == [2]
    assert storage.list('live')[0].payload == payload
    assert list(references(storage).values()) == [2]
    with storage._conn() as conn:
        assert [row[0] for row in conn.execute(
            'SELECT seq FROM transcript_evidence ORDER BY seq'
        )] == ([1] if keep_audit else [])
