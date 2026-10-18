"""Keep transcript retention exact while bounding compatibility-mirror SQL."""
import json
import sqlite3
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection


def expired_session(store, identity, cutoff):
    store.save_session(AgentSession(id=identity, agent_name='codex', status='closed'))
    with store._conn() as conn:
        conn.execute('UPDATE agent_sessions SET updated_at=? WHERE id=?',
                     ((cutoff - timedelta(days=1)).isoformat(), identity))


def mirror_rows(store):
    with store._conn() as conn:
        return [tuple(row) for row in conn.execute(
            'SELECT session_id,seq,payload FROM agent_transcript_events ORDER BY session_id,seq')]


def test_large_expired_mirror_uses_one_delete_and_keeps_canonical_audit(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC) - timedelta(days=7)
    expired_session(store, 'expired', cutoff)
    store.save_session(AgentSession(id='live', agent_name='codex'))
    store.save_session(AgentSession(id='young', agent_name='codex', status='closed'))
    events = [TranscriptEvent(session_id='expired', seq=i + 1,
                              event_type='turn_completed' if i % 100 == 0 else 'chunk',
                              payload={'text': 'retained fixture bytes'}) for i in range(2000)]
    events += [TranscriptEvent(session_id=s, seq=1, event_type='chunk', payload={'text': s})
               for s in ('live', 'young')]
    store.append_transcript_events(events)
    original = store._conn
    deletes = []

    @contextmanager
    def observed():
        with original() as conn:
            conn.set_trace_callback(lambda sql: deletes.append(1) if sql.lstrip().upper().startswith(
                'DELETE FROM AGENT_TRANSCRIPT_EVENTS') else None)
            try:
                yield conn
            finally:
                conn.set_trace_callback(None)

    with patch.object(store, '_conn', observed):
        assert store.prune_closed_session_transcripts(before=cutoff) == 2000
    assert len(deletes) == 1
    assert {(s, seq) for s, seq, _ in mirror_rows(store)} == {('live', 1), ('young', 1)}
    assert store.list_transcript_events('expired') == []
    assert store.transcript_storage_metrics()['audit_evidence'] == 16
    assert store.prune_closed_session_transcripts(before=cutoff) == 0


def test_exact_kept_sequences_survive_including_more_than_parameter_limit(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC)
    expired_session(store, 'expired', cutoff)
    with store._conn() as conn:
        conn.executemany('INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)',
                         [(f'e-{i}', 'expired', i, 'chunk', '{}', cutoff.isoformat())
                          for i in range(1300)])
    kept = set(range(1100))
    with patch.object(store.transcripts, 'prune', return_value=200), patch.object(
        store.transcripts, 'list_range',
        return_value=[SimpleNamespace(seq=i) for i in kept],
    ):
        assert store.prune_closed_session_transcripts(before=cutoff) == 200
    assert {seq for _, seq, _ in mirror_rows(store)} == kept


def test_boundary_and_live_sessions_are_not_pruned(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC)
    for identity, status, when in [('boundary', 'closed', cutoff),
                                    ('live', 'idle', cutoff - timedelta(days=30))]:
        store.save_session(AgentSession(id=identity, agent_name='codex', status=status))
        store.append_transcript_events([TranscriptEvent(
            session_id=identity, seq=1, event_type='user_message', payload={'text': identity})])
        with store._conn() as conn:
            conn.execute('UPDATE agent_sessions SET updated_at=? WHERE id=?',
                         (when.isoformat(), identity))
    before = mirror_rows(store)
    assert store.prune_closed_session_transcripts(before=cutoff) == 0
    assert mirror_rows(store) == before
    assert len(store.list_transcript_events('live')) == 1
    assert len(store.list_transcript_events('boundary')) == 1


def test_mirror_failure_rolls_back_and_retains_canonical_audit_for_retry(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC)
    expired_session(store, 'expired', cutoff)
    store.append_transcript_events([TranscriptEvent(
        session_id='expired', seq=i, event_type='turn_completed', payload={'result': 'ok'})
        for i in (1, 2)])
    before = mirror_rows(store)
    with store._conn() as conn:
        conn.execute("""CREATE TRIGGER fail_mirror BEFORE DELETE ON agent_transcript_events
                        WHEN OLD.seq=2 BEGIN SELECT RAISE(ABORT,'fixture interruption'); END""")
    with pytest.raises(sqlite3.IntegrityError, match='fixture interruption'):
        store.prune_closed_session_transcripts(before=cutoff)
    assert mirror_rows(store) == before
    assert store.list_transcript_events('expired') == []
    assert store.transcript_storage_metrics()['audit_evidence'] == 2
    with store._conn() as conn:
        conn.execute('DROP TRIGGER fail_mirror')
    assert store.prune_closed_session_transcripts(before=cutoff) == 0
    assert mirror_rows(store) == []


def test_new_canonical_event_between_prune_and_mirror_cleanup_is_kept(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC)
    expired_session(store, 'expired', cutoff)
    store.append_transcript_events([TranscriptEvent(
        session_id='expired', seq=1, event_type='chunk', payload={'text': 'old'})])
    original = store.transcripts.prune

    def prune_then_append(*args, **kwargs):
        deleted = original(*args, **kwargs)
        store.append_transcript_events([TranscriptEvent(
            session_id='expired', seq=2, event_type='user_message', payload={'text': 'new'})])
        return deleted

    with patch.object(store.transcripts, 'prune', side_effect=prune_then_append):
        assert store.prune_closed_session_transcripts(before=cutoff) == 1
    assert [event.seq for event in store.list_transcript_events('expired')] == [2]
    rows = mirror_rows(store)
    assert len(rows) == 1 and rows[0][:2] == ('expired', 2)
    assert json.loads(rows[0][2])  # Existing bounded compatibility payload is retained.


def test_empty_compatibility_mirror_never_starts_a_delete(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    cutoff = datetime.now(UTC)
    expired_session(store, 'already-pruned', cutoff)
    original = store._conn

    @contextmanager
    def read_only_mirror():
        with original() as conn:
            conn.set_authorizer(lambda action, table, *args:
                                sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_DELETE
                                and table == 'agent_transcript_events' else sqlite3.SQLITE_OK)
            try:
                yield conn
            finally:
                conn.set_authorizer(None)

    # This is a disposable test authorizer, not a production permission change.
    # A repeated sweep with no mirror rows must not request SQLite's writer lock.
    with patch.object(store, '_conn', read_only_mirror):
        assert store.prune_closed_session_transcripts(before=cutoff) == 0
