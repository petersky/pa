"""Canonical transcript index migration, uniqueness and custom-schema contracts."""
import sqlite3, threading
from pathlib import Path
import pytest
from pa.domain.transcript_storage import _ensure_session_sequence_index as guard, TranscriptStorage
NAME='idx_transcript_events_session_seq'
def names(conn):return {row[1] for row in conn.execute('PRAGMA index_list(transcript_events)')}
def canonical(conn, unique='UNIQUE(session_id,seq)'):
    conn.execute('CREATE TABLE transcript_events (id TEXT PRIMARY KEY,session_id TEXT,seq INTEGER,event_type TEXT,'+unique+')')
def old_index(conn,suffix=''):
    conn.execute('CREATE INDEX '+NAME+' ON transcript_events(session_id,seq'+suffix+')')
def test_unique_and_query_order_survive(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn);old_index(conn)
        conn.executemany('INSERT INTO transcript_events VALUES(?,?,?,?)',[(str(i),'session',i,'chunk')for i in range(50)])
        before=list(conn.execute('SELECT * FROM transcript_events ORDER BY seq DESC'))
        assert guard(conn) and NAME not in names(conn)
        assert list(conn.execute('SELECT * FROM transcript_events ORDER BY seq DESC'))==before
        with pytest.raises(sqlite3.IntegrityError):conn.execute("INSERT INTO transcript_events VALUES('duplicate','session',1,'chunk')")
        assert conn.execute('PRAGMA integrity_check').fetchall()==[('ok',)]
@pytest.mark.parametrize('unique', ['UNIQUE(session_id,seq,event_type)','UNIQUE(session_id COLLATE NOCASE,seq)','UNIQUE(session_id,seq DESC)'])
def test_distinct_constraints_keep_fallback(tmp_path,unique):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn,unique);old_index(conn)
        assert not guard(conn) and NAME in names(conn)
@pytest.mark.parametrize('expression',['session_id,seq DESC','session_id COLLATE NOCASE,seq','session_id,seq+1'])
def test_custom_named_index_preserved(tmp_path,expression):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn);conn.execute('CREATE INDEX '+NAME+' ON transcript_events('+expression+')')
        before=conn.execute('SELECT sql FROM sqlite_master WHERE name=?',(NAME,)).fetchone()
        assert not guard(conn)
        assert conn.execute('SELECT sql FROM sqlite_master WHERE name=?',(NAME,)).fetchone()==before
def test_partial_index_preserved(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn);conn.execute('CREATE INDEX '+NAME+' ON transcript_events(session_id,seq) WHERE seq>0')
        assert not guard(conn) and NAME in names(conn)
def test_user_unique_index_is_not_constraint_proof(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        conn.execute('CREATE TABLE transcript_events(session_id TEXT,seq INTEGER)')
        conn.execute('CREATE UNIQUE INDEX custom_unique ON transcript_events(session_id,seq)');old_index(conn)
        assert not guard(conn) and NAME in names(conn)
def test_fallback_without_unique_constraint(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        conn.execute('CREATE TABLE transcript_events(session_id TEXT,seq INTEGER)')
        assert not guard(conn) and NAME in names(conn)
def test_repeated_optimized_open_does_not_begin_or_write(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn);trace=[];conn.set_trace_callback(trace.append)
        assert not guard(conn) and not guard(conn)
        assert all(query.startswith('PRAGMA ')for query in trace)
        assert not conn.in_transaction
def test_caller_transaction_rollback_restores_index_and_rows(tmp_path):
    with sqlite3.connect(tmp_path/'db') as conn:
        canonical(conn);old_index(conn);conn.commit()
        conn.execute('BEGIN');conn.execute("INSERT INTO transcript_events VALUES('x','s',1,'chunk')")
        assert guard(conn) and conn.in_transaction
        conn.rollback()
        assert NAME in names(conn) and conn.execute('SELECT COUNT(*) FROM transcript_events').fetchone()[0]==0
def test_concurrent_initializers_revalidate_under_writer_claim(tmp_path):
    path=tmp_path/'db'
    with sqlite3.connect(path) as conn:canonical(conn);old_index(conn)
    barrier=threading.Barrier(2);errors=[];results=[]
    def run():
        try:
            with sqlite3.connect(path,timeout=5) as conn:
                barrier.wait(5);results.append(guard(conn))
        except BaseException as error:errors.append(error)
    threads=[threading.Thread(target=run)for _ in range(2)]
    for thread in threads:thread.start()
    for thread in threads:thread.join(10);assert not thread.is_alive()
    assert not errors and len(results)==2 and sum(results)==1
    with sqlite3.connect(path) as conn:
        assert NAME not in names(conn) and conn.execute('PRAGMA integrity_check').fetchall()==[('ok',)]


def test_new_storage_uses_native_constraint_without_duplicate(tmp_path):
    storage = TranscriptStorage(tmp_path / 'metadata.db')
    with storage._conn() as conn:
        assert NAME not in names(conn)
        indexes = list(conn.execute('PRAGMA index_list(transcript_events)'))
        assert any(row[2] and row[3] == 'u' for row in indexes)
        assert conn.execute('PRAGMA user_version').fetchone()[0] == 1


def test_reopening_legacy_storage_preserves_every_event_and_object(tmp_path):
    from pa.domain.models import TranscriptEvent
    storage = TranscriptStorage(tmp_path / 'metadata.db')
    events = [TranscriptEvent(session_id='s', seq=i, event_type='chunk',
                              payload={'text': 'cold fixture ' * 500}) for i in range(1, 5)]
    storage.append(events)
    with storage._conn() as conn:
        old_index(conn)
        before = [tuple(row) for row in conn.execute('SELECT * FROM transcript_events ORDER BY seq')]
    objects = {path: path.read_bytes() for path in storage.objects_dir.rglob('*') if path.is_file()}
    reopened = TranscriptStorage(tmp_path / 'metadata.db')
    assert reopened.list('s') == storage.list('s')
    with reopened._conn() as conn:
        assert NAME not in names(conn)
        assert [tuple(row) for row in conn.execute('SELECT * FROM transcript_events ORDER BY seq')] == before
        assert conn.execute('PRAGMA integrity_check').fetchone()[0] == 'ok'
    assert {path: path.read_bytes() for path in reopened.objects_dir.rglob('*') if path.is_file()} == objects


def test_one_shot_interruption_rolls_back_owned_migration(tmp_path):
    with sqlite3.connect(tmp_path / 'db') as conn:
        canonical(conn)
        old_index(conn)
        calls = 0
        def interrupt_once():
            nonlocal calls
            calls += 1
            return int(calls == 1)
        def interrupt_drop(sql):
            if sql.startswith('DROP INDEX'):
                conn.set_progress_handler(interrupt_once, 1)
        conn.set_trace_callback(interrupt_drop)
        with pytest.raises(sqlite3.OperationalError, match='interrupted'):
            guard(conn)
        conn.set_progress_handler(None, 0)
        conn.set_trace_callback(None)
        assert NAME in names(conn)
        assert conn.execute('PRAGMA integrity_check').fetchall() == [('ok',)]


@pytest.mark.parametrize('scoped', [False, True])
def test_persistent_interrupt_disposes_failed_handle_and_preserves_durable_state(
    tmp_path, monkeypatch, scoped
):
    from contextlib import nullcontext
    from pa.core.sqlite_metrics import sqlite_connection_scope
    from pa.domain.models import TranscriptEvent
    storage = TranscriptStorage(tmp_path / 'metadata.db')
    storage.append([TranscriptEvent(session_id='s', seq=1, event_type='turn_completed',
                                    payload={'text': 'cold interrupted fixture ' * 500})])
    with storage._conn() as conn:
        old_index(conn)
        before = [tuple(row) for row in conn.execute('SELECT * FROM transcript_events')]
    objects = {path: path.read_bytes() for path in storage.objects_dir.rglob('*') if path.is_file()}
    handles = []
    def arm_fixture_interrupt(conn):
        handles.append(conn)
        def interrupt_drop(sql):
            if sql.startswith('DROP INDEX'):
                conn.set_progress_handler(lambda: 1, 1)
        conn.set_trace_callback(interrupt_drop)
    monkeypatch.setattr(TranscriptStorage, '_set_connection_progress', staticmethod(arm_fixture_interrupt))
    # Persistent cancellation may interrupt ROLLBACK itself. The existing
    # connection owner must dispose the failed handle; it must never be reused.
    with sqlite_connection_scope() if scoped else nullcontext():
        with pytest.raises(sqlite3.OperationalError, match='interrupted'):
            TranscriptStorage(tmp_path / 'metadata.db')
    assert handles
    for handle in handles:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            handle.execute('SELECT 1')
    with sqlite3.connect(storage.db_path) as fresh:
        assert NAME in names(fresh)
        assert fresh.execute('SELECT * FROM transcript_events').fetchall() == before
        assert fresh.execute('PRAGMA integrity_check').fetchall() == [('ok',)]
        with pytest.raises(sqlite3.IntegrityError):
            row = before[0]
            fresh.execute('INSERT INTO transcript_events VALUES(?,?,?,?,?,?,?,?,?)',
                          ('duplicate', *row[1:]))
    assert {path: path.read_bytes() for path in storage.objects_dir.rglob('*') if path.is_file()} == objects
