"""Real maintenance SQL contributes numeric phases to its enclosing worker."""
import json
import sqlite3
import threading
import time
from contextlib import closing
from unittest.mock import patch

from pa.config import Settings
from pa.core.performance import OperationObservation, current_observation
from pa.domain.models import TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance import maintenance
from pa.instance.maintenance import _maintain_database


def seed(path):
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("CREATE TABLE fixture(id INTEGER PRIMARY KEY, body TEXT)")
        conn.executemany("INSERT INTO fixture VALUES(?,?)", (
            (i, "synthetic maintenance observation " * 64) for i in range(1000)
        ))
        conn.execute("DELETE FROM fixture WHERE id<700")


def test_integrity_checkpoint_and_compaction_are_observed_without_changing_rows(tmp_path):
    path = tmp_path / "maintenance.db"
    seed(path)
    observation = OperationObservation()
    token = current_observation.set(observation)
    started = time.perf_counter()
    try:
        result = _maintain_database(path)
    finally:
        finished = time.perf_counter()
        current_observation.reset(token)

    assert result["status"] == "ok"
    assert result["compacted"] and result["quick_check"] == "ok"
    assert observation.sqlite_calls > 0
    assert observation.sqlite_statement_ms > 0
    assert observation.sqlite_rows > 0
    assert set(result['phases']) == {
        'connect', 'close', 'total', 'size_initial', 'size_pre_compact', 'size_final',
        'integrity_before', 'integrity_after', 'optimize', 'checkpoint_before',
        'checkpoint_after', 'disk_space', 'vacuum',
    }
    for phase in result['phases'].values():
        assert phase['calls'] == 1 and phase['errors'] == 0
        assert phase['wall_ms'] >= 0 and phase['thread_cpu_ms'] >= 0
    # close/total finalize after the return expression; returned diagnostics
    # must include them and remain safe to serialize in the cached status API.
    assert json.loads(json.dumps(result)) == result
    assert result['phases']['total']['wall_ms'] >= result['phases']['vacuum']['wall_ms']
    # The cached result can be aligned with this process's existing worker and
    # lock timelines, including close/total finalized after the return value.
    total = result['phases']['total']
    assert started <= total['started_at_monotonic'] <= total['finished_at_monotonic'] <= finished
    for phase in result['phases'].values():
        assert total['started_at_monotonic'] <= phase['started_at_monotonic'] <= phase['finished_at_monotonic'] <= total['finished_at_monotonic']
    with closing(sqlite3.connect(path)) as conn:
        assert conn.execute("SELECT COUNT(*),MIN(id),MAX(id) FROM fixture").fetchone() == (
            300, 700, 999
        )
        assert conn.execute("PRAGMA quick_check").fetchall() == [("ok",)]


def test_busy_deferral_retains_numeric_work_and_original_retry_behavior(tmp_path):
    path = tmp_path / "busy.db"
    seed(path)
    observation = OperationObservation()
    with closing(sqlite3.connect(path)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("INSERT INTO fixture VALUES(1001,'uncommitted fixture')")
        token = current_observation.set(observation)
        try:
            result = _maintain_database(path)
        finally:
            current_observation.reset(token)
            writer.rollback()
    assert result["status"] == "deferred" and result["reason"] == "busy"
    assert not result["compacted"]
    assert observation.sqlite_calls > 0
    assert observation.sqlite_statement_ms > 0
    assert result['phases']['close']['calls'] == result['phases']['total']['calls'] == 1
    assert 'vacuum' not in result['phases']
    # A checkpoint can return a busy row rather than raise. Do not equate the
    # exception counter with all native lock wait or require a particular path.
    assert _maintain_database(path)["status"] == "ok"


def test_stopping_does_not_open_database_and_still_reports_total(tmp_path):
    path = tmp_path / 'never-created.db'
    stop = threading.Event()
    stop.set()
    result = _maintain_database(path, stop=stop)
    assert result['status'] == 'deferred' and result['reason'] == 'stopping'
    assert set(result['phases']) == {'total'}
    assert result['phases']['total']['calls'] == 1
    assert not path.exists()


def test_sweep_keeps_distinct_health_freshness_and_finalized_timings(tmp_path):
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces')
    settings.data_dir.mkdir()
    store = CardProjection(settings.db_path)
    store.append_transcript_events([
        TranscriptEvent(session_id='active', seq=1, event_type='chunk', payload={'text': 'one'})
    ])
    original = maintenance._maintain_database

    def maintain_then_publish(path, **kwargs):
        result = original(path, **kwargs)
        if path == store.db_path:
            store.append_transcript_events([
                TranscriptEvent(session_id='active', seq=2, event_type='chunk',
                                payload={'text': 'two'})
            ])
        return result

    try:
        with patch.object(maintenance, '_maintain_database', side_effect=maintain_then_publish):
            result = maintenance.run_maintenance(settings, store)
        assert result['transcript_migration']['events'] == 1
        assert result['transcript_storage']['events'] == 2
        assert result['transcript_storage']['measured_at'] >= result['transcript_migration']['measured_at']
        assert store.transcript_storage_status() == result['transcript_storage']
        assert set(result['phases']) == {
            'total', 'transcript_retention', 'mutation_retention',
            'transcript_migration_and_health', 'projection_database',
            'transcript_database', 'transcript_health_final',
            'retention_selection', 'retention_canonical',
        }
        # This fixture has no expired sessions: selection and the empty
        # canonical call are measured, but no transaction/GC/mirror is entered.
        # Keep the exact bounded set above and both fresh-health assertions.
        assert result['phases']['transcript_retention']['wall_ms'] >= sum(
            result['phases'][name]['wall_ms']
            for name in ('retention_selection', 'retention_canonical')
        )
        assert all(p['calls'] == 1 and p['errors'] == 0 for p in result['phases'].values())
        assert result['sqlite']['phases']['close']['calls'] == 1
        assert result['transcript_sqlite']['phases']['close']['calls'] == 1
        assert json.loads(json.dumps(result)) == result
    finally:
        store.close_thread_connection()
