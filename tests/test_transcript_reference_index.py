"""Cold-reference discovery stays event-derived across schema upgrades/writes."""
import sqlite3
from contextlib import closing

from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import TranscriptStorage


QUERY = "SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL"
INDEX = "idx_transcript_events_cold_hash"


def event(session, seq, text):
    return TranscriptEvent(session_id=session, seq=seq, event_type="output", payload={"text": text})


def test_existing_database_gets_covering_reference_index_without_changing_rows(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    store.append([event("hot", 1, "small"), event("cold", 1, "large" * 1000)])
    with store._conn() as conn:
        conn.execute(f"DROP INDEX IF EXISTS {INDEX}")
        before = [tuple(row) for row in conn.execute("SELECT * FROM transcript_events ORDER BY id")]
    expected = store.referenced_hashes()

    upgraded = TranscriptStorage(tmp_path / "pa.db")
    with upgraded._conn() as conn:
        after = [tuple(row) for row in conn.execute("SELECT * FROM transcript_events ORDER BY id")]
        plan = " ".join(row[3] for row in conn.execute("EXPLAIN QUERY PLAN " + QUERY))
        indexes = [row[1] for row in conn.execute("PRAGMA index_list(transcript_events)")]
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
    assert before == after
    assert upgraded.referenced_hashes() == expected
    assert f"COVERING INDEX {INDEX}" in plan
    assert indexes.count(INDEX) == 1
    # Reopening an already upgraded database is idempotent.
    reopened = TranscriptStorage(tmp_path / "pa.db")
    assert reopened.referenced_hashes() == expected


def test_reference_index_upgrade_preserves_wal_reader_and_new_publications(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    store.append([event("closed", 1, "first" * 1000)])
    with store._conn() as conn:
        conn.execute(f"DROP INDEX IF EXISTS {INDEX}")
    old_hashes = store.referenced_hashes()
    with closing(sqlite3.connect(store.db_path)) as reader:
        reader.execute("BEGIN")
        assert {row[0] for row in reader.execute(QUERY)} == old_hashes
        upgraded = TranscriptStorage(tmp_path / "pa.db")
        upgraded.append([event("live", 1, "second" * 1000)])
        # Index creation and the new writer do not invalidate the pinned view.
        assert {row[0] for row in reader.execute(QUERY)} == old_hashes
        reader.rollback()
        assert len({row[0] for row in reader.execute(QUERY)}) == 2
    upgraded.prune(["closed"], keep_audit=False)
    assert len(upgraded.referenced_hashes()) == 1
    upgraded.append([event("live", 1, "now hot")])
    assert upgraded.referenced_hashes() == set()
    assert upgraded.list_range("live")[0].payload == {"text": "now hot"}


def test_missing_event_reference_is_visible_without_object_metadata(tmp_path):
    store = TranscriptStorage(tmp_path / "pa.db")
    store.append([event("missing", 1, "missing" * 1000)])
    digest, = store.referenced_hashes()
    # Disposable inconsistent fixture: object metadata must never become the
    # authority for which event references diagnostics inspect.
    store._object_path(digest).unlink()
    with store._conn() as conn:
        conn.execute("DELETE FROM transcript_objects")
    trace = []
    original = store._new_connection

    def observed():
        conn = original()
        conn.set_trace_callback(trace.append)
        return conn

    from unittest.mock import patch
    with patch.object(store, "_new_connection", observed):
        metrics = store.metrics()
    assert store.referenced_hashes() == {digest}
    assert metrics["missing_objects"] == 1
    assert metrics["objects"] == 0
    assert metrics["integrity"] == "ok"
    assert sum(sql.upper() == "PRAGMA QUICK_CHECK" for sql in trace) == 1
