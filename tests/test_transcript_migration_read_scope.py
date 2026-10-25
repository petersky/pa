"""Migration reuses read setup without caching canonical rows or hydration."""
import json
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime

import pytest

from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.models import TranscriptEvent
from pa.domain.projection import CardProjection


def event(seq, payload=None):
    return TranscriptEvent(id=f"e-{seq}", session_id="session", seq=seq, event_type="chunk",
                           payload={} if payload is None else payload,
                           created_at=datetime(2026, 10, 25, tzinfo=UTC))


def seed(tmp_path, count):
    store = CardProjection(tmp_path / "fixture.db")
    with store._conn() as conn:
        conn.executemany("INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)",
                         [(f"e-{i}", "session", i, "chunk", "{}", "2026-10-25T00:00:00+00:00")
                          for i in range(1, count + 1)])
    store.transcripts.record_operation("legacy-v1", cursor=json.dumps(["", -1]),
                                      state="running", examined=0, changed=0)
    return store


@pytest.mark.parametrize("worker_scope", [False, True])
def test_batch_bounds_handle_borrows_but_hydrates_every_existing_row(tmp_path, monkeypatch, worker_scope):
    store = seed(tmp_path, 500)
    expected = [event(i) for i in range(1, 501)]
    store.transcripts.append(expected)
    borrows, hydrated = [], []
    original_conn, original_payload = store.transcripts._conn, store.transcripts._payload

    @contextmanager
    def counted():
        borrows.append(1)
        with original_conn() as conn:
            yield conn

    def payload(row):
        hydrated.append(row["seq"])
        return original_payload(row)

    monkeypatch.setattr(store.transcripts, "_conn", counted)
    monkeypatch.setattr(store.transcripts, "_payload", payload)
    with sqlite_connection_scope() if worker_scope else nullcontext():
        store._migrate_legacy_transcripts(batch_size=500)
    # One bounded read borrow plus operation metadata, not 500 read borrows.
    assert len(borrows) <= 4
    assert hydrated == list(range(1, 501))
    operation = store.transcripts.operation("legacy-v1")
    assert json.loads(operation["cursor"]) == ["session", 500]
    assert operation["examined"] == 500 and operation["changed"] == 0
    assert store.transcripts.list_range("session") == expected


@pytest.mark.parametrize("worker_scope", [False, True])
def test_each_lookup_sees_canonical_publication_after_previous_hydration(tmp_path, monkeypatch, worker_scope):
    store = seed(tmp_path, 3)
    store.transcripts.append([event(1)])
    original = store.transcripts._payload
    observed = []
    late = event(2, {"text": "published after first lookup"})
    with ThreadPoolExecutor(max_workers=1) as writer:
        def payload(row):
            observed.append(row["seq"])
            if row["seq"] == 1:
                writer.submit(store.transcripts.append, [late]).result(timeout=5)
            return original(row)

        monkeypatch.setattr(store.transcripts, "_payload", payload)
        with sqlite_connection_scope() if worker_scope else nullcontext():
            store._migrate_legacy_transcripts(batch_size=3)
    assert observed == [1, 2]
    assert store.transcripts.operation("legacy-v1")["changed"] == 1
    monkeypatch.setattr(store.transcripts, "_payload", original)
    assert store.transcripts.list_range("session") == [event(1), late, event(3)]


@pytest.mark.parametrize("worker_scope", [False, True])
def test_invalid_canonical_json_preserves_failure_cursor_and_defers_pending_append(tmp_path, worker_scope):
    store = seed(tmp_path, 3)
    store.transcripts.append([event(1), event(3)])
    cursor = json.dumps(["", -1])
    store.transcripts.record_operation("legacy-v1", cursor=cursor, state="running", examined=0, changed=0)
    with store.transcripts._conn() as conn:
        conn.execute("UPDATE transcript_events SET hot_payload=? WHERE seq=3", ("{invalid",))
    with sqlite_connection_scope() if worker_scope else nullcontext():
        with pytest.raises(json.JSONDecodeError):
            store._migrate_legacy_transcripts(batch_size=3)
    operation = store.transcripts.operation("legacy-v1")
    assert operation["cursor"] == cursor and operation["state"] == "failed"
    assert operation["examined"] == operation["changed"] == 0
    with store.transcripts._conn() as conn:
        assert [r[0] for r in conn.execute("SELECT seq FROM transcript_events ORDER BY seq")] == [1, 3]


def test_borrowed_range_preserves_bounds_and_caller_transaction(tmp_path):
    store = seed(tmp_path, 3)
    store.transcripts.append([event(i) for i in range(1, 4)])
    with store.transcripts._conn() as conn:
        conn.execute("CREATE TABLE caller_transaction(value TEXT)")
        conn.execute("INSERT INTO caller_transaction VALUES('pending')")
        assert conn.in_transaction
        assert store.transcripts.list_range("session", start_seq=2, end_seq=3, limit=1,
                                            _connection=conn) == [event(2)]
        assert conn.in_transaction
        conn.rollback()
        assert conn.execute("SELECT COUNT(*) FROM caller_transaction").fetchone()[0] == 0
