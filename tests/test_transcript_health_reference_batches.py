"""Fresh health must retain every event reference and propagate incomplete reads."""
import sqlite3
from unittest.mock import patch

import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.core.sqlite_metrics import ObservedCursor, sqlite_connection_scope
from pa.domain.models import TranscriptEvent
from pa.domain.transcript_storage import MAX_HOT_PAYLOAD_BYTES, TranscriptStorage


def cold(seq):
    return TranscriptEvent(session_id="cold", seq=seq, event_type="output",
                           payload={"text": f"{seq}:" + "x" * (MAX_HOT_PAYLOAD_BYTES + 100)})


@pytest.mark.parametrize("count", [0, 1, 129, 385])
def test_complete_distinct_references_and_health_across_batch_boundaries(tmp_path, count):
    store = TranscriptStorage(tmp_path / "fixture.db")
    events = [cold(i + 1) for i in range(count)]
    store.append(events + [TranscriptEvent(
        session_id="hot", seq=1, event_type="output", payload={"text": "small"})])
    if events:
        store.append([TranscriptEvent(session_id="duplicate", seq=1,
                                      event_type="output", payload=events[0].payload)])
    with store._conn() as conn:
        expected = {r[0] for r in conn.execute(
            "SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL"
        ).fetchall()}
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        actual = store.referenced_hashes()
    finally:
        current_observation.reset(token)
    assert actual == expected
    assert len(actual) == count
    if count >= 129:
        # Complete rows remain observable without one Python cursor call per row.
        assert observation.sqlite_aggregate_items == count
        assert observation.sqlite_aggregate_bytes >= count * 64
        assert observation.sqlite_rows < count
        assert observation.sqlite_calls < count // 2
    health = store.metrics()
    assert health["objects"] == count
    assert health["references"] == count + bool(events)
    assert health["missing_objects"] == 0
    assert health["integrity"] == "ok"


def test_fresh_health_checks_all_files_and_never_caches_missing_result(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(i + 1) for i in range(257)])
    digest = sorted(store.referenced_hashes())[-1]
    path = store._object_path(digest)
    assert path.resolve().is_relative_to(tmp_path.resolve())
    original = path.read_bytes()
    assert store.metrics()["missing_objects"] == 0
    # Only this disposable fixture loses a synthetic file; health must say so.
    path.unlink()
    try:
        assert store.metrics()["missing_objects"] == 1
        assert digest in store.referenced_hashes()
    finally:
        path.write_bytes(original)
    assert store.metrics()["missing_objects"] == 0
    assert store.list("cold", after_seq=256, limit=1)[0].payload == cold(257).payload


def test_failed_reference_read_raises_instead_of_returning_partial_health(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(i + 1) for i in range(257)])
    expected = store.referenced_hashes()
    fetchone = ObservedCursor.fetchone
    pages = []

    def fail_second_page(cursor):
        if cursor.description and cursor.description[0][0] == "json_group_array(cold_hash)":
            pages.append(True)
            if len(pages) == 2:
                raise sqlite3.OperationalError("fixture read failed")
        return fetchone(cursor)

    with patch.object(ObservedCursor, "fetchone", fail_second_page), sqlite_connection_scope():
        with pytest.raises(sqlite3.OperationalError, match="fixture read failed"):
            store.referenced_hashes()
    assert len(pages) == 2
    assert store.referenced_hashes() == expected
    assert store.metrics()["missing_objects"] == 0


def test_reference_values_preserve_native_nontext_fallback(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    values = ["", "\x00", "a\x00suffix", "雪", 'quote"', b"\x00\x80\xff", b"", 1, 1.25, None]
    store.append([TranscriptEvent(session_id="values", seq=i + 1, event_type="output", payload={})
                  for i in range(len(values))])
    with store._conn() as conn:
        for i, value in enumerate(values):
            conn.execute("UPDATE transcript_events SET cold_hash=? WHERE session_id='values' AND seq=?", (value, i + 1))
        expected = {row[0] for row in conn.execute(
            "SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL").fetchall()}
    assert store.referenced_hashes() == expected
    assert b"\x00\x80\xff" in expected and "1.25" in expected


def test_reference_pages_keep_one_snapshot_during_publication_and_deletion(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(i + 1) for i in range(257)])
    expected = store.referenced_hashes()
    fetchone = ObservedCursor.fetchone
    published = []

    def publish_after_first_page(cursor):
        row = fetchone(cursor)
        if cursor.description and cursor.description[0][0] == "json_group_array(cold_hash)" and not published:
            published.append(True)
            store.append([TranscriptEvent(session_id="new", seq=1, event_type="output",
                                          payload={"text": "new cold reference " * 300})])
            with store._conn() as writer:
                writer.execute("UPDATE transcript_events SET cold_hash=NULL WHERE cold_hash=?", (sorted(expected)[-1],))
        return row

    with sqlite_connection_scope(), patch.object(ObservedCursor, "fetchone", publish_after_first_page):
        assert store.referenced_hashes() == expected
    fresh = store.referenced_hashes()
    assert len(fresh - expected) == len(expected - fresh) == 1


@pytest.mark.parametrize("cancel", [False, True])
def test_reference_page_interrupt_or_cancellation_closes_snapshot_and_keeps_error(tmp_path, cancel):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(i + 1) for i in range(257)])
    expected = store.referenced_hashes()
    fetchone = ObservedCursor.fetchone
    triggered = []

    class Cancelled(BaseException):
        pass

    def interrupt_after_first_page(cursor):
        row = fetchone(cursor)
        if cursor.description and cursor.description[0][0] == "json_group_array(cold_hash)" and not triggered:
            triggered.append(True)
            if cancel:
                raise Cancelled()
            interrupted = []
            def once():
                if not interrupted:
                    interrupted.append(True)
                    return 1
                return 0
            cursor.connection.set_progress_handler(once, 1)
        return row

    with sqlite_connection_scope(), patch.object(ObservedCursor, "fetchone", interrupt_after_first_page):
        with pytest.raises(Cancelled if cancel else sqlite3.OperationalError, match=None if cancel else "interrupted"):
            store.referenced_hashes()
    assert triggered
    assert store.referenced_hashes() == expected


def test_malformed_utf8_reference_raises_native_error_then_recovers(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(1)])
    expected = store.referenced_hashes()
    with store._conn() as conn:
        conn.execute("UPDATE transcript_events SET cold_hash=CAST(x'80' AS TEXT)")
    with pytest.raises(sqlite3.OperationalError):
        store.referenced_hashes()
    with store._conn() as conn:
        conn.execute("UPDATE transcript_events SET cold_hash=?", (next(iter(expected)),))
    assert store.referenced_hashes() == expected


def test_reference_json_size_limit_falls_back_to_complete_native_snapshot(tmp_path):
    store = TranscriptStorage(tmp_path / "fixture.db")
    store.append([cold(i + 1) for i in range(257)])
    expected = store.referenced_hashes()
    factory = store._new_connection

    def limited_connection():
        conn = factory()
        # Every individual64-byte reference fits. A128-item JSON aggregate
        # does not; this must not introduce an error absent from native rows.
        conn.setlimit(sqlite3.SQLITE_LIMIT_LENGTH, 1024)
        return conn

    with patch.object(store, "_new_connection", limited_connection):
        assert store.referenced_hashes() == expected
