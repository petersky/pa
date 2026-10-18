"""Fresh health must retain every event reference and propagate incomplete reads."""
import sqlite3
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.core.performance import OperationObservation, current_observation
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
        assert observation.sqlite_rows >= count
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
    store.append([cold(1)])
    expected = store.referenced_hashes()
    closed = []

    class BrokenCursor:
        first = True

        def __iter__(self):
            yield ("partial",)
            raise sqlite3.OperationalError("fixture read failed")

        def fetchmany(self, size):
            if self.first:
                self.first = False
                return [("partial",)]
            raise sqlite3.OperationalError("fixture read failed")

    @contextmanager
    def failing_connection():
        try:
            yield SimpleNamespace(execute=lambda *a: BrokenCursor())
        finally:
            closed.append(True)

    with patch.object(store, "_conn", failing_connection):
        with pytest.raises(sqlite3.OperationalError, match="fixture read failed"):
            store.referenced_hashes()
    assert closed == [True]
    assert store.referenced_hashes() == expected
    assert store.metrics()["missing_objects"] == 0
