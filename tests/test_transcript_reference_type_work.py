"""Fresh type validation must not scan the retained event population."""
from contextlib import contextmanager
import sqlite3
from unittest.mock import patch

import pytest

from pa.domain.transcript_storage import TranscriptStorage


@pytest.mark.parametrize("count", [100, 20_000])
def test_first_snapshot_read_has_bounded_work_with_duplicate_text_references(tmp_path, count):
    store = TranscriptStorage(tmp_path / "fixture.db")
    digest = "a" * 64
    with store._conn() as conn:
        conn.executemany(
            "INSERT INTO transcript_events(id,session_id,seq,event_type,payload_hash,cold_hash,created_at) "
            "VALUES(?,'fixture',?,'output','payload',?,'2026-10-25')",
            ((str(i), i, digest) for i in range(count)),
        )
    original = store._conn
    progress = [0]
    selects = [0]

    @contextmanager
    def observed():
        with original() as conn:
            def trace(sql):
                if sql.lstrip().upper().startswith("SELECT"):
                    selects[0] += 1

            def step():
                if selects[0] == 1:
                    progress[0] += 1
                return 0

            conn.set_trace_callback(trace)
            conn.set_progress_handler(step, 100)
            try:
                yield conn
            finally:
                conn.set_progress_handler(None, 0)
                conn.set_trace_callback(None)

    with patch.object(store, "_conn", observed):
        assert store.referenced_hashes() == {digest}
    assert selects[0] >= 2  # Type check and complete reference paging both ran.
    assert progress[0] < 10, "Type validation scanned retained rows before paging"
    with store._conn() as conn:
        assert conn.execute("SELECT COUNT(*) FROM transcript_events").fetchone()[0] == count


@pytest.mark.parametrize("values", [[], [None], [1, 1.25], [1, "text"],
                                    ["text", b""], [1, b"blob"],
                                    ["", "\x00", "雪", b"\xff"]])
def test_type_extremes_preserve_native_values_without_column_affinity(tmp_path, values):
    # Malformed catalogs can contain any storage class. This private fixture
    # deliberately has no affinity so both numeric and blob endpoints exercise
    # the real read method, rather than testing a duplicate SQL expression.
    store = TranscriptStorage(tmp_path / "fixture.db")
    with sqlite3.connect(":memory:") as conn:
        conn.execute("CREATE TABLE transcript_events(cold_hash)")
        conn.execute("CREATE INDEX refs ON transcript_events(cold_hash) WHERE cold_hash IS NOT NULL")
        conn.executemany("INSERT INTO transcript_events VALUES(?)", [(v,) for v in values])
        conn.commit()
        expected = {r[0] for r in conn.execute(
            "SELECT DISTINCT cold_hash FROM transcript_events WHERE cold_hash IS NOT NULL")}

        @contextmanager
        def fixture_connection():
            yield conn

        with patch.object(store, "_conn", fixture_connection):
            assert store.referenced_hashes() == expected
        assert not conn.in_transaction
