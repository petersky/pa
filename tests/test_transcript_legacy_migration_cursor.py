"""A late migration cursor seeks instead of scanning already examined rows."""

import json
from contextlib import contextmanager
from datetime import UTC, datetime

from pa.domain.models import TranscriptEvent
from pa.domain.projection import CardProjection


def test_late_legacy_cursor_has_bounded_work_and_preserves_existing_events(tmp_path, monkeypatch):
    store = CardProjection(tmp_path / "fixture.db")
    stamp = "2026-10-25T00:00:00+00:00"
    with store._conn() as conn:
        conn.executemany(
            "INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)",
            (
                (f"e-{i}", f"session-{i // 1000:03d}", i % 1000 + 1, "chunk", "{}", stamp)
                for i in range(100000)
            ),
        )
    # The selected range is already canonical. Migration must retain its normal
    # hydration/validation behavior, cursor ordering and no-copy result.
    selected = [
        TranscriptEvent(
            id=f"e-{i}", session_id=f"session-{i // 1000:03d}", seq=i % 1000 + 1,
            event_type="chunk", payload={}, created_at=datetime(2026, 10, 25, tzinfo=UTC),
        )
        for i in range(90900, 91400)
    ]
    store.transcripts.append(selected)
    store.transcripts.record_operation(
        "legacy-v1", cursor=json.dumps(["session-090", 900]), state="running", examined=0, changed=0,
    )
    original = store._conn
    callbacks = []

    @contextmanager
    def measured():
        with original() as conn:
            def step():
                callbacks.append(1)
                return 0

            conn.set_progress_handler(step, 1000)
            try:
                yield conn
            finally:
                conn.set_progress_handler(None, 0)

    monkeypatch.setattr(store, "_conn", measured)
    store._migrate_legacy_transcripts(batch_size=500)
    # This is a VM-work guard, not a wall-clock deadline or application limit.
    # The old predicate scans over 90,000 earlier rows (>500,000 instructions).
    assert len(callbacks) * 1000 < 50000
    operation = store.transcripts.operation("legacy-v1")
    assert json.loads(operation["cursor"]) == ["session-091", 400]
    assert operation["state"] == "running"
    assert operation["examined"] == 500
    assert operation["changed"] == 0
    assert store.transcripts.list_range("session-090", start_seq=901, end_seq=1000) == selected[:100]
    assert store.transcripts.list_range("session-091", start_seq=1, end_seq=400) == selected[100:]
    with original() as conn:
        assert conn.execute("SELECT COUNT(*) FROM agent_transcript_events").fetchone()[0] == 100000
