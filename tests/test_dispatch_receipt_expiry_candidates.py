"""Receipt discovery is fresh, writer-fenced and preserves expiry eligibility."""
import sqlite3
from datetime import UTC, datetime, timedelta

import pytest

from pa.execution.dispatch import DispatchStore
from tests.test_dispatch_compaction_changes import completed


@pytest.mark.parametrize("age", [None, 0, 30])
def test_no_expirable_receipts_issue_no_delete(tmp_path, age):
    store = DispatchStore(tmp_path)
    record = completed(store)
    now = datetime.now(UTC)
    if age is not None:
        with store._transaction() as conn:
            store._persist_control_receipt_conn(
                conn, record, idempotency_key="boundary", operation="fixture", payload={},
            )
            conn.execute("UPDATE dispatch_receipts SET created_at=?", (
                (now - timedelta(days=age)).isoformat(),
            ))
    before = [tuple(row) for row in store._conn.execute("SELECT * FROM dispatch_receipts")]
    statements = []
    store._conn.set_trace_callback(statements.append)
    try:
        assert store.compact(now=now) == {"events": 0, "receipts": 0}
    finally:
        store._conn.set_trace_callback(None)
    assert not any(sql.startswith("DELETE FROM dispatch_receipts") for sql in statements)
    assert [tuple(row) for row in store._conn.execute("SELECT * FROM dispatch_receipts")] == before
    store.close()


def test_expired_receipts_keep_retained_payload_and_active_dispatch_evidence(tmp_path):
    store = DispatchStore(tmp_path)
    main = completed(store, progress=True)
    literal = main.model_copy(deep=True, update={
        "dispatch_id": 'quoted-"雪', "mutation_id": "literal-mutation",
        "idempotency_key": "literal-admit", "progress_events": [], "progress_seen_keys": [],
    })
    active = literal.model_copy(deep=True, update={
        "dispatch_id": "active", "mutation_id": "active-mutation", "state": "running",
    })
    store.put(literal)
    store.put(active)
    with store._transaction() as conn:
        for record in [main, literal, active]:
            store._persist_control_receipt_conn(
                conn, record, idempotency_key="old-control", operation="fixture", payload={},
            )
        conn.execute("UPDATE dispatch_receipts SET created_at=?", (
            (datetime.now(UTC) - timedelta(days=60)).isoformat(),
        ))
    assert store.compact() == {"events": 0, "receipts": 2}
    assert [tuple(row) for row in store._conn.execute(
        "SELECT dispatch_id,idempotency_key FROM dispatch_receipts ORDER BY dispatch_id"
    )] == [("active", "old-control"), ("dispatch", "progress")]
    assert len(store.get("dispatch").progress_events) == 1
    store.close()
    reopened = DispatchStore(tmp_path)
    assert len(reopened.get("dispatch").progress_events) == 1
    reopened.close()


def test_negative_discovery_holds_the_sqlite_writer_fence(tmp_path):
    store = DispatchStore(tmp_path)
    completed(store)
    other = sqlite3.connect(store.db_path, timeout=0)
    observations = []

    def trace(sql):
        if sql.startswith("SELECT DISTINCT dispatch_id FROM dispatch_receipts"):
            observations.append(store._conn.in_transaction)
            try:
                other.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                observations.append(getattr(exc, "sqlite_errorcode", None))
            else:
                observations.append("unexpected_other_writer")
                other.rollback()

    store._conn.set_trace_callback(trace)
    try:
        assert store.compact() == {"events": 0, "receipts": 0}
    finally:
        store._conn.set_trace_callback(None)
        other.close()
        store.close()
    assert observations == [True, sqlite3.SQLITE_BUSY]
