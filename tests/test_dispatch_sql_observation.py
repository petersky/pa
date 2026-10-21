"""Dispatch SQLite observations preserve durable and passive connection semantics."""
import sqlite3
from contextlib import contextmanager
from dataclasses import asdict

import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.core.sqlite_metrics import ObservedConnection
from pa.execution.dispatch import DispatchRecord, DispatchStore, DispatchStoreReadOnlyError
from pa.execution.progress import DispatchProgressHeartbeatV1, ProgressPhase


def heartbeat(key, sequence, receipt):
    return DispatchProgressHeartbeatV1(
        dispatch_id=key, card_id="card", acp_session_id="session",
        originating_instance_id="origin", authority_instance_id="authority",
        sequence=sequence, idempotency_key=receipt,
        phase=ProgressPhase.INVESTIGATING, summary="fixture",
    )


def setup_store(path):
    store = DispatchStore(path)
    record = DispatchRecord(
        mutation_id="fixture", card_id="card", session_id="session",
        authority_instance_id="authority", authority_url="https://fixture.invalid",
        target_instance_id="origin",
    )
    store.put(record)
    store.ingest_heartbeat(heartbeat(record.dispatch_id, 10, "prior"))
    return store, record.dispatch_id


@contextmanager
def observed():
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        yield observation
    finally:
        current_observation.reset(token)


def test_dispatch_writer_observes_lazy_rows_and_commit_without_payloads(tmp_path):
    store, key = setup_store(tmp_path)
    try:
        assert isinstance(store._conn, ObservedConnection)
        assert store._conn.row_factory is sqlite3.Row
        assert store._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 30000
        assert store._conn.execute("PRAGMA synchronous").fetchone()[0] == 1
        assert store._conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
        incoming = heartbeat(key, 11, "private-fixture-receipt")
        with observed() as observation:
            result = store.ingest_heartbeat(incoming)
        assert result.status == "accepted"
        assert observation.sqlite_calls > 0
        assert observation.sqlite_statement_ms > 0
        assert observation.sqlite_commit_ms > 0
        assert observation.sqlite_busy_failures == 0
        assert "private-fixture-receipt" not in repr(asdict(observation))
        with observed() as rows:
            cursor = store._conn.execute("SELECT 1 AS value UNION ALL SELECT 2 UNION ALL SELECT 3")
            assert cursor.fetchone()["value"] == 1
            assert [row[0] for row in cursor.fetchmany(1)] == [2]
            assert [row[0] for row in cursor] == [3]
        assert rows.sqlite_rows == 3
        assert rows.sqlite_calls == 5  # execute, two fetches, next, exhausted next
        assert rows.sqlite_commit_ms == 0
        visible = store.get(key)
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get(key) == visible
        assert store.ingest_heartbeat(incoming).status == "duplicate"
    finally:
        store.close()


@pytest.mark.parametrize("boundary,committed", [("commit_before", False), ("commit_after", True)])
def test_observed_dispatch_fault_keeps_original_error_and_commit_truth(tmp_path, boundary, committed):
    store, key = setup_store(tmp_path)
    error = RuntimeError("disposable boundary")
    incoming = heartbeat(key, 11, "incoming")
    def fail(name):
        if name == boundary:
            raise error
    try:
        store._fault_injector = fail
        with observed() as observation:
            with pytest.raises(RuntimeError) as raised:
                store.ingest_heartbeat(incoming)
        assert raised.value is error
        assert bool(getattr(error, "pa_transaction_committed", False)) is committed
        assert observation.sqlite_calls > 0
        assert (observation.sqlite_commit_ms > 0) is committed
        assert store._conn.in_transaction is False
        store._fault_injector = None
        visible = store.get(key)
        assert ("incoming" in visible.progress_seen_keys) is committed
        store.close()
        store = DispatchStore(tmp_path)
        assert store.get(key) == visible
        assert store.ingest_heartbeat(incoming).status == ("duplicate" if committed else "accepted")
    finally:
        store._fault_injector = None
        store.close()


def test_dispatch_busy_failure_is_observed_once_and_does_not_publish(tmp_path):
    store, key = setup_store(tmp_path)
    blocker = sqlite3.connect(store.db_path)
    try:
        before = store.get(key)
        # Only this disposable handle uses a zero wait; production remains 30s.
        store._conn.execute("PRAGMA busy_timeout=0")
        blocker.execute("BEGIN IMMEDIATE")
        with observed() as observation:
            with pytest.raises(sqlite3.OperationalError) as raised:
                store.ingest_heartbeat(heartbeat(key, 11, "blocked"))
        assert raised.value.sqlite_errorcode == sqlite3.SQLITE_BUSY
        assert observation.sqlite_busy_failures == 1
        assert observation.sqlite_commit_ms == 0
        assert store.get(key) == before
        assert store._conn.in_transaction is False
        blocker.rollback()
        store._conn.execute("PRAGMA busy_timeout=30000")
        assert store.ingest_heartbeat(heartbeat(key, 11, "retry")).accepted
    finally:
        blocker.close()
        store.close()


def test_observed_read_only_connection_preserves_durable_files(tmp_path, monkeypatch):
    store, key = setup_store(tmp_path)
    queries = []
    original = sqlite3.connect
    def traced(*args, **kwargs):
        connection = original(*args, **kwargs)
        connection.set_trace_callback(queries.append)
        return connection
    def files():
        # SQLite readers may update ephemeral WAL shared-memory reader marks.
        # The database, WAL and metadata must remain byte/mtime identical.
        return {p.name: (p.read_bytes(), p.stat().st_mtime_ns) for p in tmp_path.iterdir()
                if p.is_file() and not p.name.endswith("-shm")}
    try:
        before = files()
        monkeypatch.setattr(sqlite3, "connect", traced)
        with observed() as observation:
            reader = DispatchStore(tmp_path, read_only=True)
        try:
            assert isinstance(reader._conn, ObservedConnection)
            assert reader._conn.row_factory is sqlite3.Row
            assert reader.get(key) == store.get(key)
            assert observation.sqlite_calls > 0 and observation.sqlite_rows > 0
            assert observation.sqlite_commit_ms == 0
            assert queries and all(q.lstrip().upper().startswith("SELECT") for q in queries)
            assert reader._conn.execute("PRAGMA busy_timeout").fetchone()[0] == 5000
            with pytest.raises(DispatchStoreReadOnlyError):
                reader.ingest_heartbeat(heartbeat(key, 11, "forbidden"))
            with observed() as failed:
                with pytest.raises(sqlite3.OperationalError) as raised:
                    reader._conn.execute("DELETE FROM dispatch_meta")
            assert raised.value.sqlite_errorcode == sqlite3.SQLITE_READONLY
            assert failed.sqlite_busy_failures == 0
        finally:
            reader.close()
        assert files() == before
    finally:
        store.close()


def test_deferred_dispatch_opens_nothing_until_explicit_promotion(tmp_path):
    reader = DispatchStore(tmp_path, read_only=True, deferred_read_only=True)
    try:
        assert reader._conn is None and not reader.db_path.exists()
        with observed() as observation:
            with pytest.raises(DispatchStoreReadOnlyError):
                reader.get("fixture")
        assert observation.sqlite_calls == 0
        reader.promote_writer()
        assert isinstance(reader._conn, ObservedConnection)
        assert not reader.read_only and not reader.deferred_read_only
    finally:
        reader.close()
