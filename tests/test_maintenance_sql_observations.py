"""Real maintenance SQL contributes numeric phases to its enclosing worker."""
import sqlite3
from contextlib import closing

from pa.core.performance import OperationObservation, current_observation
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
    try:
        result = _maintain_database(path)
    finally:
        current_observation.reset(token)

    assert result["status"] == "ok"
    assert result["compacted"] and result["quick_check"] == "ok"
    assert observation.sqlite_calls > 0
    assert observation.sqlite_statement_ms > 0
    assert observation.sqlite_rows > 0
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
    # A checkpoint can return a busy row rather than raise. Do not equate the
    # exception counter with all native lock wait or require a particular path.
    assert _maintain_database(path)["status"] == "ok"
