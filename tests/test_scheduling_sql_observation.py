"""Scheduler SQL contributes bounded numeric diagnostics, never payloads."""
from dataclasses import asdict

from pa.core.performance import OperationObservation, current_observation
from pa.scheduling.store import Ledger, Schedule


def test_scheduler_ledger_statement_row_and_commit_observations(tmp_path):
    ledger = Ledger(tmp_path / "scheduling.db")
    record = ledger.create("owner", "create", Schedule(name="Synthetic", payload="PRIVATE_SENTINEL",
        trigger={"kind": "manual"}, destination={"kind": "inbox", "target": "owner"}), 0)
    expected = ledger.get("owner", record["id"])
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        assert ledger.get("owner", record["id"]) == expected
        ledger.fire("owner", record["id"], "fire", 1)
        assert len(ledger.history("owner", record["id"])) == 1
        assert len(ledger.pending(2)) == 1
    finally:
        current_observation.reset(token)
    assert observation.sqlite_calls > 0
    assert observation.sqlite_rows > 0
    assert observation.sqlite_statement_ms > 0
    assert observation.sqlite_commit_ms > 0
    assert observation.sqlite_statement_thread_cpu_ms > 0
    assert observation.sqlite_commit_thread_cpu_ms > 0
    assert observation.sqlite_busy_failures == 0
    assert "PRIVATE_SENTINEL" not in str(asdict(observation))
    assert ledger.get("owner", record["id"])["occurrence_count"] == 1


def test_scheduler_lazy_fetches_keep_rows_and_record_only_counts(tmp_path):
    ledger = Ledger(tmp_path / "scheduling.db")
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with ledger.connect(read_only=True) as db:
            cursor = db.execute("SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3")
            assert dict(cursor.fetchone()) == {"n": 1}
            assert [dict(row) for row in cursor.fetchmany(1)] == [{"n": 2}]
            assert [dict(row) for row in cursor] == [{"n": 3}]
    finally:
        current_observation.reset(token)
    assert observation.sqlite_rows == 3
    assert observation.sqlite_calls >= 6
    assert all(isinstance(value, (int, float, list)) for value in asdict(observation).values())
