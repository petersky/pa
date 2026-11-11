"""Private numeric observers retain real calls, not carried-forward snapshots."""
import importlib.util
import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.core.sqlite_metrics import ObservedConnection


def module():
    spec = importlib.util.spec_from_file_location(
        "systemic_commit_probes", Path(__file__).parents[1] / "scripts/benchmark_systemic_latency.py")
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


@pytest.mark.asyncio
async def test_attempt_aggregation_counts_only_requested_database_and_retains_crossing_stage():
    benchmark = module()
    reports = {key: {"status":"ok", "phases":{"vacuum":{
        "calls":1,"errors":0,"wall_ms":12,"thread_cpu_ms":9,
        "started_at_monotonic":1,"finished_at_monotonic":1.012}}}
        for key in ("sqlite", "transcript_sqlite")}
    service = SimpleNamespace(_pending_compactions={"transcript_sqlite"})
    async def execute(**kwargs):
        observer.stage = "postmeasurement"
        return reports
    service._execute = execute
    observer = benchmark.MaintenanceAttempts(service)
    original = service._execute
    assert await observer.execute(database_only=False) is reports
    assert await observer.execute(database_only=True) is reports
    snapshot = observer.snapshot()
    assert snapshot["calls"] == {"steady:full":1,"postmeasurement:database_only":1}
    assert snapshot["databases"]["sqlite"]["attempts"] == 1
    transcript = snapshot["databases"]["transcript_sqlite"]
    assert transcript["attempts"] == 2 and transcript["phases"]["vacuum"]["calls"] == 2
    assert transcript["phases"]["vacuum"]["wall_ms"] == 24
    assert transcript["outcomes"]["steady:postmeasurement:ok:none"] == 1
    assert service._execute is original


@pytest.mark.asyncio
async def test_attempt_failure_remains_visible_without_exception_body():
    benchmark = module()
    async def execute(**kwargs):
        raise RuntimeError("never export this payload")
    observer = benchmark.MaintenanceAttempts(SimpleNamespace(_execute=execute,_pending_compactions=set()))
    with pytest.raises(RuntimeError):
        await observer.execute(database_only=True)
    assert observer.snapshot()["calls"] == {"steady:database_only":1,"steady:error:RuntimeError":1}
    assert "payload" not in str(observer.snapshot())


def test_native_commit_probe_retains_durable_success_error_and_original_pragmas(tmp_path):
    benchmark = module()
    path = tmp_path / "native.db"
    probe = benchmark.CommitProbe({"sqlite":path})
    connection = sqlite3.connect(path, factory=ObservedConnection)
    try:
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("CREATE TABLE parent(id PRIMARY KEY)")
        connection.execute("CREATE TABLE child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)")
        original = {key:connection.execute("PRAGMA "+key).fetchone() for key in ("synchronous","wal_autocheckpoint")}
        probe.commit(connection)  # No transaction: do not inflate call counts.
        assert not probe.snapshot()["operations"]
        connection.execute("INSERT INTO child VALUES (7)")
        with pytest.raises(sqlite3.IntegrityError):
            probe.call("fixed.owner", probe.commit, connection)
        connection.rollback()
        connection.execute("INSERT INTO parent VALUES (7)")
        connection.execute("INSERT INTO child VALUES (7)")
        probe.call("fixed.owner", probe.commit, connection)
        assert connection.execute("SELECT id FROM child").fetchall() == [(7,)]
        for key, value in original.items():
            assert connection.execute("PRAGMA "+key).fetchone() == value
        snapshot = probe.snapshot()["operations"]["fixed.owner"]
        assert snapshot["calls"] == 2 and snapshot["errors"] == 1
        assert snapshot["max_call_finish"] >= snapshot["max_call_start"]
        assert snapshot["metadata_overhead_ms"] >= 0
        assert snapshot["max_call_wal_bytes_before"]["sqlite"] >= 0
        assert getattr(probe.local,"operation") == "other_private_work"
    finally:
        connection.close()
